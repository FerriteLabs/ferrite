//! TCP Server listener
//!
//! This module implements the main TCP server that accepts connections.

use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use crate::audit::{noop_handle, AuditLogger, SharedAuditHandle};
use crate::auth::{Acl, SharedAcl};
use crate::commands::{
    BlockingListManager, BlockingSortedSetManager, BlockingStreamManager,
    SharedBlockingListManager, SharedBlockingSortedSetManager, SharedBlockingStreamManager,
};
use crate::config::{Config, SharedConfig, DEFAULT_REPLICATION_BACKLOG_BYTES};
use crate::error::Result;
use crate::replication::{
    ReplicationPrimary, ReplicationState, ReplicationStream, SharedReplicationState,
};
use crate::storage::Store;
use ferrite_core::telemetry::metrics as otel_metrics;

use super::backpressure::{BackpressureController, SharedBackpressure};
use super::connection::Connection;
use super::handler::{Handler, HandlerDependencies};
use crate::runtime::{
    ClientRegistry, SharedClientRegistry, SharedSlowLog, SharedSubscriptionManager,
    SharedWatchRegistry, SlowLog, SubscriptionManager, WatchRegistry,
};

/// Ferrite TCP Server
pub struct Server {
    /// Server configuration (shared for hot reload)
    config: SharedConfig,

    /// TCP listener
    listener: TcpListener,

    /// Shared storage
    store: Arc<Store>,

    /// Pub/Sub subscription manager
    pubsub_manager: SharedSubscriptionManager,

    /// ACL manager
    acl: SharedAcl,

    /// Blocking list manager for BLPOP/BRPOP
    blocking_manager: SharedBlockingListManager,

    /// Blocking stream manager for XREAD BLOCK/XREADGROUP BLOCK
    blocking_stream_manager: SharedBlockingStreamManager,

    /// Blocking sorted set manager for BZPOPMIN/BZPOPMAX/BZMPOP
    blocking_zset_manager: SharedBlockingSortedSetManager,

    /// Replication state
    replication_state: SharedReplicationState,

    /// Replication primary handler (for propagating commands)
    replication_primary: Arc<ReplicationPrimary>,

    /// Audit logging handle
    audit_handle: SharedAuditHandle,

    /// Slow query log
    slowlog: SharedSlowLog,

    /// Client registry
    client_registry: SharedClientRegistry,

    /// WATCH registry for cross-connection key invalidation
    watch_registry: SharedWatchRegistry,

    /// Shutdown signal sender
    shutdown_tx: broadcast::Sender<()>,

    /// Shared backpressure controller for memory-aware write rejection
    backpressure: SharedBackpressure,
}

/// Optional dependency overrides for building a Server
#[derive(Default)]
pub struct ServerDependencies {
    pub store: Option<Arc<Store>>,
    pub pubsub_manager: Option<SharedSubscriptionManager>,
    pub acl: Option<SharedAcl>,
    pub blocking_manager: Option<SharedBlockingListManager>,
    pub blocking_stream_manager: Option<SharedBlockingStreamManager>,
    pub blocking_zset_manager: Option<SharedBlockingSortedSetManager>,
    pub slowlog: Option<SharedSlowLog>,
    pub client_registry: Option<SharedClientRegistry>,
    pub watch_registry: Option<SharedWatchRegistry>,
}

impl Server {
    /// Create a new server instance
    pub async fn new(config: Config) -> Result<Self> {
        Self::new_with_dependencies(config, ServerDependencies::default()).await
    }

    /// Create a new server instance with optional dependency overrides
    pub async fn new_with_dependencies(
        config: Config,
        dependencies: ServerDependencies,
    ) -> Result<Self> {
        let addr = config.server.address();
        let listener = TcpListener::bind(&addr).await?;

        info!("Server listening on {}", addr);

        let store = dependencies
            .store
            .unwrap_or_else(|| Arc::new(Store::new(config.storage.databases)));
        let pubsub_manager = dependencies
            .pubsub_manager
            .unwrap_or_else(|| Arc::new(SubscriptionManager::new()));
        let acl = dependencies.acl.unwrap_or_else(|| Arc::new(Acl::new()));
        let blocking_manager = dependencies
            .blocking_manager
            .unwrap_or_else(|| Arc::new(BlockingListManager::new()));
        let blocking_stream_manager = dependencies
            .blocking_stream_manager
            .unwrap_or_else(|| Arc::new(BlockingStreamManager::new()));
        let blocking_zset_manager = dependencies
            .blocking_zset_manager
            .unwrap_or_else(|| Arc::new(BlockingSortedSetManager::new()));
        let (shutdown_tx, _) = broadcast::channel(1);

        // Initialize replication
        let replication_state = Arc::new(ReplicationState::new());
        let replication_stream = Arc::new(ReplicationStream::new(
            config
                .replication
                .backlog_size
                .unwrap_or(DEFAULT_REPLICATION_BACKLOG_BYTES),
        ));
        let replication_primary = Arc::new(ReplicationPrimary::new(
            store.clone(),
            replication_state.clone(),
            replication_stream,
        ));

        // Initialize audit logging
        let audit_handle = if config.audit.enabled {
            info!("Audit logging enabled");
            match AuditLogger::new(config.audit.clone()).await {
                Ok((logger, handle)) => {
                    let handle = Arc::new(handle);
                    // Spawn the audit logger task
                    tokio::spawn(async move {
                        logger.run().await;
                    });
                    debug!("Audit logger started");
                    handle
                }
                Err(e) => {
                    error!("Failed to initialize audit logger: {}", e);
                    noop_handle()
                }
            }
        } else {
            noop_handle()
        };

        // Wrap config in SharedConfig for hot reload support
        let shared_config = SharedConfig::new(config);

        // Initialize slow log with config values
        let slowlog = dependencies
            .slowlog
            .unwrap_or_else(|| Arc::new(SlowLog::default()));

        // Initialize client registry
        let client_registry = dependencies
            .client_registry
            .unwrap_or_else(|| Arc::new(ClientRegistry::new()));

        // Initialize WATCH registry for cross-connection key invalidation
        let watch_registry = dependencies
            .watch_registry
            .unwrap_or_else(|| Arc::new(WatchRegistry::new()));

        // Initialize backpressure controller from config
        let cfg = shared_config.read();
        let backpressure = Arc::new(BackpressureController::new(
            cfg.server.max_memory,
            cfg.server.max_memory_reject_threshold,
        ));
        drop(cfg);

        Ok(Self {
            config: shared_config,
            listener,
            store,
            pubsub_manager,
            acl,
            blocking_manager,
            blocking_stream_manager,
            blocking_zset_manager,
            replication_state,
            replication_primary,
            audit_handle,
            slowlog,
            client_registry,
            watch_registry,
            shutdown_tx,
            backpressure,
        })
    }

    /// Run the server until shutdown signal is received
    pub async fn run(self) -> Result<()> {
        let shutdown_tx = self.shutdown_tx.clone();

        // Spawn shutdown signal handler
        tokio::spawn(async move {
            if let Err(e) = signal::ctrl_c().await {
                error!("Failed to listen for ctrl-c: {}", e);
                return;
            }
            info!("Shutdown signal received");
            let _ = shutdown_tx.send(());
        });

        // Spawn background memory monitor for backpressure
        if !self.backpressure.is_unlimited() {
            let bp = self.backpressure.clone();
            let mut bp_shutdown = self.shutdown_tx.subscribe();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            let rss = estimate_rss_bytes();
                            bp.update_memory(rss);
                            ferrite_core::metrics::set_backpressure_memory_ratio(
                                bp.memory_ratio(),
                            );
                        }
                        _ = bp_shutdown.recv() => break,
                    }
                }
            });
            info!(
                "Memory backpressure monitor started (max_memory={})",
                self.backpressure.max_memory_bytes()
            );
        }

        // Spawn active key expiration background task
        {
            let store = self.store.clone();
            let mut exp_shutdown = self.shutdown_tx.subscribe();
            tokio::spawn(async move {
                // Sweep every 100ms, checking up to 20 keys per database per cycle
                // (matches Redis default: 10 Hz × 20 keys = 200 keys/sec per db)
                let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            let removed = store.sweep_expired(20);
                            if removed > 0 {
                                tracing::trace!("Active expiration removed {} keys", removed);
                            }
                        }
                        _ = exp_shutdown.recv() => break,
                    }
                }
            });
            info!("Active key expiration task started (10 Hz, 20 keys/db/cycle)");
        }

        // Spawn auto-save background task (checks every 60s if enough changes warrant a save)
        {
            let store = self.store.clone();
            let mut save_shutdown = self.shutdown_tx.subscribe();
            let save_threshold: u64 = 1000; // save if >= 1000 changes
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            let changes = store.changes_since_save();
                            if changes >= save_threshold {
                                tracing::info!(
                                    "Auto-save triggered ({} changes since last save)",
                                    changes
                                );
                                let rdb_data = ferrite_core::persistence::generate_rdb(&store);
                                let path = std::path::Path::new("dump.rdb");
                                match tokio::fs::write(path, &rdb_data).await {
                                    Ok(()) => {
                                        store.reset_changes_since_save();
                                        tracing::info!(
                                            "Auto-save completed ({} bytes)",
                                            rdb_data.len()
                                        );
                                    }
                                    Err(e) => {
                                        tracing::error!("Auto-save failed: {}", e);
                                    }
                                }
                            }
                        }
                        _ = save_shutdown.recv() => break,
                    }
                }
            });
            info!(
                "Auto-save task started (every 60s, threshold: {} changes)",
                save_threshold
            );
        }

        self.accept_loop().await
    }

    /// Accept connections in a loop
    async fn accept_loop(self) -> Result<()> {
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                result = self.listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            // Check connection limit before accepting
                            let max_conn = self.config.read().server.max_connections;
                            if !super::backpressure::should_accept_connection(
                                self.client_registry.count(),
                                max_conn,
                            ) {
                                warn!(
                                    "Rejecting connection from {}: max connections ({}) reached",
                                    addr, max_conn
                                );
                                ferrite_core::metrics::record_connection_rejected();
                                drop(stream);
                                continue;
                            }

                            info!("Accepted connection from {}", addr);
                            otel_metrics::connection_opened();

                            // Disable Nagle's algorithm for low-latency command processing
                            if let Err(e) = stream.set_nodelay(true) {
                                tracing::warn!("Failed to set TCP_NODELAY: {}", e);
                            }

                            // Register client in the registry
                            let client_id = self.client_registry.register(Some(addr));

                            let connection = Connection::with_parser_limits(
                                stream,
                                self.config.read().server.parser_limits(),
                            );
                            let deps = HandlerDependencies {
                                store: self.store.clone(),
                                pubsub_manager: self.pubsub_manager.clone(),
                                acl: self.acl.clone(),
                                blocking_manager: self.blocking_manager.clone(),
                                blocking_stream_manager: self.blocking_stream_manager.clone(),
                                blocking_zset_manager: self.blocking_zset_manager.clone(),
                                replication_state: self.replication_state.clone(),
                                replication_primary: self.replication_primary.clone(),
                                audit_handle: self.audit_handle.clone(),
                                shutdown_rx: self.shutdown_tx.subscribe(),
                                shutdown_tx: self.shutdown_tx.clone(),
                                slowlog: self.slowlog.clone(),
                                client_registry: self.client_registry.clone(),
                                client_id,
                                watch_registry: self.watch_registry.clone(),
                                config: Some(self.config.clone()),
                                backpressure: self.backpressure.clone(),
                            };
                            let handler = Handler::new(connection, deps);

                            let client_registry = self.client_registry.clone();
                            tokio::spawn(async move {
                                if let Err(e) = handler.run().await {
                                    warn!("Connection error from {}: {}", addr, e);
                                }
                                // Unregister client when connection ends
                                client_registry.unregister(client_id);
                                otel_metrics::connection_closed();
                                debug!("Client {} disconnected", client_id);
                            });
                        }
                        Err(e) => {
                            error!("Failed to accept connection: {}", e);
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Server shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Get a reference to the store (for testing)
    #[cfg(test)]
    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }
}

/// Estimate the process's resident set size in bytes.
///
/// On macOS, reads from `mach_task_basic_info`. On Linux, reads from
/// `/proc/self/statm`. Falls back to 0 on unsupported platforms.
fn estimate_rss_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        // /proc/self/statm: size resident shared text lib data dt (pages)
        if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
            if let Some(rss_pages) = statm.split_whitespace().nth(1) {
                if let Ok(pages) = rss_pages.parse::<u64>() {
                    return pages * 4096; // page size is typically 4K
                }
            }
        }
        0
    }
    #[cfg(target_os = "macos")]
    {
        // Use mach API to get RSS
        use std::mem;
        extern "C" {
            fn mach_task_self() -> u32;
            fn task_info(
                target_task: u32,
                flavor: u32,
                task_info_out: *mut libc_task_basic_info,
                task_info_out_cnt: *mut u32,
            ) -> i32;
        }
        #[repr(C)]
        struct libc_task_basic_info {
            suspend_count: i32,
            virtual_size: u64,
            resident_size: u64,
            user_time: [u32; 2],
            system_time: [u32; 2],
            policy: i32,
        }
        const MACH_TASK_BASIC_INFO: u32 = 20;
        // SAFETY: zeroed memory is valid for libc_task_basic_info (all-integer POD struct)
        let mut info: libc_task_basic_info = unsafe { mem::zeroed() };
        let mut count = (mem::size_of::<libc_task_basic_info>() / mem::size_of::<u32>()) as u32;
        // SAFETY: task_info is a stable macOS Mach API; we pass correctly-sized buffer
        // and count. mach_task_self() always returns the current task port.
        let kr = unsafe {
            task_info(
                mach_task_self(),
                MACH_TASK_BASIC_INFO,
                &raw mut info,
                &raw mut count,
            )
        };
        if kr == 0 {
            return info.resident_size;
        }
        0
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        0
    }
}
