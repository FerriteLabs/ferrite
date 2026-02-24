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
                            info!("Accepted connection from {}", addr);
                            otel_metrics::connection_opened();

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
                                slowlog: self.slowlog.clone(),
                                client_registry: self.client_registry.clone(),
                                client_id,
                                watch_registry: self.watch_registry.clone(),
                                config: Some(self.config.clone()),
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
