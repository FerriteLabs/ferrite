#![allow(dead_code)]
//! Transparent Redis proxy with connection pooling
//!
//! Accepts client connections and forwards commands to backend Ferrite/Redis
//! instances through a connection pool with read/write splitting.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use thiserror::Error;

use super::connection_pool::{ConnectionPool, PoolConfig, PoolError, PoolStats};

// ── Errors ───────────────────────────────────────────────────────────────────

/// Errors from the proxy layer.
#[derive(Debug, Error)]
pub enum ProxyError {
    /// Failed to bind the listening socket.
    #[error("bind failed: {0}")]
    BindFailed(String),

    /// Underlying pool error.
    #[error("pool error: {0}")]
    PoolError(#[from] PoolError),

    /// Error related to a client connection.
    #[error("client error: {0}")]
    ClientError(String),
}

// ── Configuration ────────────────────────────────────────────────────────────

/// Configuration for the proxy.
#[derive(Debug, Clone)]
pub struct ProxyConfig {
    /// Address to listen on for client connections.
    pub listen_addr: SocketAddr,
    /// Connection pool configuration.
    pub pool: PoolConfig,
    /// Maximum number of concurrent client connections.
    pub max_clients: u32,
    /// Whether to enable command pipelining.
    pub pipeline_enabled: bool,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:6380".parse().expect("valid default listen addr"),
            pool: PoolConfig::default(),
            max_clients: 10_000,
            pipeline_enabled: true,
        }
    }
}

// ── Proxy state ──────────────────────────────────────────────────────────────

/// Running state of the proxy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyState {
    /// Proxy is accepting and processing connections.
    Running,
    /// Proxy has been stopped.
    Stopped,
}

/// Handle returned when the proxy starts.
#[derive(Debug)]
pub struct ProxyHandle {
    /// The address the proxy is listening on.
    pub addr: SocketAddr,
    /// Current state.
    pub state: ProxyState,
}

/// Aggregated proxy statistics.
#[derive(Debug, Clone)]
pub struct ProxyStats {
    /// Number of clients currently connected.
    pub clients_connected: u64,
    /// Total number of commands forwarded.
    pub commands_proxied: u64,
    /// Total bytes received from clients.
    pub bytes_in: u64,
    /// Total bytes sent to clients.
    pub bytes_out: u64,
    /// Connection pool statistics.
    pub pool_stats: PoolStats,
    /// How long the proxy has been running.
    pub uptime: Duration,
}

// ── Proxy ────────────────────────────────────────────────────────────────────

/// A transparent Redis proxy with connection pooling.
pub struct FerritProxy {
    config: ProxyConfig,
    pool: Arc<ConnectionPool>,
    started_at: RwLock<Option<Instant>>,
    clients_connected: AtomicU64,
    commands_proxied: AtomicU64,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
    state: RwLock<ProxyState>,
}

impl FerritProxy {
    /// Create a new proxy with the given configuration.
    pub fn new(config: ProxyConfig) -> Self {
        let pool = Arc::new(ConnectionPool::new(config.pool.clone()));
        Self {
            config,
            pool,
            started_at: RwLock::new(None),
            clients_connected: AtomicU64::new(0),
            commands_proxied: AtomicU64::new(0),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            state: RwLock::new(ProxyState::Stopped),
        }
    }

    /// Start the proxy and begin accepting connections.
    ///
    /// Returns a [`ProxyHandle`] describing the listening address and state.
    /// The actual accept loop would be spawned on the Tokio runtime in a
    /// full implementation; this method sets up the initial state.
    pub fn start(&self) -> Result<ProxyHandle, ProxyError> {
        let mut state = self.state.write();
        if *state == ProxyState::Running {
            return Ok(ProxyHandle {
                addr: self.config.listen_addr,
                state: ProxyState::Running,
            });
        }

        *self.started_at.write() = Some(Instant::now());
        *state = ProxyState::Running;

        Ok(ProxyHandle {
            addr: self.config.listen_addr,
            state: ProxyState::Running,
        })
    }

    /// Retrieve current proxy statistics.
    pub fn stats(&self) -> ProxyStats {
        let uptime = self
            .started_at
            .read()
            .map(|t| t.elapsed())
            .unwrap_or_default();

        ProxyStats {
            clients_connected: self.clients_connected.load(Ordering::Relaxed),
            commands_proxied: self.commands_proxied.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            pool_stats: self.pool.stats(),
            uptime,
        }
    }

    /// Record that a client connected.
    pub fn on_client_connect(&self) {
        self.clients_connected.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a client disconnected.
    pub fn on_client_disconnect(&self) {
        self.clients_connected.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record proxied command metrics.
    pub fn on_command(&self, bytes_in: u64, bytes_out: u64) {
        self.commands_proxied.fetch_add(1, Ordering::Relaxed);
        self.bytes_in.fetch_add(bytes_in, Ordering::Relaxed);
        self.bytes_out.fetch_add(bytes_out, Ordering::Relaxed);
    }

    /// Get a reference to the underlying connection pool.
    pub fn pool(&self) -> &Arc<ConnectionPool> {
        &self.pool
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_proxy() -> FerritProxy {
        FerritProxy::new(ProxyConfig::default())
    }

    #[test]
    fn test_proxy_start() {
        let proxy = default_proxy();
        let handle = proxy.start().expect("should start");
        assert_eq!(handle.state, ProxyState::Running);
        assert_eq!(handle.addr, proxy.config.listen_addr);
    }

    #[test]
    fn test_proxy_double_start() {
        let proxy = default_proxy();
        let _h1 = proxy.start().expect("first start");
        let h2 = proxy.start().expect("second start should be idempotent");
        assert_eq!(h2.state, ProxyState::Running);
    }

    #[test]
    fn test_proxy_stats_initial() {
        let proxy = default_proxy();
        let stats = proxy.stats();
        assert_eq!(stats.clients_connected, 0);
        assert_eq!(stats.commands_proxied, 0);
        assert_eq!(stats.bytes_in, 0);
        assert_eq!(stats.bytes_out, 0);
    }

    #[test]
    fn test_proxy_client_tracking() {
        let proxy = default_proxy();
        proxy.on_client_connect();
        proxy.on_client_connect();
        assert_eq!(proxy.stats().clients_connected, 2);
        proxy.on_client_disconnect();
        assert_eq!(proxy.stats().clients_connected, 1);
    }

    #[test]
    fn test_proxy_command_metrics() {
        let proxy = default_proxy();
        proxy.on_command(100, 200);
        proxy.on_command(50, 150);
        let stats = proxy.stats();
        assert_eq!(stats.commands_proxied, 2);
        assert_eq!(stats.bytes_in, 150);
        assert_eq!(stats.bytes_out, 350);
    }

    #[test]
    fn test_proxy_pool_access() {
        let proxy = default_proxy();
        let pool = proxy.pool();
        let conn = pool.get_connection(false).expect("should acquire");
        pool.return_connection(conn);
        assert_eq!(pool.stats().total_acquired, 1);
    }
}
