//! Network Transport Layer
//!
//! High-performance networking with support for multiple transport protocols:
//!
//! - **RDMA**: Sub-microsecond latency with kernel bypass (InfiniBand, RoCE, iWARP)
//! - **TCP**: Standard TCP/IP with optional optimizations (TCP_NODELAY, SO_BUSY_POLL)
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────┐
//! │                        Application Layer                            │
//! │              (Replication, Cluster, Client Connections)             │
//! └──────────────────────────────┬─────────────────────────────────────┘
//!                                │
//! ┌──────────────────────────────▼─────────────────────────────────────┐
//! │                      Transport Trait                                │
//! │           Unified interface for all network operations              │
//! └──────────────────────────────┬─────────────────────────────────────┘
//!                                │
//!            ┌───────────────────┼───────────────────┐
//!            │                   │                   │
//!            ▼                   ▼                   ▼
//!     ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
//!     │    RDMA      │   │     TCP      │   │   Hybrid     │
//!     │  Transport   │   │  Transport   │   │  Transport   │
//!     │(InfiniBand)  │   │  (tokio)     │   │ (RDMA+TCP)   │
//!     └──────────────┘   └──────────────┘   └──────────────┘
//! ```
//!
//! # Transport Selection
//!
//! The network layer automatically selects the best transport based on:
//! - Hardware availability (RDMA requires supported NICs)
//! - Network topology (RDMA typically requires same subnet)
//! - Latency requirements (RDMA for <10μs, TCP for general use)
//!
//! # Example
//!
//! ```ignore
//! use ferrite::network::{Transport, TransportConfig, create_transport};
//!
//! // Auto-detect best transport
//! let transport = create_transport(TransportConfig::auto())?;
//!
//! // Connect to peer
//! let conn = transport.connect("192.168.1.100:6379").await?;
//!
//! // Send data (uses RDMA if available, falls back to TCP)
//! conn.send(data).await?;
//! ```

pub mod api_extensions;
pub mod api_gateway;
pub mod optimization;
pub mod protocol;
pub mod rdma;
pub mod s3_compat;
pub mod s3_server;
pub mod tcp;

pub use optimization::{
    AdaptiveBatchConfig, AdaptiveBatcher, AdaptiveBatcherStats, AdaptivePoller, CacheAlignedBuffer,
    CacheAlignedPool, CacheAlignedPoolStats, CompletionCoalescer, CompletionCoalescerStats,
    NumaAllocator, NumaAllocatorStats, PollingMode, RdmaPerformanceMonitor, RdmaPerformanceStats,
    CACHE_LINE_SIZE,
};
pub use protocol::{
    BatchRequest, BatchResponse, ConnectionPool, DoorbellBatcher, ProtocolConfig, ProtocolError,
    ProtocolHandlerBuilder, ProtocolStats, RdmaKvHeader, RdmaOperationMode, RdmaProtocolHandler,
};
pub use rdma::{
    AccessFlags, ConnectionInfo, MemoryPool, MemoryPoolConfig, MemoryPoolStats, MemoryRegion,
    PathMtu, QueuePair, QueuePairAttr, QueuePairState, QueuePairType, RdmaConfig, RdmaConnection,
    RdmaError, RdmaListener, RdmaTransport, RdmaTransportBuilder, RdmaTransportStats,
    RemoteAddress, SendFlags, WorkCompletion, WorkCompletionOpcode, WorkCompletionStatus,
};
pub use tcp::{
    TcpConfig, TcpConnection, TcpError, TcpListener, TcpTransport, TcpTransportBuilder,
    TcpTransportStats,
};

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

/// Network transport errors
#[derive(Debug, Error)]
pub enum TransportError {
    /// RDMA error
    #[error("RDMA error: {0}")]
    Rdma(#[from] RdmaError),

    /// TCP error
    #[error("TCP error: {0}")]
    Tcp(#[from] TcpError),

    /// Connection refused
    #[error("connection refused: {0}")]
    ConnectionRefused(String),

    /// Timeout
    #[error("operation timed out")]
    Timeout,

    /// Transport not available
    #[error("transport not available: {0}")]
    NotAvailable(String),

    /// Invalid configuration
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result type for transport operations
pub type Result<T> = std::result::Result<T, TransportError>;

/// Transport type selection
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransportType {
    /// Automatic selection (RDMA if available, else TCP)
    #[default]
    Auto,
    /// Force RDMA (fails if not available)
    Rdma,
    /// Force TCP
    Tcp,
    /// Hybrid: RDMA for data, TCP for control
    Hybrid,
}

/// Transport configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransportConfig {
    /// Transport type
    pub transport_type: TransportType,
    /// RDMA configuration
    pub rdma: RdmaConfig,
    /// TCP configuration
    pub tcp: TcpConfig,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Keepalive interval
    pub keepalive_interval: Duration,
    /// Maximum message size
    pub max_message_size: usize,
    /// Buffer size
    pub buffer_size: usize,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            transport_type: TransportType::Auto,
            rdma: RdmaConfig::default(),
            tcp: TcpConfig::default(),
            connect_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_secs(30),
            max_message_size: 512 * 1024 * 1024, // 512MB
            buffer_size: 64 * 1024,              // 64KB
        }
    }
}

impl TransportConfig {
    /// Create auto-detect configuration
    pub fn auto() -> Self {
        Self::default()
    }

    /// Create RDMA-only configuration
    pub fn rdma() -> Self {
        Self {
            transport_type: TransportType::Rdma,
            ..Default::default()
        }
    }

    /// Create TCP-only configuration
    pub fn tcp() -> Self {
        Self {
            transport_type: TransportType::Tcp,
            ..Default::default()
        }
    }

    /// Create hybrid configuration
    pub fn hybrid() -> Self {
        Self {
            transport_type: TransportType::Hybrid,
            ..Default::default()
        }
    }
}

/// Network connection trait
#[async_trait]
pub trait Connection: Send + Sync {
    /// Get connection ID
    fn id(&self) -> u64;

    /// Get remote address
    fn remote_addr(&self) -> SocketAddr;

    /// Check if connection is active
    fn is_active(&self) -> bool;

    /// Send data
    async fn send(&self, data: Bytes) -> Result<()>;

    /// Receive data
    async fn recv(&self) -> Result<Bytes>;

    /// Close the connection
    async fn close(&self) -> Result<()>;
}

/// Network listener trait
#[async_trait]
pub trait Listener: Send + Sync {
    /// Connection type produced by this listener
    type Connection: Connection;

    /// Get local address
    fn local_addr(&self) -> SocketAddr;

    /// Accept an incoming connection
    async fn accept(&self) -> Result<Self::Connection>;

    /// Close the listener
    fn close(&self);
}

/// Network transport trait
#[async_trait]
pub trait Transport: Send + Sync {
    /// Connection type for this transport
    type Connection: Connection;
    /// Listener type for this transport
    type Listener: Listener<Connection = Self::Connection>;

    /// Get transport type
    fn transport_type(&self) -> TransportType;

    /// Check if transport is available
    fn is_available(&self) -> bool;

    /// Initialize the transport
    async fn initialize(&self) -> Result<()>;

    /// Connect to a remote peer
    async fn connect(&self, addr: SocketAddr) -> Result<Self::Connection>;

    /// Listen for incoming connections
    async fn listen(&self, addr: SocketAddr) -> Result<Self::Listener>;

    /// Shutdown the transport
    async fn shutdown(&self);
}

/// Unified transport that wraps RDMA or TCP
pub enum UnifiedTransport {
    /// RDMA transport
    Rdma(Arc<RdmaTransport>),
    /// TCP transport
    Tcp(Arc<TcpTransport>),
}

impl UnifiedTransport {
    /// Get transport type
    pub fn transport_type(&self) -> TransportType {
        match self {
            UnifiedTransport::Rdma(_) => TransportType::Rdma,
            UnifiedTransport::Tcp(_) => TransportType::Tcp,
        }
    }

    /// Check availability
    pub fn is_available(&self) -> bool {
        match self {
            UnifiedTransport::Rdma(t) => t.is_available(),
            UnifiedTransport::Tcp(t) => t.is_available(),
        }
    }

    /// Initialize
    pub async fn initialize(&self) -> Result<()> {
        match self {
            UnifiedTransport::Rdma(t) => t.initialize().await.map_err(TransportError::Rdma),
            UnifiedTransport::Tcp(t) => t.initialize().await.map_err(TransportError::Tcp),
        }
    }

    /// Shutdown
    pub async fn shutdown(&self) {
        match self {
            UnifiedTransport::Rdma(t) => t.shutdown().await,
            UnifiedTransport::Tcp(t) => t.shutdown().await,
        }
    }
}

/// Create a transport based on configuration
pub fn create_transport(config: TransportConfig) -> Result<UnifiedTransport> {
    match config.transport_type {
        TransportType::Rdma => {
            let transport = RdmaTransport::new(config.rdma)?;
            if transport.is_available() {
                Ok(UnifiedTransport::Rdma(Arc::new(transport)))
            } else {
                Err(TransportError::NotAvailable(
                    "RDMA hardware not found".into(),
                ))
            }
        }
        TransportType::Tcp => {
            let transport = TcpTransport::new(config.tcp)?;
            Ok(UnifiedTransport::Tcp(Arc::new(transport)))
        }
        TransportType::Auto => {
            // Try RDMA first, fall back to TCP
            let rdma_transport = RdmaTransport::new(config.rdma.clone())?;
            if rdma_transport.is_available() {
                tracing::info!("Using RDMA transport");
                Ok(UnifiedTransport::Rdma(Arc::new(rdma_transport)))
            } else {
                tracing::info!("RDMA not available, using TCP transport");
                let tcp_transport = TcpTransport::new(config.tcp)?;
                Ok(UnifiedTransport::Tcp(Arc::new(tcp_transport)))
            }
        }
        TransportType::Hybrid => {
            // For now, hybrid mode uses RDMA if available, else TCP
            // Full hybrid would use both (RDMA for data, TCP for control)
            create_transport(TransportConfig {
                transport_type: TransportType::Auto,
                ..config
            })
        }
    }
}

/// Transport statistics
#[derive(Clone, Debug, Default)]
pub struct TransportStats {
    /// Transport type
    pub transport_type: String,
    /// Active connections
    pub active_connections: u64,
    /// Total connections
    pub total_connections: u64,
    /// Bytes sent
    pub bytes_sent: u64,
    /// Bytes received
    pub bytes_received: u64,
    /// Average latency in microseconds
    pub avg_latency_us: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transport_config_default() {
        let config = TransportConfig::default();
        assert_eq!(config.transport_type, TransportType::Auto);
    }

    #[test]
    fn test_transport_config_variants() {
        assert_eq!(TransportConfig::rdma().transport_type, TransportType::Rdma);
        assert_eq!(TransportConfig::tcp().transport_type, TransportType::Tcp);
        assert_eq!(
            TransportConfig::hybrid().transport_type,
            TransportType::Hybrid
        );
    }

    #[test]
    fn test_create_tcp_transport() {
        let config = TransportConfig::tcp();
        let transport = create_transport(config);
        assert!(transport.is_ok());
        assert!(matches!(transport.unwrap(), UnifiedTransport::Tcp(_)));
    }
}
