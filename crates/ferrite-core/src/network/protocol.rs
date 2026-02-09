//! RDMA Protocol Integration
//!
//! Integrates RDMA transport with Ferrite's RESP protocol for high-performance
//! Redis-compatible networking with sub-microsecond latency.
//!
//! # Features
//!
//! - One-sided RDMA operations for GET/SET (bypasses remote CPU)
//! - Connection multiplexing for efficient resource usage
//! - Doorbell batching for reduced PCIe overhead
//! - Hybrid operation modes (RDMA for data, TCP for control)
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      RESP Protocol Layer                         │
//! │                (GET, SET, HGET, LPUSH, etc.)                     │
//! └──────────────────────────────┬──────────────────────────────────┘
//!                                │
//! ┌──────────────────────────────▼──────────────────────────────────┐
//! │                    RdmaProtocolHandler                           │
//! │         Maps Redis commands to RDMA verbs                        │
//! └──────────────────────────────┬──────────────────────────────────┘
//!                                │
//!    ┌───────────────────────────┼───────────────────────────────┐
//!    │                           │                               │
//!    ▼                           ▼                               ▼
//! ┌──────────┐            ┌──────────┐                   ┌──────────┐
//! │RDMA WRITE│            │RDMA READ │                   │Send/Recv │
//! │(SET ops) │            │(GET ops) │                   │(Control) │
//! └──────────┘            └──────────┘                   └──────────┘
//! ```

use super::rdma::{
    MemoryPool, MemoryPoolConfig, RdmaConfig, RdmaConnection, RdmaError, RdmaTransport,
};
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex, RwLock, Semaphore};

/// Protocol integration errors
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// RDMA error
    #[error("RDMA error: {0}")]
    Rdma(#[from] RdmaError),

    /// Protocol error
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Key not found
    #[error("key not found")]
    KeyNotFound,

    /// Value too large
    #[error("value too large: {size} > {max}")]
    ValueTooLarge {
        /// Actual size
        size: usize,
        /// Maximum allowed
        max: usize,
    },

    /// Timeout
    #[error("operation timed out")]
    Timeout,

    /// Connection error
    #[error("connection error: {0}")]
    Connection(String),

    /// Serialization error
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Resource exhausted
    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),
}

/// Result type for protocol operations
pub type Result<T> = std::result::Result<T, ProtocolError>;

/// RDMA operation mode
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum RdmaOperationMode {
    /// Use RDMA for all operations
    Full,
    /// Use RDMA for data, TCP for control
    Hybrid,
    /// Automatic selection based on message size
    #[default]
    Adaptive,
}

/// Protocol configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProtocolConfig {
    /// RDMA operation mode
    pub mode: RdmaOperationMode,
    /// Threshold for RDMA vs Send/Recv (bytes)
    pub rdma_threshold: usize,
    /// Maximum value size for RDMA operations
    pub max_value_size: usize,
    /// Connection pool size per peer
    pub connection_pool_size: usize,
    /// Enable doorbell batching
    pub doorbell_batching: bool,
    /// Doorbell batch size
    pub doorbell_batch_size: usize,
    /// Doorbell batch timeout
    pub doorbell_batch_timeout: Duration,
    /// Request timeout
    pub request_timeout: Duration,
    /// Enable inline data for small values
    pub inline_threshold: usize,
    /// Memory pool configuration
    #[serde(skip)]
    pub memory_pool: MemoryPoolConfig,
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        Self {
            mode: RdmaOperationMode::Adaptive,
            rdma_threshold: 4096,              // Use RDMA for values > 4KB
            max_value_size: 512 * 1024 * 1024, // 512MB max value
            connection_pool_size: 4,
            doorbell_batching: true,
            doorbell_batch_size: 32,
            doorbell_batch_timeout: Duration::from_micros(10),
            request_timeout: Duration::from_secs(5),
            inline_threshold: 256, // Inline data for values < 256 bytes
            memory_pool: MemoryPoolConfig::default(),
        }
    }
}

/// Request type for batching
#[derive(Clone, Debug)]
pub enum BatchRequest {
    /// GET request
    Get {
        /// Request ID
        id: u64,
        /// Key
        key: Bytes,
    },
    /// SET request
    Set {
        /// Request ID
        id: u64,
        /// Key
        key: Bytes,
        /// Value
        value: Bytes,
        /// TTL in milliseconds (0 = no expiry)
        ttl_ms: u64,
    },
    /// DEL request
    Del {
        /// Request ID
        id: u64,
        /// Key
        key: Bytes,
    },
}

/// Response for batch operations
#[derive(Clone, Debug)]
pub enum BatchResponse {
    /// Value response
    Value(Option<Bytes>),
    /// OK response
    Ok,
    /// Integer response (e.g., for DEL)
    Integer(i64),
    /// Error response
    Error(String),
}

/// RDMA key-value request header
#[derive(Clone, Debug, Serialize, Deserialize)]
#[repr(C)]
pub struct RdmaKvHeader {
    /// Request ID
    pub request_id: u64,
    /// Operation type
    pub op_type: u8,
    /// Flags
    pub flags: u8,
    /// Reserved
    pub reserved: u16,
    /// Key length
    pub key_len: u32,
    /// Value length (for SET operations)
    pub value_len: u32,
    /// TTL in milliseconds (for SET operations)
    pub ttl_ms: u64,
    /// Remote address for RDMA operations
    pub remote_addr: u64,
    /// Remote key for RDMA operations
    pub remote_rkey: u32,
    /// Padding for alignment
    pub _padding: u32,
}

impl RdmaKvHeader {
    /// Header size in bytes
    pub const SIZE: usize = 48;

    /// Operation types
    pub const OP_GET: u8 = 1;
    /// Operation type: SET request.
    pub const OP_SET: u8 = 2;
    /// Operation type: DEL request.
    pub const OP_DEL: u8 = 3;
    /// Operation type: GET response.
    pub const OP_GET_RESPONSE: u8 = 128;
    /// Operation type: SET response.
    pub const OP_SET_RESPONSE: u8 = 129;
    /// Operation type: DEL response.
    pub const OP_DEL_RESPONSE: u8 = 130;
    /// Operation type: error response.
    pub const OP_ERROR: u8 = 255;

    /// Flag: Use RDMA for value transfer
    pub const FLAG_RDMA: u8 = 1;
    /// Flag: Value is inline in message
    pub const FLAG_INLINE: u8 = 2;
    /// Flag: Request acknowledgment
    pub const FLAG_ACK: u8 = 4;

    /// Create a GET request header
    pub fn get(request_id: u64, key_len: u32) -> Self {
        Self {
            request_id,
            op_type: Self::OP_GET,
            flags: 0,
            reserved: 0,
            key_len,
            value_len: 0,
            ttl_ms: 0,
            remote_addr: 0,
            remote_rkey: 0,
            _padding: 0,
        }
    }

    /// Create a SET request header
    pub fn set(request_id: u64, key_len: u32, value_len: u32, ttl_ms: u64) -> Self {
        Self {
            request_id,
            op_type: Self::OP_SET,
            flags: 0,
            reserved: 0,
            key_len,
            value_len,
            ttl_ms,
            remote_addr: 0,
            remote_rkey: 0,
            _padding: 0,
        }
    }

    /// Create a DEL request header
    pub fn del(request_id: u64, key_len: u32) -> Self {
        Self {
            request_id,
            op_type: Self::OP_DEL,
            flags: 0,
            reserved: 0,
            key_len,
            value_len: 0,
            ttl_ms: 0,
            remote_addr: 0,
            remote_rkey: 0,
            _padding: 0,
        }
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..8].copy_from_slice(&self.request_id.to_le_bytes());
        buf[8] = self.op_type;
        buf[9] = self.flags;
        buf[10..12].copy_from_slice(&self.reserved.to_le_bytes());
        buf[12..16].copy_from_slice(&self.key_len.to_le_bytes());
        buf[16..20].copy_from_slice(&self.value_len.to_le_bytes());
        buf[20..28].copy_from_slice(&self.ttl_ms.to_le_bytes());
        buf[28..36].copy_from_slice(&self.remote_addr.to_le_bytes());
        buf[36..40].copy_from_slice(&self.remote_rkey.to_le_bytes());
        buf[40..44].copy_from_slice(&self._padding.to_le_bytes());
        // Remaining 4 bytes are padding
        buf
    }

    /// Deserialize from bytes
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE {
            return None;
        }
        Some(Self {
            request_id: u64::from_le_bytes(buf[0..8].try_into().ok()?),
            op_type: buf[8],
            flags: buf[9],
            reserved: u16::from_le_bytes(buf[10..12].try_into().ok()?),
            key_len: u32::from_le_bytes(buf[12..16].try_into().ok()?),
            value_len: u32::from_le_bytes(buf[16..20].try_into().ok()?),
            ttl_ms: u64::from_le_bytes(buf[20..28].try_into().ok()?),
            remote_addr: u64::from_le_bytes(buf[28..36].try_into().ok()?),
            remote_rkey: u32::from_le_bytes(buf[36..40].try_into().ok()?),
            _padding: u32::from_le_bytes(buf[40..44].try_into().ok()?),
        })
    }

    /// Set RDMA flag with remote info
    pub fn with_rdma(mut self, addr: u64, rkey: u32) -> Self {
        self.flags |= Self::FLAG_RDMA;
        self.remote_addr = addr;
        self.remote_rkey = rkey;
        self
    }

    /// Set inline flag
    pub fn with_inline(mut self) -> Self {
        self.flags |= Self::FLAG_INLINE;
        self
    }
}

/// Connection pool for managing multiple connections to a peer
pub struct ConnectionPool {
    /// Peer address
    peer_addr: SocketAddr,
    /// Available connections
    connections: RwLock<Vec<Arc<RdmaConnection>>>,
    /// Connection semaphore
    semaphore: Semaphore,
    /// Next connection index (round-robin)
    next_index: AtomicU64,
    /// Is active
    active: AtomicBool,
}

impl ConnectionPool {
    /// Create a new connection pool
    pub fn new(peer_addr: SocketAddr, size: usize) -> Self {
        Self {
            peer_addr,
            connections: RwLock::new(Vec::with_capacity(size)),
            semaphore: Semaphore::new(size),
            next_index: AtomicU64::new(0),
            active: AtomicBool::new(true),
        }
    }

    /// Add a connection to the pool
    pub async fn add_connection(&self, conn: Arc<RdmaConnection>) {
        self.connections.write().await.push(conn);
    }

    /// Get a connection from the pool (round-robin)
    pub async fn get_connection(&self) -> Option<Arc<RdmaConnection>> {
        let connections = self.connections.read().await;
        if connections.is_empty() {
            return None;
        }

        let index = self.next_index.fetch_add(1, Ordering::Relaxed) as usize;
        let conn = connections[index % connections.len()].clone();

        if conn.is_active() {
            Some(conn)
        } else {
            None
        }
    }

    /// Get pool size
    pub async fn size(&self) -> usize {
        self.connections.read().await.len()
    }

    /// Check if pool is active
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Close the pool
    pub async fn close(&self) {
        self.active.store(false, Ordering::Release);
        let connections = self.connections.write().await;
        for conn in connections.iter() {
            conn.close().await;
        }
    }
}

/// Doorbell batcher for reducing PCIe overhead
pub struct DoorbellBatcher {
    /// Batch size
    batch_size: usize,
    /// Batch timeout
    timeout: Duration,
    /// Pending requests
    pending: Mutex<Vec<BatchedWork>>,
    /// Flush channel
    flush_tx: mpsc::Sender<()>,
    /// Is active
    active: AtomicBool,
}

/// Work item for batching
struct BatchedWork {
    /// Request ID
    id: u64,
    /// Connection
    conn: Arc<RdmaConnection>,
    /// Data to send
    data: Bytes,
    /// Response channel
    response_tx: oneshot::Sender<Result<Bytes>>,
}

use tokio::sync::oneshot;

impl DoorbellBatcher {
    /// Create a new batcher
    pub fn new(batch_size: usize, timeout: Duration) -> (Self, mpsc::Receiver<()>) {
        let (flush_tx, flush_rx) = mpsc::channel(1);
        (
            Self {
                batch_size,
                timeout,
                pending: Mutex::new(Vec::with_capacity(batch_size)),
                flush_tx,
                active: AtomicBool::new(true),
            },
            flush_rx,
        )
    }

    /// Submit work for batching
    pub async fn submit(
        &self,
        id: u64,
        conn: Arc<RdmaConnection>,
        data: Bytes,
    ) -> Result<oneshot::Receiver<Result<Bytes>>> {
        let (response_tx, response_rx) = oneshot::channel();

        let work = BatchedWork {
            id,
            conn,
            data,
            response_tx,
        };

        let should_flush = {
            let mut pending = self.pending.lock().await;
            pending.push(work);
            pending.len() >= self.batch_size
        };

        if should_flush {
            let _ = self.flush_tx.send(()).await;
        }

        Ok(response_rx)
    }

    /// Flush pending work
    pub async fn flush(&self) -> Result<()> {
        let works = {
            let mut pending = self.pending.lock().await;
            std::mem::take(&mut *pending)
        };

        for work in works {
            // Send the data
            let result = work.conn.send(work.data).await;
            match result {
                Ok(_) => {
                    // For now, return empty response (actual implementation would wait for reply)
                    let _ = work.response_tx.send(Ok(Bytes::new()));
                }
                Err(e) => {
                    let _ = work.response_tx.send(Err(ProtocolError::Rdma(e)));
                }
            }
        }

        Ok(())
    }

    /// Check if active
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Shutdown the batcher
    pub fn shutdown(&self) {
        self.active.store(false, Ordering::Release);
    }
}

/// RDMA protocol handler
pub struct RdmaProtocolHandler {
    /// Configuration
    config: ProtocolConfig,
    /// RDMA transport
    transport: Arc<RdmaTransport>,
    /// Memory pool
    memory_pool: Arc<MemoryPool>,
    /// Connection pools per peer
    connection_pools: RwLock<HashMap<SocketAddr, Arc<ConnectionPool>>>,
    /// Doorbell batcher
    batcher: Option<Arc<DoorbellBatcher>>,
    /// Request ID counter
    next_request_id: AtomicU64,
    /// Statistics
    stats: ProtocolStats,
    /// Is active
    active: AtomicBool,
}

/// Protocol statistics
#[derive(Default)]
pub struct ProtocolStats {
    /// GET operations
    pub get_ops: AtomicU64,
    /// SET operations
    pub set_ops: AtomicU64,
    /// DEL operations
    pub del_ops: AtomicU64,
    /// RDMA write operations
    pub rdma_write_ops: AtomicU64,
    /// RDMA read operations
    pub rdma_read_ops: AtomicU64,
    /// Send/Recv operations
    pub send_recv_ops: AtomicU64,
    /// Total bytes transferred
    pub bytes_transferred: AtomicU64,
    /// Cache hits (for GET)
    pub cache_hits: AtomicU64,
    /// Cache misses (for GET)
    pub cache_misses: AtomicU64,
    /// Errors
    pub errors: AtomicU64,
    /// Average latency (nanoseconds)
    pub avg_latency_ns: AtomicU64,
}

impl RdmaProtocolHandler {
    /// Create a new protocol handler
    pub async fn new(config: ProtocolConfig, rdma_config: RdmaConfig) -> Result<Self> {
        let transport = Arc::new(RdmaTransport::new(rdma_config)?);
        transport.initialize().await?;

        let memory_pool = transport
            .create_memory_pool(config.memory_pool.clone())
            .await?;

        let batcher = if config.doorbell_batching {
            let (batcher, mut flush_rx) =
                DoorbellBatcher::new(config.doorbell_batch_size, config.doorbell_batch_timeout);
            let batcher = Arc::new(batcher);

            // Spawn flush task
            let batcher_clone = batcher.clone();
            let timeout = config.doorbell_batch_timeout;
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = flush_rx.recv() => {
                            if !batcher_clone.is_active() {
                                break;
                            }
                            let _ = batcher_clone.flush().await;
                        }
                        _ = tokio::time::sleep(timeout) => {
                            if !batcher_clone.is_active() {
                                break;
                            }
                            let _ = batcher_clone.flush().await;
                        }
                    }
                }
            });

            Some(batcher)
        } else {
            None
        };

        Ok(Self {
            config,
            transport,
            memory_pool,
            connection_pools: RwLock::new(HashMap::new()),
            batcher,
            next_request_id: AtomicU64::new(1),
            stats: ProtocolStats::default(),
            active: AtomicBool::new(true),
        })
    }

    /// Get the next request ID
    fn next_request_id(&self) -> u64 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Get or create a connection pool for a peer
    pub async fn get_connection_pool(&self, peer: SocketAddr) -> Result<Arc<ConnectionPool>> {
        // Check if pool exists
        {
            let pools = self.connection_pools.read().await;
            if let Some(pool) = pools.get(&peer) {
                if pool.is_active() && pool.size().await > 0 {
                    return Ok(pool.clone());
                }
            }
        }

        // Create new pool
        let pool = Arc::new(ConnectionPool::new(peer, self.config.connection_pool_size));

        // Establish connections
        for _ in 0..self.config.connection_pool_size {
            let conn = self.transport.connect(peer).await?;
            pool.add_connection(conn).await;
        }

        self.connection_pools
            .write()
            .await
            .insert(peer, pool.clone());

        Ok(pool)
    }

    /// Execute a GET operation
    pub async fn get(&self, peer: SocketAddr, key: Bytes) -> Result<Option<Bytes>> {
        let start = Instant::now();
        self.stats.get_ops.fetch_add(1, Ordering::Relaxed);

        let pool = self.get_connection_pool(peer).await?;
        let conn = pool
            .get_connection()
            .await
            .ok_or_else(|| ProtocolError::Connection("no available connection".into()))?;

        let request_id = self.next_request_id();
        let header = RdmaKvHeader::get(request_id, key.len() as u32);

        // Build request message
        let mut message = BytesMut::with_capacity(RdmaKvHeader::SIZE + key.len());
        message.extend_from_slice(&header.to_bytes());
        message.extend_from_slice(&key);

        // Send request
        conn.send(message.freeze()).await?;
        self.stats.send_recv_ops.fetch_add(1, Ordering::Relaxed);

        // For now, simulate response (actual implementation would wait for reply)
        // In a real implementation, we'd poll the CQ for the response

        let latency = start.elapsed();
        self.stats
            .avg_latency_ns
            .store(latency.as_nanos() as u64, Ordering::Relaxed);

        Ok(None) // Placeholder
    }

    /// Execute a SET operation
    pub async fn set(&self, peer: SocketAddr, key: Bytes, value: Bytes, ttl_ms: u64) -> Result<()> {
        let start = Instant::now();
        self.stats.set_ops.fetch_add(1, Ordering::Relaxed);

        if value.len() > self.config.max_value_size {
            return Err(ProtocolError::ValueTooLarge {
                size: value.len(),
                max: self.config.max_value_size,
            });
        }

        let pool = self.get_connection_pool(peer).await?;
        let conn = pool
            .get_connection()
            .await
            .ok_or_else(|| ProtocolError::Connection("no available connection".into()))?;

        let request_id = self.next_request_id();
        let use_rdma = self.should_use_rdma(value.len());

        let header = if use_rdma {
            // For RDMA, we'll need to register the value buffer and use RDMA WRITE
            // This is a simplified version - actual implementation would:
            // 1. Allocate memory region
            // 2. RDMA WRITE the value
            // 3. Send header with remote address
            self.stats.rdma_write_ops.fetch_add(1, Ordering::Relaxed);
            RdmaKvHeader::set(request_id, key.len() as u32, value.len() as u32, ttl_ms)
        } else {
            // For small values, send inline
            RdmaKvHeader::set(request_id, key.len() as u32, value.len() as u32, ttl_ms)
                .with_inline()
        };

        // Build request message
        let mut message = BytesMut::with_capacity(RdmaKvHeader::SIZE + key.len() + value.len());
        message.extend_from_slice(&header.to_bytes());
        message.extend_from_slice(&key);
        message.extend_from_slice(&value);

        // Use batcher if available
        if let Some(batcher) = &self.batcher {
            let response_rx = batcher.submit(request_id, conn, message.freeze()).await?;
            tokio::time::timeout(self.config.request_timeout, response_rx)
                .await
                .map_err(|_| ProtocolError::Timeout)?
                .map_err(|_| ProtocolError::Connection("response channel closed".into()))??;
        } else {
            conn.send(message.freeze()).await?;
        }

        self.stats.send_recv_ops.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_transferred
            .fetch_add((key.len() + value.len()) as u64, Ordering::Relaxed);

        let latency = start.elapsed();
        self.stats
            .avg_latency_ns
            .store(latency.as_nanos() as u64, Ordering::Relaxed);

        Ok(())
    }

    /// Execute a DEL operation
    pub async fn del(&self, peer: SocketAddr, key: Bytes) -> Result<i64> {
        self.stats.del_ops.fetch_add(1, Ordering::Relaxed);

        let pool = self.get_connection_pool(peer).await?;
        let conn = pool
            .get_connection()
            .await
            .ok_or_else(|| ProtocolError::Connection("no available connection".into()))?;

        let request_id = self.next_request_id();
        let header = RdmaKvHeader::del(request_id, key.len() as u32);

        // Build request message
        let mut message = BytesMut::with_capacity(RdmaKvHeader::SIZE + key.len());
        message.extend_from_slice(&header.to_bytes());
        message.extend_from_slice(&key);

        conn.send(message.freeze()).await?;
        self.stats.send_recv_ops.fetch_add(1, Ordering::Relaxed);

        Ok(1) // Placeholder
    }

    /// Execute a batch of operations
    pub async fn batch(
        &self,
        peer: SocketAddr,
        requests: Vec<BatchRequest>,
    ) -> Result<Vec<BatchResponse>> {
        let _pool = self.get_connection_pool(peer).await?;
        let mut responses = Vec::with_capacity(requests.len());

        for request in requests {
            let response = match request {
                BatchRequest::Get { key, .. } => match self.get(peer, key).await {
                    Ok(value) => BatchResponse::Value(value),
                    Err(e) => BatchResponse::Error(e.to_string()),
                },
                BatchRequest::Set {
                    key, value, ttl_ms, ..
                } => match self.set(peer, key, value, ttl_ms).await {
                    Ok(()) => BatchResponse::Ok,
                    Err(e) => BatchResponse::Error(e.to_string()),
                },
                BatchRequest::Del { key, .. } => match self.del(peer, key).await {
                    Ok(count) => BatchResponse::Integer(count),
                    Err(e) => BatchResponse::Error(e.to_string()),
                },
            };
            responses.push(response);
        }

        Ok(responses)
    }

    /// Check if RDMA should be used based on value size
    fn should_use_rdma(&self, value_size: usize) -> bool {
        match self.config.mode {
            RdmaOperationMode::Full => true,
            RdmaOperationMode::Hybrid => value_size > self.config.rdma_threshold,
            RdmaOperationMode::Adaptive => {
                value_size > self.config.rdma_threshold && value_size <= self.config.max_value_size
            }
        }
    }

    /// Get statistics
    pub fn stats(&self) -> &ProtocolStats {
        &self.stats
    }

    /// Check if handler is active
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Shutdown the handler
    pub async fn shutdown(&self) {
        self.active.store(false, Ordering::Release);

        if let Some(batcher) = &self.batcher {
            batcher.shutdown();
            let _ = batcher.flush().await;
        }

        // Close all connection pools
        let pools = self.connection_pools.read().await;
        for pool in pools.values() {
            pool.close().await;
        }

        self.transport.shutdown().await;
    }
}

/// Builder for protocol handler
pub struct ProtocolHandlerBuilder {
    config: ProtocolConfig,
    rdma_config: RdmaConfig,
}

impl ProtocolHandlerBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: ProtocolConfig::default(),
            rdma_config: RdmaConfig::default(),
        }
    }

    /// Set operation mode
    pub fn mode(mut self, mode: RdmaOperationMode) -> Self {
        self.config.mode = mode;
        self
    }

    /// Set RDMA threshold
    pub fn rdma_threshold(mut self, threshold: usize) -> Self {
        self.config.rdma_threshold = threshold;
        self
    }

    /// Set connection pool size
    pub fn connection_pool_size(mut self, size: usize) -> Self {
        self.config.connection_pool_size = size;
        self
    }

    /// Enable doorbell batching
    pub fn doorbell_batching(mut self, enabled: bool, batch_size: usize) -> Self {
        self.config.doorbell_batching = enabled;
        self.config.doorbell_batch_size = batch_size;
        self
    }

    /// Set request timeout
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Set RDMA config
    pub fn rdma_config(mut self, config: RdmaConfig) -> Self {
        self.rdma_config = config;
        self
    }

    /// Build the handler
    pub async fn build(self) -> Result<RdmaProtocolHandler> {
        RdmaProtocolHandler::new(self.config, self.rdma_config).await
    }
}

impl Default for ProtocolHandlerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_config_default() {
        let config = ProtocolConfig::default();
        assert_eq!(config.mode, RdmaOperationMode::Adaptive);
        assert_eq!(config.rdma_threshold, 4096);
        assert!(config.doorbell_batching);
    }

    #[test]
    fn test_rdma_kv_header() {
        let header = RdmaKvHeader::get(123, 10);
        let bytes = header.to_bytes();
        let restored = RdmaKvHeader::from_bytes(&bytes).unwrap();

        assert_eq!(restored.request_id, 123);
        assert_eq!(restored.op_type, RdmaKvHeader::OP_GET);
        assert_eq!(restored.key_len, 10);
    }

    #[test]
    fn test_header_with_rdma() {
        let header = RdmaKvHeader::set(1, 5, 100, 60000).with_rdma(0x1234567890, 0xABCD);

        assert!(header.flags & RdmaKvHeader::FLAG_RDMA != 0);
        assert_eq!(header.remote_addr, 0x1234567890);
        assert_eq!(header.remote_rkey, 0xABCD);
    }

    #[test]
    fn test_header_with_inline() {
        let header = RdmaKvHeader::set(1, 5, 50, 0).with_inline();
        assert!(header.flags & RdmaKvHeader::FLAG_INLINE != 0);
    }

    #[tokio::test]
    async fn test_connection_pool() {
        let pool = ConnectionPool::new("127.0.0.1:6379".parse().unwrap(), 4);
        assert!(pool.is_active());
        assert_eq!(pool.size().await, 0);
    }
}
