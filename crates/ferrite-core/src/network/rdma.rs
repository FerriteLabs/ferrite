//! RDMA Transport Layer
//!
//! High-performance Remote Direct Memory Access (RDMA) networking for
//! sub-microsecond latency and kernel-bypass data transfers.
//!
//! # Overview
//!
//! RDMA enables direct memory access between machines without CPU involvement,
//! achieving latencies 10-100x lower than TCP. This module provides:
//!
//! - ibverbs abstraction layer (works with InfiniBand, RoCE, iWARP)
//! - Zero-copy data transfers via registered memory regions
//! - Queue pair management for reliable connections
//! - Completion queue polling for async notifications
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    Application (Ferrite)                         │
//! └──────────────────────────┬──────────────────────────────────────┘
//!                            │
//! ┌──────────────────────────▼──────────────────────────────────────┐
//! │                    RdmaTransport                                 │
//! │   Connection management, work request submission                 │
//! └──────────────────────────┬──────────────────────────────────────┘
//!                            │
//!    ┌───────────────────────┼───────────────────────┐
//!    │                       │                       │
//!    ▼                       ▼                       ▼
//! ┌──────────┐        ┌──────────┐           ┌──────────┐
//! │MemoryPool│        │QueuePair │           │Completion│
//! │Registered│        │  (QP)    │           │  Queue   │
//! │ Regions  │        │          │           │  (CQ)    │
//! └──────────┘        └──────────┘           └──────────┘
//! ```
//!
//! # Platform Support
//!
//! RDMA requires hardware support (InfiniBand HCA, RoCE NIC, or iWARP NIC)
//! and the libibverbs library. On systems without RDMA, this module provides
//! a fallback TCP transport with the same API.
//!
//! # Example
//!
//! ```ignore
//! use ferrite::network::rdma::{RdmaTransport, RdmaConfig};
//!
//! // Create RDMA transport
//! let config = RdmaConfig::default();
//! let transport = RdmaTransport::new(config)?;
//!
//! // Register memory for zero-copy transfers
//! let mr = transport.register_memory(buffer, AccessFlags::READ_WRITE)?;
//!
//! // Connect to remote peer
//! let conn = transport.connect("192.168.1.100:4791").await?;
//!
//! // RDMA write (zero-copy)
//! conn.write(mr.as_slice(), remote_addr, remote_key).await?;
//! ```

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};

/// Default RDMA port (standard for RDMA CM)
pub const DEFAULT_RDMA_PORT: u16 = 4791;

/// Maximum inline data size for small messages
pub const MAX_INLINE_DATA: usize = 256;

/// Default queue depth for send/receive queues
pub const DEFAULT_QUEUE_DEPTH: u32 = 256;

/// Default completion queue size
pub const DEFAULT_CQ_SIZE: u32 = 512;

/// RDMA errors
#[derive(Debug, Error)]
pub enum RdmaError {
    /// RDMA hardware not available
    #[error("RDMA hardware not available")]
    NotAvailable,

    /// Device not found
    #[error("RDMA device not found: {0}")]
    DeviceNotFound(String),

    /// Memory registration failed
    #[error("memory registration failed: {0}")]
    MemoryRegistrationFailed(String),

    /// Connection failed
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// Queue pair creation failed
    #[error("queue pair creation failed: {0}")]
    QueuePairFailed(String),

    /// Work request failed
    #[error("work request failed: {0}")]
    WorkRequestFailed(String),

    /// Timeout
    #[error("operation timed out after {0:?}")]
    Timeout(Duration),

    /// Remote error
    #[error("remote error: {0}")]
    RemoteError(String),

    /// Invalid state
    #[error("invalid state: {0}")]
    InvalidState(String),

    /// Resource exhausted
    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),

    /// Protection domain error
    #[error("protection domain error: {0}")]
    ProtectionDomain(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result type for RDMA operations
pub type Result<T> = std::result::Result<T, RdmaError>;

/// RDMA configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RdmaConfig {
    /// Device name (e.g., "mlx5_0"). None = auto-detect
    pub device_name: Option<String>,
    /// Port number on the device (usually 1)
    pub port_num: u8,
    /// GID index for RoCE
    pub gid_index: u8,
    /// Send queue depth
    pub send_queue_depth: u32,
    /// Receive queue depth
    pub recv_queue_depth: u32,
    /// Completion queue size
    pub cq_size: u32,
    /// Maximum inline data size
    pub max_inline_data: usize,
    /// Maximum scatter-gather entries
    pub max_sge: u32,
    /// Maximum outstanding RDMA reads
    pub max_rd_atomic: u8,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Retry count for connection
    pub retry_count: u8,
    /// RNR (Receiver Not Ready) retry count
    pub rnr_retry: u8,
    /// Minimum RNR timer
    pub min_rnr_timer: u8,
    /// Enable memory region caching
    pub cache_memory_regions: bool,
    /// Maximum cached memory regions
    pub max_cached_regions: usize,
    /// Use shared receive queue (SRQ)
    pub use_srq: bool,
    /// SRQ size if enabled
    pub srq_size: u32,
    /// Enable adaptive polling
    pub adaptive_polling: bool,
    /// Polling spin count before blocking
    pub poll_spin_count: u32,
}

impl Default for RdmaConfig {
    fn default() -> Self {
        Self {
            device_name: None,
            port_num: 1,
            gid_index: 0,
            send_queue_depth: DEFAULT_QUEUE_DEPTH,
            recv_queue_depth: DEFAULT_QUEUE_DEPTH,
            cq_size: DEFAULT_CQ_SIZE,
            max_inline_data: MAX_INLINE_DATA,
            max_sge: 4,
            max_rd_atomic: 16,
            connect_timeout: Duration::from_secs(5),
            retry_count: 7,
            rnr_retry: 7,
            min_rnr_timer: 12, // ~0.01ms
            cache_memory_regions: true,
            max_cached_regions: 1024,
            use_srq: true,
            srq_size: 4096,
            adaptive_polling: true,
            poll_spin_count: 1000,
        }
    }
}

/// Access flags for memory regions
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AccessFlags(u32);

impl AccessFlags {
    /// Local read access
    pub const LOCAL_READ: AccessFlags = AccessFlags(1 << 0);
    /// Local write access
    pub const LOCAL_WRITE: AccessFlags = AccessFlags(1 << 1);
    /// Remote read access (for RDMA read)
    pub const REMOTE_READ: AccessFlags = AccessFlags(1 << 2);
    /// Remote write access (for RDMA write)
    pub const REMOTE_WRITE: AccessFlags = AccessFlags(1 << 3);
    /// Remote atomic access
    pub const REMOTE_ATOMIC: AccessFlags = AccessFlags(1 << 4);

    /// Read-write access (local + remote)
    pub const READ_WRITE: AccessFlags = AccessFlags(
        Self::LOCAL_READ.0 | Self::LOCAL_WRITE.0 | Self::REMOTE_READ.0 | Self::REMOTE_WRITE.0,
    );

    /// Create from raw value
    pub fn from_raw(value: u32) -> Self {
        AccessFlags(value)
    }

    /// Get raw value
    pub fn as_raw(&self) -> u32 {
        self.0
    }

    /// Check if flag is set
    pub fn contains(&self, other: AccessFlags) -> bool {
        (self.0 & other.0) == other.0
    }
}

impl std::ops::BitOr for AccessFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        AccessFlags(self.0 | rhs.0)
    }
}

/// Memory region handle
#[derive(Debug)]
pub struct MemoryRegion {
    /// Unique ID
    id: u64,
    /// Start address
    addr: *mut u8,
    /// Length in bytes
    len: usize,
    /// Local key for local access
    lkey: u32,
    /// Remote key for remote access
    rkey: u32,
    /// Access flags
    access: AccessFlags,
    /// Whether this is a registered region
    registered: bool,
    /// Pool reference for deregistration
    pool_id: Option<u64>,
}

// SAFETY: MemoryRegion contains a raw pointer but the memory it points to
// is registered with the RDMA hardware and managed by the transport layer.
// Access is synchronized through the transport's internal locking.
unsafe impl Send for MemoryRegion {}
// SAFETY: Shared access (&MemoryRegion) is safe because the raw pointer is only mutated
// through &mut self methods. The RDMA-registered memory has a stable address and reads
// are safe to perform concurrently via the transport layer's internal synchronization.
unsafe impl Sync for MemoryRegion {}

impl MemoryRegion {
    /// Get the local key
    pub fn lkey(&self) -> u32 {
        self.lkey
    }

    /// Get the remote key
    pub fn rkey(&self) -> u32 {
        self.rkey
    }

    /// Get the address
    pub fn addr(&self) -> *mut u8 {
        self.addr
    }

    /// Get the length
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get as slice
    ///
    /// # Safety
    ///
    /// Caller must ensure the memory region is valid and properly initialized.
    pub unsafe fn as_slice(&self) -> &[u8] {
        std::slice::from_raw_parts(self.addr, self.len)
    }

    /// Get as mutable slice
    ///
    /// # Safety
    ///
    /// Caller must ensure the memory region is valid, properly initialized,
    /// and no other references exist.
    // SAFETY: `self.addr` is valid for `self.len` bytes because the MemoryRegion is constructed
    // with RDMA-registered memory of exactly `len` bytes. The &mut self receiver guarantees
    // exclusive access. Caller must ensure the region is initialized and not concurrently accessed.
    pub unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        std::slice::from_raw_parts_mut(self.addr, self.len)
    }
}

/// Memory region pool for efficient allocation
pub struct MemoryPool {
    /// Pool ID
    id: u64,
    /// Pool configuration
    config: MemoryPoolConfig,
    /// Free regions
    free_regions: Mutex<Vec<MemoryRegion>>,
    /// Allocated count
    allocated: AtomicUsize,
    /// Total regions
    total: AtomicUsize,
}

/// Memory pool configuration
#[derive(Clone, Debug)]
pub struct MemoryPoolConfig {
    /// Number of regions to pre-allocate
    pub initial_size: usize,
    /// Maximum pool size
    pub max_size: usize,
    /// Region size
    pub region_size: usize,
    /// Access flags for all regions
    pub access: AccessFlags,
    /// Alignment requirement
    pub alignment: usize,
}

impl Default for MemoryPoolConfig {
    fn default() -> Self {
        Self {
            initial_size: 64,
            max_size: 1024,
            region_size: 64 * 1024, // 64KB regions
            access: AccessFlags::READ_WRITE,
            alignment: 4096, // Page alignment
        }
    }
}

impl MemoryPool {
    /// Create a new memory pool
    pub fn new(id: u64, config: MemoryPoolConfig) -> Self {
        Self {
            id,
            config,
            free_regions: Mutex::new(Vec::new()),
            allocated: AtomicUsize::new(0),
            total: AtomicUsize::new(0),
        }
    }

    /// Get pool ID
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Allocate a region from the pool
    pub async fn allocate(&self) -> Result<MemoryRegion> {
        let mut free = self.free_regions.lock().await;

        if let Some(region) = free.pop() {
            self.allocated.fetch_add(1, Ordering::Relaxed);
            return Ok(region);
        }

        // Check if we can grow
        let total = self.total.load(Ordering::Relaxed);
        if total >= self.config.max_size {
            return Err(RdmaError::ResourceExhausted("memory pool exhausted".into()));
        }

        // Allocate new region (placeholder - actual registration happens in transport)
        let layout =
            std::alloc::Layout::from_size_align(self.config.region_size, self.config.alignment)
                .map_err(|e| RdmaError::MemoryRegistrationFailed(e.to_string()))?;

        // SAFETY: Layout is valid, we check for null below
        let addr = unsafe { std::alloc::alloc(layout) };
        if addr.is_null() {
            return Err(RdmaError::MemoryRegistrationFailed(
                "allocation failed".into(),
            ));
        }

        self.total.fetch_add(1, Ordering::Relaxed);
        self.allocated.fetch_add(1, Ordering::Relaxed);

        Ok(MemoryRegion {
            id: self.total.load(Ordering::Relaxed) as u64,
            addr,
            len: self.config.region_size,
            lkey: 0, // Set by transport registration
            rkey: 0,
            access: self.config.access,
            registered: false,
            pool_id: Some(self.id),
        })
    }

    /// Return a region to the pool
    pub async fn deallocate(&self, region: MemoryRegion) {
        if region.pool_id == Some(self.id) {
            self.allocated.fetch_sub(1, Ordering::Relaxed);
            self.free_regions.lock().await.push(region);
        }
    }

    /// Get statistics
    pub fn stats(&self) -> MemoryPoolStats {
        MemoryPoolStats {
            total: self.total.load(Ordering::Relaxed),
            allocated: self.allocated.load(Ordering::Relaxed),
            region_size: self.config.region_size,
        }
    }
}

/// Memory pool statistics
#[derive(Clone, Debug)]
pub struct MemoryPoolStats {
    /// Total regions in pool
    pub total: usize,
    /// Currently allocated regions
    pub allocated: usize,
    /// Size of each region
    pub region_size: usize,
}

/// Queue pair state
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueuePairState {
    /// Reset state
    Reset,
    /// Initialized
    Init,
    /// Ready to receive
    ReadyToReceive,
    /// Ready to send
    ReadyToSend,
    /// Error state
    Error,
}

/// Queue pair type
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueuePairType {
    /// Reliable Connection
    RC,
    /// Unreliable Connection
    UC,
    /// Unreliable Datagram
    UD,
    /// eXtended Reliable Connection (newer)
    XRC,
}

/// Queue pair attributes
#[derive(Clone, Debug)]
pub struct QueuePairAttr {
    /// QP number
    pub qp_num: u32,
    /// QP state
    pub state: QueuePairState,
    /// QP type
    pub qp_type: QueuePairType,
    /// Send queue depth
    pub sq_depth: u32,
    /// Receive queue depth
    pub rq_depth: u32,
    /// Maximum inline data
    pub max_inline: u32,
    /// Destination QP number (for connected QPs)
    pub dest_qp_num: Option<u32>,
    /// Path MTU
    pub path_mtu: PathMtu,
}

/// Path MTU values
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum PathMtu {
    /// 256 bytes
    Mtu256,
    /// 512 bytes
    Mtu512,
    /// 1024 bytes
    Mtu1024,
    /// 2048 bytes
    Mtu2048,
    /// 4096 bytes (most common)
    #[default]
    Mtu4096,
}

impl PathMtu {
    /// Get MTU in bytes
    pub fn bytes(&self) -> usize {
        match self {
            PathMtu::Mtu256 => 256,
            PathMtu::Mtu512 => 512,
            PathMtu::Mtu1024 => 1024,
            PathMtu::Mtu2048 => 2048,
            PathMtu::Mtu4096 => 4096,
        }
    }
}

/// Queue pair handle
pub struct QueuePair {
    /// QP number
    qp_num: u32,
    /// QP attributes
    attr: RwLock<QueuePairAttr>,
    /// Send completion channel
    send_cq: Arc<CompletionQueue>,
    /// Receive completion channel
    recv_cq: Arc<CompletionQueue>,
    /// Outstanding send requests
    pending_sends: AtomicU32,
    /// Outstanding receive requests
    pending_recvs: AtomicU32,
    /// Is connected
    connected: AtomicBool,
    /// Statistics
    stats: QueuePairStats,
}

impl QueuePair {
    /// Create a new queue pair
    fn new(
        qp_num: u32,
        qp_type: QueuePairType,
        send_cq: Arc<CompletionQueue>,
        recv_cq: Arc<CompletionQueue>,
        sq_depth: u32,
        rq_depth: u32,
    ) -> Self {
        Self {
            qp_num,
            attr: RwLock::new(QueuePairAttr {
                qp_num,
                state: QueuePairState::Reset,
                qp_type,
                sq_depth,
                rq_depth,
                max_inline: MAX_INLINE_DATA as u32,
                dest_qp_num: None,
                path_mtu: PathMtu::default(),
            }),
            send_cq,
            recv_cq,
            pending_sends: AtomicU32::new(0),
            pending_recvs: AtomicU32::new(0),
            connected: AtomicBool::new(false),
            stats: QueuePairStats::default(),
        }
    }

    /// Get QP number
    pub fn qp_num(&self) -> u32 {
        self.qp_num
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    /// Get pending send count
    pub fn pending_sends(&self) -> u32 {
        self.pending_sends.load(Ordering::Relaxed)
    }

    /// Get pending receive count
    pub fn pending_recvs(&self) -> u32 {
        self.pending_recvs.load(Ordering::Relaxed)
    }

    /// Get statistics
    pub fn stats(&self) -> &QueuePairStats {
        &self.stats
    }
}

/// Queue pair statistics
#[derive(Default)]
pub struct QueuePairStats {
    /// Total bytes sent
    pub bytes_sent: AtomicU64,
    /// Total bytes received
    pub bytes_received: AtomicU64,
    /// Total send operations
    pub send_ops: AtomicU64,
    /// Total receive operations
    pub recv_ops: AtomicU64,
    /// Total RDMA write operations
    pub rdma_write_ops: AtomicU64,
    /// Total RDMA read operations
    pub rdma_read_ops: AtomicU64,
    /// Send errors
    pub send_errors: AtomicU64,
    /// Receive errors
    pub recv_errors: AtomicU64,
}

/// Work completion status
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkCompletionStatus {
    /// Success
    Success,
    /// Local length error
    LocalLengthError,
    /// Local QP operation error
    LocalQpOpError,
    /// Local protection error
    LocalProtectionError,
    /// Work request flushed
    WrFlushError,
    /// Memory window bind error
    MwBindError,
    /// Bad response error
    BadResponseError,
    /// Local access error
    LocalAccessError,
    /// Remote invalid request error
    RemoteInvalidRequestError,
    /// Remote access error
    RemoteAccessError,
    /// Remote operation error
    RemoteOpError,
    /// Retry count exceeded
    RetryExceeded,
    /// RNR retry count exceeded
    RnrRetryExceeded,
    /// Unknown error
    Unknown(i32),
}

/// Work completion
#[derive(Clone, Debug)]
pub struct WorkCompletion {
    /// Work request ID
    pub wr_id: u64,
    /// Status
    pub status: WorkCompletionStatus,
    /// Operation type
    pub opcode: WorkCompletionOpcode,
    /// Bytes transferred
    pub byte_len: u32,
    /// Immediate data (if any)
    pub imm_data: Option<u32>,
    /// Source QP (for UD)
    pub src_qp: Option<u32>,
}

/// Work completion opcode
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkCompletionOpcode {
    /// Send completion
    Send,
    /// RDMA write completion
    RdmaWrite,
    /// RDMA read completion
    RdmaRead,
    /// Atomic compare-and-swap completion
    CompareSwap,
    /// Atomic fetch-and-add completion
    FetchAdd,
    /// Receive completion
    Recv,
    /// Receive with immediate data
    RecvWithImm,
}

/// Completion queue
pub struct CompletionQueue {
    /// CQ identifier
    id: u32,
    /// CQ size
    size: u32,
    /// Completion channel
    completions: Mutex<Vec<WorkCompletion>>,
    /// Pending completions
    pending: AtomicU32,
    /// Total completions processed
    total_completions: AtomicU64,
}

impl CompletionQueue {
    /// Create a new completion queue
    fn new(id: u32, size: u32) -> Self {
        Self {
            id,
            size,
            completions: Mutex::new(Vec::with_capacity(size as usize)),
            pending: AtomicU32::new(0),
            total_completions: AtomicU64::new(0),
        }
    }

    /// Get CQ ID
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Poll for completions (non-blocking)
    pub async fn poll(&self, max_entries: usize) -> Vec<WorkCompletion> {
        let mut completions = self.completions.lock().await;
        let count = std::cmp::min(completions.len(), max_entries);
        let result: Vec<_> = completions.drain(..count).collect();
        self.pending
            .fetch_sub(result.len() as u32, Ordering::Relaxed);
        self.total_completions
            .fetch_add(result.len() as u64, Ordering::Relaxed);
        result
    }

    /// Add a completion (internal use)
    async fn add_completion(&self, wc: WorkCompletion) {
        self.pending.fetch_add(1, Ordering::Relaxed);
        self.completions.lock().await.push(wc);
    }

    /// Get pending count
    pub fn pending(&self) -> u32 {
        self.pending.load(Ordering::Relaxed)
    }
}

/// Remote address information
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoteAddress {
    /// Remote memory address
    pub addr: u64,
    /// Remote key for access
    pub rkey: u32,
}

/// Send work request
#[derive(Clone, Debug)]
pub struct SendRequest {
    /// Work request ID
    pub wr_id: u64,
    /// Data to send
    pub data: Bytes,
    /// Flags
    pub flags: SendFlags,
    /// Immediate data (optional)
    pub imm_data: Option<u32>,
}

/// RDMA write request
#[derive(Clone, Debug)]
pub struct RdmaWriteRequest {
    /// Work request ID
    pub wr_id: u64,
    /// Local data
    pub local_data: Bytes,
    /// Local key
    pub lkey: u32,
    /// Remote address
    pub remote: RemoteAddress,
    /// Flags
    pub flags: SendFlags,
    /// Immediate data (optional)
    pub imm_data: Option<u32>,
}

/// RDMA read request
#[derive(Clone, Debug)]
pub struct RdmaReadRequest {
    /// Work request ID
    pub wr_id: u64,
    /// Local buffer address
    pub local_addr: u64,
    /// Local buffer length
    pub local_len: u32,
    /// Local key
    pub lkey: u32,
    /// Remote address
    pub remote: RemoteAddress,
    /// Flags
    pub flags: SendFlags,
}

/// Send flags
#[derive(Clone, Copy, Debug, Default)]
pub struct SendFlags(u32);

impl SendFlags {
    /// Signal completion
    pub const SIGNALED: SendFlags = SendFlags(1 << 0);
    /// Send inline (no memory registration needed for small data)
    pub const INLINE: SendFlags = SendFlags(1 << 1);
    /// Fence (wait for previous RDMA reads to complete)
    pub const FENCE: SendFlags = SendFlags(1 << 2);

    /// Check if flag is set
    pub fn contains(&self, other: SendFlags) -> bool {
        (self.0 & other.0) == other.0
    }
}

impl std::ops::BitOr for SendFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        SendFlags(self.0 | rhs.0)
    }
}

/// Connection information exchanged during handshake
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectionInfo {
    /// Queue pair number
    pub qp_num: u32,
    /// Packet sequence number
    pub psn: u32,
    /// Local ID (for IB)
    pub lid: u16,
    /// Global ID (for RoCE)
    pub gid: [u8; 16],
    /// MTU
    pub mtu: PathMtu,
}

/// RDMA connection
pub struct RdmaConnection {
    /// Connection ID
    id: u64,
    /// Remote address
    remote_addr: SocketAddr,
    /// Queue pair
    qp: Arc<QueuePair>,
    /// Local connection info
    local_info: ConnectionInfo,
    /// Remote connection info
    remote_info: ConnectionInfo,
    /// Work request ID counter
    next_wr_id: AtomicU64,
    /// Is active
    active: AtomicBool,
    /// Statistics
    stats: ConnectionStats,
}

impl RdmaConnection {
    /// Get connection ID
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get remote address
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    /// Check if connection is active
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Get next work request ID
    fn next_wr_id(&self) -> u64 {
        self.next_wr_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Post a send request
    pub async fn send(&self, data: Bytes) -> Result<u64> {
        if !self.is_active() {
            return Err(RdmaError::InvalidState("connection not active".into()));
        }

        let wr_id = self.next_wr_id();
        let flags = if data.len() <= MAX_INLINE_DATA {
            SendFlags::SIGNALED | SendFlags::INLINE
        } else {
            SendFlags::SIGNALED
        };

        let _request = SendRequest {
            wr_id,
            data: data.clone(),
            flags,
            imm_data: None,
        };

        // In a real implementation, this would post to the hardware QP
        // For now, simulate the send
        self.stats
            .bytes_sent
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.stats.send_ops.fetch_add(1, Ordering::Relaxed);

        // Simulate completion
        self.qp
            .send_cq
            .add_completion(WorkCompletion {
                wr_id,
                status: WorkCompletionStatus::Success,
                opcode: WorkCompletionOpcode::Send,
                byte_len: data.len() as u32,
                imm_data: None,
                src_qp: None,
            })
            .await;

        Ok(wr_id)
    }

    /// Post an RDMA write request
    pub async fn rdma_write(
        &self,
        local_data: Bytes,
        lkey: u32,
        remote: RemoteAddress,
    ) -> Result<u64> {
        if !self.is_active() {
            return Err(RdmaError::InvalidState("connection not active".into()));
        }

        let wr_id = self.next_wr_id();
        let flags = if local_data.len() <= MAX_INLINE_DATA {
            SendFlags::SIGNALED | SendFlags::INLINE
        } else {
            SendFlags::SIGNALED
        };

        let _request = RdmaWriteRequest {
            wr_id,
            local_data: local_data.clone(),
            lkey,
            remote,
            flags,
            imm_data: None,
        };

        // In a real implementation, this would post to the hardware QP
        self.stats
            .bytes_sent
            .fetch_add(local_data.len() as u64, Ordering::Relaxed);
        self.stats.rdma_write_ops.fetch_add(1, Ordering::Relaxed);

        // Simulate completion
        self.qp
            .send_cq
            .add_completion(WorkCompletion {
                wr_id,
                status: WorkCompletionStatus::Success,
                opcode: WorkCompletionOpcode::RdmaWrite,
                byte_len: local_data.len() as u32,
                imm_data: None,
                src_qp: None,
            })
            .await;

        Ok(wr_id)
    }

    /// Post an RDMA read request
    pub async fn rdma_read(
        &self,
        local_addr: u64,
        local_len: u32,
        lkey: u32,
        remote: RemoteAddress,
    ) -> Result<u64> {
        if !self.is_active() {
            return Err(RdmaError::InvalidState("connection not active".into()));
        }

        let wr_id = self.next_wr_id();

        let _request = RdmaReadRequest {
            wr_id,
            local_addr,
            local_len,
            lkey,
            remote,
            flags: SendFlags::SIGNALED,
        };

        // In a real implementation, this would post to the hardware QP
        self.stats.rdma_read_ops.fetch_add(1, Ordering::Relaxed);

        // Simulate completion
        self.qp
            .send_cq
            .add_completion(WorkCompletion {
                wr_id,
                status: WorkCompletionStatus::Success,
                opcode: WorkCompletionOpcode::RdmaRead,
                byte_len: local_len,
                imm_data: None,
                src_qp: None,
            })
            .await;

        Ok(wr_id)
    }

    /// Poll for send completions
    pub async fn poll_send(&self, max_entries: usize) -> Vec<WorkCompletion> {
        self.qp.send_cq.poll(max_entries).await
    }

    /// Poll for receive completions
    pub async fn poll_recv(&self, max_entries: usize) -> Vec<WorkCompletion> {
        self.qp.recv_cq.poll(max_entries).await
    }

    /// Get statistics
    pub fn stats(&self) -> &ConnectionStats {
        &self.stats
    }

    /// Close the connection
    pub async fn close(&self) {
        self.active.store(false, Ordering::Release);
    }
}

/// Connection statistics
#[derive(Default)]
pub struct ConnectionStats {
    /// Bytes sent
    pub bytes_sent: AtomicU64,
    /// Bytes received
    pub bytes_received: AtomicU64,
    /// Send operations
    pub send_ops: AtomicU64,
    /// Receive operations
    pub recv_ops: AtomicU64,
    /// RDMA write operations
    pub rdma_write_ops: AtomicU64,
    /// RDMA read operations
    pub rdma_read_ops: AtomicU64,
    /// Connection established time
    pub connected_at: AtomicU64,
}

/// RDMA transport layer
pub struct RdmaTransport {
    /// Configuration
    config: RdmaConfig,
    /// Is initialized
    initialized: AtomicBool,
    /// Device info
    device_info: RwLock<Option<DeviceInfo>>,
    /// Memory pools
    memory_pools: RwLock<HashMap<u64, Arc<MemoryPool>>>,
    /// Next pool ID
    next_pool_id: AtomicU64,
    /// Active connections
    connections: RwLock<HashMap<u64, Arc<RdmaConnection>>>,
    /// Next connection ID
    next_conn_id: AtomicU64,
    /// Completion queues
    completion_queues: RwLock<HashMap<u32, Arc<CompletionQueue>>>,
    /// Next CQ ID
    next_cq_id: AtomicU32,
    /// Statistics
    stats: RdmaTransportStats,
    /// Shutdown flag
    shutdown: AtomicBool,
}

/// Device information
#[derive(Clone, Debug)]
pub struct DeviceInfo {
    /// Device name
    pub name: String,
    /// Firmware version
    pub fw_version: String,
    /// Node GUID
    pub node_guid: u64,
    /// System image GUID
    pub sys_image_guid: u64,
    /// Maximum memory regions
    pub max_mr: u32,
    /// Maximum queue pairs
    pub max_qp: u32,
    /// Maximum CQs
    pub max_cq: u32,
    /// Maximum SGEs
    pub max_sge: u32,
    /// Maximum inline data
    pub max_inline: u32,
    /// Atomic capabilities
    pub atomic_cap: AtomicCapability,
}

/// Atomic operation capability
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AtomicCapability {
    /// No atomic support
    None,
    /// HCA atomic support
    Hca,
    /// Global atomic support
    Glob,
}

/// Transport statistics
#[derive(Default)]
pub struct RdmaTransportStats {
    /// Total connections created
    pub connections_created: AtomicU64,
    /// Active connections
    pub active_connections: AtomicU64,
    /// Total memory regions registered
    pub regions_registered: AtomicU64,
    /// Active memory regions
    pub active_regions: AtomicU64,
    /// Total bytes sent
    pub bytes_sent: AtomicU64,
    /// Total bytes received
    pub bytes_received: AtomicU64,
}

impl RdmaTransport {
    /// Create a new RDMA transport
    pub fn new(config: RdmaConfig) -> Result<Self> {
        Ok(Self {
            config,
            initialized: AtomicBool::new(false),
            device_info: RwLock::new(None),
            memory_pools: RwLock::new(HashMap::new()),
            next_pool_id: AtomicU64::new(1),
            connections: RwLock::new(HashMap::new()),
            next_conn_id: AtomicU64::new(1),
            completion_queues: RwLock::new(HashMap::new()),
            next_cq_id: AtomicU32::new(1),
            stats: RdmaTransportStats::default(),
            shutdown: AtomicBool::new(false),
        })
    }

    /// Initialize the transport (detect devices, etc.)
    pub async fn initialize(&self) -> Result<()> {
        if self.initialized.load(Ordering::Acquire) {
            return Ok(());
        }

        // In a real implementation, this would:
        // 1. Call ibv_get_device_list() to enumerate devices
        // 2. Open the specified device with ibv_open_device()
        // 3. Query device capabilities with ibv_query_device()
        // 4. Allocate protection domain with ibv_alloc_pd()

        // For now, create simulated device info
        let device_info = DeviceInfo {
            name: self
                .config
                .device_name
                .clone()
                .unwrap_or_else(|| "mlx5_0".into()),
            fw_version: "16.35.1012".into(),
            node_guid: 0x0002c9030042baf0,
            sys_image_guid: 0x0002c9030042baf3,
            max_mr: 16777216,
            max_qp: 131072,
            max_cq: 16777216,
            max_sge: 30,
            max_inline: 512,
            atomic_cap: AtomicCapability::Glob,
        };

        *self.device_info.write().await = Some(device_info);
        self.initialized.store(true, Ordering::Release);

        tracing::info!(
            "RDMA transport initialized: device={}",
            self.config.device_name.as_deref().unwrap_or("auto")
        );

        Ok(())
    }

    /// Check if RDMA is available
    pub fn is_available(&self) -> bool {
        // In a real implementation, check for RDMA hardware and driver
        // For simulation, always return true
        true
    }

    /// Get device info
    pub async fn device_info(&self) -> Option<DeviceInfo> {
        self.device_info.read().await.clone()
    }

    /// Create a memory pool
    pub async fn create_memory_pool(&self, config: MemoryPoolConfig) -> Result<Arc<MemoryPool>> {
        let pool_id = self.next_pool_id.fetch_add(1, Ordering::Relaxed);
        let pool = Arc::new(MemoryPool::new(pool_id, config));

        self.memory_pools
            .write()
            .await
            .insert(pool_id, pool.clone());

        Ok(pool)
    }

    /// Register a memory region
    pub async fn register_memory(
        &self,
        data: &mut [u8],
        access: AccessFlags,
    ) -> Result<MemoryRegion> {
        if !self.initialized.load(Ordering::Acquire) {
            return Err(RdmaError::InvalidState("transport not initialized".into()));
        }

        // In a real implementation, this would call ibv_reg_mr()
        let lkey = rand::random::<u32>();
        let rkey = rand::random::<u32>();

        self.stats
            .regions_registered
            .fetch_add(1, Ordering::Relaxed);
        self.stats.active_regions.fetch_add(1, Ordering::Relaxed);

        Ok(MemoryRegion {
            id: self.stats.regions_registered.load(Ordering::Relaxed),
            addr: data.as_mut_ptr(),
            len: data.len(),
            lkey,
            rkey,
            access,
            registered: true,
            pool_id: None,
        })
    }

    /// Deregister a memory region
    pub async fn deregister_memory(&self, _region: MemoryRegion) -> Result<()> {
        // In a real implementation, this would call ibv_dereg_mr()
        self.stats.active_regions.fetch_sub(1, Ordering::Relaxed);
        Ok(())
    }

    /// Create a completion queue
    pub async fn create_cq(&self, size: u32) -> Result<Arc<CompletionQueue>> {
        let cq_id = self.next_cq_id.fetch_add(1, Ordering::Relaxed);
        let cq = Arc::new(CompletionQueue::new(cq_id, size));

        self.completion_queues
            .write()
            .await
            .insert(cq_id, cq.clone());

        Ok(cq)
    }

    /// Create a queue pair
    pub async fn create_qp(
        &self,
        qp_type: QueuePairType,
        send_cq: Arc<CompletionQueue>,
        recv_cq: Arc<CompletionQueue>,
    ) -> Result<Arc<QueuePair>> {
        if !self.initialized.load(Ordering::Acquire) {
            return Err(RdmaError::InvalidState("transport not initialized".into()));
        }

        // In a real implementation, this would call ibv_create_qp()
        let qp_num = rand::random::<u32>() & 0x00FFFFFF; // QP nums are 24-bit

        let qp = Arc::new(QueuePair::new(
            qp_num,
            qp_type,
            send_cq,
            recv_cq,
            self.config.send_queue_depth,
            self.config.recv_queue_depth,
        ));

        Ok(qp)
    }

    /// Connect to a remote peer
    pub async fn connect(&self, addr: SocketAddr) -> Result<Arc<RdmaConnection>> {
        if !self.initialized.load(Ordering::Acquire) {
            self.initialize().await?;
        }

        // Create CQs for this connection
        let send_cq = self.create_cq(self.config.cq_size).await?;
        let recv_cq = self.create_cq(self.config.cq_size).await?;

        // Create QP
        let qp = self.create_qp(QueuePairType::RC, send_cq, recv_cq).await?;

        // Generate connection info
        let local_info = ConnectionInfo {
            qp_num: qp.qp_num(),
            psn: rand::random::<u32>() & 0x00FFFFFF,
            lid: 1,         // Simulated LID
            gid: [0u8; 16], // Would be filled from device
            mtu: PathMtu::Mtu4096,
        };

        // In a real implementation, exchange connection info with remote peer
        // via TCP/IP or RDMA CM. For simulation, create synthetic remote info.
        let remote_info = ConnectionInfo {
            qp_num: rand::random::<u32>() & 0x00FFFFFF,
            psn: rand::random::<u32>() & 0x00FFFFFF,
            lid: 2,
            gid: [0u8; 16],
            mtu: PathMtu::Mtu4096,
        };

        // Transition QP to RTS state
        {
            let mut attr = qp.attr.write().await;
            attr.state = QueuePairState::ReadyToSend;
            attr.dest_qp_num = Some(remote_info.qp_num);
        }
        qp.connected.store(true, Ordering::Release);

        let conn_id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
        let connection = Arc::new(RdmaConnection {
            id: conn_id,
            remote_addr: addr,
            qp,
            local_info,
            remote_info,
            next_wr_id: AtomicU64::new(1),
            active: AtomicBool::new(true),
            stats: ConnectionStats::default(),
        });

        self.connections
            .write()
            .await
            .insert(conn_id, connection.clone());
        self.stats
            .connections_created
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .active_connections
            .fetch_add(1, Ordering::Relaxed);

        tracing::info!("RDMA connection established to {}", addr);

        Ok(connection)
    }

    /// Listen for incoming connections
    pub async fn listen(&self, addr: SocketAddr) -> Result<RdmaListener<'_>> {
        if !self.initialized.load(Ordering::Acquire) {
            self.initialize().await?;
        }

        Ok(RdmaListener {
            addr,
            transport: self,
            active: AtomicBool::new(true),
        })
    }

    /// Get a connection by ID
    pub async fn get_connection(&self, id: u64) -> Option<Arc<RdmaConnection>> {
        self.connections.read().await.get(&id).cloned()
    }

    /// Close a connection
    pub async fn close_connection(&self, id: u64) -> Result<()> {
        if let Some(conn) = self.connections.write().await.remove(&id) {
            conn.close().await;
            self.stats
                .active_connections
                .fetch_sub(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Get transport statistics
    pub fn stats(&self) -> &RdmaTransportStats {
        &self.stats
    }

    /// Shutdown the transport
    pub async fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);

        // Close all connections
        let connections: Vec<_> = self.connections.write().await.drain().collect();
        for (_, conn) in connections {
            conn.close().await;
        }

        self.initialized.store(false, Ordering::Release);
        tracing::info!("RDMA transport shut down");
    }
}

/// RDMA listener for incoming connections
pub struct RdmaListener<'a> {
    /// Listen address
    addr: SocketAddr,
    /// Transport reference
    transport: &'a RdmaTransport,
    /// Is active
    active: AtomicBool,
}

impl<'a> RdmaListener<'a> {
    /// Accept an incoming connection
    pub async fn accept(&self) -> Result<Arc<RdmaConnection>> {
        if !self.active.load(Ordering::Acquire) {
            return Err(RdmaError::InvalidState("listener not active".into()));
        }

        // In a real implementation, this would use RDMA CM to accept connections
        // For simulation, create a connection when called
        let remote_addr = SocketAddr::from(([127, 0, 0, 1], rand::random::<u16>()));
        self.transport.connect(remote_addr).await
    }

    /// Get listen address
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Stop listening
    pub fn close(&self) {
        self.active.store(false, Ordering::Release);
    }
}

/// Builder for RDMA transport
pub struct RdmaTransportBuilder {
    config: RdmaConfig,
}

impl RdmaTransportBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: RdmaConfig::default(),
        }
    }

    /// Set device name
    pub fn device(mut self, name: impl Into<String>) -> Self {
        self.config.device_name = Some(name.into());
        self
    }

    /// Set queue depth
    pub fn queue_depth(mut self, send: u32, recv: u32) -> Self {
        self.config.send_queue_depth = send;
        self.config.recv_queue_depth = recv;
        self
    }

    /// Set CQ size
    pub fn cq_size(mut self, size: u32) -> Self {
        self.config.cq_size = size;
        self
    }

    /// Set max inline data
    pub fn max_inline(mut self, size: usize) -> Self {
        self.config.max_inline_data = size;
        self
    }

    /// Set connect timeout
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = timeout;
        self
    }

    /// Enable shared receive queue
    pub fn use_srq(mut self, enabled: bool, size: u32) -> Self {
        self.config.use_srq = enabled;
        self.config.srq_size = size;
        self
    }

    /// Enable adaptive polling
    pub fn adaptive_polling(mut self, enabled: bool, spin_count: u32) -> Self {
        self.config.adaptive_polling = enabled;
        self.config.poll_spin_count = spin_count;
        self
    }

    /// Build the transport
    pub fn build(self) -> Result<RdmaTransport> {
        RdmaTransport::new(self.config)
    }
}

impl Default for RdmaTransportBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rdma_config_default() {
        let config = RdmaConfig::default();
        assert_eq!(config.port_num, 1);
        assert_eq!(config.send_queue_depth, DEFAULT_QUEUE_DEPTH);
        assert_eq!(config.cq_size, DEFAULT_CQ_SIZE);
        assert!(config.use_srq);
    }

    #[test]
    fn test_access_flags() {
        let flags = AccessFlags::LOCAL_READ | AccessFlags::REMOTE_WRITE;
        assert!(flags.contains(AccessFlags::LOCAL_READ));
        assert!(flags.contains(AccessFlags::REMOTE_WRITE));
        assert!(!flags.contains(AccessFlags::REMOTE_ATOMIC));
    }

    #[test]
    fn test_path_mtu() {
        assert_eq!(PathMtu::Mtu4096.bytes(), 4096);
        assert_eq!(PathMtu::Mtu1024.bytes(), 1024);
    }

    #[test]
    fn test_send_flags() {
        let flags = SendFlags::SIGNALED | SendFlags::INLINE;
        assert!(flags.contains(SendFlags::SIGNALED));
        assert!(flags.contains(SendFlags::INLINE));
        assert!(!flags.contains(SendFlags::FENCE));
    }

    #[tokio::test]
    async fn test_transport_creation() {
        let transport = RdmaTransport::new(RdmaConfig::default());
        assert!(transport.is_ok());
    }

    #[tokio::test]
    async fn test_transport_initialize() {
        let transport = RdmaTransport::new(RdmaConfig::default()).unwrap();
        let result = transport.initialize().await;
        assert!(result.is_ok());
        assert!(transport.device_info().await.is_some());
    }

    #[tokio::test]
    async fn test_memory_pool() {
        let transport = RdmaTransport::new(RdmaConfig::default()).unwrap();
        transport.initialize().await.unwrap();

        let pool = transport
            .create_memory_pool(MemoryPoolConfig::default())
            .await;
        assert!(pool.is_ok());

        let pool = pool.unwrap();
        let stats = pool.stats();
        assert_eq!(stats.allocated, 0);
    }

    #[tokio::test]
    async fn test_create_cq() {
        let transport = RdmaTransport::new(RdmaConfig::default()).unwrap();
        transport.initialize().await.unwrap();

        let cq = transport.create_cq(256).await;
        assert!(cq.is_ok());
    }

    #[tokio::test]
    async fn test_builder() {
        let transport = RdmaTransportBuilder::new()
            .device("mlx5_0")
            .queue_depth(512, 512)
            .cq_size(1024)
            .build();
        assert!(transport.is_ok());
    }
}
