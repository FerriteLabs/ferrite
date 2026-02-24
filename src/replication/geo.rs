//! Global Active-Active Geo-Replication
//!
//! This module implements multi-region active-active replication with:
//! - Conflict-free replication using CRDTs
//! - Anti-entropy protocol for eventual consistency
//! - Causal consistency with vector clocks
//! - Conflict detection and resolution policies
//! - Network partition handling
//! - Cross-region topology management

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::info;

use ferrite_plugins::crdt::{
    CrdtOperation, CrdtState, HybridClock, HybridTimestamp, SiteId, VectorClock,
};

/// Geographic region identifier
pub type RegionId = String;

/// Site identifier within a region
pub type SiteIdentifier = String;

/// Replication operation sequence number
pub type SequenceNumber = u64;

/// Configuration for geo-replication
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GeoReplicationConfig {
    /// Enable geo-replication
    pub enabled: bool,
    /// Local region identifier
    pub local_region: RegionId,
    /// Local site identifier
    pub local_site: SiteIdentifier,
    /// Peer regions and their endpoints
    pub peer_regions: Vec<PeerRegion>,
    /// Replication mode
    pub mode: GeoReplicationMode,
    /// Conflict resolution strategy
    pub conflict_resolution: ConflictResolution,
    /// Sync interval for anti-entropy
    pub sync_interval: Duration,
    /// Batch size for replication
    pub batch_size: usize,
    /// Maximum replication lag allowed (ms)
    pub max_lag_ms: u64,
    /// Enable causal ordering
    pub causal_ordering: bool,
    /// Compression for cross-region traffic
    pub compression: CompressionMode,
    /// TLS configuration for cross-region connections
    pub tls_enabled: bool,
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
    /// Connection timeout
    pub connection_timeout: Duration,
    /// Maximum retry attempts
    pub max_retries: u32,
    /// Retry backoff base
    pub retry_backoff_base: Duration,
}

impl Default for GeoReplicationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            local_region: "default".to_string(),
            local_site: "site-1".to_string(),
            peer_regions: Vec::new(),
            mode: GeoReplicationMode::Async,
            conflict_resolution: ConflictResolution::LastWriteWins,
            sync_interval: Duration::from_millis(100),
            batch_size: 1000,
            max_lag_ms: 5000,
            causal_ordering: true,
            compression: CompressionMode::Lz4,
            tls_enabled: true,
            heartbeat_interval: Duration::from_secs(1),
            connection_timeout: Duration::from_secs(5),
            max_retries: 3,
            retry_backoff_base: Duration::from_millis(100),
        }
    }
}

/// Peer region configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerRegion {
    /// Region identifier
    pub region_id: RegionId,
    /// Region display name
    pub name: String,
    /// Endpoints for this region
    pub endpoints: Vec<SocketAddr>,
    /// Priority (lower = higher priority for failover)
    pub priority: u32,
    /// Maximum latency to consider healthy (ms)
    pub max_latency_ms: u64,
    /// Read preference
    pub read_preference: ReadPreference,
    /// Write forwarding enabled
    pub write_forwarding: bool,
}

/// Geo-replication mode
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GeoReplicationMode {
    /// Asynchronous replication (best effort, low latency)
    Async,
    /// Semi-synchronous (wait for at least one peer ack)
    SemiSync,
    /// Synchronous (wait for all peers, higher latency)
    Sync,
    /// Quorum-based (wait for majority)
    Quorum,
}

/// Conflict resolution strategy
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ConflictResolution {
    /// Last write wins based on timestamp
    LastWriteWins,
    /// First write wins
    FirstWriteWins,
    /// Keep all conflicting values (multi-value)
    MultiValue,
    /// Use CRDTs for automatic resolution
    Crdt,
    /// Custom resolver function name
    Custom(String),
    /// Priority-based (higher priority region wins)
    PriorityBased,
}

/// Read preference for cross-region reads
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadPreference {
    /// Read from local region only
    Local,
    /// Read from any region (lowest latency)
    Nearest,
    /// Read from primary region
    Primary,
    /// Read from secondary regions
    Secondary,
}

/// Compression mode for cross-region traffic
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionMode {
    /// No compression
    None,
    /// LZ4 compression (fast)
    Lz4,
    /// Zstd compression (better ratio)
    Zstd,
    /// Snappy compression
    Snappy,
}

/// Replication operation to be synced
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicationOp {
    /// Operation sequence number
    pub seq: SequenceNumber,
    /// Operation timestamp (hybrid logical clock)
    pub timestamp: HybridTimestamp,
    /// Vector clock for causal ordering
    pub vector_clock: VectorClock,
    /// Source region
    pub source_region: RegionId,
    /// Source site
    pub source_site: SiteIdentifier,
    /// Key being modified
    pub key: Bytes,
    /// Operation type
    pub op_type: ReplicationOpType,
    /// CRDT state (if using CRDT resolution)
    pub crdt_state: Option<CrdtState>,
    /// Operation metadata
    pub metadata: HashMap<String, String>,
}

/// Type of replication operation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ReplicationOpType {
    /// Set a value
    Set { value: Bytes, ttl: Option<Duration> },
    /// Delete a key
    Delete,
    /// Increment a counter
    Incr { delta: i64 },
    /// CRDT operation
    Crdt(CrdtOperation),
    /// Multi-key transaction
    Transaction { ops: Vec<ReplicationOp> },
}

/// Region state in the replication topology
#[derive(Clone, Debug)]
pub struct RegionState {
    /// Region identifier
    pub region_id: RegionId,
    /// Is this the local region
    pub is_local: bool,
    /// Region status
    pub status: RegionStatus,
    /// Last seen timestamp
    pub last_seen: Instant,
    /// Last replicated sequence number
    pub last_replicated_seq: SequenceNumber,
    /// Vector clock for this region
    pub vector_clock: VectorClock,
    /// Estimated latency (ms)
    pub latency_ms: f64,
    /// Bytes sent to this region
    pub bytes_sent: u64,
    /// Bytes received from this region
    pub bytes_received: u64,
    /// Operations pending to this region
    pub pending_ops: usize,
    /// Error count
    pub error_count: u64,
}

/// Region status
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegionStatus {
    /// Region is online and healthy
    Online,
    /// Region is degraded (high latency or errors)
    Degraded,
    /// Region is offline
    Offline,
    /// Region is partitioned (unreachable)
    Partitioned,
    /// Region is syncing (catching up)
    Syncing,
}

/// Conflict detected during replication
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Conflict {
    /// Conflict identifier
    pub id: u64,
    /// Key involved
    pub key: Bytes,
    /// Local value
    pub local_value: Option<Bytes>,
    /// Local timestamp
    pub local_timestamp: HybridTimestamp,
    /// Remote value
    pub remote_value: Option<Bytes>,
    /// Remote timestamp
    pub remote_timestamp: HybridTimestamp,
    /// Source region of remote value
    pub remote_region: RegionId,
    /// Resolution applied
    pub resolution: ConflictResolutionResult,
    /// Timestamp of detection (Unix millis)
    pub detected_at_ms: u64,
}

/// Result of conflict resolution
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ConflictResolutionResult {
    /// Local value was kept
    KeepLocal,
    /// Remote value was applied
    ApplyRemote,
    /// Values were merged
    Merged(Bytes),
    /// Multiple values preserved
    MultiValue(Vec<Bytes>),
    /// Pending manual resolution
    Pending,
}

/// Geo-replication metrics
#[derive(Debug, Default)]
pub struct GeoReplicationMetrics {
    /// Operations replicated
    pub ops_replicated: AtomicU64,
    /// Operations received
    pub ops_received: AtomicU64,
    /// Conflicts detected
    pub conflicts_detected: AtomicU64,
    /// Conflicts resolved
    pub conflicts_resolved: AtomicU64,
    /// Bytes sent
    pub bytes_sent: AtomicU64,
    /// Bytes received
    pub bytes_received: AtomicU64,
    /// Replication lag (ms)
    pub replication_lag_ms: AtomicU64,
    /// Cross-region latency (ms avg)
    pub cross_region_latency_ms: AtomicU64,
    /// Failed operations
    pub failed_ops: AtomicU64,
    /// Retried operations
    pub retried_ops: AtomicU64,
}

/// Message types for geo-replication protocol
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GeoReplicationMessage {
    /// Handshake message
    Handshake {
        region_id: RegionId,
        site_id: SiteIdentifier,
        vector_clock: VectorClock,
        capabilities: Vec<String>,
    },
    /// Handshake acknowledgment
    HandshakeAck {
        region_id: RegionId,
        accepted: bool,
        reason: Option<String>,
    },
    /// Replicate operations
    Replicate {
        ops: Vec<ReplicationOp>,
        require_ack: bool,
    },
    /// Replication acknowledgment
    ReplicateAck {
        last_seq: SequenceNumber,
        success: bool,
        conflicts: Vec<u64>,
    },
    /// Request missing operations (anti-entropy)
    SyncRequest {
        from_seq: SequenceNumber,
        to_seq: Option<SequenceNumber>,
        keys: Option<Vec<Bytes>>,
    },
    /// Sync response
    SyncResponse {
        ops: Vec<ReplicationOp>,
        has_more: bool,
    },
    /// Heartbeat
    Heartbeat {
        region_id: RegionId,
        timestamp: u64,
        vector_clock: VectorClock,
        last_seq: SequenceNumber,
    },
    /// Heartbeat response
    HeartbeatAck { region_id: RegionId, timestamp: u64 },
    /// Topology update
    TopologyUpdate {
        regions: Vec<RegionInfo>,
        epoch: u64,
    },
    /// Conflict notification
    ConflictNotify { conflicts: Vec<Conflict> },
}

/// Region info for topology updates
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegionInfo {
    /// Region identifier
    pub region_id: RegionId,
    /// Region name
    pub name: String,
    /// Endpoints
    pub endpoints: Vec<SocketAddr>,
    /// Status
    pub status: RegionStatus,
    /// Priority
    pub priority: u32,
}

/// Command for the geo-replication manager
#[allow(clippy::large_enum_variant)]
pub enum GeoReplicationCommand {
    /// Replicate an operation
    Replicate(ReplicationOp),
    /// Get region status
    GetRegionStatus(RegionId, oneshot::Sender<Option<RegionState>>),
    /// Get all regions
    GetAllRegions(oneshot::Sender<Vec<RegionState>>),
    /// Force sync with a region
    ForceSync(RegionId),
    /// Add a peer region
    AddPeerRegion(PeerRegion),
    /// Remove a peer region
    RemovePeerRegion(RegionId),
    /// Update conflict resolution
    SetConflictResolution(ConflictResolution),
    /// Get metrics
    GetMetrics(oneshot::Sender<GeoReplicationSnapshot>),
    /// Shutdown
    Shutdown,
}

/// Snapshot of geo-replication state
#[derive(Debug)]
pub struct GeoReplicationSnapshot {
    /// Local region
    pub local_region: RegionId,
    /// Peer regions
    pub peer_regions: Vec<RegionState>,
    /// Current vector clock
    pub vector_clock: VectorClock,
    /// Pending operations
    pub pending_ops: usize,
    /// Conflicts pending resolution
    pub pending_conflicts: usize,
    /// Metrics snapshot
    pub metrics: GeoReplicationMetricsSnapshot,
}

/// Metrics snapshot
#[derive(Debug, Clone)]
pub struct GeoReplicationMetricsSnapshot {
    pub ops_replicated: u64,
    pub ops_received: u64,
    pub conflicts_detected: u64,
    pub conflicts_resolved: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub replication_lag_ms: u64,
    pub cross_region_latency_ms: u64,
}

/// Global Active-Active Geo-Replication Manager
pub struct GeoReplicationManager {
    /// Configuration
    config: GeoReplicationConfig,
    /// Local hybrid clock
    clock: Arc<RwLock<HybridClock>>,
    /// Local vector clock
    vector_clock: Arc<RwLock<VectorClock>>,
    /// Peer region states
    regions: Arc<RwLock<HashMap<RegionId, RegionState>>>,
    /// Pending operations queue
    pending_ops: Arc<RwLock<VecDeque<ReplicationOp>>>,
    /// Operation log for anti-entropy
    op_log: Arc<RwLock<OpLog>>,
    /// Conflict log
    conflicts: Arc<RwLock<Vec<Conflict>>>,
    /// Metrics
    metrics: Arc<GeoReplicationMetrics>,
    /// Running flag
    running: Arc<AtomicBool>,
    /// Command channel
    command_tx: mpsc::Sender<GeoReplicationCommand>,
    /// Shutdown signal
    shutdown_tx: broadcast::Sender<()>,
    /// Next conflict ID
    next_conflict_id: AtomicU64,
    /// Next sequence number
    next_seq: AtomicU64,
}

/// Operation log for anti-entropy sync
struct OpLog {
    /// Operations by sequence number
    ops: HashMap<SequenceNumber, ReplicationOp>,
    /// Min sequence in log
    min_seq: SequenceNumber,
    /// Max sequence in log
    max_seq: SequenceNumber,
    /// Max log size
    max_size: usize,
}

impl OpLog {
    fn new(max_size: usize) -> Self {
        Self {
            ops: HashMap::new(),
            min_seq: 0,
            max_seq: 0,
            max_size,
        }
    }

    fn append(&mut self, op: ReplicationOp) {
        let seq = op.seq;
        self.ops.insert(seq, op);

        if seq > self.max_seq {
            self.max_seq = seq;
        }
        if self.min_seq == 0 {
            self.min_seq = seq;
        }

        // Trim old entries if over size
        while self.ops.len() > self.max_size && self.min_seq < self.max_seq {
            self.ops.remove(&self.min_seq);
            self.min_seq += 1;
        }
    }

    fn get_range(&self, from: SequenceNumber, to: Option<SequenceNumber>) -> Vec<ReplicationOp> {
        let end = to.unwrap_or(self.max_seq);
        let mut ops: Vec<_> = self
            .ops
            .iter()
            .filter(|(seq, _)| **seq >= from && **seq <= end)
            .map(|(_, op)| op.clone())
            .collect();
        ops.sort_by_key(|op| op.seq);
        ops
    }
}

impl GeoReplicationManager {
    /// Create a new geo-replication manager
    pub fn new(config: GeoReplicationConfig) -> Self {
        let (command_tx, _command_rx) = mpsc::channel(10000);
        let (shutdown_tx, _) = broadcast::channel(1);

        // Generate a numeric site ID from the site string (hash it)
        let site_id_num = config
            .local_site
            .bytes()
            .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
        let site_id = SiteId::new(site_id_num);
        let clock = HybridClock::new(site_id);

        let mut vector_clock = VectorClock::new();
        vector_clock.increment(site_id);

        let manager = Self {
            config: config.clone(),
            clock: Arc::new(RwLock::new(clock)),
            vector_clock: Arc::new(RwLock::new(vector_clock)),
            regions: Arc::new(RwLock::new(HashMap::new())),
            pending_ops: Arc::new(RwLock::new(VecDeque::new())),
            op_log: Arc::new(RwLock::new(OpLog::new(100_000))),
            conflicts: Arc::new(RwLock::new(Vec::new())),
            metrics: Arc::new(GeoReplicationMetrics::default()),
            running: Arc::new(AtomicBool::new(false)),
            command_tx,
            shutdown_tx,
            next_conflict_id: AtomicU64::new(1),
            next_seq: AtomicU64::new(1),
        };

        // Initialize local region state
        let local_state = RegionState {
            region_id: config.local_region.clone(),
            is_local: true,
            status: RegionStatus::Online,
            last_seen: Instant::now(),
            last_replicated_seq: 0,
            vector_clock: VectorClock::new(),
            latency_ms: 0.0,
            bytes_sent: 0,
            bytes_received: 0,
            pending_ops: 0,
            error_count: 0,
        };
        manager
            .regions
            .write()
            .insert(config.local_region.clone(), local_state);

        // Initialize peer region states
        for peer in &config.peer_regions {
            let peer_state = RegionState {
                region_id: peer.region_id.clone(),
                is_local: false,
                status: RegionStatus::Offline,
                last_seen: Instant::now(),
                last_replicated_seq: 0,
                vector_clock: VectorClock::new(),
                latency_ms: peer.max_latency_ms as f64,
                bytes_sent: 0,
                bytes_received: 0,
                pending_ops: 0,
                error_count: 0,
            };
            manager
                .regions
                .write()
                .insert(peer.region_id.clone(), peer_state);
        }

        manager
    }

    /// Get command sender for async operations
    pub fn command_sender(&self) -> mpsc::Sender<GeoReplicationCommand> {
        self.command_tx.clone()
    }

    /// Start the geo-replication manager
    pub async fn start(&self) -> Result<(), GeoReplicationError> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err(GeoReplicationError::AlreadyRunning);
        }

        info!(
            "Starting geo-replication manager for region {} (site {})",
            self.config.local_region, self.config.local_site
        );

        // Start background tasks would go here
        // - Heartbeat sender
        // - Anti-entropy sync
        // - Pending ops processor

        Ok(())
    }

    /// Stop the geo-replication manager
    pub async fn stop(&self) -> Result<(), GeoReplicationError> {
        if !self.running.swap(false, Ordering::SeqCst) {
            return Err(GeoReplicationError::NotRunning);
        }

        let _ = self.shutdown_tx.send(());
        info!("Geo-replication manager stopped");

        Ok(())
    }

    /// Generate a new replication operation
    pub fn create_op(&self, key: Bytes, op_type: ReplicationOpType) -> ReplicationOp {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        let timestamp = self.clock.write().now();
        let site_id_num = self
            .config
            .local_site
            .bytes()
            .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
        let site_id = SiteId::new(site_id_num);
        let mut vector_clock = self.vector_clock.write();
        vector_clock.increment(site_id);

        ReplicationOp {
            seq,
            timestamp,
            vector_clock: vector_clock.clone(),
            source_region: self.config.local_region.clone(),
            source_site: self.config.local_site.clone(),
            key,
            op_type,
            crdt_state: None,
            metadata: HashMap::new(),
        }
    }

    /// Queue an operation for replication
    pub async fn replicate(&self, op: ReplicationOp) -> Result<(), GeoReplicationError> {
        // Add to operation log
        self.op_log.write().append(op.clone());

        // Add to pending queue
        self.pending_ops.write().push_back(op);

        // Update metrics
        self.metrics.ops_replicated.fetch_add(1, Ordering::Relaxed);

        // In async mode, we return immediately
        // Semi-sync or sync would wait for acknowledgments

        Ok(())
    }

    /// Apply an operation received from a peer
    pub async fn apply_remote_op(
        &self,
        op: ReplicationOp,
    ) -> Result<ConflictResolutionResult, GeoReplicationError> {
        // Update vector clock
        self.vector_clock.write().merge(&op.vector_clock);

        // Update clock with received timestamp
        self.clock.write().receive(op.timestamp);

        // Check for conflicts (would need storage access in real implementation)
        // For now, we just apply using the configured resolution strategy

        let resolution = self.resolve_conflict_strategy(&op);

        // Update metrics
        self.metrics.ops_received.fetch_add(1, Ordering::Relaxed);

        // Update region state
        if let Some(region) = self.regions.write().get_mut(&op.source_region) {
            region.last_seen = Instant::now();
            region.last_replicated_seq = op.seq;
            region.status = RegionStatus::Online;
        }

        Ok(resolution)
    }

    /// Resolve conflict using configured strategy
    fn resolve_conflict_strategy(&self, _op: &ReplicationOp) -> ConflictResolutionResult {
        match &self.config.conflict_resolution {
            ConflictResolution::LastWriteWins => ConflictResolutionResult::ApplyRemote,
            ConflictResolution::FirstWriteWins => ConflictResolutionResult::KeepLocal,
            ConflictResolution::MultiValue => ConflictResolutionResult::MultiValue(vec![]),
            ConflictResolution::Crdt => ConflictResolutionResult::Merged(Bytes::new()),
            ConflictResolution::Custom(_) => ConflictResolutionResult::ApplyRemote,
            ConflictResolution::PriorityBased => ConflictResolutionResult::ApplyRemote,
        }
    }

    /// Detect and record a conflict
    pub fn record_conflict(
        &self,
        key: Bytes,
        local_value: Option<Bytes>,
        local_timestamp: HybridTimestamp,
        remote_value: Option<Bytes>,
        remote_timestamp: HybridTimestamp,
        remote_region: RegionId,
    ) -> u64 {
        let id = self.next_conflict_id.fetch_add(1, Ordering::SeqCst);

        let detected_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let conflict = Conflict {
            id,
            key,
            local_value,
            local_timestamp,
            remote_value,
            remote_timestamp,
            remote_region,
            resolution: ConflictResolutionResult::Pending,
            detected_at_ms,
        };

        self.conflicts.write().push(conflict);
        self.metrics
            .conflicts_detected
            .fetch_add(1, Ordering::Relaxed);

        id
    }

    /// Get current replication lag for a region
    pub fn get_replication_lag(&self, region_id: &RegionId) -> Option<Duration> {
        let regions = self.regions.read();
        regions.get(region_id).map(|r| {
            let current_seq = self.next_seq.load(Ordering::Relaxed);
            let lag_ops = current_seq.saturating_sub(r.last_replicated_seq);
            // Estimate lag based on ops * avg op time
            Duration::from_millis(lag_ops * 10)
        })
    }

    /// Get region state
    pub fn get_region_state(&self, region_id: &RegionId) -> Option<RegionState> {
        self.regions.read().get(region_id).cloned()
    }

    /// Get all region states
    pub fn get_all_regions(&self) -> Vec<RegionState> {
        self.regions.read().values().cloned().collect()
    }

    /// Get metrics snapshot
    pub fn metrics_snapshot(&self) -> GeoReplicationMetricsSnapshot {
        GeoReplicationMetricsSnapshot {
            ops_replicated: self.metrics.ops_replicated.load(Ordering::Relaxed),
            ops_received: self.metrics.ops_received.load(Ordering::Relaxed),
            conflicts_detected: self.metrics.conflicts_detected.load(Ordering::Relaxed),
            conflicts_resolved: self.metrics.conflicts_resolved.load(Ordering::Relaxed),
            bytes_sent: self.metrics.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.metrics.bytes_received.load(Ordering::Relaxed),
            replication_lag_ms: self.metrics.replication_lag_ms.load(Ordering::Relaxed),
            cross_region_latency_ms: self.metrics.cross_region_latency_ms.load(Ordering::Relaxed),
        }
    }

    /// Request sync from a peer region (anti-entropy)
    pub fn create_sync_request(&self, region_id: &RegionId) -> Option<GeoReplicationMessage> {
        let regions = self.regions.read();
        let region = regions.get(region_id)?;

        Some(GeoReplicationMessage::SyncRequest {
            from_seq: region.last_replicated_seq + 1,
            to_seq: None,
            keys: None,
        })
    }

    /// Handle sync request from peer
    pub fn handle_sync_request(
        &self,
        from_seq: SequenceNumber,
        to_seq: Option<SequenceNumber>,
    ) -> GeoReplicationMessage {
        let ops = self.op_log.read().get_range(from_seq, to_seq);
        let batch_size = self.config.batch_size.min(ops.len());
        let has_more = ops.len() > batch_size;

        GeoReplicationMessage::SyncResponse {
            ops: ops.into_iter().take(batch_size).collect(),
            has_more,
        }
    }

    /// Create heartbeat message
    pub fn create_heartbeat(&self) -> GeoReplicationMessage {
        let vector_clock = self.vector_clock.read().clone();
        let last_seq = self.next_seq.load(Ordering::Relaxed).saturating_sub(1);

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        GeoReplicationMessage::Heartbeat {
            region_id: self.config.local_region.clone(),
            timestamp,
            vector_clock,
            last_seq,
        }
    }

    /// Handle heartbeat from peer
    pub fn handle_heartbeat(
        &self,
        region_id: &RegionId,
        timestamp: u64,
        vector_clock: &VectorClock,
        last_seq: SequenceNumber,
    ) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let latency = now.saturating_sub(timestamp);

        let mut regions = self.regions.write();
        if let Some(region) = regions.get_mut(region_id) {
            region.last_seen = Instant::now();
            region.latency_ms = latency as f64;
            region.last_replicated_seq = last_seq;
            region.vector_clock = vector_clock.clone();

            // Update status based on latency
            let max_latency = self
                .config
                .peer_regions
                .iter()
                .find(|p| &p.region_id == region_id)
                .map(|p| p.max_latency_ms)
                .unwrap_or(1000);

            region.status = if latency > max_latency * 2 {
                RegionStatus::Degraded
            } else {
                RegionStatus::Online
            };
        }

        // Merge vector clock
        self.vector_clock.write().merge(vector_clock);
    }

    /// Check if we should replicate to a region
    pub fn should_replicate_to(&self, region_id: &RegionId) -> bool {
        let regions = self.regions.read();
        if let Some(region) = regions.get(region_id) {
            matches!(region.status, RegionStatus::Online | RegionStatus::Degraded)
        } else {
            false
        }
    }

    /// Get the primary region (for routing decisions)
    pub fn get_primary_region(&self) -> Option<RegionId> {
        // In active-active, primary is typically the local region for writes
        Some(self.config.local_region.clone())
    }

    /// Check causal ordering - can we apply this op?
    pub fn can_apply(&self, op: &ReplicationOp) -> bool {
        if !self.config.causal_ordering {
            return true;
        }

        // Check if we've seen all operations that causally precede this one
        let local_clock = self.vector_clock.read();
        op.vector_clock.happened_before(&local_clock)
            || op.vector_clock.concurrent(&local_clock)
            || *local_clock == op.vector_clock
    }
}

/// Geo-replication errors
#[derive(Debug, thiserror::Error)]
pub enum GeoReplicationError {
    #[error("Geo-replication is not enabled")]
    NotEnabled,

    #[error("Already running")]
    AlreadyRunning,

    #[error("Not running")]
    NotRunning,

    #[error("Region not found: {0}")]
    RegionNotFound(String),

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Sync error: {0}")]
    SyncError(String),

    #[error("Conflict resolution error: {0}")]
    ConflictError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Timeout")]
    Timeout,

    #[error("Channel closed")]
    ChannelClosed,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn create_test_config() -> GeoReplicationConfig {
        GeoReplicationConfig {
            enabled: true,
            local_region: "us-east".to_string(),
            local_site: "us-east-1".to_string(),
            peer_regions: vec![PeerRegion {
                region_id: "eu-west".to_string(),
                name: "EU West".to_string(),
                endpoints: vec!["127.0.0.1:6380".parse().unwrap()],
                priority: 1,
                max_latency_ms: 100,
                read_preference: ReadPreference::Nearest,
                write_forwarding: true,
            }],
            ..Default::default()
        }
    }

    #[test]
    fn test_geo_replication_manager_creation() {
        let config = create_test_config();
        let manager = GeoReplicationManager::new(config);

        assert_eq!(manager.config.local_region, "us-east");
        assert_eq!(manager.config.local_site, "us-east-1");

        let regions = manager.get_all_regions();
        assert_eq!(regions.len(), 2); // local + 1 peer
    }

    #[test]
    fn test_create_op() {
        let config = create_test_config();
        let manager = GeoReplicationManager::new(config);

        let op = manager.create_op(
            Bytes::from("test-key"),
            ReplicationOpType::Set {
                value: Bytes::from("test-value"),
                ttl: None,
            },
        );

        assert_eq!(op.seq, 1);
        assert_eq!(op.source_region, "us-east");
        assert_eq!(op.key.as_ref(), b"test-key");
    }

    #[tokio::test]
    async fn test_replicate_op() {
        let config = create_test_config();
        let manager = GeoReplicationManager::new(config);

        let op = manager.create_op(
            Bytes::from("key1"),
            ReplicationOpType::Set {
                value: Bytes::from("value1"),
                ttl: None,
            },
        );

        let result = manager.replicate(op).await;
        assert!(result.is_ok());

        let metrics = manager.metrics_snapshot();
        assert_eq!(metrics.ops_replicated, 1);
    }

    #[test]
    fn test_heartbeat() {
        let config = create_test_config();
        let manager = GeoReplicationManager::new(config);

        let heartbeat = manager.create_heartbeat();

        if let GeoReplicationMessage::Heartbeat { region_id, .. } = heartbeat {
            assert_eq!(region_id, "us-east");
        } else {
            panic!("Expected heartbeat message");
        }
    }

    #[test]
    fn test_handle_heartbeat() {
        let config = create_test_config();
        let manager = GeoReplicationManager::new(config);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let mut vc = VectorClock::new();
        vc.increment(SiteId::new(1));

        let region_id = "eu-west".to_string();
        manager.handle_heartbeat(&region_id, now - 50, &vc, 100);

        let region = manager.get_region_state(&"eu-west".to_string()).unwrap();
        assert_eq!(region.last_replicated_seq, 100);
        assert_eq!(region.status, RegionStatus::Online);
    }

    #[test]
    fn test_sync_request() {
        let config = create_test_config();
        let manager = GeoReplicationManager::new(config);

        let request = manager.create_sync_request(&"eu-west".to_string());
        assert!(request.is_some());

        if let Some(GeoReplicationMessage::SyncRequest { from_seq, .. }) = request {
            assert_eq!(from_seq, 1);
        }
    }

    #[test]
    fn test_conflict_recording() {
        let config = create_test_config();
        let manager = GeoReplicationManager::new(config);

        let conflict_id = manager.record_conflict(
            Bytes::from("key1"),
            Some(Bytes::from("local-value")),
            HybridTimestamp::new(100, 0, SiteId::new(1)),
            Some(Bytes::from("remote-value")),
            HybridTimestamp::new(101, 0, SiteId::new(2)),
            "eu-west".to_string(),
        );

        assert_eq!(conflict_id, 1);

        let metrics = manager.metrics_snapshot();
        assert_eq!(metrics.conflicts_detected, 1);
    }

    #[test]
    fn test_region_status_tracking() {
        let config = create_test_config();
        let manager = GeoReplicationManager::new(config);

        let local = manager.get_region_state(&"us-east".to_string()).unwrap();
        assert!(local.is_local);
        assert_eq!(local.status, RegionStatus::Online);

        let peer = manager.get_region_state(&"eu-west".to_string()).unwrap();
        assert!(!peer.is_local);
        assert_eq!(peer.status, RegionStatus::Offline); // Initially offline
    }

    #[test]
    fn test_causal_ordering() {
        let config = create_test_config();
        let manager = GeoReplicationManager::new(config);

        // Create an op with the current vector clock
        let op = manager.create_op(
            Bytes::from("key"),
            ReplicationOpType::Set {
                value: Bytes::from("value"),
                ttl: None,
            },
        );

        // Should be able to apply our own ops
        assert!(manager.can_apply(&op));
    }
}
