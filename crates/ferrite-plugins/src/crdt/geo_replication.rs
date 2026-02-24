//! # Geo-Replication Coordinator
//!
//! Manages active-active multi-region replication using CRDTs, enabling
//! concurrent writes at multiple datacenters with automatic, deterministic
//! conflict resolution.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
//! │  US-East     │◄───►│  EU-West     │◄───►│  AP-South    │
//! │  GeoReplicator│    │  GeoReplicator│    │  GeoReplicator│
//! │  ┌──────────┐│    │  ┌──────────┐│    │  ┌──────────┐│
//! │  │VectorClk ││    │  │VectorClk ││    │  │VectorClk ││
//! │  ├──────────┤│    │  ├──────────┤│    │  ├──────────┤│
//! │  │Op Queue  ││    │  │Op Queue  ││    │  │Op Queue  ││
//! │  ├──────────┤│    │  ├──────────┤│    │  ├──────────┤│
//! │  │Conflict  ││    │  │Conflict  ││    │  │Conflict  ││
//! │  │Resolver  ││    │  │Resolver  ││    │  │Resolver  ││
//! │  └──────────┘│    │  └──────────┘│    │  └──────────┘│
//! └──────────────┘     └──────────────┘     └──────────────┘
//! ```
//!
//! # Example
//!
//! ```no_run
//! use ferrite::crdt::geo_replication::{GeoReplicator, GeoConfig, RegionInfo, ConflictPolicy};
//!
//! let config = GeoConfig {
//!     local_region: "us-east-1".to_string(),
//!     ..Default::default()
//! };
//!
//! let replicator = GeoReplicator::new(config);
//!
//! // Register remote regions
//! let region = RegionInfo::new("eu-west-1", "https://eu-west.example.com:6380");
//! let region_id = replicator.register_region(region).unwrap();
//!
//! // List active regions
//! let regions = replicator.list_regions();
//! assert_eq!(regions.len(), 1);
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur during geo-replication operations
#[derive(Debug, Clone, thiserror::Error)]
pub enum GeoError {
    /// The specified region was not found
    #[error("Region not found")]
    RegionNotFound,
    /// A region with the same ID already exists
    #[error("Region already exists")]
    RegionAlreadyExists,
    /// Replication to one or more regions failed
    #[error("Replication failed: {0}")]
    ReplicationFailed(String),
    /// Region synchronisation failed
    #[error("Sync failed: {0}")]
    SyncFailed(String),
    /// A conflict could not be resolved
    #[error("Conflict unresolvable: {0}")]
    ConflictUnresolvable(String),
    /// A quorum of regions did not acknowledge the operation
    #[error("Quorum not reached")]
    QuorumNotReached,
    /// Operation timed out
    #[error("Operation timed out")]
    Timeout,
    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Vector clock
// ---------------------------------------------------------------------------

/// Logical vector clock for causal ordering across regions
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorClock {
    /// Per-region logical counters
    pub entries: HashMap<String, u64>,
}

impl VectorClock {
    /// Create a new empty vector clock
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment the counter for `region`
    pub fn increment(&mut self, region: &str) {
        let counter = self.entries.entry(region.to_string()).or_insert(0);
        *counter += 1;
    }

    /// Get the counter value for `region`
    pub fn get(&self, region: &str) -> u64 {
        self.entries.get(region).copied().unwrap_or(0)
    }

    /// Merge with another vector clock (take element-wise max)
    pub fn merge(&mut self, other: &VectorClock) {
        for (region, &count) in &other.entries {
            let entry = self.entries.entry(region.clone()).or_insert(0);
            *entry = (*entry).max(count);
        }
    }

    /// Returns `true` if this clock causally happened-before `other`.
    ///
    /// A clock `A` happened-before `B` iff every entry in `A` is ≤ the
    /// corresponding entry in `B`, and at least one is strictly less.
    pub fn happens_before(&self, other: &VectorClock) -> bool {
        let all_le = self.entries.iter().all(|(r, &c)| other.get(r) >= c);
        let at_least_one_lt = self.entries.iter().any(|(r, &c)| other.get(r) > c)
            || other
                .entries
                .keys()
                .any(|r| !self.entries.contains_key(r) && other.get(r) > 0);
        all_le && at_least_one_lt
    }

    /// Returns `true` if this clock and `other` are concurrent (neither
    /// happened-before the other and they are not equal).
    pub fn is_concurrent(&self, other: &VectorClock) -> bool {
        !self.happens_before(other) && !other.happens_before(self) && self != other
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Replication mode for geo-replication
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationMode {
    /// Fire-and-forget async replication
    Async,
    /// Wait for a quorum of regions to acknowledge
    SyncQuorum {
        /// Minimum number of region acknowledgements required
        min_acks: usize,
    },
    /// Wait for all regions to acknowledge
    SyncAll,
}

/// Policy used to resolve conflicts between concurrent writes
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictPolicy {
    /// Most recent timestamp wins
    LastWriterWins,
    /// Numerically highest value wins
    MaxValueWins,
    /// Merge all concurrent values
    MergeAll,
    /// Delegate to a user-defined resolver by name
    Custom {
        /// Name of the registered custom resolver
        resolver_name: String,
    },
}

/// Configuration for the geo-replication coordinator
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GeoConfig {
    /// Name of the local region
    pub local_region: String,
    /// Replication mode
    pub replication_mode: ReplicationMode,
    /// Conflict resolution policy
    pub conflict_policy: ConflictPolicy,
    /// Maximum tolerated replication lag in milliseconds
    pub max_replication_lag_ms: u64,
    /// Interval between background sync sweeps in milliseconds
    pub sync_interval_ms: u64,
    /// Maximum operations per replication batch
    pub max_batch_size: usize,
    /// Whether to enforce causal ordering of operations
    pub enable_causal_ordering: bool,
}

impl Default for GeoConfig {
    fn default() -> Self {
        Self {
            local_region: "local".to_string(),
            replication_mode: ReplicationMode::Async,
            conflict_policy: ConflictPolicy::LastWriterWins,
            max_replication_lag_ms: 5000,
            sync_interval_ms: 1000,
            max_batch_size: 1000,
            enable_causal_ordering: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Region types
// ---------------------------------------------------------------------------

/// Unique identifier for a registered region
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RegionId(pub String);

impl RegionId {
    /// Generate a new random region ID
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

impl Default for RegionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for RegionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Metadata describing a remote region
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegionInfo {
    /// Unique region identifier
    pub id: RegionId,
    /// Human-readable region name
    pub name: String,
    /// Network endpoint for this region
    pub endpoint: String,
    /// Priority (lower = higher priority for conflict tie-breaking)
    pub priority: u32,
    /// Arbitrary tags for routing or filtering
    pub tags: HashMap<String, String>,
}

impl RegionInfo {
    /// Create a new region with the given `name` and `endpoint`
    pub fn new(name: &str, endpoint: &str) -> Self {
        Self {
            id: RegionId::new(),
            name: name.to_string(),
            endpoint: endpoint.to_string(),
            priority: 100,
            tags: HashMap::new(),
        }
    }
}

/// Health status of a region
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegionHealth {
    /// Region is reachable and up-to-date
    Healthy,
    /// Region is reachable but lagging
    Degraded,
    /// Region cannot be contacted
    Unreachable,
    /// Region is currently synchronising state
    Syncing,
}

impl std::fmt::Display for RegionHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegionHealth::Healthy => write!(f, "Healthy"),
            RegionHealth::Degraded => write!(f, "Degraded"),
            RegionHealth::Unreachable => write!(f, "Unreachable"),
            RegionHealth::Syncing => write!(f, "Syncing"),
        }
    }
}

/// Runtime status of a registered region
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegionStatus {
    /// Region metadata
    pub info: RegionInfo,
    /// Current health
    pub status: RegionHealth,
    /// Timestamp of the last successful sync
    pub last_sync: Option<DateTime<Utc>>,
    /// Estimated replication lag in milliseconds
    pub replication_lag_ms: u64,
    /// Number of operations pending replication to this region
    pub pending_ops: u64,
}

// ---------------------------------------------------------------------------
// CRDT operation types
// ---------------------------------------------------------------------------

/// The kind of CRDT mutation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CrdtOpType {
    /// Increment a counter by `i64`
    CounterIncrement(i64),
    /// Decrement a counter by `i64`
    CounterDecrement(i64),
    /// Add a member to a set
    SetAdd(String),
    /// Remove a member from a set
    SetRemove(String),
    /// Set a register value
    RegisterSet(String),
    /// Put a key-value pair into a map
    MapPut(String, String),
    /// Remove a key from a map
    MapRemove(String),
}

/// A single CRDT operation destined for replication
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CrdtOperation {
    /// Unique operation identifier
    pub op_id: String,
    /// Key being modified
    pub key: String,
    /// Type of mutation
    pub op_type: CrdtOpType,
    /// Originating region name
    pub region: String,
    /// Wall-clock timestamp of the operation
    pub timestamp: DateTime<Utc>,
    /// Vector clock at the time of the operation
    pub vector_clock: VectorClock,
}

// ---------------------------------------------------------------------------
// Conflict resolution
// ---------------------------------------------------------------------------

/// A pair of conflicting values detected during replication
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConflictEntry {
    /// Key where the conflict occurred
    pub key: String,
    /// Value from the local region
    pub local_value: String,
    /// Value from the remote region
    pub remote_value: String,
    /// Local vector clock at time of write
    pub local_clock: VectorClock,
    /// Remote vector clock at time of write
    pub remote_clock: VectorClock,
    /// Local wall-clock timestamp
    pub local_timestamp: DateTime<Utc>,
    /// Remote wall-clock timestamp
    pub remote_timestamp: DateTime<Utc>,
}

/// Which side of a conflict was chosen
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictWinner {
    /// Local value was chosen
    Local,
    /// Remote value was chosen
    Remote,
    /// Values were merged into a new value
    Merged,
}

/// The outcome of resolving one or more conflicts
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Resolution {
    /// Which side won
    pub winner: ConflictWinner,
    /// Merged value (present when `winner == Merged`)
    pub merged_value: Option<String>,
    /// Human-readable justification
    pub reason: String,
}

// ---------------------------------------------------------------------------
// Replication / sync results
// ---------------------------------------------------------------------------

/// Status of a replicated operation
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationStatus {
    /// Operation committed on the required number of regions
    Committed,
    /// Acknowledgements are still pending
    Pending,
    /// Replication failed with an error message
    Failed(String),
}

/// Result of replicating a single CRDT operation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicationResult {
    /// Operation that was replicated
    pub op_id: String,
    /// Number of regions that acknowledged the operation
    pub regions_acked: usize,
    /// Total number of regions the operation was sent to
    pub regions_total: usize,
    /// End-to-end latency in milliseconds
    pub latency_ms: u64,
    /// Current status
    pub status: ReplicationStatus,
}

/// Result of synchronising state with a remote region
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncResult {
    /// Region that was synchronised
    pub region_id: RegionId,
    /// Number of operations exchanged
    pub ops_synced: u64,
    /// Total bytes transferred
    pub bytes_transferred: u64,
    /// Time taken in milliseconds
    pub duration_ms: u64,
    /// Number of conflicts resolved during this sync
    pub conflicts_resolved: u64,
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

/// Aggregate statistics for the geo-replication coordinator
#[derive(Debug)]
pub struct GeoStats {
    /// Total operations replicated since startup
    pub total_ops_replicated: u64,
    /// Total conflicts detected and resolved
    pub total_conflicts: u64,
    /// Total region sync rounds completed
    pub total_syncs: u64,
    /// Average replication lag across healthy regions (ms)
    pub avg_replication_lag_ms: u64,
    /// Number of regions currently healthy
    pub regions_healthy: u64,
    /// Number of regions currently degraded
    pub regions_degraded: u64,
    /// Number of regions currently unreachable
    pub regions_unreachable: u64,
    /// Total bytes replicated across all regions
    pub bytes_replicated: u64,
}

/// Internal atomic counters for lock-free stats updates
struct AtomicStats {
    total_ops_replicated: AtomicU64,
    total_conflicts: AtomicU64,
    total_syncs: AtomicU64,
    bytes_replicated: AtomicU64,
}

impl Default for AtomicStats {
    fn default() -> Self {
        Self {
            total_ops_replicated: AtomicU64::new(0),
            total_conflicts: AtomicU64::new(0),
            total_syncs: AtomicU64::new(0),
            bytes_replicated: AtomicU64::new(0),
        }
    }
}

// ---------------------------------------------------------------------------
// GeoReplicator
// ---------------------------------------------------------------------------

/// The geo-replication coordinator.
///
/// Manages a set of remote regions and replicates CRDT operations between them
/// using the configured [`ReplicationMode`] and [`ConflictPolicy`].
///
/// Thread-safe: all state is protected by concurrent data structures
/// ([`DashMap`], [`RwLock`], atomics).
pub struct GeoReplicator {
    /// Configuration
    config: GeoConfig,
    /// Registered regions keyed by [`RegionId`]
    regions: DashMap<RegionId, RegionStatus>,
    /// Global vector clock
    vector_clock: RwLock<VectorClock>,
    /// Pending operations awaiting replication
    pending_ops: RwLock<Vec<CrdtOperation>>,
    /// Atomic statistics counters
    stats: AtomicStats,
}

impl GeoReplicator {
    /// Create a new geo-replication coordinator with the given configuration
    pub fn new(config: GeoConfig) -> Self {
        Self {
            config,
            regions: DashMap::new(),
            vector_clock: RwLock::new(VectorClock::new()),
            pending_ops: RwLock::new(Vec::new()),
            stats: AtomicStats::default(),
        }
    }

    /// Register a remote region for replication.
    ///
    /// Returns the [`RegionId`] on success or [`GeoError::RegionAlreadyExists`]
    /// if a region with the same ID is already registered.
    pub fn register_region(&self, region: RegionInfo) -> Result<RegionId, GeoError> {
        let id = region.id.clone();
        if self.regions.contains_key(&id) {
            return Err(GeoError::RegionAlreadyExists);
        }

        let status = RegionStatus {
            info: region,
            status: RegionHealth::Syncing,
            last_sync: None,
            replication_lag_ms: 0,
            pending_ops: 0,
        };
        self.regions.insert(id.clone(), status);
        Ok(id)
    }

    /// Unregister a previously registered region.
    pub fn unregister_region(&self, id: &RegionId) -> Result<(), GeoError> {
        self.regions
            .remove(id)
            .map(|_| ())
            .ok_or(GeoError::RegionNotFound)
    }

    /// Replicate a CRDT operation to registered regions.
    ///
    /// Behaviour depends on the configured [`ReplicationMode`]:
    /// - [`ReplicationMode::Async`] — returns immediately after queuing.
    /// - [`ReplicationMode::SyncQuorum`] — waits for `min_acks` acknowledgements.
    /// - [`ReplicationMode::SyncAll`] — waits for all regions.
    pub async fn replicate(&self, op: CrdtOperation) -> Result<ReplicationResult, GeoError> {
        let start = Instant::now();

        // Advance the local vector clock for this operation
        {
            let mut clock = self.vector_clock.write();
            clock.increment(&self.config.local_region);
            clock.merge(&op.vector_clock);
        }

        let regions_total = self.regions.len();

        // Queue the operation for each region
        {
            let mut pending = self.pending_ops.write();
            pending.push(op.clone());

            // Trim to max batch size
            if pending.len() > self.config.max_batch_size {
                let drain_count = pending.len() - self.config.max_batch_size;
                pending.drain(0..drain_count);
            }
        }

        // Update per-region pending counts
        for mut entry in self.regions.iter_mut() {
            entry.value_mut().pending_ops += 1;
        }

        // Determine how many acks are required
        let required_acks = match &self.config.replication_mode {
            ReplicationMode::Async => 0,
            ReplicationMode::SyncQuorum { min_acks } => *min_acks,
            ReplicationMode::SyncAll => regions_total,
        };

        // In async mode we consider the op immediately committed.
        // For sync modes in a real deployment the coordinator would wait for
        // network acknowledgements; here we simulate the result.
        let (regions_acked, status) = if regions_total == 0 {
            (0, ReplicationStatus::Committed)
        } else if required_acks == 0 {
            (0, ReplicationStatus::Pending)
        } else {
            // Count healthy/syncing regions as acked (simulated)
            let acked = self
                .regions
                .iter()
                .filter(|r| {
                    matches!(
                        r.value().status,
                        RegionHealth::Healthy | RegionHealth::Syncing
                    )
                })
                .count();
            let s = if acked >= required_acks {
                ReplicationStatus::Committed
            } else {
                ReplicationStatus::Failed(format!(
                    "only {} of {} required acks received",
                    acked, required_acks
                ))
            };
            (acked, s)
        };

        let latency_ms = start.elapsed().as_millis() as u64;
        self.stats
            .total_ops_replicated
            .fetch_add(1, Ordering::Relaxed);

        Ok(ReplicationResult {
            op_id: op.op_id,
            regions_acked,
            regions_total,
            latency_ms,
            status,
        })
    }

    /// Synchronise state with a specific remote region.
    ///
    /// Returns a [`SyncResult`] summarising the operations exchanged.
    pub async fn sync_region(&self, region_id: &RegionId) -> Result<SyncResult, GeoError> {
        let start = Instant::now();

        let mut region = self
            .regions
            .get_mut(region_id)
            .ok_or(GeoError::RegionNotFound)?;

        region.status = RegionHealth::Syncing;

        // Count pending ops for this region
        let ops_synced = region.pending_ops;
        region.pending_ops = 0;
        region.last_sync = Some(Utc::now());
        region.replication_lag_ms = 0;
        region.status = RegionHealth::Healthy;

        drop(region);

        let duration_ms = start.elapsed().as_millis() as u64;
        let bytes_transferred = ops_synced * 128; // estimated bytes per op

        self.stats.total_syncs.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_replicated
            .fetch_add(bytes_transferred, Ordering::Relaxed);

        Ok(SyncResult {
            region_id: region_id.clone(),
            ops_synced,
            bytes_transferred,
            duration_ms,
            conflicts_resolved: 0,
        })
    }

    /// Resolve a set of conflicts using the configured [`ConflictPolicy`].
    pub fn resolve_conflict(&self, conflicts: &[ConflictEntry]) -> Resolution {
        if conflicts.is_empty() {
            return Resolution {
                winner: ConflictWinner::Local,
                merged_value: None,
                reason: "no conflicts to resolve".to_string(),
            };
        }

        match &self.config.conflict_policy {
            ConflictPolicy::LastWriterWins => {
                let entry = &conflicts[0];
                if entry.local_timestamp >= entry.remote_timestamp {
                    Resolution {
                        winner: ConflictWinner::Local,
                        merged_value: None,
                        reason: "local timestamp is newer or equal".to_string(),
                    }
                } else {
                    Resolution {
                        winner: ConflictWinner::Remote,
                        merged_value: None,
                        reason: "remote timestamp is newer".to_string(),
                    }
                }
            }
            ConflictPolicy::MaxValueWins => {
                let entry = &conflicts[0];
                if entry.local_value >= entry.remote_value {
                    Resolution {
                        winner: ConflictWinner::Local,
                        merged_value: None,
                        reason: "local value is greater or equal".to_string(),
                    }
                } else {
                    Resolution {
                        winner: ConflictWinner::Remote,
                        merged_value: None,
                        reason: "remote value is greater".to_string(),
                    }
                }
            }
            ConflictPolicy::MergeAll => {
                let entry = &conflicts[0];
                let merged = format!("{}|{}", entry.local_value, entry.remote_value);
                Resolution {
                    winner: ConflictWinner::Merged,
                    merged_value: Some(merged),
                    reason: "values merged".to_string(),
                }
            }
            ConflictPolicy::Custom { resolver_name } => {
                // Custom resolvers would be looked up in a registry; fall back
                // to LWW when no registry is available.
                let entry = &conflicts[0];
                if entry.local_timestamp >= entry.remote_timestamp {
                    Resolution {
                        winner: ConflictWinner::Local,
                        merged_value: None,
                        reason: format!(
                            "custom resolver '{}' not available, fell back to LWW (local)",
                            resolver_name
                        ),
                    }
                } else {
                    Resolution {
                        winner: ConflictWinner::Remote,
                        merged_value: None,
                        reason: format!(
                            "custom resolver '{}' not available, fell back to LWW (remote)",
                            resolver_name
                        ),
                    }
                }
            }
        }
    }

    /// Return a snapshot of the current global vector clock
    pub fn get_vector_clock(&self) -> VectorClock {
        self.vector_clock.read().clone()
    }

    /// List all registered regions with their current status
    pub fn list_regions(&self) -> Vec<RegionStatus> {
        self.regions.iter().map(|r| r.value().clone()).collect()
    }

    /// Return aggregate statistics for this coordinator
    pub fn get_stats(&self) -> GeoStats {
        let (mut healthy, mut degraded, mut unreachable) = (0u64, 0u64, 0u64);
        let mut total_lag: u64 = 0;
        let mut lag_count: u64 = 0;

        for entry in self.regions.iter() {
            match entry.value().status {
                RegionHealth::Healthy => healthy += 1,
                RegionHealth::Degraded => degraded += 1,
                RegionHealth::Unreachable => unreachable += 1,
                RegionHealth::Syncing => healthy += 1,
            }
            total_lag += entry.value().replication_lag_ms;
            lag_count += 1;
        }

        let avg_lag = if lag_count > 0 {
            total_lag / lag_count
        } else {
            0
        };

        GeoStats {
            total_ops_replicated: self.stats.total_ops_replicated.load(Ordering::Relaxed),
            total_conflicts: self.stats.total_conflicts.load(Ordering::Relaxed),
            total_syncs: self.stats.total_syncs.load(Ordering::Relaxed),
            avg_replication_lag_ms: avg_lag,
            regions_healthy: healthy,
            regions_degraded: degraded,
            regions_unreachable: unreachable,
            bytes_replicated: self.stats.bytes_replicated.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- helpers ------------------------------------------------------------

    fn make_config() -> GeoConfig {
        GeoConfig {
            local_region: "us-east-1".to_string(),
            ..Default::default()
        }
    }

    fn make_region(name: &str) -> RegionInfo {
        RegionInfo::new(name, &format!("https://{}.example.com:6380", name))
    }

    fn make_op(key: &str, op_type: CrdtOpType, region: &str) -> CrdtOperation {
        CrdtOperation {
            op_id: Uuid::new_v4().to_string(),
            key: key.to_string(),
            op_type,
            region: region.to_string(),
            timestamp: Utc::now(),
            vector_clock: VectorClock::new(),
        }
    }

    fn make_conflict(
        local_val: &str,
        remote_val: &str,
        local_ts: DateTime<Utc>,
        remote_ts: DateTime<Utc>,
    ) -> ConflictEntry {
        ConflictEntry {
            key: "test-key".to_string(),
            local_value: local_val.to_string(),
            remote_value: remote_val.to_string(),
            local_clock: VectorClock::new(),
            remote_clock: VectorClock::new(),
            local_timestamp: local_ts,
            remote_timestamp: remote_ts,
        }
    }

    // -- region registration / removal --------------------------------------

    #[test]
    fn test_register_region() {
        let replicator = GeoReplicator::new(make_config());
        let region = make_region("eu-west-1");
        let id = replicator.register_region(region).unwrap();

        let regions = replicator.list_regions();
        assert_eq!(regions.len(), 1);
        assert_eq!(regions[0].info.name, "eu-west-1");
        assert_eq!(regions[0].status, RegionHealth::Syncing);
        assert_eq!(regions[0].info.id, id);
    }

    #[test]
    fn test_register_duplicate_region() {
        let replicator = GeoReplicator::new(make_config());
        let region = make_region("eu-west-1");
        let id = replicator.register_region(region.clone()).unwrap();

        // Re-registering the same RegionInfo (same id) should fail
        let dup = RegionInfo {
            id: id.clone(),
            name: "eu-west-1-dup".to_string(),
            endpoint: "https://dup.example.com".to_string(),
            priority: 50,
            tags: HashMap::new(),
        };
        let err = replicator.register_region(dup).unwrap_err();
        assert!(matches!(err, GeoError::RegionAlreadyExists));
    }

    #[test]
    fn test_unregister_region() {
        let replicator = GeoReplicator::new(make_config());
        let region = make_region("ap-south-1");
        let id = replicator.register_region(region).unwrap();

        assert_eq!(replicator.list_regions().len(), 1);
        replicator.unregister_region(&id).unwrap();
        assert_eq!(replicator.list_regions().len(), 0);
    }

    #[test]
    fn test_unregister_unknown_region() {
        let replicator = GeoReplicator::new(make_config());
        let err = replicator.unregister_region(&RegionId::new()).unwrap_err();
        assert!(matches!(err, GeoError::RegionNotFound));
    }

    #[test]
    fn test_register_multiple_regions() {
        let replicator = GeoReplicator::new(make_config());
        replicator
            .register_region(make_region("eu-west-1"))
            .unwrap();
        replicator
            .register_region(make_region("ap-south-1"))
            .unwrap();
        replicator
            .register_region(make_region("us-west-2"))
            .unwrap();

        assert_eq!(replicator.list_regions().len(), 3);
    }

    // -- vector clock -------------------------------------------------------

    #[test]
    fn test_vector_clock_increment() {
        let mut clock = VectorClock::new();
        assert_eq!(clock.get("us-east"), 0);

        clock.increment("us-east");
        assert_eq!(clock.get("us-east"), 1);

        clock.increment("us-east");
        assert_eq!(clock.get("us-east"), 2);

        clock.increment("eu-west");
        assert_eq!(clock.get("eu-west"), 1);
        assert_eq!(clock.get("us-east"), 2);
    }

    #[test]
    fn test_vector_clock_merge() {
        let mut a = VectorClock::new();
        a.increment("us-east");
        a.increment("us-east");

        let mut b = VectorClock::new();
        b.increment("eu-west");
        b.increment("eu-west");
        b.increment("eu-west");
        b.increment("us-east");

        a.merge(&b);
        assert_eq!(a.get("us-east"), 2); // max(2, 1)
        assert_eq!(a.get("eu-west"), 3); // max(0, 3)
    }

    #[test]
    fn test_vector_clock_merge_idempotent() {
        let mut a = VectorClock::new();
        a.increment("r1");
        a.increment("r2");

        let b = a.clone();
        a.merge(&b);

        assert_eq!(a.get("r1"), 1);
        assert_eq!(a.get("r2"), 1);
    }

    #[test]
    fn test_vector_clock_happens_before() {
        let mut a = VectorClock::new();
        a.increment("us-east");

        let mut b = VectorClock::new();
        b.increment("us-east");
        b.increment("us-east");

        assert!(a.happens_before(&b));
        assert!(!b.happens_before(&a));
    }

    #[test]
    fn test_vector_clock_not_happens_before_equal() {
        let mut a = VectorClock::new();
        a.increment("r1");
        let b = a.clone();

        assert!(!a.happens_before(&b));
        assert!(!b.happens_before(&a));
    }

    #[test]
    fn test_vector_clock_concurrent() {
        let mut a = VectorClock::new();
        a.increment("us-east");

        let mut b = VectorClock::new();
        b.increment("eu-west");

        assert!(a.is_concurrent(&b));
        assert!(b.is_concurrent(&a));
    }

    #[test]
    fn test_vector_clock_not_concurrent_when_ordered() {
        let mut a = VectorClock::new();
        a.increment("r1");

        let mut b = a.clone();
        b.increment("r1");

        assert!(!a.is_concurrent(&b));
    }

    #[test]
    fn test_vector_clock_empty_clocks_not_concurrent() {
        let a = VectorClock::new();
        let b = VectorClock::new();
        assert!(!a.is_concurrent(&b));
    }

    #[test]
    fn test_vector_clock_happens_before_with_extra_region() {
        let mut a = VectorClock::new();
        a.increment("r1");

        let mut b = VectorClock::new();
        b.increment("r1");
        b.increment("r2");

        assert!(a.happens_before(&b));
        assert!(!b.happens_before(&a));
    }

    // -- conflict resolution ------------------------------------------------

    #[test]
    fn test_resolve_conflict_lww_local_wins() {
        let config = GeoConfig {
            conflict_policy: ConflictPolicy::LastWriterWins,
            ..make_config()
        };
        let replicator = GeoReplicator::new(config);

        let later = Utc::now();
        let earlier = later - chrono::Duration::seconds(10);
        let conflicts = vec![make_conflict("local-v", "remote-v", later, earlier)];

        let resolution = replicator.resolve_conflict(&conflicts);
        assert_eq!(resolution.winner, ConflictWinner::Local);
        assert!(resolution.merged_value.is_none());
    }

    #[test]
    fn test_resolve_conflict_lww_remote_wins() {
        let config = GeoConfig {
            conflict_policy: ConflictPolicy::LastWriterWins,
            ..make_config()
        };
        let replicator = GeoReplicator::new(config);

        let later = Utc::now();
        let earlier = later - chrono::Duration::seconds(10);
        let conflicts = vec![make_conflict("local-v", "remote-v", earlier, later)];

        let resolution = replicator.resolve_conflict(&conflicts);
        assert_eq!(resolution.winner, ConflictWinner::Remote);
    }

    #[test]
    fn test_resolve_conflict_max_value_local_wins() {
        let config = GeoConfig {
            conflict_policy: ConflictPolicy::MaxValueWins,
            ..make_config()
        };
        let replicator = GeoReplicator::new(config);

        let now = Utc::now();
        let conflicts = vec![make_conflict("zzz", "aaa", now, now)];

        let resolution = replicator.resolve_conflict(&conflicts);
        assert_eq!(resolution.winner, ConflictWinner::Local);
    }

    #[test]
    fn test_resolve_conflict_max_value_remote_wins() {
        let config = GeoConfig {
            conflict_policy: ConflictPolicy::MaxValueWins,
            ..make_config()
        };
        let replicator = GeoReplicator::new(config);

        let now = Utc::now();
        let conflicts = vec![make_conflict("aaa", "zzz", now, now)];

        let resolution = replicator.resolve_conflict(&conflicts);
        assert_eq!(resolution.winner, ConflictWinner::Remote);
    }

    #[test]
    fn test_resolve_conflict_merge_all() {
        let config = GeoConfig {
            conflict_policy: ConflictPolicy::MergeAll,
            ..make_config()
        };
        let replicator = GeoReplicator::new(config);

        let now = Utc::now();
        let conflicts = vec![make_conflict("left", "right", now, now)];

        let resolution = replicator.resolve_conflict(&conflicts);
        assert_eq!(resolution.winner, ConflictWinner::Merged);
        assert_eq!(resolution.merged_value.as_deref(), Some("left|right"));
    }

    #[test]
    fn test_resolve_conflict_empty() {
        let replicator = GeoReplicator::new(make_config());
        let resolution = replicator.resolve_conflict(&[]);
        assert_eq!(resolution.winner, ConflictWinner::Local);
        assert!(resolution.reason.contains("no conflicts"));
    }

    // -- replication --------------------------------------------------------

    #[tokio::test]
    async fn test_replicate_async_no_regions() {
        let replicator = GeoReplicator::new(make_config());
        let op = make_op("key1", CrdtOpType::CounterIncrement(1), "us-east-1");

        let result = replicator.replicate(op).await.unwrap();
        assert_eq!(result.regions_total, 0);
        assert_eq!(result.status, ReplicationStatus::Committed);
    }

    #[tokio::test]
    async fn test_replicate_async_with_regions() {
        let replicator = GeoReplicator::new(make_config());
        replicator
            .register_region(make_region("eu-west-1"))
            .unwrap();

        let op = make_op("key1", CrdtOpType::RegisterSet("val".into()), "us-east-1");
        let result = replicator.replicate(op).await.unwrap();

        assert_eq!(result.regions_total, 1);
        // Async mode returns Pending since it doesn't wait for acks
        assert_eq!(result.status, ReplicationStatus::Pending);
    }

    #[tokio::test]
    async fn test_replicate_sync_quorum() {
        let config = GeoConfig {
            replication_mode: ReplicationMode::SyncQuorum { min_acks: 1 },
            ..make_config()
        };
        let replicator = GeoReplicator::new(config);
        replicator
            .register_region(make_region("eu-west-1"))
            .unwrap();
        replicator
            .register_region(make_region("ap-south-1"))
            .unwrap();

        let op = make_op("key2", CrdtOpType::SetAdd("member".into()), "us-east-1");
        let result = replicator.replicate(op).await.unwrap();

        assert_eq!(result.regions_total, 2);
        // Both regions are in Syncing state (healthy-equivalent) so quorum is met
        assert_eq!(result.status, ReplicationStatus::Committed);
        assert!(result.regions_acked >= 1);
    }

    #[tokio::test]
    async fn test_replicate_advances_vector_clock() {
        let replicator = GeoReplicator::new(make_config());

        let op = make_op("k", CrdtOpType::CounterIncrement(1), "us-east-1");
        replicator.replicate(op).await.unwrap();

        let clock = replicator.get_vector_clock();
        assert_eq!(clock.get("us-east-1"), 1);

        let op2 = make_op("k", CrdtOpType::CounterIncrement(2), "us-east-1");
        replicator.replicate(op2).await.unwrap();

        let clock = replicator.get_vector_clock();
        assert_eq!(clock.get("us-east-1"), 2);
    }

    // -- sync ---------------------------------------------------------------

    #[tokio::test]
    async fn test_sync_region() {
        let replicator = GeoReplicator::new(make_config());
        let id = replicator
            .register_region(make_region("eu-west-1"))
            .unwrap();

        // Replicate something first so there are pending ops
        let op = make_op("k1", CrdtOpType::CounterIncrement(1), "us-east-1");
        replicator.replicate(op).await.unwrap();

        let result = replicator.sync_region(&id).await.unwrap();
        assert_eq!(result.region_id, id);
        assert!(result.ops_synced >= 1);
        assert!(result.bytes_transferred > 0);

        // Region should now be healthy
        let regions = replicator.list_regions();
        let synced = regions.iter().find(|r| r.info.id == id).unwrap();
        assert_eq!(synced.status, RegionHealth::Healthy);
        assert!(synced.last_sync.is_some());
        assert_eq!(synced.pending_ops, 0);
    }

    #[tokio::test]
    async fn test_sync_unknown_region() {
        let replicator = GeoReplicator::new(make_config());
        let err = replicator.sync_region(&RegionId::new()).await.unwrap_err();
        assert!(matches!(err, GeoError::RegionNotFound));
    }

    // -- stats --------------------------------------------------------------

    #[test]
    fn test_stats_initial() {
        let replicator = GeoReplicator::new(make_config());
        let stats = replicator.get_stats();

        assert_eq!(stats.total_ops_replicated, 0);
        assert_eq!(stats.total_conflicts, 0);
        assert_eq!(stats.total_syncs, 0);
        assert_eq!(stats.regions_healthy, 0);
        assert_eq!(stats.bytes_replicated, 0);
    }

    #[tokio::test]
    async fn test_stats_after_replication() {
        let replicator = GeoReplicator::new(make_config());
        replicator
            .register_region(make_region("eu-west-1"))
            .unwrap();

        let op = make_op("k", CrdtOpType::CounterIncrement(1), "us-east-1");
        replicator.replicate(op).await.unwrap();

        let stats = replicator.get_stats();
        assert_eq!(stats.total_ops_replicated, 1);
    }

    #[tokio::test]
    async fn test_stats_after_sync() {
        let replicator = GeoReplicator::new(make_config());
        let id = replicator
            .register_region(make_region("eu-west-1"))
            .unwrap();

        let op = make_op("k", CrdtOpType::CounterIncrement(1), "us-east-1");
        replicator.replicate(op).await.unwrap();
        replicator.sync_region(&id).await.unwrap();

        let stats = replicator.get_stats();
        assert_eq!(stats.total_syncs, 1);
        assert!(stats.bytes_replicated > 0);
        assert!(stats.regions_healthy >= 1);
    }

    // -- causal ordering verification ---------------------------------------

    #[test]
    fn test_causal_ordering_detects_concurrent_writes() {
        // Two regions write independently → concurrent
        let mut clock_a = VectorClock::new();
        clock_a.increment("us-east");

        let mut clock_b = VectorClock::new();
        clock_b.increment("eu-west");

        assert!(clock_a.is_concurrent(&clock_b));

        // After merging, the merged clock dominates both originals
        let mut merged = clock_a.clone();
        merged.merge(&clock_b);
        merged.increment("us-east");

        assert!(clock_a.happens_before(&merged));
        assert!(clock_b.happens_before(&merged));
    }

    #[test]
    fn test_causal_chain() {
        let mut c1 = VectorClock::new();
        c1.increment("r1");

        let mut c2 = c1.clone();
        c2.increment("r2");

        let mut c3 = c2.clone();
        c3.increment("r1");

        assert!(c1.happens_before(&c2));
        assert!(c2.happens_before(&c3));
        assert!(c1.happens_before(&c3)); // transitivity
    }

    // -- config defaults ----------------------------------------------------

    #[test]
    fn test_geo_config_default() {
        let config = GeoConfig::default();
        assert_eq!(config.local_region, "local");
        assert_eq!(config.replication_mode, ReplicationMode::Async);
        assert_eq!(config.conflict_policy, ConflictPolicy::LastWriterWins);
        assert!(config.enable_causal_ordering);
        assert_eq!(config.max_batch_size, 1000);
    }

    #[test]
    fn test_region_info_new() {
        let region = RegionInfo::new("us-west-2", "https://us-west-2.example.com:6380");
        assert_eq!(region.name, "us-west-2");
        assert_eq!(region.endpoint, "https://us-west-2.example.com:6380");
        assert_eq!(region.priority, 100);
        assert!(region.tags.is_empty());
    }

    #[test]
    fn test_region_id_display() {
        let id = RegionId::new();
        let display = format!("{}", id);
        // UUID v4 string representation
        assert_eq!(display.len(), 36);
    }
}
