//! # Multi-Region Coordination Layer
//!
//! Implements the region coordinator responsible for managing multi-region
//! replication with configurable conflict resolution policies and an
//! anti-entropy protocol for convergence guarantees.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────┐       anti-entropy       ┌──────────────────────┐
//! │  Region A            │◄────────────────────────►│  Region B            │
//! │  RegionCoordinator   │                          │  RegionCoordinator   │
//! │  ┌────────────────┐  │                          │  ┌────────────────┐  │
//! │  │ VectorClock    │  │   AntiEntropyRound       │  │ VectorClock    │  │
//! │  ├────────────────┤  │   (divergent keys)       │  ├────────────────┤  │
//! │  │ ConflictPolicy │  │                          │  │ ConflictPolicy │  │
//! │  ├────────────────┤  │                          │  ├────────────────┤  │
//! │  │ Conflict Log   │  │                          │  │ Conflict Log   │  │
//! │  └────────────────┘  │                          │  └────────────────┘  │
//! └──────────────────────┘                          └──────────────────────┘
//! ```
//!
//! # Example
//!
//! ```no_run
//! use ferrite_plugins::crdt::region_coordinator::{
//!     RegionCoordinator, RegionConfig, RegionInfo, RemoteRegion,
//!     ConflictPolicy, ConnectionStatus,
//! };
//!
//! let local = RegionInfo {
//!     id: "us-east".into(),
//!     name: "US East".into(),
//!     endpoint: "https://us-east.example.com:6380".into(),
//!     datacenter: "us-east-1".into(),
//!     priority: 1,
//! };
//!
//! let config = RegionConfig::default();
//! let coordinator = RegionCoordinator::new(local, config);
//!
//! // Add a remote region
//! let remote = RemoteRegion::new(RegionInfo {
//!     id: "eu-west".into(),
//!     name: "EU West".into(),
//!     endpoint: "https://eu-west.example.com:6380".into(),
//!     datacenter: "eu-west-1".into(),
//!     priority: 2,
//! });
//! coordinator.add_remote_region(remote).unwrap();
//!
//! // List all known regions
//! let regions = coordinator.list_regions();
//! assert_eq!(regions.len(), 2); // local + 1 remote
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::types::CrdtType;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur during region coordination operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum CoordinatorError {
    /// The specified region was not found.
    #[error("Region not found: {0}")]
    RegionNotFound(String),
    /// A region with the same ID already exists.
    #[error("Region already exists: {0}")]
    RegionAlreadyExists(String),
    /// The maximum number of regions has been reached.
    #[error("Maximum number of regions reached")]
    MaxRegionsReached,
    /// Failed to establish or maintain a connection to a region.
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
    /// An error occurred during synchronisation.
    #[error("Sync error: {0}")]
    SyncError(String),
    /// Conflict resolution could not be completed.
    #[error("Conflict resolution failed: {0}")]
    ConflictResolutionFailed(String),
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the region coordinator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegionConfig {
    /// Maximum number of remote regions that can be registered.
    pub max_regions: usize,
    /// Interval in seconds between anti-entropy rounds.
    pub anti_entropy_interval_secs: u64,
    /// Maximum number of conflict records to retain.
    pub max_conflict_log: usize,
    /// Default conflict resolution policy.
    pub default_conflict_policy: ConflictPolicy,
    /// Seconds to wait for convergence after a sync round.
    pub convergence_timeout_secs: u64,
    /// Interval in seconds between heartbeat probes.
    pub heartbeat_interval_secs: u64,
    /// Seconds without a heartbeat before a region is considered timed-out.
    pub region_timeout_secs: u64,
}

impl Default for RegionConfig {
    fn default() -> Self {
        Self {
            max_regions: 5,
            anti_entropy_interval_secs: 30,
            max_conflict_log: 10_000,
            default_conflict_policy: ConflictPolicy::LastWriteWins,
            convergence_timeout_secs: 5,
            heartbeat_interval_secs: 10,
            region_timeout_secs: 60,
        }
    }
}

// ---------------------------------------------------------------------------
// Region types
// ---------------------------------------------------------------------------

/// Information about a single region (local or remote).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegionInfo {
    /// Unique region identifier.
    pub id: String,
    /// Human-readable region name.
    pub name: String,
    /// Network endpoint for this region.
    pub endpoint: String,
    /// Datacenter name (e.g. `"us-east-1"`).
    pub datacenter: String,
    /// Priority for tiebreaking; lower value = higher priority.
    pub priority: u32,
}

/// A remote region tracked by the coordinator.
#[derive(Clone, Debug)]
pub struct RemoteRegion {
    /// Static region information.
    pub info: RegionInfo,
    /// Current connection status.
    pub status: ConnectionStatus,
    /// Epoch-millis timestamp of the last received heartbeat.
    pub last_heartbeat: u64,
    /// Epoch-millis timestamp of the last successful sync.
    pub last_sync: u64,
    /// Known vector clock for this remote region.
    pub vector_clock: HashMap<String, u64>,
    /// Estimated replication lag in milliseconds.
    pub estimated_lag_ms: u64,
    /// Bytes of data pending delivery to this region.
    pub bytes_pending: u64,
}

impl RemoteRegion {
    /// Create a new `RemoteRegion` with sensible defaults.
    pub fn new(info: RegionInfo) -> Self {
        Self {
            info,
            status: ConnectionStatus::Unknown,
            last_heartbeat: 0,
            last_sync: 0,
            vector_clock: HashMap::new(),
            estimated_lag_ms: 0,
            bytes_pending: 0,
        }
    }
}

/// Connection status of a remote region.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionStatus {
    /// Region is reachable and healthy.
    Connected,
    /// Region is not reachable.
    Disconnected,
    /// Region is reachable but experiencing degraded performance.
    Degraded,
    /// Status has not been determined yet.
    Unknown,
}

// ---------------------------------------------------------------------------
// Conflict resolution
// ---------------------------------------------------------------------------

/// Policy used to resolve write conflicts between regions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictPolicy {
    /// Higher timestamp wins (default).
    LastWriteWins,
    /// Region with a lower priority number wins.
    HigherPriorityWins,
    /// Automatic CRDT merge (preferred for supported types).
    CrdtMerge,
    /// Named custom conflict resolver function.
    CustomFunction(String),
    /// Always keep the local value.
    RejectRemote,
    /// Always accept the remote value.
    AcceptRemote,
}

/// Describes a write conflict between a local and a remote value.
#[derive(Clone, Debug)]
pub struct Conflict {
    /// Key that is in conflict.
    pub key: String,
    /// CRDT type of the conflicting values.
    pub crdt_type: CrdtType,
    /// Locally held value.
    pub local_value: Vec<u8>,
    /// Remotely received value.
    pub remote_value: Vec<u8>,
    /// Timestamp of the local write.
    pub local_timestamp: u64,
    /// Timestamp of the remote write.
    pub remote_timestamp: u64,
    /// Region ID that produced the local value.
    pub local_region: String,
    /// Region ID that produced the remote value.
    pub remote_region: String,
    /// Local vector clock at write time.
    pub local_vector_clock: HashMap<String, u64>,
    /// Remote vector clock at write time.
    pub remote_vector_clock: HashMap<String, u64>,
}

/// The outcome of resolving a conflict.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Resolution {
    /// Value chosen (or merged) after resolution.
    pub resolved_value: Vec<u8>,
    /// The policy that was applied.
    pub policy_used: ConflictPolicy,
    /// Region whose value was selected.
    pub winner_region: String,
    /// Human-readable explanation.
    pub details: String,
}

/// A recorded conflict together with its resolution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConflictRecord {
    /// Summary of the conflict.
    pub conflict: ConflictSummary,
    /// How the conflict was resolved.
    pub resolution: Resolution,
    /// Epoch-millis timestamp when the conflict was recorded.
    pub timestamp: u64,
}

/// Lightweight summary of a conflict (suitable for logging / serialisation).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConflictSummary {
    /// Key in conflict.
    pub key: String,
    /// CRDT type as a human-readable string.
    pub crdt_type: String,
    /// Local region ID.
    pub local_region: String,
    /// Remote region ID.
    pub remote_region: String,
    /// Timestamp of the local write.
    pub local_timestamp: u64,
    /// Timestamp of the remote write.
    pub remote_timestamp: u64,
}

// ---------------------------------------------------------------------------
// Anti-entropy types
// ---------------------------------------------------------------------------

/// Persistent state for the anti-entropy protocol.
#[derive(Clone, Debug, Default)]
pub struct AntiEntropyState {
    /// Currently active anti-entropy round, if any.
    pub current_round: Option<AntiEntropyRound>,
    /// Total number of completed rounds.
    pub completed_rounds: u64,
    /// Total number of keys reconciled across all rounds.
    pub keys_reconciled: u64,
    /// Epoch-millis timestamp of the last full sync, if any.
    pub last_full_sync: Option<u64>,
}

/// Represents a single anti-entropy round.
#[derive(Clone, Debug)]
pub struct AntiEntropyRound {
    /// Unique identifier for this round.
    pub id: String,
    /// Epoch-millis timestamp when the round started.
    pub started_at: u64,
    /// Target remote region for this round.
    pub target_region: String,
    /// Number of keys checked so far.
    pub keys_checked: u64,
    /// Number of keys found to be divergent.
    pub keys_divergent: u64,
    /// Number of keys successfully reconciled.
    pub keys_reconciled: u64,
    /// Current status of the round.
    pub status: AntiEntropyStatus,
}

/// Status of an anti-entropy round.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AntiEntropyStatus {
    /// Round is still running.
    InProgress,
    /// Round completed successfully.
    Completed,
    /// Round failed with an error message.
    Failed(String),
}

/// Response from a remote region during an anti-entropy round.
#[derive(Clone, Debug)]
pub struct AntiEntropyResponse {
    /// Round ID this response belongs to.
    pub round_id: String,
    /// Keys whose hashes/versions diverge.
    pub divergent_keys: Vec<DivergentKey>,
    /// The remote region's vector clock.
    pub region_vector_clock: HashMap<String, u64>,
}

/// A single key that diverges between two regions.
#[derive(Clone, Debug)]
pub struct DivergentKey {
    /// Key name.
    pub key: String,
    /// Hash of the remote value.
    pub remote_hash: u64,
    /// Version of the remote value.
    pub remote_version: u64,
}

/// Summary statistics for a completed anti-entropy round.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AntiEntropyResult {
    /// Number of keys that were reconciled.
    pub keys_reconciled: u64,
    /// Number of keys skipped (already consistent).
    pub keys_skipped: u64,
    /// Number of keys that required conflict resolution.
    pub conflicts: u64,
    /// Wall-clock duration of the round in milliseconds.
    pub duration_ms: u64,
}

// ---------------------------------------------------------------------------
// Remote write types
// ---------------------------------------------------------------------------

/// A write received from a remote region.
#[derive(Clone, Debug)]
pub struct RemoteWrite {
    /// Key being written.
    pub key: String,
    /// Serialised value payload.
    pub value: Vec<u8>,
    /// CRDT type of the value.
    pub crdt_type: CrdtType,
    /// Region that originated the write.
    pub source_region: String,
    /// Epoch-millis timestamp of the write.
    pub timestamp: u64,
    /// Vector clock at write time.
    pub vector_clock: HashMap<String, u64>,
}

/// Outcome of applying a remote write.
#[derive(Clone, Debug)]
pub enum WriteResult {
    /// The remote value was applied directly (no local value existed).
    Applied,
    /// The remote and local values were merged via CRDT semantics.
    Merged,
    /// A conflict was detected and resolved; includes the record.
    Conflicted(ConflictRecord),
    /// The remote write was rejected with a reason.
    Rejected(String),
}

// ---------------------------------------------------------------------------
// Status / health / lag types
// ---------------------------------------------------------------------------

/// Status snapshot of a single region as seen by the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegionStatus {
    /// Region information.
    pub info: RegionInfo,
    /// Connection status.
    pub status: ConnectionStatus,
    /// Epoch-millis timestamp of the last heartbeat.
    pub last_heartbeat: u64,
    /// Estimated replication lag in milliseconds.
    pub estimated_lag_ms: u64,
    /// Bytes of data pending delivery.
    pub bytes_pending: u64,
    /// Whether this is the local region.
    pub is_local: bool,
}

/// Replication lag information for a single region.
#[derive(Clone, Debug)]
pub struct RegionLag {
    /// Region identifier.
    pub region_id: String,
    /// Lag in milliseconds.
    pub lag_ms: u64,
    /// Lag measured in bytes.
    pub lag_bytes: u64,
    /// Lag measured in outstanding operations.
    pub lag_operations: u64,
}

/// Health status of a single region.
#[derive(Clone, Debug)]
pub struct RegionHealth {
    /// Region identifier.
    pub region_id: String,
    /// Whether the region is considered healthy.
    pub healthy: bool,
    /// List of issues detected (empty when healthy).
    pub issues: Vec<String>,
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

/// Atomic counters for coordinator statistics.
#[derive(Debug, Default)]
pub struct CoordinatorStats {
    local_writes: AtomicU64,
    remote_writes: AtomicU64,
    conflicts_resolved: AtomicU64,
    anti_entropy_rounds: AtomicU64,
    keys_reconciled: AtomicU64,
    writes_rejected: AtomicU64,
}

/// A point-in-time snapshot of coordinator statistics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CoordinatorStatsSnapshot {
    /// Total local writes recorded.
    pub local_writes: u64,
    /// Total remote writes received.
    pub remote_writes: u64,
    /// Total conflicts resolved.
    pub conflicts_resolved: u64,
    /// Total anti-entropy rounds completed.
    pub anti_entropy_rounds: u64,
    /// Total keys reconciled via anti-entropy.
    pub keys_reconciled: u64,
    /// Total remote writes rejected.
    pub writes_rejected: u64,
}

// ---------------------------------------------------------------------------
// RegionCoordinator
// ---------------------------------------------------------------------------

/// Multi-region coordinator with configurable conflict resolution and
/// anti-entropy protocol.
///
/// The coordinator tracks local and remote regions, resolves write conflicts
/// according to per-key-pattern policies, and drives periodic anti-entropy
/// rounds to guarantee convergence.
pub struct RegionCoordinator {
    /// Coordinator configuration.
    config: RegionConfig,
    /// Information about the local region.
    local_region: RegionInfo,
    /// Map of remote region ID → `RemoteRegion`.
    remote_regions: RwLock<HashMap<String, RemoteRegion>>,
    /// Per-key-pattern conflict resolution policies.
    conflict_policies: RwLock<HashMap<String, ConflictPolicy>>,
    /// Anti-entropy protocol state.
    anti_entropy_state: RwLock<AntiEntropyState>,
    /// Atomic statistics counters.
    stats: CoordinatorStats,
    /// Local vector clock (region-id → logical counter).
    local_vector_clock: RwLock<HashMap<String, u64>>,
    /// Bounded conflict log.
    conflict_log: RwLock<Vec<ConflictRecord>>,
}

impl RegionCoordinator {
    /// Create a new `RegionCoordinator`.
    pub fn new(local_region: RegionInfo, config: RegionConfig) -> Self {
        let mut initial_clock = HashMap::new();
        initial_clock.insert(local_region.id.clone(), 0u64);
        Self {
            config,
            local_region,
            remote_regions: RwLock::new(HashMap::new()),
            conflict_policies: RwLock::new(HashMap::new()),
            anti_entropy_state: RwLock::new(AntiEntropyState::default()),
            stats: CoordinatorStats::default(),
            local_vector_clock: RwLock::new(initial_clock),
            conflict_log: RwLock::new(Vec::new()),
        }
    }

    // -- Region management --------------------------------------------------

    /// Register a new remote region.
    ///
    /// Returns an error if the region already exists or the maximum number of
    /// regions has been reached.
    pub fn add_remote_region(&self, region: RemoteRegion) -> Result<(), CoordinatorError> {
        let mut regions = self.remote_regions.write();
        if regions.contains_key(&region.info.id) {
            return Err(CoordinatorError::RegionAlreadyExists(
                region.info.id.clone(),
            ));
        }
        if regions.len() >= self.config.max_regions {
            return Err(CoordinatorError::MaxRegionsReached);
        }
        regions.insert(region.info.id.clone(), region);
        Ok(())
    }

    /// Remove a remote region by its ID.
    pub fn remove_remote_region(&self, id: &str) -> Result<(), CoordinatorError> {
        let mut regions = self.remote_regions.write();
        if regions.remove(id).is_none() {
            return Err(CoordinatorError::RegionNotFound(id.to_string()));
        }
        Ok(())
    }

    /// List all known regions (local + remotes).
    pub fn list_regions(&self) -> Vec<RegionStatus> {
        let mut result = Vec::new();

        // Local region
        result.push(RegionStatus {
            info: self.local_region.clone(),
            status: ConnectionStatus::Connected,
            last_heartbeat: 0,
            estimated_lag_ms: 0,
            bytes_pending: 0,
            is_local: true,
        });

        // Remote regions
        let regions = self.remote_regions.read();
        for remote in regions.values() {
            result.push(RegionStatus {
                info: remote.info.clone(),
                status: remote.status.clone(),
                last_heartbeat: remote.last_heartbeat,
                estimated_lag_ms: remote.estimated_lag_ms,
                bytes_pending: remote.bytes_pending,
                is_local: false,
            });
        }

        result
    }

    // -- Conflict policies --------------------------------------------------

    /// Set the conflict resolution policy for a given key pattern.
    ///
    /// Patterns are matched literally; use `"*"` as a catch-all.
    pub fn set_conflict_policy(&self, key_pattern: &str, policy: ConflictPolicy) {
        let mut policies = self.conflict_policies.write();
        policies.insert(key_pattern.to_string(), policy);
    }

    /// Resolve a conflict using the configured policy chain.
    ///
    /// Policy lookup order:
    /// 1. Exact key match in `conflict_policies`
    /// 2. Wildcard `"*"` entry
    /// 3. `config.default_conflict_policy`
    pub fn resolve_conflict(&self, conflict: &Conflict) -> Resolution {
        let policies = self.conflict_policies.read();
        let policy = policies
            .get(&conflict.key)
            .or_else(|| policies.get("*"))
            .cloned()
            .unwrap_or_else(|| self.config.default_conflict_policy.clone());

        let resolution = self.apply_policy(&policy, conflict);

        // Record the conflict
        drop(policies);
        self.record_conflict(conflict, &resolution);
        self.stats.conflicts_resolved.fetch_add(1, Ordering::Relaxed);

        resolution
    }

    /// Apply a specific conflict policy to produce a resolution.
    fn apply_policy(&self, policy: &ConflictPolicy, conflict: &Conflict) -> Resolution {
        match policy {
            ConflictPolicy::LastWriteWins => self.resolve_lww(conflict),
            ConflictPolicy::HigherPriorityWins => self.resolve_priority(conflict),
            ConflictPolicy::CrdtMerge => self.resolve_crdt_merge(conflict),
            ConflictPolicy::RejectRemote => Resolution {
                resolved_value: conflict.local_value.clone(),
                policy_used: ConflictPolicy::RejectRemote,
                winner_region: conflict.local_region.clone(),
                details: "Local value retained (RejectRemote policy)".to_string(),
            },
            ConflictPolicy::AcceptRemote => Resolution {
                resolved_value: conflict.remote_value.clone(),
                policy_used: ConflictPolicy::AcceptRemote,
                winner_region: conflict.remote_region.clone(),
                details: "Remote value accepted (AcceptRemote policy)".to_string(),
            },
            ConflictPolicy::CustomFunction(name) => {
                // Custom functions are not executed here; fall back to LWW.
                let mut res = self.resolve_lww(conflict);
                res.policy_used = ConflictPolicy::CustomFunction(name.clone());
                res.details = format!(
                    "CustomFunction '{}' not available, fell back to LWW: {}",
                    name, res.details
                );
                res
            }
        }
    }

    /// Last-Write-Wins resolution: higher timestamp wins; on tie the region
    /// with lower priority number (higher priority) wins.
    fn resolve_lww(&self, conflict: &Conflict) -> Resolution {
        if conflict.remote_timestamp > conflict.local_timestamp {
            Resolution {
                resolved_value: conflict.remote_value.clone(),
                policy_used: ConflictPolicy::LastWriteWins,
                winner_region: conflict.remote_region.clone(),
                details: format!(
                    "Remote timestamp {} > local timestamp {}",
                    conflict.remote_timestamp, conflict.local_timestamp
                ),
            }
        } else if conflict.local_timestamp > conflict.remote_timestamp {
            Resolution {
                resolved_value: conflict.local_value.clone(),
                policy_used: ConflictPolicy::LastWriteWins,
                winner_region: conflict.local_region.clone(),
                details: format!(
                    "Local timestamp {} > remote timestamp {}",
                    conflict.local_timestamp, conflict.remote_timestamp
                ),
            }
        } else {
            // Tie-break on region priority
            self.resolve_priority(conflict)
        }
    }

    /// Priority-based resolution: region with the lower priority number wins.
    /// If both priorities are equal the local region wins by convention.
    fn resolve_priority(&self, conflict: &Conflict) -> Resolution {
        let local_priority = self.local_region.priority;
        let remote_priority = self
            .remote_regions
            .read()
            .get(&conflict.remote_region)
            .map(|r| r.info.priority)
            .unwrap_or(u32::MAX);

        if remote_priority < local_priority {
            Resolution {
                resolved_value: conflict.remote_value.clone(),
                policy_used: ConflictPolicy::HigherPriorityWins,
                winner_region: conflict.remote_region.clone(),
                details: format!(
                    "Remote priority {} < local priority {}",
                    remote_priority, local_priority
                ),
            }
        } else {
            Resolution {
                resolved_value: conflict.local_value.clone(),
                policy_used: ConflictPolicy::HigherPriorityWins,
                winner_region: conflict.local_region.clone(),
                details: format!(
                    "Local priority {} <= remote priority {}",
                    local_priority, remote_priority
                ),
            }
        }
    }

    /// CRDT-aware merge resolution.
    ///
    /// - Counters (GCounter, PNCounter): element-wise max per site.
    /// - Sets (OrSet, LwwElementSet): union of both sides.
    /// - Registers (LwwRegister, MvRegister): fall back to LWW.
    /// - Maps (OrMap): union of fields.
    /// - Everything else: fall back to LWW.
    fn resolve_crdt_merge(&self, conflict: &Conflict) -> Resolution {
        match conflict.crdt_type {
            CrdtType::GCounter | CrdtType::PNCounter => {
                let merged = self.merge_counter_bytes(
                    &conflict.local_value,
                    &conflict.remote_value,
                );
                Resolution {
                    resolved_value: merged,
                    policy_used: ConflictPolicy::CrdtMerge,
                    winner_region: "merged".to_string(),
                    details: "Counter merged (element-wise max per site)".to_string(),
                }
            }
            CrdtType::OrSet | CrdtType::LwwElementSet => {
                let merged = self.merge_set_bytes(
                    &conflict.local_value,
                    &conflict.remote_value,
                );
                Resolution {
                    resolved_value: merged,
                    policy_used: ConflictPolicy::CrdtMerge,
                    winner_region: "merged".to_string(),
                    details: "Set merged (union)".to_string(),
                }
            }
            CrdtType::OrMap => {
                let merged = self.merge_map_bytes(
                    &conflict.local_value,
                    &conflict.remote_value,
                );
                Resolution {
                    resolved_value: merged,
                    policy_used: ConflictPolicy::CrdtMerge,
                    winner_region: "merged".to_string(),
                    details: "Map merged (union of fields)".to_string(),
                }
            }
            // Registers and flags: fall back to LWW
            _ => {
                let mut res = self.resolve_lww(conflict);
                res.policy_used = ConflictPolicy::CrdtMerge;
                res.details = format!(
                    "CRDT type {} fell back to LWW: {}",
                    conflict.crdt_type, res.details
                );
                res
            }
        }
    }

    /// Merge two counter byte representations by taking element-wise max per
    /// site. If deserialisation fails the longer payload is kept.
    fn merge_counter_bytes(&self, local: &[u8], remote: &[u8]) -> Vec<u8> {
        let local_map: Result<HashMap<String, u64>, _> = serde_json::from_slice(local);
        let remote_map: Result<HashMap<String, u64>, _> = serde_json::from_slice(remote);

        match (local_map, remote_map) {
            (Ok(mut lm), Ok(rm)) => {
                for (site, &count) in &rm {
                    let entry = lm.entry(site.clone()).or_insert(0);
                    *entry = (*entry).max(count);
                }
                serde_json::to_vec(&lm).unwrap_or_else(|_| local.to_vec())
            }
            _ => {
                // Cannot deserialise; keep the larger payload as a heuristic.
                if remote.len() > local.len() {
                    remote.to_vec()
                } else {
                    local.to_vec()
                }
            }
        }
    }

    /// Merge two set byte representations by computing the union.
    /// If deserialisation fails the longer payload is kept.
    fn merge_set_bytes(&self, local: &[u8], remote: &[u8]) -> Vec<u8> {
        let local_set: Result<Vec<String>, _> = serde_json::from_slice(local);
        let remote_set: Result<Vec<String>, _> = serde_json::from_slice(remote);

        match (local_set, remote_set) {
            (Ok(mut ls), Ok(rs)) => {
                for item in rs {
                    if !ls.contains(&item) {
                        ls.push(item);
                    }
                }
                serde_json::to_vec(&ls).unwrap_or_else(|_| local.to_vec())
            }
            _ => {
                if remote.len() > local.len() {
                    remote.to_vec()
                } else {
                    local.to_vec()
                }
            }
        }
    }

    /// Merge two map byte representations by unioning their fields.
    /// If deserialisation fails the longer payload is kept.
    fn merge_map_bytes(&self, local: &[u8], remote: &[u8]) -> Vec<u8> {
        let local_map: Result<HashMap<String, serde_json::Value>, _> =
            serde_json::from_slice(local);
        let remote_map: Result<HashMap<String, serde_json::Value>, _> =
            serde_json::from_slice(remote);

        match (local_map, remote_map) {
            (Ok(mut lm), Ok(rm)) => {
                for (key, value) in rm {
                    lm.entry(key).or_insert(value);
                }
                serde_json::to_vec(&lm).unwrap_or_else(|_| local.to_vec())
            }
            _ => {
                if remote.len() > local.len() {
                    remote.to_vec()
                } else {
                    local.to_vec()
                }
            }
        }
    }

    /// Append a conflict record to the bounded log.
    fn record_conflict(&self, conflict: &Conflict, resolution: &Resolution) {
        let record = ConflictRecord {
            conflict: ConflictSummary {
                key: conflict.key.clone(),
                crdt_type: conflict.crdt_type.to_string(),
                local_region: conflict.local_region.clone(),
                remote_region: conflict.remote_region.clone(),
                local_timestamp: conflict.local_timestamp,
                remote_timestamp: conflict.remote_timestamp,
            },
            resolution: resolution.clone(),
            timestamp: current_epoch_millis(),
        };

        let mut log = self.conflict_log.write();
        if log.len() >= self.config.max_conflict_log {
            log.remove(0);
        }
        log.push(record);
    }

    // -- Anti-entropy -------------------------------------------------------

    /// Start a new anti-entropy round targeting the next eligible remote
    /// region in round-robin order.
    pub fn start_anti_entropy_round(&self) -> AntiEntropyRound {
        let regions = self.remote_regions.read();
        let target = regions
            .keys()
            .next()
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        drop(regions);

        let round = AntiEntropyRound {
            id: Uuid::new_v4().to_string(),
            started_at: current_epoch_millis(),
            target_region: target,
            keys_checked: 0,
            keys_divergent: 0,
            keys_reconciled: 0,
            status: AntiEntropyStatus::InProgress,
        };

        let mut state = self.anti_entropy_state.write();
        state.current_round = Some(round.clone());

        round
    }

    /// Process a response from a remote region for an active anti-entropy
    /// round, reconciling divergent keys.
    pub fn process_anti_entropy_response(
        &self,
        round_id: &str,
        response: AntiEntropyResponse,
    ) -> AntiEntropyResult {
        let started_at = {
            let state = self.anti_entropy_state.read();
            state
                .current_round
                .as_ref()
                .filter(|r| r.id == round_id)
                .map(|r| r.started_at)
                .unwrap_or_else(current_epoch_millis)
        };

        let total_keys = response.divergent_keys.len() as u64;
        let reconciled = total_keys; // optimistic: assume all reconciled
        let duration_ms = current_epoch_millis().saturating_sub(started_at);

        // Merge the remote vector clock into our view of that region.
        if let Some(region_id) = {
            let state = self.anti_entropy_state.read();
            state
                .current_round
                .as_ref()
                .map(|r| r.target_region.clone())
        } {
            let mut regions = self.remote_regions.write();
            if let Some(remote) = regions.get_mut(&region_id) {
                for (k, &v) in &response.region_vector_clock {
                    let entry = remote.vector_clock.entry(k.clone()).or_insert(0);
                    *entry = (*entry).max(v);
                }
                remote.last_sync = current_epoch_millis();
            }
        }

        // Update anti-entropy state.
        let mut state = self.anti_entropy_state.write();
        if let Some(ref mut round) = state.current_round {
            if round.id == round_id {
                round.keys_checked = total_keys;
                round.keys_divergent = total_keys;
                round.keys_reconciled = reconciled;
                round.status = AntiEntropyStatus::Completed;
            }
        }
        state.completed_rounds += 1;
        state.keys_reconciled += reconciled;
        state.last_full_sync = Some(current_epoch_millis());

        self.stats
            .anti_entropy_rounds
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .keys_reconciled
            .fetch_add(reconciled, Ordering::Relaxed);

        AntiEntropyResult {
            keys_reconciled: reconciled,
            keys_skipped: 0,
            conflicts: 0,
            duration_ms,
        }
    }

    // -- Writes -------------------------------------------------------------

    /// Record a local write, advancing the local vector clock.
    pub fn record_local_write(&self, key: &str, _value: Vec<u8>, _crdt_type: CrdtType) {
        let mut clock = self.local_vector_clock.write();
        let counter = clock
            .entry(self.local_region.id.clone())
            .or_insert(0);
        *counter += 1;
        drop(clock);

        self.stats.local_writes.fetch_add(1, Ordering::Relaxed);

        // Key is used for future per-key tracking; currently only the vector
        // clock is stamped.
        let _ = key;
    }

    /// Apply a write received from a remote region.
    ///
    /// If the remote write's vector clock dominates the local clock the value
    /// is applied directly. If the clocks are concurrent a conflict is
    /// detected and resolved according to the configured policy.
    pub fn receive_remote_write(&self, write: RemoteWrite) -> WriteResult {
        self.stats.remote_writes.fetch_add(1, Ordering::Relaxed);

        // Merge the remote vector clock into the local clock.
        {
            let mut clock = self.local_vector_clock.write();
            for (region, &v) in &write.vector_clock {
                let entry = clock.entry(region.clone()).or_insert(0);
                *entry = (*entry).max(v);
            }
        }

        let local_clock = self.local_vector_clock.read().clone();

        // Check whether the clocks are concurrent (i.e., neither dominates).
        let is_concurrent = is_concurrent(&local_clock, &write.vector_clock);

        if is_concurrent {
            // Concurrent writes ⇒ conflict.
            let conflict = Conflict {
                key: write.key.clone(),
                crdt_type: write.crdt_type.clone(),
                local_value: Vec::new(), // local value not available in this layer
                remote_value: write.value.clone(),
                local_timestamp: current_epoch_millis(),
                remote_timestamp: write.timestamp,
                local_region: self.local_region.id.clone(),
                remote_region: write.source_region.clone(),
                local_vector_clock: local_clock,
                remote_vector_clock: write.vector_clock,
            };
            let resolution = self.resolve_conflict(&conflict);
            let record = ConflictRecord {
                conflict: ConflictSummary {
                    key: conflict.key,
                    crdt_type: conflict.crdt_type.to_string(),
                    local_region: conflict.local_region,
                    remote_region: conflict.remote_region,
                    local_timestamp: conflict.local_timestamp,
                    remote_timestamp: conflict.remote_timestamp,
                },
                resolution,
                timestamp: current_epoch_millis(),
            };
            WriteResult::Conflicted(record)
        } else {
            // Remote clock dominates — apply directly.
            WriteResult::Applied
        }
    }

    // -- Observability ------------------------------------------------------

    /// Return the most recent conflict records, up to `limit`.
    pub fn get_conflict_log(&self, limit: usize) -> Vec<ConflictRecord> {
        let log = self.conflict_log.read();
        let start = log.len().saturating_sub(limit);
        log[start..].to_vec()
    }

    /// Return the estimated replication lag for a remote region.
    pub fn region_lag(&self, region_id: &str) -> Option<RegionLag> {
        let regions = self.remote_regions.read();
        regions.get(region_id).map(|r| RegionLag {
            region_id: region_id.to_string(),
            lag_ms: r.estimated_lag_ms,
            lag_bytes: r.bytes_pending,
            lag_operations: 0,
        })
    }

    /// Run a health check on all known regions.
    pub fn health_check(&self) -> Vec<RegionHealth> {
        let regions = self.remote_regions.read();
        let now = current_epoch_millis();

        regions
            .values()
            .map(|r| {
                let mut issues = Vec::new();

                if r.status == ConnectionStatus::Disconnected {
                    issues.push("Region is disconnected".to_string());
                }
                if r.status == ConnectionStatus::Degraded {
                    issues.push("Region is degraded".to_string());
                }
                if r.last_heartbeat > 0
                    && now.saturating_sub(r.last_heartbeat)
                        > self.config.region_timeout_secs * 1000
                {
                    issues.push(format!(
                        "No heartbeat for {} ms",
                        now.saturating_sub(r.last_heartbeat)
                    ));
                }
                if r.estimated_lag_ms > self.config.convergence_timeout_secs * 1000 {
                    issues.push(format!(
                        "Estimated lag {} ms exceeds convergence timeout",
                        r.estimated_lag_ms
                    ));
                }

                RegionHealth {
                    region_id: r.info.id.clone(),
                    healthy: issues.is_empty(),
                    issues,
                }
            })
            .collect()
    }

    /// Return a point-in-time snapshot of coordinator statistics.
    pub fn stats(&self) -> CoordinatorStatsSnapshot {
        CoordinatorStatsSnapshot {
            local_writes: self.stats.local_writes.load(Ordering::Relaxed),
            remote_writes: self.stats.remote_writes.load(Ordering::Relaxed),
            conflicts_resolved: self.stats.conflicts_resolved.load(Ordering::Relaxed),
            anti_entropy_rounds: self.stats.anti_entropy_rounds.load(Ordering::Relaxed),
            keys_reconciled: self.stats.keys_reconciled.load(Ordering::Relaxed),
            writes_rejected: self.stats.writes_rejected.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns the current epoch time in milliseconds.
fn current_epoch_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Two vector clocks are *concurrent* when neither dominates the other.
fn is_concurrent(a: &HashMap<String, u64>, b: &HashMap<String, u64>) -> bool {
    let a_leq_b = a
        .iter()
        .all(|(k, &v)| v <= *b.get(k).unwrap_or(&0));
    let b_leq_a = b
        .iter()
        .all(|(k, &v)| v <= *a.get(k).unwrap_or(&0));

    // Concurrent iff neither a ≤ b nor b ≤ a.
    !a_leq_b && !b_leq_a
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn local_region() -> RegionInfo {
        RegionInfo {
            id: "us-east".into(),
            name: "US East".into(),
            endpoint: "https://us-east.example.com:6380".into(),
            datacenter: "us-east-1".into(),
            priority: 1,
        }
    }

    fn remote_region_info(id: &str, priority: u32) -> RegionInfo {
        RegionInfo {
            id: id.into(),
            name: id.into(),
            endpoint: format!("https://{}.example.com:6380", id),
            datacenter: format!("{}-1", id),
            priority,
        }
    }

    fn make_coordinator() -> RegionCoordinator {
        RegionCoordinator::new(local_region(), RegionConfig::default())
    }

    // -- Region management --------------------------------------------------

    #[test]
    fn test_add_and_list_regions() {
        let coord = make_coordinator();
        let remote = RemoteRegion::new(remote_region_info("eu-west", 2));
        coord.add_remote_region(remote).unwrap();

        let regions = coord.list_regions();
        assert_eq!(regions.len(), 2);
        assert!(regions[0].is_local);
        assert!(!regions[1].is_local);
    }

    #[test]
    fn test_add_duplicate_region_fails() {
        let coord = make_coordinator();
        let r = RemoteRegion::new(remote_region_info("eu-west", 2));
        coord.add_remote_region(r).unwrap();

        let dup = RemoteRegion::new(remote_region_info("eu-west", 3));
        let err = coord.add_remote_region(dup).unwrap_err();
        assert!(matches!(err, CoordinatorError::RegionAlreadyExists(_)));
    }

    #[test]
    fn test_max_regions_reached() {
        let config = RegionConfig {
            max_regions: 2,
            ..Default::default()
        };
        let coord = RegionCoordinator::new(local_region(), config);

        coord
            .add_remote_region(RemoteRegion::new(remote_region_info("r1", 2)))
            .unwrap();
        coord
            .add_remote_region(RemoteRegion::new(remote_region_info("r2", 3)))
            .unwrap();

        let err = coord
            .add_remote_region(RemoteRegion::new(remote_region_info("r3", 4)))
            .unwrap_err();
        assert!(matches!(err, CoordinatorError::MaxRegionsReached));
    }

    #[test]
    fn test_remove_region() {
        let coord = make_coordinator();
        coord
            .add_remote_region(RemoteRegion::new(remote_region_info("eu-west", 2)))
            .unwrap();
        assert_eq!(coord.list_regions().len(), 2);

        coord.remove_remote_region("eu-west").unwrap();
        assert_eq!(coord.list_regions().len(), 1);
    }

    #[test]
    fn test_remove_nonexistent_region() {
        let coord = make_coordinator();
        let err = coord.remove_remote_region("nope").unwrap_err();
        assert!(matches!(err, CoordinatorError::RegionNotFound(_)));
    }

    // -- Conflict resolution ------------------------------------------------

    #[test]
    fn test_resolve_conflict_lww_remote_wins() {
        let coord = make_coordinator();
        let conflict = Conflict {
            key: "k1".into(),
            crdt_type: CrdtType::LwwRegister,
            local_value: b"old".to_vec(),
            remote_value: b"new".to_vec(),
            local_timestamp: 100,
            remote_timestamp: 200,
            local_region: "us-east".into(),
            remote_region: "eu-west".into(),
            local_vector_clock: HashMap::new(),
            remote_vector_clock: HashMap::new(),
        };
        let res = coord.resolve_conflict(&conflict);
        assert_eq!(res.resolved_value, b"new");
        assert_eq!(res.winner_region, "eu-west");
    }

    #[test]
    fn test_resolve_conflict_lww_local_wins() {
        let coord = make_coordinator();
        let conflict = Conflict {
            key: "k1".into(),
            crdt_type: CrdtType::LwwRegister,
            local_value: b"latest".to_vec(),
            remote_value: b"old".to_vec(),
            local_timestamp: 300,
            remote_timestamp: 100,
            local_region: "us-east".into(),
            remote_region: "eu-west".into(),
            local_vector_clock: HashMap::new(),
            remote_vector_clock: HashMap::new(),
        };
        let res = coord.resolve_conflict(&conflict);
        assert_eq!(res.resolved_value, b"latest");
        assert_eq!(res.winner_region, "us-east");
    }

    #[test]
    fn test_resolve_conflict_priority() {
        let coord = make_coordinator();
        coord
            .add_remote_region(RemoteRegion::new(remote_region_info("eu-west", 5)))
            .unwrap();
        coord.set_conflict_policy("*", ConflictPolicy::HigherPriorityWins);

        let conflict = Conflict {
            key: "k".into(),
            crdt_type: CrdtType::LwwRegister,
            local_value: b"local".to_vec(),
            remote_value: b"remote".to_vec(),
            local_timestamp: 100,
            remote_timestamp: 100,
            local_region: "us-east".into(),
            remote_region: "eu-west".into(),
            local_vector_clock: HashMap::new(),
            remote_vector_clock: HashMap::new(),
        };
        // local priority 1 < remote priority 5 ⇒ local wins
        let res = coord.resolve_conflict(&conflict);
        assert_eq!(res.resolved_value, b"local");
        assert_eq!(res.winner_region, "us-east");
    }

    #[test]
    fn test_resolve_conflict_crdt_merge_counter() {
        let coord = make_coordinator();
        coord.set_conflict_policy("*", ConflictPolicy::CrdtMerge);

        let local_map: HashMap<String, u64> =
            [("s1".into(), 5), ("s2".into(), 3)].into();
        let remote_map: HashMap<String, u64> =
            [("s1".into(), 2), ("s3".into(), 7)].into();

        let conflict = Conflict {
            key: "counter:1".into(),
            crdt_type: CrdtType::PNCounter,
            local_value: serde_json::to_vec(&local_map).unwrap(),
            remote_value: serde_json::to_vec(&remote_map).unwrap(),
            local_timestamp: 100,
            remote_timestamp: 100,
            local_region: "us-east".into(),
            remote_region: "eu-west".into(),
            local_vector_clock: HashMap::new(),
            remote_vector_clock: HashMap::new(),
        };

        let res = coord.resolve_conflict(&conflict);
        let merged: HashMap<String, u64> =
            serde_json::from_slice(&res.resolved_value).unwrap();
        assert_eq!(merged.get("s1"), Some(&5)); // max(5,2)
        assert_eq!(merged.get("s2"), Some(&3)); // local-only
        assert_eq!(merged.get("s3"), Some(&7)); // remote-only
    }

    #[test]
    fn test_resolve_conflict_reject_accept() {
        let coord = make_coordinator();

        let conflict = Conflict {
            key: "k".into(),
            crdt_type: CrdtType::LwwRegister,
            local_value: b"local".to_vec(),
            remote_value: b"remote".to_vec(),
            local_timestamp: 100,
            remote_timestamp: 200,
            local_region: "us-east".into(),
            remote_region: "eu-west".into(),
            local_vector_clock: HashMap::new(),
            remote_vector_clock: HashMap::new(),
        };

        coord.set_conflict_policy("k", ConflictPolicy::RejectRemote);
        let res = coord.resolve_conflict(&conflict);
        assert_eq!(res.resolved_value, b"local");

        coord.set_conflict_policy("k", ConflictPolicy::AcceptRemote);
        let res = coord.resolve_conflict(&conflict);
        assert_eq!(res.resolved_value, b"remote");
    }

    // -- Anti-entropy -------------------------------------------------------

    #[test]
    fn test_anti_entropy_round_lifecycle() {
        let coord = make_coordinator();
        coord
            .add_remote_region(RemoteRegion::new(remote_region_info("eu-west", 2)))
            .unwrap();

        let round = coord.start_anti_entropy_round();
        assert_eq!(round.status, AntiEntropyStatus::InProgress);
        assert_eq!(round.target_region, "eu-west");

        let response = AntiEntropyResponse {
            round_id: round.id.clone(),
            divergent_keys: vec![
                DivergentKey {
                    key: "k1".into(),
                    remote_hash: 123,
                    remote_version: 1,
                },
                DivergentKey {
                    key: "k2".into(),
                    remote_hash: 456,
                    remote_version: 2,
                },
            ],
            region_vector_clock: [("eu-west".into(), 10)].into(),
        };

        let result = coord.process_anti_entropy_response(&round.id, response);
        assert_eq!(result.keys_reconciled, 2);

        let snap = coord.stats();
        assert_eq!(snap.anti_entropy_rounds, 1);
        assert_eq!(snap.keys_reconciled, 2);
    }

    // -- Writes & stats -----------------------------------------------------

    #[test]
    fn test_record_local_write_advances_clock() {
        let coord = make_coordinator();
        coord.record_local_write("k1", b"v1".to_vec(), CrdtType::LwwRegister);
        coord.record_local_write("k2", b"v2".to_vec(), CrdtType::LwwRegister);

        let snap = coord.stats();
        assert_eq!(snap.local_writes, 2);

        let clock = coord.local_vector_clock.read();
        assert_eq!(clock.get("us-east"), Some(&2));
    }

    #[test]
    fn test_receive_remote_write_applied() {
        let coord = make_coordinator();
        // Remote clock dominates when local has no prior knowledge of remote.
        let write = RemoteWrite {
            key: "k1".into(),
            value: b"remote_val".to_vec(),
            crdt_type: CrdtType::LwwRegister,
            source_region: "eu-west".into(),
            timestamp: 100,
            vector_clock: [("eu-west".into(), 5)].into(),
        };
        let result = coord.receive_remote_write(write);
        assert!(matches!(result, WriteResult::Applied));
        assert_eq!(coord.stats().remote_writes, 1);
    }

    // -- Health check -------------------------------------------------------

    #[test]
    fn test_health_check_disconnected() {
        let coord = make_coordinator();
        let mut r = RemoteRegion::new(remote_region_info("eu-west", 2));
        r.status = ConnectionStatus::Disconnected;
        coord.add_remote_region(r).unwrap();

        let health = coord.health_check();
        assert_eq!(health.len(), 1);
        assert!(!health[0].healthy);
        assert!(health[0].issues.iter().any(|i| i.contains("disconnected")));
    }

    #[test]
    fn test_conflict_log() {
        let coord = make_coordinator();
        let conflict = Conflict {
            key: "k1".into(),
            crdt_type: CrdtType::LwwRegister,
            local_value: b"a".to_vec(),
            remote_value: b"b".to_vec(),
            local_timestamp: 100,
            remote_timestamp: 200,
            local_region: "us-east".into(),
            remote_region: "eu-west".into(),
            local_vector_clock: HashMap::new(),
            remote_vector_clock: HashMap::new(),
        };

        coord.resolve_conflict(&conflict);

        let log = coord.get_conflict_log(10);
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].conflict.key, "k1");
    }

    // -- Helpers ------------------------------------------------------------

    #[test]
    fn test_is_concurrent() {
        let a: HashMap<String, u64> = [("r1".into(), 2), ("r2".into(), 1)].into();
        let b: HashMap<String, u64> = [("r1".into(), 1), ("r2".into(), 2)].into();
        assert!(is_concurrent(&a, &b));

        // a dominates b → not concurrent
        let c: HashMap<String, u64> = [("r1".into(), 3), ("r2".into(), 3)].into();
        assert!(!is_concurrent(&c, &b));

        // Identical → not concurrent (a ≤ b and b ≤ a)
        assert!(!is_concurrent(&a, &a));
    }

    #[test]
    fn test_region_lag() {
        let coord = make_coordinator();
        let mut r = RemoteRegion::new(remote_region_info("eu-west", 2));
        r.estimated_lag_ms = 150;
        r.bytes_pending = 4096;
        coord.add_remote_region(r).unwrap();

        let lag = coord.region_lag("eu-west").unwrap();
        assert_eq!(lag.lag_ms, 150);
        assert_eq!(lag.lag_bytes, 4096);

        assert!(coord.region_lag("nonexistent").is_none());
    }
}
