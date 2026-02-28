//! CRDT Replication Bridge
//!
//! Bridges the CRDT [`RegionCoordinator`] with the replication stream for
//! multi-region write propagation. Every local mutation is captured as a
//! [`ReplicatedWrite`] and queued for outgoing delivery, while incoming remote
//! writes are validated via vector-clock causality checks and resolved
//! according to a configurable [`ConflictResolutionPolicy`].
//!
//! [`RegionCoordinator`]: ferrite_plugins::crdt::region_coordinator::RegionCoordinator

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// ConflictResolutionPolicy
// ---------------------------------------------------------------------------

/// Policy used to resolve concurrent write conflicts between regions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictResolutionPolicy {
    /// Higher timestamp wins (default).
    LastWriteWins,
    /// Always keep the local value.
    LocalWins,
    /// Always accept the remote value.
    RemoteWins,
    /// Automatic CRDT merge.
    CrdtMerge,
    /// Named custom resolver (falls back to [`LastWriteWins`]).
    Custom(String),
}

impl Default for ConflictResolutionPolicy {
    fn default() -> Self {
        Self::LastWriteWins
    }
}

// ---------------------------------------------------------------------------
// WriteDecision
// ---------------------------------------------------------------------------

/// The outcome of evaluating a remote write against local state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteDecision {
    /// Accept the remote write as-is.
    Apply,
    /// Reject the remote write and keep local state.
    Reject {
        /// Human-readable reason for the rejection.
        reason: String,
    },
    /// Accept a CRDT-merged value.
    Merge {
        /// The merged value to store.
        merged_value: Vec<u8>,
    },
    /// A conflict was detected; includes both values and the policy applied.
    Conflict {
        /// The local value at the time of conflict.
        local: Vec<u8>,
        /// The remote value that caused the conflict.
        remote: Vec<u8>,
        /// The policy that was used to resolve the conflict.
        resolution: ConflictResolutionPolicy,
    },
    /// Defer resolution (queue for later).
    Defer,
}

// ---------------------------------------------------------------------------
// ReplicatedWrite / IncomingWrite
// ---------------------------------------------------------------------------

/// A local write captured for outgoing replication to peer regions.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicatedWrite {
    /// Key that was written.
    pub key: String,
    /// Serialised value.
    pub value: Vec<u8>,
    /// Redis command that triggered the write (e.g. `SET`, `HSET`).
    pub command: String,
    /// Region that originated the write.
    pub source_region: String,
    /// Wall-clock timestamp (epoch millis).
    pub timestamp: u64,
    /// Monotonic per-bridge sequence number.
    pub sequence: u64,
    /// Vector clock snapshot at write time.
    pub vector_clock: HashMap<String, u64>,
}

/// A write received from a remote region.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IncomingWrite {
    /// Key that was written.
    pub key: String,
    /// Serialised value.
    pub value: Vec<u8>,
    /// Redis command that triggered the write.
    pub command: String,
    /// Region that originated the write.
    pub source_region: String,
    /// Wall-clock timestamp (epoch millis).
    pub timestamp: u64,
    /// Sequence number from the source bridge.
    pub sequence: u64,
    /// Vector clock snapshot at write time.
    pub vector_clock: HashMap<String, u64>,
}

// ---------------------------------------------------------------------------
// ConflictEvent
// ---------------------------------------------------------------------------

/// A recorded conflict event for observability and auditing.
#[derive(Clone, Debug)]
pub struct ConflictEvent {
    /// Key that was in conflict.
    pub key: String,
    /// Value held locally at conflict time.
    pub local_value: Vec<u8>,
    /// Value received from the remote region.
    pub remote_value: Vec<u8>,
    /// Local region identifier.
    pub local_region: String,
    /// Remote region identifier.
    pub remote_region: String,
    /// Timestamp of the local write.
    pub local_timestamp: u64,
    /// Timestamp of the remote write.
    pub remote_timestamp: u64,
    /// The decision that was made.
    pub resolution: WriteDecision,
    /// The policy that was applied.
    pub policy_used: ConflictResolutionPolicy,
    /// Epoch-millis timestamp when the conflict was recorded.
    pub recorded_at: u64,
}

// ---------------------------------------------------------------------------
// BridgeStats / BridgeStatsSnapshot
// ---------------------------------------------------------------------------

/// Atomic counters tracking bridge activity.
#[derive(Debug, Default)]
pub struct BridgeStats {
    /// Number of local writes captured.
    pub local_writes: AtomicU64,
    /// Number of remote writes received.
    pub remote_writes_received: AtomicU64,
    /// Number of remote writes applied.
    pub writes_applied: AtomicU64,
    /// Number of remote writes rejected.
    pub writes_rejected: AtomicU64,
    /// Number of remote writes that resulted in a CRDT merge.
    pub writes_merged: AtomicU64,
    /// Total conflicts detected.
    pub conflicts_total: AtomicU64,
    /// Total conflicts resolved.
    pub conflicts_resolved: AtomicU64,
    /// Total bytes enqueued for outgoing propagation.
    pub bytes_propagated: AtomicU64,
    /// Total bytes received from remote regions.
    pub bytes_received: AtomicU64,
}

/// Point-in-time snapshot of [`BridgeStats`].
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BridgeStatsSnapshot {
    /// Number of local writes captured.
    pub local_writes: u64,
    /// Number of remote writes received.
    pub remote_writes_received: u64,
    /// Number of remote writes applied.
    pub writes_applied: u64,
    /// Number of remote writes rejected.
    pub writes_rejected: u64,
    /// Number of remote writes that resulted in a CRDT merge.
    pub writes_merged: u64,
    /// Total conflicts detected.
    pub conflicts_total: u64,
    /// Total conflicts resolved.
    pub conflicts_resolved: u64,
    /// Total bytes enqueued for outgoing propagation.
    pub bytes_propagated: u64,
    /// Total bytes received from remote regions.
    pub bytes_received: u64,
}

// ---------------------------------------------------------------------------
// CrdtBridgeConfig
// ---------------------------------------------------------------------------

/// Configuration for the [`CrdtReplicationBridge`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CrdtBridgeConfig {
    /// Whether the bridge is active.
    pub enabled: bool,
    /// Maximum number of outgoing writes to buffer.
    pub max_write_log: usize,
    /// Maximum number of incoming writes to buffer.
    pub max_incoming_queue: usize,
    /// Maximum number of conflict events to retain.
    pub max_conflict_log: usize,
    /// Default conflict resolution policy.
    pub default_policy: ConflictResolutionPolicy,
    /// Artificial propagation delay in milliseconds (0 = immediate).
    pub propagation_delay_ms: u64,
    /// Maximum number of writes to drain in a single batch.
    pub batch_size: usize,
}

impl Default for CrdtBridgeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_write_log: 100_000,
            max_incoming_queue: 100_000,
            max_conflict_log: 10_000,
            default_policy: ConflictResolutionPolicy::default(),
            propagation_delay_ms: 0,
            batch_size: 100,
        }
    }
}

// ---------------------------------------------------------------------------
// CrdtReplicationBridge
// ---------------------------------------------------------------------------

/// Bridges the CRDT conflict-resolution layer with the replication stream.
///
/// Local writes are recorded and queued for outgoing propagation to peer
/// regions; incoming remote writes are evaluated for causal ordering and
/// resolved according to the active [`ConflictResolutionPolicy`].
pub struct CrdtReplicationBridge {
    /// Bridge configuration.
    config: CrdtBridgeConfig,
    /// Identifier of the local region.
    local_region_id: String,
    /// Outgoing write log (pending propagation to peers).
    write_log: RwLock<VecDeque<ReplicatedWrite>>,
    /// Incoming writes from remote regions (pending processing).
    incoming_queue: RwLock<VecDeque<IncomingWrite>>,
    /// Conflict event log for auditing.
    conflict_log: RwLock<VecDeque<ConflictEvent>>,
    /// Atomic statistics counters.
    stats: BridgeStats,
    /// Monotonically increasing sequence counter.
    sequence: AtomicU64,
    /// Local vector clock.
    vector_clock: RwLock<HashMap<String, u64>>,
    /// Per-key-pattern conflict policies.
    policy_overrides: RwLock<Vec<(String, ConflictResolutionPolicy)>>,
}

impl CrdtReplicationBridge {
    /// Create a new bridge for `local_region_id` with the given configuration.
    pub fn new(local_region_id: String, config: CrdtBridgeConfig) -> Self {
        Self {
            config,
            local_region_id,
            write_log: RwLock::new(VecDeque::new()),
            incoming_queue: RwLock::new(VecDeque::new()),
            conflict_log: RwLock::new(VecDeque::new()),
            stats: BridgeStats::default(),
            sequence: AtomicU64::new(0),
            vector_clock: RwLock::new(HashMap::new()),
            policy_overrides: RwLock::new(Vec::new()),
        }
    }

    // -- local writes -------------------------------------------------------

    /// Record a local write for outgoing propagation.
    ///
    /// Called after a successful `SET`, `HSET`, or similar mutation on the
    /// local node.
    pub fn on_local_write(&self, key: &str, value: Vec<u8>, command: &str) {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        let timestamp = now_millis();

        // Advance the local component of the vector clock.
        let vc = {
            let mut vc = self.vector_clock.write();
            let entry = vc.entry(self.local_region_id.clone()).or_insert(0);
            *entry = seq;
            vc.clone()
        };

        let bytes_len = value.len() as u64;

        let write = ReplicatedWrite {
            key: key.to_string(),
            value,
            command: command.to_string(),
            source_region: self.local_region_id.clone(),
            timestamp,
            sequence: seq,
            vector_clock: vc,
        };

        // Push to outgoing log, evicting oldest if at capacity.
        {
            let mut log = self.write_log.write();
            if log.len() >= self.config.max_write_log {
                log.pop_front();
            }
            log.push_back(write);
        }

        self.stats.local_writes.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_propagated
            .fetch_add(bytes_len, Ordering::Relaxed);
    }

    // -- remote writes ------------------------------------------------------

    /// Evaluate an incoming remote write and return a [`WriteDecision`].
    ///
    /// The method checks causal ordering via vector clocks:
    /// - **Causally ahead** → `Apply`
    /// - **Concurrent** (neither clock dominates) → delegate to
    ///   [`apply_conflict_policy`](Self::apply_conflict_policy)
    /// - **Causally behind** → `Reject`
    pub fn on_remote_write(&self, write: IncomingWrite) -> WriteDecision {
        let bytes_len = write.value.len() as u64;
        self.stats
            .remote_writes_received
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_received
            .fetch_add(bytes_len, Ordering::Relaxed);

        let local_vc = self.vector_clock.read().clone();
        let ordering = compare_vector_clocks(&local_vc, &write.vector_clock);

        let decision = match ordering {
            VcOrdering::RemoteAhead => {
                // Remote is causally ahead — accept.
                self.merge_vector_clock(&write.vector_clock);
                self.stats.writes_applied.fetch_add(1, Ordering::Relaxed);
                WriteDecision::Apply
            }
            VcOrdering::Concurrent => {
                // Concurrent writes — use conflict policy.
                // We treat the current local_vc timestamp component as the
                // local timestamp for policy evaluation. In practice the
                // caller would supply the real local value; here we use an
                // empty placeholder since we don't have access to the
                // underlying storage.
                let local_ts = local_vc
                    .get(&self.local_region_id)
                    .copied()
                    .unwrap_or(0);
                let local_value: Vec<u8> = Vec::new(); // placeholder

                let decision = self.apply_conflict_policy(
                    &write.key,
                    &local_value,
                    &write.value,
                    write.timestamp,
                );

                // Record conflict event.
                self.record_conflict(
                    &write.key,
                    &local_value,
                    &write.value,
                    &self.local_region_id.clone(),
                    &write.source_region,
                    local_ts,
                    write.timestamp,
                    &decision,
                );

                self.merge_vector_clock(&write.vector_clock);
                decision
            }
            VcOrdering::LocalAhead => {
                // Remote is causally behind — reject.
                self.stats.writes_rejected.fetch_add(1, Ordering::Relaxed);
                WriteDecision::Reject {
                    reason: "remote write is causally behind local state".to_string(),
                }
            }
        };

        decision
    }

    // -- conflict resolution ------------------------------------------------

    /// Apply the active conflict resolution policy for `key`.
    ///
    /// - `LastWriteWins` — compare the local clock component against
    ///   `remote_ts`; higher timestamp wins.
    /// - `LocalWins` — always keep the local value.
    /// - `RemoteWins` — always accept the remote value.
    /// - `CrdtMerge` — for numeric values take the maximum; for UTF-8
    ///   strings take the longer one; for opaque bytes fall back to LWW.
    /// - `Custom` — falls back to `LastWriteWins`.
    pub fn apply_conflict_policy(
        &self,
        key: &str,
        local: &[u8],
        remote: &[u8],
        remote_ts: u64,
    ) -> WriteDecision {
        let policy = self.get_conflict_policy(key);

        self.stats.conflicts_total.fetch_add(1, Ordering::Relaxed);
        self.stats
            .conflicts_resolved
            .fetch_add(1, Ordering::Relaxed);

        match policy {
            ConflictResolutionPolicy::LastWriteWins => {
                let local_ts = self
                    .vector_clock
                    .read()
                    .get(&self.local_region_id)
                    .copied()
                    .unwrap_or(0);
                if remote_ts >= local_ts {
                    self.stats.writes_applied.fetch_add(1, Ordering::Relaxed);
                    WriteDecision::Apply
                } else {
                    self.stats.writes_rejected.fetch_add(1, Ordering::Relaxed);
                    WriteDecision::Reject {
                        reason: "local timestamp is newer (LWW)".to_string(),
                    }
                }
            }
            ConflictResolutionPolicy::LocalWins => {
                self.stats.writes_rejected.fetch_add(1, Ordering::Relaxed);
                WriteDecision::Reject {
                    reason: "LocalWins policy".to_string(),
                }
            }
            ConflictResolutionPolicy::RemoteWins => {
                self.stats.writes_applied.fetch_add(1, Ordering::Relaxed);
                WriteDecision::Apply
            }
            ConflictResolutionPolicy::CrdtMerge => {
                let merged = crdt_merge_values(local, remote);
                self.stats.writes_merged.fetch_add(1, Ordering::Relaxed);
                WriteDecision::Merge {
                    merged_value: merged,
                }
            }
            ConflictResolutionPolicy::Custom(_) => {
                // Custom resolvers are not implemented; fall back to LWW.
                let local_ts = self
                    .vector_clock
                    .read()
                    .get(&self.local_region_id)
                    .copied()
                    .unwrap_or(0);
                if remote_ts >= local_ts {
                    self.stats.writes_applied.fetch_add(1, Ordering::Relaxed);
                    WriteDecision::Apply
                } else {
                    self.stats.writes_rejected.fetch_add(1, Ordering::Relaxed);
                    WriteDecision::Reject {
                        reason: "local timestamp is newer (Custom/LWW fallback)".to_string(),
                    }
                }
            }
        }
    }

    // -- drain helpers ------------------------------------------------------

    /// Drain up to `max` outgoing writes for propagation to peer regions.
    pub fn drain_outgoing(&self, max: usize) -> Vec<ReplicatedWrite> {
        let mut log = self.write_log.write();
        let n = max.min(log.len());
        log.drain(..n).collect()
    }

    /// Drain up to `max` conflict events.
    pub fn drain_conflicts(&self, max: usize) -> Vec<ConflictEvent> {
        let mut log = self.conflict_log.write();
        let n = max.min(log.len());
        log.drain(..n).collect()
    }

    /// Number of writes pending outgoing propagation.
    pub fn pending_outgoing(&self) -> usize {
        self.write_log.read().len()
    }

    /// Number of incoming writes pending processing.
    pub fn pending_incoming(&self) -> usize {
        self.incoming_queue.read().len()
    }

    /// Return a point-in-time snapshot of bridge statistics.
    pub fn stats(&self) -> BridgeStatsSnapshot {
        BridgeStatsSnapshot {
            local_writes: self.stats.local_writes.load(Ordering::Relaxed),
            remote_writes_received: self
                .stats
                .remote_writes_received
                .load(Ordering::Relaxed),
            writes_applied: self.stats.writes_applied.load(Ordering::Relaxed),
            writes_rejected: self.stats.writes_rejected.load(Ordering::Relaxed),
            writes_merged: self.stats.writes_merged.load(Ordering::Relaxed),
            conflicts_total: self.stats.conflicts_total.load(Ordering::Relaxed),
            conflicts_resolved: self.stats.conflicts_resolved.load(Ordering::Relaxed),
            bytes_propagated: self.stats.bytes_propagated.load(Ordering::Relaxed),
            bytes_received: self.stats.bytes_received.load(Ordering::Relaxed),
        }
    }

    // -- policy management --------------------------------------------------

    /// Set a conflict resolution policy for keys matching `key_pattern`.
    ///
    /// Patterns are matched with simple glob-style prefix/suffix wildcards
    /// (e.g. `"user:*"`, `"*:counter"`). The most recently added matching
    /// pattern wins.
    pub fn set_conflict_policy(&self, key_pattern: &str, policy: ConflictResolutionPolicy) {
        let mut overrides = self.policy_overrides.write();
        // Replace existing entry for the same pattern, if any.
        if let Some(entry) = overrides.iter_mut().find(|(p, _)| p == key_pattern) {
            entry.1 = policy;
        } else {
            overrides.push((key_pattern.to_string(), policy));
        }
    }

    /// Return the conflict resolution policy that applies to `key`.
    ///
    /// Checks per-key-pattern overrides (last match wins) and falls back to
    /// the configured default policy.
    pub fn get_conflict_policy(&self, key: &str) -> ConflictResolutionPolicy {
        let overrides = self.policy_overrides.read();
        // Walk in reverse so the most recently added matching pattern wins.
        for (pattern, policy) in overrides.iter().rev() {
            if glob_matches(pattern, key) {
                return policy.clone();
            }
        }
        self.config.default_policy.clone()
    }

    // -- internals ----------------------------------------------------------

    /// Merge a remote vector clock into the local one (component-wise max).
    fn merge_vector_clock(&self, remote: &HashMap<String, u64>) {
        let mut local = self.vector_clock.write();
        for (region, &remote_val) in remote {
            let entry = local.entry(region.clone()).or_insert(0);
            if remote_val > *entry {
                *entry = remote_val;
            }
        }
    }

    /// Record a conflict event.
    fn record_conflict(
        &self,
        key: &str,
        local_value: &[u8],
        remote_value: &[u8],
        local_region: &str,
        remote_region: &str,
        local_ts: u64,
        remote_ts: u64,
        decision: &WriteDecision,
    ) {
        let policy = self.get_conflict_policy(key);
        let event = ConflictEvent {
            key: key.to_string(),
            local_value: local_value.to_vec(),
            remote_value: remote_value.to_vec(),
            local_region: local_region.to_string(),
            remote_region: remote_region.to_string(),
            local_timestamp: local_ts,
            remote_timestamp: remote_ts,
            resolution: decision.clone(),
            policy_used: policy,
            recorded_at: now_millis(),
        };

        let mut log = self.conflict_log.write();
        if log.len() >= self.config.max_conflict_log {
            log.pop_front();
        }
        log.push_back(event);
    }
}

// ---------------------------------------------------------------------------
// Vector-clock helpers
// ---------------------------------------------------------------------------

/// Causal ordering between two vector clocks.
#[derive(Debug, PartialEq, Eq)]
enum VcOrdering {
    /// Every component of the remote clock ≥ local, with at least one >.
    RemoteAhead,
    /// Every component of the local clock ≥ remote, with at least one >.
    LocalAhead,
    /// Neither clock dominates the other.
    Concurrent,
}

/// Compare two vector clocks and return their causal ordering.
fn compare_vector_clocks(
    local: &HashMap<String, u64>,
    remote: &HashMap<String, u64>,
) -> VcOrdering {
    let mut local_has_greater = false;
    let mut remote_has_greater = false;

    // Collect all region keys from both clocks.
    let all_keys: std::collections::HashSet<&String> =
        local.keys().chain(remote.keys()).collect();

    for key in all_keys {
        let l = local.get(key).copied().unwrap_or(0);
        let r = remote.get(key).copied().unwrap_or(0);
        if l > r {
            local_has_greater = true;
        }
        if r > l {
            remote_has_greater = true;
        }
    }

    match (local_has_greater, remote_has_greater) {
        (false, true) => VcOrdering::RemoteAhead,
        (true, false) => VcOrdering::LocalAhead,
        _ => VcOrdering::Concurrent, // both equal or truly concurrent
    }
}

// ---------------------------------------------------------------------------
// CRDT merge helper
// ---------------------------------------------------------------------------

/// Best-effort CRDT merge of two byte slices.
///
/// - If both can be parsed as `i64`, returns the maximum.
/// - If both are valid UTF-8 strings, returns the longer one.
/// - Otherwise falls back to last-write-wins (returns `remote`).
fn crdt_merge_values(local: &[u8], remote: &[u8]) -> Vec<u8> {
    // Try numeric merge (max).
    if let (Ok(l_str), Ok(r_str)) =
        (std::str::from_utf8(local), std::str::from_utf8(remote))
    {
        if let (Ok(l_num), Ok(r_num)) = (l_str.parse::<i64>(), r_str.parse::<i64>()) {
            let max = l_num.max(r_num);
            return max.to_string().into_bytes();
        }

        // String merge — take the longer one, break ties with remote.
        return if l_str.len() > r_str.len() {
            local.to_vec()
        } else {
            remote.to_vec()
        };
    }

    // Opaque bytes — LWW fallback (remote wins).
    remote.to_vec()
}

// ---------------------------------------------------------------------------
// Glob matching helper
// ---------------------------------------------------------------------------

/// Minimal glob matcher supporting `*` as a wildcard.
///
/// Supports patterns like `"user:*"`, `"*:counter"`, `"*session*"`, and
/// literal equality.
fn glob_matches(pattern: &str, key: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if !pattern.contains('*') {
        return pattern == key;
    }
    let parts: Vec<&str> = pattern.split('*').collect();
    // Check prefix (first segment) and suffix (last segment).
    let first = parts[0];
    let last = parts[parts.len() - 1];
    if !key.starts_with(first) || !key.ends_with(last) {
        return false;
    }
    // Walk the interior segments left to right.
    let mut cursor = first.len();
    let end = key.len() - last.len();
    for &seg in &parts[1..parts.len() - 1] {
        if let Some(pos) = key[cursor..end].find(seg) {
            cursor += pos + seg.len();
        } else {
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Timestamp helper
// ---------------------------------------------------------------------------

/// Current wall-clock time in epoch milliseconds.
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_bridge(region: &str) -> CrdtReplicationBridge {
        CrdtReplicationBridge::new(region.to_string(), CrdtBridgeConfig::default())
    }

    fn make_bridge_with_policy(
        region: &str,
        policy: ConflictResolutionPolicy,
    ) -> CrdtReplicationBridge {
        let config = CrdtBridgeConfig {
            default_policy: policy,
            ..Default::default()
        };
        CrdtReplicationBridge::new(region.to_string(), config)
    }

    fn incoming(
        key: &str,
        value: &[u8],
        source: &str,
        ts: u64,
        seq: u64,
        vc: HashMap<String, u64>,
    ) -> IncomingWrite {
        IncomingWrite {
            key: key.to_string(),
            value: value.to_vec(),
            command: "SET".to_string(),
            source_region: source.to_string(),
            timestamp: ts,
            sequence: seq,
            vector_clock: vc,
        }
    }

    // -- basic local write --------------------------------------------------

    #[test]
    fn test_local_write_enqueues() {
        let bridge = make_bridge("us-east");
        bridge.on_local_write("key1", b"val1".to_vec(), "SET");

        assert_eq!(bridge.pending_outgoing(), 1);
        let writes = bridge.drain_outgoing(10);
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].key, "key1");
        assert_eq!(writes[0].value, b"val1");
        assert_eq!(writes[0].command, "SET");
        assert_eq!(writes[0].source_region, "us-east");
        assert_eq!(writes[0].sequence, 1);
        assert!(writes[0].vector_clock.contains_key("us-east"));
    }

    #[test]
    fn test_local_write_sequence_increments() {
        let bridge = make_bridge("us-east");
        bridge.on_local_write("a", b"1".to_vec(), "SET");
        bridge.on_local_write("b", b"2".to_vec(), "SET");
        bridge.on_local_write("c", b"3".to_vec(), "SET");

        let writes = bridge.drain_outgoing(10);
        assert_eq!(writes[0].sequence, 1);
        assert_eq!(writes[1].sequence, 2);
        assert_eq!(writes[2].sequence, 3);
    }

    // -- drain outgoing -----------------------------------------------------

    #[test]
    fn test_drain_outgoing_respects_max() {
        let bridge = make_bridge("us-east");
        for i in 0..5 {
            bridge.on_local_write(&format!("k{i}"), vec![i as u8], "SET");
        }
        let batch = bridge.drain_outgoing(3);
        assert_eq!(batch.len(), 3);
        assert_eq!(bridge.pending_outgoing(), 2);
    }

    // -- remote write: causally ahead ---------------------------------------

    #[test]
    fn test_remote_write_causally_ahead_is_applied() {
        let bridge = make_bridge("us-east");
        // Remote has a higher clock component for "eu-west".
        let mut vc = HashMap::new();
        vc.insert("eu-west".to_string(), 5);

        let w = incoming("key1", b"remote_val", "eu-west", 100, 5, vc);
        let decision = bridge.on_remote_write(w);

        assert_eq!(decision, WriteDecision::Apply);
        let snap = bridge.stats();
        assert_eq!(snap.writes_applied, 1);
    }

    // -- remote write: causally behind --------------------------------------

    #[test]
    fn test_remote_write_causally_behind_is_rejected() {
        let bridge = make_bridge("us-east");
        // Advance local clock first.
        bridge.on_local_write("key1", b"local_val".to_vec(), "SET");

        // Remote clock is behind local.
        let vc = HashMap::new(); // empty = behind local
        let w = incoming("key1", b"old_remote", "eu-west", 50, 1, vc);
        let decision = bridge.on_remote_write(w);

        assert!(matches!(decision, WriteDecision::Reject { .. }));
    }

    // -- remote write: concurrent → LWW ------------------------------------

    #[test]
    fn test_concurrent_write_lww_remote_wins() {
        let bridge = make_bridge("us-east");
        // Advance local clock.
        bridge.on_local_write("key1", b"local".to_vec(), "SET");

        // Remote has a different region component — concurrent.
        let mut vc = HashMap::new();
        vc.insert("eu-west".to_string(), 10);
        let w = incoming("key1", b"remote", "eu-west", u64::MAX, 10, vc);
        let decision = bridge.on_remote_write(w);

        // LWW: remote_ts (MAX) > local sequence (1) → Apply.
        assert_eq!(decision, WriteDecision::Apply);
    }

    #[test]
    fn test_concurrent_write_lww_local_wins() {
        let bridge = make_bridge("us-east");
        // Advance local clock to a high sequence.
        for _ in 0..10 {
            bridge.on_local_write("key1", b"local".to_vec(), "SET");
        }

        let mut vc = HashMap::new();
        vc.insert("eu-west".to_string(), 5);
        // Remote timestamp is 0 — lower than local sequence (10).
        let w = incoming("key1", b"remote", "eu-west", 0, 5, vc);
        let decision = bridge.on_remote_write(w);

        assert!(matches!(decision, WriteDecision::Reject { .. }));
    }

    // -- conflict policy: LocalWins / RemoteWins ----------------------------

    #[test]
    fn test_policy_local_wins() {
        let bridge = make_bridge_with_policy("us-east", ConflictResolutionPolicy::LocalWins);
        bridge.on_local_write("k", b"local".to_vec(), "SET");

        let mut vc = HashMap::new();
        vc.insert("eu-west".to_string(), 5);
        let w = incoming("k", b"remote", "eu-west", 999, 5, vc);
        let decision = bridge.on_remote_write(w);

        assert!(matches!(decision, WriteDecision::Reject { .. }));
    }

    #[test]
    fn test_policy_remote_wins() {
        let bridge = make_bridge_with_policy("us-east", ConflictResolutionPolicy::RemoteWins);
        bridge.on_local_write("k", b"local".to_vec(), "SET");

        let mut vc = HashMap::new();
        vc.insert("eu-west".to_string(), 5);
        let w = incoming("k", b"remote", "eu-west", 999, 5, vc);
        let decision = bridge.on_remote_write(w);

        assert_eq!(decision, WriteDecision::Apply);
    }

    // -- CRDT merge ---------------------------------------------------------

    #[test]
    fn test_crdt_merge_numeric_max() {
        let merged = crdt_merge_values(b"42", b"100");
        assert_eq!(merged, b"100");

        let merged = crdt_merge_values(b"100", b"42");
        assert_eq!(merged, b"100");
    }

    #[test]
    fn test_crdt_merge_string_longer() {
        let merged = crdt_merge_values(b"short", b"a longer string");
        assert_eq!(merged, b"a longer string");

        let merged = crdt_merge_values(b"a longer string", b"short");
        assert_eq!(merged, b"a longer string");
    }

    #[test]
    fn test_crdt_merge_policy_via_bridge() {
        let bridge = make_bridge_with_policy("us-east", ConflictResolutionPolicy::CrdtMerge);
        bridge.on_local_write("counter", b"10".to_vec(), "SET");

        let mut vc = HashMap::new();
        vc.insert("eu-west".to_string(), 5);
        let w = incoming("counter", b"42", "eu-west", 100, 5, vc);
        let decision = bridge.on_remote_write(w);

        assert!(matches!(decision, WriteDecision::Merge { merged_value } if merged_value == b"42"));
    }

    // -- conflict log -------------------------------------------------------

    #[test]
    fn test_conflicts_are_logged() {
        let bridge = make_bridge("us-east");
        bridge.on_local_write("k", b"local".to_vec(), "SET");

        let mut vc = HashMap::new();
        vc.insert("eu-west".to_string(), 5);
        let w = incoming("k", b"remote", "eu-west", 999, 5, vc);
        let _ = bridge.on_remote_write(w);

        let conflicts = bridge.drain_conflicts(10);
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].key, "k");
        assert_eq!(conflicts[0].remote_region, "eu-west");
    }

    // -- stats snapshot -----------------------------------------------------

    #[test]
    fn test_stats_snapshot() {
        let bridge = make_bridge("us-east");
        bridge.on_local_write("a", b"1".to_vec(), "SET");
        bridge.on_local_write("b", b"22".to_vec(), "HSET");

        let snap = bridge.stats();
        assert_eq!(snap.local_writes, 2);
        assert_eq!(snap.bytes_propagated, 3); // 1 + 2
    }

    // -- per-key policy overrides -------------------------------------------

    #[test]
    fn test_per_key_policy_override() {
        let bridge = make_bridge("us-east");
        bridge.set_conflict_policy("session:*", ConflictResolutionPolicy::LocalWins);
        bridge.set_conflict_policy("counter:*", ConflictResolutionPolicy::CrdtMerge);

        assert_eq!(
            bridge.get_conflict_policy("session:abc"),
            ConflictResolutionPolicy::LocalWins,
        );
        assert_eq!(
            bridge.get_conflict_policy("counter:hits"),
            ConflictResolutionPolicy::CrdtMerge,
        );
        // Unmatched key falls back to default.
        assert_eq!(
            bridge.get_conflict_policy("user:123"),
            ConflictResolutionPolicy::LastWriteWins,
        );
    }

    // -- glob matcher -------------------------------------------------------

    #[test]
    fn test_glob_matches() {
        assert!(glob_matches("user:*", "user:123"));
        assert!(glob_matches("*:counter", "hits:counter"));
        assert!(glob_matches("*session*", "my_session_data"));
        assert!(glob_matches("exact", "exact"));
        assert!(!glob_matches("exact", "not_exact"));
        assert!(glob_matches("*", "anything"));
    }

    // -- write log eviction -------------------------------------------------

    #[test]
    fn test_write_log_eviction() {
        let config = CrdtBridgeConfig {
            max_write_log: 3,
            ..Default::default()
        };
        let bridge = CrdtReplicationBridge::new("r".to_string(), config);
        for i in 0..5 {
            bridge.on_local_write(&format!("k{i}"), vec![i as u8], "SET");
        }
        // Oldest two should have been evicted.
        assert_eq!(bridge.pending_outgoing(), 3);
        let writes = bridge.drain_outgoing(10);
        assert_eq!(writes[0].key, "k2");
        assert_eq!(writes[1].key, "k3");
        assert_eq!(writes[2].key, "k4");
    }

    // -- default config values ----------------------------------------------

    #[test]
    fn test_default_config() {
        let cfg = CrdtBridgeConfig::default();
        assert!(!cfg.enabled);
        assert_eq!(cfg.max_write_log, 100_000);
        assert_eq!(cfg.max_incoming_queue, 100_000);
        assert_eq!(cfg.max_conflict_log, 10_000);
        assert_eq!(cfg.default_policy, ConflictResolutionPolicy::LastWriteWins);
        assert_eq!(cfg.propagation_delay_ms, 0);
        assert_eq!(cfg.batch_size, 100);
    }

    // -- vector clock comparison --------------------------------------------

    #[test]
    fn test_vector_clock_comparison() {
        let local: HashMap<String, u64> =
            [("a".into(), 3), ("b".into(), 2)].into_iter().collect();
        let remote: HashMap<String, u64> =
            [("a".into(), 5), ("b".into(), 4)].into_iter().collect();
        assert_eq!(compare_vector_clocks(&local, &remote), VcOrdering::RemoteAhead);

        assert_eq!(compare_vector_clocks(&remote, &local), VcOrdering::LocalAhead);

        let concurrent: HashMap<String, u64> =
            [("a".into(), 1), ("b".into(), 5)].into_iter().collect();
        assert_eq!(
            compare_vector_clocks(&local, &concurrent),
            VcOrdering::Concurrent,
        );
    }
}
