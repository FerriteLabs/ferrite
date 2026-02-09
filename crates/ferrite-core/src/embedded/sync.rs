//! Sync Protocol for Offline-First Applications
//!
//! Delta synchronization with conflict resolution for edge devices
//! that need to work offline and sync when connectivity is available.
//!
//! # Features
//!
//! - Delta-based sync (only changes transferred)
//! - Vector clocks for causality tracking
//! - Configurable conflict resolution
//! - Bandwidth optimization with compression
//! - Incremental snapshots
//!
//! # Example
//!
//! ```ignore
//! use ferrite::embedded::sync::{SyncEngine, SyncConfig, ConflictResolution};
//!
//! let config = SyncConfig {
//!     conflict_resolution: ConflictResolution::LastWriteWins,
//!     compression: true,
//!     ..Default::default()
//! };
//!
//! let engine = SyncEngine::new(config);
//!
//! // Track local changes
//! engine.record_change("key1", ChangeType::Set, &value);
//!
//! // Get delta for sync
//! let delta = engine.get_delta(last_sync_version);
//!
//! // Apply remote delta
//! engine.apply_delta(&remote_delta)?;
//! ```

use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Sync configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncConfig {
    /// Node ID for this instance
    pub node_id: String,
    /// Conflict resolution strategy
    pub conflict_resolution: ConflictResolution,
    /// Enable compression for sync messages
    pub compression: bool,
    /// Maximum delta size before full sync
    pub max_delta_size: usize,
    /// Maximum changes to buffer before sync required
    pub max_pending_changes: usize,
    /// Sync message batch size
    pub batch_size: usize,
    /// Enable vector clocks for causality
    pub vector_clocks: bool,
    /// Retention period for tombstones (seconds)
    pub tombstone_retention_secs: u64,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            node_id: uuid_v4(),
            conflict_resolution: ConflictResolution::LastWriteWins,
            compression: true,
            max_delta_size: 10 * 1024 * 1024, // 10MB
            max_pending_changes: 10_000,
            batch_size: 1000,
            vector_clocks: true,
            tombstone_retention_secs: 86400 * 7, // 7 days
        }
    }
}

/// Generate a simple UUID v4
fn uuid_v4() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    let random: u64 = rand::random();

    format!("{:016x}-{:016x}", timestamp as u64, random)
}

/// Conflict resolution strategy
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictResolution {
    /// Last write wins (by timestamp)
    LastWriteWins,
    /// First write wins (keep existing)
    FirstWriteWins,
    /// Server/primary wins
    ServerWins,
    /// Client/local wins
    ClientWins,
    /// Custom resolver required
    Custom,
}

/// Type of change operation
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeType {
    /// Key was set
    Set,
    /// Key was deleted
    Delete,
    /// Key expired
    Expire,
}

/// A single change record
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangeRecord {
    /// The key that changed
    pub key: String,
    /// Type of change
    pub change_type: ChangeType,
    /// New value (for Set operations)
    pub value: Option<Bytes>,
    /// Timestamp (microseconds since epoch)
    pub timestamp: u64,
    /// Origin node ID
    pub origin: String,
    /// Sequence number at origin
    pub sequence: u64,
    /// Vector clock (optional)
    pub vector_clock: Option<VectorClock>,
}

impl ChangeRecord {
    /// Create a new change record
    pub fn new(
        key: String,
        change_type: ChangeType,
        value: Option<Bytes>,
        origin: String,
        sequence: u64,
    ) -> Self {
        Self {
            key,
            change_type,
            value,
            timestamp: current_timestamp_micros(),
            origin,
            sequence,
            vector_clock: None,
        }
    }

    /// Get the size of this record
    pub fn size(&self) -> usize {
        self.key.len() + self.value.as_ref().map(|v| v.len()).unwrap_or(0) + 64
    }
}

/// Vector clock for causality tracking
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorClock {
    /// Clock values per node
    clocks: BTreeMap<String, u64>,
}

impl VectorClock {
    /// Create a new empty vector clock
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment the clock for a node
    pub fn increment(&mut self, node_id: &str) {
        let counter = self.clocks.entry(node_id.to_string()).or_insert(0);
        *counter += 1;
    }

    /// Get the clock value for a node
    pub fn get(&self, node_id: &str) -> u64 {
        self.clocks.get(node_id).copied().unwrap_or(0)
    }

    /// Merge with another vector clock (take maximum)
    pub fn merge(&mut self, other: &VectorClock) {
        for (node, &value) in &other.clocks {
            let entry = self.clocks.entry(node.clone()).or_insert(0);
            *entry = (*entry).max(value);
        }
    }

    /// Check if this clock happened before another
    pub fn happened_before(&self, other: &VectorClock) -> bool {
        let mut dominated = false;

        for (node, &value) in &self.clocks {
            let other_value = other.get(node);
            if value > other_value {
                return false;
            }
            if value < other_value {
                dominated = true;
            }
        }

        // Check for nodes in other that aren't in self
        for (node, &value) in &other.clocks {
            if value > 0 && self.get(node) == 0 {
                dominated = true;
            }
        }

        dominated
    }

    /// Check if two clocks are concurrent (neither happened before the other)
    pub fn concurrent_with(&self, other: &VectorClock) -> bool {
        !self.happened_before(other) && !other.happened_before(self) && self != other
    }
}

/// Delta sync packet
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeltaSync {
    /// Source node ID
    pub source: String,
    /// Starting sequence number
    pub from_sequence: u64,
    /// Ending sequence number
    pub to_sequence: u64,
    /// Changes in this delta
    pub changes: Vec<ChangeRecord>,
    /// Whether this is compressed
    pub compressed: bool,
    /// Checksum
    pub checksum: u32,
    /// Vector clock snapshot
    pub vector_clock: Option<VectorClock>,
}

impl DeltaSync {
    /// Create a new delta
    pub fn new(source: String, from_sequence: u64, to_sequence: u64) -> Self {
        Self {
            source,
            from_sequence,
            to_sequence,
            changes: Vec::new(),
            compressed: false,
            checksum: 0,
            vector_clock: None,
        }
    }

    /// Add a change to the delta
    pub fn add_change(&mut self, change: ChangeRecord) {
        self.changes.push(change);
    }

    /// Get the size of this delta
    pub fn size(&self) -> usize {
        self.changes.iter().map(|c| c.size()).sum::<usize>() + 128
    }

    /// Check if delta is empty
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> Result<Bytes, SyncError> {
        let data = bincode::serialize(self).map_err(|e| SyncError::Serialization(e.to_string()))?;

        if self.compressed {
            let compressed = lz4_flex::compress_prepend_size(&data);
            Ok(Bytes::from(compressed))
        } else {
            Ok(Bytes::from(data))
        }
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8], compressed: bool) -> Result<Self, SyncError> {
        let decompressed = if compressed {
            lz4_flex::decompress_size_prepended(data)
                .map_err(|e| SyncError::Decompression(e.to_string()))?
        } else {
            data.to_vec()
        };

        bincode::deserialize(&decompressed).map_err(|e| SyncError::Serialization(e.to_string()))
    }

    /// Calculate checksum
    pub fn calculate_checksum(&mut self) {
        let data = bincode::serialize(&self.changes).unwrap_or_default();
        self.checksum = crc32fast::hash(&data);
    }

    /// Verify checksum
    pub fn verify_checksum(&self) -> bool {
        let data = bincode::serialize(&self.changes).unwrap_or_default();
        crc32fast::hash(&data) == self.checksum
    }
}

/// Sync message types
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SyncMessage {
    /// Request delta from a sequence.
    DeltaRequest {
        /// Starting sequence number.
        from_sequence: u64,
        /// Maximum response size in bytes.
        max_size: usize,
    },
    /// Delta response.
    DeltaResponse(DeltaSync),
    /// Full sync request.
    FullSyncRequest,
    /// Full sync response (snapshot).
    FullSyncResponse {
        /// Complete key-value snapshot.
        snapshot: Vec<(String, Bytes)>,
        /// Sequence number at snapshot time.
        sequence: u64,
    },
    /// Acknowledge receipt.
    Ack {
        /// Acknowledged sequence number.
        sequence: u64,
    },
    /// Error.
    Error {
        /// Error description.
        message: String,
    },
}

/// Sync state tracking
#[derive(Clone, Debug, Default)]
pub struct SyncState {
    /// Local sequence number
    pub local_sequence: u64,
    /// Last synced sequence per peer
    pub peer_sequences: HashMap<String, u64>,
    /// Number of pending changes
    pub pending_changes: usize,
    /// Last sync timestamp
    pub last_sync: Option<u64>,
    /// Is currently syncing
    pub syncing: bool,
}

/// Sync statistics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SyncStats {
    /// Total sync operations
    pub sync_count: u64,
    /// Total changes synced
    pub changes_synced: u64,
    /// Total bytes sent
    pub bytes_sent: u64,
    /// Total bytes received
    pub bytes_received: u64,
    /// Conflicts detected
    pub conflicts_detected: u64,
    /// Conflicts resolved
    pub conflicts_resolved: u64,
    /// Compression savings
    pub compression_savings: u64,
    /// Average sync latency (ms)
    pub avg_sync_latency_ms: f64,
}

/// Sync errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum SyncError {
    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Decompression error.
    #[error("decompression error: {0}")]
    Decompression(String),

    /// Checksum mismatch.
    #[error("checksum mismatch")]
    ChecksumMismatch,

    /// Sequence gap detected.
    #[error("sequence gap: expected {expected}, got {got}")]
    SequenceGap {
        /// Expected sequence number.
        expected: u64,
        /// Actual sequence number received.
        got: u64,
    },

    /// Conflict not resolved.
    #[error("unresolved conflict for key: {0}")]
    UnresolvedConflict(String),

    /// Delta too large.
    #[error("delta too large: {size} bytes (max {max})")]
    DeltaTooLarge {
        /// Actual delta size in bytes.
        size: usize,
        /// Maximum allowed size in bytes.
        max: usize,
    },

    /// Sync in progress.
    #[error("sync already in progress")]
    SyncInProgress,
}

/// Conflict resolver callback.
pub trait ConflictResolver: Send + Sync {
    /// Resolve a conflict between local and remote values for a key.
    fn resolve(&self, key: &str, local: &ChangeRecord, remote: &ChangeRecord) -> ConflictOutcome;
}

/// Outcome of conflict resolution
#[derive(Clone, Debug)]
pub enum ConflictOutcome {
    /// Keep the local value.
    KeepLocal,
    /// Accept the remote value.
    AcceptRemote,
    /// Merge values (custom merge result).
    Merge(Bytes),
    /// Skip this key (don't apply).
    Skip,
}

/// Default conflict resolver using configured strategy
struct DefaultResolver {
    strategy: ConflictResolution,
}

impl ConflictResolver for DefaultResolver {
    fn resolve(&self, _key: &str, local: &ChangeRecord, remote: &ChangeRecord) -> ConflictOutcome {
        match self.strategy {
            ConflictResolution::LastWriteWins => {
                if remote.timestamp > local.timestamp {
                    ConflictOutcome::AcceptRemote
                } else {
                    ConflictOutcome::KeepLocal
                }
            }
            ConflictResolution::FirstWriteWins => {
                if remote.timestamp < local.timestamp {
                    ConflictOutcome::AcceptRemote
                } else {
                    ConflictOutcome::KeepLocal
                }
            }
            ConflictResolution::ServerWins => ConflictOutcome::AcceptRemote,
            ConflictResolution::ClientWins => ConflictOutcome::KeepLocal,
            ConflictResolution::Custom => ConflictOutcome::Skip,
        }
    }
}

/// Sync engine for delta synchronization
pub struct SyncEngine {
    /// Configuration
    config: SyncConfig,
    /// Change log
    change_log: RwLock<Vec<ChangeRecord>>,
    /// Current local state (key -> last change)
    local_state: RwLock<HashMap<String, ChangeRecord>>,
    /// Local sequence counter
    sequence: AtomicU64,
    /// Vector clock
    vector_clock: RwLock<VectorClock>,
    /// Sync state
    state: RwLock<SyncState>,
    /// Statistics
    stats: RwLock<SyncStats>,
    /// Custom conflict resolver
    resolver: Option<Arc<dyn ConflictResolver>>,
    /// Tombstones (deleted keys)
    tombstones: RwLock<HashMap<String, u64>>,
}

impl SyncEngine {
    /// Create a new sync engine
    pub fn new(config: SyncConfig) -> Self {
        Self {
            config,
            change_log: RwLock::new(Vec::new()),
            local_state: RwLock::new(HashMap::new()),
            sequence: AtomicU64::new(0),
            vector_clock: RwLock::new(VectorClock::new()),
            state: RwLock::new(SyncState::default()),
            stats: RwLock::new(SyncStats::default()),
            resolver: None,
            tombstones: RwLock::new(HashMap::new()),
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(SyncConfig::default())
    }

    /// Set a custom conflict resolver
    pub fn with_resolver(mut self, resolver: Arc<dyn ConflictResolver>) -> Self {
        self.resolver = Some(resolver);
        self
    }

    /// Record a local change
    pub fn record_change(&self, key: &str, change_type: ChangeType, value: Option<Bytes>) {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;

        // Update vector clock
        {
            let mut vc = self.vector_clock.write();
            vc.increment(&self.config.node_id);
        }

        let mut record = ChangeRecord::new(
            key.to_string(),
            change_type,
            value,
            self.config.node_id.clone(),
            seq,
        );

        if self.config.vector_clocks {
            record.vector_clock = Some(self.vector_clock.read().clone());
        }

        // Add to change log
        {
            let mut log = self.change_log.write();
            log.push(record.clone());

            // Prune old entries if too many
            if log.len() > self.config.max_pending_changes * 2 {
                let keep_from = log.len() - self.config.max_pending_changes;
                log.drain(..keep_from);
            }
        }

        // Update local state
        {
            let mut state = self.local_state.write();
            if change_type == ChangeType::Delete {
                state.remove(key);
                self.tombstones.write().insert(key.to_string(), seq);
            } else {
                state.insert(key.to_string(), record);
            }
        }

        // Update sync state
        {
            let mut sync_state = self.state.write();
            sync_state.local_sequence = seq;
            sync_state.pending_changes += 1;
        }
    }

    /// Get delta since a sequence number
    pub fn get_delta(&self, from_sequence: u64) -> DeltaSync {
        let to_sequence = self.sequence.load(Ordering::SeqCst);

        let mut delta = DeltaSync::new(self.config.node_id.clone(), from_sequence, to_sequence);

        let log = self.change_log.read();

        for change in log.iter() {
            if change.sequence > from_sequence && change.sequence <= to_sequence {
                // Skip if exceeds max delta size
                if delta.size() >= self.config.max_delta_size {
                    break;
                }
                delta.add_change(change.clone());
            }
        }

        if self.config.vector_clocks {
            delta.vector_clock = Some(self.vector_clock.read().clone());
        }

        delta.compressed = self.config.compression;
        delta.calculate_checksum();

        delta
    }

    /// Apply a remote delta
    pub fn apply_delta(&self, delta: &DeltaSync) -> Result<ApplyResult, SyncError> {
        if !delta.verify_checksum() {
            return Err(SyncError::ChecksumMismatch);
        }

        let mut applied = 0;
        let mut skipped = 0;
        let mut conflicts = 0;

        let resolver = self.resolver.as_ref().map(|r| r.as_ref());
        let default_resolver = DefaultResolver {
            strategy: self.config.conflict_resolution,
        };

        for change in &delta.changes {
            let result = self.apply_change(change, resolver.unwrap_or(&default_resolver))?;

            match result {
                ChangeResult::Applied => applied += 1,
                ChangeResult::Skipped => skipped += 1,
                ChangeResult::Conflict => {
                    conflicts += 1;
                    applied += 1; // Conflict was resolved and applied
                }
            }
        }

        // Merge vector clocks
        if let Some(remote_vc) = &delta.vector_clock {
            let mut local_vc = self.vector_clock.write();
            local_vc.merge(remote_vc);
        }

        // Update peer sequence
        {
            let mut state = self.state.write();
            state
                .peer_sequences
                .insert(delta.source.clone(), delta.to_sequence);
            state.last_sync = Some(current_timestamp_micros());
        }

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.sync_count += 1;
            stats.changes_synced += applied;
            stats.conflicts_detected += conflicts;
            stats.conflicts_resolved += conflicts;
        }

        Ok(ApplyResult {
            applied,
            skipped,
            conflicts,
        })
    }

    fn apply_change(
        &self,
        change: &ChangeRecord,
        resolver: &dyn ConflictResolver,
    ) -> Result<ChangeResult, SyncError> {
        let local_state = self.local_state.read();

        // Check for conflict
        if let Some(local_change) = local_state.get(&change.key) {
            // Check causality using vector clocks if available
            let is_conflict = if self.config.vector_clocks {
                if let (Some(local_vc), Some(remote_vc)) =
                    (&local_change.vector_clock, &change.vector_clock)
                {
                    local_vc.concurrent_with(remote_vc)
                } else {
                    local_change.timestamp > change.timestamp - 1000 // Within 1ms
                        && local_change.origin != change.origin
                }
            } else {
                local_change.timestamp > change.timestamp - 1000
                    && local_change.origin != change.origin
            };

            if is_conflict {
                let outcome = resolver.resolve(&change.key, local_change, change);

                drop(local_state); // Release read lock

                match outcome {
                    ConflictOutcome::KeepLocal => return Ok(ChangeResult::Skipped),
                    ConflictOutcome::AcceptRemote => {
                        self.apply_change_internal(change);
                        return Ok(ChangeResult::Conflict);
                    }
                    ConflictOutcome::Merge(merged) => {
                        let mut merged_change = change.clone();
                        merged_change.value = Some(merged);
                        self.apply_change_internal(&merged_change);
                        return Ok(ChangeResult::Conflict);
                    }
                    ConflictOutcome::Skip => return Ok(ChangeResult::Skipped),
                }
            }
        }

        drop(local_state);
        self.apply_change_internal(change);
        Ok(ChangeResult::Applied)
    }

    fn apply_change_internal(&self, change: &ChangeRecord) {
        let mut local_state = self.local_state.write();

        match change.change_type {
            ChangeType::Set => {
                local_state.insert(change.key.clone(), change.clone());
            }
            ChangeType::Delete | ChangeType::Expire => {
                local_state.remove(&change.key);
                self.tombstones
                    .write()
                    .insert(change.key.clone(), change.sequence);
            }
        }
    }

    /// Get current sync state
    pub fn state(&self) -> SyncState {
        self.state.read().clone()
    }

    /// Get statistics
    pub fn stats(&self) -> SyncStats {
        self.stats.read().clone()
    }

    /// Get current sequence number
    pub fn sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
    }

    /// Get all keys (for full sync)
    pub fn get_all_keys(&self) -> Vec<String> {
        self.local_state.read().keys().cloned().collect()
    }

    /// Get value for a key
    pub fn get(&self, key: &str) -> Option<Bytes> {
        self.local_state
            .read()
            .get(key)
            .and_then(|r| r.value.clone())
    }

    /// Check if a key is tombstoned
    pub fn is_tombstoned(&self, key: &str) -> bool {
        self.tombstones.read().contains_key(key)
    }

    /// Clean up old tombstones
    pub fn cleanup_tombstones(&self) {
        let cutoff = current_timestamp_micros() - self.config.tombstone_retention_secs * 1_000_000;
        let log = self.change_log.read();

        let mut tombstones = self.tombstones.write();
        tombstones.retain(|_key, &mut seq| {
            log.iter()
                .find(|c| c.sequence == seq)
                .map(|c| c.timestamp > cutoff)
                .unwrap_or(false)
        });
    }

    /// Create a full snapshot for sync
    pub fn snapshot(&self) -> Vec<(String, Bytes)> {
        self.local_state
            .read()
            .iter()
            .filter_map(|(k, v)| v.value.clone().map(|val| (k.clone(), val)))
            .collect()
    }

    /// Reset sync state (for testing/recovery)
    pub fn reset(&self) {
        self.change_log.write().clear();
        self.local_state.write().clear();
        self.sequence.store(0, Ordering::SeqCst);
        *self.vector_clock.write() = VectorClock::new();
        *self.state.write() = SyncState::default();
        *self.stats.write() = SyncStats::default();
        self.tombstones.write().clear();
    }
}

/// Result of applying a single change
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChangeResult {
    Applied,
    Skipped,
    Conflict,
}

/// Result of applying a delta
#[derive(Clone, Debug)]
pub struct ApplyResult {
    /// Changes applied
    pub applied: u64,
    /// Changes skipped
    pub skipped: u64,
    /// Conflicts resolved
    pub conflicts: u64,
}

fn current_timestamp_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

// SAFETY: SyncEngine is safe to send across threads because all fields use RwLock or
// atomics for interior mutability. The `resolver` field is `Option<Arc<dyn ConflictResolver>>`
// which is Send. No raw pointers are stored.
unsafe impl Send for SyncEngine {}
// SAFETY: SyncEngine is safe to share across threads (&SyncEngine) because all mutable
// state is behind RwLock or atomics, preventing data races on concurrent access.
unsafe impl Sync for SyncEngine {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_clock() {
        let mut vc1 = VectorClock::new();
        vc1.increment("node1");
        vc1.increment("node1");
        assert_eq!(vc1.get("node1"), 2);

        let mut vc2 = VectorClock::new();
        vc2.increment("node2");

        assert!(!vc1.happened_before(&vc2));
        assert!(!vc2.happened_before(&vc1));
        assert!(vc1.concurrent_with(&vc2));

        vc2.merge(&vc1);
        assert_eq!(vc2.get("node1"), 2);
        assert_eq!(vc2.get("node2"), 1);
    }

    #[test]
    fn test_sync_engine_record_change() {
        let engine = SyncEngine::with_defaults();

        engine.record_change("key1", ChangeType::Set, Some(Bytes::from("value1")));
        assert_eq!(engine.sequence(), 1);

        engine.record_change("key2", ChangeType::Set, Some(Bytes::from("value2")));
        assert_eq!(engine.sequence(), 2);

        let delta = engine.get_delta(0);
        assert_eq!(delta.changes.len(), 2);
    }

    #[test]
    fn test_delta_compression() {
        let mut delta = DeltaSync::new("node1".to_string(), 0, 10);

        for i in 0..100 {
            let change = ChangeRecord::new(
                format!("key{}", i),
                ChangeType::Set,
                Some(Bytes::from(vec![b'a'; 100])),
                "node1".to_string(),
                i as u64,
            );
            delta.add_change(change);
        }

        delta.compressed = true;
        let bytes = delta.to_bytes().unwrap();

        let restored = DeltaSync::from_bytes(&bytes, true).unwrap();
        assert_eq!(restored.changes.len(), 100);
    }

    #[test]
    fn test_apply_delta() {
        let engine1 = SyncEngine::with_defaults();
        let engine2 = SyncEngine::with_defaults();

        engine1.record_change("key1", ChangeType::Set, Some(Bytes::from("value1")));
        engine1.record_change("key2", ChangeType::Set, Some(Bytes::from("value2")));

        let delta = engine1.get_delta(0);
        let result = engine2.apply_delta(&delta).unwrap();

        assert_eq!(result.applied, 2);
        assert_eq!(engine2.get("key1"), Some(Bytes::from("value1")));
    }

    #[test]
    fn test_conflict_resolution_lww() {
        let config1 = SyncConfig {
            node_id: "node1".to_string(),
            ..Default::default()
        };
        let config2 = SyncConfig {
            node_id: "node2".to_string(),
            conflict_resolution: ConflictResolution::LastWriteWins,
            ..Default::default()
        };

        let engine1 = SyncEngine::new(config1);
        let engine2 = SyncEngine::new(config2);

        // Both engines set the same key
        engine1.record_change("key1", ChangeType::Set, Some(Bytes::from("value1")));
        std::thread::sleep(std::time::Duration::from_millis(10));
        engine2.record_change("key1", ChangeType::Set, Some(Bytes::from("value2")));

        // Engine2 has newer timestamp, should win
        let delta1 = engine1.get_delta(0);
        let result = engine2.apply_delta(&delta1).unwrap();

        // Should keep value2 (newer)
        assert_eq!(engine2.get("key1"), Some(Bytes::from("value2")));
    }

    #[test]
    fn test_tombstones() {
        let engine = SyncEngine::with_defaults();

        engine.record_change("key1", ChangeType::Set, Some(Bytes::from("value1")));
        engine.record_change("key1", ChangeType::Delete, None);

        assert!(engine.is_tombstoned("key1"));
        assert_eq!(engine.get("key1"), None);
    }

    #[test]
    fn test_sync_state() {
        let engine = SyncEngine::with_defaults();

        engine.record_change("key1", ChangeType::Set, Some(Bytes::from("value1")));

        let state = engine.state();
        assert_eq!(state.local_sequence, 1);
        assert_eq!(state.pending_changes, 1);
    }
}
