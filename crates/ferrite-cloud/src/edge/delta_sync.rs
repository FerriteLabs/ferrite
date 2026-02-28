//! Delta-sync engine with Bloom filters for efficient set reconciliation.
//!
//! Provides a [`BloomFilter`] for probabilistic key membership testing and a
//! [`DeltaSyncEngine`] that orchestrates incremental synchronization between
//! edge nodes and the central cluster.

use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Bloom Filter
// ---------------------------------------------------------------------------

/// A space-efficient probabilistic data structure for set membership testing.
///
/// Uses two independent hash functions to compute `k` hash positions via the
/// Kirsch–Mitzenmacker technique: `h_i(x) = h1(x) + i * h2(x)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilter {
    /// Bit array stored as packed `u64` words.
    bits: Vec<u64>,
    /// Number of hash functions (k).
    num_hashes: u32,
    /// Total number of bits in the filter (m).
    num_bits: usize,
}

impl BloomFilter {
    /// Creates a new Bloom filter sized for `expected_items` with the given
    /// target `false_positive_rate`.
    ///
    /// # Panics
    ///
    /// Panics if `expected_items` is zero or `false_positive_rate` is not in
    /// `(0, 1)`.
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        assert!(expected_items > 0, "expected_items must be > 0");
        assert!(
            false_positive_rate > 0.0 && false_positive_rate < 1.0,
            "false_positive_rate must be in (0, 1)"
        );

        // Optimal number of bits: m = -n * ln(p) / (ln(2))^2
        let ln2 = std::f64::consts::LN_2;
        let num_bits = (-(expected_items as f64) * false_positive_rate.ln() / (ln2 * ln2))
            .ceil() as usize;
        let num_bits = num_bits.max(64); // minimum 1 word

        // Optimal number of hashes: k = (m / n) * ln(2)
        let num_hashes =
            ((num_bits as f64 / expected_items as f64) * ln2).ceil() as u32;
        let num_hashes = num_hashes.max(1);

        let num_words = num_bits.div_ceil(64);
        Self {
            bits: vec![0u64; num_words],
            num_hashes,
            num_bits,
        }
    }

    /// Inserts a key into the filter.
    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = self.hash_pair(key);
        for i in 0..self.num_hashes {
            let idx = self.bit_index(h1, h2, i);
            self.bits[idx / 64] |= 1u64 << (idx % 64);
        }
    }

    /// Tests whether a key *may* be in the set.
    ///
    /// Returns `true` if the key is probably present (with the configured
    /// false-positive rate) or `false` if the key is *definitely* absent.
    pub fn contains(&self, key: &[u8]) -> bool {
        let (h1, h2) = self.hash_pair(key);
        for i in 0..self.num_hashes {
            let idx = self.bit_index(h1, h2, i);
            if self.bits[idx / 64] & (1u64 << (idx % 64)) == 0 {
                return false;
            }
        }
        true
    }

    /// Estimates the number of items inserted using the ratio of set bits.
    pub fn estimated_count(&self) -> usize {
        let set_bits: usize = self.bits.iter().map(|w| w.count_ones() as usize).sum();
        if set_bits == 0 || set_bits >= self.num_bits {
            return set_bits; // degenerate cases
        }
        // n ≈ -(m / k) * ln(1 - X/m)
        let m = self.num_bits as f64;
        let k = self.num_hashes as f64;
        let estimate = -(m / k) * (1.0 - set_bits as f64 / m).ln();
        estimate.round() as usize
    }

    /// Merges `other` into `self` by OR-ing the bit arrays.
    ///
    /// Both filters must have identical parameters (same `num_bits` and
    /// `num_hashes`), otherwise this method panics.
    pub fn merge(&mut self, other: &BloomFilter) {
        assert_eq!(
            self.num_bits, other.num_bits,
            "cannot merge filters with different num_bits"
        );
        assert_eq!(
            self.num_hashes, other.num_hashes,
            "cannot merge filters with different num_hashes"
        );
        for (a, b) in self.bits.iter_mut().zip(other.bits.iter()) {
            *a |= *b;
        }
    }

    // -- private helpers ----------------------------------------------------

    /// Produces two independent 64-bit hashes for `key`.
    fn hash_pair(&self, key: &[u8]) -> (u64, u64) {
        let mut h1 = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut h1);
        let hash1 = h1.finish();

        // Second hash: seed the hasher with the first hash to get independence.
        let mut h2 = std::collections::hash_map::DefaultHasher::new();
        hash1.hash(&mut h2);
        key.hash(&mut h2);
        let hash2 = h2.finish();

        (hash1, hash2)
    }

    /// Computes the bit index for hash function `i`.
    fn bit_index(&self, h1: u64, h2: u64, i: u32) -> usize {
        (h1.wrapping_add((i as u64).wrapping_mul(h2)) % self.num_bits as u64) as usize
    }
}

// ---------------------------------------------------------------------------
// Delta types
// ---------------------------------------------------------------------------

/// The type of mutation recorded in a [`DeltaEntry`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeltaOperation {
    /// Key was created or updated.
    Set,
    /// Key was deleted.
    Delete,
    /// Key was expired (TTL).
    Expire,
}

/// A single key-level change to be replicated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaEntry {
    /// The affected key.
    pub key: String,
    /// The new value, or `None` for deletions.
    pub value: Option<Vec<u8>>,
    /// Kind of mutation.
    pub operation: DeltaOperation,
    /// Wall-clock timestamp in milliseconds since epoch.
    pub timestamp: u64,
    /// Logical version at which this change was made.
    pub version: u64,
    /// Identifier of the node that originated this change.
    pub node_id: String,
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration knobs for the delta-sync engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaSyncConfig {
    /// Maximum number of [`DeltaEntry`] items buffered locally.
    pub max_delta_buffer: usize,
    /// Expected number of items for Bloom filter sizing.
    pub bloom_expected_items: usize,
    /// Target false-positive rate for Bloom filters.
    pub bloom_false_positive_rate: f64,
    /// Maximum entries sent/received in a single batch.
    pub max_batch_size: usize,
    /// Whether to compress delta payloads on the wire.
    pub compression_enabled: bool,
    /// Key prefixes that should be synced with higher priority.
    pub priority_prefixes: Vec<String>,
}

impl Default for DeltaSyncConfig {
    fn default() -> Self {
        Self {
            max_delta_buffer: 50_000,
            bloom_expected_items: 100_000,
            bloom_false_positive_rate: 0.01,
            max_batch_size: 1000,
            compression_enabled: true,
            priority_prefixes: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Sync state machine
// ---------------------------------------------------------------------------

/// States of the delta-sync protocol state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncState {
    /// No sync in progress.
    Idle,
    /// Building a local Bloom filter for reconciliation.
    BuildingBloom,
    /// Sending local deltas to the remote peer.
    SendingDeltas,
    /// Receiving remote deltas.
    ReceivingDeltas,
    /// Reconciling divergent keys.
    Reconciling,
    /// Sync round completed.
    Complete,
}

// ---------------------------------------------------------------------------
// Result / status
// ---------------------------------------------------------------------------

/// Outcome of applying a batch of remote deltas.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeltaSyncResult {
    /// Number of deltas successfully applied.
    pub applied: usize,
    /// Number of deltas skipped (e.g. stale version).
    pub skipped: usize,
    /// Number of conflicting writes detected.
    pub conflicts: usize,
    /// Non-fatal error messages.
    pub errors: Vec<String>,
    /// Wall-clock duration of the apply step in milliseconds.
    pub duration_ms: u64,
}

/// Serializable snapshot of the engine's current sync status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncStatus {
    /// Current protocol state.
    pub state: SyncState,
    /// Local logical version.
    pub local_version: u64,
    /// Last-known remote logical version.
    pub remote_version: u64,
    /// Number of deltas waiting to be sent.
    pub pending_deltas: usize,
    /// Result of the most recent sync round, if any.
    pub last_sync_result: Option<DeltaSyncResult>,
}

// ---------------------------------------------------------------------------
// Delta-sync engine
// ---------------------------------------------------------------------------

/// Orchestrates delta synchronization between an edge node and the central
/// cluster using Bloom-filter-based set reconciliation.
pub struct DeltaSyncEngine {
    /// Engine configuration.
    config: DeltaSyncConfig,
    /// Monotonically increasing local logical clock.
    local_version: AtomicU64,
    /// Last-known version from the remote peer.
    remote_version: AtomicU64,
    /// FIFO buffer of locally recorded deltas awaiting transmission.
    pending_deltas: RwLock<VecDeque<DeltaEntry>>,
    /// Current protocol state.
    sync_state: RwLock<SyncState>,
    /// Result of the last completed sync round.
    last_sync_result: RwLock<Option<DeltaSyncResult>>,
}

impl DeltaSyncEngine {
    /// Creates a new engine with the given configuration.
    pub fn new(config: DeltaSyncConfig) -> Self {
        Self {
            config,
            local_version: AtomicU64::new(0),
            remote_version: AtomicU64::new(0),
            pending_deltas: RwLock::new(VecDeque::new()),
            sync_state: RwLock::new(SyncState::Idle),
            last_sync_result: RwLock::new(None),
        }
    }

    /// Builds a [`BloomFilter`] populated with the provided `keys`, sized
    /// according to the engine configuration.
    pub fn build_bloom_filter(&self, keys: &[String]) -> BloomFilter {
        *self.sync_state.write() = SyncState::BuildingBloom;

        let expected = self.config.bloom_expected_items.max(keys.len());
        let mut bloom = BloomFilter::new(expected, self.config.bloom_false_positive_rate);
        for key in keys {
            bloom.insert(key.as_bytes());
        }

        *self.sync_state.write() = SyncState::Idle;
        bloom
    }

    /// Returns the keys present in `local_keys` that are *not* contained in
    /// `remote_bloom` — i.e. the keys the remote side is likely missing.
    pub fn compute_missing_keys(
        &self,
        local_keys: &[String],
        remote_bloom: &BloomFilter,
    ) -> Vec<String> {
        local_keys
            .iter()
            .filter(|k| !remote_bloom.contains(k.as_bytes()))
            .cloned()
            .collect()
    }

    /// Records a local mutation as a pending delta.
    ///
    /// If the pending buffer is full the oldest entry is silently dropped.
    pub fn add_delta(&self, entry: DeltaEntry) {
        let mut deltas = self.pending_deltas.write();
        if deltas.len() >= self.config.max_delta_buffer {
            deltas.pop_front();
        }
        let version = self.local_version.fetch_add(1, Ordering::SeqCst) + 1;
        let mut entry = entry;
        entry.version = version;
        deltas.push_back(entry);
    }

    /// Drains up to `max` pending deltas for transmission, preferring entries
    /// whose keys match a configured priority prefix.
    pub fn drain_deltas(&self, max: usize) -> Vec<DeltaEntry> {
        let mut deltas = self.pending_deltas.write();
        let take = max.min(deltas.len());
        if take == 0 {
            return Vec::new();
        }

        *self.sync_state.write() = SyncState::SendingDeltas;

        // If priority prefixes are set, partition so priority items come first.
        if !self.config.priority_prefixes.is_empty() {
            let (mut priority, mut rest): (Vec<_>, Vec<_>) =
                deltas.drain(..take).partition(|e: &DeltaEntry| {
                    self.config
                        .priority_prefixes
                        .iter()
                        .any(|p| e.key.starts_with(p))
                });
            priority.append(&mut rest);
            return priority;
        }

        deltas.drain(..take).collect()
    }

    /// Applies a batch of remote deltas, using last-write-wins on version
    /// conflicts.
    pub fn apply_remote_deltas(&self, deltas: Vec<DeltaEntry>) -> DeltaSyncResult {
        *self.sync_state.write() = SyncState::ReceivingDeltas;

        let start = Instant::now();
        let mut result = DeltaSyncResult::default();
        let current_version = self.local_version.load(Ordering::SeqCst);

        *self.sync_state.write() = SyncState::Reconciling;

        for delta in &deltas {
            if delta.version <= current_version {
                // Remote version is stale — potential conflict.
                result.conflicts += 1;
                result.skipped += 1;
            } else {
                result.applied += 1;
            }
        }

        // Advance remote version watermark.
        if let Some(max_v) = deltas.iter().map(|d| d.version).max() {
            self.remote_version.fetch_max(max_v, Ordering::SeqCst);
        }

        result.duration_ms = start.elapsed().as_millis() as u64;

        *self.sync_state.write() = SyncState::Complete;
        *self.last_sync_result.write() = Some(result.clone());

        result
    }

    /// Returns a serializable snapshot of the current sync status.
    pub fn sync_status(&self) -> SyncStatus {
        SyncStatus {
            state: *self.sync_state.read(),
            local_version: self.local_version.load(Ordering::SeqCst),
            remote_version: self.remote_version.load(Ordering::SeqCst),
            pending_deltas: self.pending_deltas.read().len(),
            last_sync_result: self.last_sync_result.read().clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Bloom filter tests -------------------------------------------------

    #[test]
    fn bloom_insert_and_contains() {
        let mut bloom = BloomFilter::new(100, 0.01);
        bloom.insert(b"hello");
        bloom.insert(b"world");

        assert!(bloom.contains(b"hello"));
        assert!(bloom.contains(b"world"));
    }

    #[test]
    fn bloom_no_false_negatives() {
        let mut bloom = BloomFilter::new(1000, 0.01);
        let keys: Vec<String> = (0..1000).map(|i| format!("key:{i}")).collect();

        for key in &keys {
            bloom.insert(key.as_bytes());
        }

        // Every inserted key must be reported as present.
        for key in &keys {
            assert!(
                bloom.contains(key.as_bytes()),
                "false negative for {key}"
            );
        }
    }

    #[test]
    fn bloom_false_positive_rate_within_bounds() {
        let n = 1000;
        let fpr = 0.05; // 5 %
        let mut bloom = BloomFilter::new(n, fpr);

        for i in 0..n {
            bloom.insert(format!("inserted:{i}").as_bytes());
        }

        // Test 10 000 keys that were *not* inserted.
        let test_count = 10_000;
        let false_positives = (0..test_count)
            .filter(|i| bloom.contains(format!("absent:{i}").as_bytes()))
            .count();

        let observed_rate = false_positives as f64 / test_count as f64;
        // Allow generous margin (3×) to avoid flaky CI.
        assert!(
            observed_rate < fpr * 3.0,
            "observed FPR {observed_rate:.4} exceeds 3× target {fpr}"
        );
    }

    #[test]
    fn bloom_merge_combines_filters() {
        let mut a = BloomFilter::new(100, 0.01);
        let mut b = BloomFilter::new(100, 0.01);

        a.insert(b"alpha");
        b.insert(b"beta");

        assert!(!a.contains(b"beta"));
        assert!(!b.contains(b"alpha"));

        a.merge(&b);

        assert!(a.contains(b"alpha"));
        assert!(a.contains(b"beta"));
    }

    // -- DeltaSyncEngine tests ----------------------------------------------

    #[test]
    fn engine_add_and_drain_deltas() {
        let engine = DeltaSyncEngine::new(DeltaSyncConfig::default());

        for i in 0..5 {
            engine.add_delta(DeltaEntry {
                key: format!("k{i}"),
                value: Some(vec![i as u8]),
                operation: DeltaOperation::Set,
                timestamp: 1000 + i,
                version: 0, // assigned by engine
                node_id: "edge-1".into(),
            });
        }

        let batch = engine.drain_deltas(3);
        assert_eq!(batch.len(), 3);
        assert_eq!(engine.sync_status().pending_deltas, 2);
    }

    #[test]
    fn engine_compute_missing_keys() {
        let engine = DeltaSyncEngine::new(DeltaSyncConfig::default());

        let remote_keys = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let remote_bloom = engine.build_bloom_filter(&remote_keys);

        let local_keys = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
        ];

        let missing = engine.compute_missing_keys(&local_keys, &remote_bloom);
        // "d" and "e" are definitely missing; "a", "b", "c" should NOT appear.
        assert!(missing.contains(&"d".to_string()));
        assert!(missing.contains(&"e".to_string()));
        assert!(!missing.contains(&"a".to_string()));
        assert!(!missing.contains(&"b".to_string()));
        assert!(!missing.contains(&"c".to_string()));
    }

    #[test]
    fn engine_apply_remote_deltas() {
        let engine = DeltaSyncEngine::new(DeltaSyncConfig::default());

        // Advance local version to 5.
        for i in 0..5 {
            engine.add_delta(DeltaEntry {
                key: format!("local:{i}"),
                value: None,
                operation: DeltaOperation::Delete,
                timestamp: 1000,
                version: 0,
                node_id: "edge-1".into(),
            });
        }

        let remote = vec![
            DeltaEntry {
                key: "remote:0".into(),
                value: Some(b"v".to_vec()),
                operation: DeltaOperation::Set,
                timestamp: 2000,
                version: 3, // stale — local is at 5
                node_id: "central".into(),
            },
            DeltaEntry {
                key: "remote:1".into(),
                value: None,
                operation: DeltaOperation::Expire,
                timestamp: 2001,
                version: 10, // newer
                node_id: "central".into(),
            },
        ];

        let result = engine.apply_remote_deltas(remote);
        assert_eq!(result.applied, 1);
        assert_eq!(result.skipped, 1);
        assert_eq!(result.conflicts, 1);
        assert!(result.duration_ms < 1000);
    }

    #[test]
    fn engine_version_tracking() {
        let engine = DeltaSyncEngine::new(DeltaSyncConfig::default());

        assert_eq!(engine.sync_status().local_version, 0);
        assert_eq!(engine.sync_status().remote_version, 0);

        engine.add_delta(DeltaEntry {
            key: "k".into(),
            value: None,
            operation: DeltaOperation::Set,
            timestamp: 1,
            version: 0,
            node_id: "n".into(),
        });
        assert_eq!(engine.sync_status().local_version, 1);

        // Apply a remote delta with version 42.
        engine.apply_remote_deltas(vec![DeltaEntry {
            key: "r".into(),
            value: None,
            operation: DeltaOperation::Delete,
            timestamp: 2,
            version: 42,
            node_id: "central".into(),
        }]);
        assert_eq!(engine.sync_status().remote_version, 42);
        assert_eq!(engine.sync_status().state, SyncState::Complete);
    }
}
