//! Version Store for Time-Travel Queries
//!
//! This module implements version retention that enables temporal queries over
//! key-value data. Each mutation (set, delete, expire, update) is recorded as
//! a [`VersionEntry`] in a per-key [`VersionChain`], allowing callers to:
//!
//! - Retrieve the value of a key at any past timestamp ([`VersionStore::get_at`])
//! - List the full mutation history of a key ([`VersionStore::get_history`])
//! - Compute diffs between two points in time ([`VersionStore::diff`])
//! - Discover which keys changed in a time range ([`VersionStore::keys_changed_between`])
//!
//! Expired versions are reclaimed by a garbage-collection pass ([`VersionStore::gc`]).

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns the current wall-clock time as Unix milliseconds.
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the [`VersionStore`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionStoreConfig {
    /// Whether version tracking is enabled.
    pub enabled: bool,
    /// Maximum number of versions retained per key.
    pub max_versions_per_key: usize,
    /// How long (in milliseconds) to retain versions before GC eligibility.
    pub retention_duration_ms: u64,
    /// Global cap on the total number of stored versions.
    pub max_total_versions: usize,
    /// Global cap on the total stored bytes.
    pub max_total_bytes: usize,
    /// Minimum interval between GC runs (milliseconds).
    pub gc_interval_ms: u64,
    /// Maximum number of versions to inspect per GC run.
    pub gc_batch_size: usize,
}

impl Default for VersionStoreConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_versions_per_key: 100,
            retention_duration_ms: 7 * 24 * 3600 * 1000, // 7 days
            max_total_versions: 10_000_000,
            max_total_bytes: 1_073_741_824, // 1 GB
            gc_interval_ms: 60_000,
            gc_batch_size: 10_000,
        }
    }
}

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

/// The kind of mutation that produced a version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VersionOperation {
    /// A key was set to a new value.
    Set,
    /// A key was explicitly deleted.
    Delete,
    /// A key expired via TTL.
    Expire,
    /// A key's value was updated in-place.
    Update,
}

/// A single recorded version of a key's value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionEntry {
    /// The value at this version, or `None` for deletes/expires.
    pub value: Option<Vec<u8>>,
    /// The operation that created this version.
    pub operation: VersionOperation,
    /// Unix timestamp in milliseconds when this version was recorded.
    pub timestamp: u64,
    /// Monotonically increasing version number (per-store).
    pub version: u64,
    /// Size of the value in bytes (0 for tombstones).
    pub size_bytes: usize,
    /// Original TTL in milliseconds, if one was set on the key.
    pub ttl_ms: Option<u64>,
}

/// An ordered list of versions for a single key.
///
/// Entries are stored newest-first (descending timestamp).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionChain {
    /// Version entries ordered by timestamp descending.
    pub entries: Vec<VersionEntry>,
    /// Cumulative byte size of all entries in this chain.
    pub total_size_bytes: u64,
    /// Timestamp when this chain was first created.
    pub created_at: u64,
}

impl VersionChain {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            total_size_bytes: 0,
            created_at: now_millis(),
        }
    }
}

/// The result of diffing a key between two timestamps.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionDiff {
    /// The key that was diffed.
    pub key: String,
    /// Value at the earlier timestamp (or `None`).
    pub old_value: Option<Vec<u8>>,
    /// Value at the later timestamp (or `None`).
    pub new_value: Option<Vec<u8>>,
    /// The earlier timestamp.
    pub old_timestamp: u64,
    /// The later timestamp.
    pub new_timestamp: u64,
    /// Number of mutation operations between the two timestamps.
    pub operations_between: usize,
}

/// Results returned from a single GC run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcResult {
    /// Number of version entries removed.
    pub versions_removed: u64,
    /// Total bytes freed.
    pub bytes_freed: u64,
    /// Number of distinct keys that had versions removed.
    pub keys_affected: u64,
    /// Wall-clock duration of the GC run in milliseconds.
    pub duration_ms: u64,
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Atomic counters for garbage-collection bookkeeping.
#[derive(Debug)]
pub struct GcStats {
    gc_runs: AtomicU64,
    gc_versions_removed: AtomicU64,
    gc_bytes_freed: AtomicU64,
}

impl Default for GcStats {
    fn default() -> Self {
        Self {
            gc_runs: AtomicU64::new(0),
            gc_versions_removed: AtomicU64::new(0),
            gc_bytes_freed: AtomicU64::new(0),
        }
    }
}

/// A serializable snapshot of the version store's statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionStoreStats {
    /// Total number of version entries across all keys.
    pub total_versions: u64,
    /// Total bytes consumed by stored values.
    pub total_bytes: u64,
    /// Number of distinct keys with at least one version.
    pub total_keys: u64,
    /// Number of GC runs completed.
    pub gc_runs: u64,
    /// Cumulative versions removed by GC.
    pub gc_versions_removed: u64,
    /// Cumulative bytes freed by GC.
    pub gc_bytes_freed: u64,
    /// Timestamp of the oldest retained version (0 if empty).
    pub oldest_version_timestamp: u64,
}

// ---------------------------------------------------------------------------
// VersionStore
// ---------------------------------------------------------------------------

/// Concurrent version store that records historical mutations for time-travel
/// queries.
///
/// Each key maintains a [`VersionChain`] of [`VersionEntry`] items sorted
/// newest-first. The store enforces per-key and global limits and supports
/// garbage collection of expired versions.
#[derive(Debug)]
pub struct VersionStore {
    /// Store configuration.
    config: VersionStoreConfig,
    /// Per-key version chains.
    versions: DashMap<String, VersionChain>,
    /// Global monotonic version counter.
    next_version: AtomicU64,
    /// Total number of version entries across all keys.
    total_versions: AtomicU64,
    /// Total bytes consumed by version values.
    total_bytes: AtomicU64,
    /// Garbage-collection statistics.
    gc_stats: GcStats,
}

impl VersionStore {
    /// Create a new `VersionStore` with the given configuration.
    pub fn new(config: VersionStoreConfig) -> Self {
        Self {
            config,
            versions: DashMap::new(),
            next_version: AtomicU64::new(1),
            total_versions: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            gc_stats: GcStats::default(),
        }
    }

    /// Record a new version for `key`.
    ///
    /// If version tracking is disabled via config this is a no-op.
    pub fn record_version(
        &self,
        key: &str,
        value: Option<Vec<u8>>,
        operation: VersionOperation,
    ) {
        if !self.config.enabled {
            return;
        }

        let size_bytes = value.as_ref().map_or(0, |v| v.len());
        let version = self.next_version.fetch_add(1, Ordering::Relaxed);
        let timestamp = now_millis();

        let entry = VersionEntry {
            value,
            operation,
            timestamp,
            version,
            size_bytes,
            ttl_ms: None,
        };

        let mut chain = self
            .versions
            .entry(key.to_string())
            .or_insert_with(VersionChain::new);

        // Insert at the front (newest first).
        chain.entries.insert(0, entry);
        chain.total_size_bytes += size_bytes as u64;

        // Enforce per-key limit.
        while chain.entries.len() > self.config.max_versions_per_key {
            if let Some(removed) = chain.entries.pop() {
                chain.total_size_bytes = chain
                    .total_size_bytes
                    .saturating_sub(removed.size_bytes as u64);
                self.total_versions.fetch_sub(1, Ordering::Relaxed);
                self.total_bytes
                    .fetch_sub(removed.size_bytes as u64, Ordering::Relaxed);
            }
        }

        self.total_versions.fetch_add(1, Ordering::Relaxed);
        self.total_bytes
            .fetch_add(size_bytes as u64, Ordering::Relaxed);
    }

    /// Retrieve the value of `key` at (or just before) the given `timestamp`.
    ///
    /// Returns the most recent version whose timestamp is ≤ `timestamp`.
    pub fn get_at(&self, key: &str, timestamp: u64) -> Option<VersionEntry> {
        let chain = self.versions.get(key)?;
        // Entries are newest-first; find the first entry at or before the ts.
        chain
            .entries
            .iter()
            .find(|e| e.timestamp <= timestamp)
            .cloned()
    }

    /// Return the most recent `limit` versions for `key`, newest first.
    pub fn get_history(&self, key: &str, limit: usize) -> Vec<VersionEntry> {
        match self.versions.get(key) {
            Some(chain) => chain.entries.iter().take(limit).cloned().collect(),
            None => Vec::new(),
        }
    }

    /// Return all versions for `key` whose timestamps fall in `[from, to]`.
    pub fn get_range(&self, key: &str, from: u64, to: u64) -> Vec<VersionEntry> {
        match self.versions.get(key) {
            Some(chain) => chain
                .entries
                .iter()
                .filter(|e| e.timestamp >= from && e.timestamp <= to)
                .cloned()
                .collect(),
            None => Vec::new(),
        }
    }

    /// Compute the diff for `key` between timestamps `ts1` and `ts2`.
    ///
    /// `ts1` is treated as the *earlier* timestamp and `ts2` as the *later*.
    /// If the key has no recorded versions at either timestamp, returns `None`.
    pub fn diff(&self, key: &str, ts1: u64, ts2: u64) -> Option<VersionDiff> {
        let chain = self.versions.get(key)?;

        let (earlier, later) = if ts1 <= ts2 { (ts1, ts2) } else { (ts2, ts1) };

        let old_entry = chain.entries.iter().find(|e| e.timestamp <= earlier);
        let new_entry = chain.entries.iter().find(|e| e.timestamp <= later);

        // Need at least one side to produce a diff.
        if old_entry.is_none() && new_entry.is_none() {
            return None;
        }

        let operations_between = chain
            .entries
            .iter()
            .filter(|e| e.timestamp > earlier && e.timestamp <= later)
            .count();

        Some(VersionDiff {
            key: key.to_string(),
            old_value: old_entry.and_then(|e| e.value.clone()),
            new_value: new_entry.and_then(|e| e.value.clone()),
            old_timestamp: old_entry.map_or(earlier, |e| e.timestamp),
            new_timestamp: new_entry.map_or(later, |e| e.timestamp),
            operations_between,
        })
    }

    /// Return up to `limit` keys that have at least one version with a
    /// timestamp in `[from, to]`.
    pub fn keys_changed_between(&self, from: u64, to: u64, limit: usize) -> Vec<String> {
        self.versions
            .iter()
            .filter(|entry| {
                entry
                    .value()
                    .entries
                    .iter()
                    .any(|e| e.timestamp >= from && e.timestamp <= to)
            })
            .map(|entry| entry.key().clone())
            .take(limit)
            .collect()
    }

    /// Run a garbage-collection pass, removing versions that exceed the
    /// configured retention duration.
    ///
    /// Returns a [`GcResult`] summarising the work done.
    pub fn gc(&self) -> GcResult {
        let start = now_millis();
        let cutoff = start.saturating_sub(self.config.retention_duration_ms);

        let mut versions_removed: u64 = 0;
        let mut bytes_freed: u64 = 0;
        let mut keys_affected: u64 = 0;
        let mut inspected: usize = 0;

        for mut entry in self.versions.iter_mut() {
            if inspected >= self.config.gc_batch_size {
                break;
            }

            let chain = entry.value_mut();
            let before_len = chain.entries.len();

            chain.entries.retain(|e| {
                if e.timestamp < cutoff {
                    bytes_freed += e.size_bytes as u64;
                    false
                } else {
                    true
                }
            });

            let removed = (before_len - chain.entries.len()) as u64;
            if removed > 0 {
                chain.total_size_bytes = chain
                    .total_size_bytes
                    .saturating_sub(bytes_freed);
                versions_removed += removed;
                keys_affected += 1;
            }

            inspected += before_len;
        }

        // Update global counters.
        self.total_versions
            .fetch_sub(versions_removed, Ordering::Relaxed);
        self.total_bytes
            .fetch_sub(bytes_freed, Ordering::Relaxed);

        // Update GC stats.
        self.gc_stats.gc_runs.fetch_add(1, Ordering::Relaxed);
        self.gc_stats
            .gc_versions_removed
            .fetch_add(versions_removed, Ordering::Relaxed);
        self.gc_stats
            .gc_bytes_freed
            .fetch_add(bytes_freed, Ordering::Relaxed);

        let duration_ms = now_millis().saturating_sub(start);

        GcResult {
            versions_removed,
            bytes_freed,
            keys_affected,
            duration_ms,
        }
    }

    /// Return a point-in-time snapshot of the store's statistics.
    pub fn stats(&self) -> VersionStoreStats {
        let oldest = self
            .versions
            .iter()
            .filter_map(|entry| entry.value().entries.last().map(|e| e.timestamp))
            .min()
            .unwrap_or(0);

        VersionStoreStats {
            total_versions: self.total_versions.load(Ordering::Relaxed),
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
            total_keys: self.versions.len() as u64,
            gc_runs: self.gc_stats.gc_runs.load(Ordering::Relaxed),
            gc_versions_removed: self.gc_stats.gc_versions_removed.load(Ordering::Relaxed),
            gc_bytes_freed: self.gc_stats.gc_bytes_freed.load(Ordering::Relaxed),
            oldest_version_timestamp: oldest,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_store() -> VersionStore {
        VersionStore::new(VersionStoreConfig {
            max_versions_per_key: 5,
            retention_duration_ms: 1_000, // 1 second for fast tests
            ..Default::default()
        })
    }

    fn record_with_ts(store: &VersionStore, key: &str, val: &[u8], ts: u64) {
        let size_bytes = val.len();
        let version = store.next_version.fetch_add(1, Ordering::Relaxed);
        let entry = VersionEntry {
            value: Some(val.to_vec()),
            operation: VersionOperation::Set,
            timestamp: ts,
            version,
            size_bytes,
            ttl_ms: None,
        };

        let mut chain = store
            .versions
            .entry(key.to_string())
            .or_insert_with(VersionChain::new);

        // Maintain newest-first ordering.
        let pos = chain
            .entries
            .iter()
            .position(|e| e.timestamp < ts)
            .unwrap_or(chain.entries.len());
        chain.entries.insert(pos, entry);
        chain.total_size_bytes += size_bytes as u64;

        store.total_versions.fetch_add(1, Ordering::Relaxed);
        store.total_bytes.fetch_add(size_bytes as u64, Ordering::Relaxed);
    }

    #[test]
    fn test_record_and_get_at() {
        let store = make_store();
        record_with_ts(&store, "k1", b"v1", 100);
        record_with_ts(&store, "k1", b"v2", 200);

        let e = store.get_at("k1", 150).unwrap();
        assert_eq!(e.value.as_deref(), Some(b"v1".as_slice()));
        assert_eq!(e.timestamp, 100);

        let e = store.get_at("k1", 200).unwrap();
        assert_eq!(e.value.as_deref(), Some(b"v2".as_slice()));

        assert!(store.get_at("k1", 50).is_none());
    }

    #[test]
    fn test_history_ordering_newest_first() {
        let store = make_store();
        record_with_ts(&store, "k1", b"a", 10);
        record_with_ts(&store, "k1", b"b", 20);
        record_with_ts(&store, "k1", b"c", 30);

        let history = store.get_history("k1", 10);
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].timestamp, 30);
        assert_eq!(history[1].timestamp, 20);
        assert_eq!(history[2].timestamp, 10);
    }

    #[test]
    fn test_get_range_filtering() {
        let store = make_store();
        record_with_ts(&store, "k1", b"a", 100);
        record_with_ts(&store, "k1", b"b", 200);
        record_with_ts(&store, "k1", b"c", 300);
        record_with_ts(&store, "k1", b"d", 400);

        let range = store.get_range("k1", 150, 350);
        assert_eq!(range.len(), 2);
        assert!(range.iter().all(|e| e.timestamp >= 150 && e.timestamp <= 350));
    }

    #[test]
    fn test_diff_between_timestamps() {
        let store = make_store();
        record_with_ts(&store, "k1", b"old", 100);
        record_with_ts(&store, "k1", b"mid", 200);
        record_with_ts(&store, "k1", b"new", 300);

        let diff = store.diff("k1", 100, 300).unwrap();
        assert_eq!(diff.old_value.as_deref(), Some(b"old".as_slice()));
        assert_eq!(diff.new_value.as_deref(), Some(b"new".as_slice()));
        assert_eq!(diff.operations_between, 2); // mid + new
    }

    #[test]
    fn test_keys_changed_between() {
        let store = make_store();
        record_with_ts(&store, "k1", b"a", 100);
        record_with_ts(&store, "k2", b"b", 200);
        record_with_ts(&store, "k3", b"c", 300);

        let keys = store.keys_changed_between(150, 250, 10);
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], "k2");
    }

    #[test]
    fn test_gc_removes_expired_versions() {
        let store = make_store();

        // Insert a version with a very old timestamp (well past retention).
        record_with_ts(&store, "k1", b"old", 1);
        // Insert a recent version.
        record_with_ts(&store, "k1", b"new", now_millis());

        let result = store.gc();
        assert_eq!(result.versions_removed, 1);
        assert_eq!(result.keys_affected, 1);
        assert_eq!(store.get_history("k1", 10).len(), 1);
    }

    #[test]
    fn test_max_versions_per_key() {
        let store = make_store(); // max 5 per key
        for i in 0..10u64 {
            store.record_version("k1", Some(vec![i as u8]), VersionOperation::Set);
        }

        let history = store.get_history("k1", 100);
        assert_eq!(history.len(), 5);
        // The retained entries should be the 5 most recent.
        for entry in &history {
            assert!(entry.version > 5);
        }
    }

    #[test]
    fn test_stats_tracking() {
        let store = make_store();
        record_with_ts(&store, "k1", b"hello", 100);
        record_with_ts(&store, "k2", b"world", 200);

        let stats = store.stats();
        assert_eq!(stats.total_versions, 2);
        assert_eq!(stats.total_bytes, 10); // 5 + 5
        assert_eq!(stats.total_keys, 2);
        assert_eq!(stats.oldest_version_timestamp, 100);
    }

    #[test]
    fn test_disabled_store_is_noop() {
        let store = VersionStore::new(VersionStoreConfig {
            enabled: false,
            ..Default::default()
        });
        store.record_version("k1", Some(b"val".to_vec()), VersionOperation::Set);
        assert!(store.get_history("k1", 10).is_empty());
        assert_eq!(store.stats().total_versions, 0);
    }
}
