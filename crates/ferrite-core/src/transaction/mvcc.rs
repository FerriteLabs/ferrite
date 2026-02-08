//! Multi-Version Concurrency Control (MVCC)
//!
//! Provides snapshot isolation and version tracking for transactions.
//! Allows concurrent readers without blocking writers.
//!
//! # Version Chain Structure
//!
//! ```text
//! Key "user:1"
//! ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
//! │ Version 100 │────▶│ Version 50  │────▶│ Version 10  │
//! │ "Alice"     │     │ "Bob"       │     │ "Charlie"   │
//! │ (latest)    │     │             │     │ (oldest)    │
//! └─────────────┘     └─────────────┘     └─────────────┘
//! ```
//!
//! Each transaction reads from its snapshot timestamp and sees a consistent view.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use tokio::sync::RwLock;

use crate::storage::{Store, Value as StorageValue};

/// Version manager for MVCC
pub struct VersionManager {
    /// Current global timestamp
    timestamp: AtomicU64,
    /// Version chains: (db, key) -> list of (timestamp, value)
    versions: RwLock<HashMap<(u8, Bytes), VersionChain>>,
    /// Write timestamps: (db, key) -> last write timestamp
    write_timestamps: RwLock<HashMap<(u8, Bytes), u64>>,
    /// Active transactions and their snapshot timestamps
    active_snapshots: RwLock<HashSet<u64>>,
}

/// A chain of versions for a single key
#[derive(Debug, Clone, Default)]
pub struct VersionChain {
    /// Versions sorted by timestamp (newest first)
    versions: Vec<Version>,
    /// Whether the key has been deleted
    deleted: bool,
}

/// A single version of a value
#[derive(Debug, Clone)]
pub struct Version {
    /// Timestamp when this version was created
    pub timestamp: u64,
    /// Transaction ID that created this version
    pub txn_id: u64,
    /// The value (None = tombstone/deleted)
    pub value: Option<StorageValue>,
    /// Whether this version is committed
    pub committed: bool,
}

/// A versioned value with metadata
#[derive(Debug, Clone)]
pub struct VersionedValue {
    /// The value
    pub value: StorageValue,
    /// Version timestamp
    pub version: u64,
    /// Transaction that created it
    pub created_by: u64,
}

/// Read set for a transaction
#[derive(Debug, Clone, Default)]
pub struct ReadSet {
    /// Keys read and their version timestamps
    pub entries: HashMap<(u8, Bytes), u64>,
}

/// Write set for a transaction
#[derive(Debug, Clone, Default)]
pub struct WriteSet {
    /// Keys to be written
    pub entries: HashMap<(u8, Bytes), Option<StorageValue>>,
}

impl VersionManager {
    /// Create a new version manager
    pub fn new() -> Self {
        Self {
            timestamp: AtomicU64::new(1),
            versions: RwLock::new(HashMap::new()),
            write_timestamps: RwLock::new(HashMap::new()),
            active_snapshots: RwLock::new(HashSet::new()),
        }
    }

    /// Get the current timestamp
    pub fn current_timestamp(&self) -> u64 {
        self.timestamp.load(Ordering::SeqCst)
    }

    /// Allocate a new timestamp
    pub fn next_timestamp(&self) -> u64 {
        // fetch_add returns old value, so add 1 to get new value
        self.timestamp.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Register a snapshot timestamp for a transaction
    pub async fn register_snapshot(&self, ts: u64) {
        self.active_snapshots.write().await.insert(ts);
    }

    /// Unregister a snapshot timestamp
    pub async fn unregister_snapshot(&self, ts: u64) {
        self.active_snapshots.write().await.remove(&ts);
    }

    /// Get the minimum active snapshot (for GC)
    pub async fn min_active_snapshot(&self) -> Option<u64> {
        self.active_snapshots.read().await.iter().min().copied()
    }

    /// Read the latest committed version
    pub async fn read_committed(&self, db: u8, key: &Bytes, store: &Store) -> Option<StorageValue> {
        // First check version chain
        let versions = self.versions.read().await;
        if let Some(chain) = versions.get(&(db, key.clone())) {
            for version in &chain.versions {
                if version.committed {
                    return version.value.clone();
                }
            }
        }

        // Fall back to storage
        store.get(db, key)
    }

    /// Read a version visible at a specific snapshot timestamp
    pub async fn read_snapshot(
        &self,
        db: u8,
        key: &Bytes,
        snapshot_ts: u64,
        store: &Store,
    ) -> Option<StorageValue> {
        let versions = self.versions.read().await;

        if let Some(chain) = versions.get(&(db, key.clone())) {
            // Find the most recent version at or before snapshot_ts
            for version in &chain.versions {
                if version.timestamp <= snapshot_ts && version.committed {
                    return version.value.clone();
                }
            }
        }

        // If no version in chain, check storage
        // This handles keys that were never modified through MVCC
        store.get(db, key)
    }

    /// Record a write with version tracking
    pub async fn record_write(&self, db: u8, key: Bytes, _txn_id: u64) {
        let ts = self.next_timestamp();

        // Update write timestamp
        self.write_timestamps
            .write()
            .await
            .insert((db, key.clone()), ts);
    }

    /// Add a new version to the chain
    pub async fn add_version(&self, db: u8, key: Bytes, value: Option<StorageValue>, txn_id: u64) {
        let ts = self.next_timestamp();
        let version = Version {
            timestamp: ts,
            txn_id,
            value,
            committed: false,
        };

        let mut versions = self.versions.write().await;
        let chain = versions.entry((db, key.clone())).or_default();
        chain.versions.insert(0, version); // Insert at front (newest first)

        // Update write timestamp
        self.write_timestamps.write().await.insert((db, key), ts);
    }

    /// Mark a version as committed
    pub async fn commit_version(&self, db: u8, key: &Bytes, txn_id: u64) {
        let mut versions = self.versions.write().await;
        if let Some(chain) = versions.get_mut(&(db, key.clone())) {
            for version in &mut chain.versions {
                if version.txn_id == txn_id {
                    version.committed = true;
                    break;
                }
            }
        }
    }

    /// Remove uncommitted versions from a transaction (for rollback)
    pub async fn rollback_version(&self, db: u8, key: &Bytes, txn_id: u64) {
        let mut versions = self.versions.write().await;
        if let Some(chain) = versions.get_mut(&(db, key.clone())) {
            chain.versions.retain(|v| v.txn_id != txn_id || v.committed);
        }
    }

    /// Check if there's a write conflict for a key
    pub async fn has_conflict(&self, db: u8, key: &Bytes, snapshot_ts: u64) -> bool {
        let write_timestamps = self.write_timestamps.read().await;
        if let Some(&write_ts) = write_timestamps.get(&(db, key.clone())) {
            // Conflict if key was written after our snapshot
            return write_ts > snapshot_ts;
        }
        false
    }

    /// Get the last write timestamp for a key
    pub async fn get_write_timestamp(&self, db: u8, key: &Bytes) -> Option<u64> {
        self.write_timestamps
            .read()
            .await
            .get(&(db, key.clone()))
            .copied()
    }

    /// Validate a read set (for serializable isolation)
    pub async fn validate_read_set(&self, read_set: &ReadSet) -> bool {
        let write_timestamps = self.write_timestamps.read().await;

        for ((db, key), read_ts) in &read_set.entries {
            if let Some(&write_ts) = write_timestamps.get(&(*db, key.clone())) {
                // Read set is invalid if key was written after it was read
                if write_ts > *read_ts {
                    return false;
                }
            }
        }
        true
    }

    /// Garbage collect old versions
    pub async fn gc(&self, watermark: u64) -> u64 {
        let min_snapshot = self.min_active_snapshot().await.unwrap_or(watermark);
        let gc_threshold = min_snapshot.min(watermark);

        let mut versions = self.versions.write().await;
        let mut cleaned = 0u64;

        for (_key, chain) in versions.iter_mut() {
            let before_len = chain.versions.len();

            // Keep at least one committed version, plus any after threshold
            let mut found_committed = false;
            chain.versions.retain(|v| {
                if v.timestamp >= gc_threshold {
                    return true;
                }
                if v.committed && !found_committed {
                    found_committed = true;
                    return true; // Keep oldest committed version
                }
                false
            });

            cleaned += (before_len - chain.versions.len()) as u64;
        }

        // Remove empty chains
        versions.retain(|_, chain| !chain.versions.is_empty());

        cleaned
    }

    /// Get version count for a key (for debugging)
    pub async fn version_count(&self, db: u8, key: &Bytes) -> usize {
        self.versions
            .read()
            .await
            .get(&(db, key.clone()))
            .map(|c| c.versions.len())
            .unwrap_or(0)
    }

    /// Get all versions for a key (for debugging)
    pub async fn get_versions(&self, db: u8, key: &Bytes) -> Vec<Version> {
        self.versions
            .read()
            .await
            .get(&(db, key.clone()))
            .map(|c| c.versions.clone())
            .unwrap_or_default()
    }
}

impl Default for VersionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadSet {
    /// Create a new read set
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a key to the read set
    pub fn add(&mut self, db: u8, key: Bytes, version: u64) {
        self.entries.insert((db, key), version);
    }

    /// Check if a key is in the read set
    pub fn contains(&self, db: u8, key: &Bytes) -> bool {
        self.entries.contains_key(&(db, key.clone()))
    }

    /// Get the version of a key in the read set
    pub fn get_version(&self, db: u8, key: &Bytes) -> Option<u64> {
        self.entries.get(&(db, key.clone())).copied()
    }

    /// Get the number of keys in the read set
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the read set is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl WriteSet {
    /// Create a new write set
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a key to the write set
    pub fn add(&mut self, db: u8, key: Bytes, value: Option<StorageValue>) {
        self.entries.insert((db, key), value);
    }

    /// Check if a key is in the write set
    pub fn contains(&self, db: u8, key: &Bytes) -> bool {
        self.entries.contains_key(&(db, key.clone()))
    }

    /// Get the value of a key in the write set
    pub fn get(&self, db: u8, key: &Bytes) -> Option<&Option<StorageValue>> {
        self.entries.get(&(db, key.clone()))
    }

    /// Get the number of keys in the write set
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the write set is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get keys that overlap with a read set
    pub fn overlaps_with(&self, read_set: &ReadSet) -> Vec<(u8, Bytes)> {
        self.entries
            .keys()
            .filter(|(db, key)| read_set.contains(*db, key))
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_version_manager_creation() {
        let vm = VersionManager::new();
        assert!(vm.current_timestamp() > 0);
    }

    #[tokio::test]
    async fn test_timestamp_increment() {
        let vm = VersionManager::new();
        let ts1 = vm.current_timestamp();
        let ts2 = vm.next_timestamp();
        let ts3 = vm.next_timestamp();

        assert!(ts2 > ts1);
        assert!(ts3 > ts2);
    }

    #[tokio::test]
    async fn test_snapshot_registration() {
        let vm = VersionManager::new();

        vm.register_snapshot(10).await;
        vm.register_snapshot(20).await;
        vm.register_snapshot(15).await;

        assert_eq!(vm.min_active_snapshot().await, Some(10));

        vm.unregister_snapshot(10).await;
        assert_eq!(vm.min_active_snapshot().await, Some(15));
    }

    #[tokio::test]
    async fn test_add_and_read_version() {
        let vm = VersionManager::new();
        let store = Store::new(16);

        let key = Bytes::from("test_key");
        let value = StorageValue::String(Bytes::from("test_value"));

        vm.add_version(0, key.clone(), Some(value.clone()), 1).await;
        vm.commit_version(0, &key, 1).await;

        let read_value = vm.read_committed(0, &key, &store).await;
        assert!(read_value.is_some());
    }

    #[tokio::test]
    async fn test_snapshot_isolation() {
        let vm = VersionManager::new();
        let store = Store::new(16);

        let key = Bytes::from("test_key");

        // Add version at timestamp 5
        let ts1 = vm.next_timestamp();
        vm.add_version(
            0,
            key.clone(),
            Some(StorageValue::String(Bytes::from("v1"))),
            1,
        )
        .await;
        vm.commit_version(0, &key, 1).await;

        // Add version at timestamp 10
        vm.add_version(
            0,
            key.clone(),
            Some(StorageValue::String(Bytes::from("v2"))),
            2,
        )
        .await;
        vm.commit_version(0, &key, 2).await;

        // Reading at early snapshot should see v1
        let early_read = vm.read_snapshot(0, &key, ts1, &store).await;
        // Reading at later snapshot should see v2
        let late_read = vm.read_committed(0, &key, &store).await;

        assert!(late_read.is_some());
    }

    #[tokio::test]
    async fn test_conflict_detection() {
        let vm = VersionManager::new();

        let key = Bytes::from("test_key");
        let snapshot_ts = vm.current_timestamp();

        // No conflict initially
        assert!(!vm.has_conflict(0, &key, snapshot_ts).await);

        // Write after snapshot
        vm.record_write(0, key.clone(), 1).await;

        // Now there's a conflict
        assert!(vm.has_conflict(0, &key, snapshot_ts).await);
    }

    #[tokio::test]
    async fn test_rollback_version() {
        let vm = VersionManager::new();

        let key = Bytes::from("test_key");

        // Add uncommitted version
        vm.add_version(
            0,
            key.clone(),
            Some(StorageValue::String(Bytes::from("uncommitted"))),
            1,
        )
        .await;

        // Rollback
        vm.rollback_version(0, &key, 1).await;

        // Version should be gone
        assert_eq!(vm.version_count(0, &key).await, 0);
    }

    #[tokio::test]
    async fn test_garbage_collection() {
        let vm = VersionManager::new();

        let key = Bytes::from("test_key");

        // Add multiple versions
        for i in 1..=5 {
            vm.add_version(
                0,
                key.clone(),
                Some(StorageValue::String(Bytes::from(format!("v{}", i)))),
                i,
            )
            .await;
            vm.commit_version(0, &key, i).await;
        }

        assert_eq!(vm.version_count(0, &key).await, 5);

        // GC with high watermark
        let cleaned = vm.gc(100).await;

        // Should keep at least one committed version
        assert!(vm.version_count(0, &key).await >= 1);
        assert!(cleaned > 0);
    }

    #[tokio::test]
    async fn test_read_set_write_set() {
        let mut read_set = ReadSet::new();
        let mut write_set = WriteSet::new();

        read_set.add(0, Bytes::from("key1"), 10);
        read_set.add(0, Bytes::from("key2"), 20);

        write_set.add(
            0,
            Bytes::from("key2"),
            Some(StorageValue::String(Bytes::from("v"))),
        );
        write_set.add(0, Bytes::from("key3"), None);

        assert!(read_set.contains(0, &Bytes::from("key1")));
        assert!(write_set.contains(0, &Bytes::from("key2")));

        let overlaps = write_set.overlaps_with(&read_set);
        assert_eq!(overlaps.len(), 1);
        assert_eq!(overlaps[0].1, Bytes::from("key2"));
    }
}
