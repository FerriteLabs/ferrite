//! Temporal Index
//!
//! Index for efficient temporal queries. Maps keys to their version history.

use bytes::Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use super::retention::RetentionPolicy;

/// Temporal index for efficient point-in-time queries
pub struct TemporalIndex {
    /// Primary index: key -> version history
    index: DashMap<Bytes, VersionChain>,
    /// Retention policy
    retention: RetentionPolicy,
    /// Statistics
    stats: TemporalStats,
    /// Global sequence number for ordering
    sequence: AtomicU64,
}

impl TemporalIndex {
    /// Create a new temporal index
    pub fn new(retention: RetentionPolicy) -> Self {
        Self {
            index: DashMap::new(),
            retention,
            stats: TemporalStats::default(),
            sequence: AtomicU64::new(0),
        }
    }

    /// Record a new version for a key
    pub fn record_version(&self, key: Bytes, entry: VersionEntry) {
        self.stats.total_versions.fetch_add(1, Ordering::Relaxed);

        self.index
            .entry(key.clone())
            .and_modify(|chain| {
                chain.add_version(entry.clone());
            })
            .or_insert_with(|| {
                self.stats.keys_tracked.fetch_add(1, Ordering::Relaxed);
                let mut chain = VersionChain::new();
                chain.add_version(entry);
                chain
            });
    }

    /// Get the next sequence number
    pub fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::SeqCst)
    }

    /// Get the version active at a specific timestamp
    pub fn version_at(&self, key: &Bytes, timestamp: SystemTime) -> Option<VersionEntry> {
        self.index
            .get(key)
            .and_then(|chain| chain.version_at(timestamp).cloned())
    }

    /// Get all versions for a key within a time range
    pub fn versions_in_range(
        &self,
        key: &Bytes,
        from: Option<SystemTime>,
        to: Option<SystemTime>,
        limit: usize,
        ascending: bool,
    ) -> Vec<VersionEntry> {
        self.index
            .get(key)
            .map(|chain| chain.versions_in_range(from, to, limit, ascending))
            .unwrap_or_default()
    }

    /// Get version count for a key
    pub fn version_count(&self, key: &Bytes) -> usize {
        self.index.get(key).map(|chain| chain.len()).unwrap_or(0)
    }

    /// Get the first (oldest) version
    pub fn first_version(&self, key: &Bytes) -> Option<VersionEntry> {
        self.index.get(key).and_then(|chain| chain.first().cloned())
    }

    /// Get the last (newest) version
    pub fn last_version(&self, key: &Bytes) -> Option<VersionEntry> {
        self.index.get(key).and_then(|chain| chain.last().cloned())
    }

    /// Check if key has any history
    pub fn has_history(&self, key: &Bytes) -> bool {
        self.index.contains_key(key)
    }

    /// Get statistics
    pub fn stats(&self) -> TemporalStatsSnapshot {
        TemporalStatsSnapshot {
            keys_tracked: self.stats.keys_tracked.load(Ordering::Relaxed),
            total_versions: self.stats.total_versions.load(Ordering::Relaxed),
            versions_pruned: self.stats.versions_pruned.load(Ordering::Relaxed),
        }
    }

    /// Apply retention policy to all keys
    pub fn apply_retention(&self, now: SystemTime) -> RetentionResult {
        let mut versions_removed = 0u64;
        let mut keys_removed = 0u64;

        for mut entry in self.index.iter_mut() {
            let key = entry.key().clone();
            let chain = entry.value_mut();

            // Get effective policy for this key
            let effective_policy = self.retention.effective_for_key(&key);

            let removed = chain.apply_retention(&effective_policy, now);
            versions_removed += removed as u64;

            // If chain is empty after retention, mark for removal
            if chain.is_empty() {
                keys_removed += 1;
            }
        }

        // Remove empty chains
        self.index.retain(|_, chain| !chain.is_empty());

        self.stats
            .versions_pruned
            .fetch_add(versions_removed, Ordering::Relaxed);

        RetentionResult {
            versions_removed,
            keys_removed,
        }
    }

    /// Get number of tracked keys
    pub fn key_count(&self) -> usize {
        self.index.len()
    }

    /// Get retention policy
    pub fn retention(&self) -> &RetentionPolicy {
        &self.retention
    }

    /// Update retention policy
    pub fn set_retention(&mut self, policy: RetentionPolicy) {
        self.retention = policy;
    }
}

impl Default for TemporalIndex {
    fn default() -> Self {
        Self::new(RetentionPolicy::default())
    }
}

/// Chain of versions for a single key
#[derive(Clone, Debug)]
pub struct VersionChain {
    /// Sorted by timestamp (newest first for faster recent queries)
    versions: Vec<VersionEntry>,
    /// Cached current version for fast present-time queries
    current: Option<VersionEntry>,
}

impl VersionChain {
    /// Create a new empty version chain
    pub fn new() -> Self {
        Self {
            versions: Vec::new(),
            current: None,
        }
    }

    /// Add a new version (assumed to be newer than existing)
    pub fn add_version(&mut self, entry: VersionEntry) {
        // Update current
        self.current = Some(entry.clone());

        // Insert at front (newest first)
        self.versions.insert(0, entry);
    }

    /// Get version active at a specific timestamp
    pub fn version_at(&self, timestamp: SystemTime) -> Option<&VersionEntry> {
        // Binary search - versions are sorted newest first
        // Find first version with timestamp <= target
        self.versions.iter().find(|v| v.timestamp <= timestamp)
    }

    /// Get versions in a time range
    pub fn versions_in_range(
        &self,
        from: Option<SystemTime>,
        to: Option<SystemTime>,
        limit: usize,
        ascending: bool,
    ) -> Vec<VersionEntry> {
        let iter = self.versions.iter().filter(|v| {
            let after_from = from.map_or(true, |f| v.timestamp >= f);
            let before_to = to.map_or(true, |t| v.timestamp <= t);
            after_from && before_to
        });

        if ascending {
            // Reverse since we store newest first
            iter.take(limit)
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect()
        } else {
            iter.take(limit).cloned().collect()
        }
    }

    /// Get the first (oldest) version
    pub fn first(&self) -> Option<&VersionEntry> {
        self.versions.last()
    }

    /// Get the last (newest) version
    pub fn last(&self) -> Option<&VersionEntry> {
        self.versions.first()
    }

    /// Get the current version
    pub fn current(&self) -> Option<&VersionEntry> {
        self.current.as_ref()
    }

    /// Get number of versions
    pub fn len(&self) -> usize {
        self.versions.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.versions.is_empty()
    }

    /// Apply retention policy
    pub fn apply_retention(&mut self, policy: &RetentionPolicy, now: SystemTime) -> usize {
        let original_len = self.versions.len();

        // Always keep min_versions
        if self.versions.len() <= policy.min_versions {
            return 0;
        }

        // Apply max_versions limit
        if let Some(max) = policy.max_versions {
            if self.versions.len() > max {
                self.versions.truncate(max);
            }
        }

        // Apply max_age limit
        if let Some(max_age) = policy.max_age {
            if let Some(cutoff) = now.checked_sub(max_age) {
                // Keep at least min_versions
                let keep_count = policy.min_versions.max(
                    self.versions
                        .iter()
                        .take_while(|v| v.timestamp >= cutoff)
                        .count(),
                );
                self.versions.truncate(keep_count);
            }
        }

        original_len - self.versions.len()
    }
}

impl Default for VersionChain {
    fn default() -> Self {
        Self::new()
    }
}

/// A single version entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VersionEntry {
    /// Timestamp of this version
    pub timestamp: SystemTime,
    /// Logical sequence number (for ordering within same millisecond)
    pub sequence: u64,
    /// Position in HybridLog
    pub log_offset: LogOffset,
    /// Operation type
    pub op: OperationType,
    /// TTL at time of write (if any)
    pub ttl: Option<Duration>,
    /// Database index
    pub db: u8,
}

impl VersionEntry {
    /// Create a new version entry
    pub fn new(op: OperationType, log_offset: LogOffset, sequence: u64, db: u8) -> Self {
        Self {
            timestamp: SystemTime::now(),
            sequence,
            log_offset,
            op,
            ttl: None,
            db,
        }
    }

    /// Create with specific timestamp
    pub fn with_timestamp(
        op: OperationType,
        log_offset: LogOffset,
        sequence: u64,
        db: u8,
        timestamp: SystemTime,
    ) -> Self {
        Self {
            timestamp,
            sequence,
            log_offset,
            op,
            ttl: None,
            db,
        }
    }

    /// Set TTL
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Get timestamp as Unix seconds
    pub fn timestamp_secs(&self) -> u64 {
        self.timestamp
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    /// Get timestamp as Unix milliseconds
    pub fn timestamp_ms(&self) -> u64 {
        self.timestamp
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// Compact version entry for memory efficiency
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct CompactVersionEntry {
    /// Microseconds since epoch
    pub timestamp_micros: u64,
    /// Sequence number
    pub sequence: u64,
    /// Packed log offset
    pub log_offset_packed: u64,
    /// Operation type
    pub op: u8,
}

impl CompactVersionEntry {
    /// Size in bytes
    pub const fn size() -> usize {
        25 // 8 + 8 + 8 + 1
    }
}

/// Where in the log this version is stored
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum LogOffset {
    /// In mutable region (memory)
    Mutable(usize),
    /// In read-only region (mmap)
    ReadOnly {
        /// Page number
        page: u32,
        /// Offset within page
        offset: u32,
    },
    /// On disk
    Disk {
        /// File identifier
        file_id: u32,
        /// Offset in file
        offset: u64,
    },
    /// In cloud storage
    Cloud {
        /// Object key
        object_key: String,
        /// Offset in object
        offset: u64,
    },
    /// Inline small value
    Inline(Bytes),
}

impl LogOffset {
    /// Check if this is an in-memory offset
    pub fn is_memory(&self) -> bool {
        matches!(self, LogOffset::Mutable(_))
    }

    /// Check if this is a disk offset
    pub fn is_disk(&self) -> bool {
        matches!(self, LogOffset::Disk { .. } | LogOffset::ReadOnly { .. })
    }

    /// Check if this is a cloud offset
    pub fn is_cloud(&self) -> bool {
        matches!(self, LogOffset::Cloud { .. })
    }
}

/// Operation types that can be recorded
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum OperationType {
    // String operations
    /// SET command
    Set,
    /// DELETE command
    Delete,
    /// EXPIRE/TTL set
    Expire,
    /// APPEND command
    Append,
    /// INCR/DECR command
    Increment,
    /// GETSET/GETDEL command
    GetSet,

    // List operations
    /// LPUSH/RPUSH
    ListPush {
        /// Left or right
        direction: Direction,
    },
    /// LPOP/RPOP
    ListPop {
        /// Left or right
        direction: Direction,
    },
    /// LSET
    ListSet {
        /// Index
        index: i64,
    },
    /// LTRIM
    ListTrim,

    // Hash operations
    /// HSET
    HashSet {
        /// Field name
        field: Bytes,
    },
    /// HDEL
    HashDel {
        /// Field name
        field: Bytes,
    },
    /// HINCRBY
    HashIncr {
        /// Field name
        field: Bytes,
    },

    // Set operations
    /// SADD
    SetAdd,
    /// SREM
    SetRem,
    /// SPOP
    SetPop,

    // Sorted set operations
    /// ZADD
    ZSetAdd,
    /// ZREM
    ZSetRem,
    /// ZINCRBY
    ZSetIncr,

    // Stream operations
    /// XADD
    StreamAdd,
    /// XDEL
    StreamDel,
    /// XTRIM
    StreamTrim,

    // Generic
    /// Key renamed
    Rename,
    /// Key copied
    Copy,
    /// Key restored
    Restore,
}

impl OperationType {
    /// Get the operation name
    pub fn name(&self) -> &'static str {
        match self {
            OperationType::Set => "SET",
            OperationType::Delete => "DEL",
            OperationType::Expire => "EXPIRE",
            OperationType::Append => "APPEND",
            OperationType::Increment => "INCR",
            OperationType::GetSet => "GETSET",
            OperationType::ListPush {
                direction: Direction::Left,
            } => "LPUSH",
            OperationType::ListPush {
                direction: Direction::Right,
            } => "RPUSH",
            OperationType::ListPop {
                direction: Direction::Left,
            } => "LPOP",
            OperationType::ListPop {
                direction: Direction::Right,
            } => "RPOP",
            OperationType::ListSet { .. } => "LSET",
            OperationType::ListTrim => "LTRIM",
            OperationType::HashSet { .. } => "HSET",
            OperationType::HashDel { .. } => "HDEL",
            OperationType::HashIncr { .. } => "HINCRBY",
            OperationType::SetAdd => "SADD",
            OperationType::SetRem => "SREM",
            OperationType::SetPop => "SPOP",
            OperationType::ZSetAdd => "ZADD",
            OperationType::ZSetRem => "ZREM",
            OperationType::ZSetIncr => "ZINCRBY",
            OperationType::StreamAdd => "XADD",
            OperationType::StreamDel => "XDEL",
            OperationType::StreamTrim => "XTRIM",
            OperationType::Rename => "RENAME",
            OperationType::Copy => "COPY",
            OperationType::Restore => "RESTORE",
        }
    }

    /// Check if this is a deletion operation
    pub fn is_delete(&self) -> bool {
        matches!(self, OperationType::Delete)
    }

    /// Check if this is a write operation
    pub fn is_write(&self) -> bool {
        !self.is_delete()
    }
}

/// Direction for list operations
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub enum Direction {
    /// Left side
    Left,
    /// Right side
    Right,
}

/// Statistics for the temporal index
#[derive(Debug, Default)]
pub struct TemporalStats {
    /// Number of keys with history
    pub keys_tracked: AtomicU64,
    /// Total versions stored
    pub total_versions: AtomicU64,
    /// Versions removed by retention
    pub versions_pruned: AtomicU64,
}

/// Snapshot of temporal stats
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TemporalStatsSnapshot {
    /// Number of keys with history
    pub keys_tracked: u64,
    /// Total versions stored
    pub total_versions: u64,
    /// Versions removed by retention
    pub versions_pruned: u64,
}

/// Result of retention cleanup
#[derive(Clone, Debug)]
pub struct RetentionResult {
    /// Number of versions removed
    pub versions_removed: u64,
    /// Number of keys fully removed
    pub keys_removed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_timestamp(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn test_version_chain_add() {
        let mut chain = VersionChain::new();

        let v1 = VersionEntry::with_timestamp(
            OperationType::Set,
            LogOffset::Mutable(0),
            1,
            0,
            test_timestamp(1000),
        );
        chain.add_version(v1);

        let v2 = VersionEntry::with_timestamp(
            OperationType::Set,
            LogOffset::Mutable(1),
            2,
            0,
            test_timestamp(2000),
        );
        chain.add_version(v2);

        assert_eq!(chain.len(), 2);
        assert_eq!(chain.first().unwrap().sequence, 1);
        assert_eq!(chain.last().unwrap().sequence, 2);
    }

    #[test]
    fn test_version_at() {
        let mut chain = VersionChain::new();

        // Add versions at t=1000, 2000, 3000
        for (i, ts) in [1000u64, 2000, 3000].iter().enumerate() {
            let v = VersionEntry::with_timestamp(
                OperationType::Set,
                LogOffset::Mutable(i),
                i as u64 + 1,
                0,
                test_timestamp(*ts),
            );
            chain.add_version(v);
        }

        // Query at t=1500 should get version at t=1000
        let v = chain.version_at(test_timestamp(1500)).unwrap();
        assert_eq!(v.sequence, 1);

        // Query at t=2500 should get version at t=2000
        let v = chain.version_at(test_timestamp(2500)).unwrap();
        assert_eq!(v.sequence, 2);

        // Query at t=3500 should get version at t=3000
        let v = chain.version_at(test_timestamp(3500)).unwrap();
        assert_eq!(v.sequence, 3);

        // Query at t=500 should get no version
        assert!(chain.version_at(test_timestamp(500)).is_none());
    }

    #[test]
    fn test_version_chain_retention() {
        let mut chain = VersionChain::new();

        // Add 10 versions
        for i in 0..10 {
            let v = VersionEntry::with_timestamp(
                OperationType::Set,
                LogOffset::Mutable(i),
                i as u64,
                0,
                test_timestamp(1000 + i as u64 * 100),
            );
            chain.add_version(v);
        }

        assert_eq!(chain.len(), 10);

        // Apply retention with max_versions = 5
        let policy = RetentionPolicy {
            max_versions: Some(5),
            min_versions: 1,
            max_age: None, // Disable max_age for this test
            ..Default::default()
        };

        let removed = chain.apply_retention(&policy, SystemTime::now());
        assert_eq!(removed, 5);
        assert_eq!(chain.len(), 5);
    }

    #[test]
    fn test_temporal_index_record_and_query() {
        let index = TemporalIndex::default();
        let key = Bytes::from("test:key");

        // Record versions
        for i in 0..5 {
            let entry = VersionEntry::with_timestamp(
                OperationType::Set,
                LogOffset::Mutable(i),
                i as u64,
                0,
                test_timestamp(1000 + i as u64 * 100),
            );
            index.record_version(key.clone(), entry);
        }

        // Check stats
        let stats = index.stats();
        assert_eq!(stats.keys_tracked, 1);
        assert_eq!(stats.total_versions, 5);

        // Query at specific time
        let v = index.version_at(&key, test_timestamp(1250)).unwrap();
        assert_eq!(v.sequence, 2);
    }

    #[test]
    fn test_operation_type_names() {
        assert_eq!(OperationType::Set.name(), "SET");
        assert_eq!(OperationType::Delete.name(), "DEL");
        assert_eq!(
            OperationType::ListPush {
                direction: Direction::Left
            }
            .name(),
            "LPUSH"
        );
        assert_eq!(
            OperationType::HashSet {
                field: Bytes::from("f")
            }
            .name(),
            "HSET"
        );
    }

    #[test]
    fn test_log_offset_types() {
        let mutable = LogOffset::Mutable(100);
        assert!(mutable.is_memory());
        assert!(!mutable.is_disk());

        let disk = LogOffset::Disk {
            file_id: 1,
            offset: 200,
        };
        assert!(!disk.is_memory());
        assert!(disk.is_disk());

        let cloud = LogOffset::Cloud {
            object_key: "key".to_string(),
            offset: 300,
        };
        assert!(cloud.is_cloud());
    }
}
