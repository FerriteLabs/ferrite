//! Enhanced CDC Capture Manager
//!
//! Comprehensive mutation capture (all Redis operations), filtered CDC
//! subscriptions by key pattern and operation type, and checkpoint/resume
//! for restarting from the last committed position.

use super::event::{ChangeEvent, ChangeMetadata, ChangeSource, Operation};
use bytes::Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

// ---------------------------------------------------------------------------
// CdcFilter
// ---------------------------------------------------------------------------

/// A filter that decides which CDC events a subscriber receives.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CdcFilter {
    /// Key glob patterns (empty = match all).
    pub key_patterns: Vec<String>,
    /// Allowed operation names (empty = match all).
    pub operations: HashSet<String>,
    /// Allowed databases (empty = match all).
    pub databases: HashSet<u8>,
    /// Whether to include internal operations (EXPIRED, EVICTED).
    pub include_internal: bool,
}

impl CdcFilter {
    /// Returns `true` if the event matches this filter.
    pub fn matches(&self, event: &ChangeEvent) -> bool {
        // Exclude internal ops unless explicitly included
        if !self.include_internal && event.operation.is_internal() {
            return false;
        }

        // Key pattern matching
        if !self.key_patterns.is_empty() {
            let key_str = String::from_utf8_lossy(&event.key);
            if !self.key_patterns.iter().any(|p| glob_match(p, &key_str)) {
                return false;
            }
        }

        // Operation matching
        if !self.operations.is_empty() && !self.operations.contains(event.operation.name()) {
            return false;
        }

        // Database matching
        if !self.databases.is_empty() && !self.databases.contains(&event.db) {
            return false;
        }

        true
    }

    /// Builder: add a key pattern.
    pub fn with_key_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.key_patterns.push(pattern.into());
        self
    }

    /// Builder: add an allowed operation.
    pub fn with_operation(mut self, op: impl Into<String>) -> Self {
        self.operations.insert(op.into());
        self
    }

    /// Builder: include internal ops.
    pub fn with_internal(mut self) -> Self {
        self.include_internal = true;
        self
    }
}

// ---------------------------------------------------------------------------
// CdcCheckpoint
// ---------------------------------------------------------------------------

/// A checkpoint representing the last consumed position.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdcCheckpoint {
    /// Consumer / subscription name.
    pub consumer_id: String,
    /// Last acknowledged event ID.
    pub last_event_id: u64,
    /// Timestamp of the checkpoint.
    pub timestamp: SystemTime,
}

/// Stores checkpoints for resumable CDC consumption.
pub struct CdcCheckpointStore {
    /// consumer_id → checkpoint
    checkpoints: DashMap<String, CdcCheckpoint>,
}

impl CdcCheckpointStore {
    pub fn new() -> Self {
        Self {
            checkpoints: DashMap::new(),
        }
    }

    /// Save a checkpoint.
    pub fn save(&self, consumer_id: &str, last_event_id: u64) {
        self.checkpoints.insert(
            consumer_id.to_string(),
            CdcCheckpoint {
                consumer_id: consumer_id.to_string(),
                last_event_id,
                timestamp: SystemTime::now(),
            },
        );
    }

    /// Load the last checkpoint for a consumer.
    pub fn load(&self, consumer_id: &str) -> Option<CdcCheckpoint> {
        self.checkpoints.get(consumer_id).map(|v| v.clone())
    }

    /// Delete a checkpoint.
    pub fn delete(&self, consumer_id: &str) -> bool {
        self.checkpoints.remove(consumer_id).is_some()
    }

    /// List all checkpoints.
    pub fn list(&self) -> Vec<CdcCheckpoint> {
        self.checkpoints.iter().map(|e| e.value().clone()).collect()
    }
}

impl Default for CdcCheckpointStore {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// CdcCaptureManager
// ---------------------------------------------------------------------------

/// Captures all mutation operations and creates CDC events.
///
/// This is the central point where the storage engine calls into to record
/// mutations. Each method creates a fully-populated `ChangeEvent`.
pub struct CdcCaptureManager {
    next_id: AtomicU64,
    events_captured: AtomicU64,
}

impl CdcCaptureManager {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            events_captured: AtomicU64::new(0),
        }
    }

    fn next_event_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    fn make_event(
        &self,
        op: Operation,
        db: u8,
        key: Bytes,
        value: Option<Bytes>,
        old_value: Option<Bytes>,
        command: &str,
    ) -> ChangeEvent {
        self.events_captured.fetch_add(1, Ordering::Relaxed);
        ChangeEvent {
            id: self.next_event_id(),
            timestamp: SystemTime::now(),
            db,
            operation: op,
            key,
            value,
            old_value,
            metadata: ChangeMetadata::local(command),
        }
    }

    // -- String operations --------------------------------------------------

    pub fn capture_set(
        &self,
        db: u8,
        key: Bytes,
        value: Bytes,
        old_value: Option<Bytes>,
    ) -> ChangeEvent {
        self.make_event(Operation::Set, db, key, Some(value), old_value, "SET")
    }

    pub fn capture_setex(
        &self,
        db: u8,
        key: Bytes,
        value: Bytes,
        ttl_ms: u64,
        old_value: Option<Bytes>,
    ) -> ChangeEvent {
        self.make_event(
            Operation::SetEx { ttl_ms },
            db,
            key,
            Some(value),
            old_value,
            "SETEX",
        )
    }

    pub fn capture_append(
        &self,
        db: u8,
        key: Bytes,
        value: Bytes,
        old_value: Option<Bytes>,
    ) -> ChangeEvent {
        self.make_event(Operation::Append, db, key, Some(value), old_value, "APPEND")
    }

    pub fn capture_incr(&self, db: u8, key: Bytes, delta: i64, new_value: Bytes) -> ChangeEvent {
        self.make_event(
            Operation::Incr { delta },
            db,
            key,
            Some(new_value),
            None,
            "INCRBY",
        )
    }

    // -- Key operations -----------------------------------------------------

    pub fn capture_del(&self, db: u8, key: Bytes, old_value: Option<Bytes>) -> ChangeEvent {
        self.make_event(Operation::Del, db, key, None, old_value, "DEL")
    }

    pub fn capture_expire(&self, db: u8, key: Bytes, ttl_ms: u64) -> ChangeEvent {
        self.make_event(Operation::Expire { ttl_ms }, db, key, None, None, "EXPIRE")
    }

    pub fn capture_persist(&self, db: u8, key: Bytes) -> ChangeEvent {
        self.make_event(Operation::Persist, db, key, None, None, "PERSIST")
    }

    pub fn capture_rename(
        &self,
        db: u8,
        key: Bytes,
        new_key: Bytes,
        old_value: Option<Bytes>,
    ) -> ChangeEvent {
        self.make_event(
            Operation::Rename {
                new_key: new_key.clone(),
            },
            db,
            key,
            None,
            old_value,
            "RENAME",
        )
    }

    pub fn capture_expired(&self, db: u8, key: Bytes) -> ChangeEvent {
        let mut event = self.make_event(Operation::Expired, db, key, None, None, "EXPIRED");
        event.metadata.source = ChangeSource::Internal;
        event
    }

    pub fn capture_evicted(&self, db: u8, key: Bytes) -> ChangeEvent {
        let mut event = self.make_event(Operation::Evicted, db, key, None, None, "EVICTED");
        event.metadata.source = ChangeSource::Internal;
        event
    }

    // -- Hash operations ----------------------------------------------------

    pub fn capture_hset(
        &self,
        db: u8,
        key: Bytes,
        field: Bytes,
        value: Bytes,
        old_value: Option<Bytes>,
    ) -> ChangeEvent {
        self.make_event(
            Operation::HSet { field },
            db,
            key,
            Some(value),
            old_value,
            "HSET",
        )
    }

    pub fn capture_hdel(
        &self,
        db: u8,
        key: Bytes,
        field: Bytes,
        old_value: Option<Bytes>,
    ) -> ChangeEvent {
        self.make_event(Operation::HDel { field }, db, key, None, old_value, "HDEL")
    }

    pub fn capture_hincrby(
        &self,
        db: u8,
        key: Bytes,
        field: Bytes,
        delta: i64,
        new_value: Bytes,
    ) -> ChangeEvent {
        self.make_event(
            Operation::HIncrBy { field, delta },
            db,
            key,
            Some(new_value),
            None,
            "HINCRBY",
        )
    }

    // -- List operations ----------------------------------------------------

    pub fn capture_lpush(&self, db: u8, key: Bytes, count: usize, value: Bytes) -> ChangeEvent {
        self.make_event(
            Operation::LPush { count },
            db,
            key,
            Some(value),
            None,
            "LPUSH",
        )
    }

    pub fn capture_rpush(&self, db: u8, key: Bytes, count: usize, value: Bytes) -> ChangeEvent {
        self.make_event(
            Operation::RPush { count },
            db,
            key,
            Some(value),
            None,
            "RPUSH",
        )
    }

    pub fn capture_lpop(&self, db: u8, key: Bytes, old_value: Option<Bytes>) -> ChangeEvent {
        self.make_event(Operation::LPop, db, key, None, old_value, "LPOP")
    }

    pub fn capture_rpop(&self, db: u8, key: Bytes, old_value: Option<Bytes>) -> ChangeEvent {
        self.make_event(Operation::RPop, db, key, None, old_value, "RPOP")
    }

    // -- Set operations -----------------------------------------------------

    pub fn capture_sadd(&self, db: u8, key: Bytes, count: usize, value: Bytes) -> ChangeEvent {
        self.make_event(
            Operation::SAdd { count },
            db,
            key,
            Some(value),
            None,
            "SADD",
        )
    }

    pub fn capture_srem(&self, db: u8, key: Bytes, count: usize, value: Bytes) -> ChangeEvent {
        self.make_event(
            Operation::SRem { count },
            db,
            key,
            Some(value),
            None,
            "SREM",
        )
    }

    // -- Sorted set operations ----------------------------------------------

    pub fn capture_zadd(&self, db: u8, key: Bytes, count: usize, value: Bytes) -> ChangeEvent {
        self.make_event(
            Operation::ZAdd { count },
            db,
            key,
            Some(value),
            None,
            "ZADD",
        )
    }

    pub fn capture_zrem(&self, db: u8, key: Bytes, count: usize, value: Bytes) -> ChangeEvent {
        self.make_event(
            Operation::ZRem { count },
            db,
            key,
            Some(value),
            None,
            "ZREM",
        )
    }

    // -- Stream operations --------------------------------------------------

    pub fn capture_xadd(&self, db: u8, key: Bytes, entry_id: Bytes, value: Bytes) -> ChangeEvent {
        self.make_event(
            Operation::XAdd { id: entry_id },
            db,
            key,
            Some(value),
            None,
            "XADD",
        )
    }

    // -- Flush operations ---------------------------------------------------

    pub fn capture_flushdb(&self, db: u8) -> ChangeEvent {
        self.make_event(
            Operation::FlushDb,
            db,
            Bytes::from_static(b"*"),
            None,
            None,
            "FLUSHDB",
        )
    }

    pub fn capture_flushall(&self) -> ChangeEvent {
        self.make_event(
            Operation::FlushAll,
            0,
            Bytes::from_static(b"*"),
            None,
            None,
            "FLUSHALL",
        )
    }

    /// Total events captured.
    pub fn total_captured(&self) -> u64 {
        self.events_captured.load(Ordering::Relaxed)
    }
}

impl Default for CdcCaptureManager {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Glob helper
// ---------------------------------------------------------------------------

fn glob_match(pattern: &str, value: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let val: Vec<char> = value.chars().collect();
    glob_inner(&pat, &val)
}

fn glob_inner(pat: &[char], val: &[char]) -> bool {
    match (pat.first(), val.first()) {
        (None, None) => true,
        (Some('*'), _) => {
            glob_inner(&pat[1..], val) || (!val.is_empty() && glob_inner(pat, &val[1..]))
        }
        (Some('?'), Some(_)) => glob_inner(&pat[1..], &val[1..]),
        (Some(a), Some(b)) if a == b => glob_inner(&pat[1..], &val[1..]),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cdc_filter_match_all() {
        let filter = CdcFilter::default();
        let event = ChangeEvent::new(Operation::Set, Bytes::from_static(b"key"), 0);
        assert!(filter.matches(&event));
    }

    #[test]
    fn test_cdc_filter_key_pattern() {
        let filter = CdcFilter::default().with_key_pattern("user:*");
        let good = ChangeEvent::new(Operation::Set, Bytes::from_static(b"user:1"), 0);
        let bad = ChangeEvent::new(Operation::Set, Bytes::from_static(b"order:1"), 0);
        assert!(filter.matches(&good));
        assert!(!filter.matches(&bad));
    }

    #[test]
    fn test_cdc_filter_operation() {
        let filter = CdcFilter::default()
            .with_operation("SET")
            .with_operation("DEL");
        let set = ChangeEvent::new(Operation::Set, Bytes::from_static(b"k"), 0);
        let del = ChangeEvent::new(Operation::Del, Bytes::from_static(b"k"), 0);
        let append = ChangeEvent::new(Operation::Append, Bytes::from_static(b"k"), 0);
        assert!(filter.matches(&set));
        assert!(filter.matches(&del));
        assert!(!filter.matches(&append));
    }

    #[test]
    fn test_cdc_filter_excludes_internal() {
        let filter = CdcFilter::default();
        let expired = ChangeEvent::new(Operation::Expired, Bytes::from_static(b"k"), 0);
        assert!(!filter.matches(&expired));

        let filter_with = CdcFilter::default().with_internal();
        assert!(filter_with.matches(&expired));
    }

    #[test]
    fn test_checkpoint_store() {
        let store = CdcCheckpointStore::new();

        store.save("consumer1", 100);
        let cp = store.load("consumer1").unwrap();
        assert_eq!(cp.last_event_id, 100);

        store.save("consumer1", 200);
        let cp = store.load("consumer1").unwrap();
        assert_eq!(cp.last_event_id, 200);

        assert!(store.delete("consumer1"));
        assert!(store.load("consumer1").is_none());
    }

    #[test]
    fn test_capture_manager_set() {
        let mgr = CdcCaptureManager::new();
        let event = mgr.capture_set(0, Bytes::from("key"), Bytes::from("val"), None);

        assert!(matches!(event.operation, Operation::Set));
        assert_eq!(event.key, Bytes::from("key"));
        assert_eq!(event.value, Some(Bytes::from("val")));
        assert!(event.id > 0);
    }

    #[test]
    fn test_capture_manager_del() {
        let mgr = CdcCaptureManager::new();
        let event = mgr.capture_del(0, Bytes::from("key"), Some(Bytes::from("old")));

        assert!(matches!(event.operation, Operation::Del));
        assert_eq!(event.old_value, Some(Bytes::from("old")));
    }

    #[test]
    fn test_capture_manager_hash_ops() {
        let mgr = CdcCaptureManager::new();
        let event = mgr.capture_hset(
            0,
            Bytes::from("hash:1"),
            Bytes::from("field"),
            Bytes::from("value"),
            None,
        );
        assert!(matches!(event.operation, Operation::HSet { .. }));

        let event = mgr.capture_hdel(0, Bytes::from("hash:1"), Bytes::from("field"), None);
        assert!(matches!(event.operation, Operation::HDel { .. }));
    }

    #[test]
    fn test_capture_manager_list_ops() {
        let mgr = CdcCaptureManager::new();
        let event = mgr.capture_lpush(0, Bytes::from("list:1"), 2, Bytes::from("a,b"));
        assert!(matches!(event.operation, Operation::LPush { count: 2 }));

        let event = mgr.capture_rpop(0, Bytes::from("list:1"), Some(Bytes::from("b")));
        assert!(matches!(event.operation, Operation::RPop));
    }

    #[test]
    fn test_capture_manager_internal_ops() {
        let mgr = CdcCaptureManager::new();
        let expired = mgr.capture_expired(0, Bytes::from("key"));
        assert!(matches!(expired.operation, Operation::Expired));
        assert_eq!(expired.metadata.source, ChangeSource::Internal);

        let evicted = mgr.capture_evicted(0, Bytes::from("key"));
        assert!(matches!(evicted.operation, Operation::Evicted));
    }

    #[test]
    fn test_capture_manager_flush() {
        let mgr = CdcCaptureManager::new();
        let event = mgr.capture_flushdb(0);
        assert!(matches!(event.operation, Operation::FlushDb));

        let event = mgr.capture_flushall();
        assert!(matches!(event.operation, Operation::FlushAll));
    }

    #[test]
    fn test_capture_manager_counts() {
        let mgr = CdcCaptureManager::new();
        mgr.capture_set(0, Bytes::from("k"), Bytes::from("v"), None);
        mgr.capture_del(0, Bytes::from("k"), None);
        assert_eq!(mgr.total_captured(), 2);
    }

    #[test]
    fn test_checkpoint_list() {
        let store = CdcCheckpointStore::new();
        store.save("c1", 10);
        store.save("c2", 20);

        let all = store.list();
        assert_eq!(all.len(), 2);
    }
}
