//! In-memory storage implementation
//!
//! This module implements the in-memory storage engine using DashMap for
//! concurrent access.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use super::streams::Stream;
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;
use std::time::{Instant, SystemTime};

/// Process-wide monotonic start instant for coarse-grained LRU timestamps.
static START: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Returns milliseconds elapsed since the process-wide `START` instant.
#[inline]
fn coarse_now_ms() -> u64 {
    Instant::now().duration_since(*START).as_millis() as u64
}

/// Value types stored in the database
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    /// String value
    String(Bytes),
    /// List value (doubly-ended queue for efficient push/pop at both ends)
    List(VecDeque<Bytes>),
    /// Hash value (field-value mapping)
    Hash(HashMap<Bytes, Bytes>),
    /// Set value (unordered collection of unique elements)
    Set(HashSet<Bytes>),
    /// Sorted set value (ordered by score, with member-to-score mapping)
    SortedSet {
        /// Members ordered by (score, member) for range queries
        by_score: BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
        /// Member to score mapping for O(1) score lookups
        by_member: HashMap<Bytes, f64>,
    },
    /// Stream value (log-like data structure)
    Stream(Stream),
    /// HyperLogLog value (probabilistic cardinality estimation)
    HyperLogLog(Vec<u8>),
}

/// A single entry in the database
#[derive(Debug)]
pub struct Entry {
    /// The stored value
    pub value: Value,

    /// Expiration time (if any)
    pub expires_at: Option<SystemTime>,

    /// Access count for LRU tracking
    pub access_count: AtomicU64,

    /// Last access time as milliseconds since a process-wide monotonic start
    /// instant. Globally comparable across entries for correct LRU eviction.
    pub last_access: AtomicU64,
}

impl Entry {
    /// Create a new entry with a value
    pub fn new(value: Value) -> Self {
        let now = coarse_now_ms();
        Self {
            value,
            expires_at: None,
            access_count: AtomicU64::new(0),
            last_access: AtomicU64::new(now),
        }
    }

    /// Create a new entry with a value and expiration
    pub fn with_expiry(value: Value, expires_at: SystemTime) -> Self {
        let now = coarse_now_ms();
        Self {
            value,
            expires_at: Some(expires_at),
            access_count: AtomicU64::new(0),
            last_access: AtomicU64::new(now),
        }
    }

    /// Check if the entry has expired
    #[inline]
    pub fn is_expired(&self) -> bool {
        self.expires_at
            .map(|exp| SystemTime::now() >= exp)
            .unwrap_or(false)
    }

    /// Record an access to this entry
    pub fn record_access(&self) {
        self.access_count.fetch_add(1, Ordering::Relaxed);
        self.last_access.store(coarse_now_ms(), Ordering::Relaxed);
    }
}

impl Clone for Entry {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            expires_at: self.expires_at,
            access_count: AtomicU64::new(self.access_count.load(Ordering::Relaxed)),
            last_access: AtomicU64::new(self.last_access.load(Ordering::Relaxed)),
        }
    }
}

/// A single database (Redis supports 16 by default)
#[derive(Debug)]
pub struct Database {
    /// The underlying concurrent hashmap
    data: DashMap<Bytes, Entry>,
}

impl Database {
    /// Create a new empty database
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
        }
    }

    /// Get a value by key
    pub fn get(&self, key: &Bytes) -> Option<Value> {
        let entry = self.data.get(key)?;

        // Lazy expiration check
        if entry.is_expired() {
            drop(entry);
            self.data.remove(key);
            return None;
        }

        entry.record_access();
        Some(entry.value.clone())
    }

    /// Set a value
    pub fn set(&self, key: Bytes, value: Value) {
        self.data.insert(key, Entry::new(value));
    }

    /// Set a value with expiration
    pub fn set_with_expiry(&self, key: Bytes, value: Value, expires_at: SystemTime) {
        self.data.insert(key, Entry::with_expiry(value, expires_at));
    }

    /// Delete a key
    pub fn del(&self, key: &Bytes) -> bool {
        self.data.remove(key).is_some()
    }

    /// Check if a key exists
    #[inline]
    pub fn exists(&self, key: &Bytes) -> bool {
        if let Some(entry) = self.data.get(key) {
            if entry.is_expired() {
                drop(entry);
                self.data.remove(key);
                return false;
            }
            true
        } else {
            false
        }
    }

    /// Get the TTL of a key in seconds
    /// Returns None if key doesn't exist, Some(-1) if no expiry, Some(-2) if expired
    pub fn ttl(&self, key: &Bytes) -> Option<i64> {
        let entry = self.data.get(key)?;

        match entry.expires_at {
            None => Some(-1),
            Some(exp) => {
                let now = SystemTime::now();
                if now >= exp {
                    drop(entry);
                    self.data.remove(key);
                    Some(-2)
                } else {
                    Some(exp.duration_since(now).unwrap_or_default().as_secs() as i64)
                }
            }
        }
    }

    /// Set expiration on a key
    pub fn expire(&self, key: &Bytes, expires_at: SystemTime) -> bool {
        if let Some(mut entry) = self.data.get_mut(key) {
            if entry.is_expired() {
                drop(entry);
                self.data.remove(key);
                return false;
            }
            entry.expires_at = Some(expires_at);
            true
        } else {
            false
        }
    }

    /// Remove expiration from a key (PERSIST command)
    pub fn persist(&self, key: &Bytes) -> bool {
        if let Some(mut entry) = self.data.get_mut(key) {
            if entry.is_expired() {
                drop(entry);
                self.data.remove(key);
                return false;
            }
            let had_expiry = entry.expires_at.is_some();
            entry.expires_at = None;
            had_expiry
        } else {
            false
        }
    }

    /// Get the number of keys in the database
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the database is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get a mutable reference to an entry for atomic operations
    pub fn get_entry_mut(
        &self,
        key: &Bytes,
    ) -> Option<dashmap::mapref::one::RefMut<'_, Bytes, Entry>> {
        let entry = self.data.get_mut(key)?;

        if entry.is_expired() {
            drop(entry);
            self.data.remove(key);
            return None;
        }

        Some(entry)
    }

    /// Insert an entry only if it doesn't exist
    pub fn set_nx(&self, key: Bytes, value: Value) -> bool {
        // Check if key exists (with expiration check)
        if self.exists(&key) {
            return false;
        }
        self.data.insert(key, Entry::new(value));
        true
    }

    /// Get all keys in the database (for backup/iteration)
    pub fn keys(&self) -> Vec<Bytes> {
        self.data
            .iter()
            .filter(|entry| !entry.is_expired())
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Iterate over all key-value pairs in the database
    /// The callback receives (key, value) for each non-expired entry
    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(&[u8], &Value),
    {
        for entry in self.data.iter() {
            if !entry.is_expired() {
                f(entry.key(), &entry.value);
            }
        }
    }

    /// Get an entry (value + TTL info) for backup
    pub fn get_entry(&self, key: &Bytes) -> Option<(Value, Option<SystemTime>)> {
        let entry = self.data.get(key)?;

        if entry.is_expired() {
            drop(entry);
            self.data.remove(key);
            return None;
        }

        entry.record_access();
        Some((entry.value.clone(), entry.expires_at))
    }

    /// Clear all keys in the database (FLUSHDB)
    pub fn clear(&self) {
        self.data.clear();
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

use std::sync::Arc;

use crate::config::{StorageBackendType, StorageConfig};
use crate::optimizer::profiler::{CommandKind, WorkloadProfiler};
use crate::storage::hybridlog::{HybridLog, HybridLogConfig, HybridLogStats};

/// Storage backend wrapper
pub enum StorageBackend {
    /// Memory backend using DashMap
    Memory(Vec<RwLock<Database>>),
    /// HybridLog backend with tiered storage
    HybridLog(Vec<HybridLog>),
}

/// The main store containing all databases
pub struct Store {
    /// The storage backend
    backend: StorageBackend,
    /// Number of databases
    num_dbs: usize,
    /// Optional workload profiler for adaptive tiering
    profiler: Option<Arc<WorkloadProfiler>>,
}

impl Store {
    /// Create a new store with the specified number of databases (memory backend)
    pub fn new(num_databases: u8) -> Self {
        let mut databases = Vec::with_capacity(num_databases as usize);
        for _ in 0..num_databases {
            databases.push(RwLock::new(Database::new()));
        }
        Self {
            backend: StorageBackend::Memory(databases),
            num_dbs: num_databases as usize,
            profiler: None,
        }
    }

    /// Create a store with the specified configuration
    pub fn with_config(config: &StorageConfig) -> std::io::Result<Self> {
        match config.backend {
            StorageBackendType::Memory => Ok(Self::new(config.databases)),
            StorageBackendType::HybridLog => {
                let mut hybrid_logs = Vec::with_capacity(config.databases as usize);
                // Pre-allocate the vector to avoid repeated heap allocations
                // during mutable region compaction. This reduces pressure on the
                // allocator when migrating entries from the mutable to read-only region.
                for db_idx in 0..config.databases {
                    let hl_config = HybridLogConfig {
                        mutable_size: config.hybridlog_mutable_size,
                        readonly_size: config.hybridlog_readonly_size,
                        disk_size: 0, // Unlimited
                        data_dir: config.data_dir.join(format!("db{}", db_idx)),
                        auto_tiering: config.hybridlog_auto_tiering,
                        migration_threshold: config.hybridlog_migration_threshold,
                    };
                    let hl = HybridLog::new(hl_config)?;
                    hybrid_logs.push(hl);
                }
                Ok(Self {
                    backend: StorageBackend::HybridLog(hybrid_logs),
                    num_dbs: config.databases as usize,
                    profiler: None,
                })
            }
        }
    }

    /// Set the workload profiler for access pattern tracking.
    pub fn set_profiler(&mut self, profiler: Arc<WorkloadProfiler>) {
        self.profiler = Some(profiler);
    }

    /// Get the profiler reference.
    pub fn profiler(&self) -> Option<&Arc<WorkloadProfiler>> {
        self.profiler.as_ref()
    }

    /// Get the storage backend type
    pub fn backend_type(&self) -> StorageBackendType {
        match &self.backend {
            StorageBackend::Memory(_) => StorageBackendType::Memory,
            StorageBackend::HybridLog(_) => StorageBackendType::HybridLog,
        }
    }

    /// Get HybridLog statistics (returns None if not using HybridLog backend)
    pub fn hybridlog_stats(&self) -> Option<Vec<HybridLogStats>> {
        match &self.backend {
            StorageBackend::Memory(_) => None,
            StorageBackend::HybridLog(logs) => Some(logs.iter().map(|hl| hl.stats()).collect()),
        }
    }

    /// Get aggregated HybridLog statistics across all databases
    pub fn hybridlog_stats_aggregated(&self) -> Option<HybridLogStats> {
        let stats = self.hybridlog_stats()?;
        if stats.is_empty() {
            return None;
        }

        let mut total = HybridLogStats::default();
        for s in stats {
            total.key_count += s.key_count;
            total.mutable_bytes += s.mutable_bytes;
            total.readonly_bytes += s.readonly_bytes;
            total.disk_bytes += s.disk_bytes;
            total.mutable_entries += s.mutable_entries;
            total.readonly_entries += s.readonly_entries;
            total.disk_entries += s.disk_entries;
            total.get_ops += s.get_ops;
            total.set_ops += s.set_ops;
            total.del_ops += s.del_ops;
            total.mutable_to_readonly_migrations += s.mutable_to_readonly_migrations;
            total.readonly_to_disk_migrations += s.readonly_to_disk_migrations;
        }
        Some(total)
    }

    /// Reset statistics (for CONFIG RESETSTAT)
    pub fn reset_stats(&self) {
        match &self.backend {
            StorageBackend::Memory(_) => {
                // Memory backend has no operation stats to reset
            }
            StorageBackend::HybridLog(logs) => {
                for log in logs {
                    log.reset_stats();
                }
            }
        }
    }

    /// Get a reference to a database (only available for Memory backend)
    pub fn database(&self, index: u8) -> Option<&RwLock<Database>> {
        match &self.backend {
            StorageBackend::Memory(databases) => Some(&databases[index as usize]),
            StorageBackend::HybridLog(_) => None,
        }
    }

    /// Get a value from a specific database
    pub fn get(&self, db: u8, key: &Bytes) -> Option<Value> {
        if let Some(profiler) = &self.profiler {
            let key_str = String::from_utf8_lossy(key);
            profiler.record_key_access(&key_str, CommandKind::Read, None);
        }
        match &self.backend {
            StorageBackend::Memory(databases) => databases[db as usize].read().get(key),
            StorageBackend::HybridLog(logs) => {
                // HybridLog stores raw bytes, need to deserialize
                let data = logs[db as usize].get(key)?;
                let entry = super::backend::SerializableEntry::from_bytes(&data).ok()?;
                if entry.is_expired() {
                    logs[db as usize].del(key);
                    return None;
                }
                entry.to_value().ok().map(|(v, _)| v)
            }
        }
    }

    /// Get a value and expiration from a specific database
    pub fn get_entry(&self, db: u8, key: &Bytes) -> Option<(Value, Option<SystemTime>)> {
        match &self.backend {
            StorageBackend::Memory(databases) => databases[db as usize].read().get_entry(key),
            StorageBackend::HybridLog(logs) => {
                let data = logs[db as usize].get(key)?;
                let entry = super::backend::SerializableEntry::from_bytes(&data).ok()?;
                if entry.is_expired() {
                    logs[db as usize].del(key);
                    return None;
                }
                entry.to_value().ok()
            }
        }
    }

    /// Set a value in a specific database
    pub fn set(&self, db: u8, key: Bytes, value: Value) {
        if let Some(profiler) = &self.profiler {
            let key_str = String::from_utf8_lossy(&key);
            profiler.record_key_access(&key_str, CommandKind::Write, None);
        }
        match &self.backend {
            StorageBackend::Memory(databases) => databases[db as usize].read().set(key, value),
            StorageBackend::HybridLog(logs) => {
                if let Ok(entry) = super::backend::SerializableEntry::from_value(&value, None) {
                    if let Ok(data) = entry.to_bytes() {
                        let _ = logs[db as usize].set(key, data);
                    }
                }
            }
        }
    }

    /// Set a value and optional expiration
    pub fn set_entry(&self, db: u8, key: Bytes, value: Value, expires_at: Option<SystemTime>) {
        match expires_at {
            Some(expires_at) => self.set_with_expiry(db, key, value, expires_at),
            None => self.set(db, key, value),
        }
    }

    /// Set a value with expiration
    pub fn set_with_expiry(&self, db: u8, key: Bytes, value: Value, expires_at: SystemTime) {
        match &self.backend {
            StorageBackend::Memory(databases) => databases[db as usize]
                .read()
                .set_with_expiry(key, value, expires_at),
            StorageBackend::HybridLog(logs) => {
                if let Ok(entry) =
                    super::backend::SerializableEntry::from_value(&value, Some(expires_at))
                {
                    if let Ok(data) = entry.to_bytes() {
                        let _ = logs[db as usize].set(key, data);
                    }
                }
            }
        }
    }

    /// Delete keys from a specific database
    pub fn del(&self, db: u8, keys: &[Bytes]) -> i64 {
        match &self.backend {
            StorageBackend::Memory(databases) => {
                let database = databases[db as usize].read();
                keys.iter().filter(|k| database.del(k)).count() as i64
            }
            StorageBackend::HybridLog(logs) => {
                keys.iter().filter(|k| logs[db as usize].del(k)).count() as i64
            }
        }
    }

    /// Check if keys exist in a specific database
    pub fn exists(&self, db: u8, keys: &[Bytes]) -> i64 {
        match &self.backend {
            StorageBackend::Memory(databases) => {
                let database = databases[db as usize].read();
                keys.iter().filter(|k| database.exists(k)).count() as i64
            }
            StorageBackend::HybridLog(logs) => {
                keys.iter().filter(|k| logs[db as usize].exists(k)).count() as i64
            }
        }
    }

    /// Get TTL of a key
    pub fn ttl(&self, db: u8, key: &Bytes) -> Option<i64> {
        match &self.backend {
            StorageBackend::Memory(databases) => databases[db as usize].read().ttl(key),
            StorageBackend::HybridLog(logs) => logs[db as usize].ttl(key),
        }
    }

    /// Set expiration on a key
    pub fn expire(&self, db: u8, key: &Bytes, expires_at: SystemTime) -> bool {
        match &self.backend {
            StorageBackend::Memory(databases) => {
                databases[db as usize].read().expire(key, expires_at)
            }
            StorageBackend::HybridLog(logs) => logs[db as usize].expire(key, expires_at),
        }
    }

    /// Remove expiration from a key (PERSIST)
    pub fn persist(&self, db: u8, key: &Bytes) -> bool {
        match &self.backend {
            StorageBackend::Memory(databases) => databases[db as usize].read().persist(key),
            StorageBackend::HybridLog(logs) => {
                if let Some(data) = logs[db as usize].get(key) {
                    if let Ok(entry) = super::backend::SerializableEntry::from_bytes(&data) {
                        if entry.is_expired() {
                            logs[db as usize].del(key);
                            return false;
                        }
                        let had_expiry = entry.expires_at_ms.is_some();
                        if let Ok((value, _)) = entry.to_value() {
                            if let Ok(serialized) =
                                super::backend::SerializableEntry::from_value(&value, None)
                            {
                                if let Ok(bytes) = serialized.to_bytes() {
                                    if logs[db as usize].set(key.clone(), bytes).is_ok() {
                                        return had_expiry;
                                    }
                                }
                            }
                        }
                    }
                }
                false
            }
        }
    }

    /// Update last access time for keys (TOUCH)
    pub fn touch(&self, db: u8, keys: &[Bytes]) -> i64 {
        match &self.backend {
            StorageBackend::Memory(databases) => {
                let database = databases[db as usize].read();
                keys.iter()
                    .filter(|key| {
                        if let Some(entry) = database.get_entry_mut(key) {
                            entry.record_access();
                            true
                        } else {
                            false
                        }
                    })
                    .count() as i64
            }
            StorageBackend::HybridLog(logs) => keys
                .iter()
                .filter(|key| logs[db as usize].get(key).is_some())
                .count() as i64,
        }
    }

    /// Get access frequency for a key (OBJECT FREQ)
    pub fn object_freq(&self, db: u8, key: &Bytes) -> Option<u64> {
        match &self.backend {
            StorageBackend::Memory(databases) => {
                let database = databases[db as usize].read();
                let entry = database.get_entry_mut(key)?;
                Some(entry.access_count.load(Ordering::Relaxed))
            }
            StorageBackend::HybridLog(logs) => logs[db as usize].access_count(key),
        }
    }

    /// Get idle time in seconds for a key (OBJECT IDLETIME)
    pub fn object_idletime(&self, db: u8, key: &Bytes) -> Option<i64> {
        match &self.backend {
            StorageBackend::Memory(databases) => {
                let database = databases[db as usize].read();
                let entry = database.get_entry_mut(key)?;
                let last_access = entry.last_access.load(Ordering::Relaxed);
                let now = coarse_now_ms();
                Some(now.saturating_sub(last_access).div_ceil(1000) as i64)
            }
            StorageBackend::HybridLog(logs) => logs[db as usize]
                .last_access(key)
                .map(|instant| Instant::now().saturating_duration_since(instant).as_secs() as i64),
        }
    }

    /// Get all keys in a database
    pub fn keys(&self, db: u8) -> Vec<Bytes> {
        match &self.backend {
            StorageBackend::Memory(databases) => databases[db as usize].read().keys(),
            StorageBackend::HybridLog(logs) => logs[db as usize].keys(),
        }
    }

    /// Get key count for a database
    pub fn key_count(&self, db: u8) -> u64 {
        match &self.backend {
            StorageBackend::Memory(databases) => databases[db as usize].read().len() as u64,
            StorageBackend::HybridLog(logs) => logs[db as usize].len() as u64,
        }
    }

    /// Get the number of databases
    pub fn num_databases(&self) -> usize {
        self.num_dbs
    }

    /// Clear a single database (FLUSHDB)
    pub fn flush_db(&self, db: u8) {
        match &self.backend {
            StorageBackend::Memory(databases) => databases[db as usize].read().clear(),
            StorageBackend::HybridLog(logs) => {
                let log = &logs[db as usize];
                for key in log.keys() {
                    log.del(&key);
                }
            }
        }
    }

    /// Clear all databases (FLUSHALL)
    pub fn flush_all(&self) {
        match &self.backend {
            StorageBackend::Memory(databases) => {
                for db in databases {
                    db.read().clear();
                }
            }
            StorageBackend::HybridLog(logs) => {
                for log in logs {
                    for key in log.keys() {
                        log.del(&key);
                    }
                }
            }
        }
    }

    /// Check if the store is using HybridLog backend
    pub fn is_hybridlog(&self) -> bool {
        matches!(&self.backend, StorageBackend::HybridLog(_))
    }
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("num_databases", &self.num_dbs)
            .field("backend_type", &self.backend_type())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_get() {
        let db = Database::new();
        let key = Bytes::from("key");
        let value = Value::String(Bytes::from("value"));

        db.set(key.clone(), value.clone());

        let result = db.get(&key);
        assert!(result.is_some());
        if let Some(Value::String(s)) = result {
            assert_eq!(s, Bytes::from("value"));
        } else {
            panic!("Expected string value");
        }
    }

    #[test]
    fn test_del() {
        let db = Database::new();
        let key = Bytes::from("key");
        let value = Value::String(Bytes::from("value"));

        db.set(key.clone(), value);
        assert!(db.exists(&key));

        assert!(db.del(&key));
        assert!(!db.exists(&key));

        // Delete non-existent key
        assert!(!db.del(&key));
    }

    #[test]
    fn test_exists() {
        let db = Database::new();
        let key = Bytes::from("key");
        let value = Value::String(Bytes::from("value"));

        assert!(!db.exists(&key));

        db.set(key.clone(), value);
        assert!(db.exists(&key));
    }

    #[test]
    fn test_expiration() {
        use std::time::Duration;

        let db = Database::new();
        let key = Bytes::from("key");
        let value = Value::String(Bytes::from("value"));

        // Set with immediate expiration
        let expires_at = SystemTime::now() - Duration::from_secs(1);
        db.set_with_expiry(key.clone(), value, expires_at);

        // Should be expired
        assert!(db.get(&key).is_none());
        assert!(!db.exists(&key));
    }

    #[test]
    fn test_store_multiple_databases() {
        let store = Store::new(16);

        let key = Bytes::from("key");
        let value1 = Value::String(Bytes::from("value1"));
        let value2 = Value::String(Bytes::from("value2"));

        store.set(0, key.clone(), value1);
        store.set(1, key.clone(), value2);

        // Values should be isolated
        if let Some(Value::String(s)) = store.get(0, &key) {
            assert_eq!(s, Bytes::from("value1"));
        } else {
            panic!("Expected value in db 0");
        }

        if let Some(Value::String(s)) = store.get(1, &key) {
            assert_eq!(s, Bytes::from("value2"));
        } else {
            panic!("Expected value in db 1");
        }

        // Database 2 should not have the key
        assert!(store.get(2, &key).is_none());
    }

    #[test]
    fn test_set_nx() {
        let db = Database::new();
        let key = Bytes::from("key");
        let value1 = Value::String(Bytes::from("value1"));
        let value2 = Value::String(Bytes::from("value2"));

        // First set should succeed
        assert!(db.set_nx(key.clone(), value1));

        // Second set should fail
        assert!(!db.set_nx(key.clone(), value2));

        // Value should still be value1
        if let Some(Value::String(s)) = db.get(&key) {
            assert_eq!(s, Bytes::from("value1"));
        } else {
            panic!("Expected value");
        }
    }

    #[test]
    fn test_store_backend_type() {
        let store = Store::new(16);
        assert_eq!(store.backend_type(), StorageBackendType::Memory);
        assert!(!store.is_hybridlog());
        assert!(store.hybridlog_stats().is_none());
    }

    #[test]
    fn test_store_with_memory_config() {
        let config = StorageConfig::default();
        let store = Store::with_config(&config).unwrap();
        assert_eq!(store.backend_type(), StorageBackendType::Memory);
        assert_eq!(store.num_databases(), 16);
    }

    #[test]
    fn test_store_with_hybridlog_config() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let config = StorageConfig {
            backend: StorageBackendType::HybridLog,
            databases: 2,
            data_dir: dir.path().to_path_buf(),
            hybridlog_mutable_size: 4096,
            hybridlog_readonly_size: 8192,
            hybridlog_auto_tiering: true,
            hybridlog_migration_threshold: 0.8,
            ..Default::default()
        };
        let store = Store::with_config(&config).unwrap();
        assert_eq!(store.backend_type(), StorageBackendType::HybridLog);
        assert!(store.is_hybridlog());
        assert_eq!(store.num_databases(), 2);

        // Test stats
        let stats = store.hybridlog_stats().unwrap();
        assert_eq!(stats.len(), 2);

        let agg = store.hybridlog_stats_aggregated().unwrap();
        assert_eq!(agg.key_count, 0);
    }

    #[test]
    fn test_hybridlog_store_basic_ops() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let config = StorageConfig {
            backend: StorageBackendType::HybridLog,
            databases: 2,
            data_dir: dir.path().to_path_buf(),
            hybridlog_mutable_size: 64 * 1024,
            hybridlog_readonly_size: 128 * 1024,
            ..Default::default()
        };
        let store = Store::with_config(&config).unwrap();

        // Test set/get
        let key = Bytes::from("test_key");
        let value = Value::String(Bytes::from("test_value"));
        store.set(0, key.clone(), value.clone());

        let result = store.get(0, &key);
        assert!(result.is_some());
        if let Some(Value::String(s)) = result {
            assert_eq!(s, Bytes::from("test_value"));
        } else {
            panic!("Expected string value");
        }

        // Test database isolation
        assert!(store.get(1, &key).is_none());

        // Test exists
        assert_eq!(store.exists(0, &[key.clone()]), 1);
        assert_eq!(store.exists(1, &[key.clone()]), 0);

        // Test del
        assert_eq!(store.del(0, &[key.clone()]), 1);
        assert!(store.get(0, &key).is_none());
    }

    #[test]
    fn test_hybridlog_store_list_type() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let config = StorageConfig {
            backend: StorageBackendType::HybridLog,
            databases: 1,
            data_dir: dir.path().to_path_buf(),
            hybridlog_mutable_size: 64 * 1024,
            hybridlog_readonly_size: 128 * 1024,
            ..Default::default()
        };
        let store = Store::with_config(&config).unwrap();

        let key = Bytes::from("list_key");
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item1"));
        list.push_back(Bytes::from("item2"));
        let value = Value::List(list);

        store.set(0, key.clone(), value.clone());

        let result = store.get(0, &key).unwrap();
        if let Value::List(l) = result {
            assert_eq!(l.len(), 2);
            assert_eq!(l[0], Bytes::from("item1"));
            assert_eq!(l[1], Bytes::from("item2"));
        } else {
            panic!("Expected List value");
        }
    }

    #[test]
    fn test_hybridlog_store_hash_type() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let config = StorageConfig {
            backend: StorageBackendType::HybridLog,
            databases: 1,
            data_dir: dir.path().to_path_buf(),
            hybridlog_mutable_size: 64 * 1024,
            hybridlog_readonly_size: 128 * 1024,
            ..Default::default()
        };
        let store = Store::with_config(&config).unwrap();

        let key = Bytes::from("hash_key");
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("field1"), Bytes::from("value1"));
        hash.insert(Bytes::from("field2"), Bytes::from("value2"));
        let value = Value::Hash(hash);

        store.set(0, key.clone(), value.clone());

        let result = store.get(0, &key).unwrap();
        if let Value::Hash(h) = result {
            assert_eq!(h.len(), 2);
            assert_eq!(h.get(&Bytes::from("field1")), Some(&Bytes::from("value1")));
            assert_eq!(h.get(&Bytes::from("field2")), Some(&Bytes::from("value2")));
        } else {
            panic!("Expected Hash value");
        }
    }

    #[test]
    fn test_hybridlog_store_stats() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let config = StorageConfig {
            backend: StorageBackendType::HybridLog,
            databases: 1,
            data_dir: dir.path().to_path_buf(),
            hybridlog_mutable_size: 64 * 1024,
            hybridlog_readonly_size: 128 * 1024,
            ..Default::default()
        };
        let store = Store::with_config(&config).unwrap();

        // Insert some data
        for i in 0..10 {
            let key = Bytes::from(format!("key{}", i));
            let value = Value::String(Bytes::from(format!("value{}", i)));
            store.set(0, key, value);
        }

        let stats = store.hybridlog_stats_aggregated().unwrap();
        assert_eq!(stats.key_count, 10);
        assert!(stats.set_ops >= 10);
        assert!(stats.mutable_bytes > 0);
    }

    #[test]
    fn test_reset_stats_clears_counters() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let config = StorageConfig {
            backend: StorageBackendType::HybridLog,
            databases: 1,
            data_dir: dir.path().to_path_buf(),
            hybridlog_mutable_size: 64 * 1024,
            hybridlog_readonly_size: 128 * 1024,
            ..Default::default()
        };
        let store = Store::with_config(&config).unwrap();

        // Perform some operations to increment counters
        for i in 0..5 {
            let key = Bytes::from(format!("key{}", i));
            let value = Value::String(Bytes::from(format!("value{}", i)));
            store.set(0, key.clone(), value);
            store.get(0, &key);
            store.del(0, &[key]);
        }

        // Verify stats are non-zero
        let stats_before = store.hybridlog_stats_aggregated().unwrap();
        assert!(stats_before.set_ops >= 5, "Expected at least 5 set ops");
        assert!(stats_before.get_ops >= 5, "Expected at least 5 get ops");
        assert!(stats_before.del_ops >= 5, "Expected at least 5 del ops");

        // Reset stats
        store.reset_stats();

        // Verify counters are reset to zero
        let stats_after = store.hybridlog_stats_aggregated().unwrap();
        assert_eq!(stats_after.set_ops, 0, "set_ops should be reset to 0");
        assert_eq!(stats_after.get_ops, 0, "get_ops should be reset to 0");
        assert_eq!(stats_after.del_ops, 0, "del_ops should be reset to 0");
        assert_eq!(
            stats_after.mutable_to_readonly_migrations, 0,
            "migration count should be reset"
        );
    }

    #[test]
    fn test_reset_stats_memory_backend_no_op() {
        // Memory backend should handle reset_stats gracefully (no-op)
        let store = Store::new(16);
        store.reset_stats(); // Should not panic
                             // Memory backend has no stats to check, just verify it doesn't crash
    }

    #[test]
    fn test_store_profiler_integration() {
        use crate::optimizer::profiler::WorkloadProfiler;

        let profiler = Arc::new(WorkloadProfiler::new());
        let mut store = Store::new(1);
        store.set_profiler(profiler.clone());

        assert!(store.profiler().is_some());

        let key = Bytes::from("hello");
        let value = Value::String(Bytes::from("world"));
        store.set(0, key.clone(), value);
        store.get(0, &key);
        store.get(0, &key);

        let snap = profiler.snapshot();
        assert_eq!(snap.total_reads, 2);
        assert_eq!(snap.total_writes, 1);
        assert!(snap.unique_keys_accessed >= 1);
    }

    #[test]
    fn test_store_without_profiler() {
        let store = Store::new(1);
        assert!(store.profiler().is_none());

        // Operations should work fine without a profiler
        let key = Bytes::from("test");
        let value = Value::String(Bytes::from("data"));
        store.set(0, key.clone(), value);
        assert!(store.get(0, &key).is_some());
    }
}
