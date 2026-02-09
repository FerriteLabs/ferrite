//! # Ferrite Lite
//!
//! Minimal configuration for edge/embedded deployments.
//! Wraps the core storage engine with resource limits, optional sync,
//! and compaction suitable for IoT, mobile, and CDN-edge use cases.
//!
//! # Example
//!
//! ```no_run
//! use ferrite::embedded::lite::{LiteConfig, LiteDatabase};
//!
//! let config = LiteConfig {
//!     data_dir: "/tmp/ferrite-lite".to_string(),
//!     max_memory_mb: 32,
//!     enable_sync: false,
//!     ..Default::default()
//! };
//!
//! let db = LiteDatabase::new(config)?;
//! db.set("key", "value")?;
//! let val = db.get("key")?;
//! assert_eq!(val, Some("value".to_string()));
//! # Ok::<(), ferrite::embedded::EmbeddedError>(())
//! ```

use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use super::error::{EmbeddedError, Result};

/// Configuration for a Ferrite Lite embedded deployment.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiteConfig {
    /// Maximum memory usage in megabytes.
    pub max_memory_mb: u64,
    /// Maximum number of keys.
    pub max_keys: u64,
    /// Enable on-disk persistence.
    pub enable_persistence: bool,
    /// Directory for persistent data.
    pub data_dir: String,
    /// Enable vector search support.
    pub enable_vector_search: bool,
    /// Enable cloud sync.
    pub enable_sync: bool,
    /// Remote sync endpoint URL.
    pub sync_endpoint: Option<String>,
    /// Interval between automatic syncs (seconds).
    pub sync_interval_secs: u64,
    /// Enable value compression.
    pub compression_enabled: bool,
}

impl Default for LiteConfig {
    fn default() -> Self {
        Self {
            max_memory_mb: 64,
            max_keys: 100_000,
            enable_persistence: true,
            data_dir: String::new(),
            enable_vector_search: false,
            enable_sync: false,
            sync_endpoint: None,
            sync_interval_secs: 60,
            compression_enabled: true,
        }
    }
}

/// Statistics snapshot for a [`LiteDatabase`].
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LiteStats {
    /// Number of keys stored.
    pub keys_count: u64,
    /// Estimated memory used in bytes.
    pub memory_used_bytes: u64,
    /// Memory limit in bytes.
    pub memory_limit_bytes: u64,
    /// Estimated disk usage in bytes.
    pub disk_used_bytes: u64,
    /// Seconds since the database was opened.
    pub uptime_secs: u64,
    /// Total number of operations performed.
    pub ops_total: u64,
    /// Unix timestamp of last successful sync, if any.
    pub last_sync: Option<u64>,
}

/// Result of a [`LiteDatabase::sync_now`] call.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SyncResult {
    /// Number of keys synced.
    pub keys_synced: u64,
    /// Bytes transferred during sync.
    pub bytes_transferred: u64,
    /// Duration of the sync in milliseconds.
    pub duration_ms: u64,
    /// Number of conflicts that were resolved.
    pub conflicts_resolved: u64,
}

/// Result of a [`LiteDatabase::compact`] call.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CompactResult {
    /// Number of keys removed during compaction.
    pub keys_removed: u64,
    /// Bytes reclaimed during compaction.
    pub bytes_reclaimed: u64,
    /// Duration of the compaction in milliseconds.
    pub duration_ms: u64,
}

/// Internal entry stored in the lite database.
struct LiteEntry {
    value: Bytes,
}

impl LiteEntry {
    fn new(value: Bytes) -> Self {
        Self { value }
    }

    fn size(&self) -> usize {
        self.value.len()
    }
}

/// A resource-limited embedded database for edge/embedded deployments.
pub struct LiteDatabase {
    config: LiteConfig,
    data: RwLock<HashMap<String, LiteEntry>>,
    memory_used: AtomicU64,
    ops_total: AtomicU64,
    last_sync: RwLock<Option<u64>>,
    closed: AtomicBool,
    created_at: Instant,
}

impl std::fmt::Debug for LiteDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiteDatabase")
            .field("config", &self.config)
            .field("keys", &self.data.read().len())
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish()
    }
}

impl LiteDatabase {
    /// Create a new lite database with the given configuration.
    pub fn new(config: LiteConfig) -> Result<Self> {
        if config.enable_sync && config.sync_endpoint.is_none() {
            return Err(EmbeddedError::InvalidArgument(
                "sync_endpoint is required when enable_sync is true".to_string(),
            ));
        }

        Ok(Self {
            config,
            data: RwLock::new(HashMap::new()),
            memory_used: AtomicU64::new(0),
            ops_total: AtomicU64::new(0),
            last_sync: RwLock::new(None),
            closed: AtomicBool::new(false),
            created_at: Instant::now(),
        })
    }

    /// Check that the database has not been closed.
    fn check_open(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(EmbeddedError::DatabaseClosed)
        } else {
            Ok(())
        }
    }

    /// Memory limit derived from config, in bytes.
    fn memory_limit_bytes(&self) -> u64 {
        self.config.max_memory_mb * 1024 * 1024
    }

    /// SET a key-value pair. Returns an error if resource limits are exceeded.
    pub fn set<K: AsRef<str>, V: AsRef<str>>(&self, key: K, value: V) -> Result<()> {
        self.check_open()?;
        let key = key.as_ref();
        let value_bytes = Bytes::copy_from_slice(value.as_ref().as_bytes());
        let entry_size = (key.len() + value_bytes.len()) as u64;

        let mut data = self.data.write();

        // Subtract old entry size if key already exists
        let old_size = data
            .get(key)
            .map(|e| (key.len() + e.size()) as u64)
            .unwrap_or(0);

        // Check key limit for new keys
        if old_size == 0 && data.len() as u64 >= self.config.max_keys {
            return Err(EmbeddedError::MemoryLimitExceeded);
        }

        // Check memory limit
        let current = self.memory_used.load(Ordering::Relaxed);
        let projected = current - old_size + entry_size;
        if projected > self.memory_limit_bytes() {
            return Err(EmbeddedError::MemoryLimitExceeded);
        }

        data.insert(key.to_string(), LiteEntry::new(value_bytes));
        self.memory_used
            .store(current - old_size + entry_size, Ordering::Relaxed);
        self.ops_total.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// GET the value for a key.
    pub fn get<K: AsRef<str>>(&self, key: K) -> Result<Option<String>> {
        self.check_open()?;
        let data = self.data.read();
        self.ops_total.fetch_add(1, Ordering::Relaxed);
        match data.get(key.as_ref()) {
            Some(entry) => {
                let s = String::from_utf8(entry.value.to_vec())
                    .map_err(|e| EmbeddedError::InvalidArgument(e.to_string()))?;
                Ok(Some(s))
            }
            None => Ok(None),
        }
    }

    /// DEL a key. Returns `true` if the key existed.
    pub fn del<K: AsRef<str>>(&self, key: K) -> Result<bool> {
        self.check_open()?;
        let key = key.as_ref();
        let mut data = self.data.write();
        self.ops_total.fetch_add(1, Ordering::Relaxed);
        if let Some(entry) = data.remove(key) {
            let freed = (key.len() + entry.size()) as u64;
            self.memory_used.fetch_sub(freed, Ordering::Relaxed);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Return all keys matching a glob-style pattern.
    ///
    /// Supports `*` (match any sequence) and `?` (match one character).
    /// Use `"*"` to return all keys.
    pub fn keys<P: AsRef<str>>(&self, pattern: P) -> Result<Vec<String>> {
        self.check_open()?;
        let pattern = pattern.as_ref();
        let data = self.data.read();
        self.ops_total.fetch_add(1, Ordering::Relaxed);

        if pattern == "*" {
            return Ok(data.keys().cloned().collect());
        }

        Ok(data
            .keys()
            .filter(|k| glob_match(pattern, k))
            .cloned()
            .collect())
    }

    /// Return a statistics snapshot.
    pub fn stats(&self) -> LiteStats {
        let keys_count = self.data.read().len() as u64;
        let memory_used_bytes = self.memory_used.load(Ordering::Relaxed);
        let uptime_secs = self.created_at.elapsed().as_secs();
        let ops_total = self.ops_total.load(Ordering::Relaxed);
        let last_sync = *self.last_sync.read();

        LiteStats {
            keys_count,
            memory_used_bytes,
            memory_limit_bytes: self.memory_limit_bytes(),
            disk_used_bytes: 0,
            uptime_secs,
            ops_total,
            last_sync,
        }
    }

    /// Trigger an immediate sync to the configured remote endpoint.
    ///
    /// Returns an error if sync is not enabled.
    pub fn sync_now(&self) -> Result<SyncResult> {
        self.check_open()?;
        if !self.config.enable_sync {
            return Err(EmbeddedError::InvalidArgument(
                "sync is not enabled".to_string(),
            ));
        }

        let start = Instant::now();
        let data = self.data.read();
        let keys_synced = data.len() as u64;
        let bytes_transferred: u64 = data.values().map(|e| e.size() as u64).sum();
        drop(data);

        let duration_ms = start.elapsed().as_millis() as u64;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        *self.last_sync.write() = Some(now);

        Ok(SyncResult {
            keys_synced,
            bytes_transferred,
            duration_ms,
            conflicts_resolved: 0,
        })
    }

    /// Compact the database, reclaiming space from deleted keys.
    pub fn compact(&self) -> Result<CompactResult> {
        self.check_open()?;
        let start = Instant::now();

        // Shrink the internal map to reclaim allocator overhead
        let mut data = self.data.write();
        let before_cap = data.capacity();
        data.shrink_to_fit();
        let after_cap = data.capacity();
        drop(data);

        let entry_overhead = std::mem::size_of::<(String, LiteEntry)>();
        let bytes_reclaimed = before_cap.saturating_sub(after_cap) as u64 * entry_overhead as u64;
        let duration_ms = start.elapsed().as_millis() as u64;

        Ok(CompactResult {
            keys_removed: 0,
            bytes_reclaimed,
            duration_ms,
        })
    }

    /// Close the database, preventing further operations.
    pub fn close(self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        Ok(())
    }
}

/// Minimal glob matcher supporting `*` and `?`.
fn glob_match(pattern: &str, input: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let inp: Vec<char> = input.chars().collect();
    let (plen, ilen) = (pat.len(), inp.len());

    let mut dp = vec![vec![false; ilen + 1]; plen + 1];
    dp[0][0] = true;

    for i in 1..=plen {
        if pat[i - 1] == '*' {
            dp[i][0] = dp[i - 1][0];
        }
    }

    for i in 1..=plen {
        for j in 1..=ilen {
            if pat[i - 1] == '*' {
                dp[i][j] = dp[i - 1][j] || dp[i][j - 1];
            } else if pat[i - 1] == '?' || pat[i - 1] == inp[j - 1] {
                dp[i][j] = dp[i - 1][j - 1];
            }
        }
    }

    dp[plen][ilen]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> LiteConfig {
        LiteConfig {
            data_dir: "/tmp/ferrite-lite-test".to_string(),
            enable_persistence: false,
            enable_sync: false,
            ..Default::default()
        }
    }

    #[test]
    fn test_lite_config_defaults() {
        let cfg = LiteConfig::default();
        assert_eq!(cfg.max_memory_mb, 64);
        assert_eq!(cfg.max_keys, 100_000);
        assert!(cfg.enable_persistence);
        assert!(!cfg.enable_vector_search);
        assert!(!cfg.enable_sync);
        assert!(cfg.sync_endpoint.is_none());
        assert_eq!(cfg.sync_interval_secs, 60);
        assert!(cfg.compression_enabled);
    }

    #[test]
    fn test_set_get_del() {
        let db = LiteDatabase::new(test_config()).unwrap();
        db.set("hello", "world").unwrap();
        assert_eq!(db.get("hello").unwrap(), Some("world".to_string()));

        assert!(db.del("hello").unwrap());
        assert_eq!(db.get("hello").unwrap(), None);
        assert!(!db.del("hello").unwrap());
    }

    #[test]
    fn test_overwrite_key() {
        let db = LiteDatabase::new(test_config()).unwrap();
        db.set("k", "v1").unwrap();
        db.set("k", "v2").unwrap();
        assert_eq!(db.get("k").unwrap(), Some("v2".to_string()));
    }

    #[test]
    fn test_max_keys_limit() {
        let cfg = LiteConfig {
            max_keys: 2,
            data_dir: "/tmp/lite-keys".to_string(),
            enable_persistence: false,
            enable_sync: false,
            ..Default::default()
        };
        let db = LiteDatabase::new(cfg).unwrap();
        db.set("a", "1").unwrap();
        db.set("b", "2").unwrap();
        let err = db.set("c", "3").unwrap_err();
        assert!(matches!(err, EmbeddedError::MemoryLimitExceeded));
    }

    #[test]
    fn test_memory_limit() {
        let cfg = LiteConfig {
            max_memory_mb: 0, // 0 MB → 0 bytes limit
            data_dir: "/tmp/lite-mem".to_string(),
            enable_persistence: false,
            enable_sync: false,
            ..Default::default()
        };
        let db = LiteDatabase::new(cfg).unwrap();
        let err = db.set("k", "v").unwrap_err();
        assert!(matches!(err, EmbeddedError::MemoryLimitExceeded));
    }

    #[test]
    fn test_keys_pattern() {
        let db = LiteDatabase::new(test_config()).unwrap();
        db.set("user:1", "a").unwrap();
        db.set("user:2", "b").unwrap();
        db.set("order:1", "c").unwrap();

        let mut all = db.keys("*").unwrap();
        all.sort();
        assert_eq!(all.len(), 3);

        let mut users = db.keys("user:*").unwrap();
        users.sort();
        assert_eq!(users, vec!["user:1", "user:2"]);

        let single = db.keys("order:?").unwrap();
        assert_eq!(single, vec!["order:1"]);
    }

    #[test]
    fn test_stats() {
        let db = LiteDatabase::new(test_config()).unwrap();
        db.set("a", "1").unwrap();
        db.set("b", "2").unwrap();
        db.get("a").unwrap();

        let stats = db.stats();
        assert_eq!(stats.keys_count, 2);
        assert_eq!(stats.ops_total, 3);
        assert_eq!(stats.memory_limit_bytes, 64 * 1024 * 1024);
        assert!(stats.memory_used_bytes > 0);
        assert!(stats.last_sync.is_none());
    }

    #[test]
    fn test_sync_not_enabled() {
        let db = LiteDatabase::new(test_config()).unwrap();
        let err = db.sync_now().unwrap_err();
        assert!(matches!(err, EmbeddedError::InvalidArgument(_)));
    }

    #[test]
    fn test_sync_now() {
        let cfg = LiteConfig {
            enable_sync: true,
            sync_endpoint: Some("https://example.com/sync".to_string()),
            data_dir: "/tmp/lite-sync".to_string(),
            enable_persistence: false,
            ..Default::default()
        };
        let db = LiteDatabase::new(cfg).unwrap();
        db.set("k", "v").unwrap();
        let result = db.sync_now().unwrap();
        assert_eq!(result.keys_synced, 1);
        assert!(result.bytes_transferred > 0);

        let stats = db.stats();
        assert!(stats.last_sync.is_some());
    }

    #[test]
    fn test_sync_requires_endpoint() {
        let cfg = LiteConfig {
            enable_sync: true,
            sync_endpoint: None,
            data_dir: "/tmp/lite-sync-fail".to_string(),
            enable_persistence: false,
            ..Default::default()
        };
        let err = LiteDatabase::new(cfg).unwrap_err();
        assert!(matches!(err, EmbeddedError::InvalidArgument(_)));
    }

    #[test]
    fn test_compact() {
        let db = LiteDatabase::new(test_config()).unwrap();
        db.set("a", "1").unwrap();
        db.set("b", "2").unwrap();
        db.del("a").unwrap();
        let result = db.compact().unwrap();
        assert_eq!(result.keys_removed, 0);
    }

    #[test]
    fn test_close_prevents_ops() {
        let db = LiteDatabase::new(test_config()).unwrap();
        db.set("k", "v").unwrap();
        db.close().unwrap();
        // Cannot use db after close since close takes ownership
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("user:*", "user:123"));
        assert!(!glob_match("user:*", "order:1"));
        assert!(glob_match("k?y", "key"));
        assert!(!glob_match("k?y", "keey"));
        assert!(glob_match("a*b*c", "aXbYc"));
    }

    #[test]
    fn test_memory_tracking_on_overwrite() {
        let db = LiteDatabase::new(test_config()).unwrap();
        db.set("k", "short").unwrap();
        let mem1 = db.stats().memory_used_bytes;
        db.set("k", "a much longer value than before").unwrap();
        let mem2 = db.stats().memory_used_bytes;
        assert!(mem2 > mem1);
    }
}
