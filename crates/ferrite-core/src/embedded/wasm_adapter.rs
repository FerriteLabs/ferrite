//! WASM-Compatible Storage Adapter
//!
//! Provides a storage implementation for running Ferrite in WebAssembly edge
//! environments such as Cloudflare Workers, Deno Deploy, and browsers.
//!
//! # Design Goals
//!
//! - Single-threaded (WASM has no threads by default)
//! - Lazy TTL expiration on access
//! - Configurable memory and key limits
//! - Pluggable persistence backends (IndexedDB, OPFS, localStorage)
//!
//! # Example
//!
//! ```no_run
//! use ferrite_core::embedded::wasm_adapter::{WasmStore, WasmStoreConfig};
//!
//! let store = WasmStore::new(WasmStoreConfig::default());
//! store.set("hello", b"world");
//! assert_eq!(store.get("hello"), Some(b"world".to_vec()));
//! ```

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors specific to the WASM storage adapter.
#[derive(Debug, Clone, thiserror::Error)]
pub enum WasmStoreError {
    /// Key exceeds the configured maximum size.
    #[error("key too large: size {size}, limit {limit}")]
    KeyTooLarge {
        /// Size of the rejected key in bytes.
        size: usize,
        /// Configured maximum key size in bytes.
        limit: usize,
    },

    /// Value exceeds the configured maximum size.
    #[error("value too large: size {size}, limit {limit}")]
    ValueTooLarge {
        /// Size of the rejected value in bytes.
        size: usize,
        /// Configured maximum value size in bytes.
        limit: usize,
    },

    /// Total memory usage would exceed the configured limit.
    #[error("memory limit exceeded: used {used}, limit {limit}")]
    MemoryLimitExceeded {
        /// Current memory usage in bytes.
        used: usize,
        /// Configured maximum memory in bytes.
        limit: usize,
    },

    /// Maximum number of keys would be exceeded.
    #[error("max keys exceeded: count {count}, limit {limit}")]
    MaxKeysExceeded {
        /// Current number of keys.
        count: usize,
        /// Configured maximum key count.
        limit: usize,
    },

    /// The requested persistence backend is not available.
    #[error("persistence unavailable: {0}")]
    PersistenceUnavailable(String),

    /// An internal error occurred.
    #[error("internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Persistence enum
// ---------------------------------------------------------------------------

/// Persistence backend for the WASM store.
#[derive(Clone, Debug)]
pub enum WasmPersistence {
    /// In-memory only – no persistence.
    None,
    /// Browser IndexedDB backend.
    IndexedDb {
        /// Database name used in IndexedDB.
        db_name: String,
    },
    /// Origin Private File System (modern browsers).
    Opfs {
        /// Directory path within OPFS.
        directory: String,
    },
    /// Browser `localStorage` – suitable for small datasets only.
    LocalStorage {
        /// Key prefix in localStorage.
        prefix: String,
    },
    /// Custom persistence adapter identified by name.
    Custom {
        /// Adapter identifier.
        adapter_name: String,
    },
}

impl Default for WasmPersistence {
    fn default() -> Self {
        WasmPersistence::None
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for [`WasmStore`].
#[derive(Clone, Debug)]
pub struct WasmStoreConfig {
    /// Maximum memory usage in bytes.
    pub max_memory_bytes: usize,
    /// Maximum number of keys.
    pub max_keys: usize,
    /// Maximum key size in bytes.
    pub max_key_size: usize,
    /// Maximum value size in bytes.
    pub max_value_size: usize,
    /// Enable TTL-based expiration.
    pub enable_ttl: bool,
    /// Persistence backend.
    pub persistence: WasmPersistence,
}

impl Default for WasmStoreConfig {
    fn default() -> Self {
        Self {
            max_memory_bytes: 16 * 1024 * 1024, // 16MB
            max_keys: 100_000,
            max_key_size: 512,
            max_value_size: 1024 * 1024, // 1MB
            enable_ttl: true,
            persistence: WasmPersistence::None,
        }
    }
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Runtime statistics for [`WasmStore`].
#[derive(Clone, Debug, Default)]
pub struct WasmStoreStats {
    /// Total number of stored keys.
    pub total_keys: usize,
    /// Total bytes used by stored values.
    pub total_bytes: usize,
    /// Number of get operations.
    pub gets: u64,
    /// Number of set operations.
    pub sets: u64,
    /// Number of delete operations.
    pub deletes: u64,
    /// Number of cache hits (key found and not expired).
    pub cache_hits: u64,
    /// Number of cache misses (key not found or expired).
    pub cache_misses: u64,
    /// Number of entries removed due to TTL expiration.
    pub ttl_expirations: u64,
}

// ---------------------------------------------------------------------------
// Internal entry
// ---------------------------------------------------------------------------

/// Internal storage entry.
struct WasmEntry {
    /// Stored value bytes.
    value: Vec<u8>,
    /// Optional expiration timestamp in milliseconds since UNIX epoch.
    expires_at: Option<u64>,
    /// Creation timestamp in milliseconds since UNIX epoch.
    created_at: u64,
    /// Estimated in-memory size.
    size_bytes: usize,
}

impl WasmEntry {
    fn is_expired(&self, now_ms: u64) -> bool {
        match self.expires_at {
            Some(exp) => now_ms >= exp,
            None => false,
        }
    }
}

// ---------------------------------------------------------------------------
// WasmStore
// ---------------------------------------------------------------------------

/// WASM-compatible key-value store for edge environments.
///
/// Uses a plain `HashMap` internally – WASM runtimes are single-threaded so
/// no synchronization primitives are needed.
pub struct WasmStore {
    config: WasmStoreConfig,
    data: HashMap<String, WasmEntry>,
    memory_used: usize,
    stats: WasmStoreStats,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Estimate per-entry overhead (key heap + HashMap bucket + WasmEntry struct).
fn entry_overhead(key: &str) -> usize {
    key.len() + std::mem::size_of::<WasmEntry>() + 64 // bucket overhead estimate
}

impl WasmStore {
    /// Create a new WASM store with the given configuration.
    pub fn new(config: WasmStoreConfig) -> Self {
        Self {
            config,
            data: HashMap::new(),
            memory_used: 0,
            stats: WasmStoreStats::default(),
        }
    }

    /// Create a store with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(WasmStoreConfig::default())
    }

    // -- helpers ----------------------------------------------------------

    /// Remove expired entries lazily when touched during a get.
    fn maybe_expire(&mut self, key: &str) -> bool {
        if !self.config.enable_ttl {
            return false;
        }
        let now = now_ms();
        if let Some(entry) = self.data.get(key) {
            if entry.is_expired(now) {
                let size = entry.size_bytes + entry_overhead(key);
                self.data.remove(key);
                self.memory_used = self.memory_used.saturating_sub(size);
                self.stats.ttl_expirations += 1;
                return true;
            }
        }
        false
    }

    fn validate_key(&self, key: &str) -> Result<(), WasmStoreError> {
        if key.len() > self.config.max_key_size {
            return Err(WasmStoreError::KeyTooLarge {
                size: key.len(),
                limit: self.config.max_key_size,
            });
        }
        Ok(())
    }

    fn validate_value(&self, value: &[u8]) -> Result<(), WasmStoreError> {
        if value.len() > self.config.max_value_size {
            return Err(WasmStoreError::ValueTooLarge {
                size: value.len(),
                limit: self.config.max_value_size,
            });
        }
        Ok(())
    }

    fn check_limits_for_insert(&self, key: &str, value: &[u8]) -> Result<(), WasmStoreError> {
        let is_update = self.data.contains_key(key);
        if !is_update && self.data.len() >= self.config.max_keys {
            return Err(WasmStoreError::MaxKeysExceeded {
                count: self.data.len(),
                limit: self.config.max_keys,
            });
        }

        let new_size = value.len() + entry_overhead(key);
        let old_size = if is_update {
            self.data
                .get(key)
                .map(|e| e.size_bytes + entry_overhead(key))
                .unwrap_or(0)
        } else {
            0
        };
        let projected = self.memory_used + new_size - old_size;
        if projected > self.config.max_memory_bytes {
            return Err(WasmStoreError::MemoryLimitExceeded {
                used: projected,
                limit: self.config.max_memory_bytes,
            });
        }
        Ok(())
    }

    // -- public API -------------------------------------------------------

    /// Retrieve a value by key. Returns `None` if the key does not exist or
    /// has expired.
    pub fn get(&mut self, key: &str) -> Option<Vec<u8>> {
        self.stats.gets += 1;

        if self.maybe_expire(key) {
            self.stats.cache_misses += 1;
            return None;
        }

        match self.data.get(key) {
            Some(entry) => {
                self.stats.cache_hits += 1;
                Some(entry.value.clone())
            }
            None => {
                self.stats.cache_misses += 1;
                None
            }
        }
    }

    /// Store a key-value pair. Returns `true` on success.
    pub fn set(&mut self, key: &str, value: &[u8]) -> bool {
        self.set_inner(key, value, None)
    }

    /// Store a key-value pair with a TTL in milliseconds.
    pub fn set_with_ttl(&mut self, key: &str, value: &[u8], ttl_ms: u64) -> bool {
        self.set_inner(key, value, Some(ttl_ms))
    }

    fn set_inner(&mut self, key: &str, value: &[u8], ttl_ms: Option<u64>) -> bool {
        if self.validate_key(key).is_err() || self.validate_value(value).is_err() {
            return false;
        }
        if self.check_limits_for_insert(key, value).is_err() {
            return false;
        }

        self.stats.sets += 1;

        // Remove old entry memory accounting
        if let Some(old) = self.data.get(key) {
            let old_size = old.size_bytes + entry_overhead(key);
            self.memory_used = self.memory_used.saturating_sub(old_size);
        }

        let now = now_ms();
        let size_bytes = value.len();
        let expires_at = if self.config.enable_ttl {
            ttl_ms.map(|t| now + t)
        } else {
            None
        };

        let entry = WasmEntry {
            value: value.to_vec(),
            expires_at,
            created_at: now,
            size_bytes,
        };

        let added = size_bytes + entry_overhead(key);
        self.memory_used += added;
        self.data.insert(key.to_string(), entry);
        true
    }

    /// Delete a key. Returns `true` if the key existed.
    pub fn delete(&mut self, key: &str) -> bool {
        self.stats.deletes += 1;
        if let Some(entry) = self.data.remove(key) {
            let size = entry.size_bytes + entry_overhead(key);
            self.memory_used = self.memory_used.saturating_sub(size);
            true
        } else {
            false
        }
    }

    /// Check if a key exists (and is not expired).
    pub fn exists(&mut self, key: &str) -> bool {
        if self.maybe_expire(key) {
            return false;
        }
        self.data.contains_key(key)
    }

    /// Return keys matching a glob-style pattern. Supports `*` and `?`
    /// wildcards.
    pub fn keys(&mut self, pattern: &str) -> Vec<String> {
        // Lazily expire while iterating
        if self.config.enable_ttl {
            let now = now_ms();
            let expired_keys: Vec<String> = self
                .data
                .iter()
                .filter(|(_, e)| e.is_expired(now))
                .map(|(k, _)| k.clone())
                .collect();
            for k in &expired_keys {
                if let Some(entry) = self.data.remove(k) {
                    let size = entry.size_bytes + entry_overhead(k);
                    self.memory_used = self.memory_used.saturating_sub(size);
                    self.stats.ttl_expirations += 1;
                }
            }
        }

        self.data
            .keys()
            .filter(|k| glob_match(pattern, k))
            .cloned()
            .collect()
    }

    /// Remove all keys.
    pub fn flush(&mut self) {
        self.data.clear();
        self.memory_used = 0;
    }

    /// Number of stored (non-expired) keys.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Estimated memory usage in bytes.
    pub fn memory_usage(&self) -> usize {
        self.memory_used
    }

    /// Snapshot of the current statistics.
    pub fn get_stats(&self) -> WasmStoreStats {
        WasmStoreStats {
            total_keys: self.data.len(),
            total_bytes: self.memory_used,
            ..self.stats.clone()
        }
    }
}

// ---------------------------------------------------------------------------
// Glob matching helper
// ---------------------------------------------------------------------------

/// Simple glob matcher supporting `*` (any sequence) and `?` (single char).
fn glob_match(pattern: &str, input: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let inp: Vec<char> = input.chars().collect();
    glob_match_inner(&pat, &inp)
}

fn glob_match_inner(pat: &[char], inp: &[char]) -> bool {
    match (pat.first(), inp.first()) {
        (None, None) => true,
        (Some('*'), _) => {
            // '*' matches zero or more characters
            glob_match_inner(&pat[1..], inp)
                || (!inp.is_empty() && glob_match_inner(pat, &inp[1..]))
        }
        (Some('?'), Some(_)) => glob_match_inner(&pat[1..], &inp[1..]),
        (Some(a), Some(b)) if a == b => glob_match_inner(&pat[1..], &inp[1..]),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn small_config() -> WasmStoreConfig {
        WasmStoreConfig {
            max_memory_bytes: 4096,
            max_keys: 10,
            max_key_size: 64,
            max_value_size: 256,
            enable_ttl: true,
            persistence: WasmPersistence::None,
        }
    }

    // -- basic CRUD -------------------------------------------------------

    #[test]
    fn test_set_and_get() {
        let mut store = WasmStore::new(WasmStoreConfig::default());
        assert!(store.set("key1", b"value1"));
        assert_eq!(store.get("key1"), Some(b"value1".to_vec()));
    }

    #[test]
    fn test_get_missing_key() {
        let mut store = WasmStore::new(WasmStoreConfig::default());
        assert_eq!(store.get("nonexistent"), None);
    }

    #[test]
    fn test_overwrite_key() {
        let mut store = WasmStore::new(WasmStoreConfig::default());
        store.set("k", b"v1");
        store.set("k", b"v2");
        assert_eq!(store.get("k"), Some(b"v2".to_vec()));
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_delete() {
        let mut store = WasmStore::new(WasmStoreConfig::default());
        store.set("k", b"v");
        assert!(store.delete("k"));
        assert_eq!(store.get("k"), None);
        assert!(!store.delete("k")); // already gone
    }

    #[test]
    fn test_exists() {
        let mut store = WasmStore::new(WasmStoreConfig::default());
        store.set("k", b"v");
        assert!(store.exists("k"));
        assert!(!store.exists("missing"));
    }

    #[test]
    fn test_flush() {
        let mut store = WasmStore::new(WasmStoreConfig::default());
        store.set("a", b"1");
        store.set("b", b"2");
        store.flush();
        assert_eq!(store.len(), 0);
        assert_eq!(store.memory_usage(), 0);
    }

    // -- TTL expiration ---------------------------------------------------

    #[test]
    fn test_ttl_not_yet_expired() {
        let mut store = WasmStore::new(WasmStoreConfig::default());
        store.set_with_ttl("k", b"v", 60_000); // 60 seconds
        assert_eq!(store.get("k"), Some(b"v".to_vec()));
    }

    #[test]
    fn test_ttl_expired() {
        let mut store = WasmStore::new(WasmStoreConfig::default());
        // Insert with TTL of 0ms → already expired
        store.set_with_ttl("k", b"v", 0);
        // Give a tiny bit of time so now_ms() > created_at + 0
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert_eq!(store.get("k"), None);
        assert_eq!(store.get_stats().ttl_expirations, 1);
    }

    #[test]
    fn test_ttl_disabled() {
        let mut config = WasmStoreConfig::default();
        config.enable_ttl = false;
        let mut store = WasmStore::new(config);
        store.set_with_ttl("k", b"v", 0);
        // TTL is disabled – entry should still be present
        assert!(store.exists("k"));
    }

    // -- memory limits ----------------------------------------------------

    #[test]
    fn test_memory_limit_exceeded() {
        let config = small_config();
        let mut store = WasmStore::new(config);
        // Fill up memory
        let big_value = vec![0u8; 256];
        let mut inserted = 0;
        for i in 0..100 {
            if store.set(&format!("k{i}"), &big_value) {
                inserted += 1;
            } else {
                break;
            }
        }
        assert!(inserted > 0);
        assert!(inserted < 100);
    }

    // -- key limits -------------------------------------------------------

    #[test]
    fn test_max_keys_exceeded() {
        let config = small_config();
        let mut store = WasmStore::new(config);
        for i in 0..10 {
            assert!(store.set(&format!("k{i}"), b"v"));
        }
        // 11th key should be rejected
        assert!(!store.set("overflow", b"v"));
    }

    #[test]
    fn test_key_too_large() {
        let config = small_config();
        let mut store = WasmStore::new(config);
        let big_key = "x".repeat(65);
        assert!(!store.set(&big_key, b"v"));
    }

    #[test]
    fn test_value_too_large() {
        let config = small_config();
        let mut store = WasmStore::new(config);
        let big_value = vec![0u8; 257];
        assert!(!store.set("k", &big_value));
    }

    // -- pattern matching -------------------------------------------------

    #[test]
    fn test_keys_wildcard_star() {
        let mut store = WasmStore::new(WasmStoreConfig::default());
        store.set("user:1", b"a");
        store.set("user:2", b"b");
        store.set("order:1", b"c");
        let mut keys = store.keys("user:*");
        keys.sort();
        assert_eq!(keys, vec!["user:1", "user:2"]);
    }

    #[test]
    fn test_keys_wildcard_question() {
        let mut store = WasmStore::new(WasmStoreConfig::default());
        store.set("ab", b"1");
        store.set("ac", b"2");
        store.set("abc", b"3");
        let mut keys = store.keys("a?");
        keys.sort();
        assert_eq!(keys, vec!["ab", "ac"]);
    }

    #[test]
    fn test_keys_match_all() {
        let mut store = WasmStore::new(WasmStoreConfig::default());
        store.set("x", b"1");
        store.set("y", b"2");
        assert_eq!(store.keys("*").len(), 2);
    }

    // -- stats ------------------------------------------------------------

    #[test]
    fn test_stats_basic() {
        let mut store = WasmStore::new(WasmStoreConfig::default());
        store.set("a", b"1");
        store.set("b", b"2");
        store.get("a");
        store.get("missing");
        store.delete("b");

        let stats = store.get_stats();
        assert_eq!(stats.sets, 2);
        assert_eq!(stats.gets, 2);
        assert_eq!(stats.deletes, 1);
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.cache_misses, 1);
        assert_eq!(stats.total_keys, 1);
    }

    // -- glob matching ----------------------------------------------------

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("hello", "hello"));
        assert!(!glob_match("hello", "world"));
        assert!(glob_match("h?llo", "hello"));
        assert!(glob_match("h*o", "hello"));
        assert!(glob_match("h*o", "ho"));
        assert!(!glob_match("h?o", "ho"));
        assert!(glob_match("*bar*", "foobarbaz"));
    }

    // -- WasmPersistence defaults -----------------------------------------

    #[test]
    fn test_default_persistence_is_none() {
        let config = WasmStoreConfig::default();
        assert!(matches!(config.persistence, WasmPersistence::None));
    }
}
