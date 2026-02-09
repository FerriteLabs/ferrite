//! Edge Computing Optimizations
//!
//! Size-optimized components for edge deployment with minimal binary size
//! and memory footprint. Designed for IoT, mobile, and embedded systems.
//!
//! # Design Goals
//!
//! - Minimal binary size (<2MB stripped)
//! - Low memory footprint (<16MB base)
//! - No runtime dependencies
//! - Deterministic performance
//!
//! # Feature Flags
//!
//! Control what's included in your build:
//!
//! ```toml
//! [features]
//! edge-minimal = []  # Core key-value only
//! edge-standard = [] # + Lists, Sets, Hashes
//! edge-full = []     # + Sorted Sets, Streams
//! ```

use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Edge-optimized configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgeConfig {
    /// Maximum memory usage in bytes
    pub max_memory: usize,
    /// Maximum number of keys
    pub max_keys: usize,
    /// Enable persistence
    pub persistence: bool,
    /// Persistence path
    pub persistence_path: Option<String>,
    /// Sync interval in milliseconds (0 = manual sync only)
    pub sync_interval_ms: u64,
    /// Enable compression
    pub compression: bool,
    /// Compression level (1-9)
    pub compression_level: u8,
    /// Enable key expiration
    pub enable_ttl: bool,
    /// TTL check interval in seconds
    pub ttl_check_interval_secs: u64,
    /// Value size limit in bytes
    pub max_value_size: usize,
    /// Key size limit in bytes
    pub max_key_size: usize,
    /// Read-only mode – all writes are rejected.
    pub readonly: bool,
}

impl Default for EdgeConfig {
    fn default() -> Self {
        Self {
            max_memory: 16 * 1024 * 1024, // 16MB
            max_keys: 100_000,
            persistence: false,
            persistence_path: None,
            sync_interval_ms: 0,
            compression: false,
            compression_level: 1,
            enable_ttl: true,
            ttl_check_interval_secs: 60,
            max_value_size: 1024 * 1024, // 1MB
            max_key_size: 1024,          // 1KB
            readonly: false,
        }
    }
}

impl EdgeConfig {
    /// Ultra-minimal configuration for constrained devices
    pub fn minimal() -> Self {
        Self {
            max_memory: 4 * 1024 * 1024, // 4MB
            max_keys: 10_000,
            persistence: false,
            persistence_path: None,
            sync_interval_ms: 0,
            compression: false,
            compression_level: 1,
            enable_ttl: false,
            ttl_check_interval_secs: 0,
            max_value_size: 64 * 1024, // 64KB
            max_key_size: 256,
            readonly: false,
        }
    }

    /// Standard edge configuration
    pub fn standard() -> Self {
        Self::default()
    }

    /// Full-featured edge configuration
    pub fn full() -> Self {
        Self {
            max_memory: 64 * 1024 * 1024, // 64MB
            max_keys: 1_000_000,
            persistence: true,
            persistence_path: None,
            sync_interval_ms: 5000,
            compression: true,
            compression_level: 3,
            enable_ttl: true,
            ttl_check_interval_secs: 30,
            max_value_size: 4 * 1024 * 1024, // 4MB
            max_key_size: 4096,
            readonly: false,
        }
    }

    /// Mobile-optimized configuration
    pub fn mobile() -> Self {
        Self {
            max_memory: 32 * 1024 * 1024, // 32MB
            max_keys: 50_000,
            persistence: true,
            persistence_path: None,
            sync_interval_ms: 10000, // Less frequent syncs for battery
            compression: true,
            compression_level: 6,
            enable_ttl: true,
            ttl_check_interval_secs: 120,
            max_value_size: 1024 * 1024,
            max_key_size: 512,
            readonly: false,
        }
    }

    /// IoT device configuration
    pub fn iot() -> Self {
        Self {
            max_memory: 2 * 1024 * 1024, // 2MB
            max_keys: 1_000,
            persistence: true,
            persistence_path: None,
            sync_interval_ms: 60000, // Infrequent syncs
            compression: true,
            compression_level: 9, // Maximum compression
            enable_ttl: true,
            ttl_check_interval_secs: 300,
            max_value_size: 4 * 1024, // 4KB
            max_key_size: 64,
            readonly: false,
        }
    }
}

/// Compact value representation for edge storage
#[derive(Clone)]
pub enum CompactValue {
    /// Inline small value (up to 23 bytes, fits in 24-byte struct).
    Inline(InlineValue),
    /// Heap-allocated value.
    Heap(Bytes),
    /// Compressed value.
    Compressed(CompressedValue),
}

/// Inline value for small data (avoids heap allocation)
#[derive(Clone, Copy)]
pub struct InlineValue {
    data: [u8; 23],
    len: u8,
}

impl InlineValue {
    const MAX_LEN: usize = 23;

    /// Create from bytes if small enough
    pub fn try_new(data: &[u8]) -> Option<Self> {
        if data.len() > Self::MAX_LEN {
            return None;
        }
        let mut value = Self {
            data: [0u8; 23],
            len: data.len() as u8,
        };
        value.data[..data.len()].copy_from_slice(data);
        Some(value)
    }

    /// Get the data as a slice
    pub fn as_slice(&self) -> &[u8] {
        &self.data[..self.len as usize]
    }

    /// Get the length
    pub fn len(&self) -> usize {
        self.len as usize
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// Compressed value wrapper
#[derive(Clone)]
pub struct CompressedValue {
    /// Compressed data
    data: Bytes,
    /// Original size
    original_size: u32,
    /// Compression algorithm
    algorithm: CompressionAlgorithm,
}

impl CompressedValue {
    /// Create a new compressed value
    pub fn new(data: Bytes, original_size: u32, algorithm: CompressionAlgorithm) -> Self {
        Self {
            data,
            original_size,
            algorithm,
        }
    }

    /// Get compressed data
    pub fn compressed_data(&self) -> &[u8] {
        &self.data
    }

    /// Get original size
    pub fn original_size(&self) -> usize {
        self.original_size as usize
    }

    /// Get compression ratio
    pub fn compression_ratio(&self) -> f32 {
        if self.original_size == 0 {
            1.0
        } else {
            self.data.len() as f32 / self.original_size as f32
        }
    }

    /// Decompress the value
    pub fn decompress(&self) -> Result<Bytes, EdgeError> {
        match self.algorithm {
            CompressionAlgorithm::None => Ok(self.data.clone()),
            CompressionAlgorithm::Lz4 => {
                let decompressed = lz4_flex::decompress_size_prepended(&self.data)
                    .map_err(|e| EdgeError::Decompression(e.to_string()))?;
                Ok(Bytes::from(decompressed))
            }
        }
    }
}

/// Compression algorithms
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    /// No compression
    None,
    /// LZ4 fast compression
    Lz4,
}

impl CompactValue {
    /// Create a compact value from bytes
    pub fn from_bytes(data: &[u8], compress: bool, _level: u8) -> Self {
        // Try inline first for small values
        if data.len() <= InlineValue::MAX_LEN {
            if let Some(inline) = InlineValue::try_new(data) {
                return CompactValue::Inline(inline);
            }
        }

        // Try compression for larger values
        if compress && data.len() > 64 {
            let compressed = lz4_flex::compress_prepend_size(data);
            // Only use compression if it actually reduces size
            if compressed.len() < data.len() {
                return CompactValue::Compressed(CompressedValue::new(
                    Bytes::from(compressed),
                    data.len() as u32,
                    CompressionAlgorithm::Lz4,
                ));
            }
        }

        // Fall back to heap allocation
        CompactValue::Heap(Bytes::copy_from_slice(data))
    }

    /// Get the value as bytes
    pub fn as_bytes(&self) -> Result<Bytes, EdgeError> {
        match self {
            CompactValue::Inline(v) => Ok(Bytes::copy_from_slice(v.as_slice())),
            CompactValue::Heap(b) => Ok(b.clone()),
            CompactValue::Compressed(c) => c.decompress(),
        }
    }

    /// Get the stored size (compressed if applicable)
    pub fn stored_size(&self) -> usize {
        match self {
            CompactValue::Inline(v) => v.len(),
            CompactValue::Heap(b) => b.len(),
            CompactValue::Compressed(c) => c.data.len(),
        }
    }

    /// Get the logical size (uncompressed)
    pub fn logical_size(&self) -> usize {
        match self {
            CompactValue::Inline(v) => v.len(),
            CompactValue::Heap(b) => b.len(),
            CompactValue::Compressed(c) => c.original_size(),
        }
    }

    /// Check if value is compressed
    pub fn is_compressed(&self) -> bool {
        matches!(self, CompactValue::Compressed(_))
    }

    /// Check if value is inline
    pub fn is_inline(&self) -> bool {
        matches!(self, CompactValue::Inline(_))
    }
}

/// Edge storage errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum EdgeError {
    /// Memory limit exceeded.
    #[error("memory limit exceeded: used {used}, limit {limit}")]
    MemoryLimitExceeded {
        /// Current memory usage in bytes.
        used: usize,
        /// Configured maximum memory in bytes.
        limit: usize,
    },

    /// Key limit exceeded.
    #[error("key limit exceeded: count {count}, limit {limit}")]
    KeyLimitExceeded {
        /// Current number of keys.
        count: usize,
        /// Configured maximum key count.
        limit: usize,
    },

    /// Value too large.
    #[error("value too large: size {size}, limit {limit}")]
    ValueTooLarge {
        /// Size of the rejected value in bytes.
        size: usize,
        /// Configured maximum value size in bytes.
        limit: usize,
    },

    /// Key too large.
    #[error("key too large: size {size}, limit {limit}")]
    KeyTooLarge {
        /// Size of the rejected key in bytes.
        size: usize,
        /// Configured maximum key size in bytes.
        limit: usize,
    },

    /// Key not found.
    #[error("key not found: {0}")]
    KeyNotFound(String),

    /// Compression error.
    #[error("compression error: {0}")]
    Compression(String),

    /// Decompression error.
    #[error("decompression error: {0}")]
    Decompression(String),

    /// Persistence error.
    #[error("persistence error: {0}")]
    Persistence(String),

    /// Type mismatch.
    #[error("type mismatch: expected {expected}, got {got}")]
    TypeMismatch {
        /// Expected type name.
        expected: String,
        /// Actual type name.
        got: String,
    },

    /// Store is in read-only mode.
    #[error("store is in read-only mode")]
    ReadOnly,
}

/// Entry in the edge store with metadata
struct EdgeEntry {
    /// The value
    value: CompactValue,
    /// Expiration time (Unix timestamp in seconds, 0 = no expiry)
    expires_at: u64,
    /// Last access time
    last_access: u64,
    /// Access count
    access_count: u32,
}

impl EdgeEntry {
    fn new(value: CompactValue, ttl_secs: Option<u64>) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            value,
            expires_at: ttl_secs.map(|t| now + t).unwrap_or(0),
            last_access: now,
            access_count: 0,
        }
    }

    fn is_expired(&self) -> bool {
        if self.expires_at == 0 {
            return false;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now >= self.expires_at
    }

    fn touch(&mut self) {
        self.last_access = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.access_count = self.access_count.saturating_add(1);
    }
}

/// Minimal edge key-value store
pub struct EdgeStore {
    /// Configuration
    config: EdgeConfig,
    /// Data storage
    data: RwLock<HashMap<String, EdgeEntry>>,
    /// Current memory usage estimate
    memory_used: AtomicUsize,
    /// Statistics
    stats: EdgeStats,
}

/// Edge store statistics
#[derive(Default)]
pub struct EdgeStats {
    /// Get operations
    pub gets: AtomicU64,
    /// Set operations
    pub sets: AtomicU64,
    /// Delete operations
    pub deletes: AtomicU64,
    /// Cache hits
    pub hits: AtomicU64,
    /// Cache misses
    pub misses: AtomicU64,
    /// Expired keys removed
    pub expired: AtomicU64,
    /// Evicted keys
    pub evicted: AtomicU64,
    /// Compression savings in bytes
    pub compression_savings: AtomicU64,
    /// Inline value count
    pub inline_values: AtomicU64,
}

impl EdgeStore {
    /// Create a new edge store
    pub fn new(config: EdgeConfig) -> Self {
        Self {
            config,
            data: RwLock::new(HashMap::new()),
            memory_used: AtomicUsize::new(0),
            stats: EdgeStats::default(),
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(EdgeConfig::default())
    }

    /// Create a minimal store
    pub fn minimal() -> Self {
        Self::new(EdgeConfig::minimal())
    }

    /// Get a value
    pub fn get(&self, key: &str) -> Result<Option<Bytes>, EdgeError> {
        self.stats.gets.fetch_add(1, Ordering::Relaxed);

        let mut data = self.data.write();

        if let Some(entry) = data.get_mut(key) {
            if entry.is_expired() {
                let size = entry.value.stored_size() + key.len();
                data.remove(key);
                self.memory_used.fetch_sub(size, Ordering::Relaxed);
                self.stats.expired.fetch_add(1, Ordering::Relaxed);
                self.stats.misses.fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }

            entry.touch();
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            return entry.value.as_bytes().map(Some);
        }

        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }

    /// Set a value
    pub fn set(&self, key: &str, value: &[u8], ttl_secs: Option<u64>) -> Result<(), EdgeError> {
        // Reject writes in read-only mode
        if self.config.readonly {
            return Err(EdgeError::ReadOnly);
        }

        // Validate sizes
        if key.len() > self.config.max_key_size {
            return Err(EdgeError::KeyTooLarge {
                size: key.len(),
                limit: self.config.max_key_size,
            });
        }

        if value.len() > self.config.max_value_size {
            return Err(EdgeError::ValueTooLarge {
                size: value.len(),
                limit: self.config.max_value_size,
            });
        }

        let compact = CompactValue::from_bytes(
            value,
            self.config.compression,
            self.config.compression_level,
        );

        let entry_size = compact.stored_size() + key.len();

        // Track compression savings
        if compact.is_compressed() {
            let savings = value.len() - compact.stored_size();
            self.stats
                .compression_savings
                .fetch_add(savings as u64, Ordering::Relaxed);
        }

        if compact.is_inline() {
            self.stats.inline_values.fetch_add(1, Ordering::Relaxed);
        }

        let mut data = self.data.write();

        // Check key limit
        if !data.contains_key(key) && data.len() >= self.config.max_keys {
            // Try to evict expired entries first
            self.evict_expired_locked(&mut data);

            if data.len() >= self.config.max_keys {
                // Evict LRU entry
                self.evict_lru_locked(&mut data);
            }

            if data.len() >= self.config.max_keys {
                return Err(EdgeError::KeyLimitExceeded {
                    count: data.len(),
                    limit: self.config.max_keys,
                });
            }
        }

        // Check memory limit
        let current_memory = self.memory_used.load(Ordering::Relaxed);
        let old_size = data
            .get(key)
            .map(|e| e.value.stored_size() + key.len())
            .unwrap_or(0);
        let new_memory = current_memory - old_size + entry_size;

        if new_memory > self.config.max_memory {
            // Try eviction
            self.evict_expired_locked(&mut data);
            self.evict_lru_locked(&mut data);

            let current_memory = self.memory_used.load(Ordering::Relaxed);
            if current_memory + entry_size - old_size > self.config.max_memory {
                return Err(EdgeError::MemoryLimitExceeded {
                    used: current_memory,
                    limit: self.config.max_memory,
                });
            }
        }

        // Update memory tracking
        self.memory_used.fetch_sub(old_size, Ordering::Relaxed);
        self.memory_used.fetch_add(entry_size, Ordering::Relaxed);

        let entry = EdgeEntry::new(compact, ttl_secs);
        data.insert(key.to_string(), entry);

        self.stats.sets.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Delete a key
    pub fn delete(&self, key: &str) -> bool {
        let mut data = self.data.write();

        if let Some(entry) = data.remove(key) {
            let size = entry.value.stored_size() + key.len();
            self.memory_used.fetch_sub(size, Ordering::Relaxed);
            self.stats.deletes.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Check if a key exists
    pub fn exists(&self, key: &str) -> bool {
        let data = self.data.read();
        if let Some(entry) = data.get(key) {
            !entry.is_expired()
        } else {
            false
        }
    }

    /// Get the number of keys
    pub fn len(&self) -> usize {
        self.data.read().len()
    }

    /// Check if store is empty
    pub fn is_empty(&self) -> bool {
        self.data.read().is_empty()
    }

    /// Get memory usage
    pub fn memory_usage(&self) -> usize {
        self.memory_used.load(Ordering::Relaxed)
    }

    /// Get statistics
    pub fn stats(&self) -> EdgeStoreStats {
        EdgeStoreStats {
            keys: self.len(),
            memory_used: self.memory_used.load(Ordering::Relaxed),
            memory_limit: self.config.max_memory,
            gets: self.stats.gets.load(Ordering::Relaxed),
            sets: self.stats.sets.load(Ordering::Relaxed),
            deletes: self.stats.deletes.load(Ordering::Relaxed),
            hits: self.stats.hits.load(Ordering::Relaxed),
            misses: self.stats.misses.load(Ordering::Relaxed),
            hit_rate: self.hit_rate(),
            expired: self.stats.expired.load(Ordering::Relaxed),
            evicted: self.stats.evicted.load(Ordering::Relaxed),
            compression_savings: self.stats.compression_savings.load(Ordering::Relaxed),
            inline_values: self.stats.inline_values.load(Ordering::Relaxed),
        }
    }

    /// Clear all data
    pub fn clear(&self) {
        let mut data = self.data.write();
        data.clear();
        self.memory_used.store(0, Ordering::Relaxed);
    }

    /// Get all keys (for iteration)
    pub fn keys(&self) -> Vec<String> {
        self.data.read().keys().cloned().collect()
    }

    /// Run TTL cleanup
    pub fn cleanup_expired(&self) -> usize {
        let mut data = self.data.write();
        self.evict_expired_locked(&mut data)
    }

    /// Get hit rate
    pub fn hit_rate(&self) -> f64 {
        let hits = self.stats.hits.load(Ordering::Relaxed);
        let misses = self.stats.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    // === Private methods ===

    fn evict_expired_locked(&self, data: &mut HashMap<String, EdgeEntry>) -> usize {
        let expired: Vec<String> = data
            .iter()
            .filter(|(_, e)| e.is_expired())
            .map(|(k, _)| k.clone())
            .collect();

        let count = expired.len();

        for key in expired {
            if let Some(entry) = data.remove(&key) {
                let size = entry.value.stored_size() + key.len();
                self.memory_used.fetch_sub(size, Ordering::Relaxed);
                self.stats.expired.fetch_add(1, Ordering::Relaxed);
            }
        }

        count
    }

    fn evict_lru_locked(&self, data: &mut HashMap<String, EdgeEntry>) {
        // Find LRU entry
        let lru_key = data
            .iter()
            .min_by_key(|(_, e)| e.last_access)
            .map(|(k, _)| k.clone());

        if let Some(key) = lru_key {
            if let Some(entry) = data.remove(&key) {
                let size = entry.value.stored_size() + key.len();
                self.memory_used.fetch_sub(size, Ordering::Relaxed);
                self.stats.evicted.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Edge store statistics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgeStoreStats {
    /// Number of keys
    pub keys: usize,
    /// Memory used in bytes
    pub memory_used: usize,
    /// Memory limit in bytes
    pub memory_limit: usize,
    /// Total get operations
    pub gets: u64,
    /// Total set operations
    pub sets: u64,
    /// Total delete operations
    pub deletes: u64,
    /// Cache hits
    pub hits: u64,
    /// Cache misses
    pub misses: u64,
    /// Hit rate
    pub hit_rate: f64,
    /// Expired keys
    pub expired: u64,
    /// Evicted keys
    pub evicted: u64,
    /// Compression savings in bytes
    pub compression_savings: u64,
    /// Number of inline values
    pub inline_values: u64,
}

// SAFETY: EdgeStore is safe to send across threads because all interior mutability is
// managed through RwLock (data) and atomics (memory_used, stats). No raw pointers or
// non-Send types are stored. The RwLock ensures exclusive access for writes.
unsafe impl Send for EdgeStore {}
// SAFETY: EdgeStore is safe to share across threads (&EdgeStore) because reads go through
// RwLock::read and stats use atomic operations, preventing data races.
unsafe impl Sync for EdgeStore {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_edge_config_presets() {
        let minimal = EdgeConfig::minimal();
        assert_eq!(minimal.max_memory, 4 * 1024 * 1024);

        let mobile = EdgeConfig::mobile();
        assert!(mobile.compression);

        let iot = EdgeConfig::iot();
        assert_eq!(iot.max_keys, 1_000);
    }

    #[test]
    fn test_inline_value() {
        let small_data = b"hello";
        let inline = InlineValue::try_new(small_data);
        assert!(inline.is_some());
        assert_eq!(inline.unwrap().as_slice(), small_data);

        let large_data = vec![0u8; 100];
        let inline = InlineValue::try_new(&large_data);
        assert!(inline.is_none());
    }

    #[test]
    fn test_compact_value_inline() {
        let value = CompactValue::from_bytes(b"hello", false, 0);
        assert!(value.is_inline());
        assert_eq!(value.as_bytes().unwrap().as_ref(), b"hello");
    }

    #[test]
    fn test_compact_value_heap() {
        let data = vec![0u8; 100];
        let value = CompactValue::from_bytes(&data, false, 0);
        assert!(!value.is_inline());
        assert!(!value.is_compressed());
    }

    #[test]
    fn test_compact_value_compressed() {
        // Compressible data (repeated pattern)
        let data = vec![b'a'; 1000];
        let value = CompactValue::from_bytes(&data, true, 1);
        assert!(value.is_compressed());
        assert!(value.stored_size() < value.logical_size());

        // Verify decompression
        let decompressed = value.as_bytes().unwrap();
        assert_eq!(decompressed.as_ref(), &data[..]);
    }

    #[test]
    fn test_edge_store_basic() {
        let store = EdgeStore::minimal();

        store.set("key1", b"value1", None).unwrap();
        assert_eq!(store.get("key1").unwrap(), Some(Bytes::from("value1")));

        assert!(store.delete("key1"));
        assert_eq!(store.get("key1").unwrap(), None);
    }

    #[test]
    fn test_edge_store_ttl() {
        let store = EdgeStore::with_defaults();

        // Set with 0 second TTL (immediate expiry)
        store.set("expiring", b"value", Some(0)).unwrap();

        // Should be expired immediately
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert_eq!(store.get("expiring").unwrap(), None);
    }

    #[test]
    fn test_edge_store_size_limits() {
        let config = EdgeConfig {
            max_key_size: 10,
            max_value_size: 100,
            ..EdgeConfig::minimal()
        };
        let store = EdgeStore::new(config);

        // Key too large
        let result = store.set("this_key_is_too_long", b"value", None);
        assert!(matches!(result, Err(EdgeError::KeyTooLarge { .. })));

        // Value too large
        let large_value = vec![0u8; 200];
        let result = store.set("key", &large_value, None);
        assert!(matches!(result, Err(EdgeError::ValueTooLarge { .. })));
    }

    #[test]
    fn test_edge_store_stats() {
        let store = EdgeStore::minimal();

        store.set("key1", b"value1", None).unwrap();
        store.get("key1").unwrap();
        store.get("nonexistent").unwrap();

        let stats = store.stats();
        assert_eq!(stats.sets, 1);
        assert_eq!(stats.gets, 2);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hit_rate, 0.5);
    }

    #[test]
    fn test_edge_store_memory_tracking() {
        let store = EdgeStore::minimal();

        store.set("key1", b"value1", None).unwrap();
        let mem1 = store.memory_usage();
        assert!(mem1 > 0);

        store.set("key2", b"value2", None).unwrap();
        let mem2 = store.memory_usage();
        assert!(mem2 > mem1);

        store.delete("key1");
        let mem3 = store.memory_usage();
        assert!(mem3 < mem2);
    }

    #[test]
    fn test_edge_store_readonly() {
        let config = EdgeConfig {
            readonly: true,
            ..EdgeConfig::minimal()
        };
        let store = EdgeStore::new(config);

        let result = store.set("key", b"value", None);
        assert!(matches!(result, Err(EdgeError::ReadOnly)));
    }
}
