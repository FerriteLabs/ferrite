//! WASM+WASI Edge Adapter
//!
//! Compilation and deployment targets for running Ferrite at the edge via
//! WebAssembly. Provides platform adapters for Cloudflare Workers, Deno Deploy,
//! Fastly Compute, and generic WASI runtimes.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────┐
//! │              Ferrite Edge Runtime                │
//! │  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
//! │  │ In-Memory│  │  RESP    │  │  Sync    │      │
//! │  │  Store   │  │ Protocol │  │  Engine  │      │
//! │  └──────────┘  └──────────┘  └──────────┘      │
//! ├─────────────────────────────────────────────────┤
//! │              Platform Adapters                   │
//! │  ┌────────────┐ ┌──────┐ ┌────────┐ ┌──────┐   │
//! │  │ Cloudflare │ │ Deno │ │ Fastly │ │ WASI │   │
//! │  └────────────┘ └──────┘ └────────┘ └──────┘   │
//! └─────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Platform targets
// ---------------------------------------------------------------------------

/// Supported edge deployment platforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EdgePlatform {
    /// Cloudflare Workers (uses KV / Durable Objects for persistence)
    CloudflareWorkers,
    /// Deno Deploy
    DenoDeploy,
    /// Fastly Compute
    FastlyCompute,
    /// Generic WASI runtime
    Wasi,
    /// Embedded WASM (in-process)
    EmbeddedWasm,
}

impl std::fmt::Display for EdgePlatform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CloudflareWorkers => write!(f, "cloudflare-workers"),
            Self::DenoDeploy => write!(f, "deno-deploy"),
            Self::FastlyCompute => write!(f, "fastly-compute"),
            Self::Wasi => write!(f, "wasi"),
            Self::EmbeddedWasm => write!(f, "embedded-wasm"),
        }
    }
}

// ---------------------------------------------------------------------------
// WASI Adapter configuration
// ---------------------------------------------------------------------------

/// Configuration for building a WASM+WASI edge bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasiAdapterConfig {
    /// Target platform
    pub platform: EdgePlatform,
    /// Maximum memory (bytes) the WASM module may use
    pub max_memory_bytes: usize,
    /// Features to include in the lite build
    pub features: WasiFeatureFlags,
    /// Persistence adapter (platform-specific)
    pub persistence: PersistenceAdapter,
    /// Origin server URL for replication
    pub origin_url: Option<String>,
    /// Sync interval with origin
    pub sync_interval: Duration,
    /// Maximum WASM binary size
    pub max_binary_size: usize,
}

impl Default for WasiAdapterConfig {
    fn default() -> Self {
        Self {
            platform: EdgePlatform::Wasi,
            max_memory_bytes: 128 * 1024 * 1024, // 128 MB
            features: WasiFeatureFlags::default(),
            persistence: PersistenceAdapter::InMemory,
            origin_url: None,
            sync_interval: Duration::from_secs(30),
            max_binary_size: 50 * 1024 * 1024, // 50 MB
        }
    }
}

/// Feature flags for lite/edge builds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasiFeatureFlags {
    pub strings: bool,
    pub lists: bool,
    pub hashes: bool,
    pub sets: bool,
    pub sorted_sets: bool,
    pub pubsub: bool,
    pub expiry: bool,
    pub transactions: bool,
    pub vector_search: bool,
    pub semantic_cache: bool,
}

impl Default for WasiFeatureFlags {
    fn default() -> Self {
        Self {
            strings: true,
            lists: true,
            hashes: true,
            sets: true,
            sorted_sets: true,
            pubsub: true,
            expiry: true,
            transactions: false,
            vector_search: false,
            semantic_cache: false,
        }
    }
}

/// Persistence backends for edge deployments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PersistenceAdapter {
    /// No persistence — all data in WASM linear memory
    InMemory,
    /// Cloudflare KV for persistence
    CloudflareKv { namespace_id: String },
    /// Cloudflare Durable Objects
    CloudflareDurableObject { class_name: String },
    /// Generic HTTP endpoint for persistence
    HttpStore { endpoint: String },
    /// Local filesystem (WASI only)
    WasiFilesystem { path: String },
}

impl Default for PersistenceAdapter {
    fn default() -> Self {
        Self::InMemory
    }
}

// ---------------------------------------------------------------------------
// Edge Sync Protocol
// ---------------------------------------------------------------------------

/// Lightweight replication protocol for eventual consistency between edge and origin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncMessage {
    /// Monotonic sequence number
    pub sequence: u64,
    /// Operation (SET, DEL, EXPIRE, etc.)
    pub operation: SyncOperation,
    /// Originating node ID
    pub node_id: String,
    /// Timestamp (ms since epoch)
    pub timestamp: u64,
}

/// Sync operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncOperation {
    Set {
        key: String,
        value: Vec<u8>,
        ttl_ms: Option<u64>,
    },
    Delete {
        key: String,
    },
    Expire {
        key: String,
        ttl_ms: u64,
    },
    FullSync {
        snapshot: Vec<(String, Vec<u8>)>,
    },
}

/// State of the sync engine.
#[derive(Debug)]
pub struct SyncState {
    pub last_sequence: AtomicU64,
    pub pending_ops: AtomicU64,
    pub connected: AtomicBool,
    pub last_sync: parking_lot::RwLock<Option<Instant>>,
}

impl Default for SyncState {
    fn default() -> Self {
        Self {
            last_sequence: AtomicU64::new(0),
            pending_ops: AtomicU64::new(0),
            connected: AtomicBool::new(false),
            last_sync: parking_lot::RwLock::new(None),
        }
    }
}

impl SyncState {
    pub fn mark_synced(&self) {
        *self.last_sync.write() = Some(Instant::now());
        self.pending_ops.store(0, Ordering::Relaxed);
    }

    pub fn record_op(&self) -> u64 {
        self.pending_ops.fetch_add(1, Ordering::Relaxed);
        self.last_sequence.fetch_add(1, Ordering::SeqCst)
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Edge Instance
// ---------------------------------------------------------------------------

/// An edge Ferrite instance — the central coordinator for edge deployments.
pub struct EdgeInstance {
    config: WasiAdapterConfig,
    sync_state: Arc<SyncState>,
    metrics: Arc<EdgeMetrics>,
    store: Arc<parking_lot::RwLock<HashMap<String, EdgeValue>>>,
}

#[derive(Debug, Clone)]
struct EdgeValue {
    data: Vec<u8>,
    expires_at: Option<u64>,
}

/// Metrics for the edge instance.
#[derive(Debug, Default)]
pub struct EdgeMetrics {
    pub commands_processed: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub sync_operations: AtomicU64,
    pub bytes_stored: AtomicU64,
    pub cold_starts: AtomicU64,
}

impl EdgeInstance {
    /// Create a new edge instance.
    pub fn new(config: WasiAdapterConfig) -> Self {
        Self {
            config,
            sync_state: Arc::new(SyncState::default()),
            metrics: Arc::new(EdgeMetrics::default()),
            store: Arc::new(parking_lot::RwLock::new(HashMap::new())),
        }
    }

    /// Execute a GET operation.
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.metrics
            .commands_processed
            .fetch_add(1, Ordering::Relaxed);
        let store = self.store.read();
        match store.get(key) {
            Some(val) => {
                if let Some(exp) = val.expires_at {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    if now > exp {
                        self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
                        return None;
                    }
                }
                self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                Some(val.data.clone())
            }
            None => {
                self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Execute a SET operation.
    pub fn set(&self, key: &str, value: Vec<u8>, ttl_ms: Option<u64>) {
        self.metrics
            .commands_processed
            .fetch_add(1, Ordering::Relaxed);
        let expires_at = ttl_ms.map(|ttl| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64
                + ttl
        });

        self.metrics
            .bytes_stored
            .fetch_add(value.len() as u64, Ordering::Relaxed);

        let mut store = self.store.write();
        store.insert(
            key.to_string(),
            EdgeValue {
                data: value,
                expires_at,
            },
        );

        self.sync_state.record_op();
    }

    /// Execute a DEL operation.
    pub fn del(&self, key: &str) -> bool {
        self.metrics
            .commands_processed
            .fetch_add(1, Ordering::Relaxed);
        let mut store = self.store.write();
        let existed = store.remove(key).is_some();
        if existed {
            self.sync_state.record_op();
        }
        existed
    }

    /// Check if key exists.
    pub fn exists(&self, key: &str) -> bool {
        let store = self.store.read();
        store.contains_key(key)
    }

    /// Get number of keys.
    pub fn dbsize(&self) -> usize {
        self.store.read().len()
    }

    /// Generate a sync snapshot of all data.
    pub fn snapshot(&self) -> Vec<(String, Vec<u8>)> {
        let store = self.store.read();
        store
            .iter()
            .map(|(k, v)| (k.clone(), v.data.clone()))
            .collect()
    }

    /// Apply a sync message from the origin.
    pub fn apply_sync(&self, msg: SyncMessage) {
        self.metrics.sync_operations.fetch_add(1, Ordering::Relaxed);
        match msg.operation {
            SyncOperation::Set { key, value, ttl_ms } => {
                self.set(&key, value, ttl_ms);
            }
            SyncOperation::Delete { key } => {
                self.del(&key);
            }
            SyncOperation::Expire { key, ttl_ms } => {
                let mut store = self.store.write();
                if let Some(val) = store.get_mut(&key) {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    val.expires_at = Some(now + ttl_ms);
                }
            }
            SyncOperation::FullSync { snapshot } => {
                let mut store = self.store.write();
                store.clear();
                for (k, v) in snapshot {
                    store.insert(
                        k,
                        EdgeValue {
                            data: v,
                            expires_at: None,
                        },
                    );
                }
            }
        }
        self.sync_state
            .last_sequence
            .store(msg.sequence, Ordering::SeqCst);
    }

    /// Get metrics reference.
    pub fn metrics(&self) -> &EdgeMetrics {
        &self.metrics
    }

    /// Get sync state reference.
    pub fn sync_state(&self) -> &SyncState {
        &self.sync_state
    }

    /// Get configuration.
    pub fn config(&self) -> &WasiAdapterConfig {
        &self.config
    }

    /// Memory usage estimation.
    pub fn estimated_memory_bytes(&self) -> usize {
        let store = self.store.read();
        store
            .iter()
            .map(|(k, v)| k.len() + v.data.len() + 32) // 32 bytes overhead per entry
            .sum()
    }

    /// Check if within memory budget.
    pub fn within_memory_budget(&self) -> bool {
        self.estimated_memory_bytes() < self.config.max_memory_bytes
    }
}

// ---------------------------------------------------------------------------
// Build manifest
// ---------------------------------------------------------------------------

/// Describes the contents and capabilities of a WASM edge bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeBuildManifest {
    pub name: String,
    pub version: String,
    pub platform: EdgePlatform,
    pub features: WasiFeatureFlags,
    pub wasm_size_bytes: usize,
    pub supported_commands: Vec<String>,
    pub persistence: PersistenceAdapter,
    pub created_at: String,
}

impl EdgeBuildManifest {
    /// Create a manifest for the current configuration.
    pub fn from_config(config: &WasiAdapterConfig) -> Self {
        let mut commands = vec![
            "GET", "SET", "DEL", "EXISTS", "EXPIRE", "TTL", "PING", "ECHO", "DBSIZE",
        ];
        if config.features.lists {
            commands.extend_from_slice(&["LPUSH", "RPUSH", "LPOP", "RPOP", "LRANGE", "LLEN"]);
        }
        if config.features.hashes {
            commands.extend_from_slice(&["HSET", "HGET", "HDEL", "HGETALL", "HLEN"]);
        }
        if config.features.sets {
            commands.extend_from_slice(&["SADD", "SREM", "SMEMBERS", "SCARD"]);
        }
        if config.features.sorted_sets {
            commands.extend_from_slice(&["ZADD", "ZREM", "ZSCORE", "ZRANGE", "ZCARD"]);
        }
        if config.features.pubsub {
            commands.extend_from_slice(&["PUBLISH", "SUBSCRIBE", "UNSUBSCRIBE"]);
        }

        Self {
            name: "ferrite-edge".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            platform: config.platform,
            features: config.features.clone(),
            wasm_size_bytes: 0, // filled after build
            supported_commands: commands.into_iter().map(String::from).collect(),
            persistence: config.persistence.clone(),
            created_at: chrono::Utc::now().to_rfc3339(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_edge_instance_get_set_del() {
        let inst = EdgeInstance::new(WasiAdapterConfig::default());

        assert!(inst.get("key1").is_none());
        inst.set("key1", b"value1".to_vec(), None);
        assert_eq!(inst.get("key1"), Some(b"value1".to_vec()));

        assert!(inst.del("key1"));
        assert!(inst.get("key1").is_none());
        assert!(!inst.del("key1"));
    }

    #[test]
    fn test_edge_instance_dbsize() {
        let inst = EdgeInstance::new(WasiAdapterConfig::default());
        assert_eq!(inst.dbsize(), 0);
        inst.set("a", b"1".to_vec(), None);
        inst.set("b", b"2".to_vec(), None);
        assert_eq!(inst.dbsize(), 2);
    }

    #[test]
    fn test_edge_instance_snapshot_and_sync() {
        let inst = EdgeInstance::new(WasiAdapterConfig::default());
        inst.set("x", b"1".to_vec(), None);
        inst.set("y", b"2".to_vec(), None);

        let snap = inst.snapshot();
        assert_eq!(snap.len(), 2);

        let inst2 = EdgeInstance::new(WasiAdapterConfig::default());
        inst2.apply_sync(SyncMessage {
            sequence: 1,
            operation: SyncOperation::FullSync { snapshot: snap },
            node_id: "origin".to_string(),
            timestamp: 0,
        });
        assert_eq!(inst2.get("x"), Some(b"1".to_vec()));
        assert_eq!(inst2.get("y"), Some(b"2".to_vec()));
    }

    #[test]
    fn test_memory_budget() {
        let config = WasiAdapterConfig {
            max_memory_bytes: 100,
            ..Default::default()
        };
        let inst = EdgeInstance::new(config);
        assert!(inst.within_memory_budget());

        inst.set("big_key", vec![0u8; 200], None);
        assert!(!inst.within_memory_budget());
    }

    #[test]
    fn test_build_manifest() {
        let config = WasiAdapterConfig::default();
        let manifest = EdgeBuildManifest::from_config(&config);
        assert_eq!(manifest.platform, EdgePlatform::Wasi);
        assert!(manifest.supported_commands.contains(&"GET".to_string()));
        assert!(manifest.supported_commands.contains(&"LPUSH".to_string()));
    }

    #[test]
    fn test_sync_state() {
        let state = SyncState::default();
        assert_eq!(state.last_sequence.load(Ordering::SeqCst), 0);
        let seq = state.record_op();
        assert_eq!(seq, 0);
        assert_eq!(state.pending_ops.load(Ordering::Relaxed), 1);
        state.mark_synced();
        assert_eq!(state.pending_ops.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_edge_platform_display() {
        assert_eq!(
            EdgePlatform::CloudflareWorkers.to_string(),
            "cloudflare-workers"
        );
        assert_eq!(EdgePlatform::Wasi.to_string(), "wasi");
    }
}
