//! Production-grade metrics instrumentation for Ferrite
//!
//! This module provides a comprehensive instrumentation layer for recording
//! and exporting operational metrics: command latencies, connection events,
//! memory usage, storage tiering, replication lag, and keyspace statistics.
//!
//! # Example
//!
//! ```rust
//! use ferrite_core::metrics::instrumentation::{MetricsRegistry, ConnectionEvent, StorageTier, StorageOp};
//!
//! let registry = MetricsRegistry::new();
//! registry.record_command("GET", 150, true);
//! registry.record_connection(ConnectionEvent::Opened);
//! registry.record_storage_op(StorageTier::Memory, StorageOp::Read, 42, 1024);
//!
//! let snapshot = registry.get_snapshot();
//! assert_eq!(snapshot.total_commands, 1);
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/// Errors that can occur during metrics operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MetricsError {
    /// A metric could not be registered.
    RegistrationFailed(String),
    /// A snapshot could not be generated.
    SnapshotFailed(String),
    /// An internal metrics error.
    Internal(String),
}

impl std::fmt::Display for MetricsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetricsError::RegistrationFailed(msg) => write!(f, "Registration failed: {msg}"),
            MetricsError::SnapshotFailed(msg) => write!(f, "Snapshot failed: {msg}"),
            MetricsError::Internal(msg) => write!(f, "Internal metrics error: {msg}"),
        }
    }
}

impl std::error::Error for MetricsError {}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// A connection lifecycle event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConnectionEvent {
    /// A new client connection was opened.
    Opened,
    /// A client connection was closed.
    Closed,
    /// A connection attempt was rejected.
    Rejected {
        /// The reason the connection was rejected.
        reason: String,
    },
    /// A client failed authentication.
    AuthFailed,
}

/// The storage tier where an operation occurred.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum StorageTier {
    /// In-memory mutable region.
    Memory,
    /// Memory-mapped read-only region.
    Mmap,
    /// On-disk region via io_uring.
    Disk,
    /// Remote cloud object storage.
    Cloud,
}

/// The type of storage operation.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum StorageOp {
    /// A read operation.
    Read,
    /// A write operation.
    Write,
    /// A delete operation.
    Delete,
    /// A compaction operation.
    Compact,
    /// A tier migration operation.
    Migrate,
}

/// The reason a key was evicted.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum EvictionReason {
    /// Least-recently-used eviction.
    Lru,
    /// Least-frequently-used eviction.
    Lfu,
    /// Time-to-live expiration.
    Ttl,
    /// Manual eviction by a client.
    Manual,
    /// Eviction due to memory pressure.
    MemoryPressure,
}

/// A keyspace event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KeyEvent {
    /// A key was set.
    Set,
    /// A key was read.
    Get,
    /// A key was deleted.
    Delete,
    /// A key expired.
    Expire,
    /// A key was evicted.
    Evict {
        /// The reason for eviction.
        reason: EvictionReason,
    },
}

// ---------------------------------------------------------------------------
// Snapshot / metrics structs
// ---------------------------------------------------------------------------

/// Per-command execution statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandMetrics {
    /// The command name (e.g. "GET", "SET").
    pub command: String,
    /// Total number of invocations.
    pub calls: u64,
    /// Total number of failed invocations.
    pub errors: u64,
    /// Cumulative latency in microseconds.
    pub total_latency_us: u64,
    /// Minimum observed latency in microseconds.
    pub min_latency_us: u64,
    /// Maximum observed latency in microseconds.
    pub max_latency_us: u64,
    /// Average latency in microseconds.
    pub avg_latency_us: f64,
}

/// Latency percentiles computed from recent observations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LatencyPercentiles {
    /// 50th percentile (median) in microseconds.
    pub p50_us: u64,
    /// 90th percentile in microseconds.
    pub p90_us: u64,
    /// 95th percentile in microseconds.
    pub p95_us: u64,
    /// 99th percentile in microseconds.
    pub p99_us: u64,
    /// 99.9th percentile in microseconds.
    pub p999_us: u64,
}

/// A point-in-time snapshot of memory usage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemorySnapshot {
    /// Total memory available to the process in bytes.
    pub total_bytes: u64,
    /// Memory currently in use in bytes.
    pub used_bytes: u64,
    /// Resident set size in bytes.
    pub rss_bytes: u64,
    /// Fragmentation ratio (rss / used).
    pub fragmentation_ratio: f64,
    /// Peak memory usage in bytes.
    pub peak_bytes: u64,
    /// Bytes used by the dataset itself.
    pub dataset_bytes: u64,
    /// Bytes of internal overhead.
    pub overhead_bytes: u64,
    /// Memory usage broken down by type.
    pub by_type: HashMap<String, u64>,
}

impl Default for MemorySnapshot {
    fn default() -> Self {
        Self {
            total_bytes: 0,
            used_bytes: 0,
            rss_bytes: 0,
            fragmentation_ratio: 0.0,
            peak_bytes: 0,
            dataset_bytes: 0,
            overhead_bytes: 0,
            by_type: HashMap::new(),
        }
    }
}

/// Keyspace statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KeyspaceMetrics {
    /// Total number of keys.
    pub total_keys: u64,
    /// Number of keys with an expiration set.
    pub expires: u64,
    /// Average TTL of expiring keys in milliseconds.
    pub avg_ttl_ms: u64,
    /// Total number of evicted keys.
    pub evicted_keys: u64,
    /// Cache hit rate (hits / (hits + misses)).
    pub hit_rate: f64,
    /// Total cache hits.
    pub hits: u64,
    /// Total cache misses.
    pub misses: u64,
}

/// Persistence engine metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceMetrics {
    /// Whether the AOF is enabled.
    pub aof_enabled: bool,
    /// Current AOF file size in bytes.
    pub aof_size_bytes: u64,
    /// Duration of the last AOF write in milliseconds.
    pub aof_last_write_ms: u64,
    /// Timestamp of the last RDB save, if any.
    pub rdb_last_save_time: Option<DateTime<Utc>>,
    /// Number of changes since the last RDB save.
    pub rdb_changes_since_save: u64,
}

impl Default for PersistenceMetrics {
    fn default() -> Self {
        Self {
            aof_enabled: false,
            aof_size_bytes: 0,
            aof_last_write_ms: 0,
            rdb_last_save_time: None,
            rdb_changes_since_save: 0,
        }
    }
}

/// Replication statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReplicationMetrics {
    /// Role of this instance (e.g. "master" or "replica").
    pub role: String,
    /// Number of currently connected replicas.
    pub connected_replicas: u64,
    /// Current replication offset in bytes.
    pub replication_offset: u64,
    /// Estimated replication lag in milliseconds.
    pub replication_lag_ms: u64,
}

/// Tiered storage metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TieringMetrics {
    /// Keys in the in-memory tier.
    pub memory_keys: u64,
    /// Bytes in the in-memory tier.
    pub memory_bytes: u64,
    /// Keys in the mmap tier.
    pub mmap_keys: u64,
    /// Bytes in the mmap tier.
    pub mmap_bytes: u64,
    /// Keys in the disk tier.
    pub disk_keys: u64,
    /// Bytes in the disk tier.
    pub disk_bytes: u64,
    /// Keys in the cloud tier.
    pub cloud_keys: u64,
    /// Bytes in the cloud tier.
    pub cloud_bytes: u64,
    /// Total migrations performed.
    pub migrations_total: u64,
    /// Currently active migrations.
    pub migrations_active: u64,
}

/// A complete point-in-time metrics export.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    /// Timestamp of the snapshot.
    pub timestamp: DateTime<Utc>,
    /// Server uptime in seconds.
    pub uptime_secs: u64,
    /// Total commands processed.
    pub total_commands: u64,
    /// Total command errors.
    pub total_errors: u64,
    /// Estimated operations per second.
    pub ops_per_sec: f64,
    /// Currently connected clients.
    pub connected_clients: u64,
    /// Currently blocked clients.
    pub blocked_clients: u64,
    /// Total connections received since start.
    pub total_connections: u64,
    /// Total connections rejected since start.
    pub rejected_connections: u64,
    /// Memory usage snapshot.
    pub memory: MemorySnapshot,
    /// Keyspace statistics.
    pub keyspace: KeyspaceMetrics,
    /// Persistence statistics.
    pub persistence: PersistenceMetrics,
    /// Replication statistics.
    pub replication: ReplicationMetrics,
    /// Tiered storage statistics.
    pub tiering: TieringMetrics,
    /// Per-command statistics.
    pub command_stats: Vec<CommandMetrics>,
}

// ---------------------------------------------------------------------------
// LatencyTracker (internal)
// ---------------------------------------------------------------------------

/// Default capacity for latency sample buffers.
const DEFAULT_LATENCY_CAPACITY: usize = 10_000;

/// Fixed-size ring buffer for latency percentile computation.
///
/// Keeps the most recent `capacity` observations. On `percentiles()`, a
/// sorted clone is used to compute positional percentiles.
#[derive(Debug)]
struct LatencyTracker {
    /// Observation samples.
    samples: RwLock<Vec<u64>>,
    /// Maximum number of samples to retain.
    capacity: usize,
}

impl LatencyTracker {
    /// Create a new tracker with the given capacity.
    fn new(capacity: usize) -> Self {
        Self {
            samples: RwLock::new(Vec::with_capacity(capacity.min(1024))),
            capacity,
        }
    }

    /// Record a latency observation in microseconds.
    fn record(&self, value_us: u64) {
        let mut samples = self.samples.write();
        if samples.len() >= self.capacity {
            samples.remove(0);
        }
        samples.push(value_us);
    }

    /// Compute latency percentiles from recorded observations.
    ///
    /// Returns `None` if no observations have been recorded.
    fn percentiles(&self) -> Option<LatencyPercentiles> {
        let samples = self.samples.read();
        if samples.is_empty() {
            return None;
        }
        let mut sorted = samples.clone();
        sorted.sort_unstable();
        let len = sorted.len();

        let pct = |p: f64| -> u64 {
            let idx = ((p / 100.0) * len as f64).ceil() as usize;
            sorted[idx.saturating_sub(1).min(len - 1)]
        };

        Some(LatencyPercentiles {
            p50_us: pct(50.0),
            p90_us: pct(90.0),
            p95_us: pct(95.0),
            p99_us: pct(99.0),
            p999_us: pct(99.9),
        })
    }

    /// Return the number of recorded observations.
    #[allow(dead_code)]
    fn len(&self) -> usize {
        self.samples.read().len()
    }

    /// Clear all recorded observations.
    #[allow(dead_code)]
    fn clear(&self) {
        self.samples.write().clear();
    }
}

// ---------------------------------------------------------------------------
// Internal per-command state
// ---------------------------------------------------------------------------

/// Internal mutable state for a single command.
#[derive(Debug)]
struct CommandState {
    calls: AtomicU64,
    errors: AtomicU64,
    total_latency_us: AtomicU64,
    min_latency_us: AtomicU64,
    max_latency_us: AtomicU64,
    tracker: LatencyTracker,
}

impl CommandState {
    fn new() -> Self {
        Self {
            calls: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
            min_latency_us: AtomicU64::new(u64::MAX),
            max_latency_us: AtomicU64::new(0),
            tracker: LatencyTracker::new(DEFAULT_LATENCY_CAPACITY),
        }
    }

    fn record(&self, latency_us: u64, success: bool) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
        self.total_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);
        self.tracker.record(latency_us);

        // Update min
        let mut current = self.min_latency_us.load(Ordering::Relaxed);
        while latency_us < current {
            match self.min_latency_us.compare_exchange_weak(
                current,
                latency_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }

        // Update max
        current = self.max_latency_us.load(Ordering::Relaxed);
        while latency_us > current {
            match self.max_latency_us.compare_exchange_weak(
                current,
                latency_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    fn to_metrics(&self, command: &str) -> CommandMetrics {
        let calls = self.calls.load(Ordering::Relaxed);
        let total = self.total_latency_us.load(Ordering::Relaxed);
        let avg = if calls > 0 {
            total as f64 / calls as f64
        } else {
            0.0
        };
        let min = self.min_latency_us.load(Ordering::Relaxed);
        CommandMetrics {
            command: command.to_string(),
            calls,
            errors: self.errors.load(Ordering::Relaxed),
            total_latency_us: total,
            min_latency_us: if min == u64::MAX { 0 } else { min },
            max_latency_us: self.max_latency_us.load(Ordering::Relaxed),
            avg_latency_us: avg,
        }
    }
}

// ---------------------------------------------------------------------------
// Storage tier state
// ---------------------------------------------------------------------------

/// Internal mutable state for a (tier, op) pair.
#[derive(Debug)]
struct StorageTierState {
    ops: AtomicU64,
    total_latency_us: AtomicU64,
    total_bytes: AtomicU64,
}

impl StorageTierState {
    fn new() -> Self {
        Self {
            ops: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
        }
    }
}

// ---------------------------------------------------------------------------
// MetricsRegistry
// ---------------------------------------------------------------------------

/// Central instrumentation registry for Ferrite.
///
/// Thread-safe and lock-free on hot paths (command recording uses atomics).
/// Collects command latencies, connection events, memory snapshots, storage
/// tiering stats, replication lag, and keyspace events.
#[derive(Debug)]
pub struct MetricsRegistry {
    /// Instant when the registry was created.
    start_time: Instant,

    // -- commands --
    commands: RwLock<HashMap<String, CommandState>>,

    // -- connections --
    connected_clients: AtomicU64,
    total_connections: AtomicU64,
    rejected_connections: AtomicU64,
    auth_failures: AtomicU64,

    // -- memory --
    memory: RwLock<MemorySnapshot>,

    // -- storage --
    storage: RwLock<HashMap<(StorageTier, StorageOp), StorageTierState>>,

    // -- replication --
    replication_lag_bytes: AtomicU64,
    replication_lag_ms: AtomicU64,

    // -- keyspace --
    key_sets: AtomicU64,
    key_gets: AtomicU64,
    key_deletes: AtomicU64,
    key_expires: AtomicU64,
    key_evictions: AtomicU64,
    key_hits: AtomicU64,
    key_misses: AtomicU64,
}

impl MetricsRegistry {
    /// Create a new, empty metrics registry.
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            commands: RwLock::new(HashMap::new()),
            connected_clients: AtomicU64::new(0),
            total_connections: AtomicU64::new(0),
            rejected_connections: AtomicU64::new(0),
            auth_failures: AtomicU64::new(0),
            memory: RwLock::new(MemorySnapshot::default()),
            storage: RwLock::new(HashMap::new()),
            replication_lag_bytes: AtomicU64::new(0),
            replication_lag_ms: AtomicU64::new(0),
            key_sets: AtomicU64::new(0),
            key_gets: AtomicU64::new(0),
            key_deletes: AtomicU64::new(0),
            key_expires: AtomicU64::new(0),
            key_evictions: AtomicU64::new(0),
            key_hits: AtomicU64::new(0),
            key_misses: AtomicU64::new(0),
        }
    }

    /// Record a command execution.
    pub fn record_command(&self, cmd: &str, latency_us: u64, success: bool) {
        let upper = cmd.to_uppercase();
        // Fast path: read lock
        {
            let map = self.commands.read();
            if let Some(state) = map.get(&upper) {
                state.record(latency_us, success);
                return;
            }
        }
        // Slow path: write lock to insert
        let mut map = self.commands.write();
        let state = map.entry(upper).or_insert_with(CommandState::new);
        state.record(latency_us, success);
    }

    /// Record a connection lifecycle event.
    pub fn record_connection(&self, event: ConnectionEvent) {
        match event {
            ConnectionEvent::Opened => {
                self.connected_clients.fetch_add(1, Ordering::Relaxed);
                self.total_connections.fetch_add(1, Ordering::Relaxed);
            }
            ConnectionEvent::Closed => {
                // Saturating subtract via compare-exchange loop
                let mut current = self.connected_clients.load(Ordering::Relaxed);
                loop {
                    let new_val = current.saturating_sub(1);
                    match self.connected_clients.compare_exchange_weak(
                        current,
                        new_val,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(actual) => current = actual,
                    }
                }
            }
            ConnectionEvent::Rejected { .. } => {
                self.rejected_connections.fetch_add(1, Ordering::Relaxed);
            }
            ConnectionEvent::AuthFailed => {
                self.auth_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Record a memory usage snapshot.
    pub fn record_memory(&self, usage: MemorySnapshot) {
        *self.memory.write() = usage;
    }

    /// Record a storage tier operation.
    pub fn record_storage_op(&self, tier: StorageTier, op: StorageOp, latency_us: u64, bytes: u64) {
        let key = (tier, op);
        // Fast path
        {
            let map = self.storage.read();
            if let Some(state) = map.get(&key) {
                state.ops.fetch_add(1, Ordering::Relaxed);
                state
                    .total_latency_us
                    .fetch_add(latency_us, Ordering::Relaxed);
                state.total_bytes.fetch_add(bytes, Ordering::Relaxed);
                return;
            }
        }
        // Slow path
        let mut map = self.storage.write();
        let state = map.entry(key).or_insert_with(StorageTierState::new);
        state.ops.fetch_add(1, Ordering::Relaxed);
        state
            .total_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);
        state.total_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record replication lag.
    pub fn record_replication(&self, lag_bytes: u64, lag_ms: u64) {
        self.replication_lag_bytes
            .store(lag_bytes, Ordering::Relaxed);
        self.replication_lag_ms.store(lag_ms, Ordering::Relaxed);
    }

    /// Record a keyspace event.
    pub fn record_key_event(&self, event: KeyEvent) {
        match &event {
            KeyEvent::Set => {
                self.key_sets.fetch_add(1, Ordering::Relaxed);
            }
            KeyEvent::Get => {
                self.key_gets.fetch_add(1, Ordering::Relaxed);
                self.key_hits.fetch_add(1, Ordering::Relaxed);
            }
            KeyEvent::Delete => {
                self.key_deletes.fetch_add(1, Ordering::Relaxed);
            }
            KeyEvent::Expire => {
                self.key_expires.fetch_add(1, Ordering::Relaxed);
            }
            KeyEvent::Evict { .. } => {
                self.key_evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Record a cache miss (GET that did not find a key).
    ///
    /// Call this separately from `record_key_event(KeyEvent::Get)` when a GET
    /// does not find the requested key.
    pub fn record_miss(&self) {
        self.key_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Produce a complete metrics snapshot.
    pub fn get_snapshot(&self) -> MetricsSnapshot {
        let uptime = self.start_time.elapsed().as_secs();
        let total_commands: u64 = self
            .commands
            .read()
            .values()
            .map(|s| s.calls.load(Ordering::Relaxed))
            .sum();
        let total_errors: u64 = self
            .commands
            .read()
            .values()
            .map(|s| s.errors.load(Ordering::Relaxed))
            .sum();
        let ops_per_sec = if uptime > 0 {
            total_commands as f64 / uptime as f64
        } else {
            total_commands as f64
        };

        let hits = self.key_hits.load(Ordering::Relaxed);
        let misses = self.key_misses.load(Ordering::Relaxed);
        let hit_rate = if hits + misses > 0 {
            hits as f64 / (hits + misses) as f64
        } else {
            0.0
        };

        let tiering = self.build_tiering_metrics();

        MetricsSnapshot {
            timestamp: Utc::now(),
            uptime_secs: uptime,
            total_commands,
            total_errors,
            ops_per_sec,
            connected_clients: self.connected_clients.load(Ordering::Relaxed),
            blocked_clients: 0,
            total_connections: self.total_connections.load(Ordering::Relaxed),
            rejected_connections: self.rejected_connections.load(Ordering::Relaxed),
            memory: self.memory.read().clone(),
            keyspace: KeyspaceMetrics {
                total_keys: self.key_sets.load(Ordering::Relaxed),
                expires: self.key_expires.load(Ordering::Relaxed),
                avg_ttl_ms: 0,
                evicted_keys: self.key_evictions.load(Ordering::Relaxed),
                hit_rate,
                hits,
                misses,
            },
            persistence: PersistenceMetrics::default(),
            replication: ReplicationMetrics {
                role: "master".to_string(),
                connected_replicas: 0,
                replication_offset: self.replication_lag_bytes.load(Ordering::Relaxed),
                replication_lag_ms: self.replication_lag_ms.load(Ordering::Relaxed),
            },
            tiering,
            command_stats: self.get_command_stats(),
        }
    }

    /// Return per-command statistics sorted by command name.
    pub fn get_command_stats(&self) -> Vec<CommandMetrics> {
        let map = self.commands.read();
        let mut stats: Vec<CommandMetrics> = map
            .iter()
            .map(|(name, state)| state.to_metrics(name))
            .collect();
        stats.sort_by(|a, b| a.command.cmp(&b.command));
        stats
    }

    /// Compute latency percentiles for a specific command.
    pub fn get_latency_percentiles(&self, command: &str) -> Option<LatencyPercentiles> {
        let upper = command.to_uppercase();
        let map = self.commands.read();
        map.get(&upper)
            .and_then(|state| state.tracker.percentiles())
    }

    /// Reset all metrics to their initial state.
    pub fn reset(&self) {
        self.commands.write().clear();
        self.connected_clients.store(0, Ordering::Relaxed);
        self.total_connections.store(0, Ordering::Relaxed);
        self.rejected_connections.store(0, Ordering::Relaxed);
        self.auth_failures.store(0, Ordering::Relaxed);
        *self.memory.write() = MemorySnapshot::default();
        self.storage.write().clear();
        self.replication_lag_bytes.store(0, Ordering::Relaxed);
        self.replication_lag_ms.store(0, Ordering::Relaxed);
        self.key_sets.store(0, Ordering::Relaxed);
        self.key_gets.store(0, Ordering::Relaxed);
        self.key_deletes.store(0, Ordering::Relaxed);
        self.key_expires.store(0, Ordering::Relaxed);
        self.key_evictions.store(0, Ordering::Relaxed);
        self.key_hits.store(0, Ordering::Relaxed);
        self.key_misses.store(0, Ordering::Relaxed);
    }

    /// Build tiering metrics from the storage state map.
    fn build_tiering_metrics(&self) -> TieringMetrics {
        let map = self.storage.read();
        let mut tm = TieringMetrics::default();

        for ((tier, op), state) in map.iter() {
            let bytes = state.total_bytes.load(Ordering::Relaxed);
            let ops = state.ops.load(Ordering::Relaxed);
            match tier {
                StorageTier::Memory => {
                    tm.memory_bytes += bytes;
                    tm.memory_keys += ops;
                }
                StorageTier::Mmap => {
                    tm.mmap_bytes += bytes;
                    tm.mmap_keys += ops;
                }
                StorageTier::Disk => {
                    tm.disk_bytes += bytes;
                    tm.disk_keys += ops;
                }
                StorageTier::Cloud => {
                    tm.cloud_bytes += bytes;
                    tm.cloud_keys += ops;
                }
            }
            if matches!(op, StorageOp::Migrate) {
                tm.migrations_total += ops;
            }
        }
        tm
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// GrafanaDashboardGenerator
// ---------------------------------------------------------------------------

/// Generates a Grafana JSON dashboard for Ferrite metrics.
///
/// Produces a complete, importable Grafana dashboard with panels for
/// ops/sec, latency P99, memory usage, connected clients, hit rate,
/// tiering distribution, replication lag, keyspace, and command distribution.
#[derive(Debug, Clone)]
pub struct GrafanaDashboardGenerator;

impl GrafanaDashboardGenerator {
    /// Create a new dashboard generator.
    pub fn new() -> Self {
        Self
    }

    /// Generate a Grafana dashboard as a JSON string.
    pub fn generate(&self) -> String {
        serde_json::json!({
            "id": null,
            "uid": "ferrite-overview",
            "title": "Ferrite Overview",
            "tags": ["ferrite", "redis", "database"],
            "timezone": "browser",
            "schemaVersion": 39,
            "version": 1,
            "refresh": "10s",
            "time": {
                "from": "now-1h",
                "to": "now"
            },
            "templating": {
                "list": []
            },
            "annotations": {
                "list": []
            },
            "panels": [
                self.panel_ops_per_sec(0),
                self.panel_latency_p99(1),
                self.panel_memory_usage(2),
                self.panel_connected_clients(3),
                self.panel_hit_rate(4),
                self.panel_tiering_distribution(5),
                self.panel_replication_lag(6),
                self.panel_keyspace(7),
                self.panel_command_distribution(8),
            ]
        })
        .to_string()
    }

    fn panel_ops_per_sec(&self, id: u32) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "title": "Operations Per Second",
            "type": "timeseries",
            "gridPos": { "h": 8, "w": 12, "x": 0, "y": 0 },
            "targets": [{
                "expr": "rate(ferrite_commands_total[1m])",
                "legendFormat": "ops/sec"
            }],
            "fieldConfig": {
                "defaults": { "unit": "ops" }
            }
        })
    }

    fn panel_latency_p99(&self, id: u32) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "title": "Latency P99",
            "type": "timeseries",
            "gridPos": { "h": 8, "w": 12, "x": 12, "y": 0 },
            "targets": [{
                "expr": "histogram_quantile(0.99, rate(ferrite_command_duration_seconds_bucket[5m]))",
                "legendFormat": "p99"
            }],
            "fieldConfig": {
                "defaults": { "unit": "µs" }
            }
        })
    }

    fn panel_memory_usage(&self, id: u32) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "title": "Memory Usage",
            "type": "timeseries",
            "gridPos": { "h": 8, "w": 12, "x": 0, "y": 8 },
            "targets": [
                { "expr": "ferrite_memory_used_bytes", "legendFormat": "used" },
                { "expr": "ferrite_memory_rss_bytes", "legendFormat": "rss" },
                { "expr": "ferrite_memory_peak_bytes", "legendFormat": "peak" }
            ],
            "fieldConfig": {
                "defaults": { "unit": "bytes" }
            }
        })
    }

    fn panel_connected_clients(&self, id: u32) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "title": "Connected Clients",
            "type": "stat",
            "gridPos": { "h": 8, "w": 6, "x": 12, "y": 8 },
            "targets": [{
                "expr": "ferrite_connected_clients",
                "legendFormat": "clients"
            }]
        })
    }

    fn panel_hit_rate(&self, id: u32) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "title": "Cache Hit Rate",
            "type": "gauge",
            "gridPos": { "h": 8, "w": 6, "x": 18, "y": 8 },
            "targets": [{
                "expr": "ferrite_keyspace_hits / (ferrite_keyspace_hits + ferrite_keyspace_misses)",
                "legendFormat": "hit rate"
            }],
            "fieldConfig": {
                "defaults": { "unit": "percentunit", "min": 0, "max": 1 }
            }
        })
    }

    fn panel_tiering_distribution(&self, id: u32) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "title": "Tiering Distribution",
            "type": "piechart",
            "gridPos": { "h": 8, "w": 12, "x": 0, "y": 16 },
            "targets": [
                { "expr": "ferrite_tier_memory_bytes", "legendFormat": "memory" },
                { "expr": "ferrite_tier_mmap_bytes", "legendFormat": "mmap" },
                { "expr": "ferrite_tier_disk_bytes", "legendFormat": "disk" },
                { "expr": "ferrite_tier_cloud_bytes", "legendFormat": "cloud" }
            ]
        })
    }

    fn panel_replication_lag(&self, id: u32) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "title": "Replication Lag",
            "type": "timeseries",
            "gridPos": { "h": 8, "w": 12, "x": 12, "y": 16 },
            "targets": [{
                "expr": "ferrite_replication_lag_ms",
                "legendFormat": "lag"
            }],
            "fieldConfig": {
                "defaults": { "unit": "ms" }
            }
        })
    }

    fn panel_keyspace(&self, id: u32) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "title": "Keyspace",
            "type": "timeseries",
            "gridPos": { "h": 8, "w": 12, "x": 0, "y": 24 },
            "targets": [
                { "expr": "ferrite_keyspace_keys", "legendFormat": "total keys" },
                { "expr": "ferrite_keyspace_expires", "legendFormat": "expires" },
                { "expr": "ferrite_keyspace_evicted", "legendFormat": "evicted" }
            ]
        })
    }

    fn panel_command_distribution(&self, id: u32) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "title": "Command Distribution",
            "type": "barchart",
            "gridPos": { "h": 8, "w": 12, "x": 12, "y": 24 },
            "targets": [{
                "expr": "topk(10, sum by (command) (ferrite_commands_total))",
                "legendFormat": "{{command}}"
            }]
        })
    }
}

impl Default for GrafanaDashboardGenerator {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_recording_and_stats() {
        let reg = MetricsRegistry::new();
        reg.record_command("GET", 100, true);
        reg.record_command("GET", 200, true);
        reg.record_command("GET", 300, false);
        reg.record_command("SET", 50, true);

        let stats = reg.get_command_stats();
        assert_eq!(stats.len(), 2);

        let get = stats.iter().find(|s| s.command == "GET").unwrap();
        assert_eq!(get.calls, 3);
        assert_eq!(get.errors, 1);
        assert_eq!(get.total_latency_us, 600);
        assert_eq!(get.min_latency_us, 100);
        assert_eq!(get.max_latency_us, 300);
        assert!((get.avg_latency_us - 200.0).abs() < f64::EPSILON);

        let set = stats.iter().find(|s| s.command == "SET").unwrap();
        assert_eq!(set.calls, 1);
        assert_eq!(set.errors, 0);
    }

    #[test]
    fn test_command_case_insensitive() {
        let reg = MetricsRegistry::new();
        reg.record_command("get", 10, true);
        reg.record_command("Get", 20, true);
        reg.record_command("GET", 30, true);

        let stats = reg.get_command_stats();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].command, "GET");
        assert_eq!(stats[0].calls, 3);
    }

    #[test]
    fn test_latency_percentiles_known_data() {
        let tracker = LatencyTracker::new(100);
        // Insert values 1..=100
        for i in 1..=100 {
            tracker.record(i);
        }

        let p = tracker.percentiles().unwrap();
        // p50 ≈ 50, p90 ≈ 90, p95 ≈ 95, p99 ≈ 99
        assert_eq!(p.p50_us, 50);
        assert_eq!(p.p90_us, 90);
        assert_eq!(p.p95_us, 95);
        assert_eq!(p.p99_us, 99);
        assert_eq!(p.p999_us, 100);
    }

    #[test]
    fn test_latency_percentiles_empty() {
        let tracker = LatencyTracker::new(100);
        assert!(tracker.percentiles().is_none());
    }

    #[test]
    fn test_latency_percentiles_single_value() {
        let tracker = LatencyTracker::new(100);
        tracker.record(42);
        let p = tracker.percentiles().unwrap();
        assert_eq!(p.p50_us, 42);
        assert_eq!(p.p99_us, 42);
    }

    #[test]
    fn test_latency_tracker_capacity() {
        let tracker = LatencyTracker::new(5);
        for i in 0..10 {
            tracker.record(i);
        }
        // Only the last 5 values should be retained: 5,6,7,8,9
        assert_eq!(tracker.len(), 5);
        let p = tracker.percentiles().unwrap();
        assert_eq!(p.p50_us, 7);
    }

    #[test]
    fn test_latency_percentiles_via_registry() {
        let reg = MetricsRegistry::new();
        for i in 1..=100 {
            reg.record_command("PING", i, true);
        }
        let p = reg.get_latency_percentiles("ping").unwrap();
        assert_eq!(p.p50_us, 50);
        assert_eq!(p.p99_us, 99);
    }

    #[test]
    fn test_connection_events() {
        let reg = MetricsRegistry::new();
        reg.record_connection(ConnectionEvent::Opened);
        reg.record_connection(ConnectionEvent::Opened);
        reg.record_connection(ConnectionEvent::Opened);
        reg.record_connection(ConnectionEvent::Closed);
        reg.record_connection(ConnectionEvent::Rejected {
            reason: "max clients".to_string(),
        });
        reg.record_connection(ConnectionEvent::AuthFailed);

        let snap = reg.get_snapshot();
        assert_eq!(snap.connected_clients, 2);
        assert_eq!(snap.total_connections, 3);
        assert_eq!(snap.rejected_connections, 1);
    }

    #[test]
    fn test_connection_close_saturates_at_zero() {
        let reg = MetricsRegistry::new();
        reg.record_connection(ConnectionEvent::Closed);
        let snap = reg.get_snapshot();
        assert_eq!(snap.connected_clients, 0);
    }

    #[test]
    fn test_memory_snapshot() {
        let reg = MetricsRegistry::new();
        let mem = MemorySnapshot {
            total_bytes: 1_073_741_824,
            used_bytes: 536_870_912,
            rss_bytes: 600_000_000,
            fragmentation_ratio: 1.12,
            peak_bytes: 700_000_000,
            dataset_bytes: 500_000_000,
            overhead_bytes: 36_870_912,
            by_type: HashMap::from([("strings".to_string(), 200_000_000)]),
        };
        reg.record_memory(mem.clone());

        let snap = reg.get_snapshot();
        assert_eq!(snap.memory.total_bytes, 1_073_741_824);
        assert_eq!(snap.memory.used_bytes, 536_870_912);
        assert_eq!(snap.memory.rss_bytes, 600_000_000);
        assert!((snap.memory.fragmentation_ratio - 1.12).abs() < f64::EPSILON);
        assert_eq!(snap.memory.by_type.get("strings"), Some(&200_000_000));
    }

    #[test]
    fn test_metrics_snapshot_generation() {
        let reg = MetricsRegistry::new();
        reg.record_command("GET", 100, true);
        reg.record_command("SET", 200, true);
        reg.record_connection(ConnectionEvent::Opened);
        reg.record_key_event(KeyEvent::Set);
        reg.record_key_event(KeyEvent::Get);
        reg.record_replication(1024, 5);

        let snap = reg.get_snapshot();
        assert_eq!(snap.total_commands, 2);
        assert_eq!(snap.total_errors, 0);
        assert_eq!(snap.connected_clients, 1);
        assert_eq!(snap.total_connections, 1);
        assert!(snap.ops_per_sec >= 0.0);
        assert_eq!(snap.keyspace.hits, 1);
        assert_eq!(snap.replication.replication_lag_ms, 5);
        assert_eq!(snap.command_stats.len(), 2);
    }

    #[test]
    fn test_grafana_dashboard_json_validity() {
        let gen = GrafanaDashboardGenerator::new();
        let json_str = gen.generate();
        let parsed: serde_json::Value =
            serde_json::from_str(&json_str).expect("Dashboard JSON should be valid");

        assert_eq!(parsed["title"], "Ferrite Overview");
        assert!(parsed["panels"].is_array());

        let panels = parsed["panels"].as_array().unwrap();
        assert_eq!(panels.len(), 9);

        // Verify expected panel titles exist
        let titles: Vec<&str> = panels
            .iter()
            .map(|p| p["title"].as_str().unwrap())
            .collect();
        assert!(titles.contains(&"Operations Per Second"));
        assert!(titles.contains(&"Latency P99"));
        assert!(titles.contains(&"Memory Usage"));
        assert!(titles.contains(&"Connected Clients"));
        assert!(titles.contains(&"Cache Hit Rate"));
        assert!(titles.contains(&"Tiering Distribution"));
        assert!(titles.contains(&"Replication Lag"));
        assert!(titles.contains(&"Keyspace"));
        assert!(titles.contains(&"Command Distribution"));
    }

    #[test]
    fn test_reset() {
        let reg = MetricsRegistry::new();
        reg.record_command("GET", 100, true);
        reg.record_connection(ConnectionEvent::Opened);
        reg.record_key_event(KeyEvent::Set);
        reg.record_key_event(KeyEvent::Get);
        reg.record_replication(1024, 5);
        reg.record_storage_op(StorageTier::Memory, StorageOp::Read, 10, 256);

        reg.reset();

        let snap = reg.get_snapshot();
        assert_eq!(snap.total_commands, 0);
        assert_eq!(snap.total_errors, 0);
        assert_eq!(snap.connected_clients, 0);
        assert_eq!(snap.total_connections, 0);
        assert_eq!(snap.rejected_connections, 0);
        assert_eq!(snap.keyspace.hits, 0);
        assert_eq!(snap.keyspace.misses, 0);
        assert_eq!(snap.keyspace.evicted_keys, 0);
        assert_eq!(snap.replication.replication_lag_ms, 0);
        assert!(reg.get_command_stats().is_empty());
        assert!(reg.get_latency_percentiles("GET").is_none());
    }

    #[test]
    fn test_key_events_hits_misses_evictions() {
        let reg = MetricsRegistry::new();

        // 3 hits
        reg.record_key_event(KeyEvent::Get);
        reg.record_key_event(KeyEvent::Get);
        reg.record_key_event(KeyEvent::Get);

        // 1 miss
        reg.record_miss();

        // 2 evictions
        reg.record_key_event(KeyEvent::Evict {
            reason: EvictionReason::Lru,
        });
        reg.record_key_event(KeyEvent::Evict {
            reason: EvictionReason::MemoryPressure,
        });

        // 1 expire
        reg.record_key_event(KeyEvent::Expire);

        let snap = reg.get_snapshot();
        assert_eq!(snap.keyspace.hits, 3);
        assert_eq!(snap.keyspace.misses, 1);
        assert_eq!(snap.keyspace.evicted_keys, 2);
        assert_eq!(snap.keyspace.expires, 1);
        // hit_rate = 3 / (3 + 1) = 0.75
        assert!((snap.keyspace.hit_rate - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn test_storage_tier_recording() {
        let reg = MetricsRegistry::new();
        reg.record_storage_op(StorageTier::Memory, StorageOp::Read, 10, 1024);
        reg.record_storage_op(StorageTier::Memory, StorageOp::Write, 20, 2048);
        reg.record_storage_op(StorageTier::Disk, StorageOp::Read, 500, 4096);
        reg.record_storage_op(StorageTier::Disk, StorageOp::Migrate, 1000, 8192);
        reg.record_storage_op(StorageTier::Mmap, StorageOp::Read, 100, 512);
        reg.record_storage_op(StorageTier::Cloud, StorageOp::Write, 5000, 16384);

        let snap = reg.get_snapshot();
        assert_eq!(snap.tiering.memory_bytes, 1024 + 2048);
        assert_eq!(snap.tiering.disk_bytes, 4096 + 8192);
        assert_eq!(snap.tiering.mmap_bytes, 512);
        assert_eq!(snap.tiering.cloud_bytes, 16384);
        assert_eq!(snap.tiering.migrations_total, 1);
    }

    #[test]
    fn test_metrics_error_display() {
        let err = MetricsError::RegistrationFailed("duplicate".to_string());
        assert_eq!(err.to_string(), "Registration failed: duplicate");

        let err = MetricsError::SnapshotFailed("timeout".to_string());
        assert_eq!(err.to_string(), "Snapshot failed: timeout");

        let err = MetricsError::Internal("oops".to_string());
        assert_eq!(err.to_string(), "Internal metrics error: oops");
    }

    #[test]
    fn test_memory_snapshot_default() {
        let mem = MemorySnapshot::default();
        assert_eq!(mem.total_bytes, 0);
        assert_eq!(mem.used_bytes, 0);
        assert!(mem.by_type.is_empty());
    }

    #[test]
    fn test_default_impls() {
        let _reg = MetricsRegistry::default();
        let _gen = GrafanaDashboardGenerator::default();
        let _ks = KeyspaceMetrics::default();
        let _pm = PersistenceMetrics::default();
        let _rm = ReplicationMetrics::default();
        let _tm = TieringMetrics::default();
    }
}
