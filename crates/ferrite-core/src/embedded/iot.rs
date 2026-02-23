//! IoT-Specific Features for Edge/Embedded Deployments
//!
//! Provides optimized primitives for resource-constrained IoT devices:
//!
//! - **Sensor data ingestion**: Time-series–optimized writes for sensor readings
//! - **Edge aggregation**: Aggregate data locally before syncing to cloud
//! - **Edge sync**: Sync local data to a central Ferrite server
//! - **Offline mode**: Queue writes when disconnected, sync when reconnected
//! - **Resource-constrained config**: Memory/key limits with eviction policies
//!
//! # Example
//!
//! ```rust,ignore
//! use ferrite::embedded::iot::*;
//!
//! let config = EmbeddedIoTConfig::default_iot();
//! let ingestion = SensorIngestion::new(SensorConfig::default());
//!
//! // Ingest a sensor reading
//! ingestion.ingest(SensorReading {
//!     sensor_id: "temp-001".into(),
//!     value: 22.5,
//!     unit: "celsius".into(),
//!     timestamp: None,
//!     metadata: Default::default(),
//! });
//!
//! // Get aggregated stats
//! let stats = ingestion.aggregate("temp-001");
//! ```

use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Resource-Constrained Configuration
// ---------------------------------------------------------------------------

/// Configuration for embedded IoT mode with strict resource constraints.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EmbeddedIoTConfig {
    /// Maximum memory budget in bytes (default: 64 MB for IoT).
    pub max_memory: usize,
    /// Optional hard limit on key count.
    pub max_keys: Option<usize>,
    /// How data is persisted.
    pub persistence: PersistenceMode,
    /// Eviction policy when limits are reached.
    pub eviction_policy: EvictionPolicy,
    /// Whether to compress values with LZ4.
    pub compression: bool,
    /// Read-only mode (rejects all writes).
    pub readonly: bool,
}

impl Default for EmbeddedIoTConfig {
    fn default() -> Self {
        Self {
            max_memory: 64 * 1024 * 1024, // 64 MB
            max_keys: None,
            persistence: PersistenceMode::None,
            eviction_policy: EvictionPolicy::Lru,
            compression: false,
            readonly: false,
        }
    }
}

impl EmbeddedIoTConfig {
    /// Preset for a typical IoT gateway (Raspberry Pi-class).
    pub fn default_iot() -> Self {
        Self {
            max_memory: 64 * 1024 * 1024,
            max_keys: Some(100_000),
            persistence: PersistenceMode::Periodic(Duration::from_secs(300)),
            eviction_policy: EvictionPolicy::Lru,
            compression: true,
            readonly: false,
        }
    }

    /// Preset for an extremely constrained micro-controller.
    pub fn micro() -> Self {
        Self {
            max_memory: 2 * 1024 * 1024,
            max_keys: Some(1_000),
            persistence: PersistenceMode::OnShutdown,
            eviction_policy: EvictionPolicy::Lru,
            compression: true,
            readonly: false,
        }
    }

    /// Preset for a mobile/tablet companion cache.
    pub fn mobile() -> Self {
        Self {
            max_memory: 32 * 1024 * 1024,
            max_keys: Some(50_000),
            persistence: PersistenceMode::Periodic(Duration::from_secs(60)),
            eviction_policy: EvictionPolicy::Lfu,
            compression: true,
            readonly: false,
        }
    }
}

/// How data is persisted to stable storage.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PersistenceMode {
    /// Pure in-memory – no persistence.
    None,
    /// Persist only on graceful shutdown.
    OnShutdown,
    /// Periodic snapshots at the given interval.
    Periodic(Duration),
    /// Write-ahead log for crash recovery.
    WAL,
}

/// Eviction policy used when memory or key limits are reached.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvictionPolicy {
    /// Least Recently Used.
    Lru,
    /// Least Frequently Used.
    Lfu,
    /// Remove the oldest keys first.
    OldestFirst,
    /// Random eviction.
    Random,
    /// Never evict – return an error instead.
    NoEviction,
}

// ---------------------------------------------------------------------------
// Sensor Data Ingestion
// ---------------------------------------------------------------------------

/// Configuration for the sensor ingestion pipeline.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SensorConfig {
    /// Maximum readings to keep per sensor in the sliding window.
    pub max_readings_per_sensor: usize,
    /// Automatically compute running aggregates.
    pub auto_aggregate: bool,
    /// Compress stored readings.
    pub compress_readings: bool,
    /// Default TTL for sensor readings (0 = no expiry).
    pub default_ttl_secs: u64,
}

impl Default for SensorConfig {
    fn default() -> Self {
        Self {
            max_readings_per_sensor: 1000,
            auto_aggregate: true,
            compress_readings: false,
            default_ttl_secs: 0,
        }
    }
}

/// A single sensor reading.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SensorReading {
    /// Unique sensor identifier.
    pub sensor_id: String,
    /// Numeric value of the reading.
    pub value: f64,
    /// Unit of measurement (e.g. `"celsius"`, `"hPa"`).
    pub unit: String,
    /// Timestamp in microseconds since epoch (auto-filled if `None`).
    pub timestamp: Option<u64>,
    /// Arbitrary key-value metadata.
    pub metadata: HashMap<String, String>,
}

/// Running aggregate for a single sensor.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SensorAggregate {
    /// Number of readings ingested.
    pub count: u64,
    /// Sum of all values.
    pub sum: f64,
    /// Minimum value observed.
    pub min: f64,
    /// Maximum value observed.
    pub max: f64,
    /// Timestamp of the latest reading.
    pub last_timestamp: u64,
    /// Latest value.
    pub last_value: f64,
}

impl SensorAggregate {
    fn update(&mut self, value: f64, timestamp: u64) {
        if self.count == 0 {
            self.min = value;
            self.max = value;
        } else {
            if value < self.min {
                self.min = value;
            }
            if value > self.max {
                self.max = value;
            }
        }
        self.count += 1;
        self.sum += value;
        self.last_timestamp = timestamp;
        self.last_value = value;
    }

    /// Average value.
    pub fn avg(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }
}

/// Optimised time-series sensor ingestion engine.
pub struct SensorIngestion {
    config: SensorConfig,
    /// Per-sensor sliding window of recent readings.
    readings: RwLock<HashMap<String, VecDeque<SensorReading>>>,
    /// Per-sensor running aggregates.
    aggregates: RwLock<HashMap<String, SensorAggregate>>,
    /// Total readings ingested.
    total_ingested: AtomicU64,
}

impl SensorIngestion {
    /// Create a new ingestion pipeline.
    pub fn new(config: SensorConfig) -> Self {
        Self {
            config,
            readings: RwLock::new(HashMap::new()),
            aggregates: RwLock::new(HashMap::new()),
            total_ingested: AtomicU64::new(0),
        }
    }

    /// Ingest a sensor reading.
    pub fn ingest(&self, mut reading: SensorReading) {
        // Auto-fill timestamp if missing.
        if reading.timestamp.is_none() {
            reading.timestamp = Some(now_micros());
        }
        let ts = reading.timestamp.unwrap_or_default();

        // Update aggregates.
        if self.config.auto_aggregate {
            let mut aggs = self.aggregates.write();
            aggs.entry(reading.sensor_id.clone())
                .or_default()
                .update(reading.value, ts);
        }

        // Append to sliding window.
        let mut readings = self.readings.write();
        let window = readings.entry(reading.sensor_id.clone()).or_default();
        window.push_back(reading);
        // Trim window
        while window.len() > self.config.max_readings_per_sensor {
            window.pop_front();
        }

        self.total_ingested.fetch_add(1, Ordering::Relaxed);
    }

    /// Ingest multiple readings in a batch (optimised single lock acquisition).
    pub fn ingest_batch(&self, readings: Vec<SensorReading>) {
        let mut store = self.readings.write();
        let mut aggs = self.aggregates.write();
        for mut reading in readings {
            if reading.timestamp.is_none() {
                reading.timestamp = Some(now_micros());
            }
            let ts = reading.timestamp.unwrap_or_default();

            if self.config.auto_aggregate {
                aggs.entry(reading.sensor_id.clone())
                    .or_default()
                    .update(reading.value, ts);
            }

            let window = store.entry(reading.sensor_id.clone()).or_default();
            window.push_back(reading);
            while window.len() > self.config.max_readings_per_sensor {
                window.pop_front();
            }

            self.total_ingested.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get the running aggregate for a sensor.
    pub fn aggregate(&self, sensor_id: &str) -> Option<SensorAggregate> {
        self.aggregates.read().get(sensor_id).cloned()
    }

    /// Get the latest N readings for a sensor.
    pub fn latest(&self, sensor_id: &str, n: usize) -> Vec<SensorReading> {
        let readings = self.readings.read();
        readings
            .get(sensor_id)
            .map(|w| w.iter().rev().take(n).cloned().collect())
            .unwrap_or_default()
    }

    /// List all known sensor IDs.
    pub fn sensor_ids(&self) -> Vec<String> {
        self.readings.read().keys().cloned().collect()
    }

    /// Total number of readings ingested.
    pub fn total_ingested(&self) -> u64 {
        self.total_ingested.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Edge Aggregation
// ---------------------------------------------------------------------------

/// Pre-aggregated bucket ready for cloud upload.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggregationBucket {
    /// Sensor ID.
    pub sensor_id: String,
    /// Bucket start timestamp (micros).
    pub start_time: u64,
    /// Bucket end timestamp (micros).
    pub end_time: u64,
    /// Aggregate statistics for the bucket.
    pub stats: SensorAggregate,
}

/// Aggregates sensor data into time-bucketed summaries before syncing to cloud.
pub struct EdgeAggregator {
    /// Bucket duration.
    bucket_duration: Duration,
    /// Completed buckets waiting for upload.
    pending_buckets: RwLock<VecDeque<AggregationBucket>>,
    /// Maximum pending buckets to retain.
    max_pending: usize,
    /// Current open buckets (sensor_id -> in-progress bucket).
    current_buckets: RwLock<HashMap<String, AggregationBucket>>,
}

impl EdgeAggregator {
    /// Create a new aggregator with the given bucket duration.
    pub fn new(bucket_duration: Duration, max_pending: usize) -> Self {
        Self {
            bucket_duration,
            pending_buckets: RwLock::new(VecDeque::new()),
            max_pending,
            current_buckets: RwLock::new(HashMap::new()),
        }
    }

    /// Feed a reading into the aggregator.
    pub fn add_reading(&self, sensor_id: &str, value: f64, timestamp: u64) {
        let mut current = self.current_buckets.write();
        let bucket = current
            .entry(sensor_id.to_string())
            .or_insert_with(|| AggregationBucket {
                sensor_id: sensor_id.to_string(),
                start_time: timestamp,
                end_time: timestamp + self.bucket_duration.as_micros() as u64,
                stats: SensorAggregate::default(),
            });

        // Rotate bucket if the reading falls outside the current window.
        if timestamp >= bucket.end_time {
            let completed = bucket.clone();
            // Start new bucket
            *bucket = AggregationBucket {
                sensor_id: sensor_id.to_string(),
                start_time: timestamp,
                end_time: timestamp + self.bucket_duration.as_micros() as u64,
                stats: SensorAggregate::default(),
            };
            // Push completed bucket
            let mut pending = self.pending_buckets.write();
            pending.push_back(completed);
            while pending.len() > self.max_pending {
                pending.pop_front();
            }
        }

        bucket.stats.update(value, timestamp);
    }

    /// Drain all completed buckets ready for upload.
    pub fn drain_pending(&self) -> Vec<AggregationBucket> {
        let mut pending = self.pending_buckets.write();
        pending.drain(..).collect()
    }

    /// Flush all current (open) buckets into the pending queue.
    pub fn flush(&self) {
        let mut current = self.current_buckets.write();
        let mut pending = self.pending_buckets.write();
        for (_, bucket) in current.drain() {
            if bucket.stats.count > 0 {
                pending.push_back(bucket);
            }
        }
        while pending.len() > self.max_pending {
            pending.pop_front();
        }
    }

    /// Number of pending buckets waiting for upload.
    pub fn pending_count(&self) -> usize {
        self.pending_buckets.read().len()
    }
}

// ---------------------------------------------------------------------------
// Edge Sync
// ---------------------------------------------------------------------------

/// Configuration for edge-to-cloud synchronisation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgeSyncConfig {
    /// Remote Ferrite server hostname or IP.
    pub remote_host: String,
    /// Remote Ferrite server port.
    pub remote_port: u16,
    /// How often to sync (background interval).
    pub sync_interval: Duration,
    /// Maximum number of changes per sync batch.
    pub batch_size: usize,
    /// How to resolve conflicts between edge and cloud.
    pub conflict_resolution: ConflictStrategy,
    /// Enable compression for sync payloads.
    pub compress: bool,
    /// Retry count on failure.
    pub max_retries: u32,
}

impl Default for EdgeSyncConfig {
    fn default() -> Self {
        Self {
            remote_host: "localhost".to_string(),
            remote_port: 6380,
            sync_interval: Duration::from_secs(30),
            batch_size: 1000,
            conflict_resolution: ConflictStrategy::LastWriteWins,
            compress: true,
            max_retries: 3,
        }
    }
}

/// Strategy for resolving conflicts between local and remote data.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ConflictStrategy {
    /// The most recent write wins (by timestamp).
    LastWriteWins,
    /// Remote/cloud always wins.
    RemoteWins,
    /// Local/edge always wins.
    LocalWins,
    /// Custom resolver (name of a registered resolver function).
    Custom(String),
}

/// Manages synchronisation state between edge and cloud.
pub struct EdgeSyncManager {
    config: EdgeSyncConfig,
    /// Whether a sync is currently in progress.
    syncing: AtomicBool,
    /// Number of successful syncs.
    sync_count: AtomicU64,
    /// Number of failed syncs.
    sync_failures: AtomicU64,
    /// Offline operation queue.
    offline_queue: RwLock<VecDeque<OfflineOperation>>,
    /// Maximum offline queue size.
    max_queue_size: usize,
}

/// An operation queued while the device is offline.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OfflineOperation {
    /// The key affected.
    pub key: String,
    /// Type of operation.
    pub op_type: OfflineOpType,
    /// Value payload (for set-like operations).
    pub value: Option<Bytes>,
    /// Timestamp when the operation was queued.
    pub queued_at: u64,
}

/// Type of queued offline operation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OfflineOpType {
    /// Set a key.
    Set,
    /// Delete a key.
    Delete,
    /// Increment a counter.
    Increment(i64),
    /// Push to a list.
    ListPush,
    /// Add to a set.
    SetAdd,
}

impl EdgeSyncManager {
    /// Create a new sync manager.
    pub fn new(config: EdgeSyncConfig) -> Self {
        Self {
            config,
            syncing: AtomicBool::new(false),
            sync_count: AtomicU64::new(0),
            sync_failures: AtomicU64::new(0),
            offline_queue: RwLock::new(VecDeque::new()),
            max_queue_size: 10_000,
        }
    }

    /// Create with a custom queue size.
    pub fn with_queue_size(mut self, size: usize) -> Self {
        self.max_queue_size = size;
        self
    }

    /// Queue an operation for later sync.
    pub fn queue_operation(&self, op: OfflineOperation) -> Result<(), IoTError> {
        let mut q = self.offline_queue.write();
        if q.len() >= self.max_queue_size {
            return Err(IoTError::QueueFull {
                size: q.len(),
                max: self.max_queue_size,
            });
        }
        q.push_back(op);
        Ok(())
    }

    /// Drain up to `batch_size` operations from the offline queue.
    pub fn drain_batch(&self) -> Vec<OfflineOperation> {
        let mut q = self.offline_queue.write();
        let n = self.config.batch_size.min(q.len());
        q.drain(..n).collect()
    }

    /// Number of operations waiting in the offline queue.
    pub fn queue_len(&self) -> usize {
        self.offline_queue.read().len()
    }

    /// Whether a sync is currently in progress.
    pub fn is_syncing(&self) -> bool {
        self.syncing.load(Ordering::Relaxed)
    }

    /// Mark sync as started. Returns `false` if already syncing.
    pub fn start_sync(&self) -> bool {
        self.syncing
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
    }

    /// Mark sync as finished.
    pub fn finish_sync(&self, success: bool) {
        if success {
            self.sync_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.sync_failures.fetch_add(1, Ordering::Relaxed);
        }
        self.syncing.store(false, Ordering::SeqCst);
    }

    /// Get sync statistics.
    pub fn stats(&self) -> EdgeSyncStats {
        EdgeSyncStats {
            sync_count: self.sync_count.load(Ordering::Relaxed),
            sync_failures: self.sync_failures.load(Ordering::Relaxed),
            queue_len: self.queue_len(),
            is_syncing: self.is_syncing(),
        }
    }

    /// Reference to the configuration.
    pub fn config(&self) -> &EdgeSyncConfig {
        &self.config
    }
}

/// Statistics for edge sync operations.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EdgeSyncStats {
    /// Total successful sync operations.
    pub sync_count: u64,
    /// Total failed sync operations.
    pub sync_failures: u64,
    /// Current offline queue length.
    pub queue_len: usize,
    /// Whether a sync is currently in progress.
    pub is_syncing: bool,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors specific to IoT / edge operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum IoTError {
    /// The offline queue is full.
    #[error("offline queue full: {size}/{max}")]
    QueueFull {
        /// Current queue size.
        size: usize,
        /// Maximum queue capacity.
        max: usize,
    },

    /// Sync is already in progress.
    #[error("sync already in progress")]
    SyncInProgress,

    /// Connection to remote server failed.
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// Read-only mode rejects writes.
    #[error("database is in read-only mode")]
    ReadOnly,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- EmbeddedIoTConfig tests --

    #[test]
    fn test_iot_config_defaults() {
        let cfg = EmbeddedIoTConfig::default();
        assert_eq!(cfg.max_memory, 64 * 1024 * 1024);
        assert!(cfg.max_keys.is_none());
        assert!(!cfg.readonly);
    }

    #[test]
    fn test_iot_config_presets() {
        let iot = EmbeddedIoTConfig::default_iot();
        assert_eq!(iot.max_keys, Some(100_000));
        assert!(iot.compression);

        let micro = EmbeddedIoTConfig::micro();
        assert_eq!(micro.max_memory, 2 * 1024 * 1024);
        assert_eq!(micro.max_keys, Some(1_000));

        let mobile = EmbeddedIoTConfig::mobile();
        assert_eq!(mobile.max_memory, 32 * 1024 * 1024);
        assert!(matches!(mobile.eviction_policy, EvictionPolicy::Lfu));
    }

    #[test]
    fn test_persistence_mode_variants() {
        let _none = PersistenceMode::None;
        let _shutdown = PersistenceMode::OnShutdown;
        let _periodic = PersistenceMode::Periodic(Duration::from_secs(60));
        let _wal = PersistenceMode::WAL;
    }

    // -- SensorIngestion tests --

    #[test]
    fn test_sensor_ingestion_basic() {
        let ingestion = SensorIngestion::new(SensorConfig::default());

        ingestion.ingest(SensorReading {
            sensor_id: "temp-001".to_string(),
            value: 22.5,
            unit: "celsius".to_string(),
            timestamp: Some(1_000_000),
            metadata: HashMap::new(),
        });

        assert_eq!(ingestion.total_ingested(), 1);
        let agg = ingestion.aggregate("temp-001").unwrap();
        assert_eq!(agg.count, 1);
        assert_eq!(agg.last_value, 22.5);
    }

    #[test]
    fn test_sensor_ingestion_aggregate() {
        let ingestion = SensorIngestion::new(SensorConfig::default());

        for i in 0..10 {
            ingestion.ingest(SensorReading {
                sensor_id: "temp".to_string(),
                value: 20.0 + i as f64,
                unit: "C".to_string(),
                timestamp: Some(i * 1000),
                metadata: HashMap::new(),
            });
        }

        let agg = ingestion.aggregate("temp").unwrap();
        assert_eq!(agg.count, 10);
        assert_eq!(agg.min, 20.0);
        assert_eq!(agg.max, 29.0);
        assert!((agg.avg() - 24.5).abs() < 0.001);
    }

    #[test]
    fn test_sensor_ingestion_sliding_window() {
        let config = SensorConfig {
            max_readings_per_sensor: 5,
            ..Default::default()
        };
        let ingestion = SensorIngestion::new(config);

        for i in 0..10 {
            ingestion.ingest(SensorReading {
                sensor_id: "s1".to_string(),
                value: i as f64,
                unit: "u".to_string(),
                timestamp: Some(i * 1000),
                metadata: HashMap::new(),
            });
        }

        let latest = ingestion.latest("s1", 10);
        // Window is capped at 5
        assert_eq!(latest.len(), 5);
        // Latest readings (reversed from deque)
        assert_eq!(latest[0].value, 9.0);
    }

    #[test]
    fn test_sensor_ingestion_batch() {
        let ingestion = SensorIngestion::new(SensorConfig::default());

        let batch: Vec<SensorReading> = (0..100)
            .map(|i| SensorReading {
                sensor_id: format!("sensor-{}", i % 5),
                value: i as f64,
                unit: "u".to_string(),
                timestamp: Some(i * 1000),
                metadata: HashMap::new(),
            })
            .collect();

        ingestion.ingest_batch(batch);
        assert_eq!(ingestion.total_ingested(), 100);
        assert_eq!(ingestion.sensor_ids().len(), 5);
    }

    // -- EdgeAggregator tests --

    #[test]
    fn test_edge_aggregator_basic() {
        let aggregator = EdgeAggregator::new(Duration::from_secs(60), 100);

        let base = 1_000_000_u64;
        for i in 0..5 {
            aggregator.add_reading("temp", 20.0 + i as f64, base + i * 1_000_000);
        }

        // No bucket rotation yet, so pending should be 0
        assert_eq!(aggregator.pending_count(), 0);

        // Flush to move open buckets to pending
        aggregator.flush();
        assert_eq!(aggregator.pending_count(), 1);

        let buckets = aggregator.drain_pending();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].stats.count, 5);
        assert_eq!(buckets[0].stats.min, 20.0);
    }

    #[test]
    fn test_edge_aggregator_rotation() {
        let aggregator = EdgeAggregator::new(Duration::from_secs(1), 100);

        let base = 1_000_000_u64; // 1 second in micros
        aggregator.add_reading("s1", 10.0, base);
        // Jump forward past bucket boundary
        aggregator.add_reading("s1", 20.0, base + 2_000_000);

        // First bucket should have been rotated to pending
        assert_eq!(aggregator.pending_count(), 1);
    }

    // -- EdgeSyncManager tests --

    #[test]
    fn test_edge_sync_manager_queue() {
        let mgr = EdgeSyncManager::new(EdgeSyncConfig::default());

        mgr.queue_operation(OfflineOperation {
            key: "key1".to_string(),
            op_type: OfflineOpType::Set,
            value: Some(Bytes::from("value1")),
            queued_at: now_micros(),
        })
        .unwrap();

        assert_eq!(mgr.queue_len(), 1);

        let batch = mgr.drain_batch();
        assert_eq!(batch.len(), 1);
        assert_eq!(mgr.queue_len(), 0);
    }

    #[test]
    fn test_edge_sync_manager_queue_full() {
        let mgr = EdgeSyncManager::new(EdgeSyncConfig::default()).with_queue_size(2);

        for i in 0..2 {
            mgr.queue_operation(OfflineOperation {
                key: format!("key{}", i),
                op_type: OfflineOpType::Set,
                value: None,
                queued_at: now_micros(),
            })
            .unwrap();
        }

        let result = mgr.queue_operation(OfflineOperation {
            key: "overflow".to_string(),
            op_type: OfflineOpType::Set,
            value: None,
            queued_at: now_micros(),
        });
        assert!(matches!(result, Err(IoTError::QueueFull { .. })));
    }

    #[test]
    fn test_edge_sync_manager_sync_lifecycle() {
        let mgr = EdgeSyncManager::new(EdgeSyncConfig::default());

        assert!(!mgr.is_syncing());
        assert!(mgr.start_sync());
        assert!(mgr.is_syncing());

        // Cannot start another sync while one is running
        assert!(!mgr.start_sync());

        mgr.finish_sync(true);
        assert!(!mgr.is_syncing());

        let stats = mgr.stats();
        assert_eq!(stats.sync_count, 1);
        assert_eq!(stats.sync_failures, 0);
    }

    #[test]
    fn test_edge_sync_manager_failure_tracking() {
        let mgr = EdgeSyncManager::new(EdgeSyncConfig::default());

        mgr.start_sync();
        mgr.finish_sync(false);

        mgr.start_sync();
        mgr.finish_sync(true);

        let stats = mgr.stats();
        assert_eq!(stats.sync_count, 1);
        assert_eq!(stats.sync_failures, 1);
    }

    // -- EvictionPolicy tests --

    #[test]
    fn test_eviction_policy_equality() {
        assert_eq!(EvictionPolicy::Lru, EvictionPolicy::Lru);
        assert_ne!(EvictionPolicy::Lru, EvictionPolicy::Lfu);
    }
}
