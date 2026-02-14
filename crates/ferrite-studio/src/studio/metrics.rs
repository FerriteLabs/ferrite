//! Real-time metrics for Ferrite Studio
//!
//! Provides a `MetricsSnapshot` that captures all key server metrics at a point
//! in time, plus an SSE-compatible stream for live dashboard updates.

use std::collections::HashMap;
use std::time::Instant;

use serde::{Deserialize, Serialize};

/// A point-in-time snapshot of server metrics for the real-time dashboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    /// Operations per second (computed over the last interval).
    pub ops_per_sec: f64,
    /// Total memory used by the engine (bytes).
    pub memory_used: u64,
    /// Total system memory available (bytes).
    pub memory_total: u64,
    /// Memory fragmentation ratio.
    pub fragmentation_ratio: f64,
    /// Number of currently connected clients.
    pub connected_clients: u32,
    /// Total number of keys across all databases.
    pub total_keys: u64,
    /// Cache hit rate (0.0–1.0).
    pub hit_rate: f64,
    /// Median latency in microseconds.
    pub latency_p50: f64,
    /// 99th-percentile latency in microseconds.
    pub latency_p99: f64,
    /// 99.9th-percentile latency in microseconds.
    pub latency_p999: f64,
    /// Server uptime in seconds.
    pub uptime_seconds: u64,
    /// Server version.
    pub version: String,
    /// Command execution counts by command name.
    pub commands: HashMap<String, u64>,
    /// Key count per database index.
    pub keys_per_db: HashMap<String, u64>,
    /// Timestamp of this snapshot (epoch millis).
    pub timestamp_ms: u64,
}

impl MetricsSnapshot {
    /// Create a placeholder snapshot with example data.
    pub fn placeholder() -> Self {
        let mut commands = HashMap::new();
        commands.insert("GET".to_string(), 45_000);
        commands.insert("SET".to_string(), 30_000);
        commands.insert("DEL".to_string(), 5_000);
        commands.insert("HGET".to_string(), 10_000);
        commands.insert("LPUSH".to_string(), 8_000);
        commands.insert("ZADD".to_string(), 2_000);

        let mut keys_per_db = HashMap::new();
        keys_per_db.insert("db0".to_string(), 850);
        keys_per_db.insert("db1".to_string(), 150);

        Self {
            ops_per_sec: 5000.0,
            memory_used: 100 * 1024 * 1024,
            memory_total: 16u64 * 1024 * 1024 * 1024,
            fragmentation_ratio: 1.1,
            connected_clients: 10,
            total_keys: 1000,
            hit_rate: 0.92,
            latency_p50: 45.0,
            latency_p99: 250.0,
            latency_p999: 1200.0,
            uptime_seconds: 3600,
            version: env!("CARGO_PKG_VERSION").to_string(),
            commands,
            keys_per_db,
            timestamp_ms: epoch_ms(),
        }
    }

    /// Serialize this snapshot as an SSE `data:` event line.
    pub fn to_sse_event(&self) -> String {
        let json = serde_json::to_string(self).unwrap_or_default();
        format!("event: metrics\ndata: {}\n\n", json)
    }
}

/// Collector that tracks metrics over time for the dashboard.
pub struct MetricsCollector {
    /// Total commands since start.
    total_commands: std::sync::atomic::AtomicU64,
    /// Cache hits.
    cache_hits: std::sync::atomic::AtomicU64,
    /// Cache misses.
    cache_misses: std::sync::atomic::AtomicU64,
    /// Connected clients.
    connected_clients: std::sync::atomic::AtomicU32,
    /// Total keys.
    total_keys: std::sync::atomic::AtomicU64,
    /// Memory used bytes.
    memory_used: std::sync::atomic::AtomicU64,
    /// Memory total bytes.
    memory_total: std::sync::atomic::AtomicU64,
    /// Command counts by name.
    command_counts: parking_lot::RwLock<HashMap<String, u64>>,
    /// Keys per database.
    keys_per_db: parking_lot::RwLock<HashMap<String, u64>>,
    /// Latency samples (microseconds).
    latency_samples: parking_lot::RwLock<Vec<f64>>,
    /// Start time.
    started_at: Instant,
    /// Snapshot for ops/sec computation.
    last_ops_snapshot: parking_lot::RwLock<(Instant, u64)>,
}

impl MetricsCollector {
    /// Create a new metrics collector.
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            total_commands: 0.into(),
            cache_hits: 0.into(),
            cache_misses: 0.into(),
            connected_clients: 0.into(),
            total_keys: 0.into(),
            memory_used: (100 * 1024 * 1024_u64).into(),
            memory_total: (16u64 * 1024 * 1024 * 1024).into(),
            command_counts: parking_lot::RwLock::new(HashMap::new()),
            keys_per_db: parking_lot::RwLock::new(HashMap::new()),
            latency_samples: parking_lot::RwLock::new(Vec::new()),
            started_at: now,
            last_ops_snapshot: parking_lot::RwLock::new((now, 0)),
        }
    }

    /// Record a command execution.
    pub fn record_command(&self, command: &str, latency_us: f64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.total_commands.fetch_add(1, Relaxed);
        {
            let mut counts = self.command_counts.write();
            *counts.entry(command.to_uppercase()).or_insert(0) += 1;
        }
        {
            let mut samples = self.latency_samples.write();
            samples.push(latency_us);
            // Keep only the last 10 000 samples.
            if samples.len() > 10_000 {
                let drain = samples.len() - 10_000;
                samples.drain(..drain);
            }
        }
    }

    /// Record a cache hit.
    pub fn record_hit(&self) {
        self.cache_hits
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Record a cache miss.
    pub fn record_miss(&self) {
        self.cache_misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Update connected client count.
    pub fn set_connected_clients(&self, count: u32) {
        self.connected_clients
            .store(count, std::sync::atomic::Ordering::Relaxed);
    }

    /// Update total key count.
    pub fn set_total_keys(&self, count: u64) {
        self.total_keys
            .store(count, std::sync::atomic::Ordering::Relaxed);
    }

    /// Update keys per database.
    pub fn set_keys_per_db(&self, db: &str, count: u64) {
        self.keys_per_db.write().insert(db.to_string(), count);
    }

    /// Take a snapshot of all current metrics.
    pub fn snapshot(&self) -> MetricsSnapshot {
        use std::sync::atomic::Ordering::Relaxed;

        let total_cmds = self.total_commands.load(Relaxed);
        let hits = self.cache_hits.load(Relaxed);
        let misses = self.cache_misses.load(Relaxed);

        // Compute ops/sec.
        let ops_per_sec = {
            let mut last = self.last_ops_snapshot.write();
            let elapsed = last.0.elapsed().as_secs_f64();
            let delta = total_cmds.saturating_sub(last.1);
            let ops = if elapsed > 0.0 {
                delta as f64 / elapsed
            } else {
                0.0
            };
            *last = (Instant::now(), total_cmds);
            ops
        };

        let hit_rate = {
            let total = hits + misses;
            if total > 0 {
                hits as f64 / total as f64
            } else {
                0.0
            }
        };

        let (p50, p99, p999) = self.compute_percentiles();

        MetricsSnapshot {
            ops_per_sec,
            memory_used: self.memory_used.load(Relaxed),
            memory_total: self.memory_total.load(Relaxed),
            fragmentation_ratio: 1.0,
            connected_clients: self.connected_clients.load(Relaxed),
            total_keys: self.total_keys.load(Relaxed),
            hit_rate,
            latency_p50: p50,
            latency_p99: p99,
            latency_p999: p999,
            uptime_seconds: self.started_at.elapsed().as_secs(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            commands: self.command_counts.read().clone(),
            keys_per_db: self.keys_per_db.read().clone(),
            timestamp_ms: epoch_ms(),
        }
    }

    fn compute_percentiles(&self) -> (f64, f64, f64) {
        let samples = self.latency_samples.read();
        if samples.is_empty() {
            return (0.0, 0.0, 0.0);
        }
        let mut sorted = samples.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let len = sorted.len();
        let p50 = sorted[(len as f64 * 0.50) as usize];
        let p99 = sorted[((len as f64 * 0.99) as usize).min(len - 1)];
        let p999 = sorted[((len as f64 * 0.999) as usize).min(len - 1)];
        (p50, p99, p999)
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_placeholder_snapshot() {
        let snap = MetricsSnapshot::placeholder();
        assert!(snap.ops_per_sec > 0.0);
        assert!(snap.memory_used > 0);
        assert!(snap.connected_clients > 0);
        assert!(snap.total_keys > 0);
        assert!(!snap.commands.is_empty());
        assert!(!snap.keys_per_db.is_empty());
        assert!(!snap.version.is_empty());
    }

    #[test]
    fn test_snapshot_sse_event() {
        let snap = MetricsSnapshot::placeholder();
        let sse = snap.to_sse_event();
        assert!(sse.starts_with("event: metrics\ndata: "));
        assert!(sse.ends_with("\n\n"));
        assert!(sse.contains("ops_per_sec"));
    }

    #[test]
    fn test_snapshot_serialization() {
        let snap = MetricsSnapshot::placeholder();
        let json = serde_json::to_string(&snap).unwrap();
        let deserialized: MetricsSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.connected_clients, snap.connected_clients);
        assert_eq!(deserialized.total_keys, snap.total_keys);
    }

    #[test]
    fn test_collector_record_command() {
        let collector = MetricsCollector::new();
        collector.record_command("GET", 50.0);
        collector.record_command("GET", 100.0);
        collector.record_command("SET", 200.0);

        let snap = collector.snapshot();
        assert_eq!(snap.commands.get("GET"), Some(&2));
        assert_eq!(snap.commands.get("SET"), Some(&1));
    }

    #[test]
    fn test_collector_hit_rate() {
        let collector = MetricsCollector::new();
        for _ in 0..90 {
            collector.record_hit();
        }
        for _ in 0..10 {
            collector.record_miss();
        }
        let snap = collector.snapshot();
        assert!((snap.hit_rate - 0.9).abs() < 0.01);
    }

    #[test]
    fn test_collector_percentiles() {
        let collector = MetricsCollector::new();
        for i in 1..=100 {
            collector.record_command("GET", i as f64);
        }
        let snap = collector.snapshot();
        assert!(snap.latency_p50 > 0.0);
        assert!(snap.latency_p99 >= snap.latency_p50);
        assert!(snap.latency_p999 >= snap.latency_p99);
    }

    #[test]
    fn test_collector_connected_clients() {
        let collector = MetricsCollector::new();
        collector.set_connected_clients(42);
        let snap = collector.snapshot();
        assert_eq!(snap.connected_clients, 42);
    }

    #[test]
    fn test_collector_keys_per_db() {
        let collector = MetricsCollector::new();
        collector.set_keys_per_db("db0", 500);
        collector.set_keys_per_db("db1", 100);
        collector.set_total_keys(600);

        let snap = collector.snapshot();
        assert_eq!(snap.total_keys, 600);
        assert_eq!(snap.keys_per_db.get("db0"), Some(&500));
        assert_eq!(snap.keys_per_db.get("db1"), Some(&100));
    }

    #[test]
    fn test_collector_empty_percentiles() {
        let collector = MetricsCollector::new();
        let snap = collector.snapshot();
        assert_eq!(snap.latency_p50, 0.0);
        assert_eq!(snap.latency_p99, 0.0);
        assert_eq!(snap.latency_p999, 0.0);
    }

    #[test]
    fn test_collector_default() {
        let collector = MetricsCollector::default();
        let snap = collector.snapshot();
        assert!(snap.memory_total > 0);
    }

    #[test]
    fn test_collector_uptime() {
        let collector = MetricsCollector::new();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let snap = collector.snapshot();
        // Uptime is at least 0 (could be 0 if rounded to seconds).
        assert!(snap.uptime_seconds < 10);
    }

    #[test]
    fn test_collector_latency_window() {
        let collector = MetricsCollector::new();
        // Overflow the sample window.
        for i in 0..15_000 {
            collector.record_command("GET", i as f64);
        }
        let samples = collector.latency_samples.read();
        assert!(samples.len() <= 10_000);
    }
}
