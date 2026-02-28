#![forbid(unsafe_code)]
//! Workload profiler — tracks command frequencies, access patterns, and rolling statistics.

use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use super::tiering::{TierMove, TierRecommendation, TierThresholds, TuningReport, WorkloadReport};

/// Maximum number of entries in the ring buffer for ops/sec calculation.
const RING_BUFFER_SIZE: usize = 300;
/// Maximum number of hot keys to track.
const MAX_HOT_KEYS: usize = 1000;

/// A point-in-time snapshot of workload metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadSnapshot {
    /// Command frequencies by type (e.g., "GET" → 1500).
    pub command_frequencies: Vec<(String, u64)>,
    /// Operations per second (rolling average).
    pub ops_per_sec: f64,
    /// Read/write ratio (reads / total).
    pub read_write_ratio: f64,
    /// Total read count in the snapshot window.
    pub total_reads: u64,
    /// Total write count in the snapshot window.
    pub total_writes: u64,
    /// Hot keys — keys with the highest access frequency.
    pub hot_keys: Vec<(String, u64)>,
    /// Average value size in bytes (estimated).
    pub avg_value_size: f64,
    /// Memory usage fraction (0.0 – 1.0).
    pub memory_usage_fraction: f64,
    /// Number of unique keys accessed.
    pub unique_keys_accessed: u64,
    /// Timestamp (millis since profiler start).
    pub timestamp_ms: u64,
}

impl Default for WorkloadSnapshot {
    fn default() -> Self {
        Self {
            command_frequencies: Vec::new(),
            ops_per_sec: 0.0,
            read_write_ratio: 0.5,
            total_reads: 0,
            total_writes: 0,
            hot_keys: Vec::new(),
            avg_value_size: 0.0,
            memory_usage_fraction: 0.0,
            unique_keys_accessed: 0,
            timestamp_ms: 0,
        }
    }
}

/// Tracks per-key access statistics.
#[derive(Debug)]
struct KeyStats {
    reads: AtomicU64,
    writes: AtomicU64,
    last_access_ms: AtomicU64,
    total_value_bytes: AtomicU64,
    access_count: AtomicU64,
}

impl KeyStats {
    fn new() -> Self {
        Self {
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            last_access_ms: AtomicU64::new(0),
            total_value_bytes: AtomicU64::new(0),
            access_count: AtomicU64::new(0),
        }
    }
}

/// Ring buffer entry for ops/sec tracking.
#[derive(Debug, Clone, Copy)]
struct OpsEntry {
    count: u64,
    timestamp_ms: u64,
}

/// Whether a command is classified as a read or write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandKind {
    /// Read operation.
    Read,
    /// Write operation.
    Write,
}

/// Workload profiler that tracks command frequencies and access patterns.
#[derive(Debug)]
pub struct WorkloadProfiler {
    /// Command frequencies by name.
    command_counts: DashMap<String, AtomicU64>,
    /// Per-key access stats.
    key_stats: DashMap<String, Arc<KeyStats>>,
    /// Ring buffer for ops/sec.
    ops_ring: RwLock<VecDeque<OpsEntry>>,
    /// Global read counter.
    total_reads: AtomicU64,
    /// Global write counter.
    total_writes: AtomicU64,
    /// Total value bytes observed (for average).
    total_value_bytes: AtomicU64,
    /// Total value observations count.
    value_observations: AtomicU64,
    /// Memory usage fraction set externally.
    memory_usage: RwLock<f64>,
    /// Start instant for timestamp calculations.
    start: Instant,
}

impl Default for WorkloadProfiler {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkloadProfiler {
    /// Create a new profiler.
    pub fn new() -> Self {
        Self {
            command_counts: DashMap::new(),
            key_stats: DashMap::new(),
            ops_ring: RwLock::new(VecDeque::with_capacity(RING_BUFFER_SIZE)),
            total_reads: AtomicU64::new(0),
            total_writes: AtomicU64::new(0),
            total_value_bytes: AtomicU64::new(0),
            value_observations: AtomicU64::new(0),
            memory_usage: RwLock::new(0.0),
            start: Instant::now(),
        }
    }

    /// Record a command execution.
    pub fn record_command(&self, command_name: &str, kind: CommandKind) {
        // Increment command frequency.
        let entry = self
            .command_counts
            .entry(command_name.to_uppercase())
            .or_insert_with(|| AtomicU64::new(0));
        entry.value().fetch_add(1, Ordering::Relaxed);

        match kind {
            CommandKind::Read => {
                self.total_reads.fetch_add(1, Ordering::Relaxed);
            }
            CommandKind::Write => {
                self.total_writes.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Record in ops ring buffer.
        let now_ms = self.start.elapsed().as_millis() as u64;
        let mut ring = self.ops_ring.write();
        ring.push_back(OpsEntry {
            count: 1,
            timestamp_ms: now_ms,
        });
        if ring.len() > RING_BUFFER_SIZE {
            ring.pop_front();
        }
    }

    /// Record a key access with optional value size.
    pub fn record_key_access(&self, key: &str, kind: CommandKind, value_size: Option<usize>) {
        let now_ms = self.start.elapsed().as_millis() as u64;
        let stats = self
            .key_stats
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(KeyStats::new()));

        let s = stats.value();
        match kind {
            CommandKind::Read => {
                s.reads.fetch_add(1, Ordering::Relaxed);
            }
            CommandKind::Write => {
                s.writes.fetch_add(1, Ordering::Relaxed);
            }
        }
        s.last_access_ms.store(now_ms, Ordering::Relaxed);
        s.access_count.fetch_add(1, Ordering::Relaxed);

        if let Some(sz) = value_size {
            s.total_value_bytes.fetch_add(sz as u64, Ordering::Relaxed);
            self.total_value_bytes
                .fetch_add(sz as u64, Ordering::Relaxed);
            self.value_observations.fetch_add(1, Ordering::Relaxed);
        }

        // Evict least-recent keys if we're tracking too many.
        if self.key_stats.len() > MAX_HOT_KEYS * 2 {
            self.evict_cold_keys();
        }
    }

    /// Set the current memory usage fraction (0.0–1.0).
    pub fn set_memory_usage(&self, fraction: f64) {
        *self.memory_usage.write() = fraction.clamp(0.0, 1.0);
    }

    /// Take a snapshot of the current workload metrics.
    pub fn snapshot(&self) -> WorkloadSnapshot {
        let now_ms = self.start.elapsed().as_millis() as u64;

        // Command frequencies.
        let mut command_frequencies: Vec<(String, u64)> = self
            .command_counts
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        command_frequencies.sort_by(|a, b| b.1.cmp(&a.1));

        // Ops/sec from ring buffer.
        let ops_per_sec = {
            let ring = self.ops_ring.read();
            if ring.len() < 2 {
                0.0
            } else {
                let first_ts = ring.front().map(|e| e.timestamp_ms).unwrap_or(now_ms);
                let window_ms = now_ms.saturating_sub(first_ts).max(1);
                let total_ops: u64 = ring.iter().map(|e| e.count).sum();
                (total_ops as f64) / (window_ms as f64 / 1000.0)
            }
        };

        let total_reads = self.total_reads.load(Ordering::Relaxed);
        let total_writes = self.total_writes.load(Ordering::Relaxed);
        let total = total_reads + total_writes;
        let read_write_ratio = if total > 0 {
            total_reads as f64 / total as f64
        } else {
            0.5
        };

        // Hot keys.
        let mut hot_keys: Vec<(String, u64)> = self
            .key_stats
            .iter()
            .map(|e| {
                (
                    e.key().clone(),
                    e.value().access_count.load(Ordering::Relaxed),
                )
            })
            .collect();
        hot_keys.sort_by(|a, b| b.1.cmp(&a.1));
        hot_keys.truncate(20);

        // Avg value size.
        let observations = self.value_observations.load(Ordering::Relaxed);
        let avg_value_size = if observations > 0 {
            self.total_value_bytes.load(Ordering::Relaxed) as f64 / observations as f64
        } else {
            0.0
        };

        WorkloadSnapshot {
            command_frequencies,
            ops_per_sec,
            read_write_ratio,
            total_reads,
            total_writes,
            hot_keys,
            avg_value_size,
            memory_usage_fraction: *self.memory_usage.read(),
            unique_keys_accessed: self.key_stats.len() as u64,
            timestamp_ms: now_ms,
        }
    }

    /// Classify a key based on its access frequency and recency.
    ///
    /// Uses the provided thresholds to decide whether the key belongs in the
    /// hot, warm, or cold tier.
    pub fn classify(&self, key: &str, thresholds: &TierThresholds) -> TierRecommendation {
        let elapsed_secs = self.start.elapsed().as_secs().max(1);
        if let Some(stats) = self.key_stats.get(key) {
            let total = stats.access_count.load(Ordering::Relaxed);
            let freq = total as f64 / elapsed_secs as f64;
            let last_ms = stats.last_access_ms.load(Ordering::Relaxed);
            let now_ms = self.start.elapsed().as_millis() as u64;
            let secs_ago = (now_ms.saturating_sub(last_ms)) / 1000;
            thresholds.classify(freq, secs_ago)
        } else {
            TierRecommendation::Cold
        }
    }

    /// Generate a full workload report with tier classification for all tracked keys.
    pub fn report(&self, thresholds: &TierThresholds) -> WorkloadReport {
        let elapsed_secs = self.start.elapsed().as_secs().max(1);
        let now_ms = self.start.elapsed().as_millis() as u64;

        let mut hot: u64 = 0;
        let mut warm: u64 = 0;
        let mut cold: u64 = 0;

        for entry in self.key_stats.iter() {
            let total = entry.value().access_count.load(Ordering::Relaxed);
            let freq = total as f64 / elapsed_secs as f64;
            let last_ms = entry.value().last_access_ms.load(Ordering::Relaxed);
            let secs_ago = (now_ms.saturating_sub(last_ms)) / 1000;
            match thresholds.classify(freq, secs_ago) {
                TierRecommendation::Hot => hot += 1,
                TierRecommendation::Warm => warm += 1,
                TierRecommendation::Cold => cold += 1,
            }
        }

        let total_reads = self.total_reads.load(Ordering::Relaxed);
        let total_writes = self.total_writes.load(Ordering::Relaxed);
        let total = total_reads + total_writes;
        let rw_ratio = if total > 0 {
            total_reads as f64 / total as f64
        } else {
            0.5
        };

        let snap = self.snapshot();

        WorkloadReport {
            total_keys_analyzed: self.key_stats.len() as u64,
            hot_keys: hot,
            warm_keys: warm,
            cold_keys: cold,
            read_write_ratio: rw_ratio,
            throughput_ops_per_sec: snap.ops_per_sec,
            avg_value_size: snap.avg_value_size,
            memory_usage_fraction: snap.memory_usage_fraction,
        }
    }

    /// Generate a tuning report with tier-move recommendations.
    ///
    /// Identifies keys whose current usage pattern suggests they belong in a
    /// different tier and returns concrete move recommendations.
    pub fn tuning_report(&self, thresholds: &TierThresholds) -> TuningReport {
        let elapsed_secs = self.start.elapsed().as_secs().max(1);
        let now_ms = self.start.elapsed().as_millis() as u64;

        let mut hot: u64 = 0;
        let mut warm: u64 = 0;
        let mut cold: u64 = 0;
        let mut recommendations = Vec::new();

        for entry in self.key_stats.iter() {
            let total = entry.value().access_count.load(Ordering::Relaxed);
            let freq = total as f64 / elapsed_secs as f64;
            let last_ms = entry.value().last_access_ms.load(Ordering::Relaxed);
            let secs_ago = (now_ms.saturating_sub(last_ms)) / 1000;
            let tier = thresholds.classify(freq, secs_ago);

            match tier {
                TierRecommendation::Hot => hot += 1,
                TierRecommendation::Warm => warm += 1,
                TierRecommendation::Cold => cold += 1,
            }

            // Recommend moves for keys that look like they changed tiers.
            // Heuristic: high-frequency keys sitting in cold, or zero-frequency
            // keys sitting in hot, deserve a move.
            let current_tier = if freq >= thresholds.hot_threshold * 2.0 {
                // Was very hot before
                TierRecommendation::Hot
            } else if secs_ago >= thresholds.cold_threshold_secs * 2 {
                TierRecommendation::Cold
            } else {
                TierRecommendation::Warm
            };

            if current_tier != tier {
                recommendations.push(TierMove {
                    key_pattern: entry.key().clone(),
                    current_tier,
                    recommended_tier: tier,
                    access_frequency: freq,
                    last_access_secs_ago: secs_ago,
                });
            }
        }

        // Cap recommendations to avoid huge reports.
        recommendations.sort_by(|a, b| {
            b.access_frequency
                .partial_cmp(&a.access_frequency)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        recommendations.truncate(100);

        let total_keys = self.key_stats.len() as u64;
        let estimated_savings = if total_keys > 0 {
            (cold as f64 / total_keys as f64) * 100.0
        } else {
            0.0
        };

        let total_reads = self.total_reads.load(Ordering::Relaxed);
        let total_writes = self.total_writes.load(Ordering::Relaxed);
        let total = total_reads + total_writes;
        let rw_ratio = if total > 0 {
            total_reads as f64 / total as f64
        } else {
            0.5
        };

        let snap = self.snapshot();

        TuningReport {
            total_keys_analyzed: total_keys,
            hot_keys: hot,
            warm_keys: warm,
            cold_keys: cold,
            estimated_memory_savings_pct: estimated_savings,
            recommendations,
            read_write_ratio: rw_ratio,
            throughput_ops_per_sec: snap.ops_per_sec,
        }
    }

    /// Remove the least-accessed keys to keep the map bounded.
    fn evict_cold_keys(&self) {
        let mut entries: Vec<(String, u64)> = self
            .key_stats
            .iter()
            .map(|e| {
                (
                    e.key().clone(),
                    e.value().access_count.load(Ordering::Relaxed),
                )
            })
            .collect();
        entries.sort_by(|a, b| a.1.cmp(&b.1));

        let to_remove = entries.len().saturating_sub(MAX_HOT_KEYS);
        for (key, _) in entries.into_iter().take(to_remove) {
            self.key_stats.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_snapshot() {
        let profiler = WorkloadProfiler::new();
        profiler.record_command("GET", CommandKind::Read);
        profiler.record_command("GET", CommandKind::Read);
        profiler.record_command("SET", CommandKind::Write);

        let snap = profiler.snapshot();
        assert_eq!(snap.total_reads, 2);
        assert_eq!(snap.total_writes, 1);
        assert!(snap.read_write_ratio > 0.6);
    }

    #[test]
    fn test_key_access_tracking() {
        let profiler = WorkloadProfiler::new();
        profiler.record_key_access("user:1", CommandKind::Read, Some(256));
        profiler.record_key_access("user:1", CommandKind::Read, Some(256));
        profiler.record_key_access("user:2", CommandKind::Write, Some(512));

        let snap = profiler.snapshot();
        assert_eq!(snap.unique_keys_accessed, 2);
        assert!(!snap.hot_keys.is_empty());
        assert!(snap.avg_value_size > 0.0);
    }

    #[test]
    fn test_memory_usage_clamped() {
        let profiler = WorkloadProfiler::new();
        profiler.set_memory_usage(1.5);
        let snap = profiler.snapshot();
        assert!((snap.memory_usage_fraction - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_default_snapshot() {
        let snap = WorkloadSnapshot::default();
        assert_eq!(snap.total_reads, 0);
        assert_eq!(snap.total_writes, 0);
    }
}
