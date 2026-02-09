//! Latency Heatmap and Tier Distribution Tracking
//!
//! Per-command latency distribution tracking with HDR-histogram-like bucketing,
//! and tier distribution visualization data over time.

use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Latency bucket boundaries in nanoseconds.
const BUCKET_BOUNDARIES_NS: [u64; 8] = [
    100,           // <100ns
    1_000,         // <1µs
    10_000,        // <10µs
    100_000,       // <100µs
    1_000_000,     // <1ms
    10_000_000,    // <10ms
    100_000_000,   // <100ms
    1_000_000_000, // <1s
];

/// Human-readable labels for each latency bucket.
const BUCKET_LABELS: [&str; 9] = [
    "<100ns", "<1µs", "<10µs", "<100µs", "<1ms", "<10ms", "<100ms", "<1s", ">=1s",
];

/// Number of latency buckets.
const NUM_BUCKETS: usize = 9;

/// Map a latency duration to its bucket index.
fn bucket_index(latency: Duration) -> usize {
    let ns = latency.as_nanos() as u64;
    for (i, &boundary) in BUCKET_BOUNDARIES_NS.iter().enumerate() {
        if ns < boundary {
            return i;
        }
    }
    NUM_BUCKETS - 1
}

/// A single time slot in the heatmap.
#[derive(Clone, Debug)]
struct TimeSlot {
    timestamp: Instant,
    counts: [u64; NUM_BUCKETS],
}

/// Per-command latency bucket tracking.
pub struct CommandLatencyBuckets {
    time_slots: VecDeque<TimeSlot>,
    resolution: Duration,
    retention: Duration,
    /// Running totals for summary computation.
    total_ops: u64,
    latency_sum_ns: u128,
    all_latencies: Vec<u64>,
}

impl CommandLatencyBuckets {
    fn new(resolution: Duration, retention: Duration) -> Self {
        Self {
            time_slots: VecDeque::new(),
            resolution,
            retention,
            total_ops: 0,
            latency_sum_ns: 0,
            all_latencies: Vec::new(),
        }
    }

    fn record(&mut self, latency: Duration, now: Instant) {
        self.evict_old(now);

        let idx = bucket_index(latency);

        // Check if the latest slot is within the current resolution window
        let needs_new_slot = match self.time_slots.back() {
            Some(slot) => now.duration_since(slot.timestamp) >= self.resolution,
            None => true,
        };

        if needs_new_slot {
            let mut counts = [0u64; NUM_BUCKETS];
            counts[idx] = 1;
            self.time_slots.push_back(TimeSlot {
                timestamp: now,
                counts,
            });
        } else if let Some(slot) = self.time_slots.back_mut() {
            slot.counts[idx] += 1;
        }

        self.total_ops += 1;
        self.latency_sum_ns += latency.as_nanos();
        self.all_latencies.push(latency.as_nanos() as u64);
    }

    fn evict_old(&mut self, now: Instant) {
        while let Some(front) = self.time_slots.front() {
            if now.duration_since(front.timestamp) > self.retention {
                self.time_slots.pop_front();
            } else {
                break;
            }
        }
    }
}

/// A row in the heatmap output.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HeatmapRow {
    /// Unix timestamp in milliseconds.
    pub timestamp_ms: u64,
    /// Count per bucket.
    pub counts: Vec<u64>,
}

/// Serializable heatmap data for a single command.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HeatmapData {
    /// Command name.
    pub command: String,
    /// Time-ordered rows of bucket counts.
    pub time_slots: Vec<HeatmapRow>,
    /// Human-readable labels for each bucket.
    pub bucket_labels: Vec<String>,
}

/// Latency summary for a command.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LatencySummary {
    /// Command name.
    pub command: String,
    /// Total number of recorded operations.
    pub total_ops: u64,
    /// 50th percentile latency.
    pub p50: Duration,
    /// 90th percentile latency.
    pub p90: Duration,
    /// 99th percentile latency.
    pub p99: Duration,
    /// 99.9th percentile latency.
    pub p999: Duration,
    /// Mean latency.
    pub mean: Duration,
}

/// Per-command latency heatmap tracker.
pub struct LatencyHeatmap {
    buckets: DashMap<String, CommandLatencyBuckets>,
    time_resolution: Duration,
    retention: Duration,
}

impl LatencyHeatmap {
    /// Create a new heatmap tracker with the given resolution and retention.
    pub fn new(resolution: Duration, retention: Duration) -> Self {
        Self {
            buckets: DashMap::new(),
            time_resolution: resolution,
            retention,
        }
    }

    /// Record a latency observation for a command.
    pub fn record(&self, command: &str, latency: Duration) {
        let now = Instant::now();
        let mut entry = self
            .buckets
            .entry(command.to_string())
            .or_insert_with(|| CommandLatencyBuckets::new(self.time_resolution, self.retention));
        entry.record(latency, now);
    }

    /// Get heatmap data for a command over the given duration.
    pub fn get_heatmap(&self, command: &str, duration: Duration) -> HeatmapData {
        let now_system = SystemTime::now();
        let now_instant = Instant::now();
        let labels: Vec<String> = BUCKET_LABELS.iter().map(|s| s.to_string()).collect();

        let rows = if let Some(entry) = self.buckets.get(command) {
            entry
                .time_slots
                .iter()
                .filter(|slot| now_instant.duration_since(slot.timestamp) <= duration)
                .map(|slot| {
                    let elapsed = now_instant.duration_since(slot.timestamp);
                    let slot_system = now_system - elapsed;
                    let ts_ms = slot_system
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    HeatmapRow {
                        timestamp_ms: ts_ms,
                        counts: slot.counts.to_vec(),
                    }
                })
                .collect()
        } else {
            Vec::new()
        };

        HeatmapData {
            command: command.to_string(),
            time_slots: rows,
            bucket_labels: labels,
        }
    }

    /// List all commands that have been tracked.
    pub fn get_all_commands(&self) -> Vec<String> {
        self.buckets.iter().map(|e| e.key().clone()).collect()
    }

    /// Get a latency summary for a command.
    pub fn get_summary(&self, command: &str) -> Option<LatencySummary> {
        let entry = self.buckets.get(command)?;
        if entry.total_ops == 0 {
            return None;
        }

        let mean_ns = (entry.latency_sum_ns / entry.total_ops as u128) as u64;

        let mut sorted = entry.all_latencies.clone();
        sorted.sort_unstable();

        let percentile = |p: f64| -> Duration {
            let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
            Duration::from_nanos(sorted[idx.min(sorted.len() - 1)])
        };

        Some(LatencySummary {
            command: command.to_string(),
            total_ops: entry.total_ops,
            p50: percentile(50.0),
            p90: percentile(90.0),
            p99: percentile(99.0),
            p999: percentile(99.9),
            mean: Duration::from_nanos(mean_ns),
        })
    }
}

/// A point-in-time snapshot of tier distribution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TierDistributionSnapshot {
    /// Number of keys per tier (e.g., "hot" -> 1000).
    pub keys_per_tier: HashMap<String, u64>,
    /// Bytes per tier.
    pub bytes_per_tier: HashMap<String, u64>,
}

/// A timestamped tier snapshot.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TierSnapshot {
    /// Unix timestamp in milliseconds.
    pub timestamp_ms: u64,
    /// Keys per tier.
    pub keys_per_tier: HashMap<String, u64>,
    /// Bytes per tier.
    pub bytes_per_tier: HashMap<String, u64>,
}

/// Collector that tracks tier distribution over time.
pub struct TierStatsCollector {
    snapshots: RwLock<VecDeque<(Instant, TierSnapshot)>>,
    max_retention: Duration,
}

impl TierStatsCollector {
    /// Create a new tier stats collector with the given retention.
    pub fn new(max_retention: Duration) -> Self {
        Self {
            snapshots: RwLock::new(VecDeque::new()),
            max_retention,
        }
    }

    /// Record a new tier distribution snapshot.
    pub fn record_snapshot(&self, distribution: TierDistributionSnapshot) {
        let now = Instant::now();
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let snapshot = TierSnapshot {
            timestamp_ms: ts_ms,
            keys_per_tier: distribution.keys_per_tier,
            bytes_per_tier: distribution.bytes_per_tier,
        };

        let mut snaps = self.snapshots.write();
        // Evict old entries
        while let Some((ts, _)) = snaps.front() {
            if now.duration_since(*ts) > self.max_retention {
                snaps.pop_front();
            } else {
                break;
            }
        }
        snaps.push_back((now, snapshot));
    }

    /// Get tier snapshots within the given duration.
    pub fn get_history(&self, duration: Duration) -> Vec<TierSnapshot> {
        let now = Instant::now();
        let snaps = self.snapshots.read();
        snaps
            .iter()
            .filter(|(ts, _)| now.duration_since(*ts) <= duration)
            .map(|(_, s)| s.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_index() {
        assert_eq!(bucket_index(Duration::from_nanos(50)), 0); // <100ns
        assert_eq!(bucket_index(Duration::from_nanos(500)), 1); // <1µs
        assert_eq!(bucket_index(Duration::from_micros(5)), 2); // <10µs
        assert_eq!(bucket_index(Duration::from_micros(50)), 3); // <100µs
        assert_eq!(bucket_index(Duration::from_micros(500)), 4); // <1ms
        assert_eq!(bucket_index(Duration::from_millis(5)), 5); // <10ms
        assert_eq!(bucket_index(Duration::from_millis(50)), 6); // <100ms
        assert_eq!(bucket_index(Duration::from_millis(500)), 7); // <1s
        assert_eq!(bucket_index(Duration::from_secs(2)), 8); // >=1s
    }

    #[test]
    fn test_heatmap_record_and_retrieve() {
        let hm = LatencyHeatmap::new(Duration::from_secs(1), Duration::from_secs(300));
        hm.record("GET", Duration::from_micros(50));
        hm.record("GET", Duration::from_micros(500));
        hm.record("SET", Duration::from_millis(1));

        let cmds = hm.get_all_commands();
        assert!(cmds.contains(&"GET".to_string()));
        assert!(cmds.contains(&"SET".to_string()));

        let data = hm.get_heatmap("GET", Duration::from_secs(60));
        assert_eq!(data.command, "GET");
        assert_eq!(data.bucket_labels.len(), NUM_BUCKETS);
        assert!(!data.time_slots.is_empty());
    }

    #[test]
    fn test_heatmap_summary() {
        let hm = LatencyHeatmap::new(Duration::from_secs(1), Duration::from_secs(300));
        for i in 1..=100 {
            hm.record("PING", Duration::from_micros(i));
        }
        let summary = hm.get_summary("PING").unwrap();
        assert_eq!(summary.command, "PING");
        assert_eq!(summary.total_ops, 100);
        assert!(summary.p50 > Duration::ZERO);
        assert!(summary.p99 >= summary.p50);
    }

    #[test]
    fn test_heatmap_missing_command() {
        let hm = LatencyHeatmap::new(Duration::from_secs(1), Duration::from_secs(300));
        let data = hm.get_heatmap("NONEXIST", Duration::from_secs(60));
        assert!(data.time_slots.is_empty());
        assert!(hm.get_summary("NONEXIST").is_none());
    }

    #[test]
    fn test_tier_stats_collector() {
        let collector = TierStatsCollector::new(Duration::from_secs(300));

        let mut keys = HashMap::new();
        keys.insert("hot".to_string(), 1000);
        keys.insert("warm".to_string(), 5000);
        keys.insert("cold".to_string(), 50000);

        let mut bytes = HashMap::new();
        bytes.insert("hot".to_string(), 1024 * 1024);
        bytes.insert("warm".to_string(), 10 * 1024 * 1024);
        bytes.insert("cold".to_string(), 100 * 1024 * 1024);

        collector.record_snapshot(TierDistributionSnapshot {
            keys_per_tier: keys,
            bytes_per_tier: bytes,
        });

        let history = collector.get_history(Duration::from_secs(60));
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].keys_per_tier["hot"], 1000);
        assert_eq!(history[0].bytes_per_tier["cold"], 100 * 1024 * 1024);
    }

    #[test]
    fn test_tier_stats_retention() {
        let collector = TierStatsCollector::new(Duration::from_secs(1));

        // Record a snapshot
        collector.record_snapshot(TierDistributionSnapshot {
            keys_per_tier: HashMap::new(),
            bytes_per_tier: HashMap::new(),
        });

        // Immediately available
        assert_eq!(collector.get_history(Duration::from_secs(60)).len(), 1);
    }
}
