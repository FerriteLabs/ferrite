//! Per-Command Query Profiler with SLO Tracking
//!
//! Provides per-command latency profiling with HDR histogram percentile
//! calculation, per-client statistics, and configurable SLO monitoring.
//!
//! # Example
//!
//! ```ignore
//! use ferrite_core::observability::query_profiler::{QueryProfiler, QueryProfilerConfig};
//!
//! let profiler = QueryProfiler::new(QueryProfilerConfig::default());
//! let span = profiler.begin_command("GET", "client:1", 1);
//! // ... execute command ...
//! profiler.end_command(span);
//!
//! let stats = profiler.get_command_stats("GET").unwrap();
//! println!("p99 latency: {}us", stats.p99_us);
//! ```

use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the query profiler.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryProfilerConfig {
    /// Whether profiling is enabled.
    pub enabled: bool,
    /// Per-command SLO thresholds in microseconds.
    pub slo_threshold_us: HashMap<String, u64>,
    /// Default SLO threshold for commands without an explicit entry.
    pub default_slo_us: u64,
    /// Maximum number of distinct commands to track.
    pub max_tracked_commands: usize,
    /// Maximum number of distinct clients to track.
    pub max_tracked_clients: usize,
}

impl Default for QueryProfilerConfig {
    fn default() -> Self {
        let mut slo_threshold_us = HashMap::new();
        slo_threshold_us.insert("GET".to_string(), 1000);
        slo_threshold_us.insert("SET".to_string(), 1000);
        slo_threshold_us.insert("QUERY".to_string(), 30000);
        Self {
            enabled: true,
            slo_threshold_us,
            default_slo_us: 5000,
            max_tracked_commands: 1000,
            max_tracked_clients: 10000,
        }
    }
}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// How to sort command statistics in [`QueryProfiler::get_top_commands`].
#[derive(Clone, Copy, Debug)]
pub enum SortBy {
    /// Sort by total number of calls (descending).
    TotalCalls,
    /// Sort by average latency (descending).
    AvgLatency,
    /// Sort by maximum observed latency (descending).
    MaxLatency,
    /// Sort by error count (descending).
    Errors,
}

/// Result of an SLO check against a single command execution.
#[derive(Clone, Debug, PartialEq)]
pub enum SloResult {
    /// Latency is within the SLO threshold.
    Ok,
    /// Latency is between 80% and 100% of the threshold.
    Warning(String),
    /// Latency exceeds the SLO threshold.
    Violation(String),
}

// ---------------------------------------------------------------------------
// CommandSpan – in-flight command handle
// ---------------------------------------------------------------------------

/// Represents an in-flight command being profiled.
///
/// Created by [`QueryProfiler::begin_command`] and consumed by
/// [`QueryProfiler::end_command`].
pub struct CommandSpan {
    /// The command name (e.g. `"GET"`).
    pub command: String,
    /// Identifier of the client that issued the command.
    pub client_id: String,
    /// Number of arguments supplied with the command.
    pub args_count: usize,
    /// When the command started executing.
    pub start_time: Instant,
    /// Extracted key pattern (e.g. `"users:*"` derived from `"users:123"`).
    pub key_pattern: Option<String>,
}

// ---------------------------------------------------------------------------
// LatencyHistogram – logarithmic bucket histogram
// ---------------------------------------------------------------------------

/// HDR histogram approximation using logarithmic buckets.
///
/// Buckets:
/// 0 → [0, 10) µs
/// 1 → [10, 100) µs
/// 2 → [100, 1_000) µs  (1 ms)
/// 3 → [1_000, 10_000) µs  (10 ms)
/// 4 → [10_000, 100_000) µs  (100 ms)
/// 5 → [100_000, 1_000_000) µs  (1 s)
/// 6 → [1_000_000, ∞) µs
#[derive(Debug, Clone)]
pub struct LatencyHistogram {
    /// Counts per bucket.
    buckets: [u64; 7],
}

/// Upper bounds (exclusive) for each bucket in microseconds.
const BUCKET_BOUNDS: [u64; 6] = [10, 100, 1_000, 10_000, 100_000, 1_000_000];

/// Representative midpoint values used for percentile interpolation.
const BUCKET_MIDPOINTS: [u64; 7] = [5, 55, 550, 5_500, 55_000, 550_000, 1_500_000];

impl LatencyHistogram {
    /// Create an empty histogram.
    pub fn new() -> Self {
        Self { buckets: [0; 7] }
    }

    /// Record a latency sample.
    pub fn record(&mut self, latency_us: u64) {
        let idx = BUCKET_BOUNDS
            .iter()
            .position(|&b| latency_us < b)
            .unwrap_or(6);
        self.buckets[idx] += 1;
    }

    /// Total number of recorded samples.
    pub fn total_count(&self) -> u64 {
        self.buckets.iter().sum()
    }

    /// Return the estimated latency at the given percentile (0.0–100.0).
    pub fn percentile(&self, p: f64) -> u64 {
        let total = self.total_count();
        if total == 0 {
            return 0;
        }
        let threshold = ((p / 100.0) * total as f64).ceil() as u64;
        let mut cumulative: u64 = 0;
        for (i, &count) in self.buckets.iter().enumerate() {
            cumulative += count;
            if cumulative >= threshold {
                return BUCKET_MIDPOINTS[i];
            }
        }
        *BUCKET_MIDPOINTS.last().expect("midpoints not empty")
    }

    /// 50th percentile (median).
    pub fn p50(&self) -> u64 {
        self.percentile(50.0)
    }

    /// 95th percentile.
    pub fn p95(&self) -> u64 {
        self.percentile(95.0)
    }

    /// 99th percentile.
    pub fn p99(&self) -> u64 {
        self.percentile(99.0)
    }

    /// 99.9th percentile.
    pub fn p999(&self) -> u64 {
        self.percentile(99.9)
    }
}

// ---------------------------------------------------------------------------
// CommandProfile – aggregate per-command stats (interior-mutable)
// ---------------------------------------------------------------------------

/// Aggregate statistics for a single command type.
pub struct CommandProfile {
    /// Command name.
    pub command: String,
    /// Total number of calls observed.
    pub total_calls: AtomicU64,
    /// Sum of all latencies in microseconds.
    pub total_latency_us: AtomicU64,
    /// Minimum observed latency in microseconds.
    pub min_latency_us: AtomicU64,
    /// Maximum observed latency in microseconds.
    pub max_latency_us: AtomicU64,
    /// Number of errors.
    pub errors: AtomicU64,
    /// Latency histogram for percentile calculation.
    pub latency_histogram: RwLock<LatencyHistogram>,
}

impl CommandProfile {
    fn new(command: String) -> Self {
        Self {
            command,
            total_calls: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
            min_latency_us: AtomicU64::new(u64::MAX),
            max_latency_us: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            latency_histogram: RwLock::new(LatencyHistogram::new()),
        }
    }

    fn record(&self, latency_us: u64) {
        self.total_calls.fetch_add(1, Ordering::Relaxed);
        self.total_latency_us.fetch_add(latency_us, Ordering::Relaxed);
        self.latency_histogram.write().record(latency_us);

        // Update min (CAS loop)
        let mut current = self.min_latency_us.load(Ordering::Relaxed);
        while latency_us < current {
            match self.min_latency_us.compare_exchange_weak(
                current,
                latency_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(v) => current = v,
            }
        }

        // Update max (CAS loop)
        let mut current = self.max_latency_us.load(Ordering::Relaxed);
        while latency_us > current {
            match self.max_latency_us.compare_exchange_weak(
                current,
                latency_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(v) => current = v,
            }
        }
    }

    fn snapshot(&self) -> CommandStatsSnapshot {
        let total_calls = self.total_calls.load(Ordering::Relaxed);
        let total_latency = self.total_latency_us.load(Ordering::Relaxed);
        let avg = if total_calls > 0 {
            total_latency as f64 / total_calls as f64
        } else {
            0.0
        };
        let min = self.min_latency_us.load(Ordering::Relaxed);
        let max = self.max_latency_us.load(Ordering::Relaxed);
        let hist = self.latency_histogram.read();
        CommandStatsSnapshot {
            command: self.command.clone(),
            total_calls,
            avg_latency_us: avg,
            min_latency_us: if min == u64::MAX { 0 } else { min },
            max_latency_us: max,
            p50_us: hist.p50(),
            p95_us: hist.p95(),
            p99_us: hist.p99(),
            p999_us: hist.p999(),
            errors: self.errors.load(Ordering::Relaxed),
            ops_per_sec: 0.0, // caller may fill in with wall-clock info
        }
    }
}

// ---------------------------------------------------------------------------
// ClientProfile
// ---------------------------------------------------------------------------

/// Per-client aggregate statistics.
pub struct ClientProfile {
    /// Client identifier.
    pub client_id: String,
    /// Total commands executed by this client.
    pub total_commands: AtomicU64,
    /// Cumulative latency in microseconds.
    pub total_latency_us: AtomicU64,
    /// Timestamp (microseconds since epoch-ish) of last command.
    pub last_command_time: AtomicU64,
}

impl ClientProfile {
    fn new(client_id: String) -> Self {
        Self {
            client_id,
            total_commands: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
            last_command_time: AtomicU64::new(0),
        }
    }
}

// ---------------------------------------------------------------------------
// Snapshot types (serializable)
// ---------------------------------------------------------------------------

/// Point-in-time snapshot of a command's profiling statistics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandStatsSnapshot {
    /// Command name.
    pub command: String,
    /// Total number of calls.
    pub total_calls: u64,
    /// Average latency in microseconds.
    pub avg_latency_us: f64,
    /// Minimum latency in microseconds.
    pub min_latency_us: u64,
    /// Maximum latency in microseconds.
    pub max_latency_us: u64,
    /// 50th percentile latency.
    pub p50_us: u64,
    /// 95th percentile latency.
    pub p95_us: u64,
    /// 99th percentile latency.
    pub p99_us: u64,
    /// 99.9th percentile latency.
    pub p999_us: u64,
    /// Number of errors.
    pub errors: u64,
    /// Estimated operations per second.
    pub ops_per_sec: f64,
}

/// Point-in-time snapshot of a client's profiling statistics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientStatsSnapshot {
    /// Client identifier.
    pub client_id: String,
    /// Total commands executed.
    pub total_commands: u64,
    /// Cumulative latency in microseconds.
    pub total_latency_us: u64,
    /// Timestamp of last command (microseconds).
    pub last_command_time: u64,
}

// ---------------------------------------------------------------------------
// QueryProfiler
// ---------------------------------------------------------------------------

/// Per-command profiler with latency percentile tracking and SLO monitoring.
///
/// Thread-safe: all internal state is protected by atomic operations or
/// concurrent maps, so the profiler can be shared across threads via `Arc`.
pub struct QueryProfiler {
    /// Profiler configuration.
    config: QueryProfilerConfig,
    /// Per-command aggregate profiles.
    command_stats: DashMap<String, CommandProfile>,
    /// Per-client aggregate profiles.
    client_stats: DashMap<String, ClientProfile>,
    /// Number of commands currently in flight.
    active_commands: AtomicU64,
    /// Total commands completed since creation (or last reset).
    total_commands: AtomicU64,
    /// Number of SLO violations observed.
    slo_violations: AtomicU64,
}

impl QueryProfiler {
    /// Create a new profiler with the given configuration.
    pub fn new(config: QueryProfilerConfig) -> Self {
        Self {
            config,
            command_stats: DashMap::new(),
            client_stats: DashMap::new(),
            active_commands: AtomicU64::new(0),
            total_commands: AtomicU64::new(0),
            slo_violations: AtomicU64::new(0),
        }
    }

    /// Begin profiling a command execution, returning a [`CommandSpan`].
    ///
    /// The returned span must be passed to [`end_command`](Self::end_command)
    /// when the command finishes to record the measurement.
    pub fn begin_command(&self, command: &str, client_id: &str, args_count: usize) -> CommandSpan {
        self.active_commands.fetch_add(1, Ordering::Relaxed);
        CommandSpan {
            command: command.to_uppercase(),
            client_id: client_id.to_string(),
            args_count,
            start_time: Instant::now(),
            key_pattern: None,
        }
    }

    /// Finish profiling a command execution and record the measurement.
    pub fn end_command(&self, span: CommandSpan) {
        let latency_us = span.start_time.elapsed().as_micros() as u64;
        self.active_commands.fetch_sub(1, Ordering::Relaxed);
        self.total_commands.fetch_add(1, Ordering::Relaxed);

        if !self.config.enabled {
            return;
        }

        // Record into command profile (insert if first time)
        if self.command_stats.len() < self.config.max_tracked_commands
            || self.command_stats.contains_key(&span.command)
        {
            self.command_stats
                .entry(span.command.clone())
                .or_insert_with(|| CommandProfile::new(span.command.clone()))
                .record(latency_us);
        }

        // Record into client profile
        if self.client_stats.len() < self.config.max_tracked_clients
            || self.client_stats.contains_key(&span.client_id)
        {
            let profile = self
                .client_stats
                .entry(span.client_id.clone())
                .or_insert_with(|| ClientProfile::new(span.client_id.clone()));
            profile.total_commands.fetch_add(1, Ordering::Relaxed);
            profile.total_latency_us.fetch_add(latency_us, Ordering::Relaxed);
            profile.last_command_time.store(latency_us, Ordering::Relaxed);
        }

        // SLO check
        if let SloResult::Violation(_) = self.check_slo(&span.command, latency_us) {
            self.slo_violations.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Return a snapshot of statistics for the given command, if tracked.
    pub fn get_command_stats(&self, command: &str) -> Option<CommandStatsSnapshot> {
        self.command_stats.get(command).map(|p| p.snapshot())
    }

    /// Return the top *n* commands sorted by the given criterion.
    pub fn get_top_commands(&self, n: usize, sort_by: SortBy) -> Vec<CommandStatsSnapshot> {
        let mut snapshots: Vec<CommandStatsSnapshot> = self
            .command_stats
            .iter()
            .map(|entry| entry.value().snapshot())
            .collect();

        snapshots.sort_by(|a, b| {
            let cmp = match sort_by {
                SortBy::TotalCalls => b.total_calls.cmp(&a.total_calls),
                SortBy::AvgLatency => b
                    .avg_latency_us
                    .partial_cmp(&a.avg_latency_us)
                    .unwrap_or(std::cmp::Ordering::Equal),
                SortBy::MaxLatency => b.max_latency_us.cmp(&a.max_latency_us),
                SortBy::Errors => b.errors.cmp(&a.errors),
            };
            cmp
        });

        snapshots.truncate(n);
        snapshots
    }

    /// Return a snapshot of statistics for the given client, if tracked.
    pub fn get_client_stats(&self, client_id: &str) -> Option<ClientStatsSnapshot> {
        self.client_stats.get(client_id).map(|p| ClientStatsSnapshot {
            client_id: p.client_id.clone(),
            total_commands: p.total_commands.load(Ordering::Relaxed),
            total_latency_us: p.total_latency_us.load(Ordering::Relaxed),
            last_command_time: p.last_command_time.load(Ordering::Relaxed),
        })
    }

    /// Check if the given latency violates the SLO for the specified command.
    pub fn check_slo(&self, command: &str, latency_us: u64) -> SloResult {
        let threshold = self
            .config
            .slo_threshold_us
            .get(command)
            .copied()
            .unwrap_or(self.config.default_slo_us);

        if latency_us > threshold {
            SloResult::Violation(format!(
                "{} latency {}us exceeds SLO {}us",
                command, latency_us, threshold
            ))
        } else if latency_us > threshold * 80 / 100 {
            SloResult::Warning(format!(
                "{} latency {}us is within 20% of SLO {}us",
                command, latency_us, threshold
            ))
        } else {
            SloResult::Ok
        }
    }

    /// Reset all collected statistics.
    pub fn reset_stats(&self) {
        self.command_stats.clear();
        self.client_stats.clear();
        self.active_commands.store(0, Ordering::Relaxed);
        self.total_commands.store(0, Ordering::Relaxed);
        self.slo_violations.store(0, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_begin_end_command_records_stats() {
        let profiler = QueryProfiler::new(QueryProfilerConfig::default());
        let span = profiler.begin_command("GET", "client:1", 1);
        assert_eq!(profiler.active_commands.load(Ordering::Relaxed), 1);
        profiler.end_command(span);

        let stats = profiler.get_command_stats("GET").expect("stats must exist");
        assert_eq!(stats.total_calls, 1);
        assert_eq!(stats.command, "GET");
        assert_eq!(profiler.total_commands.load(Ordering::Relaxed), 1);
        assert_eq!(profiler.active_commands.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_latency_histogram_percentiles() {
        let mut hist = LatencyHistogram::new();
        // 90 samples in [0,10) bucket, 10 samples in [100,1000) bucket
        for _ in 0..90 {
            hist.record(5);
        }
        for _ in 0..10 {
            hist.record(500);
        }

        assert_eq!(hist.total_count(), 100);
        // p50 should be in the first bucket
        assert_eq!(hist.p50(), BUCKET_MIDPOINTS[0]);
        // p95 should be in the third bucket (100–1000 us)
        assert_eq!(hist.p95(), BUCKET_MIDPOINTS[2]);
        // p99 should be in the third bucket
        assert_eq!(hist.p99(), BUCKET_MIDPOINTS[2]);
    }

    #[test]
    fn test_per_command_stats_accumulation() {
        let profiler = QueryProfiler::new(QueryProfilerConfig::default());

        for _ in 0..10 {
            let span = profiler.begin_command("SET", "c1", 2);
            profiler.end_command(span);
        }
        for _ in 0..5 {
            let span = profiler.begin_command("GET", "c1", 1);
            profiler.end_command(span);
        }

        let set_stats = profiler.get_command_stats("SET").expect("SET stats");
        assert_eq!(set_stats.total_calls, 10);

        let get_stats = profiler.get_command_stats("GET").expect("GET stats");
        assert_eq!(get_stats.total_calls, 5);
    }

    #[test]
    fn test_top_commands_sorting() {
        let profiler = QueryProfiler::new(QueryProfilerConfig::default());

        for _ in 0..20 {
            let span = profiler.begin_command("SET", "c1", 2);
            profiler.end_command(span);
        }
        for _ in 0..5 {
            let span = profiler.begin_command("GET", "c1", 1);
            profiler.end_command(span);
        }
        for _ in 0..10 {
            let span = profiler.begin_command("DEL", "c1", 1);
            profiler.end_command(span);
        }

        let top = profiler.get_top_commands(2, SortBy::TotalCalls);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].command, "SET");
        assert_eq!(top[0].total_calls, 20);
        assert_eq!(top[1].command, "DEL");
        assert_eq!(top[1].total_calls, 10);
    }

    #[test]
    fn test_slo_violation_detection() {
        let profiler = QueryProfiler::new(QueryProfilerConfig::default());

        // GET SLO is 1000 µs by default
        assert_eq!(profiler.check_slo("GET", 500), SloResult::Ok);

        // 850 µs → within 20% of 1000 → Warning
        match profiler.check_slo("GET", 850) {
            SloResult::Warning(_) => {}
            other => panic!("expected Warning, got {:?}", other),
        }

        // 1500 µs → Violation
        match profiler.check_slo("GET", 1500) {
            SloResult::Violation(_) => {}
            other => panic!("expected Violation, got {:?}", other),
        }

        // Unknown command → uses default_slo_us (5000)
        assert_eq!(profiler.check_slo("CUSTOM", 4000), SloResult::Ok);
        match profiler.check_slo("CUSTOM", 6000) {
            SloResult::Violation(_) => {}
            other => panic!("expected Violation, got {:?}", other),
        }
    }

    #[test]
    fn test_client_stats_tracking() {
        let profiler = QueryProfiler::new(QueryProfilerConfig::default());

        for _ in 0..3 {
            let span = profiler.begin_command("GET", "client:42", 1);
            profiler.end_command(span);
        }

        let cs = profiler
            .get_client_stats("client:42")
            .expect("client stats");
        assert_eq!(cs.client_id, "client:42");
        assert_eq!(cs.total_commands, 3);
    }

    #[test]
    fn test_reset_stats_clears_everything() {
        let profiler = QueryProfiler::new(QueryProfilerConfig::default());

        let span = profiler.begin_command("GET", "c1", 1);
        profiler.end_command(span);
        assert!(profiler.get_command_stats("GET").is_some());

        profiler.reset_stats();

        assert!(profiler.get_command_stats("GET").is_none());
        assert!(profiler.get_client_stats("c1").is_none());
        assert_eq!(profiler.total_commands.load(Ordering::Relaxed), 0);
        assert_eq!(profiler.slo_violations.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_multiple_concurrent_commands() {
        let profiler = std::sync::Arc::new(QueryProfiler::new(QueryProfilerConfig::default()));
        let mut handles = vec![];

        for i in 0..8 {
            let p = profiler.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let cmd = if i % 2 == 0 { "GET" } else { "SET" };
                    let span = p.begin_command(cmd, &format!("client:{}", i), 1);
                    p.end_command(span);
                }
            }));
        }

        for h in handles {
            h.join().expect("thread panicked");
        }

        assert_eq!(profiler.total_commands.load(Ordering::Relaxed), 800);

        let get_stats = profiler.get_command_stats("GET").expect("GET stats");
        let set_stats = profiler.get_command_stats("SET").expect("SET stats");
        assert_eq!(get_stats.total_calls + set_stats.total_calls, 800);
    }
}
