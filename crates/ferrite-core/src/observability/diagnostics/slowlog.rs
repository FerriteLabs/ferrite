//! Enhanced Slow Query Analyzer
//!
//! Provides pattern analysis over slow query entries including percentile
//! latency calculations and top-command breakdowns.

#![forbid(unsafe_code)]

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

/// A single slow query entry with full context.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AnalyzedSlowQueryEntry {
    /// Monotonically increasing identifier.
    pub id: u64,
    /// Unix timestamp in microseconds.
    pub timestamp: u64,
    /// The command name (e.g. "SET", "HGETALL").
    pub command: String,
    /// Command arguments.
    pub args: Vec<String>,
    /// Execution duration in microseconds.
    pub duration_us: u64,
    /// Client address, if known.
    pub client_addr: Option<String>,
    /// Database index.
    pub database: u8,
}

/// Statistical report over accumulated slow queries.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SlowQueryReport {
    /// Total slow queries recorded.
    pub total: u64,
    /// Mean duration in microseconds.
    pub avg_duration_us: u64,
    /// 50th percentile duration.
    pub p50_us: u64,
    /// 99th percentile duration.
    pub p99_us: u64,
    /// Top commands by occurrence count.
    pub top_commands: Vec<(String, u64)>,
    /// Top key patterns by occurrence count.
    pub top_patterns: Vec<(String, u64)>,
}

/// Ring-buffer slow query analyzer with pattern detection.
pub struct SlowQueryAnalyzer {
    entries: RwLock<VecDeque<AnalyzedSlowQueryEntry>>,
    max_entries: usize,
    threshold_us: u64,
    total_recorded: RwLock<u64>,
}

impl SlowQueryAnalyzer {
    /// Create a new analyzer.
    ///
    /// * `max_entries` – ring-buffer capacity.
    /// * `threshold_us` – minimum duration to be considered slow.
    pub fn new(max_entries: usize, threshold_us: u64) -> Self {
        Self {
            entries: RwLock::new(VecDeque::with_capacity(max_entries)),
            max_entries,
            threshold_us,
            total_recorded: RwLock::new(0),
        }
    }

    /// Record a slow query entry. Entries below the threshold are ignored.
    pub fn record(&self, entry: AnalyzedSlowQueryEntry) {
        if entry.duration_us < self.threshold_us {
            return;
        }
        let mut entries = self.entries.write();
        let mut total = self.total_recorded.write();
        *total += 1;
        if entries.len() >= self.max_entries {
            entries.pop_front();
        }
        entries.push_back(entry);
    }

    /// Get the latest `count` slow query entries (most recent first).
    pub fn get(&self, count: usize) -> Vec<AnalyzedSlowQueryEntry> {
        let entries = self.entries.read();
        entries.iter().rev().take(count).cloned().collect()
    }

    /// Clear all entries and return the number that were removed.
    pub fn reset(&self) -> u64 {
        let mut entries = self.entries.write();
        let count = entries.len() as u64;
        entries.clear();
        count
    }

    /// Total number of slow queries ever recorded (may exceed buffer size).
    pub fn total_recorded(&self) -> u64 {
        *self.total_recorded.read()
    }

    /// Analyze the current buffer and produce a statistical report.
    pub fn analyze(&self) -> SlowQueryReport {
        let entries = self.entries.read();

        if entries.is_empty() {
            return SlowQueryReport {
                total: 0,
                avg_duration_us: 0,
                p50_us: 0,
                p99_us: 0,
                top_commands: Vec::new(),
                top_patterns: Vec::new(),
            };
        }

        let mut durations: Vec<u64> = entries.iter().map(|e| e.duration_us).collect();
        durations.sort_unstable();

        let total = durations.len() as u64;
        let sum: u64 = durations.iter().sum();
        let avg = sum / total;
        let p50 = percentile(&durations, 50.0);
        let p99 = percentile(&durations, 99.0);

        // Count commands
        let mut cmd_counts: HashMap<String, u64> = HashMap::new();
        for e in entries.iter() {
            *cmd_counts.entry(e.command.clone()).or_default() += 1;
        }
        let mut top_commands: Vec<(String, u64)> = cmd_counts.into_iter().collect();
        top_commands.sort_by(|a, b| b.1.cmp(&a.1));
        top_commands.truncate(10);

        // Count key patterns (first arg as proxy)
        let mut pattern_counts: HashMap<String, u64> = HashMap::new();
        for e in entries.iter() {
            let pattern = if let Some(first) = e.args.first() {
                extract_pattern(first)
            } else {
                "(no-args)".to_string()
            };
            *pattern_counts.entry(pattern).or_default() += 1;
        }
        let mut top_patterns: Vec<(String, u64)> = pattern_counts.into_iter().collect();
        top_patterns.sort_by(|a, b| b.1.cmp(&a.1));
        top_patterns.truncate(10);

        SlowQueryReport {
            total,
            avg_duration_us: avg,
            p50_us: p50,
            p99_us: p99,
            top_commands,
            top_patterns,
        }
    }

    /// Current threshold in microseconds.
    pub fn threshold(&self) -> u64 {
        self.threshold_us
    }

    /// Current number of buffered entries.
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }
}

/// Compute a percentile value from a sorted slice.
fn percentile(sorted: &[u64], pct: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((pct / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

/// Extract a generalised key pattern by replacing trailing numeric segments
/// with `*` (e.g. `user:12345` → `user:*`).
fn extract_pattern(key: &str) -> String {
    let parts: Vec<&str> = key.split(':').collect();
    if parts.len() <= 1 {
        return key.to_string();
    }
    parts
        .iter()
        .map(|p| {
            if p.chars().all(|c| c.is_ascii_digit()) {
                "*"
            } else {
                p
            }
        })
        .collect::<Vec<_>>()
        .join(":")
}

/// Helper to get a current timestamp in microseconds.
pub(crate) fn current_timestamp_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(id: u64, cmd: &str, dur: u64) -> AnalyzedSlowQueryEntry {
        AnalyzedSlowQueryEntry {
            id,
            timestamp: current_timestamp_us(),
            command: cmd.to_string(),
            args: vec!["key:123".to_string()],
            duration_us: dur,
            client_addr: None,
            database: 0,
        }
    }

    #[test]
    fn test_record_and_get() {
        let analyzer = SlowQueryAnalyzer::new(100, 1_000);
        analyzer.record(make_entry(1, "GET", 5_000));
        analyzer.record(make_entry(2, "SET", 3_000));
        assert_eq!(analyzer.len(), 2);

        let recent = analyzer.get(1);
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].command, "SET");
    }

    #[test]
    fn test_below_threshold_ignored() {
        let analyzer = SlowQueryAnalyzer::new(100, 10_000);
        analyzer.record(make_entry(1, "GET", 500));
        assert!(analyzer.is_empty());
    }

    #[test]
    fn test_ring_buffer_eviction() {
        let analyzer = SlowQueryAnalyzer::new(3, 100);
        for i in 0..5 {
            analyzer.record(make_entry(i, "CMD", 200 + i * 100));
        }
        assert_eq!(analyzer.len(), 3);
        let entries = analyzer.get(10);
        assert_eq!(entries[0].id, 4);
    }

    #[test]
    fn test_reset() {
        let analyzer = SlowQueryAnalyzer::new(100, 100);
        analyzer.record(make_entry(1, "GET", 200));
        analyzer.record(make_entry(2, "SET", 300));
        let cleared = analyzer.reset();
        assert_eq!(cleared, 2);
        assert!(analyzer.is_empty());
    }

    #[test]
    fn test_analyze_report() {
        let analyzer = SlowQueryAnalyzer::new(100, 100);
        analyzer.record(make_entry(1, "GET", 1_000));
        analyzer.record(make_entry(2, "GET", 2_000));
        analyzer.record(make_entry(3, "SET", 3_000));
        analyzer.record(make_entry(4, "SET", 4_000));
        analyzer.record(make_entry(5, "GET", 5_000));

        let report = analyzer.analyze();
        assert_eq!(report.total, 5);
        assert_eq!(report.avg_duration_us, 3_000);
        assert!(report.p50_us >= 2_000 && report.p50_us <= 3_000);
        assert!(report.p99_us >= 4_000);
        assert!(!report.top_commands.is_empty());
        assert_eq!(report.top_commands[0].0, "GET");
    }

    #[test]
    fn test_percentile_single() {
        assert_eq!(percentile(&[100], 50.0), 100);
        assert_eq!(percentile(&[100], 99.0), 100);
    }

    #[test]
    fn test_percentile_empty() {
        assert_eq!(percentile(&[], 50.0), 0);
    }

    #[test]
    fn test_extract_pattern() {
        assert_eq!(extract_pattern("user:12345"), "user:*");
        assert_eq!(extract_pattern("session:abc:999"), "session:abc:*");
        assert_eq!(extract_pattern("simple"), "simple");
    }
}
