//! Query Pattern Collector
//!
//! Collects and aggregates access patterns for analysis by the auto-indexing engine.
//! Tracks key patterns, access types, latencies, and field access frequencies.

use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Access type for a query
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AccessType {
    /// Read operation (GET, HGET, etc.)
    Read,
    /// Write operation (SET, HSET, etc.)
    Write,
    /// Scan operation (SCAN, HSCAN, etc.)
    Scan,
    /// Range query
    Range,
    /// Delete operation
    Delete,
}

impl AccessType {
    /// Whether this access type typically benefits from indexing
    pub fn benefits_from_index(&self) -> bool {
        matches!(
            self,
            AccessType::Read | AccessType::Scan | AccessType::Range
        )
    }

    /// Weight for this access type in recommendation scoring
    pub fn weight(&self) -> f64 {
        match self {
            AccessType::Read => 1.0,
            AccessType::Scan => 1.5,  // Scans benefit more from indexes
            AccessType::Range => 2.0, // Range queries benefit most
            AccessType::Write => 0.3, // Writes are penalized (index maintenance)
            AccessType::Delete => 0.2,
        }
    }
}

/// A recorded access pattern
#[derive(Clone, Debug)]
pub struct AccessPattern {
    /// The key pattern (e.g., "users:*", "orders:*:items")
    pub pattern: String,
    /// Type of access
    pub access_type: AccessType,
    /// Latency of the access
    pub latency: Duration,
    /// Fields accessed (for hash/json operations)
    pub fields: Vec<String>,
    /// Timestamp
    pub timestamp: Instant,
}

/// Aggregated query pattern statistics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryPattern {
    /// The key pattern
    pub pattern: String,
    /// Total access count
    pub access_count: u64,
    /// Read count
    pub read_count: u64,
    /// Write count
    pub write_count: u64,
    /// Scan count
    pub scan_count: u64,
    /// Range query count
    pub range_count: u64,
    /// Average latency in microseconds
    pub avg_latency_us: u64,
    /// P50 latency in microseconds
    pub p50_latency_us: u64,
    /// P99 latency in microseconds
    pub p99_latency_us: u64,
    /// Most frequently accessed fields
    pub hot_fields: Vec<(String, u64)>,
    /// First seen timestamp
    pub first_seen: u64,
    /// Last seen timestamp
    pub last_seen: u64,
    /// Access frequency (per minute)
    pub frequency_per_minute: f64,
}

impl QueryPattern {
    /// Calculate read/write ratio
    pub fn read_write_ratio(&self) -> f64 {
        if self.write_count == 0 {
            f64::INFINITY
        } else {
            self.read_count as f64 / self.write_count as f64
        }
    }

    /// Check if pattern is read-heavy (good for indexing)
    pub fn is_read_heavy(&self) -> bool {
        self.read_write_ratio() > 2.0
    }

    /// Calculate hotness score (for prioritization)
    pub fn hotness_score(&self) -> f64 {
        let recency_factor = 1.0; // Would decay based on last_seen
        let frequency_factor = self.frequency_per_minute.ln_1p();
        let latency_factor = (self.avg_latency_us as f64 / 1000.0).max(1.0); // Higher latency = more benefit

        recency_factor * frequency_factor * latency_factor
    }
}

/// Pattern collector for tracking access patterns
pub struct PatternCollector {
    /// Pattern statistics
    patterns: DashMap<String, PatternStats>,
    /// Recent accesses for latency percentile calculation
    recent_accesses: DashMap<String, RwLock<VecDeque<AccessRecord>>>,
    /// Field access counts per pattern
    field_counts: DashMap<String, DashMap<String, AtomicU64>>,
    /// Total accesses recorded
    total_accesses: AtomicU64,
    /// History retention in hours
    retention_hours: u64,
    /// Creation time
    created_at: Instant,
}

/// Internal statistics for a pattern
struct PatternStats {
    read_count: AtomicU64,
    write_count: AtomicU64,
    scan_count: AtomicU64,
    range_count: AtomicU64,
    delete_count: AtomicU64,
    total_latency_us: AtomicU64,
    first_seen: RwLock<u64>,
    last_seen: AtomicU64,
}

/// A single access record
#[derive(Clone, Debug)]
struct AccessRecord {
    access_type: AccessType,
    latency_us: u64,
    timestamp: Instant,
}

impl PatternCollector {
    /// Create a new pattern collector
    pub fn new(retention_hours: u64) -> Self {
        Self {
            patterns: DashMap::new(),
            recent_accesses: DashMap::new(),
            field_counts: DashMap::new(),
            total_accesses: AtomicU64::new(0),
            retention_hours,
            created_at: Instant::now(),
        }
    }

    /// Record an access
    pub fn record(
        &self,
        pattern: &str,
        access_type: AccessType,
        latency: Duration,
        fields: Option<&[String]>,
    ) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let latency_us = latency.as_micros() as u64;

        // Update or create pattern stats
        self.patterns
            .entry(pattern.to_string())
            .or_insert_with(|| PatternStats {
                read_count: AtomicU64::new(0),
                write_count: AtomicU64::new(0),
                scan_count: AtomicU64::new(0),
                range_count: AtomicU64::new(0),
                delete_count: AtomicU64::new(0),
                total_latency_us: AtomicU64::new(0),
                first_seen: RwLock::new(now),
                last_seen: AtomicU64::new(now),
            })
            .value()
            .update(access_type, latency_us, now);

        // Record for percentile calculation
        self.recent_accesses
            .entry(pattern.to_string())
            .or_insert_with(|| RwLock::new(VecDeque::with_capacity(1000)));

        if let Some(accesses) = self.recent_accesses.get(pattern) {
            let mut accesses = accesses.write();
            accesses.push_back(AccessRecord {
                access_type,
                latency_us,
                timestamp: Instant::now(),
            });

            // Keep only recent records (last 1000 or within retention period)
            while accesses.len() > 1000 {
                accesses.pop_front();
            }
        }

        // Track field access
        if let Some(fields) = fields {
            let field_map = self
                .field_counts
                .entry(pattern.to_string())
                .or_default();

            for field in fields {
                field_map
                    .entry(field.clone())
                    .or_insert_with(|| AtomicU64::new(0))
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        self.total_accesses.fetch_add(1, Ordering::Relaxed);
    }

    /// Get hot patterns sorted by hotness score
    pub fn get_hot_patterns(&self, limit: usize) -> Vec<QueryPattern> {
        let mut patterns: Vec<QueryPattern> = self
            .patterns
            .iter()
            .map(|entry| self.build_query_pattern(entry.key(), entry.value()))
            .collect();

        // Sort by hotness score (descending)
        patterns.sort_by(|a, b| {
            b.hotness_score()
                .partial_cmp(&a.hotness_score())
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        patterns.truncate(limit);
        patterns
    }

    /// Get pattern statistics
    pub fn get_pattern(&self, pattern: &str) -> Option<QueryPattern> {
        self.patterns
            .get(pattern)
            .map(|entry| self.build_query_pattern(pattern, entry.value()))
    }

    /// Get all patterns
    pub fn get_all_patterns(&self) -> Vec<QueryPattern> {
        self.patterns
            .iter()
            .map(|entry| self.build_query_pattern(entry.key(), entry.value()))
            .collect()
    }

    /// Get total access count
    pub fn total_accesses(&self) -> u64 {
        self.total_accesses.load(Ordering::Relaxed)
    }

    /// Get pattern count
    pub fn pattern_count(&self) -> usize {
        self.patterns.len()
    }

    /// Clear old data
    pub fn cleanup(&self) {
        let retention_secs = self.retention_hours * 3600;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let cutoff = now.saturating_sub(retention_secs);

        // Remove patterns that haven't been accessed within retention period
        self.patterns
            .retain(|_, stats| stats.last_seen.load(Ordering::Relaxed) > cutoff);

        // Clean up associated data
        self.recent_accesses
            .retain(|k, _| self.patterns.contains_key(k));
        self.field_counts
            .retain(|k, _| self.patterns.contains_key(k));
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.patterns.clear();
        self.recent_accesses.clear();
        self.field_counts.clear();
        self.total_accesses.store(0, Ordering::Relaxed);
    }

    /// Build a QueryPattern from internal stats
    fn build_query_pattern(&self, pattern: &str, stats: &PatternStats) -> QueryPattern {
        let read_count = stats.read_count.load(Ordering::Relaxed);
        let write_count = stats.write_count.load(Ordering::Relaxed);
        let scan_count = stats.scan_count.load(Ordering::Relaxed);
        let range_count = stats.range_count.load(Ordering::Relaxed);
        let delete_count = stats.delete_count.load(Ordering::Relaxed);
        let total_count = read_count + write_count + scan_count + range_count + delete_count;
        let total_latency = stats.total_latency_us.load(Ordering::Relaxed);

        let avg_latency = if total_count > 0 {
            total_latency / total_count
        } else {
            0
        };

        // Calculate percentiles from recent accesses
        let (p50, p99) = self.calculate_percentiles(pattern);

        // Get hot fields
        let hot_fields = self.get_hot_fields(pattern, 10);

        // Calculate frequency
        let first_seen = *stats.first_seen.read();
        let last_seen = stats.last_seen.load(Ordering::Relaxed);
        let duration_secs = (last_seen - first_seen).max(1);
        let frequency_per_minute = (total_count as f64 / duration_secs as f64) * 60.0;

        QueryPattern {
            pattern: pattern.to_string(),
            access_count: total_count,
            read_count,
            write_count,
            scan_count,
            range_count,
            avg_latency_us: avg_latency,
            p50_latency_us: p50,
            p99_latency_us: p99,
            hot_fields,
            first_seen,
            last_seen,
            frequency_per_minute,
        }
    }

    /// Calculate latency percentiles for a pattern
    fn calculate_percentiles(&self, pattern: &str) -> (u64, u64) {
        if let Some(accesses) = self.recent_accesses.get(pattern) {
            let accesses = accesses.read();
            if accesses.is_empty() {
                return (0, 0);
            }

            let mut latencies: Vec<u64> = accesses.iter().map(|r| r.latency_us).collect();
            latencies.sort_unstable();

            let p50_idx = (latencies.len() as f64 * 0.50) as usize;
            let p99_idx = (latencies.len() as f64 * 0.99) as usize;

            let p50 = latencies
                .get(p50_idx.min(latencies.len() - 1))
                .copied()
                .unwrap_or(0);
            let p99 = latencies
                .get(p99_idx.min(latencies.len() - 1))
                .copied()
                .unwrap_or(0);

            (p50, p99)
        } else {
            (0, 0)
        }
    }

    /// Get most frequently accessed fields for a pattern
    fn get_hot_fields(&self, pattern: &str, limit: usize) -> Vec<(String, u64)> {
        if let Some(field_map) = self.field_counts.get(pattern) {
            let mut fields: Vec<(String, u64)> = field_map
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().load(Ordering::Relaxed)))
                .collect();

            fields.sort_by(|a, b| b.1.cmp(&a.1));
            fields.truncate(limit);
            fields
        } else {
            Vec::new()
        }
    }
}

impl PatternStats {
    fn update(&self, access_type: AccessType, latency_us: u64, timestamp: u64) {
        match access_type {
            AccessType::Read => {
                self.read_count.fetch_add(1, Ordering::Relaxed);
            }
            AccessType::Write => {
                self.write_count.fetch_add(1, Ordering::Relaxed);
            }
            AccessType::Scan => {
                self.scan_count.fetch_add(1, Ordering::Relaxed);
            }
            AccessType::Range => {
                self.range_count.fetch_add(1, Ordering::Relaxed);
            }
            AccessType::Delete => {
                self.delete_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        self.total_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);
        self.last_seen.store(timestamp, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_access_type_weight() {
        assert!(AccessType::Range.weight() > AccessType::Read.weight());
        assert!(AccessType::Read.weight() > AccessType::Write.weight());
    }

    #[test]
    fn test_collector_record() {
        let collector = PatternCollector::new(24);

        collector.record(
            "users:*",
            AccessType::Read,
            Duration::from_micros(100),
            None,
        );
        collector.record(
            "users:*",
            AccessType::Read,
            Duration::from_micros(200),
            None,
        );
        collector.record(
            "users:*",
            AccessType::Write,
            Duration::from_micros(50),
            None,
        );

        assert_eq!(collector.total_accesses(), 3);
        assert_eq!(collector.pattern_count(), 1);

        let pattern = collector.get_pattern("users:*").unwrap();
        assert_eq!(pattern.access_count, 3);
        assert_eq!(pattern.read_count, 2);
        assert_eq!(pattern.write_count, 1);
    }

    #[test]
    fn test_collector_fields() {
        let collector = PatternCollector::new(24);

        let fields = vec!["name".to_string(), "email".to_string()];
        collector.record(
            "users:*",
            AccessType::Read,
            Duration::from_micros(100),
            Some(&fields),
        );
        collector.record(
            "users:*",
            AccessType::Read,
            Duration::from_micros(100),
            Some(&["name".to_string()]),
        );

        let pattern = collector.get_pattern("users:*").unwrap();
        assert!(!pattern.hot_fields.is_empty());

        // "name" should be the hottest field
        assert_eq!(pattern.hot_fields[0].0, "name");
        assert_eq!(pattern.hot_fields[0].1, 2);
    }

    #[test]
    fn test_hot_patterns() {
        let collector = PatternCollector::new(24);

        // Add many accesses to one pattern
        for _ in 0..100 {
            collector.record("hot:*", AccessType::Read, Duration::from_micros(500), None);
        }

        // Add few accesses to another
        for _ in 0..5 {
            collector.record("cold:*", AccessType::Read, Duration::from_micros(50), None);
        }

        let hot = collector.get_hot_patterns(10);
        assert_eq!(hot[0].pattern, "hot:*");
    }

    #[test]
    fn test_read_write_ratio() {
        let collector = PatternCollector::new(24);

        for _ in 0..80 {
            collector.record(
                "read_heavy:*",
                AccessType::Read,
                Duration::from_micros(100),
                None,
            );
        }
        for _ in 0..20 {
            collector.record(
                "read_heavy:*",
                AccessType::Write,
                Duration::from_micros(50),
                None,
            );
        }

        let pattern = collector.get_pattern("read_heavy:*").unwrap();
        assert!(pattern.is_read_heavy());
        assert_eq!(pattern.read_write_ratio(), 4.0);
    }

    #[test]
    fn test_reset() {
        let collector = PatternCollector::new(24);

        collector.record(
            "users:*",
            AccessType::Read,
            Duration::from_micros(100),
            None,
        );
        assert_eq!(collector.pattern_count(), 1);

        collector.reset();
        assert_eq!(collector.pattern_count(), 0);
        assert_eq!(collector.total_accesses(), 0);
    }
}
