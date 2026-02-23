//! Prometheus metrics for cluster slot migration.
//!
//! Exposes counters, gauges, and histograms for tracking migration
//! progress and health at the metrics endpoint.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::RwLock;

/// Slot migration metrics collector.
///
/// These are designed to be registered with the `metrics` crate and
/// scraped by Prometheus. The struct also provides a self-contained
/// snapshot for internal use.
pub struct MigrationMetrics {
    // ── counters ─────────────────────────────────────────────────
    /// Total keys successfully migrated.
    keys_migrated: AtomicU64,
    /// Total keys that failed to migrate.
    keys_failed: AtomicU64,
    /// Total slot migrations completed.
    slots_completed: AtomicU64,
    /// Total slot migrations failed.
    slots_failed: AtomicU64,

    // ── gauges ───────────────────────────────────────────────────
    /// Number of slots currently in a transitional state
    /// (MIGRATING or IMPORTING).
    transitional_slots: AtomicU64,

    // ── histogram (approximation) ────────────────────────────────
    /// Recorded durations of completed slot migrations (in ms).
    /// In production, use the `metrics` crate histogram; here we
    /// store raw observations for summary statistics.
    duration_observations: RwLock<Vec<u64>>,
}

impl MigrationMetrics {
    /// Create a new metrics collector.
    pub fn new() -> Self {
        Self {
            keys_migrated: AtomicU64::new(0),
            keys_failed: AtomicU64::new(0),
            slots_completed: AtomicU64::new(0),
            slots_failed: AtomicU64::new(0),
            transitional_slots: AtomicU64::new(0),
            duration_observations: RwLock::new(Vec::new()),
        }
    }

    /// Register metrics with the `metrics` crate (call once at startup).
    pub fn register(&self) {
        // Describe metrics so they appear in /metrics output.
        metrics::describe_counter!(
            "ferrite_cluster_keys_migrated_total",
            "Total keys migrated between nodes"
        );
        metrics::describe_counter!(
            "ferrite_cluster_keys_migration_failed_total",
            "Total keys that failed to migrate"
        );
        metrics::describe_counter!(
            "ferrite_cluster_slot_migrations_completed_total",
            "Total slot migrations completed"
        );
        metrics::describe_counter!(
            "ferrite_cluster_slot_migrations_failed_total",
            "Total slot migrations that failed"
        );
        metrics::describe_gauge!(
            "ferrite_cluster_transitional_slots",
            "Number of slots in MIGRATING or IMPORTING state"
        );
        metrics::describe_histogram!(
            "ferrite_cluster_slot_migration_duration_ms",
            "Duration of slot migrations in milliseconds"
        );
    }

    // ── recording ────────────────────────────────────────────────

    /// Record successful key migrations.
    pub fn record_keys_migrated(&self, count: u64) {
        self.keys_migrated.fetch_add(count, Ordering::Relaxed);
        metrics::counter!("ferrite_cluster_keys_migrated_total").increment(count);
    }

    /// Record failed key migrations.
    pub fn record_keys_failed(&self, count: u64) {
        self.keys_failed.fetch_add(count, Ordering::Relaxed);
        metrics::counter!("ferrite_cluster_keys_migration_failed_total").increment(count);
    }

    /// Record a completed slot migration with its duration.
    pub fn record_slot_completed(&self, duration: Duration) {
        self.slots_completed.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("ferrite_cluster_slot_migrations_completed_total").increment(1);

        let ms = duration.as_millis() as f64;
        metrics::histogram!("ferrite_cluster_slot_migration_duration_ms").record(ms);
        self.duration_observations
            .write()
            .push(duration.as_millis() as u64);
    }

    /// Record a failed slot migration.
    pub fn record_slot_failed(&self) {
        self.slots_failed.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("ferrite_cluster_slot_migrations_failed_total").increment(1);
    }

    /// Update the gauge of transitional (migrating/importing) slots.
    pub fn set_transitional_slots(&self, count: u64) {
        self.transitional_slots.store(count, Ordering::Relaxed);
        metrics::gauge!("ferrite_cluster_transitional_slots").set(count as f64);
    }

    // ── snapshots ────────────────────────────────────────────────

    /// Snapshot of all counters.
    pub fn snapshot(&self) -> MigrationMetricsSnapshot {
        let observations = self.duration_observations.read();
        let (avg_ms, p50_ms, p99_ms) = if observations.is_empty() {
            (0.0, 0, 0)
        } else {
            let mut sorted = observations.clone();
            sorted.sort_unstable();
            let avg = sorted.iter().sum::<u64>() as f64 / sorted.len() as f64;
            let p50 = sorted[sorted.len() / 2];
            let p99_idx = ((sorted.len() as f64) * 0.99).ceil() as usize;
            let p99 = sorted[p99_idx.min(sorted.len() - 1)];
            (avg, p50, p99)
        };

        MigrationMetricsSnapshot {
            keys_migrated: self.keys_migrated.load(Ordering::Relaxed),
            keys_failed: self.keys_failed.load(Ordering::Relaxed),
            slots_completed: self.slots_completed.load(Ordering::Relaxed),
            slots_failed: self.slots_failed.load(Ordering::Relaxed),
            transitional_slots: self.transitional_slots.load(Ordering::Relaxed),
            avg_duration_ms: avg_ms,
            p50_duration_ms: p50_ms,
            p99_duration_ms: p99_ms,
        }
    }
}

impl Default for MigrationMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// A point-in-time snapshot of migration metrics.
#[derive(Debug, Clone)]
pub struct MigrationMetricsSnapshot {
    /// Total keys migrated.
    pub keys_migrated: u64,
    /// Total keys that failed to migrate.
    pub keys_failed: u64,
    /// Total slots completed.
    pub slots_completed: u64,
    /// Total slots failed.
    pub slots_failed: u64,
    /// Slots currently in transition.
    pub transitional_slots: u64,
    /// Average migration duration (ms).
    pub avg_duration_ms: f64,
    /// P50 migration duration (ms).
    pub p50_duration_ms: u64,
    /// P99 migration duration (ms).
    pub p99_duration_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_snapshot() {
        let m = MigrationMetrics::new();
        let snap = m.snapshot();
        assert_eq!(snap.keys_migrated, 0);
        assert_eq!(snap.keys_failed, 0);
        assert_eq!(snap.slots_completed, 0);
        assert_eq!(snap.slots_failed, 0);
        assert_eq!(snap.transitional_slots, 0);
    }

    #[test]
    fn test_record_keys() {
        let m = MigrationMetrics::new();
        m.record_keys_migrated(100);
        m.record_keys_migrated(50);
        m.record_keys_failed(3);

        let snap = m.snapshot();
        assert_eq!(snap.keys_migrated, 150);
        assert_eq!(snap.keys_failed, 3);
    }

    #[test]
    fn test_record_slot_completed() {
        let m = MigrationMetrics::new();
        m.record_slot_completed(Duration::from_millis(200));
        m.record_slot_completed(Duration::from_millis(400));

        let snap = m.snapshot();
        assert_eq!(snap.slots_completed, 2);
        assert!(snap.avg_duration_ms > 250.0);
        assert!(snap.p50_duration_ms >= 200);
    }

    #[test]
    fn test_record_slot_failed() {
        let m = MigrationMetrics::new();
        m.record_slot_failed();
        m.record_slot_failed();

        let snap = m.snapshot();
        assert_eq!(snap.slots_failed, 2);
    }

    #[test]
    fn test_transitional_slots_gauge() {
        let m = MigrationMetrics::new();
        m.set_transitional_slots(5);
        assert_eq!(m.snapshot().transitional_slots, 5);

        m.set_transitional_slots(0);
        assert_eq!(m.snapshot().transitional_slots, 0);
    }

    #[test]
    fn test_duration_percentiles() {
        let m = MigrationMetrics::new();
        for i in 1..=100 {
            m.record_slot_completed(Duration::from_millis(i * 10));
        }

        let snap = m.snapshot();
        assert_eq!(snap.slots_completed, 100);
        // p50 should be around 500ms (50th value)
        assert!(snap.p50_duration_ms >= 400 && snap.p50_duration_ms <= 600);
        // p99 should be around 990-1000ms
        assert!(snap.p99_duration_ms >= 900);
    }
}
