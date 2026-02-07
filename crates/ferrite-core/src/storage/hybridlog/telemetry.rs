//! Per-Tier Telemetry for HybridLog
//!
//! Records and exposes Prometheus-compatible metrics for each storage tier:
//! per-tier byte sizes, promotion/demotion rates, read/write latency histograms,
//! and cache hit rates.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Per-tier metrics container
#[derive(Debug)]
pub struct TierMetrics {
    /// Human-readable tier name
    pub name: &'static str,
    /// Current size in bytes
    pub size_bytes: AtomicU64,
    /// Total read operations
    pub read_ops: AtomicU64,
    /// Total write operations
    pub write_ops: AtomicU64,
    /// Cache/prefetch hits
    pub cache_hits: AtomicU64,
    /// Cache/prefetch misses
    pub cache_misses: AtomicU64,
    /// Cumulative read latency in microseconds (for computing avg)
    pub read_latency_us_sum: AtomicU64,
    /// Cumulative write latency in microseconds
    pub write_latency_us_sum: AtomicU64,
    /// Promotions into this tier (from a colder tier)
    pub promotions: AtomicU64,
    /// Demotions out of this tier (to a colder tier)
    pub demotions: AtomicU64,
}

impl TierMetrics {
    /// Create a new metrics container for the given tier
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            size_bytes: AtomicU64::new(0),
            read_ops: AtomicU64::new(0),
            write_ops: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            read_latency_us_sum: AtomicU64::new(0),
            write_latency_us_sum: AtomicU64::new(0),
            promotions: AtomicU64::new(0),
            demotions: AtomicU64::new(0),
        }
    }

    /// Record a read operation with measured latency
    pub fn record_read(&self, latency: std::time::Duration) {
        self.read_ops.fetch_add(1, Ordering::Relaxed);
        self.read_latency_us_sum
            .fetch_add(latency.as_micros() as u64, Ordering::Relaxed);
    }

    /// Record a write operation with measured latency
    pub fn record_write(&self, latency: std::time::Duration) {
        self.write_ops.fetch_add(1, Ordering::Relaxed);
        self.write_latency_us_sum
            .fetch_add(latency.as_micros() as u64, Ordering::Relaxed);
    }

    /// Record a cache hit
    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache miss
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a promotion into this tier
    pub fn record_promotion(&self) {
        self.promotions.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a demotion out of this tier
    pub fn record_demotion(&self) {
        self.demotions.fetch_add(1, Ordering::Relaxed);
    }

    /// Update the current size
    pub fn set_size(&self, bytes: u64) {
        self.size_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Get a snapshot of this tier's metrics
    pub fn snapshot(&self) -> TierSnapshot {
        let reads = self.read_ops.load(Ordering::Relaxed);
        let writes = self.write_ops.load(Ordering::Relaxed);
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total_cache = hits + misses;

        TierSnapshot {
            name: self.name,
            size_bytes: self.size_bytes.load(Ordering::Relaxed),
            read_ops: reads,
            write_ops: writes,
            cache_hit_rate: if total_cache > 0 {
                hits as f64 / total_cache as f64
            } else {
                0.0
            },
            avg_read_latency_us: if reads > 0 {
                self.read_latency_us_sum.load(Ordering::Relaxed) as f64 / reads as f64
            } else {
                0.0
            },
            avg_write_latency_us: if writes > 0 {
                self.write_latency_us_sum.load(Ordering::Relaxed) as f64 / writes as f64
            } else {
                0.0
            },
            promotions: self.promotions.load(Ordering::Relaxed),
            demotions: self.demotions.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of a single tier's metrics
#[derive(Debug, Clone)]
pub struct TierSnapshot {
    /// Tier name
    pub name: &'static str,
    /// Current size in bytes
    pub size_bytes: u64,
    /// Total read operations
    pub read_ops: u64,
    /// Total write operations
    pub write_ops: u64,
    /// Cache hit rate (0.0–1.0)
    pub cache_hit_rate: f64,
    /// Average read latency in microseconds
    pub avg_read_latency_us: f64,
    /// Average write latency in microseconds
    pub avg_write_latency_us: f64,
    /// Promotions into this tier
    pub promotions: u64,
    /// Demotions out of this tier
    pub demotions: u64,
}

/// Aggregated telemetry across all three tiers
#[derive(Debug)]
pub struct HybridLogTelemetry {
    /// Mutable (hot) tier metrics
    pub mutable: TierMetrics,
    /// ReadOnly (warm) tier metrics
    pub readonly: TierMetrics,
    /// Disk (cold) tier metrics
    pub disk: TierMetrics,
}

impl Default for HybridLogTelemetry {
    fn default() -> Self {
        Self::new()
    }
}

impl HybridLogTelemetry {
    /// Create a new telemetry instance
    pub fn new() -> Self {
        Self {
            mutable: TierMetrics::new("mutable"),
            readonly: TierMetrics::new("readonly"),
            disk: TierMetrics::new("disk"),
        }
    }

    /// Get snapshots for all tiers
    pub fn snapshot_all(&self) -> [TierSnapshot; 3] {
        [
            self.mutable.snapshot(),
            self.readonly.snapshot(),
            self.disk.snapshot(),
        ]
    }

    /// Register all metrics with the `metrics` crate for Prometheus export
    pub fn register_prometheus_metrics(&self) {
        let tiers = self.snapshot_all();
        for tier in &tiers {
            let tier_name = tier.name;
            metrics::gauge!("hybridlog_tier_size_bytes", "tier" => tier_name)
                .set(tier.size_bytes as f64);
            metrics::counter!("hybridlog_tier_read_ops_total", "tier" => tier_name)
                .absolute(tier.read_ops);
            metrics::counter!("hybridlog_tier_write_ops_total", "tier" => tier_name)
                .absolute(tier.write_ops);
            metrics::gauge!("hybridlog_tier_cache_hit_rate", "tier" => tier_name)
                .set(tier.cache_hit_rate);
            metrics::gauge!("hybridlog_tier_avg_read_latency_us", "tier" => tier_name)
                .set(tier.avg_read_latency_us);
            metrics::gauge!("hybridlog_tier_avg_write_latency_us", "tier" => tier_name)
                .set(tier.avg_write_latency_us);
            metrics::counter!("hybridlog_tier_promotions_total", "tier" => tier_name)
                .absolute(tier.promotions);
            metrics::counter!("hybridlog_tier_demotions_total", "tier" => tier_name)
                .absolute(tier.demotions);
        }
    }
}

/// RAII guard for timing operations
pub struct LatencyTimer {
    start: Instant,
}

impl LatencyTimer {
    /// Start a new timer
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// Finish and return elapsed duration
    pub fn finish(self) -> std::time::Duration {
        self.start.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_tier_metrics_read_write() {
        let m = TierMetrics::new("test");
        m.record_read(Duration::from_micros(100));
        m.record_read(Duration::from_micros(200));
        m.record_write(Duration::from_micros(50));

        let snap = m.snapshot();
        assert_eq!(snap.read_ops, 2);
        assert_eq!(snap.write_ops, 1);
        assert!((snap.avg_read_latency_us - 150.0).abs() < 0.01);
        assert!((snap.avg_write_latency_us - 50.0).abs() < 0.01);
    }

    #[test]
    fn test_tier_metrics_cache_hit_rate() {
        let m = TierMetrics::new("test");
        m.record_cache_hit();
        m.record_cache_hit();
        m.record_cache_hit();
        m.record_cache_miss();

        let snap = m.snapshot();
        assert!((snap.cache_hit_rate - 0.75).abs() < 0.01);
    }

    #[test]
    fn test_tier_metrics_cache_hit_rate_zero() {
        let m = TierMetrics::new("test");
        let snap = m.snapshot();
        assert!((snap.cache_hit_rate - 0.0).abs() < 0.01);
    }

    #[test]
    fn test_tier_metrics_promotions_demotions() {
        let m = TierMetrics::new("test");
        m.record_promotion();
        m.record_promotion();
        m.record_demotion();

        let snap = m.snapshot();
        assert_eq!(snap.promotions, 2);
        assert_eq!(snap.demotions, 1);
    }

    #[test]
    fn test_tier_metrics_set_size() {
        let m = TierMetrics::new("test");
        m.set_size(1024);
        assert_eq!(m.snapshot().size_bytes, 1024);
    }

    #[test]
    fn test_telemetry_snapshot_all() {
        let t = HybridLogTelemetry::new();
        t.mutable.set_size(100);
        t.readonly.set_size(200);
        t.disk.set_size(300);

        let snaps = t.snapshot_all();
        assert_eq!(snaps[0].name, "mutable");
        assert_eq!(snaps[0].size_bytes, 100);
        assert_eq!(snaps[1].name, "readonly");
        assert_eq!(snaps[1].size_bytes, 200);
        assert_eq!(snaps[2].name, "disk");
        assert_eq!(snaps[2].size_bytes, 300);
    }

    #[test]
    fn test_latency_timer() {
        let timer = LatencyTimer::start();
        std::thread::sleep(Duration::from_millis(10));
        let elapsed = timer.finish();
        assert!(elapsed >= Duration::from_millis(5));
    }

    #[test]
    fn test_avg_latency_zero_ops() {
        let m = TierMetrics::new("empty");
        let snap = m.snapshot();
        assert!((snap.avg_read_latency_us - 0.0).abs() < 0.01);
        assert!((snap.avg_write_latency_us - 0.0).abs() < 0.01);
    }
}
