//! Per-function WASM observability
//!
//! Tracks execution metrics for each WASM function: call counts,
//! execution time histograms, memory usage, and error rates.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Per-function execution metrics
pub struct FunctionMetrics {
    /// Function name
    name: String,
    /// Total call count
    call_count: AtomicU64,
    /// Successful call count
    success_count: AtomicU64,
    /// Error count
    error_count: AtomicU64,
    /// Histogram buckets for execution time (microseconds)
    histogram: RwLock<ExecutionHistogram>,
    /// Current memory usage in bytes
    memory_usage: AtomicU64,
    /// Peak memory usage in bytes
    memory_peak: AtomicU64,
    /// Total fuel consumed
    total_fuel: AtomicU64,
}

impl FunctionMetrics {
    /// Create metrics for a function
    pub fn new(name: String) -> Self {
        Self {
            name,
            call_count: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            histogram: RwLock::new(ExecutionHistogram::new()),
            memory_usage: AtomicU64::new(0),
            memory_peak: AtomicU64::new(0),
            total_fuel: AtomicU64::new(0),
        }
    }

    /// Record a successful execution
    pub fn record_success(&self, duration: Duration, memory_bytes: u64, fuel: u64) {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        self.success_count.fetch_add(1, Ordering::Relaxed);
        self.total_fuel.fetch_add(fuel, Ordering::Relaxed);
        self.histogram.write().record(duration);
        self.update_memory(memory_bytes);
    }

    /// Record a failed execution
    pub fn record_error(&self, duration: Duration) {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        self.error_count.fetch_add(1, Ordering::Relaxed);
        self.histogram.write().record(duration);
    }

    /// Update current memory usage
    pub fn update_memory(&self, bytes: u64) {
        self.memory_usage.store(bytes, Ordering::Relaxed);
        let mut peak = self.memory_peak.load(Ordering::Relaxed);
        while bytes > peak {
            match self.memory_peak.compare_exchange_weak(
                peak,
                bytes,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => peak = actual,
            }
        }
    }

    /// Get a snapshot of the current metrics
    pub fn snapshot(&self) -> FunctionMetricsSnapshot {
        let histogram = self.histogram.read();
        let total = self.call_count.load(Ordering::Relaxed);
        let errors = self.error_count.load(Ordering::Relaxed);

        FunctionMetricsSnapshot {
            name: self.name.clone(),
            call_count: total,
            success_count: self.success_count.load(Ordering::Relaxed),
            error_count: errors,
            error_rate: if total > 0 {
                errors as f64 / total as f64
            } else {
                0.0
            },
            avg_duration: histogram.mean(),
            p50_duration: histogram.percentile(50.0),
            p95_duration: histogram.percentile(95.0),
            p99_duration: histogram.percentile(99.0),
            max_duration: histogram.max(),
            memory_usage: self.memory_usage.load(Ordering::Relaxed),
            memory_peak: self.memory_peak.load(Ordering::Relaxed),
            total_fuel: self.total_fuel.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of function metrics at a point in time
#[derive(Clone, Debug)]
pub struct FunctionMetricsSnapshot {
    /// Function name
    pub name: String,
    /// Total call count
    pub call_count: u64,
    /// Successful calls
    pub success_count: u64,
    /// Error count
    pub error_count: u64,
    /// Error rate (0.0 to 1.0)
    pub error_rate: f64,
    /// Average execution time
    pub avg_duration: Duration,
    /// P50 (median) execution time
    pub p50_duration: Duration,
    /// P95 execution time
    pub p95_duration: Duration,
    /// P99 execution time
    pub p99_duration: Duration,
    /// Maximum execution time
    pub max_duration: Duration,
    /// Current memory usage in bytes
    pub memory_usage: u64,
    /// Peak memory usage in bytes
    pub memory_peak: u64,
    /// Total fuel consumed
    pub total_fuel: u64,
}

/// Simple execution time histogram using sorted samples with capping
struct ExecutionHistogram {
    /// Sorted sample durations (microseconds)
    samples: Vec<u64>,
    /// Maximum samples to retain
    max_samples: usize,
    /// Running sum for mean calculation
    sum_us: u64,
    /// Total count (may exceed samples retained)
    total_count: u64,
    /// Maximum value seen
    max_us: u64,
}

impl ExecutionHistogram {
    fn new() -> Self {
        Self {
            samples: Vec::with_capacity(1024),
            max_samples: 10_000,
            sum_us: 0,
            total_count: 0,
            max_us: 0,
        }
    }

    fn record(&mut self, duration: Duration) {
        let us = duration.as_micros() as u64;
        self.sum_us = self.sum_us.saturating_add(us);
        self.total_count += 1;
        if us > self.max_us {
            self.max_us = us;
        }

        if self.samples.len() < self.max_samples {
            // Insert in sorted position
            let pos = self.samples.partition_point(|&x| x <= us);
            self.samples.insert(pos, us);
        } else {
            // Evict the oldest by keeping a rolling window (downsample)
            // Replace a random position to maintain approximate distribution
            let idx = (self.total_count as usize) % self.max_samples;
            self.samples[idx] = us;
            self.samples.sort_unstable();
        }
    }

    fn mean(&self) -> Duration {
        if self.total_count == 0 {
            return Duration::ZERO;
        }
        Duration::from_micros(self.sum_us / self.total_count)
    }

    fn percentile(&self, pct: f64) -> Duration {
        if self.samples.is_empty() {
            return Duration::ZERO;
        }
        let idx = ((pct / 100.0) * (self.samples.len() - 1) as f64).round() as usize;
        let idx = idx.min(self.samples.len() - 1);
        Duration::from_micros(self.samples[idx])
    }

    fn max(&self) -> Duration {
        Duration::from_micros(self.max_us)
    }
}

/// Registry of per-function metrics
pub struct WasmMetricsRegistry {
    metrics: RwLock<HashMap<String, Arc<FunctionMetrics>>>,
}

impl WasmMetricsRegistry {
    /// Create a new metrics registry
    pub fn new() -> Self {
        Self {
            metrics: RwLock::new(HashMap::new()),
        }
    }

    /// Get or create metrics for a function
    pub fn get_or_create(&self, name: &str) -> Arc<FunctionMetrics> {
        // Fast path: read lock
        {
            let metrics = self.metrics.read();
            if let Some(m) = metrics.get(name) {
                return Arc::clone(m);
            }
        }
        // Slow path: write lock
        let mut metrics = self.metrics.write();
        metrics
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(FunctionMetrics::new(name.to_string())))
            .clone()
    }

    /// Remove metrics for a function
    pub fn remove(&self, name: &str) {
        self.metrics.write().remove(name);
    }

    /// Get snapshots of all function metrics
    pub fn all_snapshots(&self) -> Vec<FunctionMetricsSnapshot> {
        let metrics = self.metrics.read();
        metrics.values().map(|m| m.snapshot()).collect()
    }

    /// Get a snapshot for a specific function
    pub fn snapshot(&self, name: &str) -> Option<FunctionMetricsSnapshot> {
        let metrics = self.metrics.read();
        metrics.get(name).map(|m| m.snapshot())
    }

    /// Get the number of tracked functions
    pub fn len(&self) -> usize {
        self.metrics.read().len()
    }

    /// Check if no functions are being tracked
    pub fn is_empty(&self) -> bool {
        self.metrics.read().is_empty()
    }
}

impl Default for WasmMetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_function_metrics_success() {
        let metrics = FunctionMetrics::new("test_fn".to_string());

        metrics.record_success(Duration::from_millis(10), 1024, 500);
        metrics.record_success(Duration::from_millis(20), 2048, 600);
        metrics.record_success(Duration::from_millis(15), 1536, 550);

        let snap = metrics.snapshot();
        assert_eq!(snap.name, "test_fn");
        assert_eq!(snap.call_count, 3);
        assert_eq!(snap.success_count, 3);
        assert_eq!(snap.error_count, 0);
        assert_eq!(snap.error_rate, 0.0);
        assert!(snap.avg_duration > Duration::ZERO);
        assert_eq!(snap.memory_peak, 2048);
        assert_eq!(snap.total_fuel, 1650);
    }

    #[test]
    fn test_function_metrics_errors() {
        let metrics = FunctionMetrics::new("failing_fn".to_string());

        metrics.record_success(Duration::from_millis(5), 512, 100);
        metrics.record_error(Duration::from_millis(1));
        metrics.record_error(Duration::from_millis(2));

        let snap = metrics.snapshot();
        assert_eq!(snap.call_count, 3);
        assert_eq!(snap.error_count, 2);
        assert!((snap.error_rate - 2.0 / 3.0).abs() < 0.001);
    }

    #[test]
    fn test_histogram_percentiles() {
        let mut hist = ExecutionHistogram::new();

        for i in 1..=100 {
            hist.record(Duration::from_micros(i));
        }

        assert_eq!(hist.mean(), Duration::from_micros(50)); // avg of 1..=100 = 50.5, truncated
        assert!(hist.percentile(50.0) >= Duration::from_micros(49));
        assert!(hist.percentile(50.0) <= Duration::from_micros(51));
        assert!(hist.percentile(99.0) >= Duration::from_micros(98));
        assert_eq!(hist.max(), Duration::from_micros(100));
    }

    #[test]
    fn test_histogram_empty() {
        let hist = ExecutionHistogram::new();

        assert_eq!(hist.mean(), Duration::ZERO);
        assert_eq!(hist.percentile(50.0), Duration::ZERO);
        assert_eq!(hist.max(), Duration::ZERO);
    }

    #[test]
    fn test_metrics_registry() {
        let registry = WasmMetricsRegistry::new();
        assert!(registry.is_empty());

        let m1 = registry.get_or_create("func_a");
        m1.record_success(Duration::from_millis(10), 1024, 500);

        let m2 = registry.get_or_create("func_b");
        m2.record_error(Duration::from_millis(1));

        assert_eq!(registry.len(), 2);

        let snap_a = registry.snapshot("func_a").unwrap();
        assert_eq!(snap_a.call_count, 1);
        assert_eq!(snap_a.success_count, 1);

        let snap_b = registry.snapshot("func_b").unwrap();
        assert_eq!(snap_b.error_count, 1);

        let all = registry.all_snapshots();
        assert_eq!(all.len(), 2);

        registry.remove("func_a");
        assert_eq!(registry.len(), 1);
        assert!(registry.snapshot("func_a").is_none());
    }

    #[test]
    fn test_metrics_registry_dedup() {
        let registry = WasmMetricsRegistry::new();

        let m1 = registry.get_or_create("same_fn");
        let m2 = registry.get_or_create("same_fn");

        m1.record_success(Duration::from_millis(10), 1024, 100);

        // m2 should be the same Arc
        let snap = m2.snapshot();
        assert_eq!(snap.call_count, 1);
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_memory_peak_tracking() {
        let metrics = FunctionMetrics::new("mem_test".to_string());

        metrics.update_memory(1000);
        metrics.update_memory(5000);
        metrics.update_memory(3000);

        let snap = metrics.snapshot();
        assert_eq!(snap.memory_usage, 3000);
        assert_eq!(snap.memory_peak, 5000);
    }
}
