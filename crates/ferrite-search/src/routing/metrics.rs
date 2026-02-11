//! Routing metrics for adaptive routing

use super::RouteTarget;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Metrics collector for routing decisions
pub struct RoutingMetrics {
    /// Per-target metrics
    targets: DashMap<String, TargetMetrics>,
    /// Global metrics
    global: GlobalMetrics,
}

impl RoutingMetrics {
    /// Create new metrics collector
    pub fn new() -> Self {
        Self {
            targets: DashMap::new(),
            global: GlobalMetrics::default(),
        }
    }

    /// Record an execution
    pub fn record(&self, target: &RouteTarget, latency: Duration, success: bool) {
        let target_key = target_to_key(target);

        self.targets
            .entry(target_key)
            .or_default()
            .record(latency, success);

        self.global.record(latency, success);
    }

    /// Get target with least latency
    pub fn least_latency_target(&self) -> Option<RouteTarget> {
        self.targets
            .iter()
            .filter(|e| e.request_count.load(Ordering::Relaxed) > 10) // Need enough samples
            .min_by(|a, b| {
                let a_lat = a.avg_latency_ms();
                let b_lat = b.avg_latency_ms();
                a_lat
                    .partial_cmp(&b_lat)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|e| key_to_target(e.key()))
    }

    /// Get target with least connections
    pub fn least_connections_target(&self) -> Option<RouteTarget> {
        self.targets
            .iter()
            .min_by_key(|e| e.active_connections.load(Ordering::Relaxed))
            .map(|e| key_to_target(e.key()))
    }

    /// Get metrics snapshot
    pub fn snapshot(&self) -> MetricsSnapshot {
        let targets: Vec<_> = self
            .targets
            .iter()
            .map(|e| TargetSnapshot {
                target: e.key().clone(),
                request_count: e.request_count.load(Ordering::Relaxed),
                success_count: e.success_count.load(Ordering::Relaxed),
                error_count: e.error_count.load(Ordering::Relaxed),
                avg_latency_ms: e.avg_latency_ms(),
                p99_latency_ms: e.p99_latency_ms(),
                active_connections: e.active_connections.load(Ordering::Relaxed),
            })
            .collect();

        MetricsSnapshot {
            targets,
            total_requests: self.global.total_requests.load(Ordering::Relaxed),
            total_errors: self.global.total_errors.load(Ordering::Relaxed),
            avg_latency_ms: self.global.avg_latency_ms(),
        }
    }

    /// Get metrics for a specific target
    pub fn get_target_metrics(&self, target: &RouteTarget) -> Option<TargetSnapshot> {
        let key = target_to_key(target);
        self.targets.get(&key).map(|e| TargetSnapshot {
            target: key,
            request_count: e.request_count.load(Ordering::Relaxed),
            success_count: e.success_count.load(Ordering::Relaxed),
            error_count: e.error_count.load(Ordering::Relaxed),
            avg_latency_ms: e.avg_latency_ms(),
            p99_latency_ms: e.p99_latency_ms(),
            active_connections: e.active_connections.load(Ordering::Relaxed),
        })
    }

    /// Increment active connections for a target
    pub fn inc_connections(&self, target: &RouteTarget) {
        let key = target_to_key(target);
        self.targets
            .entry(key)
            .or_default()
            .active_connections
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement active connections for a target
    pub fn dec_connections(&self, target: &RouteTarget) {
        let key = target_to_key(target);
        if let Some(metrics) = self.targets.get(&key) {
            metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.targets.clear();
        self.global.reset();
    }
}

impl Default for RoutingMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Metrics for a single routing target
pub struct TargetMetrics {
    /// Total requests
    pub request_count: AtomicU64,
    /// Successful requests
    pub success_count: AtomicU64,
    /// Failed requests
    pub error_count: AtomicU64,
    /// Total latency in microseconds (for averaging)
    total_latency_us: AtomicU64,
    /// Max latency seen
    max_latency_us: AtomicU64,
    /// Active connections
    pub active_connections: AtomicU64,
    /// Last updated
    last_updated: parking_lot::RwLock<Instant>,
    /// Recent latencies for percentile calculation
    recent_latencies: parking_lot::RwLock<Vec<u64>>,
}

impl TargetMetrics {
    /// Create new target metrics
    pub fn new() -> Self {
        Self {
            request_count: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
            max_latency_us: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            last_updated: parking_lot::RwLock::new(Instant::now()),
            recent_latencies: parking_lot::RwLock::new(Vec::with_capacity(1000)),
        }
    }

    /// Record an execution
    pub fn record(&self, latency: Duration, success: bool) {
        let latency_us = latency.as_micros() as u64;

        self.request_count.fetch_add(1, Ordering::Relaxed);
        self.total_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);

        if success {
            self.success_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.error_count.fetch_add(1, Ordering::Relaxed);
        }

        // Update max latency
        let mut max = self.max_latency_us.load(Ordering::Relaxed);
        while latency_us > max {
            match self.max_latency_us.compare_exchange_weak(
                max,
                latency_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => max = current,
            }
        }

        // Track recent latencies for percentiles
        let mut recent = self.recent_latencies.write();
        if recent.len() >= 1000 {
            recent.remove(0);
        }
        recent.push(latency_us);

        *self.last_updated.write() = Instant::now();
    }

    /// Get average latency in milliseconds
    pub fn avg_latency_ms(&self) -> f64 {
        let count = self.request_count.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }
        let total_us = self.total_latency_us.load(Ordering::Relaxed);
        (total_us as f64 / count as f64) / 1000.0
    }

    /// Get P99 latency in milliseconds
    pub fn p99_latency_ms(&self) -> f64 {
        let recent = self.recent_latencies.read();
        if recent.is_empty() {
            return 0.0;
        }

        let mut sorted: Vec<_> = recent.clone();
        sorted.sort_unstable();

        let idx = (sorted.len() as f64 * 0.99) as usize;
        let idx = idx.min(sorted.len() - 1);
        sorted[idx] as f64 / 1000.0
    }

    /// Get success rate
    pub fn success_rate(&self) -> f64 {
        let total = self.request_count.load(Ordering::Relaxed);
        if total == 0 {
            return 1.0;
        }
        let success = self.success_count.load(Ordering::Relaxed);
        success as f64 / total as f64
    }
}

impl Default for TargetMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Global metrics
#[derive(Default)]
struct GlobalMetrics {
    total_requests: AtomicU64,
    total_errors: AtomicU64,
    total_latency_us: AtomicU64,
}

impl GlobalMetrics {
    fn record(&self, latency: Duration, success: bool) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.total_latency_us
            .fetch_add(latency.as_micros() as u64, Ordering::Relaxed);

        if !success {
            self.total_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn avg_latency_ms(&self) -> f64 {
        let count = self.total_requests.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }
        let total_us = self.total_latency_us.load(Ordering::Relaxed);
        (total_us as f64 / count as f64) / 1000.0
    }

    fn reset(&self) {
        self.total_requests.store(0, Ordering::Relaxed);
        self.total_errors.store(0, Ordering::Relaxed);
        self.total_latency_us.store(0, Ordering::Relaxed);
    }
}

/// Snapshot of metrics
#[derive(Clone, Debug)]
pub struct MetricsSnapshot {
    /// Per-target snapshots
    pub targets: Vec<TargetSnapshot>,
    /// Total requests
    pub total_requests: u64,
    /// Total errors
    pub total_errors: u64,
    /// Global average latency
    pub avg_latency_ms: f64,
}

impl MetricsSnapshot {
    /// Get the slowest target
    pub fn slowest_target(&self) -> Option<&TargetSnapshot> {
        self.targets
            .iter()
            .filter(|t| t.request_count > 10)
            .max_by(|a, b| {
                a.avg_latency_ms
                    .partial_cmp(&b.avg_latency_ms)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    }

    /// Get the fastest target
    pub fn fastest_target(&self) -> Option<&TargetSnapshot> {
        self.targets
            .iter()
            .filter(|t| t.request_count > 10)
            .min_by(|a, b| {
                a.avg_latency_ms
                    .partial_cmp(&b.avg_latency_ms)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    }
}

/// Snapshot of a single target's metrics
#[derive(Clone, Debug)]
pub struct TargetSnapshot {
    /// Target identifier
    pub target: String,
    /// Request count
    pub request_count: u64,
    /// Success count
    pub success_count: u64,
    /// Error count
    pub error_count: u64,
    /// Average latency (ms)
    pub avg_latency_ms: f64,
    /// P99 latency (ms)
    pub p99_latency_ms: f64,
    /// Active connections
    pub active_connections: u64,
}

impl TargetSnapshot {
    /// Get success rate
    pub fn success_rate(&self) -> f64 {
        if self.request_count == 0 {
            return 1.0;
        }
        self.success_count as f64 / self.request_count as f64
    }
}

// Helper functions

fn target_to_key(target: &RouteTarget) -> String {
    match target {
        RouteTarget::Primary => "primary".to_string(),
        RouteTarget::AnyReplica => "replica:any".to_string(),
        RouteTarget::SpecificReplica(id) => format!("replica:{}", id),
        RouteTarget::SpecificNode(id) => format!("node:{}", id),
        RouteTarget::LocalPreferred => "local".to_string(),
        RouteTarget::ConsistentHash => "consistent".to_string(),
        RouteTarget::Broadcast => "broadcast".to_string(),
    }
}

fn key_to_target(key: &str) -> RouteTarget {
    if key == "primary" {
        RouteTarget::Primary
    } else if key == "replica:any" {
        RouteTarget::AnyReplica
    } else if key == "local" {
        RouteTarget::LocalPreferred
    } else if key == "consistent" {
        RouteTarget::ConsistentHash
    } else if key == "broadcast" {
        RouteTarget::Broadcast
    } else if let Some(id) = key.strip_prefix("replica:") {
        RouteTarget::SpecificReplica(id.to_string())
    } else if let Some(id) = key.strip_prefix("node:") {
        RouteTarget::SpecificNode(id.to_string())
    } else {
        RouteTarget::Primary
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_recording() {
        let metrics = RoutingMetrics::new();

        metrics.record(&RouteTarget::Primary, Duration::from_millis(10), true);
        metrics.record(&RouteTarget::Primary, Duration::from_millis(20), true);
        metrics.record(&RouteTarget::Primary, Duration::from_millis(30), false);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.total_requests, 3);
        assert_eq!(snapshot.total_errors, 1);
    }

    #[test]
    fn test_target_metrics() {
        let tm = TargetMetrics::new();

        for i in 0..100 {
            tm.record(Duration::from_millis(i), true);
        }

        assert_eq!(tm.request_count.load(Ordering::Relaxed), 100);
        assert!(tm.avg_latency_ms() > 0.0);
        assert!(tm.p99_latency_ms() >= tm.avg_latency_ms());
    }

    #[test]
    fn test_least_latency_selection() {
        let metrics = RoutingMetrics::new();

        // Add samples for primary (slow)
        for _ in 0..20 {
            metrics.record(&RouteTarget::Primary, Duration::from_millis(100), true);
        }

        // Add samples for replica (fast)
        for _ in 0..20 {
            metrics.record(&RouteTarget::AnyReplica, Duration::from_millis(10), true);
        }

        let best = metrics.least_latency_target();
        assert_eq!(best, Some(RouteTarget::AnyReplica));
    }
}
