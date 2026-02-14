//! Operator Metrics
//!
//! Provides observability metrics for the Ferrite Kubernetes Operator.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Metrics for the operator
pub struct OperatorMetrics {
    /// Total reconciliations
    pub reconciliations_total: AtomicU64,
    /// Successful reconciliations
    pub reconciliations_success: AtomicU64,
    /// Failed reconciliations
    pub reconciliations_failed: AtomicU64,
    /// Reconciliation duration histogram (buckets in ms)
    reconciliation_duration: RwLock<HistogramData>,
    /// Clusters managed
    pub clusters_total: AtomicU64,
    /// Ready clusters
    pub clusters_ready: AtomicU64,
    /// Per-cluster metrics
    cluster_metrics: RwLock<HashMap<String, ClusterMetrics>>,
    /// Queue depth
    pub queue_depth: AtomicU64,
    /// Leader status
    pub is_leader: AtomicU64,
}

impl OperatorMetrics {
    /// Create new metrics
    pub fn new() -> Self {
        Self {
            reconciliations_total: AtomicU64::new(0),
            reconciliations_success: AtomicU64::new(0),
            reconciliations_failed: AtomicU64::new(0),
            reconciliation_duration: RwLock::new(HistogramData::new()),
            clusters_total: AtomicU64::new(0),
            clusters_ready: AtomicU64::new(0),
            cluster_metrics: RwLock::new(HashMap::new()),
            queue_depth: AtomicU64::new(0),
            is_leader: AtomicU64::new(0),
        }
    }

    /// Record a successful reconciliation
    pub fn record_reconcile_success(&self, cluster: &str) {
        self.reconciliations_total.fetch_add(1, Ordering::Relaxed);
        self.reconciliations_success.fetch_add(1, Ordering::Relaxed);

        let mut metrics = self.cluster_metrics.write();
        let entry = metrics
            .entry(cluster.to_string())
            .or_default();
        entry.reconciliations_total.fetch_add(1, Ordering::Relaxed);
        entry
            .reconciliations_success
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed reconciliation
    pub fn record_reconcile_error(&self, cluster: &str) {
        self.reconciliations_total.fetch_add(1, Ordering::Relaxed);
        self.reconciliations_failed.fetch_add(1, Ordering::Relaxed);

        let mut metrics = self.cluster_metrics.write();
        let entry = metrics
            .entry(cluster.to_string())
            .or_default();
        entry.reconciliations_total.fetch_add(1, Ordering::Relaxed);
        entry.reconciliations_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record reconciliation duration
    pub fn record_reconcile_duration(&self, duration_ms: u64) {
        let mut histogram = self.reconciliation_duration.write();
        histogram.observe(duration_ms as f64);
    }

    /// Start a reconciliation timer
    pub fn start_reconcile_timer(&self) -> ReconcileTimer<'_> {
        ReconcileTimer {
            start: Instant::now(),
            metrics: self,
        }
    }

    /// Set leader status
    pub fn set_leader(&self, is_leader: bool) {
        self.is_leader
            .store(if is_leader { 1 } else { 0 }, Ordering::Release);
    }

    /// Update cluster counts
    pub fn update_cluster_counts(&self, total: u64, ready: u64) {
        self.clusters_total.store(total, Ordering::Release);
        self.clusters_ready.store(ready, Ordering::Release);
    }

    /// Update queue depth
    pub fn update_queue_depth(&self, depth: u64) {
        self.queue_depth.store(depth, Ordering::Release);
    }

    /// Get cluster metrics snapshot
    pub fn get_cluster_metrics(&self, cluster: &str) -> Option<ClusterMetricsSnapshot> {
        self.cluster_metrics
            .read()
            .get(cluster)
            .map(|m| m.snapshot())
    }

    /// Get all metrics as Prometheus format
    pub fn to_prometheus(&self) -> String {
        let mut output = String::new();

        // Total reconciliations
        output.push_str(
            "# HELP ferrite_operator_reconciliations_total Total number of reconciliations\n",
        );
        output.push_str("# TYPE ferrite_operator_reconciliations_total counter\n");
        output.push_str(&format!(
            "ferrite_operator_reconciliations_total {}\n",
            self.reconciliations_total.load(Ordering::Relaxed)
        ));

        // Successful reconciliations
        output.push_str(
            "# HELP ferrite_operator_reconciliations_success_total Successful reconciliations\n",
        );
        output.push_str("# TYPE ferrite_operator_reconciliations_success_total counter\n");
        output.push_str(&format!(
            "ferrite_operator_reconciliations_success_total {}\n",
            self.reconciliations_success.load(Ordering::Relaxed)
        ));

        // Failed reconciliations
        output.push_str(
            "# HELP ferrite_operator_reconciliations_failed_total Failed reconciliations\n",
        );
        output.push_str("# TYPE ferrite_operator_reconciliations_failed_total counter\n");
        output.push_str(&format!(
            "ferrite_operator_reconciliations_failed_total {}\n",
            self.reconciliations_failed.load(Ordering::Relaxed)
        ));

        // Cluster counts
        output.push_str("# HELP ferrite_operator_clusters_total Total clusters managed\n");
        output.push_str("# TYPE ferrite_operator_clusters_total gauge\n");
        output.push_str(&format!(
            "ferrite_operator_clusters_total {}\n",
            self.clusters_total.load(Ordering::Relaxed)
        ));

        output.push_str("# HELP ferrite_operator_clusters_ready Ready clusters\n");
        output.push_str("# TYPE ferrite_operator_clusters_ready gauge\n");
        output.push_str(&format!(
            "ferrite_operator_clusters_ready {}\n",
            self.clusters_ready.load(Ordering::Relaxed)
        ));

        // Queue depth
        output.push_str("# HELP ferrite_operator_queue_depth Reconciliation queue depth\n");
        output.push_str("# TYPE ferrite_operator_queue_depth gauge\n");
        output.push_str(&format!(
            "ferrite_operator_queue_depth {}\n",
            self.queue_depth.load(Ordering::Relaxed)
        ));

        // Leader status
        output.push_str("# HELP ferrite_operator_leader Is this instance the leader\n");
        output.push_str("# TYPE ferrite_operator_leader gauge\n");
        output.push_str(&format!(
            "ferrite_operator_leader {}\n",
            self.is_leader.load(Ordering::Relaxed)
        ));

        // Duration histogram
        let histogram = self.reconciliation_duration.read();
        output.push_str(
            "# HELP ferrite_operator_reconcile_duration_seconds Reconciliation duration\n",
        );
        output.push_str("# TYPE ferrite_operator_reconcile_duration_seconds histogram\n");
        for (bucket, count) in histogram.buckets.iter() {
            output.push_str(&format!(
                "ferrite_operator_reconcile_duration_seconds_bucket{{le=\"{}\"}} {}\n",
                bucket, count
            ));
        }
        output.push_str(&format!(
            "ferrite_operator_reconcile_duration_seconds_sum {}\n",
            histogram.sum
        ));
        output.push_str(&format!(
            "ferrite_operator_reconcile_duration_seconds_count {}\n",
            histogram.count
        ));

        // Per-cluster metrics
        output.push_str("\n# Per-cluster metrics\n");
        for (cluster, metrics) in self.cluster_metrics.read().iter() {
            output.push_str(&format!(
                "ferrite_operator_cluster_reconciliations_total{{cluster=\"{}\"}} {}\n",
                cluster,
                metrics.reconciliations_total.load(Ordering::Relaxed)
            ));
            output.push_str(&format!(
                "ferrite_operator_cluster_ready{{cluster=\"{}\"}} {}\n",
                cluster,
                metrics.is_ready.load(Ordering::Relaxed)
            ));
        }

        output
    }
}

impl Default for OperatorMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Timer for reconciliation duration
pub struct ReconcileTimer<'a> {
    start: Instant,
    metrics: &'a OperatorMetrics,
}

impl<'a> Drop for ReconcileTimer<'a> {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        self.metrics
            .record_reconcile_duration(duration.as_millis() as u64);
    }
}

/// Per-cluster metrics
pub struct ClusterMetrics {
    /// Total reconciliations for this cluster
    pub reconciliations_total: AtomicU64,
    /// Successful reconciliations
    pub reconciliations_success: AtomicU64,
    /// Failed reconciliations
    pub reconciliations_failed: AtomicU64,
    /// Cluster ready status
    pub is_ready: AtomicU64,
    /// Replica count
    pub replicas: AtomicU64,
    /// Ready replicas
    pub ready_replicas: AtomicU64,
}

/// Snapshot of cluster metrics (for reading)
#[derive(Debug, Clone)]
pub struct ClusterMetricsSnapshot {
    /// Total reconciliations for this cluster
    pub reconciliations_total: u64,
    /// Successful reconciliations
    pub reconciliations_success: u64,
    /// Failed reconciliations
    pub reconciliations_failed: u64,
    /// Cluster ready status
    pub is_ready: bool,
    /// Replica count
    pub replicas: u64,
    /// Ready replicas
    pub ready_replicas: u64,
}

impl ClusterMetrics {
    /// Create new cluster metrics
    pub fn new() -> Self {
        Self {
            reconciliations_total: AtomicU64::new(0),
            reconciliations_success: AtomicU64::new(0),
            reconciliations_failed: AtomicU64::new(0),
            is_ready: AtomicU64::new(0),
            replicas: AtomicU64::new(0),
            ready_replicas: AtomicU64::new(0),
        }
    }

    /// Update ready status
    pub fn set_ready(&self, ready: bool) {
        self.is_ready
            .store(if ready { 1 } else { 0 }, Ordering::Release);
    }

    /// Update replica counts
    pub fn update_replicas(&self, replicas: u64, ready: u64) {
        self.replicas.store(replicas, Ordering::Release);
        self.ready_replicas.store(ready, Ordering::Release);
    }

    /// Get a snapshot of the metrics
    pub fn snapshot(&self) -> ClusterMetricsSnapshot {
        ClusterMetricsSnapshot {
            reconciliations_total: self.reconciliations_total.load(Ordering::Relaxed),
            reconciliations_success: self.reconciliations_success.load(Ordering::Relaxed),
            reconciliations_failed: self.reconciliations_failed.load(Ordering::Relaxed),
            is_ready: self.is_ready.load(Ordering::Relaxed) != 0,
            replicas: self.replicas.load(Ordering::Relaxed),
            ready_replicas: self.ready_replicas.load(Ordering::Relaxed),
        }
    }
}

impl Default for ClusterMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple histogram data
struct HistogramData {
    /// Bucket boundaries and counts
    buckets: Vec<(f64, u64)>,
    /// Sum of all observations
    sum: f64,
    /// Count of observations
    count: u64,
}

impl HistogramData {
    fn new() -> Self {
        // Default buckets for milliseconds
        let boundaries = vec![
            1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 5000.0,
        ];
        Self {
            buckets: boundaries.into_iter().map(|b| (b, 0)).collect(),
            sum: 0.0,
            count: 0,
        }
    }

    fn observe(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;

        for (bucket, count) in &mut self.buckets {
            if value <= *bucket {
                *count += 1;
            }
        }
    }
}

/// Alerts configuration
#[derive(Debug, Clone)]
pub struct AlertConfig {
    /// Enable alerting
    pub enabled: bool,
    /// Alert on reconciliation failures
    pub on_reconcile_failure: bool,
    /// Threshold for reconciliation failure rate
    pub failure_rate_threshold: f64,
    /// Alert on queue depth
    pub on_high_queue_depth: bool,
    /// Queue depth threshold
    pub queue_depth_threshold: u64,
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            on_reconcile_failure: true,
            failure_rate_threshold: 0.1, // 10% failure rate
            on_high_queue_depth: true,
            queue_depth_threshold: 100,
        }
    }
}

/// Check alerts and return any that are firing
pub fn check_alerts(metrics: &OperatorMetrics, config: &AlertConfig) -> Vec<Alert> {
    let mut alerts = Vec::new();

    if !config.enabled {
        return alerts;
    }

    // Check failure rate
    if config.on_reconcile_failure {
        let total = metrics.reconciliations_total.load(Ordering::Relaxed) as f64;
        let failed = metrics.reconciliations_failed.load(Ordering::Relaxed) as f64;

        if total > 0.0 {
            let failure_rate = failed / total;
            if failure_rate > config.failure_rate_threshold {
                alerts.push(Alert {
                    name: "HighReconciliationFailureRate".to_string(),
                    severity: AlertSeverity::Warning,
                    message: format!(
                        "Reconciliation failure rate is {:.2}%, threshold is {:.2}%",
                        failure_rate * 100.0,
                        config.failure_rate_threshold * 100.0
                    ),
                });
            }
        }
    }

    // Check queue depth
    if config.on_high_queue_depth {
        let depth = metrics.queue_depth.load(Ordering::Relaxed);
        if depth > config.queue_depth_threshold {
            alerts.push(Alert {
                name: "HighQueueDepth".to_string(),
                severity: AlertSeverity::Warning,
                message: format!(
                    "Queue depth is {}, threshold is {}",
                    depth, config.queue_depth_threshold
                ),
            });
        }
    }

    alerts
}

/// Alert
#[derive(Debug, Clone)]
pub struct Alert {
    /// Alert name
    pub name: String,
    /// Severity
    pub severity: AlertSeverity,
    /// Message
    pub message: String,
}

/// Alert severity
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlertSeverity {
    /// Info
    Info,
    /// Warning
    Warning,
    /// Critical
    Critical,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = OperatorMetrics::new();
        assert_eq!(metrics.reconciliations_total.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_record_success() {
        let metrics = OperatorMetrics::new();

        metrics.record_reconcile_success("test-cluster");
        metrics.record_reconcile_success("test-cluster");

        assert_eq!(metrics.reconciliations_total.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.reconciliations_success.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_record_error() {
        let metrics = OperatorMetrics::new();

        metrics.record_reconcile_error("test-cluster");

        assert_eq!(metrics.reconciliations_total.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.reconciliations_failed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_prometheus_output() {
        let metrics = OperatorMetrics::new();
        metrics.record_reconcile_success("test");

        let output = metrics.to_prometheus();
        assert!(output.contains("ferrite_operator_reconciliations_total 1"));
    }

    #[test]
    fn test_alerts() {
        let metrics = OperatorMetrics::new();

        // Simulate 50% failure rate
        for _ in 0..5 {
            metrics.record_reconcile_success("test");
        }
        for _ in 0..5 {
            metrics.record_reconcile_error("test");
        }

        let config = AlertConfig {
            failure_rate_threshold: 0.3,
            ..Default::default()
        };

        let alerts = check_alerts(&metrics, &config);
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].name, "HighReconciliationFailureRate");
    }

    #[test]
    fn test_cluster_metrics() {
        let metrics = OperatorMetrics::new();

        metrics.record_reconcile_success("cluster-1");
        metrics.record_reconcile_success("cluster-1");
        metrics.record_reconcile_success("cluster-2");

        let cluster1 = metrics.get_cluster_metrics("cluster-1").unwrap();
        assert_eq!(cluster1.reconciliations_total, 2);

        let cluster2 = metrics.get_cluster_metrics("cluster-2").unwrap();
        assert_eq!(cluster2.reconciliations_total, 1);
    }

    #[test]
    fn test_leader_status() {
        let metrics = OperatorMetrics::new();

        assert_eq!(metrics.is_leader.load(Ordering::Relaxed), 0);

        metrics.set_leader(true);
        assert_eq!(metrics.is_leader.load(Ordering::Relaxed), 1);

        metrics.set_leader(false);
        assert_eq!(metrics.is_leader.load(Ordering::Relaxed), 0);
    }
}
