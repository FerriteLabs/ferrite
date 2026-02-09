//! Anomaly Detection with Alerting
//!
//! Statistical anomaly detection using sliding window statistics and Z-score
//! analysis. Tracks multiple metric streams and generates alerts when values
//! deviate significantly from the baseline.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant, SystemTime};
use uuid::Uuid;

/// Type alias for alert callbacks.
pub type AlertCallback = Box<dyn Fn(&Alert) + Send + Sync>;

/// Metric name identifying a tracked metric stream.
#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum MetricName {
    /// Per-command latency (parameter is the command name).
    Latency(String),
    /// Overall throughput (ops/sec).
    Throughput,
    /// Error rate (errors/sec).
    ErrorRate,
    /// Memory usage in bytes.
    MemoryUsage,
    /// Number of active connections.
    ConnectionCount,
    /// Hit rate for a storage tier.
    TierHitRate(String),
    /// User-defined metric.
    Custom(String),
}

impl std::fmt::Display for MetricName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetricName::Latency(cmd) => write!(f, "latency:{}", cmd),
            MetricName::Throughput => write!(f, "throughput"),
            MetricName::ErrorRate => write!(f, "error_rate"),
            MetricName::MemoryUsage => write!(f, "memory_usage"),
            MetricName::ConnectionCount => write!(f, "connection_count"),
            MetricName::TierHitRate(tier) => write!(f, "tier_hit_rate:{}", tier),
            MetricName::Custom(name) => write!(f, "custom:{}", name),
        }
    }
}

/// Severity level for an alert.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum AlertSeverity {
    /// Informational – minor deviation.
    Info,
    /// Warning – notable deviation.
    Warning,
    /// Critical – severe deviation.
    Critical,
}

/// An alert raised when an anomaly is detected.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Alert {
    /// Unique alert identifier.
    pub id: String,
    /// When the alert was generated.
    pub timestamp: SystemTime,
    /// String representation of the metric name.
    pub metric: String,
    /// Alert severity.
    pub severity: AlertSeverity,
    /// Human-readable description.
    pub message: String,
    /// The observed value that triggered the alert.
    pub current_value: f64,
    /// Expected value range (mean ± threshold * std_dev).
    pub expected_range: (f64, f64),
    /// Z-score of the observed value.
    pub z_score: f64,
}

/// Baseline statistics for a metric.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BaselineStats {
    /// Current mean.
    pub mean: f64,
    /// Current standard deviation.
    pub std_dev: f64,
    /// Minimum observed value.
    pub min: f64,
    /// Maximum observed value.
    pub max: f64,
    /// Number of samples in the window.
    pub count: u64,
}

/// Configuration for the anomaly detector.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AnomalyConfig {
    /// Whether anomaly detection is enabled.
    pub enabled: bool,
    /// Z-score threshold to trigger an alert.
    pub z_score_threshold: f64,
    /// Sliding window duration in seconds.
    pub window_duration_secs: u64,
    /// Minimum number of samples before detection starts.
    pub min_samples: usize,
    /// Seconds to suppress duplicate alerts for the same metric.
    pub alert_suppression_secs: u64,
    /// Maximum number of alerts to retain.
    pub max_alerts: usize,
}

impl Default for AnomalyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            z_score_threshold: 3.0,
            window_duration_secs: 300,
            min_samples: 30,
            alert_suppression_secs: 60,
            max_alerts: 1000,
        }
    }
}

/// Sliding window that maintains online statistics over a time-bounded window.
pub struct SlidingWindow {
    samples: VecDeque<(Instant, f64)>,
    window_size: Duration,
    sum: f64,
    sum_sq: f64,
    count: u64,
    min: f64,
    max: f64,
}

impl SlidingWindow {
    /// Create a new sliding window with the given duration.
    pub fn new(window_size: Duration) -> Self {
        Self {
            samples: VecDeque::new(),
            window_size,
            sum: 0.0,
            sum_sq: 0.0,
            count: 0,
            min: f64::MAX,
            max: f64::MIN,
        }
    }

    /// Add a value and evict samples outside the window.
    pub fn add(&mut self, value: f64, timestamp: Instant) {
        self.evict_old(timestamp);
        self.samples.push_back((timestamp, value));
        self.sum += value;
        self.sum_sq += value * value;
        self.count += 1;
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
    }

    /// Mean of the current window.
    pub fn mean(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        self.sum / self.count as f64
    }

    /// Standard deviation of the current window.
    pub fn std_dev(&self) -> f64 {
        if self.count < 2 {
            return 0.0;
        }
        let n = self.count as f64;
        let variance = (self.sum_sq - (self.sum * self.sum) / n) / (n - 1.0);
        if variance < 0.0 {
            // Guard against floating-point rounding
            return 0.0;
        }
        variance.sqrt()
    }

    /// Approximate percentile via sorting the current samples.
    pub fn percentile(&self, p: f64) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let mut sorted: Vec<f64> = self.samples.iter().map(|(_, v)| *v).collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }

    /// Compute the Z-score of a given value relative to the window.
    pub fn z_score(&self, value: f64) -> f64 {
        let sd = self.std_dev();
        if sd == 0.0 {
            // When all samples are identical, any different value is infinitely far
            let diff = (value - self.mean()).abs();
            if diff > f64::EPSILON {
                return if value > self.mean() {
                    f64::MAX
                } else {
                    f64::MIN
                };
            }
            return 0.0;
        }
        (value - self.mean()) / sd
    }

    /// Check whether a value is anomalous given a Z-score threshold.
    pub fn is_anomaly(&self, value: f64, threshold: f64) -> bool {
        if self.count < 2 {
            return false;
        }
        self.z_score(value).abs() > threshold
    }

    /// Evict samples older than the window boundary.
    fn evict_old(&mut self, now: Instant) {
        while let Some(&(ts, val)) = self.samples.front() {
            if now.duration_since(ts) > self.window_size {
                self.samples.pop_front();
                self.sum -= val;
                self.sum_sq -= val * val;
                self.count -= 1;
            } else {
                break;
            }
        }
        // Recompute min/max after eviction
        if self.samples.is_empty() {
            self.min = f64::MAX;
            self.max = f64::MIN;
        } else {
            self.min = self
                .samples
                .iter()
                .map(|(_, v)| *v)
                .fold(f64::MAX, f64::min);
            self.max = self
                .samples
                .iter()
                .map(|(_, v)| *v)
                .fold(f64::MIN, f64::max);
        }
    }
}

/// Statistical anomaly detector with alerting.
pub struct AnomalyDetector {
    windows: RwLock<HashMap<MetricName, SlidingWindow>>,
    alerts: RwLock<VecDeque<Alert>>,
    config: AnomalyConfig,
    alert_callbacks: RwLock<Vec<AlertCallback>>,
    suppression_until: RwLock<HashMap<MetricName, Instant>>,
}

impl AnomalyDetector {
    /// Create a new anomaly detector with the given configuration.
    pub fn new(config: AnomalyConfig) -> Self {
        Self {
            windows: RwLock::new(HashMap::new()),
            alerts: RwLock::new(VecDeque::new()),
            config,
            alert_callbacks: RwLock::new(Vec::new()),
            suppression_until: RwLock::new(HashMap::new()),
        }
    }

    /// Record a metric observation.
    pub fn record_metric(&self, name: MetricName, value: f64, timestamp: Instant) {
        if !self.config.enabled {
            return;
        }
        let mut windows = self.windows.write();
        let window = windows.entry(name.clone()).or_insert_with(|| {
            SlidingWindow::new(Duration::from_secs(self.config.window_duration_secs))
        });

        // Check for anomaly before adding (compare against existing baseline)
        if window.count >= self.config.min_samples as u64
            && window.is_anomaly(value, self.config.z_score_threshold)
        {
            let z = window.z_score(value);
            let mean = window.mean();
            let sd = window.std_dev();
            // Drop windows lock before acquiring other locks
            drop(windows);
            self.maybe_fire_alert(&name, value, z, mean, sd, timestamp);
            // Re-acquire to add the sample
            let mut windows = self.windows.write();
            let window = windows.get_mut(&name).expect("window exists");
            window.add(value, timestamp);
        } else {
            window.add(value, timestamp);
        }
    }

    /// Check all metric windows for anomalies and return any new alerts.
    pub fn check_anomalies(&self) -> Vec<Alert> {
        let windows = self.windows.read();
        let mut new_alerts = Vec::new();
        for (name, window) in windows.iter() {
            if window.count < self.config.min_samples as u64 {
                continue;
            }
            if let Some(&(_, last_val)) = window.samples.back() {
                if window.is_anomaly(last_val, self.config.z_score_threshold) {
                    let z = window.z_score(last_val);
                    let mean = window.mean();
                    let sd = window.std_dev();
                    let severity = severity_from_z(z);
                    let lo = mean - self.config.z_score_threshold * sd;
                    let hi = mean + self.config.z_score_threshold * sd;
                    let alert = Alert {
                        id: Uuid::new_v4().to_string(),
                        timestamp: SystemTime::now(),
                        metric: name.to_string(),
                        severity,
                        message: format!(
                            "Anomaly detected for {}: value={:.4}, z_score={:.2}, expected=[{:.4}, {:.4}]",
                            name, last_val, z, lo, hi
                        ),
                        current_value: last_val,
                        expected_range: (lo, hi),
                        z_score: z,
                    };
                    new_alerts.push(alert);
                }
            }
        }
        new_alerts
    }

    /// Return alerts generated since the given instant (or all if `None`).
    pub fn get_alerts(&self, since: Option<Instant>) -> Vec<Alert> {
        let alerts = self.alerts.read();
        match since {
            Some(cutoff) => {
                let cutoff_system = SystemTime::now() - cutoff.elapsed();
                alerts
                    .iter()
                    .filter(|a| a.timestamp >= cutoff_system)
                    .cloned()
                    .collect()
            }
            None => alerts.iter().cloned().collect(),
        }
    }

    /// Clear all stored alerts.
    pub fn clear_alerts(&self) {
        self.alerts.write().clear();
    }

    /// Register a callback to be invoked when an alert fires.
    pub fn on_alert(&self, callback: impl Fn(&Alert) + Send + Sync + 'static) {
        self.alert_callbacks.write().push(Box::new(callback));
    }

    /// Get baseline statistics for a metric.
    pub fn get_baseline(&self, name: &MetricName) -> Option<BaselineStats> {
        let windows = self.windows.read();
        let window = windows.get(name)?;
        if window.count == 0 {
            return None;
        }
        Some(BaselineStats {
            mean: window.mean(),
            std_dev: window.std_dev(),
            min: window.min,
            max: window.max,
            count: window.count,
        })
    }

    /// Fire an alert if suppression has expired.
    fn maybe_fire_alert(
        &self,
        name: &MetricName,
        value: f64,
        z: f64,
        mean: f64,
        sd: f64,
        timestamp: Instant,
    ) {
        // Check suppression
        {
            let suppression = self.suppression_until.read();
            if let Some(&until) = suppression.get(name) {
                if timestamp < until {
                    return;
                }
            }
        }

        let severity = severity_from_z(z);
        let lo = mean - self.config.z_score_threshold * sd;
        let hi = mean + self.config.z_score_threshold * sd;

        let alert = Alert {
            id: Uuid::new_v4().to_string(),
            timestamp: SystemTime::now(),
            metric: name.to_string(),
            severity,
            message: format!(
                "Anomaly detected for {}: value={:.4}, z_score={:.2}, expected=[{:.4}, {:.4}]",
                name, value, z, lo, hi
            ),
            current_value: value,
            expected_range: (lo, hi),
            z_score: z,
        };

        // Store alert
        {
            let mut alerts = self.alerts.write();
            if alerts.len() >= self.config.max_alerts {
                alerts.pop_front();
            }
            alerts.push_back(alert.clone());
        }

        // Update suppression
        {
            let mut suppression = self.suppression_until.write();
            suppression.insert(
                name.clone(),
                timestamp + Duration::from_secs(self.config.alert_suppression_secs),
            );
        }

        // Fire callbacks
        let callbacks = self.alert_callbacks.read();
        for cb in callbacks.iter() {
            cb(&alert);
        }
    }
}

/// Map Z-score magnitude to severity.
fn severity_from_z(z: f64) -> AlertSeverity {
    let abs_z = z.abs();
    if abs_z >= 5.0 {
        AlertSeverity::Critical
    } else if abs_z >= 4.0 {
        AlertSeverity::Warning
    } else {
        AlertSeverity::Info
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_sliding_window_basic_stats() {
        let mut w = SlidingWindow::new(Duration::from_secs(60));
        let now = Instant::now();
        for i in 0..10 {
            w.add(i as f64, now);
        }
        assert_eq!(w.count, 10);
        assert!((w.mean() - 4.5).abs() < 0.001);
        assert!(w.std_dev() > 0.0);
        assert!((w.min - 0.0).abs() < f64::EPSILON);
        assert!((w.max - 9.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_sliding_window_percentile() {
        let mut w = SlidingWindow::new(Duration::from_secs(60));
        let now = Instant::now();
        for i in 1..=100 {
            w.add(i as f64, now);
        }
        let p50 = w.percentile(50.0);
        assert!((p50 - 50.0).abs() < 1.5);
        let p99 = w.percentile(99.0);
        assert!((p99 - 99.0).abs() < 1.5);
    }

    #[test]
    fn test_sliding_window_eviction() {
        let mut w = SlidingWindow::new(Duration::from_secs(1));
        let start = Instant::now();
        w.add(1.0, start);
        // Simulate time passing
        let later = start + Duration::from_secs(2);
        w.add(2.0, later);
        assert_eq!(w.count, 1);
        assert!((w.mean() - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_z_score_and_anomaly() {
        let mut w = SlidingWindow::new(Duration::from_secs(60));
        let now = Instant::now();
        // Add a tight cluster of values around 100
        for _ in 0..50 {
            w.add(100.0, now);
        }
        w.add(100.1, now);
        w.add(99.9, now);
        // A value far from the mean should be anomalous
        assert!(w.z_score(200.0).abs() > 3.0);
        assert!(w.is_anomaly(200.0, 3.0));
        // A value near the mean should not
        assert!(!w.is_anomaly(100.0, 3.0));
    }

    #[test]
    fn test_anomaly_detector_records_and_detects() {
        let config = AnomalyConfig {
            min_samples: 5,
            z_score_threshold: 2.0,
            alert_suppression_secs: 0,
            ..AnomalyConfig::default()
        };
        let detector = AnomalyDetector::new(config);
        let now = Instant::now();

        // Build a baseline
        for i in 0..10 {
            detector.record_metric(
                MetricName::Throughput,
                100.0,
                now + Duration::from_millis(i),
            );
        }
        // Inject an anomaly
        detector.record_metric(
            MetricName::Throughput,
            500.0,
            now + Duration::from_millis(10),
        );

        let alerts = detector.get_alerts(None);
        assert!(!alerts.is_empty());
        assert!(alerts[0].z_score.abs() > 2.0);
    }

    #[test]
    fn test_alert_suppression() {
        let config = AnomalyConfig {
            min_samples: 5,
            z_score_threshold: 2.0,
            alert_suppression_secs: 120,
            ..AnomalyConfig::default()
        };
        let detector = AnomalyDetector::new(config);
        let now = Instant::now();

        for i in 0..10 {
            detector.record_metric(MetricName::ErrorRate, 1.0, now + Duration::from_millis(i));
        }
        // Two anomalies close together – only one alert due to suppression
        detector.record_metric(
            MetricName::ErrorRate,
            100.0,
            now + Duration::from_millis(10),
        );
        detector.record_metric(
            MetricName::ErrorRate,
            100.0,
            now + Duration::from_millis(11),
        );

        let alerts = detector.get_alerts(None);
        assert_eq!(alerts.len(), 1);
    }

    #[test]
    fn test_alert_callback() {
        let config = AnomalyConfig {
            min_samples: 5,
            z_score_threshold: 2.0,
            alert_suppression_secs: 0,
            ..AnomalyConfig::default()
        };
        let detector = AnomalyDetector::new(config);
        let count = Arc::new(AtomicU64::new(0));
        let count_clone = count.clone();
        detector.on_alert(move |_alert| {
            count_clone.fetch_add(1, Ordering::SeqCst);
        });

        let now = Instant::now();
        for i in 0..10 {
            detector.record_metric(
                MetricName::Throughput,
                100.0,
                now + Duration::from_millis(i),
            );
        }
        detector.record_metric(
            MetricName::Throughput,
            500.0,
            now + Duration::from_millis(10),
        );
        assert!(count.load(Ordering::SeqCst) >= 1);
    }

    #[test]
    fn test_clear_alerts() {
        let config = AnomalyConfig {
            min_samples: 5,
            z_score_threshold: 2.0,
            alert_suppression_secs: 0,
            ..AnomalyConfig::default()
        };
        let detector = AnomalyDetector::new(config);
        let now = Instant::now();
        for i in 0..10 {
            detector.record_metric(
                MetricName::Throughput,
                100.0,
                now + Duration::from_millis(i),
            );
        }
        detector.record_metric(
            MetricName::Throughput,
            500.0,
            now + Duration::from_millis(10),
        );
        assert!(!detector.get_alerts(None).is_empty());
        detector.clear_alerts();
        assert!(detector.get_alerts(None).is_empty());
    }

    #[test]
    fn test_get_baseline() {
        let detector = AnomalyDetector::new(AnomalyConfig::default());
        let now = Instant::now();
        for i in 0..10 {
            detector.record_metric(
                MetricName::MemoryUsage,
                (100 + i) as f64,
                now + Duration::from_millis(i),
            );
        }
        let baseline = detector.get_baseline(&MetricName::MemoryUsage).unwrap();
        assert_eq!(baseline.count, 10);
        assert!(baseline.mean > 0.0);
    }

    #[test]
    fn test_metric_name_display() {
        assert_eq!(MetricName::Latency("GET".into()).to_string(), "latency:GET");
        assert_eq!(MetricName::Throughput.to_string(), "throughput");
        assert_eq!(MetricName::Custom("foo".into()).to_string(), "custom:foo");
    }
}
