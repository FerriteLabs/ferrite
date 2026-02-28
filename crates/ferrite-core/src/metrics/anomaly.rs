//! Anomaly Detection & Alerting
//!
//! Provides statistical anomaly detection over Ferrite's metrics streams
//! with configurable alert rules and webhook-based notifications.
//!
//! # Architecture
//!
//! ```text
//! MetricsRegistry → AnomalyDetector → AlertRule evaluation → AlertManager → Webhooks
//! ```
//!
//! # Detection Methods
//!
//! - **Z-Score**: Detects values that deviate significantly from the rolling mean
//! - **Rate-of-Change**: Detects sudden spikes or drops in metric values
//! - **Threshold**: Simple upper/lower bound alerts
//! - **Percentile Breach**: Alerts when latency percentiles exceed SLO targets
//!
//! # Example
//!
//! ```rust,ignore
//! use ferrite_core::metrics::anomaly::{AnomalyDetector, AlertRule, AlertSeverity};
//!
//! let mut detector = AnomalyDetector::new(Default::default());
//! detector.add_rule(AlertRule::latency_p99("GET", 1000, AlertSeverity::Warning));
//! detector.add_rule(AlertRule::error_rate_spike(0.05, AlertSeverity::Critical));
//! detector.observe("latency_us", 1500.0);
//! let alerts = detector.check();
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the anomaly detection engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyConfig {
    /// Enable anomaly detection
    pub enabled: bool,
    /// Size of the rolling window for statistical analysis
    pub window_size: usize,
    /// How often to evaluate rules (seconds)
    pub evaluation_interval_secs: u64,
    /// Default Z-score threshold for anomaly detection
    pub z_score_threshold: f64,
    /// Maximum number of alerts to retain in history
    pub max_alert_history: usize,
    /// Suppress duplicate alerts within this cooldown period (seconds)
    pub alert_cooldown_secs: u64,
    /// Webhook endpoints for alert delivery
    pub webhooks: Vec<WebhookConfig>,
}

impl Default for AnomalyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            window_size: 100,
            evaluation_interval_secs: 60,
            z_score_threshold: 3.0,
            max_alert_history: 1000,
            alert_cooldown_secs: 300,
            webhooks: Vec::new(),
        }
    }
}

/// Configuration for an alert webhook endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    /// Webhook URL
    pub url: String,
    /// Minimum severity to trigger this webhook
    pub min_severity: AlertSeverity,
    /// Custom headers (e.g., auth tokens)
    pub headers: HashMap<String, String>,
    /// Request timeout in seconds
    pub timeout_secs: u64,
}

// ---------------------------------------------------------------------------
// Alert types
// ---------------------------------------------------------------------------

/// Severity levels for alerts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum AlertSeverity {
    /// Informational — metric is noteworthy but not actionable
    Info,
    /// Warning — metric is approaching a problematic threshold
    Warning,
    /// Critical — immediate attention required
    Critical,
}

impl std::fmt::Display for AlertSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertSeverity::Info => write!(f, "INFO"),
            AlertSeverity::Warning => write!(f, "WARNING"),
            AlertSeverity::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// The type of detection that triggered an alert.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DetectionMethod {
    /// Value exceeded a static threshold
    Threshold {
        /// The threshold that was exceeded
        threshold: f64,
        /// The observed value
        observed: f64,
    },
    /// Z-score exceeded the configured threshold
    ZScore {
        /// Computed z-score
        z_score: f64,
        /// Rolling mean
        mean: f64,
        /// Rolling standard deviation
        std_dev: f64,
        /// The observed value
        observed: f64,
    },
    /// Sudden rate of change detected
    RateOfChange {
        /// Change rate (value/second)
        rate: f64,
        /// Maximum allowed rate
        max_rate: f64,
    },
    /// Latency percentile exceeded SLO
    PercentileBreach {
        /// Which percentile (e.g., 99.0)
        percentile: f64,
        /// SLO target in microseconds
        slo_us: u64,
        /// Observed value in microseconds
        observed_us: u64,
    },
}

/// A fired alert.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    /// Unique alert identifier
    pub id: u64,
    /// Rule name that triggered this alert
    pub rule_name: String,
    /// Metric name involved
    pub metric: String,
    /// Severity level
    pub severity: AlertSeverity,
    /// Detection method and details
    pub detection: DetectionMethod,
    /// Human-readable description
    pub message: String,
    /// When the alert was fired (Unix timestamp milliseconds)
    pub fired_at_ms: u64,
    /// Whether the alert has been acknowledged
    pub acknowledged: bool,
}

// ---------------------------------------------------------------------------
// Alert rules
// ---------------------------------------------------------------------------

/// A rule that defines when to fire an alert.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    /// Rule name (unique identifier)
    pub name: String,
    /// Metric name to monitor
    pub metric: String,
    /// Rule type
    pub rule_type: RuleType,
    /// Severity when fired
    pub severity: AlertSeverity,
    /// Whether this rule is enabled
    pub enabled: bool,
}

/// The type of alert evaluation to perform.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RuleType {
    /// Fire when value exceeds upper threshold
    UpperThreshold(f64),
    /// Fire when value drops below lower threshold
    LowerThreshold(f64),
    /// Fire when Z-score exceeds threshold (statistical anomaly)
    ZScore(f64),
    /// Fire when rate of change exceeds max (units/second)
    RateOfChange(f64),
    /// Fire when a latency percentile exceeds target (percentile, target_us)
    PercentileBreach {
        /// Percentile to check (e.g., 99.0)
        percentile: f64,
        /// Target in microseconds
        target_us: u64,
    },
}

impl AlertRule {
    /// Create a rule that fires when a command's P99 latency exceeds a threshold.
    pub fn latency_p99(command: &str, target_us: u64, severity: AlertSeverity) -> Self {
        Self {
            name: format!("{}_p99_latency", command.to_lowercase()),
            metric: format!("command_latency_{}", command.to_lowercase()),
            rule_type: RuleType::PercentileBreach {
                percentile: 99.0,
                target_us,
            },
            severity,
            enabled: true,
        }
    }

    /// Create a rule that fires when the error rate exceeds a threshold.
    pub fn error_rate_spike(max_rate: f64, severity: AlertSeverity) -> Self {
        Self {
            name: "error_rate_spike".to_string(),
            metric: "error_rate".to_string(),
            rule_type: RuleType::UpperThreshold(max_rate),
            severity,
            enabled: true,
        }
    }

    /// Create a rule that fires on memory usage exceeding a percentage.
    pub fn memory_high(threshold_ratio: f64, severity: AlertSeverity) -> Self {
        Self {
            name: "memory_high".to_string(),
            metric: "memory_usage_ratio".to_string(),
            rule_type: RuleType::UpperThreshold(threshold_ratio),
            severity,
            enabled: true,
        }
    }

    /// Create a Z-score based anomaly rule for any metric.
    pub fn anomaly(metric: &str, z_threshold: f64, severity: AlertSeverity) -> Self {
        Self {
            name: format!("{}_anomaly", metric),
            metric: metric.to_string(),
            rule_type: RuleType::ZScore(z_threshold),
            severity,
            enabled: true,
        }
    }

    /// Create a rule for replication lag.
    pub fn replication_lag(max_lag_ms: f64, severity: AlertSeverity) -> Self {
        Self {
            name: "replication_lag_high".to_string(),
            metric: "replication_lag_ms".to_string(),
            rule_type: RuleType::UpperThreshold(max_lag_ms),
            severity,
            enabled: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Rolling statistics
// ---------------------------------------------------------------------------

/// Maintains a rolling window of observations for statistical analysis.
#[derive(Debug)]
struct RollingStats {
    values: Vec<f64>,
    window_size: usize,
    head: usize,
    count: usize,
    sum: f64,
    sum_sq: f64,
    last_value: Option<f64>,
    last_timestamp: Option<Instant>,
}

impl RollingStats {
    fn new(window_size: usize) -> Self {
        Self {
            values: vec![0.0; window_size],
            window_size,
            head: 0,
            count: 0,
            sum: 0.0,
            sum_sq: 0.0,
            last_value: None,
            last_timestamp: None,
        }
    }

    fn push(&mut self, value: f64) {
        if self.count >= self.window_size {
            // Evict oldest value
            let old = self.values[self.head];
            self.sum -= old;
            self.sum_sq -= old * old;
        } else {
            self.count += 1;
        }

        self.values[self.head] = value;
        self.sum += value;
        self.sum_sq += value * value;
        self.head = (self.head + 1) % self.window_size;

        self.last_value = Some(value);
        self.last_timestamp = Some(Instant::now());
    }

    fn mean(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        self.sum / self.count as f64
    }

    fn std_dev(&self) -> f64 {
        if self.count < 2 {
            return 0.0;
        }
        let n = self.count as f64;
        let variance = (self.sum_sq / n) - (self.mean() * self.mean());
        if variance <= 0.0 {
            0.0
        } else {
            variance.sqrt()
        }
    }

    fn z_score(&self, value: f64) -> f64 {
        let sd = self.std_dev();
        if sd < f64::EPSILON {
            return 0.0;
        }
        (value - self.mean()) / sd
    }

    fn rate_of_change(&self) -> Option<f64> {
        if self.count < 2 {
            return None;
        }
        let prev_idx = if self.head == 0 {
            self.window_size - 1
        } else {
            self.head - 1
        };
        let prev_prev_idx = if prev_idx == 0 {
            self.window_size - 1
        } else {
            prev_idx - 1
        };

        if self.count < 2 {
            return None;
        }

        let current = self.values[prev_idx];
        let previous = self.values[prev_prev_idx];
        Some(current - previous)
    }
}

// ---------------------------------------------------------------------------
// Anomaly detector
// ---------------------------------------------------------------------------

/// The main anomaly detection engine.
///
/// Observes metric values, maintains rolling statistics, evaluates alert rules,
/// and manages the alert lifecycle (firing, cooldown, history).
pub struct AnomalyDetector {
    config: AnomalyConfig,
    rules: RwLock<Vec<AlertRule>>,
    stats: RwLock<HashMap<String, RollingStats>>,
    alerts: RwLock<Vec<Alert>>,
    alert_counter: AtomicU64,
    last_fired: RwLock<HashMap<String, Instant>>,
}

impl AnomalyDetector {
    /// Create a new detector with the given configuration.
    pub fn new(config: AnomalyConfig) -> Self {
        info!(
            window = config.window_size,
            z_threshold = config.z_score_threshold,
            "anomaly detector initialized"
        );
        Self {
            config,
            rules: RwLock::new(Vec::new()),
            stats: RwLock::new(HashMap::new()),
            alerts: RwLock::new(Vec::new()),
            alert_counter: AtomicU64::new(0),
            last_fired: RwLock::new(HashMap::new()),
        }
    }

    /// Add an alert rule.
    pub fn add_rule(&self, rule: AlertRule) {
        debug!(rule = %rule.name, metric = %rule.metric, "added alert rule");
        self.rules.write().push(rule);
    }

    /// Remove an alert rule by name.
    pub fn remove_rule(&self, name: &str) -> bool {
        let mut rules = self.rules.write();
        let before = rules.len();
        rules.retain(|r| r.name != name);
        rules.len() < before
    }

    /// List all configured rules.
    pub fn list_rules(&self) -> Vec<AlertRule> {
        self.rules.read().clone()
    }

    /// Observe a new metric value. Updates rolling statistics.
    pub fn observe(&self, metric: &str, value: f64) {
        let mut stats = self.stats.write();
        let entry = stats
            .entry(metric.to_string())
            .or_insert_with(|| RollingStats::new(self.config.window_size));
        entry.push(value);
    }

    /// Evaluate all rules against current statistics and return any new alerts.
    pub fn check(&self) -> Vec<Alert> {
        let rules = self.rules.read();
        let stats = self.stats.read();
        let mut new_alerts = Vec::new();

        for rule in rules.iter() {
            if !rule.enabled {
                continue;
            }

            let Some(metric_stats) = stats.get(&rule.metric) else {
                continue;
            };

            let Some(current_value) = metric_stats.last_value else {
                continue;
            };

            // Check cooldown
            if self.is_in_cooldown(&rule.name) {
                continue;
            }

            let detection = match &rule.rule_type {
                RuleType::UpperThreshold(threshold) => {
                    if current_value > *threshold {
                        Some(DetectionMethod::Threshold {
                            threshold: *threshold,
                            observed: current_value,
                        })
                    } else {
                        None
                    }
                }
                RuleType::LowerThreshold(threshold) => {
                    if current_value < *threshold {
                        Some(DetectionMethod::Threshold {
                            threshold: *threshold,
                            observed: current_value,
                        })
                    } else {
                        None
                    }
                }
                RuleType::ZScore(z_threshold) => {
                    if metric_stats.count >= 10 {
                        let z = metric_stats.z_score(current_value);
                        if z.abs() > *z_threshold {
                            Some(DetectionMethod::ZScore {
                                z_score: z,
                                mean: metric_stats.mean(),
                                std_dev: metric_stats.std_dev(),
                                observed: current_value,
                            })
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                RuleType::RateOfChange(max_rate) => {
                    if let Some(rate) = metric_stats.rate_of_change() {
                        if rate.abs() > *max_rate {
                            Some(DetectionMethod::RateOfChange {
                                rate,
                                max_rate: *max_rate,
                            })
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                RuleType::PercentileBreach {
                    percentile: _,
                    target_us,
                } => {
                    if current_value as u64 > *target_us {
                        Some(DetectionMethod::PercentileBreach {
                            percentile: 99.0,
                            slo_us: *target_us,
                            observed_us: current_value as u64,
                        })
                    } else {
                        None
                    }
                }
            };

            if let Some(detection) = detection {
                let alert = self.fire_alert(&rule.name, &rule.metric, rule.severity, detection);
                new_alerts.push(alert);
            }
        }

        new_alerts
    }

    /// Fire an alert and record it.
    fn fire_alert(
        &self,
        rule_name: &str,
        metric: &str,
        severity: AlertSeverity,
        detection: DetectionMethod,
    ) -> Alert {
        let id = self.alert_counter.fetch_add(1, Ordering::Relaxed);
        let message = format_alert_message(rule_name, metric, &severity, &detection);

        let alert = Alert {
            id,
            rule_name: rule_name.to_string(),
            metric: metric.to_string(),
            severity,
            detection,
            message: message.clone(),
            fired_at_ms: chrono::Utc::now().timestamp_millis() as u64,
            acknowledged: false,
        };

        // Record cooldown
        self.last_fired
            .write()
            .insert(rule_name.to_string(), Instant::now());

        // Store in history
        let mut alerts = self.alerts.write();
        alerts.push(alert.clone());
        if alerts.len() > self.config.max_alert_history {
            alerts.remove(0);
        }

        warn!(
            rule = rule_name,
            metric = metric,
            severity = %severity,
            message = %message,
            "alert fired"
        );

        alert
    }

    /// Check if a rule is in cooldown (recently fired).
    fn is_in_cooldown(&self, rule_name: &str) -> bool {
        let last_fired = self.last_fired.read();
        if let Some(last) = last_fired.get(rule_name) {
            last.elapsed() < Duration::from_secs(self.config.alert_cooldown_secs)
        } else {
            false
        }
    }

    /// Get all alerts (optionally filtered by severity).
    pub fn get_alerts(&self, min_severity: Option<AlertSeverity>) -> Vec<Alert> {
        let alerts = self.alerts.read();
        match min_severity {
            Some(sev) => alerts
                .iter()
                .filter(|a| a.severity >= sev)
                .cloned()
                .collect(),
            None => alerts.clone(),
        }
    }

    /// Get recent alerts (within the last N seconds).
    pub fn get_recent_alerts(&self, seconds: u64) -> Vec<Alert> {
        let cutoff = chrono::Utc::now().timestamp_millis() as u64 - (seconds * 1000);
        self.alerts
            .read()
            .iter()
            .filter(|a| a.fired_at_ms >= cutoff)
            .cloned()
            .collect()
    }

    /// Acknowledge an alert by ID.
    pub fn acknowledge(&self, alert_id: u64) -> bool {
        let mut alerts = self.alerts.write();
        if let Some(alert) = alerts.iter_mut().find(|a| a.id == alert_id) {
            alert.acknowledged = true;
            true
        } else {
            false
        }
    }

    /// Clear all alerts.
    pub fn clear_alerts(&self) {
        self.alerts.write().clear();
    }

    /// Get a summary of the detector's state.
    pub fn summary(&self) -> DetectorSummary {
        let alerts = self.alerts.read();
        let rules = self.rules.read();
        let stats = self.stats.read();

        DetectorSummary {
            total_rules: rules.len(),
            enabled_rules: rules.iter().filter(|r| r.enabled).count(),
            total_alerts: alerts.len(),
            unacknowledged_alerts: alerts.iter().filter(|a| !a.acknowledged).count(),
            critical_alerts: alerts
                .iter()
                .filter(|a| a.severity == AlertSeverity::Critical && !a.acknowledged)
                .count(),
            tracked_metrics: stats.len(),
        }
    }

    /// Build a list of webhook payloads for pending alerts.
    pub fn pending_webhook_payloads(&self) -> Vec<WebhookPayload> {
        let alerts = self.alerts.read();
        let unacked: Vec<&Alert> = alerts.iter().filter(|a| !a.acknowledged).collect();

        self.config
            .webhooks
            .iter()
            .flat_map(|webhook| {
                unacked
                    .iter()
                    .filter(|a| a.severity >= webhook.min_severity)
                    .map(|alert| WebhookPayload {
                        url: webhook.url.clone(),
                        headers: webhook.headers.clone(),
                        timeout_secs: webhook.timeout_secs,
                        alert: (*alert).clone(),
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }
}

/// Summary of the anomaly detector state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectorSummary {
    /// Total number of configured rules
    pub total_rules: usize,
    /// Number of enabled rules
    pub enabled_rules: usize,
    /// Total alerts in history
    pub total_alerts: usize,
    /// Alerts not yet acknowledged
    pub unacknowledged_alerts: usize,
    /// Critical unacknowledged alerts
    pub critical_alerts: usize,
    /// Number of metrics being tracked
    pub tracked_metrics: usize,
}

/// A webhook delivery payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookPayload {
    /// Target URL
    pub url: String,
    /// Custom headers
    pub headers: HashMap<String, String>,
    /// Timeout in seconds
    pub timeout_secs: u64,
    /// The alert to deliver
    pub alert: Alert,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn format_alert_message(
    rule_name: &str,
    metric: &str,
    severity: &AlertSeverity,
    detection: &DetectionMethod,
) -> String {
    match detection {
        DetectionMethod::Threshold {
            threshold,
            observed,
        } => {
            format!(
                "[{severity}] {rule_name}: {metric} = {observed:.2} (threshold: {threshold:.2})"
            )
        }
        DetectionMethod::ZScore {
            z_score,
            mean,
            std_dev,
            observed,
        } => {
            format!(
                "[{severity}] {rule_name}: {metric} = {observed:.2} is {z_score:.1}σ from mean {mean:.2} (σ={std_dev:.2})"
            )
        }
        DetectionMethod::RateOfChange { rate, max_rate } => {
            format!(
                "[{severity}] {rule_name}: {metric} changing at {rate:.2}/s (max: {max_rate:.2}/s)"
            )
        }
        DetectionMethod::PercentileBreach {
            percentile,
            slo_us,
            observed_us,
        } => {
            format!(
                "[{severity}] {rule_name}: {metric} P{percentile:.0} = {observed_us}µs exceeds SLO {slo_us}µs"
            )
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rolling_stats_mean_and_stddev() {
        let mut stats = RollingStats::new(5);
        for v in &[10.0, 20.0, 30.0, 40.0, 50.0] {
            stats.push(*v);
        }
        assert!((stats.mean() - 30.0).abs() < 0.01);
        assert!(stats.std_dev() > 0.0);
    }

    #[test]
    fn test_rolling_stats_z_score() {
        let mut stats = RollingStats::new(100);
        // Build a baseline
        for i in 0..50 {
            stats.push(100.0 + (i as f64 % 5.0));
        }
        // Normal value should have low z-score
        let z_normal = stats.z_score(102.0);
        assert!(z_normal.abs() < 2.0);

        // Outlier should have high z-score
        let z_outlier = stats.z_score(200.0);
        assert!(z_outlier.abs() > 3.0);
    }

    #[test]
    fn test_threshold_alert() {
        let detector = AnomalyDetector::new(AnomalyConfig {
            alert_cooldown_secs: 0,
            ..Default::default()
        });
        detector.add_rule(AlertRule {
            name: "high_mem".to_string(),
            metric: "memory".to_string(),
            rule_type: RuleType::UpperThreshold(80.0),
            severity: AlertSeverity::Warning,
            enabled: true,
        });

        detector.observe("memory", 50.0);
        assert!(detector.check().is_empty());

        detector.observe("memory", 90.0);
        let alerts = detector.check();
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].severity, AlertSeverity::Warning);
        assert_eq!(alerts[0].rule_name, "high_mem");
    }

    #[test]
    fn test_z_score_alert() {
        let detector = AnomalyDetector::new(AnomalyConfig {
            alert_cooldown_secs: 0,
            z_score_threshold: 3.0,
            ..Default::default()
        });
        detector.add_rule(AlertRule::anomaly("latency", 3.0, AlertSeverity::Critical));

        // Build baseline
        for _ in 0..20 {
            detector.observe("latency", 100.0);
        }
        assert!(detector.check().is_empty());

        // Inject anomaly
        detector.observe("latency", 500.0);
        let alerts = detector.check();
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].severity, AlertSeverity::Critical);
    }

    #[test]
    fn test_alert_cooldown() {
        let detector = AnomalyDetector::new(AnomalyConfig {
            alert_cooldown_secs: 600, // 10 minutes
            ..Default::default()
        });
        detector.add_rule(AlertRule {
            name: "test".to_string(),
            metric: "m".to_string(),
            rule_type: RuleType::UpperThreshold(50.0),
            severity: AlertSeverity::Info,
            enabled: true,
        });

        detector.observe("m", 100.0);
        let a1 = detector.check();
        assert_eq!(a1.len(), 1);

        // Second check should be suppressed (cooldown)
        detector.observe("m", 100.0);
        let a2 = detector.check();
        assert!(a2.is_empty());
    }

    #[test]
    fn test_acknowledge_alert() {
        let detector = AnomalyDetector::new(AnomalyConfig {
            alert_cooldown_secs: 0,
            ..Default::default()
        });
        detector.add_rule(AlertRule::error_rate_spike(0.01, AlertSeverity::Critical));

        detector.observe("error_rate", 0.5);
        let alerts = detector.check();
        assert_eq!(alerts.len(), 1);

        assert!(detector.acknowledge(alerts[0].id));
        let summary = detector.summary();
        assert_eq!(summary.unacknowledged_alerts, 0);
    }

    #[test]
    fn test_latency_p99_rule() {
        let detector = AnomalyDetector::new(AnomalyConfig {
            alert_cooldown_secs: 0,
            ..Default::default()
        });
        detector.add_rule(AlertRule::latency_p99("GET", 1000, AlertSeverity::Warning));

        detector.observe("command_latency_get", 500.0);
        assert!(detector.check().is_empty());

        detector.observe("command_latency_get", 1500.0);
        let alerts = detector.check();
        assert_eq!(alerts.len(), 1);
        assert!(matches!(
            alerts[0].detection,
            DetectionMethod::PercentileBreach { .. }
        ));
    }

    #[test]
    fn test_summary() {
        let detector = AnomalyDetector::new(Default::default());
        detector.add_rule(AlertRule::memory_high(0.9, AlertSeverity::Warning));
        detector.add_rule(AlertRule::replication_lag(100.0, AlertSeverity::Critical));

        let summary = detector.summary();
        assert_eq!(summary.total_rules, 2);
        assert_eq!(summary.enabled_rules, 2);
        assert_eq!(summary.total_alerts, 0);
    }

    #[test]
    fn test_webhook_payloads() {
        let detector = AnomalyDetector::new(AnomalyConfig {
            alert_cooldown_secs: 0,
            webhooks: vec![WebhookConfig {
                url: "https://hooks.example.com/alert".to_string(),
                min_severity: AlertSeverity::Warning,
                headers: HashMap::new(),
                timeout_secs: 10,
            }],
            ..Default::default()
        });
        detector.add_rule(AlertRule::error_rate_spike(0.01, AlertSeverity::Critical));

        detector.observe("error_rate", 0.5);
        detector.check();

        let payloads = detector.pending_webhook_payloads();
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].url, "https://hooks.example.com/alert");
    }

    #[test]
    fn test_remove_rule() {
        let detector = AnomalyDetector::new(Default::default());
        detector.add_rule(AlertRule::memory_high(0.9, AlertSeverity::Warning));
        assert_eq!(detector.list_rules().len(), 1);

        assert!(detector.remove_rule("memory_high"));
        assert_eq!(detector.list_rules().len(), 0);
        assert!(!detector.remove_rule("nonexistent"));
    }
}
