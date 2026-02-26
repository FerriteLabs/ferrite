//! Metrics-to-Anomaly bridge
//!
//! Periodically samples the [`MetricsRegistry`] and feeds observations into
//! the [`AnomalyDetector`], then dispatches any fired alerts.
//!
//! # Usage
//!
//! ```rust,ignore
//! use ferrite_core::metrics::bridge::{MetricsBridge, BridgeConfig};
//!
//! let registry = Arc::new(MetricsRegistry::new());
//! let detector = Arc::new(AnomalyDetector::new(Default::default()));
//! let bridge = MetricsBridge::new(registry, detector, BridgeConfig::default());
//!
//! // Spawn the background evaluation loop
//! tokio::spawn(bridge.run());
//! ```

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use super::anomaly::{AlertRule, AlertSeverity, AnomalyDetector};
use super::instrumentation::MetricsRegistry;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the metrics-to-anomaly bridge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeConfig {
    /// How often to sample metrics and evaluate rules (seconds).
    pub sample_interval_secs: u64,
    /// Whether to register default alert rules on startup.
    pub register_default_rules: bool,
    /// P99 latency SLO for GET commands (microseconds).
    pub get_p99_slo_us: u64,
    /// P99 latency SLO for SET commands (microseconds).
    pub set_p99_slo_us: u64,
    /// Error rate threshold (0.0–1.0) for triggering alerts.
    pub error_rate_threshold: f64,
    /// Memory usage ratio threshold (0.0–1.0) for warnings.
    pub memory_warn_ratio: f64,
    /// Memory usage ratio threshold (0.0–1.0) for critical alerts.
    pub memory_critical_ratio: f64,
    /// Replication lag threshold (ms) for warnings.
    pub replication_lag_warn_ms: f64,
    /// Connected clients threshold for warnings.
    pub connected_clients_warn: u64,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self {
            sample_interval_secs: 30,
            register_default_rules: true,
            get_p99_slo_us: 1_000,   // 1ms P99 SLO
            set_p99_slo_us: 5_000,   // 5ms P99 SLO
            error_rate_threshold: 0.05, // 5% error rate
            memory_warn_ratio: 0.80,
            memory_critical_ratio: 0.95,
            replication_lag_warn_ms: 1_000.0,
            connected_clients_warn: 9_000,
        }
    }
}

// ---------------------------------------------------------------------------
// Bridge
// ---------------------------------------------------------------------------

/// Bridges the MetricsRegistry to the AnomalyDetector.
///
/// On each tick, it snapshots the registry, pushes observations into the
/// detector, evaluates rules, and logs any new alerts.
pub struct MetricsBridge {
    registry: Arc<MetricsRegistry>,
    detector: Arc<AnomalyDetector>,
    config: BridgeConfig,
}

impl MetricsBridge {
    /// Create a new bridge.
    pub fn new(
        registry: Arc<MetricsRegistry>,
        detector: Arc<AnomalyDetector>,
        config: BridgeConfig,
    ) -> Self {
        if config.register_default_rules {
            Self::register_defaults(&detector, &config);
        }
        info!(
            interval_secs = config.sample_interval_secs,
            "metrics-anomaly bridge initialized"
        );
        Self {
            registry,
            detector,
            config,
        }
    }

    /// Register sensible default alert rules.
    fn register_defaults(detector: &AnomalyDetector, config: &BridgeConfig) {
        // Error rate
        detector.add_rule(AlertRule::error_rate_spike(
            config.error_rate_threshold,
            AlertSeverity::Critical,
        ));

        // Memory
        detector.add_rule(AlertRule::memory_high(
            config.memory_warn_ratio,
            AlertSeverity::Warning,
        ));
        detector.add_rule(AlertRule {
            name: "memory_critical".to_string(),
            metric: "memory_usage_ratio".to_string(),
            rule_type: super::anomaly::RuleType::UpperThreshold(config.memory_critical_ratio),
            severity: AlertSeverity::Critical,
            enabled: true,
        });

        // Replication lag
        detector.add_rule(AlertRule::replication_lag(
            config.replication_lag_warn_ms,
            AlertSeverity::Warning,
        ));

        // Connected clients
        detector.add_rule(AlertRule {
            name: "connected_clients_high".to_string(),
            metric: "connected_clients".to_string(),
            rule_type: super::anomaly::RuleType::UpperThreshold(
                config.connected_clients_warn as f64,
            ),
            severity: AlertSeverity::Warning,
            enabled: true,
        });

        // OPS anomaly (Z-score based)
        detector.add_rule(AlertRule::anomaly(
            "ops_per_sec",
            3.0,
            AlertSeverity::Info,
        ));

        // Latency P99 SLOs
        detector.add_rule(AlertRule::latency_p99(
            "GET",
            config.get_p99_slo_us,
            AlertSeverity::Warning,
        ));
        detector.add_rule(AlertRule::latency_p99(
            "SET",
            config.set_p99_slo_us,
            AlertSeverity::Warning,
        ));

        debug!("registered {} default alert rules", 8);
    }

    /// Run the bridge loop. Runs until the task is cancelled.
    pub async fn run(self) {
        let interval = Duration::from_secs(self.config.sample_interval_secs);

        loop {
            tokio::time::sleep(interval).await;
            self.sample_and_evaluate();
        }
    }

    /// Run a single sample-and-evaluate cycle (public for testing).
    pub fn sample_and_evaluate(&self) {
        let snapshot = self.registry.get_snapshot();

        // Feed core metrics into detector
        self.detector
            .observe("ops_per_sec", snapshot.ops_per_sec);
        self.detector
            .observe("connected_clients", snapshot.connected_clients as f64);
        self.detector
            .observe("total_errors", snapshot.total_errors as f64);
        self.detector
            .observe("total_commands", snapshot.total_commands as f64);

        // Error rate
        if snapshot.total_commands > 0 {
            let error_rate = snapshot.total_errors as f64 / snapshot.total_commands as f64;
            self.detector.observe("error_rate", error_rate);
        }

        // Memory ratio
        let mem = &snapshot.memory;
        if mem.total_bytes > 0 {
            let ratio = mem.used_bytes as f64 / mem.total_bytes as f64;
            self.detector.observe("memory_usage_ratio", ratio);
        }

        // Replication lag
        self.detector
            .observe("replication_lag_ms", snapshot.replication.replication_lag_ms as f64);

        // Keyspace hit rate
        self.detector.observe("hit_rate", snapshot.keyspace.hit_rate);

        // Per-command latency P99s
        let command_stats = self.registry.get_command_stats();
        for cmd in &command_stats {
            if let Some(percentiles) = self.registry.get_latency_percentiles(&cmd.command) {
                let metric = format!("command_latency_{}", cmd.command.to_lowercase());
                self.detector.observe(&metric, percentiles.p99_us as f64);
            }
        }

        // Evaluate rules
        let alerts = self.detector.check();
        for alert in &alerts {
            warn!(
                id = alert.id,
                rule = %alert.rule_name,
                severity = %alert.severity,
                message = %alert.message,
                "anomaly alert fired"
            );
        }

        debug!(
            ops = snapshot.ops_per_sec,
            clients = snapshot.connected_clients,
            alerts = alerts.len(),
            "metrics bridge cycle"
        );
    }

    /// Get a reference to the anomaly detector.
    pub fn detector(&self) -> &Arc<AnomalyDetector> {
        &self.detector
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bridge_creation_registers_defaults() {
        let registry = Arc::new(MetricsRegistry::new());
        let detector = Arc::new(AnomalyDetector::new(Default::default()));
        let _bridge = MetricsBridge::new(registry, detector.clone(), BridgeConfig::default());

        let rules = detector.list_rules();
        assert!(rules.len() >= 7, "should register at least 7 default rules, got {}", rules.len());

        let rule_names: Vec<&str> = rules.iter().map(|r| r.name.as_str()).collect();
        assert!(rule_names.contains(&"error_rate_spike"));
        assert!(rule_names.contains(&"memory_high"));
        assert!(rule_names.contains(&"memory_critical"));
        assert!(rule_names.contains(&"replication_lag_high"));
        assert!(rule_names.contains(&"ops_per_sec_anomaly"));
    }

    #[test]
    fn test_bridge_sample_no_panic() {
        let registry = Arc::new(MetricsRegistry::new());
        let detector = Arc::new(AnomalyDetector::new(Default::default()));
        let bridge = MetricsBridge::new(registry, detector, BridgeConfig::default());

        // Should not panic on empty registry
        bridge.sample_and_evaluate();
    }

    #[test]
    fn test_bridge_observes_metrics() {
        let registry = Arc::new(MetricsRegistry::new());
        let detector = Arc::new(AnomalyDetector::new(Default::default()));

        // Record some commands
        for _ in 0..10 {
            registry.record_command("GET", 100, true);
        }
        registry.record_command("GET", 200, false);

        let bridge = MetricsBridge::new(
            registry,
            detector.clone(),
            BridgeConfig {
                register_default_rules: false,
                ..Default::default()
            },
        );

        bridge.sample_and_evaluate();

        let summary = detector.summary();
        assert!(summary.tracked_metrics > 0, "should observe some metrics");
    }

    #[test]
    fn test_bridge_fires_alert_on_high_error_rate() {
        let registry = Arc::new(MetricsRegistry::new());
        let detector = Arc::new(AnomalyDetector::new(super::super::anomaly::AnomalyConfig {
            alert_cooldown_secs: 0,
            ..Default::default()
        }));

        // Create a scenario with 50% error rate
        for _ in 0..50 {
            registry.record_command("SET", 100, true);
        }
        for _ in 0..50 {
            registry.record_command("SET", 100, false);
        }

        let bridge = MetricsBridge::new(
            registry,
            detector.clone(),
            BridgeConfig {
                error_rate_threshold: 0.10,
                ..Default::default()
            },
        );

        bridge.sample_and_evaluate();

        let alerts = detector.get_alerts(Some(AlertSeverity::Critical));
        assert!(
            !alerts.is_empty(),
            "should fire critical alert on 50% error rate"
        );
    }

    #[test]
    fn test_bridge_config_default() {
        let cfg = BridgeConfig::default();
        assert_eq!(cfg.sample_interval_secs, 30);
        assert!(cfg.register_default_rules);
        assert_eq!(cfg.get_p99_slo_us, 1_000);
        assert_eq!(cfg.error_rate_threshold, 0.05);
    }

    #[test]
    fn test_bridge_no_default_rules() {
        let registry = Arc::new(MetricsRegistry::new());
        let detector = Arc::new(AnomalyDetector::new(Default::default()));
        let _bridge = MetricsBridge::new(
            registry,
            detector.clone(),
            BridgeConfig {
                register_default_rules: false,
                ..Default::default()
            },
        );

        assert_eq!(detector.list_rules().len(), 0);
    }
}
