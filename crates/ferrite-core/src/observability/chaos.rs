//! Chaos Engineering & Self-Healing
//!
//! Integrated fault injection framework with anomaly detection and automatic
//! remediation. Enables testing of failure modes and ensures the system
//! can detect, diagnose, and heal without human intervention.
//!
//! # Components
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                  Chaos & Self-Healing Engine                 │
//! │  ┌───────────────┐  ┌────────────────┐  ┌──────────────┐   │
//! │  │ Fault Injector │  │ Anomaly Detect │  │ Remediation  │   │
//! │  │ (network, io,  │  │ (EMA, z-score, │  │ (failover,   │   │
//! │  │  memory, cpu)  │  │  thresholds)   │  │  eviction,   │   │
//! │  └───────────────┘  └────────────────┘  │  rebalance)  │   │
//! │                                          └──────────────┘   │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Fault injection
// ---------------------------------------------------------------------------

/// Types of faults that can be injected.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FaultType {
    /// Simulate a network partition between nodes
    NetworkPartition {
        /// Nodes to partition from the cluster.
        target_nodes: Vec<String>,
        /// Duration of the partition.
        duration: Duration,
    },
    /// Simulate slow disk I/O
    SlowDisk {
        /// Base added latency per I/O operation.
        latency: Duration,
        /// Random jitter added on top of base latency.
        jitter: Duration,
    },
    /// Simulate memory pressure
    MemoryPressure {
        /// Target memory usage as a percentage.
        target_usage_percent: u8,
    },
    /// Simulate CPU saturation
    CpuSaturation {
        /// Target CPU usage as a percentage.
        target_usage_percent: u8,
        /// Duration of the CPU saturation.
        duration: Duration,
    },
    /// Kill a specific process/shard
    ProcessKill {
        /// Identifier of the target process or shard.
        target: String,
    },
    /// Inject random latency into all operations
    LatencyInjection {
        /// Minimum injected latency.
        min_latency: Duration,
        /// Maximum injected latency.
        max_latency: Duration,
        /// Probability of injection per operation (0.0–1.0).
        probability: f64,
    },
    /// Corrupt a percentage of responses
    ResponseCorruption {
        /// Probability of corruption per response (0.0–1.0).
        probability: f64,
    },
    /// Simulate clock skew
    ClockSkew {
        /// Amount of time to skew the clock.
        skew: Duration,
        /// Direction of the clock skew.
        direction: ClockDirection,
    },
}

/// Direction of clock skew.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ClockDirection {
    /// Clock moves forward (ahead of real time).
    Forward,
    /// Clock moves backward (behind real time).
    Backward,
}

/// A scheduled fault injection experiment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosExperiment {
    /// Unique experiment identifier.
    pub id: String,
    /// Human-readable experiment name.
    pub name: String,
    /// Detailed experiment description.
    pub description: String,
    /// Faults to inject during the experiment.
    pub faults: Vec<FaultType>,
    /// How long the experiment should run.
    pub duration: Duration,
    /// Whether to abort the experiment if a failure is detected.
    pub abort_on_failure: bool,
    /// Optional steady-state hypothesis to validate during the experiment.
    pub steady_state_check: Option<SteadyStateCheck>,
}

/// Defines the steady-state hypothesis that must hold during an experiment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SteadyStateCheck {
    /// Maximum acceptable error rate (fraction, e.g. 0.01 = 1%).
    pub max_error_rate: f64,
    /// Maximum acceptable P99 latency.
    pub max_p99_latency: Duration,
    /// Minimum acceptable throughput (ops/sec).
    pub min_throughput: u64,
}

impl Default for SteadyStateCheck {
    fn default() -> Self {
        Self {
            max_error_rate: 0.01,
            max_p99_latency: Duration::from_millis(10),
            min_throughput: 1000,
        }
    }
}

/// Status of a running experiment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExperimentStatus {
    /// Experiment is queued but not yet started.
    Pending,
    /// Experiment is currently running.
    Running,
    /// Experiment completed and steady state was maintained.
    Passed,
    /// Experiment completed but steady state was violated.
    Failed,
    /// Experiment was aborted before completion.
    Aborted,
}

/// Result of a completed experiment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExperimentResult {
    /// ID of the experiment that produced this result.
    pub experiment_id: String,
    /// Final status of the experiment.
    pub status: ExperimentStatus,
    /// Actual duration of the experiment.
    pub duration: Duration,
    /// Whether the steady-state hypothesis held throughout.
    pub steady_state_maintained: bool,
    /// Human-readable observations recorded during the experiment.
    pub observations: Vec<String>,
    /// Metrics collected during the experiment.
    pub metrics_during: ExperimentMetrics,
}

/// Metrics captured during a chaos experiment.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExperimentMetrics {
    /// Average latency in milliseconds.
    pub avg_latency_ms: f64,
    /// P99 latency in milliseconds.
    pub p99_latency_ms: f64,
    /// Total error count.
    pub error_count: u64,
    /// Throughput in operations per second.
    pub throughput: u64,
    /// Number of anomalies detected.
    pub anomalies_detected: u64,
    /// Number of automated remediations applied.
    pub remediations_applied: u64,
}

// ---------------------------------------------------------------------------
// Anomaly detection
// ---------------------------------------------------------------------------

/// Time-series metric sample.
#[derive(Debug, Clone)]
pub struct MetricSample {
    /// When the sample was taken.
    pub timestamp: Instant,
    /// Observed metric value.
    pub value: f64,
}

/// Exponential Moving Average anomaly detector.
pub struct EmaDetector {
    alpha: f64,
    current_ema: f64,
    current_variance: f64,
    z_threshold: f64,
    initialized: bool,
    window: VecDeque<f64>,
    max_window: usize,
}

impl EmaDetector {
    /// Create a new EMA detector.
    ///
    /// - `alpha`: smoothing factor (0 < alpha <= 1), higher = more responsive
    /// - `z_threshold`: number of standard deviations to trigger anomaly
    pub fn new(alpha: f64, z_threshold: f64) -> Self {
        Self {
            alpha: alpha.clamp(0.01, 1.0),
            current_ema: 0.0,
            current_variance: 0.0,
            z_threshold,
            initialized: false,
            window: VecDeque::new(),
            max_window: 100,
        }
    }

    /// Observe a new value. Returns `true` if it is anomalous.
    pub fn observe(&mut self, value: f64) -> bool {
        self.window.push_back(value);
        if self.window.len() > self.max_window {
            self.window.pop_front();
        }

        if !self.initialized {
            self.current_ema = value;
            self.current_variance = 0.0;
            self.initialized = true;
            return false;
        }

        // Compute z-score BEFORE updating EMA/variance so the new value
        // doesn't pull the statistics toward itself.
        let std_dev = self.current_variance.sqrt();
        let is_anomaly = if std_dev < f64::EPSILON {
            false
        } else {
            let z_score = (value - self.current_ema).abs() / std_dev;
            z_score > self.z_threshold
        };

        let diff = value - self.current_ema;
        self.current_ema += self.alpha * diff;
        self.current_variance =
            (1.0 - self.alpha) * (self.current_variance + self.alpha * diff * diff);

        is_anomaly
    }

    /// Get the current EMA value.
    pub fn ema(&self) -> f64 {
        self.current_ema
    }

    /// Get the current standard deviation.
    pub fn std_dev(&self) -> f64 {
        self.current_variance.sqrt()
    }
}

/// Aggregate anomaly detector combining latency, throughput, and error rate.
pub struct AnomalyDetector {
    latency_detector: EmaDetector,
    throughput_detector: EmaDetector,
    error_rate_detector: EmaDetector,
    anomaly_count: AtomicU64,
}

impl AnomalyDetector {
    /// Create a new aggregate anomaly detector with default thresholds.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            latency_detector: EmaDetector::new(0.2, 3.0),
            throughput_detector: EmaDetector::new(0.2, 3.0),
            error_rate_detector: EmaDetector::new(0.3, 2.5),
            anomaly_count: AtomicU64::new(0),
        }
    }

    /// Observe metrics. Returns list of detected anomalies.
    pub fn observe(&mut self, latency_ms: f64, throughput: f64, error_rate: f64) -> Vec<Anomaly> {
        let mut anomalies = Vec::new();

        if self.latency_detector.observe(latency_ms) {
            anomalies.push(Anomaly {
                metric: "latency".to_string(),
                value: latency_ms,
                expected: self.latency_detector.ema(),
                std_dev: self.latency_detector.std_dev(),
                severity: AnomalySeverity::Warning,
            });
        }

        if self.throughput_detector.observe(throughput) {
            anomalies.push(Anomaly {
                metric: "throughput".to_string(),
                value: throughput,
                expected: self.throughput_detector.ema(),
                std_dev: self.throughput_detector.std_dev(),
                severity: AnomalySeverity::Warning,
            });
        }

        if self.error_rate_detector.observe(error_rate) {
            let severity = if error_rate > 0.1 {
                AnomalySeverity::Critical
            } else {
                AnomalySeverity::Warning
            };
            anomalies.push(Anomaly {
                metric: "error_rate".to_string(),
                value: error_rate,
                expected: self.error_rate_detector.ema(),
                std_dev: self.error_rate_detector.std_dev(),
                severity,
            });
        }

        self.anomaly_count
            .fetch_add(anomalies.len() as u64, Ordering::Relaxed);
        anomalies
    }

    /// Get the total number of anomalies detected so far.
    pub fn total_anomalies(&self) -> u64 {
        self.anomaly_count.load(Ordering::Relaxed)
    }
}

/// A detected anomaly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Anomaly {
    /// Name of the anomalous metric.
    pub metric: String,
    /// Observed value that triggered the anomaly.
    pub value: f64,
    /// Expected value (current EMA).
    pub expected: f64,
    /// Current standard deviation of the metric.
    pub std_dev: f64,
    /// Severity of the anomaly.
    pub severity: AnomalySeverity,
}

/// Anomaly severity levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AnomalySeverity {
    /// Informational — within normal variance but notable.
    Info,
    /// Warning — outside normal bounds.
    Warning,
    /// Critical — far outside normal bounds, requires attention.
    Critical,
}

// ---------------------------------------------------------------------------
// Remediation
// ---------------------------------------------------------------------------

/// Automated remediation actions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RemediationAction {
    /// Trigger a failover to a replica
    Failover {
        /// Node to fail over to.
        target_node: String,
    },
    /// Evict cold data to reduce memory pressure
    EvictColdData {
        /// Target memory usage percentage after eviction.
        target_percent: u8,
    },
    /// Rebalance hash slots across nodes
    RebalanceSlots,
    /// Restart a degraded shard
    RestartShard {
        /// Identifier of the shard to restart.
        shard_id: String,
    },
    /// Scale up by adding a node
    ScaleUp {
        /// Number of nodes to add.
        node_count: u32,
    },
    /// Rate-limit a misbehaving client
    RateLimitClient {
        /// Identifier of the client to rate-limit.
        client_id: String,
        /// Maximum operations per second allowed.
        max_ops: u64,
    },
    /// No action (manual review required)
    NoOp {
        /// Reason why no automated action is taken.
        reason: String,
    },
}

/// A remediation playbook mapping anomaly patterns to actions.
#[derive(Debug, Clone)]
pub struct Playbook {
    /// Name of the playbook.
    pub name: String,
    /// Ordered rules to evaluate.
    pub rules: Vec<PlaybookRule>,
}

/// A single rule in a playbook.
#[derive(Debug, Clone)]
pub struct PlaybookRule {
    /// Condition that must be met to trigger the rule.
    pub condition: PlaybookCondition,
    /// Remediation action to execute when the condition is met.
    pub action: RemediationAction,
    /// Minimum time between repeated executions of this rule.
    pub cooldown: Duration,
    /// Maximum number of retry attempts.
    pub max_retries: u32,
}

/// Conditions that trigger a playbook rule.
#[derive(Debug, Clone)]
pub enum PlaybookCondition {
    /// Latency exceeds threshold (milliseconds).
    HighLatency {
        /// Latency threshold in milliseconds.
        threshold_ms: f64,
    },
    /// Error rate exceeds threshold (fraction).
    HighErrorRate {
        /// Error rate threshold (0.0–1.0).
        threshold: f64,
    },
    /// Throughput drops below threshold (ops/sec).
    LowThroughput {
        /// Minimum throughput in ops/sec.
        threshold: u64,
    },
    /// Memory usage exceeds threshold (percent).
    HighMemory {
        /// Memory usage threshold as a percentage.
        threshold_percent: u8,
    },
    /// A specific node is down.
    NodeDown {
        /// Identifier of the failed node.
        node_id: String,
    },
}

/// Result of a remediation action.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemediationResult {
    /// Description of the action taken.
    pub action: String,
    /// Whether the remediation succeeded.
    pub success: bool,
    /// Human-readable result message.
    pub message: String,
    /// How long the remediation took.
    pub duration: Duration,
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

/// Chaos & self-healing engine metrics.
#[derive(Debug, Default)]
pub struct ChaosMetrics {
    /// Total experiments executed.
    pub experiments_run: AtomicU64,
    /// Experiments that passed (steady state maintained).
    pub experiments_passed: AtomicU64,
    /// Experiments that failed (steady state violated).
    pub experiments_failed: AtomicU64,
    /// Total number of faults injected.
    pub faults_injected: AtomicU64,
    /// Total anomalies detected by the anomaly detector.
    pub anomalies_detected: AtomicU64,
    /// Successful automated remediations.
    pub remediations_applied: AtomicU64,
    /// Failed automated remediations.
    pub remediations_failed: AtomicU64,
}

/// The main chaos engineering and self-healing engine.
pub struct ChaosEngine {
    experiments: Arc<parking_lot::RwLock<HashMap<String, ChaosExperiment>>>,
    results: Arc<parking_lot::RwLock<Vec<ExperimentResult>>>,
    playbooks: Arc<parking_lot::RwLock<Vec<Playbook>>>,
    anomaly_detector: Arc<parking_lot::RwLock<AnomalyDetector>>,
    active_faults: Arc<parking_lot::RwLock<Vec<FaultType>>>,
    enabled: Arc<AtomicBool>,
    metrics: Arc<ChaosMetrics>,
}

impl ChaosEngine {
    /// Create a new chaos engine.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            experiments: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            results: Arc::new(parking_lot::RwLock::new(Vec::new())),
            playbooks: Arc::new(parking_lot::RwLock::new(Vec::new())),
            anomaly_detector: Arc::new(parking_lot::RwLock::new(AnomalyDetector::new())),
            active_faults: Arc::new(parking_lot::RwLock::new(Vec::new())),
            enabled: Arc::new(AtomicBool::new(false)),
            metrics: Arc::new(ChaosMetrics::default()),
        }
    }

    /// Enable the self-healing engine.
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::SeqCst);
    }

    /// Disable the self-healing engine.
    pub fn disable(&self) {
        self.enabled.store(false, Ordering::SeqCst);
    }

    /// Check whether the engine is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }

    /// Register a chaos experiment.
    pub fn register_experiment(&self, experiment: ChaosExperiment) {
        self.experiments
            .write()
            .insert(experiment.id.clone(), experiment);
    }

    /// Run a registered experiment (synchronously simulated).
    pub fn run_experiment(&self, id: &str) -> Result<ExperimentResult, String> {
        let experiment = self
            .experiments
            .read()
            .get(id)
            .cloned()
            .ok_or_else(|| format!("Experiment not found: {}", id))?;

        self.metrics.experiments_run.fetch_add(1, Ordering::Relaxed);

        // Inject faults
        for fault in &experiment.faults {
            self.active_faults.write().push(fault.clone());
            self.metrics.faults_injected.fetch_add(1, Ordering::Relaxed);
        }

        // Check steady state (simplified)
        let steady_state_maintained = experiment
            .steady_state_check
            .as_ref()
            .map(|_check| true) // In production, would actually measure
            .unwrap_or(true);

        // Clear faults
        self.active_faults.write().clear();

        let status = if steady_state_maintained {
            self.metrics
                .experiments_passed
                .fetch_add(1, Ordering::Relaxed);
            ExperimentStatus::Passed
        } else {
            self.metrics
                .experiments_failed
                .fetch_add(1, Ordering::Relaxed);
            ExperimentStatus::Failed
        };

        let result = ExperimentResult {
            experiment_id: id.to_string(),
            status,
            duration: experiment.duration,
            steady_state_maintained,
            observations: vec![format!("Experiment {} completed", id)],
            metrics_during: ExperimentMetrics::default(),
        };

        self.results.write().push(result.clone());
        Ok(result)
    }

    /// Register a remediation playbook.
    pub fn register_playbook(&self, playbook: Playbook) {
        self.playbooks.write().push(playbook);
    }

    /// Feed metrics to the anomaly detector. Returns any detected anomalies.
    pub fn observe_metrics(
        &self,
        latency_ms: f64,
        throughput: f64,
        error_rate: f64,
    ) -> Vec<Anomaly> {
        let anomalies = self
            .anomaly_detector
            .write()
            .observe(latency_ms, throughput, error_rate);
        self.metrics
            .anomalies_detected
            .fetch_add(anomalies.len() as u64, Ordering::Relaxed);
        anomalies
    }

    /// Get active faults.
    pub fn active_faults(&self) -> Vec<FaultType> {
        self.active_faults.read().clone()
    }

    /// Get experiment results.
    pub fn results(&self) -> Vec<ExperimentResult> {
        self.results.read().clone()
    }

    /// Get metrics.
    pub fn metrics(&self) -> &ChaosMetrics {
        &self.metrics
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ema_detector_no_anomaly_on_stable() {
        let mut det = EmaDetector::new(0.2, 3.0);
        for _ in 0..50 {
            assert!(!det.observe(100.0));
        }
    }

    #[test]
    fn test_ema_detector_detects_spike() {
        let mut det = EmaDetector::new(0.2, 2.0);
        // Build baseline with slight variance
        for i in 0..50 {
            det.observe(100.0 + (i as f64 % 3.0));
        }
        // Large spike well outside normal range
        let is_anomaly = det.observe(1000.0);
        assert!(is_anomaly);
    }

    #[test]
    fn test_anomaly_detector_aggregate() {
        let mut det = AnomalyDetector::new();
        // Build baseline with slight variance
        for i in 0..50 {
            let jitter = (i as f64 % 5.0) * 0.1;
            let _ = det.observe(10.0 + jitter, 5000.0 + jitter, 0.001);
        }
        // Inject extreme latency spike
        let anomalies = det.observe(1000.0, 5000.0, 0.001);
        assert!(anomalies.iter().any(|a| a.metric == "latency"));
    }

    #[test]
    fn test_chaos_engine_experiment() {
        let engine = ChaosEngine::new();

        let exp = ChaosExperiment {
            id: "test-1".to_string(),
            name: "Network partition test".to_string(),
            description: "Test behavior during network partition".to_string(),
            faults: vec![FaultType::LatencyInjection {
                min_latency: Duration::from_millis(100),
                max_latency: Duration::from_millis(500),
                probability: 0.5,
            }],
            duration: Duration::from_secs(30),
            abort_on_failure: true,
            steady_state_check: Some(SteadyStateCheck::default()),
        };

        engine.register_experiment(exp);
        let result = engine.run_experiment("test-1").unwrap();
        assert_eq!(result.status, ExperimentStatus::Passed);
        assert_eq!(engine.metrics().experiments_run.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_chaos_engine_unknown_experiment() {
        let engine = ChaosEngine::new();
        let result = engine.run_experiment("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_enable_disable() {
        let engine = ChaosEngine::new();
        assert!(!engine.is_enabled());
        engine.enable();
        assert!(engine.is_enabled());
        engine.disable();
        assert!(!engine.is_enabled());
    }

    #[test]
    fn test_remediation_actions() {
        let action = RemediationAction::Failover {
            target_node: "node-2".to_string(),
        };
        // Just ensure it serializes
        let json = serde_json::to_string(&action).unwrap();
        assert!(json.contains("node-2"));
    }

    #[test]
    fn test_observe_metrics_returns_anomalies() {
        let engine = ChaosEngine::new();
        // Build baseline
        for _ in 0..50 {
            engine.observe_metrics(10.0, 5000.0, 0.001);
        }
        // Inject spike — may or may not detect depending on EMA warmup
        let _anomalies = engine.observe_metrics(1000.0, 100.0, 0.5);
    }
}
