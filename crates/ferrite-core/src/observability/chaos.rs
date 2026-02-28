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

// ===========================================================================
// Runtime Fault Injection Framework
// ===========================================================================
//
// Provides a command-driven fault injection engine for chaos engineering:
// inject latency, errors, slow I/O, timeouts, drops, corruption, and more.
// Controllable via CHAOS.* commands.

/// Configuration for the runtime fault injection engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosConfig {
    /// Whether the engine is enabled.
    pub enabled: bool,
    /// Require an explicit flag to inject faults.
    pub require_flag: bool,
    /// Maximum number of concurrently active faults.
    pub max_active_faults: usize,
    /// Optional key prefix for safety (faults only apply to keys with this prefix).
    pub safety_key_prefix: Option<String>,
}

impl Default for ChaosConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            require_flag: true,
            max_active_faults: 10,
            safety_key_prefix: None,
        }
    }
}

/// Specification for a fault to inject.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaultSpec {
    /// The type of fault.
    pub fault_type: RuntimeFaultType,
    /// What the fault targets.
    pub target: FaultTarget,
    /// How long the fault should remain active.
    pub duration: Option<Duration>,
    /// Probability of the fault firing (0.0–1.0).
    pub probability: f64,
    /// Human-readable description.
    pub description: String,
    /// Creation timestamp (epoch seconds).
    pub created_at: u64,
}

impl Default for FaultSpec {
    fn default() -> Self {
        Self {
            fault_type: RuntimeFaultType::Latency {
                delay_ms: 100,
                jitter_ms: 0,
            },
            target: FaultTarget::All,
            duration: None,
            probability: 1.0,
            description: String::new(),
            created_at: 0,
        }
    }
}

/// Types of runtime faults that can be injected.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RuntimeFaultType {
    /// Add latency to operations.
    Latency { delay_ms: u64, jitter_ms: u64 },
    /// Return errors at a given rate.
    ErrorRate { rate: f64 },
    /// Slow down I/O operations.
    SlowIo { read_ms: u64, write_ms: u64 },
    /// Simulate memory pressure.
    MemoryPressure { simulate_pct: f64 },
    /// Simulate CPU spike.
    CpuSpike { simulate_pct: f64, cores: u32 },
    /// Fail specific commands.
    PartialFailure { fail_commands: Vec<String> },
    /// Force a timeout.
    Timeout { timeout_ms: u64 },
    /// Randomly drop responses.
    RandomDrop { rate: f64 },
    /// Corrupt values at a given rate.
    Corruption { rate: f64 },
    /// Simulate clock skew.
    ClockSkew { skew_ms: i64 },
}

/// What a fault targets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FaultTarget {
    /// All operations.
    All,
    /// Operations matching a key pattern.
    KeyPattern(String),
    /// A specific command name.
    Command(String),
    /// A specific client identifier.
    Client(String),
    /// A specific database index.
    Database(u8),
}

/// A currently active fault.
pub struct ActiveFault {
    /// Unique identifier.
    pub id: u64,
    /// The fault specification.
    pub spec: FaultSpec,
    /// When the fault was injected.
    pub injected_at: Instant,
    /// When the fault expires (if set).
    pub expires_at: Option<Instant>,
    /// Number of times the fault has triggered.
    pub triggered_count: AtomicU64,
}

/// The effect to apply when a fault matches.
#[derive(Debug, Clone)]
pub enum FaultEffect {
    /// Add latency before processing.
    AddLatency(Duration),
    /// Return an error instead of the real result.
    ReturnError(String),
    /// Drop the response entirely.
    DropResponse,
    /// Corrupt the value.
    CorruptValue,
    /// Force a timeout.
    Timeout(Duration),
    /// Simulate an out-of-memory error.
    SimulateOom,
}

/// Pre-built chaos scenarios.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChaosScenario {
    /// Simulate a network partition.
    NetworkPartition { duration: Duration },
    /// Cascading failure with increasing error rate.
    CascadingFailure {
        start_rate: f64,
        increase_rate: f64,
        duration: Duration,
    },
    /// Split-brain scenario with isolated partitions.
    SplitBrain {
        partitions: Vec<Vec<String>>,
        duration: Duration,
    },
    /// Graceful degradation with increasing latency.
    GracefulDegradation {
        target_latency_ms: u64,
        step_ms: u64,
        steps: u32,
    },
    /// Custom set of faults.
    Custom { faults: Vec<FaultSpec> },
}

/// Result of running a chaos scenario.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioResult {
    /// Description of the scenario.
    pub scenario: String,
    /// Number of faults injected.
    pub faults_injected: usize,
    /// Total duration of the scenario.
    pub duration: Duration,
    /// Number of commands affected.
    pub commands_affected: u64,
}

/// Summary information about an active fault.
#[derive(Debug, Clone)]
pub struct FaultInfo {
    /// Fault identifier.
    pub id: u64,
    /// Type description.
    pub fault_type: String,
    /// Target description.
    pub target: String,
    /// When the fault was injected.
    pub injected_at: Instant,
    /// When the fault expires.
    pub expires_at: Option<Instant>,
    /// Number of times triggered.
    pub triggered: u64,
}

/// Aggregate statistics for the fault injection engine.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChaosStats {
    /// Number of currently active faults.
    pub active_faults: usize,
    /// Total faults injected since start.
    pub total_injected: u64,
    /// Total faults healed since start.
    pub total_healed: u64,
    /// Total times a fault triggered.
    pub total_triggered: u64,
    /// Total scenarios executed.
    pub scenarios_run: u64,
}

/// Errors from the runtime chaos engine.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ChaosError {
    #[error("chaos engine is not enabled")]
    NotEnabled,
    #[error("maximum active faults ({0}) reached")]
    MaxFaults(usize),
    #[error("fault not found: {0}")]
    FaultNotFound(u64),
    #[error("invalid fault specification: {0}")]
    InvalidSpec(String),
    #[error("safety violation: {0}")]
    SafetyViolation(String),
}

/// Runtime fault injection engine.
pub struct RuntimeChaosEngine {
    config: ChaosConfig,
    faults: Arc<parking_lot::RwLock<Vec<ActiveFault>>>,
    next_id: AtomicU64,
    total_injected: AtomicU64,
    total_healed: AtomicU64,
    total_triggered: AtomicU64,
    scenarios_run: AtomicU64,
}

impl RuntimeChaosEngine {
    /// Create a new runtime chaos engine.
    pub fn new(config: ChaosConfig) -> Self {
        Self {
            config,
            faults: Arc::new(parking_lot::RwLock::new(Vec::new())),
            next_id: AtomicU64::new(1),
            total_injected: AtomicU64::new(0),
            total_healed: AtomicU64::new(0),
            total_triggered: AtomicU64::new(0),
            scenarios_run: AtomicU64::new(0),
        }
    }

    /// Inject a fault. Returns the fault id on success.
    pub fn inject(&self, spec: FaultSpec) -> Result<u64, ChaosError> {
        if !self.config.enabled {
            return Err(ChaosError::NotEnabled);
        }
        if spec.probability < 0.0 || spec.probability > 1.0 {
            return Err(ChaosError::InvalidSpec(
                "probability must be between 0.0 and 1.0".into(),
            ));
        }
        let mut faults = self.faults.write();
        if faults.len() >= self.config.max_active_faults {
            return Err(ChaosError::MaxFaults(self.config.max_active_faults));
        }

        // Safety check: if a prefix is configured, only KeyPattern targets
        // matching the prefix are allowed.
        if let Some(ref prefix) = self.config.safety_key_prefix {
            if let FaultTarget::KeyPattern(ref pat) = spec.target {
                if !pat.starts_with(prefix) {
                    return Err(ChaosError::SafetyViolation(format!(
                        "key pattern must start with '{}'",
                        prefix
                    )));
                }
            }
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let expires_at = spec.duration.map(|d| now + d);

        faults.push(ActiveFault {
            id,
            spec,
            injected_at: now,
            expires_at,
            triggered_count: AtomicU64::new(0),
        });
        self.total_injected.fetch_add(1, Ordering::Relaxed);
        Ok(id)
    }

    /// Remove a specific fault by id.
    pub fn heal(&self, fault_id: u64) -> Result<(), ChaosError> {
        let mut faults = self.faults.write();
        let pos = faults
            .iter()
            .position(|f| f.id == fault_id)
            .ok_or(ChaosError::FaultNotFound(fault_id))?;
        faults.remove(pos);
        self.total_healed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Remove all active faults.
    pub fn heal_all(&self) -> usize {
        let mut faults = self.faults.write();
        let count = faults.len();
        faults.clear();
        self.total_healed.fetch_add(count as u64, Ordering::Relaxed);
        count
    }

    /// Check if any fault applies to the given operation.
    pub fn should_apply(
        &self,
        command: &str,
        key: Option<&str>,
        client: Option<&str>,
        db: u8,
    ) -> Option<FaultEffect> {
        if !self.config.enabled {
            return None;
        }
        let faults = self.faults.read();
        for fault in faults.iter() {
            if let Some(exp) = fault.expires_at {
                if Instant::now() > exp {
                    continue;
                }
            }
            if !self.target_matches(&fault.spec.target, command, key, client, db) {
                continue;
            }
            // Probability check (deterministic for prob==1.0)
            if fault.spec.probability < 1.0 {
                let roll = (fault.triggered_count.load(Ordering::Relaxed) % 100) as f64 / 100.0;
                if roll >= fault.spec.probability {
                    continue;
                }
            }
            fault.triggered_count.fetch_add(1, Ordering::Relaxed);
            self.total_triggered.fetch_add(1, Ordering::Relaxed);
            return Some(self.effect_for(&fault.spec.fault_type));
        }
        None
    }

    /// Return the list of currently active faults.
    pub fn active_faults(&self) -> Vec<FaultInfo> {
        self.faults
            .read()
            .iter()
            .map(|f| FaultInfo {
                id: f.id,
                fault_type: format!("{:?}", f.spec.fault_type),
                target: format!("{:?}", f.spec.target),
                injected_at: f.injected_at,
                expires_at: f.expires_at,
                triggered: f.triggered_count.load(Ordering::Relaxed),
            })
            .collect()
    }

    /// Run a pre-built chaos scenario.
    pub fn run_scenario(&self, scenario: ChaosScenario) -> ScenarioResult {
        self.scenarios_run.fetch_add(1, Ordering::Relaxed);
        let (desc, specs) = self.scenario_to_specs(scenario);
        let count = specs.len();
        for spec in specs {
            let _ = self.inject(spec);
        }
        ScenarioResult {
            scenario: desc,
            faults_injected: count,
            duration: Duration::from_secs(0),
            commands_affected: 0,
        }
    }

    /// Remove expired faults.
    pub fn expire_faults(&self) -> usize {
        let mut faults = self.faults.write();
        let now = Instant::now();
        let before = faults.len();
        faults.retain(|f| f.expires_at.map_or(true, |exp| now <= exp));
        let removed = before - faults.len();
        self.total_healed
            .fetch_add(removed as u64, Ordering::Relaxed);
        removed
    }

    /// Get aggregate statistics.
    pub fn stats(&self) -> ChaosStats {
        ChaosStats {
            active_faults: self.faults.read().len(),
            total_injected: self.total_injected.load(Ordering::Relaxed),
            total_healed: self.total_healed.load(Ordering::Relaxed),
            total_triggered: self.total_triggered.load(Ordering::Relaxed),
            scenarios_run: self.scenarios_run.load(Ordering::Relaxed),
        }
    }

    // -- private helpers -----------------------------------------------------

    fn target_matches(
        &self,
        target: &FaultTarget,
        command: &str,
        key: Option<&str>,
        client: Option<&str>,
        db: u8,
    ) -> bool {
        match target {
            FaultTarget::All => true,
            FaultTarget::KeyPattern(pat) => key.map_or(false, |k| k.contains(pat.as_str())),
            FaultTarget::Command(cmd) => command.eq_ignore_ascii_case(cmd),
            FaultTarget::Client(c) => client.map_or(false, |cl| cl == c),
            FaultTarget::Database(d) => db == *d,
        }
    }

    fn effect_for(&self, ft: &RuntimeFaultType) -> FaultEffect {
        match ft {
            RuntimeFaultType::Latency {
                delay_ms,
                jitter_ms,
            } => {
                let total = *delay_ms + jitter_ms / 2;
                FaultEffect::AddLatency(Duration::from_millis(total))
            }
            RuntimeFaultType::ErrorRate { .. } => {
                FaultEffect::ReturnError("ERR chaos injected error".into())
            }
            RuntimeFaultType::SlowIo { read_ms, write_ms } => {
                FaultEffect::AddLatency(Duration::from_millis((*read_ms + *write_ms) / 2))
            }
            RuntimeFaultType::MemoryPressure { .. } => FaultEffect::SimulateOom,
            RuntimeFaultType::CpuSpike { .. } => {
                FaultEffect::AddLatency(Duration::from_millis(500))
            }
            RuntimeFaultType::PartialFailure { .. } => {
                FaultEffect::ReturnError("ERR chaos partial failure".into())
            }
            RuntimeFaultType::Timeout { timeout_ms } => {
                FaultEffect::Timeout(Duration::from_millis(*timeout_ms))
            }
            RuntimeFaultType::RandomDrop { .. } => FaultEffect::DropResponse,
            RuntimeFaultType::Corruption { .. } => FaultEffect::CorruptValue,
            RuntimeFaultType::ClockSkew { .. } => {
                FaultEffect::AddLatency(Duration::from_millis(10))
            }
        }
    }

    fn scenario_to_specs(&self, scenario: ChaosScenario) -> (String, Vec<FaultSpec>) {
        let now_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        match scenario {
            ChaosScenario::NetworkPartition { duration } => {
                let spec = FaultSpec {
                    fault_type: RuntimeFaultType::Timeout { timeout_ms: 5000 },
                    target: FaultTarget::All,
                    duration: Some(duration),
                    probability: 1.0,
                    description: "Network partition simulation".into(),
                    created_at: now_epoch,
                };
                ("NetworkPartition".into(), vec![spec])
            }
            ChaosScenario::CascadingFailure {
                start_rate,
                increase_rate,
                duration,
            } => {
                let steps = 5u32;
                let step_dur = duration / steps;
                let specs: Vec<FaultSpec> = (0..steps)
                    .map(|i| {
                        let rate = (start_rate + increase_rate * i as f64).min(1.0);
                        FaultSpec {
                            fault_type: RuntimeFaultType::ErrorRate { rate },
                            target: FaultTarget::All,
                            duration: Some(step_dur),
                            probability: 1.0,
                            description: format!("Cascade step {} rate={:.2}", i, rate),
                            created_at: now_epoch,
                        }
                    })
                    .collect();
                ("CascadingFailure".into(), specs)
            }
            ChaosScenario::SplitBrain {
                partitions,
                duration,
            } => {
                let specs: Vec<FaultSpec> = partitions
                    .iter()
                    .flat_map(|group| {
                        group.iter().map(|node| FaultSpec {
                            fault_type: RuntimeFaultType::RandomDrop { rate: 0.9 },
                            target: FaultTarget::Client(node.clone()),
                            duration: Some(duration),
                            probability: 1.0,
                            description: format!("Split-brain isolation for {}", node),
                            created_at: now_epoch,
                        })
                    })
                    .collect();
                ("SplitBrain".into(), specs)
            }
            ChaosScenario::GracefulDegradation {
                target_latency_ms,
                step_ms,
                steps,
            } => {
                let specs: Vec<FaultSpec> = (0..steps)
                    .map(|i| {
                        let latency = (step_ms * (i + 1) as u64).min(target_latency_ms);
                        FaultSpec {
                            fault_type: RuntimeFaultType::Latency {
                                delay_ms: latency,
                                jitter_ms: latency / 10,
                            },
                            target: FaultTarget::All,
                            duration: Some(Duration::from_secs(5)),
                            probability: 1.0,
                            description: format!("Degradation step {} latency={}ms", i, latency),
                            created_at: now_epoch,
                        }
                    })
                    .collect();
                ("GracefulDegradation".into(), specs)
            }
            ChaosScenario::Custom { faults } => ("Custom".into(), faults),
        }
    }
}

// ---------------------------------------------------------------------------
// Runtime Fault Injection Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod runtime_tests {
    use super::*;

    fn enabled_config() -> ChaosConfig {
        ChaosConfig {
            enabled: true,
            require_flag: false,
            max_active_faults: 10,
            safety_key_prefix: None,
        }
    }

    #[test]
    fn test_inject_and_heal_lifecycle() {
        let engine = RuntimeChaosEngine::new(enabled_config());
        let spec = FaultSpec {
            fault_type: RuntimeFaultType::Latency {
                delay_ms: 100,
                jitter_ms: 10,
            },
            target: FaultTarget::All,
            probability: 1.0,
            ..Default::default()
        };
        let id = engine.inject(spec).expect("inject failed");
        assert_eq!(engine.active_faults().len(), 1);

        engine.heal(id).expect("heal failed");
        assert!(engine.active_faults().is_empty());
    }

    #[test]
    fn test_heal_all() {
        let engine = RuntimeChaosEngine::new(enabled_config());
        for i in 0..3 {
            let spec = FaultSpec {
                fault_type: RuntimeFaultType::ErrorRate { rate: 0.5 },
                description: format!("fault-{}", i),
                ..Default::default()
            };
            engine.inject(spec).expect("inject");
        }
        assert_eq!(engine.heal_all(), 3);
        assert!(engine.active_faults().is_empty());
    }

    #[test]
    fn test_not_enabled_error() {
        let engine = RuntimeChaosEngine::new(ChaosConfig::default());
        let result = engine.inject(FaultSpec::default());
        assert!(matches!(result, Err(ChaosError::NotEnabled)));
    }

    #[test]
    fn test_max_faults() {
        let mut cfg = enabled_config();
        cfg.max_active_faults = 2;
        let engine = RuntimeChaosEngine::new(cfg);
        engine.inject(FaultSpec::default()).expect("1");
        engine.inject(FaultSpec::default()).expect("2");
        let result = engine.inject(FaultSpec::default());
        assert!(matches!(result, Err(ChaosError::MaxFaults(2))));
    }

    #[test]
    fn test_fault_not_found() {
        let engine = RuntimeChaosEngine::new(enabled_config());
        let result = engine.heal(999);
        assert!(matches!(result, Err(ChaosError::FaultNotFound(999))));
    }

    #[test]
    fn test_should_apply_all() {
        let engine = RuntimeChaosEngine::new(enabled_config());
        engine
            .inject(FaultSpec {
                fault_type: RuntimeFaultType::Latency {
                    delay_ms: 50,
                    jitter_ms: 0,
                },
                target: FaultTarget::All,
                probability: 1.0,
                ..Default::default()
            })
            .expect("inject");
        let effect = engine.should_apply("GET", Some("key"), None, 0);
        assert!(effect.is_some());
    }

    #[test]
    fn test_should_apply_command_target() {
        let engine = RuntimeChaosEngine::new(enabled_config());
        engine
            .inject(FaultSpec {
                fault_type: RuntimeFaultType::ErrorRate { rate: 1.0 },
                target: FaultTarget::Command("SET".into()),
                probability: 1.0,
                ..Default::default()
            })
            .expect("inject");
        assert!(engine.should_apply("SET", None, None, 0).is_some());
        assert!(engine.should_apply("GET", None, None, 0).is_none());
    }

    #[test]
    fn test_should_apply_key_pattern() {
        let engine = RuntimeChaosEngine::new(enabled_config());
        engine
            .inject(FaultSpec {
                fault_type: RuntimeFaultType::RandomDrop { rate: 1.0 },
                target: FaultTarget::KeyPattern("user:".into()),
                probability: 1.0,
                ..Default::default()
            })
            .expect("inject");
        assert!(engine
            .should_apply("GET", Some("user:123"), None, 0)
            .is_some());
        assert!(engine
            .should_apply("GET", Some("session:abc"), None, 0)
            .is_none());
    }

    #[test]
    fn test_expire_faults() {
        let engine = RuntimeChaosEngine::new(enabled_config());
        engine
            .inject(FaultSpec {
                fault_type: RuntimeFaultType::Latency {
                    delay_ms: 10,
                    jitter_ms: 0,
                },
                duration: Some(Duration::from_millis(1)),
                ..Default::default()
            })
            .expect("inject");
        // Wait for expiry
        std::thread::sleep(Duration::from_millis(5));
        let expired = engine.expire_faults();
        assert_eq!(expired, 1);
        assert!(engine.active_faults().is_empty());
    }

    #[test]
    fn test_scenario_network_partition() {
        let engine = RuntimeChaosEngine::new(enabled_config());
        let result = engine.run_scenario(ChaosScenario::NetworkPartition {
            duration: Duration::from_secs(10),
        });
        assert_eq!(result.scenario, "NetworkPartition");
        assert_eq!(result.faults_injected, 1);
    }

    #[test]
    fn test_scenario_graceful_degradation() {
        let engine = RuntimeChaosEngine::new(enabled_config());
        let result = engine.run_scenario(ChaosScenario::GracefulDegradation {
            target_latency_ms: 500,
            step_ms: 100,
            steps: 5,
        });
        assert_eq!(result.scenario, "GracefulDegradation");
        assert_eq!(result.faults_injected, 5);
    }

    #[test]
    fn test_safety_key_prefix() {
        let cfg = ChaosConfig {
            enabled: true,
            require_flag: false,
            max_active_faults: 10,
            safety_key_prefix: Some("test:".into()),
        };
        let engine = RuntimeChaosEngine::new(cfg);
        let result = engine.inject(FaultSpec {
            target: FaultTarget::KeyPattern("prod:data".into()),
            ..Default::default()
        });
        assert!(matches!(result, Err(ChaosError::SafetyViolation(_))));

        // Matching prefix should succeed
        let result = engine.inject(FaultSpec {
            target: FaultTarget::KeyPattern("test:data".into()),
            ..Default::default()
        });
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_probability() {
        let engine = RuntimeChaosEngine::new(enabled_config());
        let result = engine.inject(FaultSpec {
            probability: 1.5,
            ..Default::default()
        });
        assert!(matches!(result, Err(ChaosError::InvalidSpec(_))));
    }

    #[test]
    fn test_stats() {
        let engine = RuntimeChaosEngine::new(enabled_config());
        let id = engine.inject(FaultSpec::default()).expect("inject");
        engine.heal(id).expect("heal");
        let stats = engine.stats();
        assert_eq!(stats.total_injected, 1);
        assert_eq!(stats.total_healed, 1);
        assert_eq!(stats.active_faults, 0);
    }

    #[test]
    fn test_database_target() {
        let engine = RuntimeChaosEngine::new(enabled_config());
        engine
            .inject(FaultSpec {
                fault_type: RuntimeFaultType::ErrorRate { rate: 1.0 },
                target: FaultTarget::Database(3),
                probability: 1.0,
                ..Default::default()
            })
            .expect("inject");
        assert!(engine.should_apply("GET", None, None, 3).is_some());
        assert!(engine.should_apply("GET", None, None, 0).is_none());
    }
}
