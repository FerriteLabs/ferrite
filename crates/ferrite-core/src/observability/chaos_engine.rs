//! Chaos Experiment Runner
//!
//! Provides fault injection, scenario management, and safety validation for
//! chaos engineering experiments. Complements the existing `chaos` module with
//! an actionable experiment runner and builtin scenarios.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                    ChaosEngine                          │
//! │  ┌──────────────┐  ┌───────────────┐  ┌─────────────┐  │
//! │  │ FaultInjector │  │ ScenarioRunner│  │  Safety     │  │
//! │  │ (inject/heal) │  │ (step-based)  │  │  Validator  │  │
//! │  └──────────────┘  └───────────────┘  └─────────────┘  │
//! └─────────────────────────────────────────────────────────┘
//! ```

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/// Errors returned by the chaos engine.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ChaosError {
    /// Chaos engineering is not enabled.
    #[error("chaos engineering is not enabled")]
    NotEnabled,

    /// Caller lacks the required ACL permission.
    #[error("permission denied: chaos ACL permission required")]
    PermissionDenied,

    /// Maximum concurrent faults exceeded.
    #[error("max concurrent faults exceeded (limit: {0})")]
    MaxFaultsExceeded(usize),

    /// Fault with the given ID was not found.
    #[error("fault not found: {0}")]
    FaultNotFound(String),

    /// Fault injection failed.
    #[error("injection failed: {0}")]
    InjectionFailed(String),

    /// Fault healing failed.
    #[error("heal failed: {0}")]
    HealFailed(String),

    /// Scenario execution timed out.
    #[error("scenario timed out")]
    ScenarioTimeout,

    /// A safety violation was detected.
    #[error("safety violation: {0}")]
    SafetyViolation(String),

    /// Internal engine error.
    #[error("internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Core value types
// ---------------------------------------------------------------------------

/// Unique identifier for an injected fault, wrapping a UUID.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FaultId(pub String);

impl FaultId {
    /// Generate a new random fault ID.
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

impl Default for FaultId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for FaultId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Types of faults that can be injected by the chaos engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FaultType {
    /// Simulate a full network partition.
    NetworkPartition,
    /// Add latency to network operations.
    NetworkLatency {
        /// Milliseconds of latency to add.
        added_ms: u64,
    },
    /// Drop a percentage of packets.
    PacketLoss {
        /// Percentage of packets to drop (0.0–100.0).
        percent: f64,
    },
    /// Slow down disk I/O by a factor.
    DiskSlowdown {
        /// Slowdown multiplier (e.g., 2.0 = 2× slower).
        factor: f64,
    },
    /// Simulate a full disk.
    DiskFull,
    /// Simulate memory pressure.
    MemoryPressure {
        /// Target memory usage percentage (0.0–100.0).
        percent: f64,
    },
    /// Simulate a CPU spike.
    CpuSpike {
        /// Target CPU usage percentage (0.0–100.0).
        percent: f64,
    },
    /// Skew the system clock.
    ClockSkew {
        /// Clock offset in milliseconds (positive = forward, negative = backward).
        offset_ms: i64,
    },
    /// Pause a process for a duration.
    ProcessPause {
        /// Duration of the pause in milliseconds.
        duration_ms: u64,
    },
    /// Drop a percentage of connections.
    ConnectionDrop {
        /// Percentage of connections to drop (0.0–100.0).
        percent: f64,
    },
}

impl fmt::Display for FaultType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NetworkPartition => write!(f, "NetworkPartition"),
            Self::NetworkLatency { added_ms } => write!(f, "NetworkLatency({}ms)", added_ms),
            Self::PacketLoss { percent } => write!(f, "PacketLoss({:.1}%)", percent),
            Self::DiskSlowdown { factor } => write!(f, "DiskSlowdown({:.1}x)", factor),
            Self::DiskFull => write!(f, "DiskFull"),
            Self::MemoryPressure { percent } => write!(f, "MemoryPressure({:.1}%)", percent),
            Self::CpuSpike { percent } => write!(f, "CpuSpike({:.1}%)", percent),
            Self::ClockSkew { offset_ms } => write!(f, "ClockSkew({}ms)", offset_ms),
            Self::ProcessPause { duration_ms } => write!(f, "ProcessPause({}ms)", duration_ms),
            Self::ConnectionDrop { percent } => write!(f, "ConnectionDrop({:.1}%)", percent),
        }
    }
}

/// Target for fault injection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FaultTarget {
    /// The local node.
    Local,
    /// A specific node by identifier.
    Node(String),
    /// All replicas in the cluster.
    AllReplicas,
    /// A randomly selected node.
    RandomNode,
    /// A specific shard.
    Shard(u16),
}

/// Intensity of a fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FaultIntensity {
    /// Minimal disruption.
    Low,
    /// Moderate disruption.
    Medium,
    /// Significant disruption.
    High,
    /// Maximum disruption.
    Extreme,
}

/// Specification for a fault to inject.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaultSpec {
    /// The type of fault to inject.
    pub fault_type: FaultType,
    /// Where to inject the fault.
    pub target: FaultTarget,
    /// Optional duration in seconds; `None` means indefinite until healed.
    pub duration_secs: Option<u64>,
    /// How intense the fault should be.
    pub intensity: FaultIntensity,
    /// Optional human-readable description.
    pub description: Option<String>,
}

/// Status of an active fault.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FaultStatus {
    /// Fault is currently active.
    Active,
    /// Fault is in the process of being healed.
    Healing,
    /// Fault has been fully healed.
    Healed,
    /// Fault healing failed.
    Failed(String),
}

/// An actively injected fault.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveFault {
    /// Unique identifier.
    pub id: FaultId,
    /// The fault specification.
    pub spec: FaultSpec,
    /// When the fault was injected.
    pub injected_at: DateTime<Utc>,
    /// When the fault will be auto-healed, if applicable.
    pub auto_heal_at: Option<DateTime<Utc>>,
    /// Current status.
    pub status: FaultStatus,
}

// ---------------------------------------------------------------------------
// Scenario types
// ---------------------------------------------------------------------------

/// A pre-built chaos experiment scenario.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosScenario {
    /// Scenario name.
    pub name: String,
    /// Human-readable description.
    pub description: String,
    /// Ordered steps to execute.
    pub steps: Vec<ScenarioStep>,
    /// Safety assertions to validate after execution.
    pub safety_assertions: Vec<SafetyAssertion>,
    /// Maximum execution time in seconds.
    pub timeout_secs: u64,
}

/// A single step in a chaos scenario.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioStep {
    /// The action to perform.
    pub action: StepAction,
    /// Delay in milliseconds after this step completes.
    pub delay_after_ms: u64,
    /// Human-readable description of the step.
    pub description: String,
}

/// Actions that can be performed in a scenario step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StepAction {
    /// Inject a fault.
    InjectFault(FaultSpec),
    /// Heal a specific fault by ID.
    HealFault(FaultId),
    /// Heal all active faults.
    HealAll,
    /// Wait for a duration in milliseconds.
    WaitMs(u64),
    /// Run safety validation.
    ValidateSafety,
    /// Check data consistency.
    CheckDataConsistency,
    /// Measure current latency.
    MeasureLatency,
}

/// A safety assertion to validate after a scenario.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafetyAssertion {
    /// The type of assertion.
    pub assertion_type: AssertionType,
    /// Human-readable description.
    pub description: String,
}

/// Types of safety assertions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AssertionType {
    /// Assert no data was lost.
    NoDataLoss,
    /// Assert recovery happened within a time limit.
    RecoveryWithin {
        /// Maximum acceptable recovery time in seconds.
        max_secs: u64,
    },
    /// Assert latency stays below a threshold.
    LatencyBelow {
        /// Maximum acceptable latency in milliseconds.
        max_ms: u64,
    },
    /// Assert availability stays above a threshold.
    AvailabilityAbove {
        /// Minimum acceptable availability percentage (0.0–100.0).
        min_percent: f64,
    },
    /// Assert no split-brain condition occurred.
    NoSplitBrain,
    /// Assert a leader was elected.
    LeaderElected,
}

/// Result of a completed scenario execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioResult {
    /// Name of the executed scenario.
    pub scenario_name: String,
    /// Whether the scenario passed all assertions.
    pub passed: bool,
    /// Number of steps completed.
    pub steps_completed: usize,
    /// Total number of steps.
    pub steps_total: usize,
    /// Assertions that passed.
    pub assertions_passed: Vec<String>,
    /// Assertions that failed.
    pub assertions_failed: Vec<String>,
    /// Total execution duration in milliseconds.
    pub duration_ms: u64,
    /// Human-readable observations from the run.
    pub observations: Vec<String>,
}

// ---------------------------------------------------------------------------
// Safety types
// ---------------------------------------------------------------------------

/// Report from a safety validation check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafetyReport {
    /// Whether the system is considered safe.
    pub safe: bool,
    /// Individual safety checks.
    pub checks: Vec<SafetyCheck>,
}

/// A single safety check result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafetyCheck {
    /// Name of the check.
    pub name: String,
    /// Whether the check passed.
    pub passed: bool,
    /// Human-readable result message.
    pub message: String,
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

/// Statistics about chaos engine activity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosStats {
    /// Total faults injected since engine creation.
    pub total_faults_injected: u64,
    /// Total faults healed.
    pub total_faults_healed: u64,
    /// Total scenarios executed.
    pub total_scenarios_run: u64,
    /// Scenarios that passed.
    pub scenarios_passed: u64,
    /// Scenarios that failed.
    pub scenarios_failed: u64,
    /// Total automatic heals triggered.
    pub total_auto_heals: u64,
    /// Total safety violations detected.
    pub safety_violations: u64,
}

/// Atomic counters for chaos engine stats.
#[derive(Debug, Default)]
struct AtomicChaosStats {
    total_faults_injected: AtomicU64,
    total_faults_healed: AtomicU64,
    total_scenarios_run: AtomicU64,
    scenarios_passed: AtomicU64,
    scenarios_failed: AtomicU64,
    total_auto_heals: AtomicU64,
    safety_violations: AtomicU64,
}

impl AtomicChaosStats {
    fn snapshot(&self) -> ChaosStats {
        ChaosStats {
            total_faults_injected: self.total_faults_injected.load(Ordering::Relaxed),
            total_faults_healed: self.total_faults_healed.load(Ordering::Relaxed),
            total_scenarios_run: self.total_scenarios_run.load(Ordering::Relaxed),
            scenarios_passed: self.scenarios_passed.load(Ordering::Relaxed),
            scenarios_failed: self.scenarios_failed.load(Ordering::Relaxed),
            total_auto_heals: self.total_auto_heals.load(Ordering::Relaxed),
            safety_violations: self.safety_violations.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the chaos engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosConfig {
    /// Whether chaos engineering is enabled.
    pub enabled: bool,
    /// Require ACL permission to inject faults.
    pub require_acl_permission: bool,
    /// Maximum number of concurrent active faults.
    pub max_concurrent_faults: usize,
    /// Seconds before an indefinite fault is auto-healed.
    pub auto_heal_timeout_secs: u64,
    /// Whether to run safety checks before injection.
    pub safety_checks_enabled: bool,
    /// Fault types that are allowed to be injected.
    pub allowed_fault_types: Vec<FaultType>,
}

impl Default for ChaosConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            require_acl_permission: true,
            max_concurrent_faults: 5,
            auto_heal_timeout_secs: 300,
            safety_checks_enabled: true,
            allowed_fault_types: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

/// The chaos experiment runner.
///
/// Orchestrates fault injection, scenario execution, and safety validation.
pub struct ChaosEngine {
    config: RwLock<ChaosConfig>,
    active_faults: DashMap<FaultId, ActiveFault>,
    stats: Arc<AtomicChaosStats>,
}

impl ChaosEngine {
    /// Create a new chaos engine with the given configuration.
    pub fn new(config: ChaosConfig) -> Self {
        Self {
            config: RwLock::new(config),
            active_faults: DashMap::new(),
            stats: Arc::new(AtomicChaosStats::default()),
        }
    }

    /// Inject a fault into the system.
    ///
    /// Returns a [`FaultId`] that can later be used to heal the fault.
    pub fn inject_fault(&self, fault: FaultSpec) -> Result<FaultId, ChaosError> {
        let config = self.config.read();

        if !config.enabled {
            return Err(ChaosError::NotEnabled);
        }

        if !config.allowed_fault_types.is_empty() {
            let type_name = fault_type_discriminant(&fault.fault_type);
            let allowed = config
                .allowed_fault_types
                .iter()
                .any(|ft| fault_type_discriminant(ft) == type_name);
            if !allowed {
                return Err(ChaosError::InjectionFailed(format!(
                    "fault type {} is not in the allowed list",
                    fault.fault_type
                )));
            }
        }

        let active_count = self
            .active_faults
            .iter()
            .filter(|f| f.status == FaultStatus::Active)
            .count();
        if active_count >= config.max_concurrent_faults {
            return Err(ChaosError::MaxFaultsExceeded(config.max_concurrent_faults));
        }

        if config.safety_checks_enabled {
            let report = self.run_safety_checks();
            if !report.safe {
                let failures: Vec<_> = report
                    .checks
                    .iter()
                    .filter(|c| !c.passed)
                    .map(|c| c.message.clone())
                    .collect();
                return Err(ChaosError::SafetyViolation(failures.join("; ")));
            }
        }

        let now = Utc::now();
        let id = FaultId::new();
        let auto_heal_at = fault
            .duration_secs
            .map(|secs| now + chrono::Duration::seconds(secs as i64))
            .or_else(|| {
                Some(now + chrono::Duration::seconds(config.auto_heal_timeout_secs as i64))
            });

        let active = ActiveFault {
            id: id.clone(),
            spec: fault,
            injected_at: now,
            auto_heal_at,
            status: FaultStatus::Active,
        };

        self.active_faults.insert(id.clone(), active);
        self.stats
            .total_faults_injected
            .fetch_add(1, Ordering::Relaxed);

        Ok(id)
    }

    /// Heal (remove) a specific active fault.
    pub fn heal_fault(&self, id: &FaultId) -> Result<(), ChaosError> {
        let mut entry = self
            .active_faults
            .get_mut(id)
            .ok_or_else(|| ChaosError::FaultNotFound(id.to_string()))?;

        match &entry.status {
            FaultStatus::Healed => {
                return Err(ChaosError::HealFailed("fault already healed".to_string()));
            }
            FaultStatus::Failed(msg) => {
                return Err(ChaosError::HealFailed(format!(
                    "fault in failed state: {}",
                    msg
                )));
            }
            _ => {}
        }

        entry.status = FaultStatus::Healing;
        drop(entry);

        // Mark as healed
        if let Some(mut entry) = self.active_faults.get_mut(id) {
            entry.status = FaultStatus::Healed;
        }

        self.stats
            .total_faults_healed
            .fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Heal all active faults. Returns the number of faults healed.
    pub fn heal_all(&self) -> usize {
        let mut healed = 0usize;
        for mut entry in self.active_faults.iter_mut() {
            if entry.status == FaultStatus::Active || entry.status == FaultStatus::Healing {
                entry.status = FaultStatus::Healed;
                healed += 1;
            }
        }
        self.stats
            .total_faults_healed
            .fetch_add(healed as u64, Ordering::Relaxed);
        healed
    }

    /// Run a chaos scenario end-to-end.
    pub fn run_scenario(&self, scenario: ChaosScenario) -> ScenarioResult {
        let start = std::time::Instant::now();
        let steps_total = scenario.steps.len();
        let mut steps_completed = 0usize;
        let mut observations = Vec::new();
        let mut injected_ids: Vec<FaultId> = Vec::new();

        self.stats
            .total_scenarios_run
            .fetch_add(1, Ordering::Relaxed);

        let timeout = std::time::Duration::from_secs(scenario.timeout_secs);

        for step in &scenario.steps {
            if start.elapsed() > timeout {
                observations.push("Scenario timed out".to_string());
                break;
            }

            match &step.action {
                StepAction::InjectFault(spec) => match self.inject_fault(spec.clone()) {
                    Ok(id) => {
                        observations.push(format!("Injected fault {}: {}", id, spec.fault_type));
                        injected_ids.push(id);
                    }
                    Err(e) => {
                        observations.push(format!("Failed to inject fault: {}", e));
                    }
                },
                StepAction::HealFault(id) => match self.heal_fault(id) {
                    Ok(()) => {
                        observations.push(format!("Healed fault {}", id));
                    }
                    Err(e) => {
                        observations.push(format!("Failed to heal fault {}: {}", id, e));
                    }
                },
                StepAction::HealAll => {
                    let count = self.heal_all();
                    observations.push(format!("Healed all {} active faults", count));
                }
                StepAction::WaitMs(ms) => {
                    std::thread::sleep(std::time::Duration::from_millis(*ms));
                    observations.push(format!("Waited {}ms", ms));
                }
                StepAction::ValidateSafety => {
                    let report = self.validate_safety();
                    observations.push(format!("Safety validation: safe={}", report.safe));
                }
                StepAction::CheckDataConsistency => {
                    observations.push("Data consistency check: passed (simulated)".to_string());
                }
                StepAction::MeasureLatency => {
                    observations.push("Latency measurement: captured (simulated)".to_string());
                }
            }
            steps_completed += 1;

            if step.delay_after_ms > 0 {
                std::thread::sleep(std::time::Duration::from_millis(step.delay_after_ms));
            }
        }

        // Evaluate safety assertions
        let mut assertions_passed = Vec::new();
        let mut assertions_failed = Vec::new();

        for assertion in &scenario.safety_assertions {
            let passed = self.evaluate_assertion(&assertion.assertion_type);
            if passed {
                assertions_passed.push(assertion.description.clone());
            } else {
                assertions_failed.push(assertion.description.clone());
            }
        }

        let passed = assertions_failed.is_empty();
        if passed {
            self.stats.scenarios_passed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.scenarios_failed.fetch_add(1, Ordering::Relaxed);
            self.stats
                .safety_violations
                .fetch_add(assertions_failed.len() as u64, Ordering::Relaxed);
        }

        let duration_ms = start.elapsed().as_millis() as u64;

        ScenarioResult {
            scenario_name: scenario.name,
            passed,
            steps_completed,
            steps_total,
            assertions_passed,
            assertions_failed,
            duration_ms,
            observations,
        }
    }

    /// List all active (non-healed) faults.
    pub fn list_active_faults(&self) -> Vec<ActiveFault> {
        self.active_faults
            .iter()
            .filter(|f| f.status == FaultStatus::Active)
            .map(|f| f.value().clone())
            .collect()
    }

    /// Run a safety validation and return a report.
    pub fn validate_safety(&self) -> SafetyReport {
        self.run_safety_checks()
    }

    /// Get a snapshot of engine statistics.
    pub fn get_stats(&self) -> ChaosStats {
        self.stats.snapshot()
    }

    /// Process auto-heals for faults that have exceeded their timeout.
    ///
    /// Returns the number of faults auto-healed.
    pub fn process_auto_heals(&self) -> usize {
        let now = Utc::now();
        let mut healed = 0usize;

        for mut entry in self.active_faults.iter_mut() {
            if entry.status != FaultStatus::Active {
                continue;
            }
            if let Some(heal_at) = entry.auto_heal_at {
                if now >= heal_at {
                    entry.status = FaultStatus::Healed;
                    healed += 1;
                }
            }
        }

        if healed > 0 {
            self.stats
                .total_auto_heals
                .fetch_add(healed as u64, Ordering::Relaxed);
            self.stats
                .total_faults_healed
                .fetch_add(healed as u64, Ordering::Relaxed);
        }

        healed
    }

    // -- private helpers -----------------------------------------------------

    fn run_safety_checks(&self) -> SafetyReport {
        let mut checks = Vec::new();

        // Check 1: Active fault count within limits
        let active_count = self
            .active_faults
            .iter()
            .filter(|f| f.status == FaultStatus::Active)
            .count();
        let max = self.config.read().max_concurrent_faults;
        checks.push(SafetyCheck {
            name: "active_fault_limit".to_string(),
            passed: active_count < max,
            message: format!("{} active faults (limit: {})", active_count, max),
        });

        // Check 2: No extreme-intensity faults on AllReplicas
        let extreme_all = self.active_faults.iter().any(|f| {
            f.status == FaultStatus::Active
                && f.spec.intensity == FaultIntensity::Extreme
                && matches!(f.spec.target, FaultTarget::AllReplicas)
        });
        checks.push(SafetyCheck {
            name: "no_extreme_all_replicas".to_string(),
            passed: !extreme_all,
            message: if extreme_all {
                "Extreme fault targeting all replicas detected".to_string()
            } else {
                "No extreme-all-replicas faults".to_string()
            },
        });

        // Check 3: No network partition + disk full combo
        let has_partition = self.active_faults.iter().any(|f| {
            f.status == FaultStatus::Active
                && matches!(f.spec.fault_type, FaultType::NetworkPartition)
        });
        let has_disk_full = self.active_faults.iter().any(|f| {
            f.status == FaultStatus::Active && matches!(f.spec.fault_type, FaultType::DiskFull)
        });
        checks.push(SafetyCheck {
            name: "no_partition_plus_disk_full".to_string(),
            passed: !(has_partition && has_disk_full),
            message: if has_partition && has_disk_full {
                "Dangerous combination: NetworkPartition + DiskFull".to_string()
            } else {
                "No dangerous fault combinations".to_string()
            },
        });

        let safe = checks.iter().all(|c| c.passed);
        SafetyReport { safe, checks }
    }

    fn evaluate_assertion(&self, assertion: &AssertionType) -> bool {
        // In production these would query real system state.
        // Here we provide simulated results based on current engine state.
        match assertion {
            AssertionType::NoDataLoss => true,
            AssertionType::RecoveryWithin { .. } => {
                // Pass if no active faults remain
                self.list_active_faults().is_empty()
            }
            AssertionType::LatencyBelow { .. } => true,
            AssertionType::AvailabilityAbove { .. } => true,
            AssertionType::NoSplitBrain => {
                // Fail if a network partition is still active
                !self.active_faults.iter().any(|f| {
                    f.status == FaultStatus::Active
                        && matches!(f.spec.fault_type, FaultType::NetworkPartition)
                })
            }
            AssertionType::LeaderElected => true,
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns a discriminant string for a [`FaultType`] variant, ignoring inner data.
fn fault_type_discriminant(ft: &FaultType) -> &'static str {
    match ft {
        FaultType::NetworkPartition => "NetworkPartition",
        FaultType::NetworkLatency { .. } => "NetworkLatency",
        FaultType::PacketLoss { .. } => "PacketLoss",
        FaultType::DiskSlowdown { .. } => "DiskSlowdown",
        FaultType::DiskFull => "DiskFull",
        FaultType::MemoryPressure { .. } => "MemoryPressure",
        FaultType::CpuSpike { .. } => "CpuSpike",
        FaultType::ClockSkew { .. } => "ClockSkew",
        FaultType::ProcessPause { .. } => "ProcessPause",
        FaultType::ConnectionDrop { .. } => "ConnectionDrop",
    }
}

// ---------------------------------------------------------------------------
// Built-in scenarios
// ---------------------------------------------------------------------------

/// Returns the collection of built-in chaos scenarios.
pub fn builtin_scenarios() -> Vec<ChaosScenario> {
    vec![
        leader_failover_scenario(),
        replica_recovery_scenario(),
        network_partition_heal_scenario(),
        gradual_degradation_scenario(),
        memory_pressure_eviction_scenario(),
    ]
}

fn leader_failover_scenario() -> ChaosScenario {
    ChaosScenario {
        name: "leader-failover".to_string(),
        description:
            "Inject NetworkPartition on primary, validate leader election and no data loss"
                .to_string(),
        steps: vec![
            ScenarioStep {
                action: StepAction::InjectFault(FaultSpec {
                    fault_type: FaultType::NetworkPartition,
                    target: FaultTarget::Node("primary".to_string()),
                    duration_secs: Some(10),
                    intensity: FaultIntensity::High,
                    description: Some("Partition primary node".to_string()),
                }),
                delay_after_ms: 100,
                description: "Partition the primary node from the cluster".to_string(),
            },
            ScenarioStep {
                action: StepAction::WaitMs(500),
                delay_after_ms: 0,
                description: "Wait for failover to occur".to_string(),
            },
            ScenarioStep {
                action: StepAction::ValidateSafety,
                delay_after_ms: 0,
                description: "Validate system safety during partition".to_string(),
            },
            ScenarioStep {
                action: StepAction::HealAll,
                delay_after_ms: 100,
                description: "Heal all faults".to_string(),
            },
        ],
        safety_assertions: vec![
            SafetyAssertion {
                assertion_type: AssertionType::LeaderElected,
                description: "A new leader must be elected".to_string(),
            },
            SafetyAssertion {
                assertion_type: AssertionType::NoDataLoss,
                description: "No data loss during failover".to_string(),
            },
        ],
        timeout_secs: 30,
    }
}

fn replica_recovery_scenario() -> ChaosScenario {
    ChaosScenario {
        name: "replica-recovery".to_string(),
        description: "Kill a replica, wait, then validate recovery and data sync".to_string(),
        steps: vec![
            ScenarioStep {
                action: StepAction::InjectFault(FaultSpec {
                    fault_type: FaultType::ProcessPause { duration_ms: 2000 },
                    target: FaultTarget::Node("replica-1".to_string()),
                    duration_secs: Some(5),
                    intensity: FaultIntensity::High,
                    description: Some("Pause replica-1 process".to_string()),
                }),
                delay_after_ms: 100,
                description: "Pause the replica process".to_string(),
            },
            ScenarioStep {
                action: StepAction::WaitMs(500),
                delay_after_ms: 0,
                description: "Wait for replica to be detected as down".to_string(),
            },
            ScenarioStep {
                action: StepAction::HealAll,
                delay_after_ms: 200,
                description: "Heal all faults and allow recovery".to_string(),
            },
            ScenarioStep {
                action: StepAction::CheckDataConsistency,
                delay_after_ms: 0,
                description: "Check data consistency after recovery".to_string(),
            },
        ],
        safety_assertions: vec![
            SafetyAssertion {
                assertion_type: AssertionType::RecoveryWithin { max_secs: 10 },
                description: "Replica recovers within 10 seconds".to_string(),
            },
            SafetyAssertion {
                assertion_type: AssertionType::NoDataLoss,
                description: "No data loss during replica recovery".to_string(),
            },
        ],
        timeout_secs: 30,
    }
}

fn network_partition_heal_scenario() -> ChaosScenario {
    ChaosScenario {
        name: "network-partition-heal".to_string(),
        description: "Partition two nodes, heal, and validate no split brain".to_string(),
        steps: vec![
            ScenarioStep {
                action: StepAction::InjectFault(FaultSpec {
                    fault_type: FaultType::NetworkPartition,
                    target: FaultTarget::Node("node-a".to_string()),
                    duration_secs: Some(10),
                    intensity: FaultIntensity::High,
                    description: Some("Partition node-a".to_string()),
                }),
                delay_after_ms: 50,
                description: "Partition node-a from the cluster".to_string(),
            },
            ScenarioStep {
                action: StepAction::InjectFault(FaultSpec {
                    fault_type: FaultType::NetworkPartition,
                    target: FaultTarget::Node("node-b".to_string()),
                    duration_secs: Some(10),
                    intensity: FaultIntensity::High,
                    description: Some("Partition node-b".to_string()),
                }),
                delay_after_ms: 100,
                description: "Partition node-b from the cluster".to_string(),
            },
            ScenarioStep {
                action: StepAction::WaitMs(300),
                delay_after_ms: 0,
                description: "Wait while partitioned".to_string(),
            },
            ScenarioStep {
                action: StepAction::HealAll,
                delay_after_ms: 200,
                description: "Heal all network partitions".to_string(),
            },
            ScenarioStep {
                action: StepAction::ValidateSafety,
                delay_after_ms: 0,
                description: "Validate no split brain after heal".to_string(),
            },
        ],
        safety_assertions: vec![
            SafetyAssertion {
                assertion_type: AssertionType::NoSplitBrain,
                description: "No split-brain condition after partition heal".to_string(),
            },
            SafetyAssertion {
                assertion_type: AssertionType::NoDataLoss,
                description: "No data loss during partition".to_string(),
            },
        ],
        timeout_secs: 60,
    }
}

fn gradual_degradation_scenario() -> ChaosScenario {
    ChaosScenario {
        name: "gradual-degradation".to_string(),
        description: "Inject increasing latency to measure throughput degradation curve"
            .to_string(),
        steps: vec![
            ScenarioStep {
                action: StepAction::MeasureLatency,
                delay_after_ms: 50,
                description: "Baseline latency measurement".to_string(),
            },
            ScenarioStep {
                action: StepAction::InjectFault(FaultSpec {
                    fault_type: FaultType::NetworkLatency { added_ms: 10 },
                    target: FaultTarget::Local,
                    duration_secs: Some(30),
                    intensity: FaultIntensity::Low,
                    description: Some("Add 10ms latency".to_string()),
                }),
                delay_after_ms: 100,
                description: "Inject 10ms network latency".to_string(),
            },
            ScenarioStep {
                action: StepAction::MeasureLatency,
                delay_after_ms: 50,
                description: "Measure with 10ms added latency".to_string(),
            },
            ScenarioStep {
                action: StepAction::InjectFault(FaultSpec {
                    fault_type: FaultType::NetworkLatency { added_ms: 50 },
                    target: FaultTarget::Local,
                    duration_secs: Some(30),
                    intensity: FaultIntensity::Medium,
                    description: Some("Add 50ms latency".to_string()),
                }),
                delay_after_ms: 100,
                description: "Inject 50ms network latency".to_string(),
            },
            ScenarioStep {
                action: StepAction::MeasureLatency,
                delay_after_ms: 50,
                description: "Measure with 50ms added latency".to_string(),
            },
            ScenarioStep {
                action: StepAction::InjectFault(FaultSpec {
                    fault_type: FaultType::NetworkLatency { added_ms: 200 },
                    target: FaultTarget::Local,
                    duration_secs: Some(30),
                    intensity: FaultIntensity::High,
                    description: Some("Add 200ms latency".to_string()),
                }),
                delay_after_ms: 100,
                description: "Inject 200ms network latency".to_string(),
            },
            ScenarioStep {
                action: StepAction::MeasureLatency,
                delay_after_ms: 50,
                description: "Measure with 200ms added latency".to_string(),
            },
            ScenarioStep {
                action: StepAction::HealAll,
                delay_after_ms: 100,
                description: "Heal all latency faults".to_string(),
            },
            ScenarioStep {
                action: StepAction::MeasureLatency,
                delay_after_ms: 0,
                description: "Measure recovery latency".to_string(),
            },
        ],
        safety_assertions: vec![SafetyAssertion {
            assertion_type: AssertionType::LatencyBelow { max_ms: 500 },
            description: "Latency stays below 500ms during degradation".to_string(),
        }],
        timeout_secs: 120,
    }
}

fn memory_pressure_eviction_scenario() -> ChaosScenario {
    ChaosScenario {
        name: "memory-pressure-eviction".to_string(),
        description: "Fill memory to 95%, validate LRU eviction and no OOM".to_string(),
        steps: vec![
            ScenarioStep {
                action: StepAction::InjectFault(FaultSpec {
                    fault_type: FaultType::MemoryPressure { percent: 80.0 },
                    target: FaultTarget::Local,
                    duration_secs: Some(20),
                    intensity: FaultIntensity::Medium,
                    description: Some("Memory pressure 80%".to_string()),
                }),
                delay_after_ms: 200,
                description: "Apply 80% memory pressure".to_string(),
            },
            ScenarioStep {
                action: StepAction::ValidateSafety,
                delay_after_ms: 50,
                description: "Validate safety at 80% memory".to_string(),
            },
            ScenarioStep {
                action: StepAction::InjectFault(FaultSpec {
                    fault_type: FaultType::MemoryPressure { percent: 95.0 },
                    target: FaultTarget::Local,
                    duration_secs: Some(15),
                    intensity: FaultIntensity::High,
                    description: Some("Memory pressure 95%".to_string()),
                }),
                delay_after_ms: 300,
                description: "Apply 95% memory pressure".to_string(),
            },
            ScenarioStep {
                action: StepAction::CheckDataConsistency,
                delay_after_ms: 0,
                description: "Check data consistency under memory pressure".to_string(),
            },
            ScenarioStep {
                action: StepAction::HealAll,
                delay_after_ms: 100,
                description: "Heal all memory pressure faults".to_string(),
            },
            ScenarioStep {
                action: StepAction::ValidateSafety,
                delay_after_ms: 0,
                description: "Validate safety after memory recovery".to_string(),
            },
        ],
        safety_assertions: vec![
            SafetyAssertion {
                assertion_type: AssertionType::AvailabilityAbove { min_percent: 99.0 },
                description: "Availability stays above 99% under memory pressure".to_string(),
            },
            SafetyAssertion {
                assertion_type: AssertionType::NoDataLoss,
                description: "No data loss during eviction".to_string(),
            },
        ],
        timeout_secs: 60,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn enabled_config() -> ChaosConfig {
        ChaosConfig {
            enabled: true,
            require_acl_permission: false,
            max_concurrent_faults: 5,
            auto_heal_timeout_secs: 300,
            safety_checks_enabled: false,
            allowed_fault_types: Vec::new(),
        }
    }

    fn simple_fault() -> FaultSpec {
        FaultSpec {
            fault_type: FaultType::NetworkLatency { added_ms: 50 },
            target: FaultTarget::Local,
            duration_secs: Some(60),
            intensity: FaultIntensity::Medium,
            description: Some("test fault".to_string()),
        }
    }

    // -- fault injection / heal lifecycle ------------------------------------

    #[test]
    fn test_inject_and_heal_lifecycle() {
        let engine = ChaosEngine::new(enabled_config());

        let id = engine
            .inject_fault(simple_fault())
            .expect("inject should succeed");
        assert_eq!(engine.list_active_faults().len(), 1);

        engine.heal_fault(&id).expect("heal should succeed");
        assert_eq!(engine.list_active_faults().len(), 0);
    }

    #[test]
    fn test_inject_multiple_faults() {
        let engine = ChaosEngine::new(enabled_config());

        let id1 = engine.inject_fault(simple_fault()).expect("inject 1");
        let id2 = engine.inject_fault(simple_fault()).expect("inject 2");
        assert_eq!(engine.list_active_faults().len(), 2);

        engine.heal_fault(&id1).expect("heal 1");
        assert_eq!(engine.list_active_faults().len(), 1);

        engine.heal_fault(&id2).expect("heal 2");
        assert_eq!(engine.list_active_faults().len(), 0);
    }

    #[test]
    fn test_heal_all() {
        let engine = ChaosEngine::new(enabled_config());

        engine.inject_fault(simple_fault()).expect("inject 1");
        engine.inject_fault(simple_fault()).expect("inject 2");
        engine.inject_fault(simple_fault()).expect("inject 3");

        let healed = engine.heal_all();
        assert_eq!(healed, 3);
        assert_eq!(engine.list_active_faults().len(), 0);
    }

    #[test]
    fn test_heal_nonexistent_fault() {
        let engine = ChaosEngine::new(enabled_config());
        let bogus_id = FaultId::new();
        let err = engine.heal_fault(&bogus_id).unwrap_err();
        assert!(matches!(err, ChaosError::FaultNotFound(_)));
    }

    #[test]
    fn test_heal_already_healed_fault() {
        let engine = ChaosEngine::new(enabled_config());
        let id = engine.inject_fault(simple_fault()).expect("inject");
        engine.heal_fault(&id).expect("first heal");
        let err = engine.heal_fault(&id).unwrap_err();
        assert!(matches!(err, ChaosError::HealFailed(_)));
    }

    // -- disabled engine -----------------------------------------------------

    #[test]
    fn test_inject_when_not_enabled() {
        let config = ChaosConfig::default(); // enabled: false
        let engine = ChaosEngine::new(config);
        let err = engine.inject_fault(simple_fault()).unwrap_err();
        assert!(matches!(err, ChaosError::NotEnabled));
    }

    // -- max concurrent faults -----------------------------------------------

    #[test]
    fn test_max_concurrent_faults_exceeded() {
        let mut config = enabled_config();
        config.max_concurrent_faults = 2;
        let engine = ChaosEngine::new(config);

        engine.inject_fault(simple_fault()).expect("inject 1");
        engine.inject_fault(simple_fault()).expect("inject 2");
        let err = engine.inject_fault(simple_fault()).unwrap_err();
        assert!(matches!(err, ChaosError::MaxFaultsExceeded(2)));
    }

    // -- allowed fault types filter ------------------------------------------

    #[test]
    fn test_allowed_fault_types_filter() {
        let mut config = enabled_config();
        config.allowed_fault_types = vec![FaultType::DiskFull];
        let engine = ChaosEngine::new(config);

        // NetworkLatency is not in allowed list
        let err = engine.inject_fault(simple_fault()).unwrap_err();
        assert!(matches!(err, ChaosError::InjectionFailed(_)));

        // DiskFull is allowed
        let disk_fault = FaultSpec {
            fault_type: FaultType::DiskFull,
            target: FaultTarget::Local,
            duration_secs: Some(10),
            intensity: FaultIntensity::Low,
            description: None,
        };
        engine
            .inject_fault(disk_fault)
            .expect("DiskFull should be allowed");
    }

    // -- safety validation ---------------------------------------------------

    #[test]
    fn test_safety_report_clean() {
        let engine = ChaosEngine::new(enabled_config());
        let report = engine.validate_safety();
        assert!(report.safe);
        assert!(report.checks.iter().all(|c| c.passed));
    }

    #[test]
    fn test_safety_blocks_extreme_all_replicas() {
        let mut config = enabled_config();
        config.safety_checks_enabled = true;
        let engine = ChaosEngine::new(config);

        // First inject an extreme-all-replicas fault (bypassing safety by injecting directly)
        let id = FaultId::new();
        engine.active_faults.insert(
            id.clone(),
            ActiveFault {
                id,
                spec: FaultSpec {
                    fault_type: FaultType::NetworkPartition,
                    target: FaultTarget::AllReplicas,
                    duration_secs: Some(10),
                    intensity: FaultIntensity::Extreme,
                    description: None,
                },
                injected_at: Utc::now(),
                auto_heal_at: None,
                status: FaultStatus::Active,
            },
        );

        let report = engine.validate_safety();
        assert!(!report.safe);
        assert!(report
            .checks
            .iter()
            .any(|c| c.name == "no_extreme_all_replicas" && !c.passed));
    }

    #[test]
    fn test_safety_blocks_partition_plus_disk_full() {
        let engine = ChaosEngine::new(enabled_config());

        // Inject partition
        let id1 = FaultId::new();
        engine.active_faults.insert(
            id1.clone(),
            ActiveFault {
                id: id1,
                spec: FaultSpec {
                    fault_type: FaultType::NetworkPartition,
                    target: FaultTarget::Local,
                    duration_secs: Some(30),
                    intensity: FaultIntensity::High,
                    description: None,
                },
                injected_at: Utc::now(),
                auto_heal_at: None,
                status: FaultStatus::Active,
            },
        );

        // Inject disk full
        let id2 = FaultId::new();
        engine.active_faults.insert(
            id2.clone(),
            ActiveFault {
                id: id2,
                spec: FaultSpec {
                    fault_type: FaultType::DiskFull,
                    target: FaultTarget::Local,
                    duration_secs: Some(30),
                    intensity: FaultIntensity::High,
                    description: None,
                },
                injected_at: Utc::now(),
                auto_heal_at: None,
                status: FaultStatus::Active,
            },
        );

        let report = engine.validate_safety();
        assert!(!report.safe);
        assert!(report
            .checks
            .iter()
            .any(|c| c.name == "no_partition_plus_disk_full" && !c.passed));
    }

    // -- scenario execution --------------------------------------------------

    #[test]
    fn test_run_simple_scenario() {
        let engine = ChaosEngine::new(enabled_config());

        let scenario = ChaosScenario {
            name: "simple-test".to_string(),
            description: "Inject a fault, heal, validate".to_string(),
            steps: vec![
                ScenarioStep {
                    action: StepAction::InjectFault(simple_fault()),
                    delay_after_ms: 0,
                    description: "Inject test fault".to_string(),
                },
                ScenarioStep {
                    action: StepAction::ValidateSafety,
                    delay_after_ms: 0,
                    description: "Validate safety".to_string(),
                },
                ScenarioStep {
                    action: StepAction::HealAll,
                    delay_after_ms: 0,
                    description: "Heal all".to_string(),
                },
            ],
            safety_assertions: vec![SafetyAssertion {
                assertion_type: AssertionType::NoDataLoss,
                description: "No data loss".to_string(),
            }],
            timeout_secs: 10,
        };

        let result = engine.run_scenario(scenario);
        assert!(result.passed);
        assert_eq!(result.steps_completed, 3);
        assert_eq!(result.steps_total, 3);
        assert_eq!(result.assertions_passed.len(), 1);
        assert!(result.assertions_failed.is_empty());
    }

    #[test]
    fn test_scenario_with_failing_assertion() {
        let engine = ChaosEngine::new(enabled_config());

        // Inject a partition and don't heal → NoSplitBrain should fail
        let scenario = ChaosScenario {
            name: "fail-test".to_string(),
            description: "Scenario that should fail".to_string(),
            steps: vec![ScenarioStep {
                action: StepAction::InjectFault(FaultSpec {
                    fault_type: FaultType::NetworkPartition,
                    target: FaultTarget::Local,
                    duration_secs: Some(60),
                    intensity: FaultIntensity::High,
                    description: None,
                }),
                delay_after_ms: 0,
                description: "Inject partition".to_string(),
            }],
            safety_assertions: vec![SafetyAssertion {
                assertion_type: AssertionType::NoSplitBrain,
                description: "No split brain".to_string(),
            }],
            timeout_secs: 10,
        };

        let result = engine.run_scenario(scenario);
        assert!(!result.passed);
        assert_eq!(result.assertions_failed.len(), 1);
    }

    // -- builtin scenarios ---------------------------------------------------

    #[test]
    fn test_builtin_scenarios_exist() {
        let scenarios = builtin_scenarios();
        assert_eq!(scenarios.len(), 5);

        let names: Vec<&str> = scenarios.iter().map(|s| s.name.as_str()).collect();
        assert!(names.contains(&"leader-failover"));
        assert!(names.contains(&"replica-recovery"));
        assert!(names.contains(&"network-partition-heal"));
        assert!(names.contains(&"gradual-degradation"));
        assert!(names.contains(&"memory-pressure-eviction"));
    }

    #[test]
    fn test_builtin_scenarios_are_runnable() {
        let engine = ChaosEngine::new(enabled_config());

        for scenario in builtin_scenarios() {
            let name = scenario.name.clone();
            let result = engine.run_scenario(scenario);
            assert_eq!(result.scenario_name, name);
            assert_eq!(result.steps_completed, result.steps_total);
            // Heal everything between scenarios
            engine.heal_all();
        }
    }

    // -- auto-heal timeout ---------------------------------------------------

    #[test]
    fn test_auto_heal_timeout() {
        let engine = ChaosEngine::new(enabled_config());

        // Directly insert a fault whose auto_heal_at is in the past
        let id = FaultId::new();
        let past = Utc::now() - chrono::Duration::seconds(10);
        engine.active_faults.insert(
            id.clone(),
            ActiveFault {
                id,
                spec: simple_fault(),
                injected_at: past - chrono::Duration::seconds(300),
                auto_heal_at: Some(past),
                status: FaultStatus::Active,
            },
        );

        assert_eq!(engine.list_active_faults().len(), 1);
        let healed = engine.process_auto_heals();
        assert_eq!(healed, 1);
        assert_eq!(engine.list_active_faults().len(), 0);
    }

    #[test]
    fn test_auto_heal_not_yet_due() {
        let engine = ChaosEngine::new(enabled_config());

        let id = FaultId::new();
        let future = Utc::now() + chrono::Duration::seconds(3600);
        engine.active_faults.insert(
            id.clone(),
            ActiveFault {
                id,
                spec: simple_fault(),
                injected_at: Utc::now(),
                auto_heal_at: Some(future),
                status: FaultStatus::Active,
            },
        );

        let healed = engine.process_auto_heals();
        assert_eq!(healed, 0);
        assert_eq!(engine.list_active_faults().len(), 1);
    }

    // -- stats tracking ------------------------------------------------------

    #[test]
    fn test_stats_tracking() {
        let engine = ChaosEngine::new(enabled_config());

        let id1 = engine.inject_fault(simple_fault()).expect("inject 1");
        let id2 = engine.inject_fault(simple_fault()).expect("inject 2");
        engine.heal_fault(&id1).expect("heal 1");
        engine.heal_fault(&id2).expect("heal 2");

        let stats = engine.get_stats();
        assert_eq!(stats.total_faults_injected, 2);
        assert_eq!(stats.total_faults_healed, 2);
    }

    #[test]
    fn test_scenario_stats() {
        let engine = ChaosEngine::new(enabled_config());

        let passing = ChaosScenario {
            name: "pass".to_string(),
            description: "passes".to_string(),
            steps: vec![],
            safety_assertions: vec![SafetyAssertion {
                assertion_type: AssertionType::NoDataLoss,
                description: "no data loss".to_string(),
            }],
            timeout_secs: 10,
        };

        engine.run_scenario(passing);

        let failing = ChaosScenario {
            name: "fail".to_string(),
            description: "fails".to_string(),
            steps: vec![ScenarioStep {
                action: StepAction::InjectFault(FaultSpec {
                    fault_type: FaultType::NetworkPartition,
                    target: FaultTarget::Local,
                    duration_secs: Some(60),
                    intensity: FaultIntensity::High,
                    description: None,
                }),
                delay_after_ms: 0,
                description: "inject".to_string(),
            }],
            safety_assertions: vec![SafetyAssertion {
                assertion_type: AssertionType::NoSplitBrain,
                description: "no split brain".to_string(),
            }],
            timeout_secs: 10,
        };

        engine.run_scenario(failing);
        engine.heal_all();

        let stats = engine.get_stats();
        assert_eq!(stats.total_scenarios_run, 2);
        assert_eq!(stats.scenarios_passed, 1);
        assert_eq!(stats.scenarios_failed, 1);
    }

    // -- config validation ---------------------------------------------------

    #[test]
    fn test_default_config() {
        let config = ChaosConfig::default();
        assert!(!config.enabled);
        assert!(config.require_acl_permission);
        assert_eq!(config.max_concurrent_faults, 5);
        assert_eq!(config.auto_heal_timeout_secs, 300);
        assert!(config.safety_checks_enabled);
        assert!(config.allowed_fault_types.is_empty());
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let config = ChaosConfig {
            enabled: true,
            require_acl_permission: false,
            max_concurrent_faults: 10,
            auto_heal_timeout_secs: 600,
            safety_checks_enabled: true,
            allowed_fault_types: vec![FaultType::NetworkPartition, FaultType::DiskFull],
        };
        let json = serde_json::to_string(&config).expect("serialize");
        let deserialized: ChaosConfig = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deserialized.enabled, config.enabled);
        assert_eq!(
            deserialized.max_concurrent_faults,
            config.max_concurrent_faults
        );
        assert_eq!(
            deserialized.auto_heal_timeout_secs,
            config.auto_heal_timeout_secs
        );
    }

    // -- error cases ---------------------------------------------------------

    #[test]
    fn test_error_display() {
        let err = ChaosError::NotEnabled;
        assert_eq!(err.to_string(), "chaos engineering is not enabled");

        let err = ChaosError::MaxFaultsExceeded(5);
        assert!(err.to_string().contains("5"));

        let err = ChaosError::FaultNotFound("abc-123".to_string());
        assert!(err.to_string().contains("abc-123"));
    }

    #[test]
    fn test_fault_id_display_and_equality() {
        let id1 = FaultId::new();
        let id2 = FaultId::new();
        assert_ne!(id1, id2);

        let display = format!("{}", id1);
        assert!(!display.is_empty());
    }

    #[test]
    fn test_fault_type_display() {
        assert_eq!(
            format!("{}", FaultType::NetworkPartition),
            "NetworkPartition"
        );
        assert_eq!(
            format!("{}", FaultType::NetworkLatency { added_ms: 100 }),
            "NetworkLatency(100ms)"
        );
        assert_eq!(
            format!("{}", FaultType::PacketLoss { percent: 5.0 }),
            "PacketLoss(5.0%)"
        );
    }

    // -- safety check during injection with safety enabled -------------------

    #[test]
    fn test_safety_check_blocks_dangerous_injection() {
        let mut config = enabled_config();
        config.safety_checks_enabled = true;
        config.max_concurrent_faults = 10;
        let engine = ChaosEngine::new(config);

        // Manually insert a dangerous combo (partition + disk full)
        let id1 = FaultId::new();
        engine.active_faults.insert(
            id1.clone(),
            ActiveFault {
                id: id1,
                spec: FaultSpec {
                    fault_type: FaultType::NetworkPartition,
                    target: FaultTarget::Local,
                    duration_secs: Some(60),
                    intensity: FaultIntensity::High,
                    description: None,
                },
                injected_at: Utc::now(),
                auto_heal_at: None,
                status: FaultStatus::Active,
            },
        );
        let id2 = FaultId::new();
        engine.active_faults.insert(
            id2.clone(),
            ActiveFault {
                id: id2,
                spec: FaultSpec {
                    fault_type: FaultType::DiskFull,
                    target: FaultTarget::Local,
                    duration_secs: Some(60),
                    intensity: FaultIntensity::High,
                    description: None,
                },
                injected_at: Utc::now(),
                auto_heal_at: None,
                status: FaultStatus::Active,
            },
        );

        // Now try to inject another fault — safety check should block it
        let err = engine.inject_fault(simple_fault()).unwrap_err();
        assert!(matches!(err, ChaosError::SafetyViolation(_)));
    }

    #[test]
    fn test_auto_heal_stats() {
        let engine = ChaosEngine::new(enabled_config());

        let id = FaultId::new();
        let past = Utc::now() - chrono::Duration::seconds(10);
        engine.active_faults.insert(
            id.clone(),
            ActiveFault {
                id,
                spec: simple_fault(),
                injected_at: past - chrono::Duration::seconds(300),
                auto_heal_at: Some(past),
                status: FaultStatus::Active,
            },
        );

        engine.process_auto_heals();
        let stats = engine.get_stats();
        assert_eq!(stats.total_auto_heals, 1);
        assert_eq!(stats.total_faults_healed, 1);
    }

    #[test]
    fn test_scenario_with_wait_and_measure() {
        let engine = ChaosEngine::new(enabled_config());

        let scenario = ChaosScenario {
            name: "wait-measure".to_string(),
            description: "Wait and measure".to_string(),
            steps: vec![
                ScenarioStep {
                    action: StepAction::WaitMs(1),
                    delay_after_ms: 0,
                    description: "Wait 1ms".to_string(),
                },
                ScenarioStep {
                    action: StepAction::MeasureLatency,
                    delay_after_ms: 0,
                    description: "Measure latency".to_string(),
                },
                ScenarioStep {
                    action: StepAction::CheckDataConsistency,
                    delay_after_ms: 0,
                    description: "Check consistency".to_string(),
                },
            ],
            safety_assertions: vec![],
            timeout_secs: 10,
        };

        let result = engine.run_scenario(scenario);
        assert!(result.passed);
        assert_eq!(result.steps_completed, 3);
        assert!(result.observations.iter().any(|o| o.contains("Waited")));
        assert!(result.observations.iter().any(|o| o.contains("Latency")));
        assert!(result
            .observations
            .iter()
            .any(|o| o.contains("consistency")));
    }
}
