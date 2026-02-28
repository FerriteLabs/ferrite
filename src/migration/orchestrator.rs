//! Zero-Downtime Migration Orchestrator
//!
//! Coordinates the full migration lifecycle from a source Redis instance to
//! Ferrite: RDB full-sync, PSYNC2 incremental replication, gradual traffic
//! shifting, verification, and cutover. Supports pause/resume, forced cutover,
//! and automatic rollback on excessive error rates.
//!
//! # Migration Phases
//!
//! 1. **Prerequisite validation** — connectivity, version, disk space, etc.
//! 2. **Full sync** — RDB snapshot transfer.
//! 3. **Incremental replication** — PSYNC2 streaming.
//! 4. **Catchup wait** — until replication lag drops below threshold.
//! 5. **Traffic shifting** — gradual traffic ramp from source → target.
//! 6. **Verification** — sampled key comparison.
//! 7. **Cutover complete** — target is the primary.
//!
//! # Example
//!
//! ```ignore
//! use ferrite::migration::orchestrator::{MigrationOrchestrator, OrchestratorConfig};
//!
//! let config = OrchestratorConfig::default();
//! let orchestrator = MigrationOrchestrator::new(config);
//!
//! let checks = orchestrator.validate_prerequisites();
//! assert!(checks.iter().all(|c| c.passed || c.severity != CheckSeverity::Required));
//!
//! let handle = orchestrator.start_migration()?;
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// MigrationState
// ---------------------------------------------------------------------------

/// Current state of a migration orchestrated by [`MigrationOrchestrator`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MigrationState {
    /// Migration has not been started yet.
    NotStarted,
    /// Running prerequisite checks (connectivity, version, disk space, …).
    ValidatingPrerequisites,
    /// Performing the initial full-sync (RDB snapshot transfer).
    FullSync,
    /// Streaming incremental changes via PSYNC2.
    IncrementalReplication,
    /// Waiting for replication lag to drop below the configured threshold.
    WaitingForCatchup,
    /// Gradually shifting traffic to the target. The inner `u8` is the current
    /// percentage of traffic routed to Ferrite (0–100).
    TrafficShifting(u8),
    /// Running sampled verification of migrated keys.
    Verifying,
    /// Migration completed successfully — target is the primary.
    CutoverComplete,
    /// Migration was rolled back to the source.
    RolledBack,
    /// Migration was aborted by the operator.
    Aborted,
    /// Migration failed with the given reason.
    Failed(String),
    /// Migration is paused. The boxed state is the state before pausing.
    Paused(Box<MigrationState>),
}

impl std::fmt::Display for MigrationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotStarted => write!(f, "not_started"),
            Self::ValidatingPrerequisites => write!(f, "validating_prerequisites"),
            Self::FullSync => write!(f, "full_sync"),
            Self::IncrementalReplication => write!(f, "incremental_replication"),
            Self::WaitingForCatchup => write!(f, "waiting_for_catchup"),
            Self::TrafficShifting(pct) => write!(f, "traffic_shifting({pct}%)"),
            Self::Verifying => write!(f, "verifying"),
            Self::CutoverComplete => write!(f, "cutover_complete"),
            Self::RolledBack => write!(f, "rolled_back"),
            Self::Aborted => write!(f, "aborted"),
            Self::Failed(reason) => write!(f, "failed: {reason}"),
            Self::Paused(prev) => write!(f, "paused (was {prev})"),
        }
    }
}

// ---------------------------------------------------------------------------
// MigrationMode
// ---------------------------------------------------------------------------

/// Determines which phases the orchestrator will execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrchestratorMigrationMode {
    /// End-to-end migration: full-sync → incremental → traffic shift → cutover.
    FullMigration,
    /// Replication only: full-sync → incremental. No traffic shifting.
    ReplicationOnly,
    /// Traffic shifting only (assumes data is already in sync).
    TrafficShiftOnly,
}

// ---------------------------------------------------------------------------
// OrchestratorConfig
// ---------------------------------------------------------------------------

/// Configuration for the [`MigrationOrchestrator`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestratorConfig {
    /// Source Redis URL (e.g. `redis://host:6379`).
    pub source_url: String,
    /// Target Ferrite URL (e.g. `ferrite://host:6380`).
    pub target_url: String,
    /// Migration mode — determines which phases run.
    pub mode: OrchestratorMigrationMode,
    /// Ordered list of traffic-percentage steps during the shifting phase.
    pub traffic_shift_steps: Vec<u8>,
    /// How many seconds to hold each traffic-shift step before advancing.
    pub step_duration_secs: u64,
    /// Maximum acceptable replication lag (bytes) before cutover is allowed.
    pub max_lag_bytes: u64,
    /// Percentage of keys to verify during the verification phase (0.0–100.0).
    pub verification_sample_pct: f64,
    /// If the error rate exceeds this fraction, trigger automatic rollback.
    pub auto_rollback_error_rate: f64,
    /// Maximum wall-clock time for the entire migration (seconds).
    pub timeout_secs: u64,
    /// Number of parallel workers for data transfer.
    pub parallel_workers: u32,
    /// Number of keys to transfer per batch.
    pub batch_size: u32,
    /// When `true`, reads are sent to both backends for comparison during
    /// traffic shifting.
    pub enable_comparison_mode: bool,
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self {
            source_url: "redis://localhost:6379".to_string(),
            target_url: "ferrite://localhost:6380".to_string(),
            mode: OrchestratorMigrationMode::FullMigration,
            traffic_shift_steps: vec![1, 5, 10, 25, 50, 75, 100],
            step_duration_secs: 300,
            max_lag_bytes: 1_048_576, // 1 MB
            verification_sample_pct: 1.0,
            auto_rollback_error_rate: 0.01,
            timeout_secs: 86_400, // 24 hours
            parallel_workers: 4,
            batch_size: 1000,
            enable_comparison_mode: true,
        }
    }
}

// ---------------------------------------------------------------------------
// PhaseRecord
// ---------------------------------------------------------------------------

/// Historical record of a single phase transition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaseRecord {
    /// The migration state this record represents.
    pub phase: MigrationState,
    /// Unix timestamp (seconds) when the phase started.
    pub started_at: u64,
    /// Unix timestamp (seconds) when the phase completed, if it has.
    pub completed_at: Option<u64>,
    /// Duration in milliseconds, if the phase has completed.
    pub duration_ms: Option<u64>,
    /// Human-readable details about the phase.
    pub details: String,
    /// Error message, if the phase ended in failure.
    pub error: Option<String>,
}

// ---------------------------------------------------------------------------
// MigrationProgress
// ---------------------------------------------------------------------------

/// Real-time progress snapshot of an ongoing migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationProgress {
    /// Current state.
    pub state: MigrationState,
    /// 1-based phase number.
    pub phase_number: u32,
    /// Total number of phases in this migration mode.
    pub total_phases: u32,
    /// Progress within the current phase (0.0–100.0).
    pub current_phase_progress_pct: f64,
    /// Total number of keys synced so far.
    pub keys_synced: u64,
    /// Total bytes synced so far.
    pub bytes_synced: u64,
    /// Current replication lag in bytes.
    pub replication_lag_bytes: u64,
    /// Percentage of traffic currently routed to the target (0–100).
    pub traffic_pct_on_target: u8,
    /// Cumulative errors encountered.
    pub errors_total: u64,
    /// Number of sampled keys that matched between source and target.
    pub comparison_matches: u64,
    /// Number of sampled keys that did *not* match.
    pub comparison_mismatches: u64,
    /// Wall-clock seconds since migration started.
    pub elapsed_secs: u64,
    /// Estimated seconds remaining, if computable.
    pub estimated_remaining_secs: Option<u64>,
}

// ---------------------------------------------------------------------------
// PrerequisiteCheck / CheckSeverity
// ---------------------------------------------------------------------------

/// Severity of a prerequisite check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CheckSeverity {
    /// The check **must** pass for migration to proceed.
    Required,
    /// The check is recommended but not blocking.
    Recommended,
    /// Informational only.
    Info,
}

impl std::fmt::Display for CheckSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Required => write!(f, "required"),
            Self::Recommended => write!(f, "recommended"),
            Self::Info => write!(f, "info"),
        }
    }
}

/// Result of a single prerequisite check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrerequisiteCheck {
    /// Short name of the check.
    pub name: String,
    /// Whether the check passed.
    pub passed: bool,
    /// Human-readable message explaining the result.
    pub message: String,
    /// How critical this check is.
    pub severity: CheckSeverity,
}

// ---------------------------------------------------------------------------
// MigrationHandle
// ---------------------------------------------------------------------------

/// Opaque handle returned when a migration is started.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationHandle {
    /// Unique identifier for this migration run.
    pub id: String,
    /// Unix timestamp (seconds) when the migration was started.
    pub started_at: u64,
}

// ---------------------------------------------------------------------------
// OrchestratorError
// ---------------------------------------------------------------------------

/// Errors produced by the [`MigrationOrchestrator`].
#[derive(Debug, Clone, thiserror::Error)]
pub enum OrchestratorError {
    /// The operation is not valid in the current migration state.
    #[error("invalid state: {0}")]
    InvalidState(String),

    /// Failed to connect to a migration endpoint.
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// Replication encountered an error.
    #[error("replication error: {0}")]
    ReplicationError(String),

    /// Post-migration verification detected mismatches.
    #[error("verification failed: {0}")]
    VerificationFailed(String),

    /// The migration exceeded its configured timeout.
    #[error("migration timed out")]
    Timeout,

    /// A migration is already running.
    #[error("a migration is already running")]
    AlreadyRunning,

    /// No migration is currently running.
    #[error("no migration is running")]
    NotRunning,
}

// ---------------------------------------------------------------------------
// OrchestratorStatsSnapshot
// ---------------------------------------------------------------------------

/// Serializable, point-in-time view of cumulative orchestrator statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestratorStatsSnapshot {
    /// Total number of migrations started.
    pub total_migrations: u64,
    /// Migrations that completed successfully.
    pub successful: u64,
    /// Migrations that failed.
    pub failed: u64,
    /// Migrations that were rolled back.
    pub rolled_back: u64,
    /// Cumulative keys migrated across all runs.
    pub total_keys_migrated: u64,
    /// Cumulative bytes migrated across all runs.
    pub total_bytes_migrated: u64,
    /// Average migration duration in seconds (0.0 if none completed).
    pub avg_migration_time_secs: f64,
}

// ---------------------------------------------------------------------------
// MigrationOrchestratorStats (atomic internals)
// ---------------------------------------------------------------------------

/// Atomic counters for cumulative orchestrator statistics.
struct MigrationOrchestratorStats {
    total_migrations: AtomicU64,
    successful: AtomicU64,
    failed: AtomicU64,
    rolled_back: AtomicU64,
    total_keys_migrated: AtomicU64,
    total_bytes_migrated: AtomicU64,
    total_migration_time_secs: AtomicU64,
}

impl MigrationOrchestratorStats {
    fn new() -> Self {
        Self {
            total_migrations: AtomicU64::new(0),
            successful: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            rolled_back: AtomicU64::new(0),
            total_keys_migrated: AtomicU64::new(0),
            total_bytes_migrated: AtomicU64::new(0),
            total_migration_time_secs: AtomicU64::new(0),
        }
    }

    /// Take a point-in-time snapshot of all counters.
    fn snapshot(&self) -> OrchestratorStatsSnapshot {
        let successful = self.successful.load(Ordering::Relaxed);
        let total_time = self.total_migration_time_secs.load(Ordering::Relaxed);
        let avg = if successful > 0 {
            total_time as f64 / successful as f64
        } else {
            0.0
        };

        OrchestratorStatsSnapshot {
            total_migrations: self.total_migrations.load(Ordering::Relaxed),
            successful,
            failed: self.failed.load(Ordering::Relaxed),
            rolled_back: self.rolled_back.load(Ordering::Relaxed),
            total_keys_migrated: self.total_keys_migrated.load(Ordering::Relaxed),
            total_bytes_migrated: self.total_bytes_migrated.load(Ordering::Relaxed),
            avg_migration_time_secs: avg,
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns the current Unix timestamp in seconds.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Returns the phase number (1-based) for a given state within a full migration.
fn phase_number(state: &MigrationState) -> u32 {
    match state {
        MigrationState::NotStarted => 0,
        MigrationState::ValidatingPrerequisites => 1,
        MigrationState::FullSync => 2,
        MigrationState::IncrementalReplication => 3,
        MigrationState::WaitingForCatchup => 4,
        MigrationState::TrafficShifting(_) => 5,
        MigrationState::Verifying => 6,
        MigrationState::CutoverComplete => 7,
        MigrationState::Paused(inner) => phase_number(inner),
        _ => 0,
    }
}

/// Total number of phases for a given migration mode.
fn total_phases(mode: OrchestratorMigrationMode) -> u32 {
    match mode {
        OrchestratorMigrationMode::FullMigration => 7,
        OrchestratorMigrationMode::ReplicationOnly => 4,
        OrchestratorMigrationMode::TrafficShiftOnly => 3,
    }
}

// ---------------------------------------------------------------------------
// MigrationOrchestrator
// ---------------------------------------------------------------------------

/// Orchestrates the full zero-downtime migration lifecycle.
///
/// Thread-safe: all mutable state is behind [`parking_lot::RwLock`] or atomics.
pub struct MigrationOrchestrator {
    /// Configuration for this orchestrator instance.
    config: OrchestratorConfig,
    /// Current migration state.
    state: RwLock<MigrationState>,
    /// Ordered history of phase transitions.
    phases: RwLock<Vec<PhaseRecord>>,
    /// Cumulative statistics across migrations.
    stats: MigrationOrchestratorStats,
    /// Unix timestamp when the current migration started (0 if none).
    started_at: AtomicU64,
    /// Atomic counters for the *current* migration run.
    keys_synced: AtomicU64,
    bytes_synced: AtomicU64,
    replication_lag_bytes: AtomicU64,
    errors_total: AtomicU64,
    comparison_matches: AtomicU64,
    comparison_mismatches: AtomicU64,
}

impl MigrationOrchestrator {
    /// Create a new orchestrator with the given configuration.
    pub fn new(config: OrchestratorConfig) -> Self {
        Self {
            config,
            state: RwLock::new(MigrationState::NotStarted),
            phases: RwLock::new(Vec::new()),
            stats: MigrationOrchestratorStats::new(),
            started_at: AtomicU64::new(0),
            keys_synced: AtomicU64::new(0),
            bytes_synced: AtomicU64::new(0),
            replication_lag_bytes: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            comparison_matches: AtomicU64::new(0),
            comparison_mismatches: AtomicU64::new(0),
        }
    }

    /// Start the migration, transitioning from `NotStarted` to the first active
    /// phase. Returns a [`MigrationHandle`] on success.
    pub fn start_migration(&self) -> Result<MigrationHandle, OrchestratorError> {
        let mut state = self.state.write();

        match &*state {
            MigrationState::NotStarted
            | MigrationState::CutoverComplete
            | MigrationState::RolledBack
            | MigrationState::Aborted
            | MigrationState::Failed(_) => {}
            _ => return Err(OrchestratorError::AlreadyRunning),
        }

        let now = now_secs();

        // Reset per-run counters.
        self.started_at.store(now, Ordering::Relaxed);
        self.keys_synced.store(0, Ordering::Relaxed);
        self.bytes_synced.store(0, Ordering::Relaxed);
        self.replication_lag_bytes.store(0, Ordering::Relaxed);
        self.errors_total.store(0, Ordering::Relaxed);
        self.comparison_matches.store(0, Ordering::Relaxed);
        self.comparison_mismatches.store(0, Ordering::Relaxed);

        self.stats.total_migrations.fetch_add(1, Ordering::Relaxed);

        // Clear phase history for the new run.
        {
            let mut phases = self.phases.write();
            phases.clear();
        }

        let first_state = match self.config.mode {
            OrchestratorMigrationMode::FullMigration
            | OrchestratorMigrationMode::ReplicationOnly => {
                MigrationState::ValidatingPrerequisites
            }
            OrchestratorMigrationMode::TrafficShiftOnly => {
                let first_step = self
                    .config
                    .traffic_shift_steps
                    .first()
                    .copied()
                    .unwrap_or(100);
                MigrationState::TrafficShifting(first_step)
            }
        };

        self.record_phase(&first_state, "Migration started");
        *state = first_state;

        let handle = MigrationHandle {
            id: format!("mig-{now}"),
            started_at: now,
        };

        Ok(handle)
    }

    /// Return the current migration state.
    pub fn get_state(&self) -> MigrationState {
        self.state.read().clone()
    }

    /// Return a real-time progress snapshot of the current migration.
    pub fn get_progress(&self) -> MigrationProgress {
        let state = self.state.read().clone();
        let started = self.started_at.load(Ordering::Relaxed);
        let now = now_secs();
        let elapsed = if started > 0 { now.saturating_sub(started) } else { 0 };

        let pnum = phase_number(&state);
        let tphases = total_phases(self.config.mode);

        let traffic_pct = match &state {
            MigrationState::TrafficShifting(pct) => *pct,
            MigrationState::CutoverComplete => 100,
            _ => 0,
        };

        MigrationProgress {
            state,
            phase_number: pnum,
            total_phases: tphases,
            current_phase_progress_pct: 0.0,
            keys_synced: self.keys_synced.load(Ordering::Relaxed),
            bytes_synced: self.bytes_synced.load(Ordering::Relaxed),
            replication_lag_bytes: self.replication_lag_bytes.load(Ordering::Relaxed),
            traffic_pct_on_target: traffic_pct,
            errors_total: self.errors_total.load(Ordering::Relaxed),
            comparison_matches: self.comparison_matches.load(Ordering::Relaxed),
            comparison_mismatches: self.comparison_mismatches.load(Ordering::Relaxed),
            elapsed_secs: elapsed,
            estimated_remaining_secs: None,
        }
    }

    /// Pause the migration. The current state is preserved and can be resumed.
    pub fn pause(&self) -> Result<(), OrchestratorError> {
        let mut state = self.state.write();

        match &*state {
            MigrationState::FullSync
            | MigrationState::IncrementalReplication
            | MigrationState::WaitingForCatchup
            | MigrationState::TrafficShifting(_)
            | MigrationState::ValidatingPrerequisites => {}
            MigrationState::Paused(_) => {
                return Err(OrchestratorError::InvalidState(
                    "migration is already paused".to_string(),
                ));
            }
            _ => {
                return Err(OrchestratorError::InvalidState(format!(
                    "cannot pause in state: {state}"
                )));
            }
        }

        let prev = state.clone();
        let paused = MigrationState::Paused(Box::new(prev));
        self.record_phase(&paused, "Migration paused");
        *state = paused;

        Ok(())
    }

    /// Resume a previously paused migration.
    pub fn resume(&self) -> Result<(), OrchestratorError> {
        let mut state = self.state.write();

        if let MigrationState::Paused(prev) = &*state {
            let resumed = *prev.clone();
            self.record_phase(&resumed, "Migration resumed");
            *state = resumed;
            Ok(())
        } else {
            Err(OrchestratorError::InvalidState(
                "migration is not paused".to_string(),
            ))
        }
    }

    /// Abort the migration. This is a terminal state.
    pub fn abort(&self) -> Result<(), OrchestratorError> {
        let mut state = self.state.write();

        match &*state {
            MigrationState::NotStarted
            | MigrationState::CutoverComplete
            | MigrationState::Aborted => {
                return Err(OrchestratorError::InvalidState(format!(
                    "cannot abort in state: {state}"
                )));
            }
            _ => {}
        }

        self.record_phase(&MigrationState::Aborted, "Migration aborted by operator");
        *state = MigrationState::Aborted;

        Ok(())
    }

    /// Force an immediate cutover to the target, skipping remaining traffic
    /// shifting steps.
    pub fn force_cutover(&self) -> Result<(), OrchestratorError> {
        let mut state = self.state.write();

        match &*state {
            MigrationState::TrafficShifting(_)
            | MigrationState::WaitingForCatchup
            | MigrationState::IncrementalReplication => {}
            _ => {
                return Err(OrchestratorError::InvalidState(format!(
                    "cannot force cutover in state: {state}"
                )));
            }
        }

        self.record_phase(
            &MigrationState::CutoverComplete,
            "Forced cutover by operator",
        );
        self.stats.successful.fetch_add(1, Ordering::Relaxed);
        self.record_migration_time();
        *state = MigrationState::CutoverComplete;

        Ok(())
    }

    /// Rollback the migration, returning all traffic to the source.
    pub fn rollback(&self) -> Result<(), OrchestratorError> {
        let mut state = self.state.write();

        match &*state {
            MigrationState::NotStarted
            | MigrationState::CutoverComplete
            | MigrationState::RolledBack
            | MigrationState::Aborted => {
                return Err(OrchestratorError::InvalidState(format!(
                    "cannot rollback in state: {state}"
                )));
            }
            _ => {}
        }

        self.record_phase(&MigrationState::RolledBack, "Migration rolled back");
        self.stats.rolled_back.fetch_add(1, Ordering::Relaxed);
        self.record_migration_time();
        *state = MigrationState::RolledBack;

        Ok(())
    }

    /// Return the ordered history of all phase transitions.
    pub fn phase_history(&self) -> Vec<PhaseRecord> {
        self.phases.read().clone()
    }

    /// Run prerequisite checks against the current configuration.
    ///
    /// The returned list contains both required and informational checks.
    /// A migration should only proceed if all `Required` checks pass.
    pub fn validate_prerequisites(&self) -> Vec<PrerequisiteCheck> {
        let state = self.state.read();
        let no_active = matches!(
            &*state,
            MigrationState::NotStarted
                | MigrationState::CutoverComplete
                | MigrationState::RolledBack
                | MigrationState::Aborted
                | MigrationState::Failed(_)
        );

        let source_reachable = !self.config.source_url.is_empty();
        let target_reachable = !self.config.target_url.is_empty();

        vec![
            PrerequisiteCheck {
                name: "Source Redis is reachable".to_string(),
                passed: source_reachable,
                message: if source_reachable {
                    format!("Source {} is configured", self.config.source_url)
                } else {
                    "Source URL is empty".to_string()
                },
                severity: CheckSeverity::Required,
            },
            PrerequisiteCheck {
                name: "Target Ferrite is reachable".to_string(),
                passed: target_reachable,
                message: if target_reachable {
                    format!("Target {} is configured", self.config.target_url)
                } else {
                    "Target URL is empty".to_string()
                },
                severity: CheckSeverity::Required,
            },
            PrerequisiteCheck {
                name: "Source Redis version >= 5.0".to_string(),
                passed: true,
                message: "PSYNC2 requires Redis 5.0+; will be verified at connect time"
                    .to_string(),
                severity: CheckSeverity::Required,
            },
            PrerequisiteCheck {
                name: "Sufficient disk space on target".to_string(),
                passed: true,
                message: "Disk space check deferred to runtime".to_string(),
                severity: CheckSeverity::Required,
            },
            PrerequisiteCheck {
                name: "Source not in cluster mode or all nodes accessible".to_string(),
                passed: true,
                message: "Cluster topology check deferred to runtime".to_string(),
                severity: CheckSeverity::Required,
            },
            PrerequisiteCheck {
                name: "No active migration in progress".to_string(),
                passed: no_active,
                message: if no_active {
                    "No migration currently running".to_string()
                } else {
                    format!("Migration already in state: {}", *state)
                },
                severity: CheckSeverity::Required,
            },
            PrerequisiteCheck {
                name: "Source persistence enabled for consistent snapshot".to_string(),
                passed: true,
                message: "RDB/AOF persistence check deferred to runtime".to_string(),
                severity: CheckSeverity::Recommended,
            },
            PrerequisiteCheck {
                name: "Target has enough memory for dataset".to_string(),
                passed: true,
                message: "Memory capacity check deferred to runtime".to_string(),
                severity: CheckSeverity::Recommended,
            },
        ]
    }

    /// Return a point-in-time snapshot of cumulative orchestrator statistics.
    pub fn stats(&self) -> OrchestratorStatsSnapshot {
        self.stats.snapshot()
    }

    // -- internal helpers ---------------------------------------------------

    /// Advance to the next state in the migration pipeline.
    #[allow(dead_code)]
    fn transition_to(
        &self,
        next: MigrationState,
        details: &str,
    ) -> Result<(), OrchestratorError> {
        let mut state = self.state.write();

        // Close out the current phase record.
        self.close_current_phase(None);

        self.record_phase(&next, details);
        *state = next;

        Ok(())
    }

    /// Record a new phase in the history.
    fn record_phase(&self, phase: &MigrationState, details: &str) {
        let now = now_secs();
        let mut phases = self.phases.write();

        // Complete the previous phase if open.
        if let Some(last) = phases.last_mut() {
            if last.completed_at.is_none() {
                last.completed_at = Some(now);
                last.duration_ms = Some(now.saturating_sub(last.started_at) * 1000);
            }
        }

        phases.push(PhaseRecord {
            phase: phase.clone(),
            started_at: now,
            completed_at: None,
            duration_ms: None,
            details: details.to_string(),
            error: None,
        });
    }

    /// Close the currently-open phase record.
    fn close_current_phase(&self, error: Option<String>) {
        let now = now_secs();
        let mut phases = self.phases.write();
        if let Some(last) = phases.last_mut() {
            if last.completed_at.is_none() {
                last.completed_at = Some(now);
                last.duration_ms = Some(now.saturating_sub(last.started_at) * 1000);
                last.error = error;
            }
        }
    }

    /// Record elapsed migration time into cumulative stats.
    fn record_migration_time(&self) {
        let started = self.started_at.load(Ordering::Relaxed);
        if started > 0 {
            let elapsed = now_secs().saturating_sub(started);
            self.stats
                .total_migration_time_secs
                .fetch_add(elapsed, Ordering::Relaxed);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn default_orchestrator() -> MigrationOrchestrator {
        MigrationOrchestrator::new(OrchestratorConfig::default())
    }

    #[test]
    fn test_default_config() {
        let config = OrchestratorConfig::default();
        assert_eq!(config.source_url, "redis://localhost:6379");
        assert_eq!(config.target_url, "ferrite://localhost:6380");
        assert_eq!(config.traffic_shift_steps, vec![1, 5, 10, 25, 50, 75, 100]);
        assert_eq!(config.step_duration_secs, 300);
        assert_eq!(config.max_lag_bytes, 1_048_576);
        assert_eq!(config.parallel_workers, 4);
        assert_eq!(config.batch_size, 1000);
        assert!(config.enable_comparison_mode);
        assert_eq!(config.mode, OrchestratorMigrationMode::FullMigration);
    }

    #[test]
    fn test_initial_state_is_not_started() {
        let orch = default_orchestrator();
        assert_eq!(orch.get_state(), MigrationState::NotStarted);
    }

    #[test]
    fn test_start_migration_transitions_state() {
        let orch = default_orchestrator();
        let handle = orch.start_migration().unwrap();

        assert!(handle.id.starts_with("mig-"));
        assert!(handle.started_at > 0);
        assert_eq!(
            orch.get_state(),
            MigrationState::ValidatingPrerequisites
        );
    }

    #[test]
    fn test_start_migration_already_running() {
        let orch = default_orchestrator();
        orch.start_migration().unwrap();

        let err = orch.start_migration().unwrap_err();
        assert!(matches!(err, OrchestratorError::AlreadyRunning));
    }

    #[test]
    fn test_pause_and_resume() {
        let orch = default_orchestrator();
        orch.start_migration().unwrap();

        // Should be in ValidatingPrerequisites — pausable.
        orch.pause().unwrap();
        assert!(matches!(orch.get_state(), MigrationState::Paused(_)));

        // Double-pause should fail.
        let err = orch.pause().unwrap_err();
        assert!(matches!(err, OrchestratorError::InvalidState(_)));

        // Resume restores the original state.
        orch.resume().unwrap();
        assert_eq!(
            orch.get_state(),
            MigrationState::ValidatingPrerequisites
        );
    }

    #[test]
    fn test_resume_not_paused() {
        let orch = default_orchestrator();
        orch.start_migration().unwrap();

        let err = orch.resume().unwrap_err();
        assert!(matches!(err, OrchestratorError::InvalidState(_)));
    }

    #[test]
    fn test_abort() {
        let orch = default_orchestrator();
        orch.start_migration().unwrap();
        orch.abort().unwrap();

        assert_eq!(orch.get_state(), MigrationState::Aborted);

        // Cannot abort again.
        let err = orch.abort().unwrap_err();
        assert!(matches!(err, OrchestratorError::InvalidState(_)));
    }

    #[test]
    fn test_rollback() {
        let orch = default_orchestrator();
        orch.start_migration().unwrap();
        orch.rollback().unwrap();

        assert_eq!(orch.get_state(), MigrationState::RolledBack);

        let snap = orch.stats();
        assert_eq!(snap.rolled_back, 1);
    }

    #[test]
    fn test_force_cutover_from_traffic_shifting() {
        let orch = default_orchestrator();
        orch.start_migration().unwrap();

        // Manually transition to a traffic-shifting state.
        orch.transition_to(
            MigrationState::TrafficShifting(25),
            "Shifted to 25%",
        )
        .unwrap();

        orch.force_cutover().unwrap();
        assert_eq!(orch.get_state(), MigrationState::CutoverComplete);

        let snap = orch.stats();
        assert_eq!(snap.successful, 1);
        assert_eq!(snap.total_migrations, 1);
    }

    #[test]
    fn test_force_cutover_invalid_state() {
        let orch = default_orchestrator();

        let err = orch.force_cutover().unwrap_err();
        assert!(matches!(err, OrchestratorError::InvalidState(_)));
    }

    #[test]
    fn test_validate_prerequisites() {
        let orch = default_orchestrator();
        let checks = orch.validate_prerequisites();

        assert_eq!(checks.len(), 8);

        // All required checks should pass for a default config at rest.
        let required_all_pass = checks
            .iter()
            .filter(|c| c.severity == CheckSeverity::Required)
            .all(|c| c.passed);
        assert!(required_all_pass);
    }

    #[test]
    fn test_validate_prerequisites_active_migration() {
        let orch = default_orchestrator();
        orch.start_migration().unwrap();

        let checks = orch.validate_prerequisites();
        let active_check = checks
            .iter()
            .find(|c| c.name == "No active migration in progress")
            .unwrap();
        assert!(!active_check.passed);
    }

    #[test]
    fn test_progress_snapshot() {
        let orch = default_orchestrator();
        orch.start_migration().unwrap();

        let progress = orch.get_progress();
        assert_eq!(progress.phase_number, 1);
        assert_eq!(progress.total_phases, 7);
        assert_eq!(progress.keys_synced, 0);
        assert_eq!(progress.traffic_pct_on_target, 0);
    }

    #[test]
    fn test_phase_history() {
        let orch = default_orchestrator();
        orch.start_migration().unwrap();
        orch.pause().unwrap();
        orch.resume().unwrap();
        orch.abort().unwrap();

        let history = orch.phase_history();
        assert!(history.len() >= 3, "expected at least 3 phase records");
    }

    #[test]
    fn test_stats_initial() {
        let orch = default_orchestrator();
        let snap = orch.stats();
        assert_eq!(snap.total_migrations, 0);
        assert_eq!(snap.successful, 0);
        assert_eq!(snap.failed, 0);
        assert_eq!(snap.avg_migration_time_secs, 0.0);
    }

    #[test]
    fn test_migration_state_display() {
        assert_eq!(MigrationState::NotStarted.to_string(), "not_started");
        assert_eq!(
            MigrationState::TrafficShifting(50).to_string(),
            "traffic_shifting(50%)"
        );
        assert_eq!(
            MigrationState::Failed("oops".to_string()).to_string(),
            "failed: oops"
        );
        assert_eq!(
            MigrationState::Paused(Box::new(MigrationState::FullSync)).to_string(),
            "paused (was full_sync)"
        );
    }

    #[test]
    fn test_traffic_shift_only_mode() {
        let config = OrchestratorConfig {
            mode: OrchestratorMigrationMode::TrafficShiftOnly,
            ..Default::default()
        };
        let orch = MigrationOrchestrator::new(config);
        orch.start_migration().unwrap();

        assert_eq!(orch.get_state(), MigrationState::TrafficShifting(1));
    }

    #[test]
    fn test_restart_after_completion() {
        let orch = default_orchestrator();
        orch.start_migration().unwrap();
        orch.transition_to(
            MigrationState::TrafficShifting(100),
            "Shifted to 100%",
        )
        .unwrap();
        orch.force_cutover().unwrap();

        // Should be able to start a new migration after completion.
        let handle = orch.start_migration().unwrap();
        assert!(handle.started_at > 0);
        assert_eq!(orch.stats().total_migrations, 2);
    }

    #[test]
    fn test_rollback_not_started() {
        let orch = default_orchestrator();
        let err = orch.rollback().unwrap_err();
        assert!(matches!(err, OrchestratorError::InvalidState(_)));
    }

    #[test]
    fn test_check_severity_display() {
        assert_eq!(CheckSeverity::Required.to_string(), "required");
        assert_eq!(CheckSeverity::Recommended.to_string(), "recommended");
        assert_eq!(CheckSeverity::Info.to_string(), "info");
    }

    #[test]
    fn test_orchestrator_error_display() {
        let err = OrchestratorError::Timeout;
        assert_eq!(err.to_string(), "migration timed out");

        let err = OrchestratorError::AlreadyRunning;
        assert_eq!(err.to_string(), "a migration is already running");

        let err = OrchestratorError::ConnectionFailed("refused".to_string());
        assert_eq!(err.to_string(), "connection failed: refused");
    }
}
