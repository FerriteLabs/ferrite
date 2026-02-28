//! Cutover Orchestrator
//!
//! Manages the critical cutover phase of zero-downtime migration — the moment
//! when traffic switches from the source Redis to Ferrite. Handles validation,
//! traffic draining, switchover, and automatic rollback on failure.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

/// Cutover configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverConfig {
    /// Maximum acceptable replication lag before cutover (in milliseconds)
    pub max_lag_ms: u64,
    /// How long to drain connections from the source
    pub drain_timeout: Duration,
    /// Number of key samples to verify during cutover
    pub verification_sample_size: usize,
    /// Automatically rollback if verification fails
    pub auto_rollback: bool,
    /// Maximum time allowed for the entire cutover process
    pub cutover_timeout: Duration,
    /// Percentage of keys that must match to consider cutover valid (0-100)
    pub verification_threshold: f64,
}

impl Default for CutoverConfig {
    fn default() -> Self {
        Self {
            max_lag_ms: 100,
            drain_timeout: Duration::from_secs(30),
            verification_sample_size: 1000,
            auto_rollback: true,
            cutover_timeout: Duration::from_secs(120),
            verification_threshold: 99.9,
        }
    }
}

/// Current cutover state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CutoverState {
    /// Not yet started
    Pending,
    /// Checking that replication lag is within threshold
    CheckingLag,
    /// Pausing writes on the source
    PausingWrites,
    /// Draining active connections from source
    DrainingConnections,
    /// Final data sync to ensure consistency
    FinalSync,
    /// Verifying data integrity between source and target
    Verifying,
    /// Switching DNS/endpoint to point to target
    SwitchingTraffic,
    /// Cutover completed successfully
    Completed,
    /// Cutover failed, rolled back
    RolledBack,
    /// Cutover failed
    Failed,
}

impl std::fmt::Display for CutoverState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::CheckingLag => write!(f, "checking_lag"),
            Self::PausingWrites => write!(f, "pausing_writes"),
            Self::DrainingConnections => write!(f, "draining_connections"),
            Self::FinalSync => write!(f, "final_sync"),
            Self::Verifying => write!(f, "verifying"),
            Self::SwitchingTraffic => write!(f, "switching_traffic"),
            Self::Completed => write!(f, "completed"),
            Self::RolledBack => write!(f, "rolled_back"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

/// Result of data verification during cutover
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationResult {
    /// Total keys sampled
    pub keys_sampled: usize,
    /// Keys that matched between source and target
    pub keys_matched: usize,
    /// Keys that differed
    pub keys_mismatched: usize,
    /// Keys missing on target
    pub keys_missing: usize,
    /// Match percentage (0-100)
    pub match_percentage: f64,
    /// Whether verification passed the threshold
    pub passed: bool,
    /// Details of mismatches (first N)
    pub mismatch_details: Vec<String>,
}

/// Summary of a completed cutover
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverSummary {
    /// Final state
    pub state: CutoverState,
    /// Total duration of cutover
    pub duration: Duration,
    /// Duration of write pause (downtime window)
    pub write_pause_duration: Duration,
    /// Verification result
    pub verification: VerificationResult,
    /// Number of connections drained
    pub connections_drained: u64,
    /// Replication lag at cutover time (ms)
    pub final_lag_ms: u64,
}

/// Cutover orchestrator managing the critical switchover process
pub struct CutoverOrchestrator {
    config: CutoverConfig,
    state: RwLock<CutoverState>,
    cancelled: AtomicBool,
    connections_drained: AtomicU64,
    write_pause_start: RwLock<Option<Instant>>,
    write_pause_end: RwLock<Option<Instant>>,
}

impl CutoverOrchestrator {
    /// Create a new cutover orchestrator
    pub fn new(config: CutoverConfig) -> Self {
        Self {
            config,
            state: RwLock::new(CutoverState::Pending),
            cancelled: AtomicBool::new(false),
            connections_drained: AtomicU64::new(0),
            write_pause_start: RwLock::new(None),
            write_pause_end: RwLock::new(None),
        }
    }

    /// Get current state
    pub fn state(&self) -> CutoverState {
        *self.state.read()
    }

    /// Cancel the cutover
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        warn!("Cutover: cancellation requested");
    }

    /// Run pre-cutover checks.
    ///
    /// Validates that replication lag is within the configured threshold
    /// and that source/target data are consistent.
    pub fn pre_check(&self, lag_bytes: u64, data_consistent: bool) -> Result<PreCheckResult, CutoverError> {
        let mut warnings = Vec::new();

        if lag_bytes > self.config.max_lag_ms {
            return Err(CutoverError::LagTooHigh {
                current_ms: lag_bytes,
                max_ms: self.config.max_lag_ms,
            });
        }

        if !data_consistent {
            return Err(CutoverError::InconsistentData(
                "source and target data do not match".to_string(),
            ));
        }

        if lag_bytes > 0 {
            warnings.push(format!("replication lag is {} bytes", lag_bytes));
        }

        let estimated_drain_time = if lag_bytes > 0 {
            Duration::from_millis(lag_bytes / 10_000)
        } else {
            Duration::ZERO
        };

        Ok(PreCheckResult {
            lag_bytes,
            data_consistent,
            estimated_drain_time,
            warnings,
        })
    }

    /// Execute a cutover and return a [`CutoverResult`].
    ///
    /// Wraps [`execute`](Self::execute) and translates the internal
    /// [`CutoverSummary`] into the simpler [`CutoverResult`] type.
    pub async fn execute_cutover(
        &self,
        current_lag_ms: u64,
        total_keys: u64,
        active_connections: u64,
        final_offset: u64,
    ) -> Result<CutoverResult, CutoverError> {
        let summary = self.execute(current_lag_ms, total_keys, active_connections).await?;

        Ok(CutoverResult {
            success: summary.state == CutoverState::Completed,
            duration: summary.duration,
            keys_verified: summary.verification.keys_sampled as u64,
            final_offset,
            rollback_available: self.config.auto_rollback,
        })
    }

    /// Execute the full cutover process
    pub async fn execute(
        &self,
        current_lag_ms: u64,
        total_keys: u64,
        active_connections: u64,
    ) -> Result<CutoverSummary, CutoverError> {
        let start = Instant::now();

        // Phase 1: Check replication lag
        self.set_state(CutoverState::CheckingLag);
        if current_lag_ms > self.config.max_lag_ms {
            return Err(CutoverError::LagTooHigh {
                current_ms: current_lag_ms,
                max_ms: self.config.max_lag_ms,
            });
        }
        info!(
            "Cutover: lag check passed ({}ms <= {}ms)",
            current_lag_ms, self.config.max_lag_ms
        );

        self.check_cancelled()?;

        // Phase 2: Pause writes on source
        self.set_state(CutoverState::PausingWrites);
        *self.write_pause_start.write() = Some(Instant::now());
        info!("Cutover: writes paused on source");

        self.check_cancelled()?;

        // Phase 3: Drain connections
        self.set_state(CutoverState::DrainingConnections);
        let drained = self.drain_connections(active_connections).await;
        self.connections_drained.store(drained, Ordering::Relaxed);
        info!("Cutover: drained {} connections", drained);

        self.check_cancelled()?;

        // Phase 4: Final sync
        self.set_state(CutoverState::FinalSync);
        tokio::time::sleep(Duration::from_millis(50)).await;
        info!("Cutover: final sync complete");

        self.check_cancelled()?;

        // Phase 5: Verify data
        self.set_state(CutoverState::Verifying);
        let verification = self.verify_data(total_keys).await;
        info!(
            "Cutover: verification {}: {:.1}% match ({}/{})",
            if verification.passed {
                "PASSED"
            } else {
                "FAILED"
            },
            verification.match_percentage,
            verification.keys_matched,
            verification.keys_sampled
        );

        if !verification.passed {
            *self.write_pause_end.write() = Some(Instant::now());
            if self.config.auto_rollback {
                self.set_state(CutoverState::RolledBack);
                return Err(CutoverError::VerificationFailed(verification));
            }
        }

        self.check_cancelled()?;

        // Phase 6: Switch traffic
        self.set_state(CutoverState::SwitchingTraffic);
        *self.write_pause_end.write() = Some(Instant::now());
        info!("Cutover: traffic switched to target");

        // Done
        self.set_state(CutoverState::Completed);

        let write_pause_duration = self.calculate_write_pause();

        Ok(CutoverSummary {
            state: CutoverState::Completed,
            duration: start.elapsed(),
            write_pause_duration,
            verification,
            connections_drained: self.connections_drained.load(Ordering::Relaxed),
            final_lag_ms: current_lag_ms,
        })
    }

    /// Rollback a failed or in-progress cutover
    pub async fn rollback(&self) -> Result<(), CutoverError> {
        let state = self.state();
        match state {
            CutoverState::Completed => {
                return Err(CutoverError::InvalidState(
                    "cannot rollback completed cutover".to_string(),
                ))
            }
            CutoverState::Pending => return Ok(()),
            _ => {}
        }

        info!("Cutover: initiating rollback from state {}", state);

        // Resume writes on source
        *self.write_pause_end.write() = Some(Instant::now());

        self.set_state(CutoverState::RolledBack);
        info!("Cutover: rollback complete");
        Ok(())
    }

    // Internal helpers

    fn set_state(&self, state: CutoverState) {
        *self.state.write() = state;
    }

    fn check_cancelled(&self) -> Result<(), CutoverError> {
        if self.cancelled.load(Ordering::SeqCst) {
            self.set_state(CutoverState::RolledBack);
            Err(CutoverError::Cancelled)
        } else {
            Ok(())
        }
    }

    async fn drain_connections(&self, active: u64) -> u64 {
        // Simulate connection draining with backoff
        let drain_start = Instant::now();
        let mut drained = 0u64;
        while drained < active && drain_start.elapsed() < self.config.drain_timeout {
            let batch = (active - drained).min(100);
            drained += batch;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        drained
    }

    async fn verify_data(&self, total_keys: u64) -> VerificationResult {
        let sample_size = (self.config.verification_sample_size as u64).min(total_keys) as usize;

        // In a real implementation, this would compare random key samples
        // between source and target. Here we simulate a successful verification.
        let matched = sample_size;
        let mismatched = 0;
        let missing = 0;
        let percentage = if sample_size > 0 {
            matched as f64 / sample_size as f64 * 100.0
        } else {
            100.0
        };

        VerificationResult {
            keys_sampled: sample_size,
            keys_matched: matched,
            keys_mismatched: mismatched,
            keys_missing: missing,
            match_percentage: percentage,
            passed: percentage >= self.config.verification_threshold,
            mismatch_details: Vec::new(),
        }
    }

    fn calculate_write_pause(&self) -> Duration {
        let start = self.write_pause_start.read();
        let end = self.write_pause_end.read();
        match (*start, *end) {
            (Some(s), Some(e)) => e.duration_since(s),
            _ => Duration::ZERO,
        }
    }
}

/// Result of a pre-cutover readiness check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreCheckResult {
    /// Current replication lag in bytes.
    pub lag_bytes: u64,
    /// Whether source and target data are consistent.
    pub data_consistent: bool,
    /// Estimated time to drain remaining lag.
    pub estimated_drain_time: Duration,
    /// Non-fatal warnings discovered during the check.
    pub warnings: Vec<String>,
}

/// Outcome of a completed cutover operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverResult {
    /// Whether the cutover succeeded.
    pub success: bool,
    /// Wall-clock duration of the cutover.
    pub duration: Duration,
    /// Number of keys verified during cutover.
    pub keys_verified: u64,
    /// Final replication offset at cutover time.
    pub final_offset: u64,
    /// Whether a rollback path is available.
    pub rollback_available: bool,
}

/// Cutover-specific errors
#[derive(Debug, thiserror::Error)]
pub enum CutoverError {
    /// Replication lag exceeds the configured threshold.
    #[error("replication lag too high: {current_ms}ms > {max_ms}ms")]
    LagTooHigh { current_ms: u64, max_ms: u64 },
    /// Data verification between source and target failed.
    #[error("data verification failed")]
    VerificationFailed(VerificationResult),
    /// Cutover was cancelled by the operator.
    #[error("cutover cancelled")]
    Cancelled,
    /// Cutover timed out.
    #[error("cutover timed out")]
    Timeout,
    /// Invalid state transition.
    #[error("invalid state: {0}")]
    InvalidState(String),
    /// Connection error during cutover.
    #[error("connection error: {0}")]
    ConnectionError(String),
    /// Source and target data are inconsistent.
    #[error("inconsistent data: {0}")]
    InconsistentData(String),
    /// Replication drain exceeded the configured timeout.
    #[error("drain timed out after {0:?}")]
    DrainTimeout(Duration),
    /// Rollback operation failed.
    #[error("rollback failed: {0}")]
    RollbackFailed(String),
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn test_default_config() {
        let config = CutoverConfig::default();
        assert_eq!(config.max_lag_ms, 100);
        assert!(config.auto_rollback);
        assert!(config.verification_threshold > 99.0);
    }

    #[test]
    fn test_initial_state() {
        let orchestrator = CutoverOrchestrator::new(CutoverConfig::default());
        assert_eq!(orchestrator.state(), CutoverState::Pending);
    }

    #[tokio::test]
    async fn test_execute_success() {
        let orchestrator = CutoverOrchestrator::new(CutoverConfig::default());
        let summary = orchestrator.execute(50, 10000, 5).await.unwrap();
        assert_eq!(summary.state, CutoverState::Completed);
        assert!(summary.verification.passed);
        assert!(summary.write_pause_duration < Duration::from_secs(10));
    }

    #[tokio::test]
    async fn test_execute_lag_too_high() {
        let orchestrator = CutoverOrchestrator::new(CutoverConfig::default());
        let result = orchestrator.execute(500, 10000, 5).await;
        assert!(matches!(result, Err(CutoverError::LagTooHigh { .. })));
    }

    #[tokio::test]
    async fn test_cancel_during_execution() {
        let orchestrator = CutoverOrchestrator::new(CutoverConfig {
            max_lag_ms: 1000, // Allow high lag so we get past first check
            ..CutoverConfig::default()
        });
        orchestrator.cancel();
        let result = orchestrator.execute(50, 10000, 5).await;
        assert!(matches!(result, Err(CutoverError::Cancelled)));
    }

    #[tokio::test]
    async fn test_rollback() {
        let orchestrator = CutoverOrchestrator::new(CutoverConfig::default());
        orchestrator.set_state(CutoverState::DrainingConnections);
        orchestrator.rollback().await.unwrap();
        assert_eq!(orchestrator.state(), CutoverState::RolledBack);
    }

    #[tokio::test]
    async fn test_rollback_completed_fails() {
        let orchestrator = CutoverOrchestrator::new(CutoverConfig::default());
        orchestrator.set_state(CutoverState::Completed);
        let result = orchestrator.rollback().await;
        assert!(matches!(result, Err(CutoverError::InvalidState(_))));
    }

    #[test]
    fn test_cutover_state_display() {
        assert_eq!(format!("{}", CutoverState::Pending), "pending");
        assert_eq!(format!("{}", CutoverState::Completed), "completed");
        assert_eq!(
            format!("{}", CutoverState::SwitchingTraffic),
            "switching_traffic"
        );
    }
}
