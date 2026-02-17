//! Migration Executor
//!
//! Executes migration plans with progress tracking.

use super::planner::{MigrationPhase, MigrationPlan, MigrationStep};
use super::{MigrationConfig, MigrationError, Result};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Migration executor
pub struct MigrationExecutor {
    /// Current progress
    progress: Arc<RwLock<Option<MigrationProgress>>>,
    /// Cancel flag
    cancelled: Arc<AtomicBool>,
    /// Statistics
    stats: ExecutorStats,
}

impl MigrationExecutor {
    /// Create a new executor
    pub fn new() -> Self {
        Self {
            progress: Arc::new(RwLock::new(None)),
            cancelled: Arc::new(AtomicBool::new(false)),
            stats: ExecutorStats::default(),
        }
    }

    /// Execute a migration plan
    pub async fn execute(
        &self,
        plan: &MigrationPlan,
        config: &MigrationConfig,
    ) -> Result<MigrationResult> {
        // Reset state
        self.cancelled.store(false, Ordering::SeqCst);

        // Initialize progress
        let progress = MigrationProgress {
            plan_id: plan.id.clone(),
            current_step: 0,
            total_steps: plan.steps.len(),
            current_phase: MigrationPhase::PreMigration,
            keys_migrated: 0,
            total_keys: plan.total_keys,
            bytes_migrated: 0,
            total_bytes: plan.total_bytes,
            started_at: current_timestamp(),
            elapsed_secs: 0,
            estimated_remaining_secs: plan.total_estimated_duration_secs,
            current_step_progress: 0.0,
            errors: Vec::new(),
            status: MigrationStatus::Running,
        };

        *self.progress.write().await = Some(progress.clone());

        let mut result = MigrationResult {
            plan_id: plan.id.clone(),
            success: false,
            steps_completed: 0,
            steps_failed: 0,
            keys_migrated: 0,
            bytes_migrated: 0,
            started_at: current_timestamp(),
            completed_at: None,
            duration_secs: 0,
            errors: Vec::new(),
            warnings: Vec::new(),
            rollback_info: None,
        };

        // Execute each step
        for (i, step) in plan.steps.iter().enumerate() {
            // Check for cancellation
            if self.cancelled.load(Ordering::SeqCst) {
                result.errors.push(MigrationErrorInfo {
                    step: i,
                    code: "CANCELLED".to_string(),
                    message: "Migration was cancelled".to_string(),
                    recoverable: true,
                });
                break;
            }

            // Update progress
            {
                let mut progress = self.progress.write().await;
                if let Some(p) = progress.as_mut() {
                    p.current_step = i;
                    p.current_phase = step.phase.clone();
                    p.current_step_progress = 0.0;
                    p.elapsed_secs = (current_timestamp() - p.started_at) / 1000;
                }
            }

            // Execute step
            match self.execute_step(step, config, i).await {
                Ok(step_result) => {
                    result.steps_completed += 1;
                    result.keys_migrated += step_result.keys_transferred;
                    result.bytes_migrated += step_result.bytes_transferred;

                    // Update progress
                    let mut progress = self.progress.write().await;
                    if let Some(p) = progress.as_mut() {
                        p.keys_migrated = result.keys_migrated;
                        p.bytes_migrated = result.bytes_migrated;
                        p.current_step_progress = 100.0;
                    }

                    self.stats.steps_completed.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    result.steps_failed += 1;
                    result.errors.push(MigrationErrorInfo {
                        step: i,
                        code: "STEP_FAILED".to_string(),
                        message: e.to_string(),
                        recoverable: step.can_fail,
                    });

                    self.stats.steps_failed.fetch_add(1, Ordering::Relaxed);

                    // If step can't fail, abort
                    if !step.can_fail {
                        break;
                    }
                }
            }
        }

        // Finalize result
        result.completed_at = Some(current_timestamp());
        result.duration_secs =
            (result.completed_at.unwrap_or(result.started_at) - result.started_at) / 1000;
        result.success = result.steps_failed == 0;

        // Update progress
        {
            let mut progress = self.progress.write().await;
            if let Some(p) = progress.as_mut() {
                p.status = if result.success {
                    MigrationStatus::Completed
                } else {
                    MigrationStatus::Failed
                };
            }
        }

        if result.success {
            self.stats
                .migrations_completed
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.migrations_failed.fetch_add(1, Ordering::Relaxed);
        }

        Ok(result)
    }

    /// Execute a single step
    async fn execute_step(
        &self,
        step: &MigrationStep,
        _config: &MigrationConfig,
        step_index: usize,
    ) -> Result<StepResult> {
        // Simulate step execution based on phase
        // In a real implementation, this would perform actual migration operations

        let mut result = StepResult {
            step_index,
            success: true,
            keys_transferred: 0,
            bytes_transferred: 0,
            duration_ms: 0,
            error: None,
        };

        let start = current_timestamp();

        match step.phase {
            MigrationPhase::PreMigration => {
                // Simulate connectivity check or snapshot creation
                self.simulate_work(step.estimated_duration_secs).await;
            }
            MigrationPhase::DataTransfer => {
                // Simulate data transfer
                if let Some(count_str) = step.data.get("key_count") {
                    if let Ok(count) = count_str.parse::<u64>() {
                        result.keys_transferred = count;
                        result.bytes_transferred = count * 100; // Estimate 100 bytes per key
                    }
                }

                // Simulate transfer with progress updates
                let duration = step.estimated_duration_secs;
                for i in 0..10 {
                    self.simulate_work(duration / 10).await;

                    // Update progress
                    let mut progress = self.progress.write().await;
                    if let Some(p) = progress.as_mut() {
                        p.current_step_progress = ((i + 1) as f64 / 10.0) * 100.0;
                        p.keys_migrated += result.keys_transferred / 10;
                        p.bytes_migrated += result.bytes_transferred / 10;
                    }

                    // Check cancellation
                    if self.cancelled.load(Ordering::SeqCst) {
                        return Err(MigrationError::DataTransfer("Cancelled".to_string()));
                    }
                }
            }
            MigrationPhase::Validation => {
                // Simulate validation
                self.simulate_work(step.estimated_duration_secs).await;
            }
            MigrationPhase::Cutover => {
                // Simulate cutover
                self.simulate_work(step.estimated_duration_secs).await;
            }
            MigrationPhase::Cleanup => {
                // Simulate cleanup
                self.simulate_work(step.estimated_duration_secs).await;
            }
        }

        result.duration_ms = current_timestamp() - start;
        Ok(result)
    }

    /// Simulate work (for demo/testing)
    async fn simulate_work(&self, duration_secs: u64) {
        // In production, this would be actual work
        // For demo, we just sleep briefly
        tokio::time::sleep(tokio::time::Duration::from_millis(
            duration_secs.min(1) * 10,
        ))
        .await;
    }

    /// Rollback a migration
    pub async fn rollback(&self, _result: &MigrationResult) -> Result<()> {
        // In a real implementation, this would:
        // 1. Stop any in-progress operations
        // 2. Delete transferred data from target
        // 3. Restore any modified configurations
        // 4. Update status

        self.stats.rollbacks.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Cancel current migration
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// Get current progress
    pub fn current_progress(&self) -> Option<MigrationProgress> {
        // Use try_read to avoid blocking
        self.progress.try_read().ok().and_then(|p| p.clone())
    }

    /// Get executor statistics
    pub fn stats(&self) -> ExecutorStatsSnapshot {
        ExecutorStatsSnapshot {
            migrations_completed: self.stats.migrations_completed.load(Ordering::Relaxed),
            migrations_failed: self.stats.migrations_failed.load(Ordering::Relaxed),
            steps_completed: self.stats.steps_completed.load(Ordering::Relaxed),
            steps_failed: self.stats.steps_failed.load(Ordering::Relaxed),
            rollbacks: self.stats.rollbacks.load(Ordering::Relaxed),
        }
    }
}

impl Default for MigrationExecutor {
    fn default() -> Self {
        Self::new()
    }
}

/// Migration progress
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationProgress {
    /// Plan ID
    pub plan_id: String,
    /// Current step index
    pub current_step: usize,
    /// Total steps
    pub total_steps: usize,
    /// Current phase
    pub current_phase: MigrationPhase,
    /// Keys migrated
    pub keys_migrated: u64,
    /// Total keys
    pub total_keys: u64,
    /// Bytes migrated
    pub bytes_migrated: u64,
    /// Total bytes
    pub total_bytes: u64,
    /// Start timestamp
    pub started_at: u64,
    /// Elapsed seconds
    pub elapsed_secs: u64,
    /// Estimated remaining seconds
    pub estimated_remaining_secs: u64,
    /// Current step progress (0-100)
    pub current_step_progress: f64,
    /// Errors encountered
    pub errors: Vec<String>,
    /// Current status
    pub status: MigrationStatus,
}

impl MigrationProgress {
    /// Get overall progress percentage
    pub fn overall_percent(&self) -> f64 {
        if self.total_keys == 0 {
            return (self.current_step as f64 / self.total_steps as f64) * 100.0;
        }
        (self.keys_migrated as f64 / self.total_keys as f64) * 100.0
    }

    /// Get transfer rate (keys per second)
    pub fn keys_per_second(&self) -> f64 {
        if self.elapsed_secs == 0 {
            return 0.0;
        }
        self.keys_migrated as f64 / self.elapsed_secs as f64
    }

    /// Get transfer rate (bytes per second)
    pub fn bytes_per_second(&self) -> f64 {
        if self.elapsed_secs == 0 {
            return 0.0;
        }
        self.bytes_migrated as f64 / self.elapsed_secs as f64
    }
}

/// Migration status
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationStatus {
    /// Not started
    Pending,
    /// Running
    Running,
    /// Paused
    Paused,
    /// Completed successfully
    Completed,
    /// Failed
    Failed,
    /// Cancelled
    Cancelled,
}

/// Migration result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationResult {
    /// Plan ID
    pub plan_id: String,
    /// Whether migration succeeded
    pub success: bool,
    /// Steps completed
    pub steps_completed: usize,
    /// Steps failed
    pub steps_failed: usize,
    /// Total keys migrated
    pub keys_migrated: u64,
    /// Total bytes migrated
    pub bytes_migrated: u64,
    /// Start timestamp
    pub started_at: u64,
    /// Completion timestamp
    pub completed_at: Option<u64>,
    /// Duration in seconds
    pub duration_secs: u64,
    /// Errors encountered
    pub errors: Vec<MigrationErrorInfo>,
    /// Warnings
    pub warnings: Vec<String>,
    /// Rollback information (if rollback was performed)
    pub rollback_info: Option<RollbackInfo>,
}

/// Step execution result
#[derive(Clone, Debug)]
struct StepResult {
    step_index: usize,
    success: bool,
    keys_transferred: u64,
    bytes_transferred: u64,
    duration_ms: u64,
    error: Option<String>,
}

/// Migration error information
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationErrorInfo {
    /// Step index where error occurred
    pub step: usize,
    /// Error code
    pub code: String,
    /// Error message
    pub message: String,
    /// Whether the error is recoverable
    pub recoverable: bool,
}

/// Rollback information
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RollbackInfo {
    /// Rollback timestamp
    pub timestamp: u64,
    /// Keys rolled back
    pub keys_rolled_back: u64,
    /// Success
    pub success: bool,
    /// Error (if failed)
    pub error: Option<String>,
}

/// Executor statistics
#[derive(Default)]
struct ExecutorStats {
    migrations_completed: AtomicU64,
    migrations_failed: AtomicU64,
    steps_completed: AtomicU64,
    steps_failed: AtomicU64,
    rollbacks: AtomicU64,
}

/// Statistics snapshot
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutorStatsSnapshot {
    /// Completed migrations
    pub migrations_completed: u64,
    /// Failed migrations
    pub migrations_failed: u64,
    /// Completed steps
    pub steps_completed: u64,
    /// Failed steps
    pub steps_failed: u64,
    /// Rollbacks performed
    pub rollbacks: u64,
}

/// Get current timestamp in milliseconds
fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_creation() {
        let executor = MigrationExecutor::new();
        assert!(executor.current_progress().is_none());
    }

    #[test]
    fn test_progress_calculations() {
        let progress = MigrationProgress {
            plan_id: "test".to_string(),
            current_step: 1,
            total_steps: 4,
            current_phase: MigrationPhase::DataTransfer,
            keys_migrated: 5000,
            total_keys: 10000,
            bytes_migrated: 500000,
            total_bytes: 1000000,
            started_at: 0,
            elapsed_secs: 10,
            estimated_remaining_secs: 10,
            current_step_progress: 50.0,
            errors: vec![],
            status: MigrationStatus::Running,
        };

        assert_eq!(progress.overall_percent(), 50.0);
        assert_eq!(progress.keys_per_second(), 500.0);
        assert_eq!(progress.bytes_per_second(), 50000.0);
    }

    #[test]
    fn test_cancel() {
        let executor = MigrationExecutor::new();
        assert!(!executor.cancelled.load(Ordering::SeqCst));

        executor.cancel();
        assert!(executor.cancelled.load(Ordering::SeqCst));
    }
}
