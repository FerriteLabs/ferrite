//! Function Scheduler
//!
//! Schedule function executions based on cron expressions or intervals.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

/// Function scheduler
pub struct FunctionScheduler {
    /// Scheduled tasks
    tasks: RwLock<HashMap<String, ScheduledTask>>,
    /// Running flag
    running: AtomicBool,
    /// Next task ID
    next_id: AtomicU64,
}

/// Scheduled task
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScheduledTask {
    /// Task ID
    pub id: String,
    /// Function name
    pub function_name: String,
    /// Schedule configuration
    pub schedule: ScheduleConfig,
    /// Task status
    pub status: TaskStatus,
    /// Last execution
    pub last_execution: Option<ExecutionRecord>,
    /// Next scheduled time
    pub next_execution: Option<u64>,
    /// Enabled
    pub enabled: bool,
    /// Created at
    pub created_at: u64,
    /// Updated at
    pub updated_at: u64,
    /// Payload to pass to function
    pub payload: Option<Vec<u8>>,
    /// Metadata
    pub metadata: HashMap<String, String>,
}

/// Schedule configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ScheduleConfig {
    /// Cron expression (e.g., "0 * * * *" for every hour)
    Cron {
        expression: String,
        timezone: String,
    },
    /// Fixed rate (run every N seconds)
    FixedRate { interval_secs: u64 },
    /// Fixed delay (N seconds after previous completion)
    FixedDelay { delay_secs: u64 },
    /// One-time execution at specific time
    OneTime { execute_at: u64 },
}

impl ScheduleConfig {
    /// Create a cron schedule
    pub fn cron(expression: &str) -> Self {
        Self::Cron {
            expression: expression.to_string(),
            timezone: "UTC".to_string(),
        }
    }

    /// Create a fixed rate schedule
    pub fn every(interval: Duration) -> Self {
        Self::FixedRate {
            interval_secs: interval.as_secs(),
        }
    }

    /// Create a one-time schedule
    pub fn at(timestamp: u64) -> Self {
        Self::OneTime {
            execute_at: timestamp,
        }
    }
}

/// Task status
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    /// Scheduled and waiting
    Scheduled,
    /// Currently running
    Running,
    /// Completed (for one-time tasks)
    Completed,
    /// Failed
    Failed(String),
    /// Paused
    Paused,
}

/// Execution record
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionRecord {
    /// Execution ID
    pub id: String,
    /// Started at
    pub started_at: u64,
    /// Completed at
    pub completed_at: Option<u64>,
    /// Success
    pub success: bool,
    /// Error message
    pub error: Option<String>,
    /// Duration (ms)
    pub duration_ms: u64,
}

impl FunctionScheduler {
    /// Create a new scheduler
    pub fn new() -> Self {
        Self {
            tasks: RwLock::new(HashMap::new()),
            running: AtomicBool::new(false),
            next_id: AtomicU64::new(1),
        }
    }

    /// Start the scheduler
    pub fn start(&self) {
        self.running.store(true, Ordering::SeqCst);
        info!("Function scheduler started");
    }

    /// Stop the scheduler
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        info!("Function scheduler stopped");
    }

    /// Schedule a function
    pub fn schedule(
        &self,
        function_name: &str,
        schedule: ScheduleConfig,
        payload: Option<Vec<u8>>,
    ) -> String {
        let id = format!("task_{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let next_execution = self.calculate_next_execution(&schedule, now);

        let task = ScheduledTask {
            id: id.clone(),
            function_name: function_name.to_string(),
            schedule,
            status: TaskStatus::Scheduled,
            last_execution: None,
            next_execution,
            enabled: true,
            created_at: now,
            updated_at: now,
            payload,
            metadata: HashMap::new(),
        };

        self.tasks.write().insert(id.clone(), task);
        debug!("Scheduled task {} for function {}", id, function_name);

        id
    }

    /// Unschedule a task
    pub fn unschedule(&self, task_id: &str) -> bool {
        self.tasks.write().remove(task_id).is_some()
    }

    /// Pause a task
    pub fn pause(&self, task_id: &str) -> bool {
        if let Some(task) = self.tasks.write().get_mut(task_id) {
            task.status = TaskStatus::Paused;
            task.enabled = false;
            true
        } else {
            false
        }
    }

    /// Resume a task
    pub fn resume(&self, task_id: &str) -> bool {
        if let Some(task) = self.tasks.write().get_mut(task_id) {
            task.status = TaskStatus::Scheduled;
            task.enabled = true;
            true
        } else {
            false
        }
    }

    /// Get task
    pub fn get_task(&self, task_id: &str) -> Option<ScheduledTask> {
        self.tasks.read().get(task_id).cloned()
    }

    /// List all tasks
    pub fn list_tasks(&self) -> Vec<ScheduledTask> {
        self.tasks.read().values().cloned().collect()
    }

    /// List tasks for a function
    pub fn list_tasks_for_function(&self, function_name: &str) -> Vec<ScheduledTask> {
        self.tasks
            .read()
            .values()
            .filter(|t| t.function_name == function_name)
            .cloned()
            .collect()
    }

    /// Get due tasks
    pub fn get_due_tasks(&self) -> Vec<ScheduledTask> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.tasks
            .read()
            .values()
            .filter(|t| {
                t.enabled
                    && t.status == TaskStatus::Scheduled
                    && t.next_execution.map(|n| n <= now).unwrap_or(false)
            })
            .cloned()
            .collect()
    }

    /// Mark task as running
    pub fn mark_running(&self, task_id: &str) -> bool {
        if let Some(task) = self.tasks.write().get_mut(task_id) {
            task.status = TaskStatus::Running;
            true
        } else {
            false
        }
    }

    /// Record execution completion
    pub fn record_completion(
        &self,
        task_id: &str,
        success: bool,
        error: Option<String>,
        duration_ms: u64,
    ) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if let Some(task) = self.tasks.write().get_mut(task_id) {
            let exec_id = format!("exec_{}_{}", task_id, now);

            task.last_execution = Some(ExecutionRecord {
                id: exec_id,
                started_at: now.saturating_sub(duration_ms / 1000),
                completed_at: Some(now),
                success,
                error: error.clone(),
                duration_ms,
            });

            if success {
                // Calculate next execution
                task.next_execution = self.calculate_next_execution(&task.schedule, now);

                // For one-time tasks, mark as completed
                if matches!(task.schedule, ScheduleConfig::OneTime { .. }) {
                    task.status = TaskStatus::Completed;
                } else {
                    task.status = TaskStatus::Scheduled;
                }
            } else {
                task.status = TaskStatus::Failed(error.unwrap_or_default());
            }

            task.updated_at = now;
        }
    }

    fn calculate_next_execution(&self, schedule: &ScheduleConfig, from: u64) -> Option<u64> {
        match schedule {
            ScheduleConfig::Cron { expression, .. } => {
                // Simplified cron parsing - in production use a proper cron library
                self.parse_cron_next(expression, from)
            }
            ScheduleConfig::FixedRate { interval_secs } => Some(from + interval_secs),
            ScheduleConfig::FixedDelay { delay_secs } => Some(from + delay_secs),
            ScheduleConfig::OneTime { execute_at } => {
                if *execute_at > from {
                    Some(*execute_at)
                } else {
                    None
                }
            }
        }
    }

    fn parse_cron_next(&self, _expression: &str, from: u64) -> Option<u64> {
        // Simplified: just return next hour
        // In production, use a proper cron parsing library
        let hour = 3600;
        Some((from / hour + 1) * hour)
    }
}

impl Default for FunctionScheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schedule_task() {
        let scheduler = FunctionScheduler::new();

        let task_id = scheduler.schedule(
            "my-function",
            ScheduleConfig::every(Duration::from_secs(60)),
            None,
        );

        let task = scheduler.get_task(&task_id);
        assert!(task.is_some());
        assert_eq!(task.unwrap().function_name, "my-function");
    }

    #[test]
    fn test_unschedule_task() {
        let scheduler = FunctionScheduler::new();

        let task_id = scheduler.schedule(
            "to-remove",
            ScheduleConfig::every(Duration::from_secs(60)),
            None,
        );

        assert!(scheduler.unschedule(&task_id));
        assert!(scheduler.get_task(&task_id).is_none());
    }

    #[test]
    fn test_pause_resume() {
        let scheduler = FunctionScheduler::new();

        let task_id = scheduler.schedule(
            "pausable",
            ScheduleConfig::every(Duration::from_secs(60)),
            None,
        );

        assert!(scheduler.pause(&task_id));
        let task = scheduler.get_task(&task_id).unwrap();
        assert_eq!(task.status, TaskStatus::Paused);

        assert!(scheduler.resume(&task_id));
        let task = scheduler.get_task(&task_id).unwrap();
        assert_eq!(task.status, TaskStatus::Scheduled);
    }

    #[test]
    fn test_cron_schedule() {
        let schedule = ScheduleConfig::cron("0 * * * *");
        if let ScheduleConfig::Cron {
            expression,
            timezone,
        } = schedule
        {
            assert_eq!(expression, "0 * * * *");
            assert_eq!(timezone, "UTC");
        } else {
            panic!("Expected Cron schedule");
        }
    }

    #[test]
    fn test_list_tasks_for_function() {
        let scheduler = FunctionScheduler::new();

        scheduler.schedule(
            "func-a",
            ScheduleConfig::every(Duration::from_secs(60)),
            None,
        );
        scheduler.schedule(
            "func-a",
            ScheduleConfig::every(Duration::from_secs(120)),
            None,
        );
        scheduler.schedule(
            "func-b",
            ScheduleConfig::every(Duration::from_secs(60)),
            None,
        );

        let func_a_tasks = scheduler.list_tasks_for_function("func-a");
        assert_eq!(func_a_tasks.len(), 2);

        let func_b_tasks = scheduler.list_tasks_for_function("func-b");
        assert_eq!(func_b_tasks.len(), 1);
    }

    #[test]
    fn test_record_completion() {
        let scheduler = FunctionScheduler::new();

        let task_id = scheduler.schedule(
            "completable",
            ScheduleConfig::every(Duration::from_secs(60)),
            None,
        );

        scheduler.mark_running(&task_id);
        scheduler.record_completion(&task_id, true, None, 100);

        let task = scheduler.get_task(&task_id).unwrap();
        assert!(task.last_execution.is_some());
        assert!(task.last_execution.as_ref().unwrap().success);
        assert_eq!(task.status, TaskStatus::Scheduled);
    }
}
