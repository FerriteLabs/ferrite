//! Migration Planner
//!
//! Plans migration steps based on source analysis.

use super::{CompatibilityReport, DataType, MigrationConfig, MigrationMode, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Migration planner
pub struct MigrationPlanner {
    /// Step templates
    step_templates: HashMap<String, StepTemplate>,
}

impl MigrationPlanner {
    /// Create a new planner
    pub fn new() -> Self {
        let mut planner = Self {
            step_templates: HashMap::new(),
        };
        planner.setup_templates();
        planner
    }

    /// Setup step templates
    fn setup_templates(&mut self) {
        // Pre-migration steps
        self.step_templates.insert(
            "verify_connectivity".to_string(),
            StepTemplate {
                name: "Verify Connectivity".to_string(),
                description: "Verify connection to source and target".to_string(),
                phase: MigrationPhase::PreMigration,
                estimated_duration_secs: 5,
                can_fail: false,
                rollback_action: None,
            },
        );

        self.step_templates.insert(
            "create_snapshot".to_string(),
            StepTemplate {
                name: "Create Snapshot".to_string(),
                description: "Create a point-in-time snapshot of source data".to_string(),
                phase: MigrationPhase::PreMigration,
                estimated_duration_secs: 60,
                can_fail: false,
                rollback_action: Some("delete_snapshot".to_string()),
            },
        );

        self.step_templates.insert(
            "setup_replication".to_string(),
            StepTemplate {
                name: "Setup Replication".to_string(),
                description: "Configure target as replica of source".to_string(),
                phase: MigrationPhase::PreMigration,
                estimated_duration_secs: 10,
                can_fail: false,
                rollback_action: Some("teardown_replication".to_string()),
            },
        );

        // Migration steps
        self.step_templates.insert(
            "transfer_keys".to_string(),
            StepTemplate {
                name: "Transfer Keys".to_string(),
                description: "Transfer keys from source to target".to_string(),
                phase: MigrationPhase::DataTransfer,
                estimated_duration_secs: 0, // Calculated based on data size
                can_fail: true,
                rollback_action: Some("delete_transferred_keys".to_string()),
            },
        );

        self.step_templates.insert(
            "sync_changes".to_string(),
            StepTemplate {
                name: "Sync Changes".to_string(),
                description: "Sync changes made during initial transfer".to_string(),
                phase: MigrationPhase::DataTransfer,
                estimated_duration_secs: 30,
                can_fail: true,
                rollback_action: None,
            },
        );

        // Post-migration steps
        self.step_templates.insert(
            "verify_data".to_string(),
            StepTemplate {
                name: "Verify Data".to_string(),
                description: "Verify data integrity between source and target".to_string(),
                phase: MigrationPhase::Validation,
                estimated_duration_secs: 60,
                can_fail: true,
                rollback_action: None,
            },
        );

        self.step_templates.insert(
            "switch_traffic".to_string(),
            StepTemplate {
                name: "Switch Traffic".to_string(),
                description: "Switch application traffic to target".to_string(),
                phase: MigrationPhase::Cutover,
                estimated_duration_secs: 5,
                can_fail: false,
                rollback_action: Some("revert_traffic".to_string()),
            },
        );

        self.step_templates.insert(
            "cleanup_source".to_string(),
            StepTemplate {
                name: "Cleanup Source".to_string(),
                description: "Clean up source data (optional)".to_string(),
                phase: MigrationPhase::Cleanup,
                estimated_duration_secs: 30,
                can_fail: true,
                rollback_action: None,
            },
        );
    }

    /// Create a migration plan
    pub async fn create_plan(
        &self,
        config: &MigrationConfig,
        report: &CompatibilityReport,
    ) -> Result<MigrationPlan> {
        let mut steps = Vec::new();
        let mut step_order = 0;

        // Phase 1: Pre-migration
        steps.push(self.create_step("verify_connectivity", &mut step_order, config)?);

        match config.mode {
            MigrationMode::Snapshot => {
                steps.push(self.create_step("create_snapshot", &mut step_order, config)?);
            }
            MigrationMode::Live => {
                steps.push(self.create_step("setup_replication", &mut step_order, config)?);
            }
            MigrationMode::Incremental => {
                // No special setup for incremental
            }
            MigrationMode::DualWrite => {
                steps.push(MigrationStep {
                    order: step_order,
                    name: "Setup Dual Write".to_string(),
                    description: "Configure dual-write proxy".to_string(),
                    phase: MigrationPhase::PreMigration,
                    estimated_duration_secs: 30,
                    status: StepStatus::Pending,
                    progress_percent: 0.0,
                    can_fail: false,
                    rollback_action: Some("disable_dual_write".to_string()),
                    data: HashMap::new(),
                });
                step_order += 1;
            }
        }

        // Phase 2: Data Transfer
        // Create transfer steps for each data type
        for (data_type, count) in &report.stats.keys_by_type {
            if *count > 0 {
                let transfer_time = self.estimate_transfer_time(*count, data_type);
                steps.push(MigrationStep {
                    order: step_order,
                    name: format!("Transfer {} keys", data_type_name(data_type)),
                    description: format!(
                        "Transfer {} {} keys to target",
                        count,
                        data_type_name(data_type)
                    ),
                    phase: MigrationPhase::DataTransfer,
                    estimated_duration_secs: transfer_time,
                    status: StepStatus::Pending,
                    progress_percent: 0.0,
                    can_fail: true,
                    rollback_action: Some("delete_transferred_keys".to_string()),
                    data: vec![
                        ("data_type".to_string(), format!("{:?}", data_type)),
                        ("key_count".to_string(), count.to_string()),
                    ]
                    .into_iter()
                    .collect(),
                });
                step_order += 1;
            }
        }

        // Sync changes (for live migration)
        if config.mode == MigrationMode::Live {
            steps.push(self.create_step("sync_changes", &mut step_order, config)?);
        }

        // Phase 3: Validation
        if config.verify {
            steps.push(self.create_step("verify_data", &mut step_order, config)?);
        }

        // Phase 4: Cutover
        steps.push(self.create_step("switch_traffic", &mut step_order, config)?);

        // Phase 5: Cleanup
        if !config.keep_source {
            steps.push(self.create_step("cleanup_source", &mut step_order, config)?);
        }

        // Calculate totals
        let total_duration: u64 = steps.iter().map(|s| s.estimated_duration_secs).sum();

        Ok(MigrationPlan {
            id: generate_plan_id(),
            config: config.clone(),
            steps,
            total_estimated_duration_secs: total_duration,
            total_keys: report.stats.total_keys,
            total_bytes: report.stats.memory_bytes,
            created_at: current_timestamp(),
            status: PlanStatus::Ready,
        })
    }

    /// Create a step from template
    fn create_step(
        &self,
        template_id: &str,
        order: &mut u32,
        _config: &MigrationConfig,
    ) -> Result<MigrationStep> {
        let template = self.step_templates.get(template_id).ok_or_else(|| {
            super::MigrationError::Config(format!("unknown step template: {}", template_id))
        })?;
        let step = MigrationStep {
            order: *order,
            name: template.name.clone(),
            description: template.description.clone(),
            phase: template.phase.clone(),
            estimated_duration_secs: template.estimated_duration_secs,
            status: StepStatus::Pending,
            progress_percent: 0.0,
            can_fail: template.can_fail,
            rollback_action: template.rollback_action.clone(),
            data: HashMap::new(),
        };
        *order += 1;
        Ok(step)
    }

    /// Estimate transfer time for a data type
    fn estimate_transfer_time(&self, count: u64, data_type: &DataType) -> u64 {
        // Base: 10000 keys/second
        let base_rate = 10_000.0;

        // Adjust for data type complexity
        let complexity_factor = match data_type {
            DataType::String => 1.0,
            DataType::List => 1.5,
            DataType::Set => 1.2,
            DataType::SortedSet => 1.5,
            DataType::Hash => 1.3,
            DataType::Stream => 2.0,
            DataType::Json => 1.8,
            DataType::TimeSeries => 1.5,
            DataType::Graph => 3.0,
            _ => 1.0,
        };

        let adjusted_rate = base_rate / complexity_factor;
        (count as f64 / adjusted_rate).ceil() as u64 + 1
    }

    /// Update plan status
    pub fn update_plan_status(&self, plan: &mut MigrationPlan) {
        let completed = plan
            .steps
            .iter()
            .filter(|s| s.status == StepStatus::Completed)
            .count();
        let failed = plan
            .steps
            .iter()
            .filter(|s| s.status == StepStatus::Failed)
            .count();

        if failed > 0 {
            plan.status = PlanStatus::Failed;
        } else if completed == plan.steps.len() {
            plan.status = PlanStatus::Completed;
        } else if completed > 0 {
            plan.status = PlanStatus::InProgress;
        }
    }
}

impl Default for MigrationPlanner {
    fn default() -> Self {
        Self::new()
    }
}

/// Migration plan
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationPlan {
    /// Plan ID
    pub id: String,
    /// Configuration used
    pub config: MigrationConfig,
    /// Migration steps
    pub steps: Vec<MigrationStep>,
    /// Total estimated duration
    pub total_estimated_duration_secs: u64,
    /// Total keys to migrate
    pub total_keys: u64,
    /// Total bytes to migrate
    pub total_bytes: u64,
    /// Plan creation time
    pub created_at: u64,
    /// Plan status
    pub status: PlanStatus,
}

impl MigrationPlan {
    /// Get current phase
    pub fn current_phase(&self) -> Option<MigrationPhase> {
        self.steps
            .iter()
            .find(|s| s.status == StepStatus::InProgress)
            .map(|s| s.phase.clone())
    }

    /// Get overall progress percentage
    pub fn progress_percent(&self) -> f64 {
        let total_weight: f64 = self
            .steps
            .iter()
            .map(|s| s.estimated_duration_secs as f64)
            .sum();

        if total_weight == 0.0 {
            return 0.0;
        }

        let completed_weight: f64 = self
            .steps
            .iter()
            .map(|s| {
                let weight = s.estimated_duration_secs as f64;
                match s.status {
                    StepStatus::Completed => weight,
                    StepStatus::InProgress => weight * (s.progress_percent / 100.0),
                    _ => 0.0,
                }
            })
            .sum();

        (completed_weight / total_weight) * 100.0
    }

    /// Get remaining steps
    pub fn remaining_steps(&self) -> Vec<&MigrationStep> {
        self.steps
            .iter()
            .filter(|s| s.status == StepStatus::Pending)
            .collect()
    }
}

/// Migration step
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationStep {
    /// Step order
    pub order: u32,
    /// Step name
    pub name: String,
    /// Step description
    pub description: String,
    /// Migration phase
    pub phase: MigrationPhase,
    /// Estimated duration
    pub estimated_duration_secs: u64,
    /// Current status
    pub status: StepStatus,
    /// Progress percentage (0-100)
    pub progress_percent: f64,
    /// Whether step can fail without aborting
    pub can_fail: bool,
    /// Rollback action (if applicable)
    pub rollback_action: Option<String>,
    /// Additional data
    pub data: HashMap<String, String>,
}

/// Step template
#[derive(Clone, Debug)]
struct StepTemplate {
    name: String,
    description: String,
    phase: MigrationPhase,
    estimated_duration_secs: u64,
    can_fail: bool,
    rollback_action: Option<String>,
}

/// Migration phase
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationPhase {
    /// Pre-migration setup
    PreMigration,
    /// Data transfer
    DataTransfer,
    /// Validation
    Validation,
    /// Traffic cutover
    Cutover,
    /// Cleanup
    Cleanup,
}

/// Step status
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StepStatus {
    /// Pending execution
    Pending,
    /// Currently executing
    InProgress,
    /// Completed successfully
    Completed,
    /// Failed
    Failed,
    /// Skipped
    Skipped,
    /// Rolled back
    RolledBack,
}

/// Plan status
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlanStatus {
    /// Ready to execute
    Ready,
    /// In progress
    InProgress,
    /// Completed successfully
    Completed,
    /// Failed
    Failed,
    /// Cancelled
    Cancelled,
}

/// Get human-readable name for data type
fn data_type_name(dt: &DataType) -> &'static str {
    match dt {
        DataType::String => "String",
        DataType::List => "List",
        DataType::Set => "Set",
        DataType::SortedSet => "Sorted Set",
        DataType::Hash => "Hash",
        DataType::Stream => "Stream",
        DataType::HyperLogLog => "HyperLogLog",
        DataType::Bitmap => "Bitmap",
        DataType::Geo => "Geo",
        DataType::Json => "JSON",
        DataType::TimeSeries => "Time Series",
        DataType::Graph => "Graph",
        DataType::Search => "Search Index",
        DataType::Bloom => "Bloom Filter",
        DataType::Unknown => "Unknown",
    }
}

/// Generate a unique plan ID
fn generate_plan_id() -> String {
    format!("plan-{}", current_timestamp())
}

/// Get current timestamp
fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::migration::SourceStats;

    fn mock_report() -> CompatibilityReport {
        CompatibilityReport {
            source_url: "redis://localhost:6379".to_string(),
            stats: SourceStats {
                total_keys: 10000,
                keys_by_type: vec![
                    (DataType::String, 5000),
                    (DataType::Hash, 3000),
                    (DataType::List, 2000),
                ]
                .into_iter()
                .collect(),
                memory_bytes: 100 * 1024 * 1024,
                ..Default::default()
            },
            issues: vec![],
            recommendations: vec![],
            compatibility_score: 95.0,
            can_migrate: true,
            estimated_duration_secs: 120,
        }
    }

    #[tokio::test]
    async fn test_create_plan() {
        let planner = MigrationPlanner::new();
        let config = MigrationConfig::default();
        let report = mock_report();

        let plan = planner.create_plan(&config, &report).await.unwrap();

        assert!(!plan.steps.is_empty());
        assert_eq!(plan.status, PlanStatus::Ready);
        assert!(plan.total_estimated_duration_secs > 0);
    }

    #[test]
    fn test_plan_progress() {
        let plan = MigrationPlan {
            id: "test".to_string(),
            config: MigrationConfig::default(),
            steps: vec![
                MigrationStep {
                    order: 0,
                    name: "Step 1".to_string(),
                    description: "".to_string(),
                    phase: MigrationPhase::PreMigration,
                    estimated_duration_secs: 10,
                    status: StepStatus::Completed,
                    progress_percent: 100.0,
                    can_fail: false,
                    rollback_action: None,
                    data: HashMap::new(),
                },
                MigrationStep {
                    order: 1,
                    name: "Step 2".to_string(),
                    description: "".to_string(),
                    phase: MigrationPhase::DataTransfer,
                    estimated_duration_secs: 10,
                    status: StepStatus::Pending,
                    progress_percent: 0.0,
                    can_fail: false,
                    rollback_action: None,
                    data: HashMap::new(),
                },
            ],
            total_estimated_duration_secs: 20,
            total_keys: 1000,
            total_bytes: 1000,
            created_at: 0,
            status: PlanStatus::InProgress,
        };

        assert_eq!(plan.progress_percent(), 50.0);
    }
}
