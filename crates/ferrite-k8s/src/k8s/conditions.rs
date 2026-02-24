//! Typed Kubernetes Status Conditions for Ferrite Operator
//!
//! Provides well-known condition types and a manager for atomically
//! updating conditions on [`FerriteClusterStatus`].
//!
//! # Standard Conditions
//!
//! | Type        | Meaning                                            |
//! |-------------|----------------------------------------------------|
//! | `Ready`     | All desired replicas are running and passing probes |
//! | `Degraded`  | Cluster is functional but experiencing issues       |
//! | `Scaling`   | A scale-up or scale-down operation is in progress   |
//! | `Upgrading` | A rolling update to a new version is in progress    |

use super::crd::FerriteClusterStatus;
use super::Condition;

// ============================================================================
// Condition type constants
// ============================================================================

/// All desired replicas are running and passing health probes.
pub const CONDITION_READY: &str = "Ready";

/// Cluster is functional but experiencing issues (e.g. partial replica failure).
pub const CONDITION_DEGRADED: &str = "Degraded";

/// A horizontal scaling operation is in progress.
pub const CONDITION_SCALING: &str = "Scaling";

/// A rolling update / version upgrade is in progress.
pub const CONDITION_UPGRADING: &str = "Upgrading";

// ============================================================================
// Condition reason constants
// ============================================================================

// Ready reasons
pub const REASON_ALL_REPLICAS_READY: &str = "AllReplicasReady";
pub const REASON_REPLICAS_NOT_READY: &str = "ReplicasNotReady";
pub const REASON_CLUSTER_INITIALIZING: &str = "ClusterInitializing";

// Degraded reasons
pub const REASON_PARTIAL_REPLICA_FAILURE: &str = "PartialReplicaFailure";
pub const REASON_PRIMARY_UNHEALTHY: &str = "PrimaryUnhealthy";
pub const REASON_QUORUM_AT_RISK: &str = "QuorumAtRisk";
pub const REASON_CLUSTER_HEALTHY: &str = "ClusterHealthy";

// Scaling reasons
pub const REASON_SCALING_UP: &str = "ScalingUp";
pub const REASON_SCALING_DOWN: &str = "ScalingDown";
pub const REASON_SCALING_COMPLETE: &str = "ScalingComplete";
pub const REASON_SLOT_MIGRATION_IN_PROGRESS: &str = "SlotMigrationInProgress";

// Upgrading reasons
pub const REASON_ROLLING_UPDATE_IN_PROGRESS: &str = "RollingUpdateInProgress";
pub const REASON_ROLLBACK_IN_PROGRESS: &str = "RollbackInProgress";
pub const REASON_UPGRADE_COMPLETE: &str = "UpgradeComplete";
pub const REASON_PRE_UPDATE_CHECK: &str = "PreUpdateCheck";
pub const REASON_POST_UPDATE_VERIFY: &str = "PostUpdateVerify";

// ============================================================================
// StatusConditionManager
// ============================================================================

/// Helper for atomically setting well-known conditions on a cluster status.
///
/// Each setter updates (or inserts) the condition with a timestamp, reason,
/// and human-readable message.
pub struct StatusConditionManager<'a> {
    status: &'a mut FerriteClusterStatus,
}

impl<'a> StatusConditionManager<'a> {
    /// Wrap a mutable reference to a cluster status.
    pub fn new(status: &'a mut FerriteClusterStatus) -> Self {
        Self { status }
    }

    // -- Ready ---------------------------------------------------------------

    /// Mark the cluster as ready (all replicas healthy).
    pub fn set_ready(&mut self, ready_replicas: i32, desired_replicas: i32) {
        let is_ready = ready_replicas >= desired_replicas && desired_replicas > 0;
        let (reason, message) = if is_ready {
            (
                REASON_ALL_REPLICAS_READY,
                format!("{}/{} replicas ready", ready_replicas, desired_replicas),
            )
        } else if ready_replicas > 0 {
            (
                REASON_REPLICAS_NOT_READY,
                format!(
                    "{}/{} replicas ready, waiting for remaining",
                    ready_replicas, desired_replicas
                ),
            )
        } else {
            (
                REASON_CLUSTER_INITIALIZING,
                format!(
                    "0/{} replicas ready, cluster initializing",
                    desired_replicas
                ),
            )
        };

        self.status.set_condition(
            Condition::new(CONDITION_READY, is_ready)
                .with_reason(reason)
                .with_message(message),
        );
    }

    // -- Degraded ------------------------------------------------------------

    /// Mark the cluster as degraded due to partial replica failure.
    pub fn set_degraded_partial_failure(&mut self, ready_replicas: i32, desired_replicas: i32) {
        let is_degraded = ready_replicas < desired_replicas && ready_replicas > 0;
        self.status.set_condition(
            Condition::new(CONDITION_DEGRADED, is_degraded)
                .with_reason(if is_degraded {
                    REASON_PARTIAL_REPLICA_FAILURE
                } else {
                    REASON_CLUSTER_HEALTHY
                })
                .with_message(if is_degraded {
                    format!(
                        "Only {}/{} replicas healthy",
                        ready_replicas, desired_replicas
                    )
                } else {
                    "All replicas healthy".to_string()
                }),
        );
    }

    /// Mark the cluster as degraded because the primary is unhealthy.
    pub fn set_degraded_primary_unhealthy(&mut self, primary_name: &str) {
        self.status.set_condition(
            Condition::new(CONDITION_DEGRADED, true)
                .with_reason(REASON_PRIMARY_UNHEALTHY)
                .with_message(format!("Primary pod {} is not ready", primary_name)),
        );
    }

    /// Mark the cluster as degraded because quorum is at risk.
    pub fn set_degraded_quorum_at_risk(&mut self, ready_replicas: i32, quorum_size: i32) {
        self.status.set_condition(
            Condition::new(CONDITION_DEGRADED, true)
                .with_reason(REASON_QUORUM_AT_RISK)
                .with_message(format!(
                    "Only {} replicas ready, quorum requires {}",
                    ready_replicas, quorum_size
                )),
        );
    }

    /// Clear the degraded condition.
    pub fn clear_degraded(&mut self) {
        self.status.set_condition(
            Condition::new(CONDITION_DEGRADED, false)
                .with_reason(REASON_CLUSTER_HEALTHY)
                .with_message("Cluster is healthy"),
        );
    }

    // -- Scaling -------------------------------------------------------------

    /// Mark a scale-up in progress.
    pub fn set_scaling_up(&mut self, from: i32, to: i32) {
        self.status.set_condition(
            Condition::new(CONDITION_SCALING, true)
                .with_reason(REASON_SCALING_UP)
                .with_message(format!("Scaling from {} to {} replicas", from, to)),
        );
    }

    /// Mark a scale-down in progress.
    pub fn set_scaling_down(&mut self, from: i32, to: i32) {
        self.status.set_condition(
            Condition::new(CONDITION_SCALING, true)
                .with_reason(REASON_SCALING_DOWN)
                .with_message(format!("Scaling from {} to {} replicas", from, to)),
        );
    }

    /// Mark slot migration in progress during scale-down.
    pub fn set_scaling_slot_migration(&mut self, pods_remaining: i32) {
        self.status.set_condition(
            Condition::new(CONDITION_SCALING, true)
                .with_reason(REASON_SLOT_MIGRATION_IN_PROGRESS)
                .with_message(format!(
                    "Migrating slots from {} pods before removal",
                    pods_remaining
                )),
        );
    }

    /// Clear the scaling condition.
    pub fn clear_scaling(&mut self) {
        self.status.set_condition(
            Condition::new(CONDITION_SCALING, false)
                .with_reason(REASON_SCALING_COMPLETE)
                .with_message("Scaling operation complete"),
        );
    }

    // -- Upgrading -----------------------------------------------------------

    /// Mark a rolling update in progress.
    pub fn set_upgrading(
        &mut self,
        from_version: &str,
        to_version: &str,
        completed: usize,
        total: usize,
    ) {
        self.status.set_condition(
            Condition::new(CONDITION_UPGRADING, true)
                .with_reason(REASON_ROLLING_UPDATE_IN_PROGRESS)
                .with_message(format!(
                    "Upgrading {} → {}: {}/{} pods updated",
                    from_version, to_version, completed, total
                )),
        );
    }

    /// Mark a rollback in progress.
    pub fn set_upgrading_rollback(&mut self, target_version: &str, reason: &str) {
        self.status.set_condition(
            Condition::new(CONDITION_UPGRADING, true)
                .with_reason(REASON_ROLLBACK_IN_PROGRESS)
                .with_message(format!("Rolling back to {}: {}", target_version, reason)),
        );
    }

    /// Mark a pre-update health check phase.
    pub fn set_upgrading_pre_check(&mut self) {
        self.status.set_condition(
            Condition::new(CONDITION_UPGRADING, true)
                .with_reason(REASON_PRE_UPDATE_CHECK)
                .with_message("Running pre-update health checks"),
        );
    }

    /// Mark a post-update verification phase.
    pub fn set_upgrading_post_verify(&mut self, completed: usize, total: usize) {
        self.status.set_condition(
            Condition::new(CONDITION_UPGRADING, true)
                .with_reason(REASON_POST_UPDATE_VERIFY)
                .with_message(format!(
                    "Verifying updated pods: {}/{} verified",
                    completed, total
                )),
        );
    }

    /// Clear the upgrading condition.
    pub fn clear_upgrading(&mut self) {
        self.status.set_condition(
            Condition::new(CONDITION_UPGRADING, false)
                .with_reason(REASON_UPGRADE_COMPLETE)
                .with_message("Upgrade complete"),
        );
    }

    /// Convenience: set all four conditions for a fully healthy cluster.
    pub fn set_all_healthy(&mut self, ready_replicas: i32, desired_replicas: i32) {
        self.set_ready(ready_replicas, desired_replicas);
        self.clear_degraded();
        self.clear_scaling();
        self.clear_upgrading();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_status() -> FerriteClusterStatus {
        FerriteClusterStatus::default()
    }

    #[test]
    fn test_set_ready_all_replicas() {
        let mut status = empty_status();
        let mut mgr = StatusConditionManager::new(&mut status);
        mgr.set_ready(3, 3);

        assert!(status.is_condition_true(CONDITION_READY));
        let cond = status.get_condition(CONDITION_READY).unwrap();
        assert_eq!(cond.reason.as_deref(), Some(REASON_ALL_REPLICAS_READY));
    }

    #[test]
    fn test_set_ready_partial() {
        let mut status = empty_status();
        let mut mgr = StatusConditionManager::new(&mut status);
        mgr.set_ready(1, 3);

        assert!(!status.is_condition_true(CONDITION_READY));
        let cond = status.get_condition(CONDITION_READY).unwrap();
        assert_eq!(cond.reason.as_deref(), Some(REASON_REPLICAS_NOT_READY));
    }

    #[test]
    fn test_set_ready_zero() {
        let mut status = empty_status();
        let mut mgr = StatusConditionManager::new(&mut status);
        mgr.set_ready(0, 3);

        assert!(!status.is_condition_true(CONDITION_READY));
        let cond = status.get_condition(CONDITION_READY).unwrap();
        assert_eq!(cond.reason.as_deref(), Some(REASON_CLUSTER_INITIALIZING));
    }

    #[test]
    fn test_set_degraded_partial_failure() {
        let mut status = empty_status();
        let mut mgr = StatusConditionManager::new(&mut status);
        mgr.set_degraded_partial_failure(2, 3);

        assert!(status.is_condition_true(CONDITION_DEGRADED));
        let cond = status.get_condition(CONDITION_DEGRADED).unwrap();
        assert_eq!(cond.reason.as_deref(), Some(REASON_PARTIAL_REPLICA_FAILURE));
    }

    #[test]
    fn test_degraded_clears_when_healthy() {
        let mut status = empty_status();
        let mut mgr = StatusConditionManager::new(&mut status);
        mgr.set_degraded_partial_failure(3, 3);

        assert!(!status.is_condition_true(CONDITION_DEGRADED));
    }

    #[test]
    fn test_set_scaling_up() {
        let mut status = empty_status();
        let mut mgr = StatusConditionManager::new(&mut status);
        mgr.set_scaling_up(3, 5);

        assert!(status.is_condition_true(CONDITION_SCALING));
        let cond = status.get_condition(CONDITION_SCALING).unwrap();
        assert_eq!(cond.reason.as_deref(), Some(REASON_SCALING_UP));
        assert!(cond.message.as_deref().unwrap().contains("3"));
        assert!(cond.message.as_deref().unwrap().contains("5"));
    }

    #[test]
    fn test_set_scaling_down() {
        let mut status = empty_status();
        let mut mgr = StatusConditionManager::new(&mut status);
        mgr.set_scaling_down(5, 3);

        assert!(status.is_condition_true(CONDITION_SCALING));
        let cond = status.get_condition(CONDITION_SCALING).unwrap();
        assert_eq!(cond.reason.as_deref(), Some(REASON_SCALING_DOWN));
    }

    #[test]
    fn test_clear_scaling() {
        let mut status = empty_status();
        let mut mgr = StatusConditionManager::new(&mut status);
        mgr.set_scaling_up(3, 5);
        assert!(status.is_condition_true(CONDITION_SCALING));

        let mut mgr = StatusConditionManager::new(&mut status);
        mgr.clear_scaling();
        assert!(!status.is_condition_true(CONDITION_SCALING));
    }

    #[test]
    fn test_set_upgrading() {
        let mut status = empty_status();
        let mut mgr = StatusConditionManager::new(&mut status);
        mgr.set_upgrading("1.0.0", "2.0.0", 1, 3);

        assert!(status.is_condition_true(CONDITION_UPGRADING));
        let cond = status.get_condition(CONDITION_UPGRADING).unwrap();
        assert_eq!(
            cond.reason.as_deref(),
            Some(REASON_ROLLING_UPDATE_IN_PROGRESS)
        );
        assert!(cond.message.as_deref().unwrap().contains("1.0.0"));
        assert!(cond.message.as_deref().unwrap().contains("2.0.0"));
    }

    #[test]
    fn test_set_upgrading_rollback() {
        let mut status = empty_status();
        let mut mgr = StatusConditionManager::new(&mut status);
        mgr.set_upgrading_rollback("1.0.0", "health check failed");

        assert!(status.is_condition_true(CONDITION_UPGRADING));
        let cond = status.get_condition(CONDITION_UPGRADING).unwrap();
        assert_eq!(cond.reason.as_deref(), Some(REASON_ROLLBACK_IN_PROGRESS));
    }

    #[test]
    fn test_clear_upgrading() {
        let mut status = empty_status();
        let mut mgr = StatusConditionManager::new(&mut status);
        mgr.set_upgrading("1.0.0", "2.0.0", 3, 3);
        assert!(status.is_condition_true(CONDITION_UPGRADING));

        let mut mgr = StatusConditionManager::new(&mut status);
        mgr.clear_upgrading();
        assert!(!status.is_condition_true(CONDITION_UPGRADING));
    }

    #[test]
    fn test_set_all_healthy() {
        let mut status = empty_status();
        let mut mgr = StatusConditionManager::new(&mut status);
        mgr.set_all_healthy(3, 3);

        assert!(status.is_condition_true(CONDITION_READY));
        assert!(!status.is_condition_true(CONDITION_DEGRADED));
        assert!(!status.is_condition_true(CONDITION_SCALING));
        assert!(!status.is_condition_true(CONDITION_UPGRADING));
        assert_eq!(status.conditions.len(), 4);
    }

    #[test]
    fn test_condition_replacement() {
        let mut status = empty_status();

        // Set ready = true
        let mut mgr = StatusConditionManager::new(&mut status);
        mgr.set_ready(3, 3);
        assert!(status.is_condition_true(CONDITION_READY));

        // Update to ready = false
        let mut mgr = StatusConditionManager::new(&mut status);
        mgr.set_ready(1, 3);
        assert!(!status.is_condition_true(CONDITION_READY));

        // Should still have exactly one Ready condition
        let ready_count = status
            .conditions
            .iter()
            .filter(|c| c.condition_type == CONDITION_READY)
            .count();
        assert_eq!(ready_count, 1);
    }

    #[test]
    fn test_degraded_primary_unhealthy() {
        let mut status = empty_status();
        let mut mgr = StatusConditionManager::new(&mut status);
        mgr.set_degraded_primary_unhealthy("test-0");

        assert!(status.is_condition_true(CONDITION_DEGRADED));
        let cond = status.get_condition(CONDITION_DEGRADED).unwrap();
        assert_eq!(cond.reason.as_deref(), Some(REASON_PRIMARY_UNHEALTHY));
        assert!(cond.message.as_deref().unwrap().contains("test-0"));
    }

    #[test]
    fn test_degraded_quorum_at_risk() {
        let mut status = empty_status();
        let mut mgr = StatusConditionManager::new(&mut status);
        mgr.set_degraded_quorum_at_risk(1, 2);

        assert!(status.is_condition_true(CONDITION_DEGRADED));
        let cond = status.get_condition(CONDITION_DEGRADED).unwrap();
        assert_eq!(cond.reason.as_deref(), Some(REASON_QUORUM_AT_RISK));
    }

    #[test]
    fn test_scaling_slot_migration() {
        let mut status = empty_status();
        let mut mgr = StatusConditionManager::new(&mut status);
        mgr.set_scaling_slot_migration(2);

        assert!(status.is_condition_true(CONDITION_SCALING));
        let cond = status.get_condition(CONDITION_SCALING).unwrap();
        assert_eq!(
            cond.reason.as_deref(),
            Some(REASON_SLOT_MIGRATION_IN_PROGRESS)
        );
    }

    #[test]
    fn test_upgrading_pre_check() {
        let mut status = empty_status();
        let mut mgr = StatusConditionManager::new(&mut status);
        mgr.set_upgrading_pre_check();

        assert!(status.is_condition_true(CONDITION_UPGRADING));
        let cond = status.get_condition(CONDITION_UPGRADING).unwrap();
        assert_eq!(cond.reason.as_deref(), Some(REASON_PRE_UPDATE_CHECK));
    }

    #[test]
    fn test_upgrading_post_verify() {
        let mut status = empty_status();
        let mut mgr = StatusConditionManager::new(&mut status);
        mgr.set_upgrading_post_verify(2, 3);

        assert!(status.is_condition_true(CONDITION_UPGRADING));
        let cond = status.get_condition(CONDITION_UPGRADING).unwrap();
        assert_eq!(cond.reason.as_deref(), Some(REASON_POST_UPDATE_VERIFY));
    }
}
