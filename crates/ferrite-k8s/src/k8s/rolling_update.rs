//! Rolling Update Strategy
//!
//! Orchestrates zero-downtime version upgrades for Ferrite clusters.
//!
//! # Update order
//!
//! 1. Update **replicas** first (highest ordinal → lowest)
//! 2. Health-check each pod before proceeding to the next
//! 3. Update the **primary** last
//! 4. Automatic rollback if any pod fails health checks after update

use super::crd::*;
use super::operator::OperatorError;
use super::reconciler::ReconcileAction;
use super::resources::{PodInfo, ResourceManager};
use super::*;
use std::sync::Arc;
use std::time::Duration;

/// Manages rolling update operations for Ferrite clusters.
pub struct RollingUpdateManager {
    /// Resource manager for querying pod state
    resource_manager: Arc<ResourceManager>,
    /// Health check timeout per pod
    health_check_timeout: Duration,
    /// Maximum consecutive failures before triggering rollback
    max_failures: u32,
    /// Maximum pods unavailable at once
    max_unavailable: i32,
    /// Maximum extra pods above desired count
    max_surge: i32,
    /// Whether to run pre-update health check
    pre_update_health_check: bool,
    /// Whether to run post-update verification
    post_update_verify: bool,
}

/// The strategy used for updating pods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateStrategy {
    /// Rolling update — pods are updated one by one
    RollingUpdate,
    /// On-delete — pods are only updated when manually deleted
    OnDelete,
}

impl Default for UpdateStrategy {
    fn default() -> Self {
        Self::RollingUpdate
    }
}

/// A plan describing which pods need updating and in what order.
#[derive(Debug)]
pub struct UpdatePlan {
    /// Pods to update, in order (replicas first, primary last)
    pub pods_to_update: Vec<String>,
    /// Target image/version
    pub target_version: String,
    /// Previous image/version (for rollback)
    pub previous_version: String,
    /// Strategy
    pub strategy: UpdateStrategy,
    /// Whether pre-update check passed
    pub pre_check_passed: bool,
}

/// Progress tracker for an in-flight rolling update.
#[derive(Debug, Clone)]
pub struct UpdateProgress {
    /// Total pods to update
    pub total: usize,
    /// Pods successfully updated
    pub completed: usize,
    /// Pods that failed the update
    pub failed: usize,
    /// Pod currently being updated (if any)
    pub current_pod: Option<String>,
}

/// Outcome of a single update step.
#[derive(Debug)]
pub enum UpdateStepResult {
    /// Pod updated and healthy — continue to next
    Continue,
    /// All pods updated — done (or pending post-verification)
    Complete,
    /// Pod failed health check — should rollback
    Rollback { failed_pod: String, reason: String },
    /// Post-update verification passed
    PostVerifyPassed,
    /// Post-update verification failed
    PostVerifyFailed { reason: String },
}

impl RollingUpdateManager {
    /// Create a new rolling update manager with default settings.
    pub fn new(resource_manager: Arc<ResourceManager>) -> Self {
        Self {
            resource_manager,
            health_check_timeout: Duration::from_secs(60),
            max_failures: 1,
            max_unavailable: 1,
            max_surge: 0,
            pre_update_health_check: true,
            post_update_verify: true,
        }
    }

    /// Create a new rolling update manager with custom settings.
    pub fn with_settings(
        resource_manager: Arc<ResourceManager>,
        health_check_timeout: Duration,
        max_failures: u32,
    ) -> Self {
        Self {
            resource_manager,
            health_check_timeout,
            max_failures,
            max_unavailable: 1,
            max_surge: 0,
            pre_update_health_check: true,
            post_update_verify: true,
        }
    }

    /// Create a rolling update manager from a cluster's `RollingUpdateConfig`.
    pub fn from_config(
        resource_manager: Arc<ResourceManager>,
        config: &super::crd::RollingUpdateConfig,
        replicas: i32,
    ) -> Self {
        Self {
            resource_manager,
            health_check_timeout: Duration::from_secs(config.health_check_timeout_secs),
            max_failures: config.max_consecutive_failures,
            max_unavailable: config.max_unavailable_count(replicas),
            max_surge: config.max_surge_count(replicas),
            pre_update_health_check: config.pre_update_health_check,
            post_update_verify: config.post_update_verify,
        }
    }

    /// Plan a rolling update from the current version to a new version.
    ///
    /// Pods are ordered so that replicas are updated before the primary,
    /// and higher-ordinal replicas are updated first.
    /// If `pre_update_health_check` is enabled, the plan will be rejected
    /// if the cluster is not fully healthy.
    pub async fn plan_update(
        &self,
        cluster: &FerriteCluster,
        target_version: &str,
    ) -> Result<UpdatePlan, OperatorError> {
        let namespace = cluster.metadata.namespace.as_deref().unwrap_or("default");
        let pods = self
            .resource_manager
            .list_pods(namespace, &cluster.metadata.name)
            .await?;

        let previous_version = cluster.spec.version.clone();

        // Pre-update health check
        let pre_check_passed = if self.pre_update_health_check {
            let all_healthy = pods.iter().all(|p| p.ready);
            if !all_healthy {
                tracing::warn!(
                    cluster = %cluster.metadata.name,
                    unhealthy = pods.iter().filter(|p| !p.ready).count(),
                    "Pre-update health check: not all pods are healthy"
                );
            }
            all_healthy
        } else {
            true
        };

        // Separate primary and replicas
        let mut replicas: Vec<&PodInfo> =
            pods.iter().filter(|p| !p.is_primary).collect();
        let primary: Option<&PodInfo> = pods.iter().find(|p| p.is_primary);

        // Sort replicas by ordinal descending (update highest first)
        replicas.sort_by(|a, b| {
            let ord_a = ordinal_from_name(&a.name);
            let ord_b = ordinal_from_name(&b.name);
            ord_b.cmp(&ord_a)
        });

        let mut pods_to_update: Vec<String> =
            replicas.iter().map(|p| p.name.clone()).collect();

        // Primary is updated last
        if let Some(p) = primary {
            pods_to_update.push(p.name.clone());
        }

        tracing::info!(
            cluster = %cluster.metadata.name,
            from = %previous_version,
            to = %target_version,
            pod_count = pods_to_update.len(),
            max_unavailable = self.max_unavailable,
            max_surge = self.max_surge,
            pre_check = pre_check_passed,
            "Rolling update planned"
        );

        Ok(UpdatePlan {
            pods_to_update,
            target_version: target_version.to_string(),
            previous_version,
            strategy: UpdateStrategy::RollingUpdate,
            pre_check_passed,
        })
    }

    /// Execute one step of the rolling update.
    ///
    /// Deletes the current pod (StatefulSet will recreate it with the new
    /// image) and checks its health.  Returns [`UpdateStepResult::Rollback`]
    /// if the health check fails.
    pub async fn execute_step(
        &self,
        cluster: &FerriteCluster,
        plan: &UpdatePlan,
        progress: &mut UpdateProgress,
    ) -> Result<UpdateStepResult, OperatorError> {
        if progress.completed >= plan.pods_to_update.len() {
            return Ok(UpdateStepResult::Complete);
        }

        let pod_name = &plan.pods_to_update[progress.completed];
        progress.current_pod = Some(pod_name.clone());

        tracing::info!(
            cluster = %cluster.metadata.name,
            pod = %pod_name,
            step = format!("{}/{}", progress.completed + 1, progress.total),
            "Updating pod"
        );

        // Delete the pod so StatefulSet recreates it with the new spec
        self.resource_manager.delete_pod(pod_name).await?;

        // Check health of the new pod
        let healthy = self.check_pod_health(cluster, pod_name).await?;

        if healthy {
            progress.completed += 1;
            progress.current_pod = None;

            if progress.completed >= progress.total {
                Ok(UpdateStepResult::Complete)
            } else {
                Ok(UpdateStepResult::Continue)
            }
        } else {
            progress.failed += 1;
            Ok(UpdateStepResult::Rollback {
                failed_pod: pod_name.clone(),
                reason: format!(
                    "Pod {} failed health check after update to {}",
                    pod_name, plan.target_version
                ),
            })
        }
    }

    /// Check whether a pod is healthy after an update.
    ///
    /// In a real implementation this would poll the pod readiness probe.
    /// Here we check the ResourceManager's simulated state.
    pub async fn check_pod_health(
        &self,
        cluster: &FerriteCluster,
        pod_name: &str,
    ) -> Result<bool, OperatorError> {
        let namespace = cluster.metadata.namespace.as_deref().unwrap_or("default");
        let pods = self
            .resource_manager
            .list_pods(namespace, &cluster.metadata.name)
            .await?;

        Ok(pods
            .iter()
            .find(|p| p.name == pod_name)
            .map(|p| p.ready)
            .unwrap_or(false))
    }

    /// Generate reconcile actions to rollback to the previous version.
    ///
    /// The rollback follows the same ordering as the update (replicas first,
    /// primary last) but targets the original version.
    pub async fn rollback(
        &self,
        cluster: &FerriteCluster,
        previous_version: &str,
    ) -> Result<Vec<ReconcileAction>, OperatorError> {
        tracing::warn!(
            cluster = %cluster.metadata.name,
            target = %previous_version,
            "Initiating rollback"
        );

        let mut actions = Vec::new();

        // Update status to indicate rollback
        let mut status = cluster.status.clone().unwrap_or_default();
        status.phase = ClusterPhase::Upgrading;
        status.set_condition(
            Condition::new("Rollback", true)
                .with_reason("UpdateFailed")
                .with_message(format!("Rolling back to version {}", previous_version)),
        );
        actions.push(ReconcileAction::UpdateStatus(status));

        // Delete all pods so they are recreated with the old spec
        // (In production, the StatefulSet spec would be reverted first)
        let namespace = cluster.metadata.namespace.as_deref().unwrap_or("default");
        let pods = self
            .resource_manager
            .list_pods(namespace, &cluster.metadata.name)
            .await?;

        // Replicas first (high ordinal → low), then primary
        let mut replicas: Vec<&PodInfo> =
            pods.iter().filter(|p| !p.is_primary).collect();
        replicas.sort_by(|a, b| {
            ordinal_from_name(&b.name).cmp(&ordinal_from_name(&a.name))
        });

        for r in &replicas {
            actions.push(ReconcileAction::DeletePod(r.name.clone()));
        }
        if let Some(primary) = pods.iter().find(|p| p.is_primary) {
            actions.push(ReconcileAction::DeletePod(primary.name.clone()));
        }

        Ok(actions)
    }

    /// Create a fresh [`UpdateProgress`] for a plan.
    pub fn new_progress(plan: &UpdatePlan) -> UpdateProgress {
        UpdateProgress {
            total: plan.pods_to_update.len(),
            completed: 0,
            failed: 0,
            current_pod: None,
        }
    }

    /// Get the configured health check timeout.
    pub fn health_check_timeout(&self) -> Duration {
        self.health_check_timeout
    }

    /// Get the max consecutive failures before rollback.
    pub fn max_failures(&self) -> u32 {
        self.max_failures
    }

    /// Get max unavailable pods during update.
    pub fn max_unavailable(&self) -> i32 {
        self.max_unavailable
    }

    /// Get max surge pods during update.
    pub fn max_surge(&self) -> i32 {
        self.max_surge
    }

    /// Run post-update verification: check that all pods are healthy after
    /// the update is complete.
    pub async fn post_update_verify(
        &self,
        cluster: &FerriteCluster,
        plan: &UpdatePlan,
    ) -> Result<UpdateStepResult, OperatorError> {
        if !self.post_update_verify {
            return Ok(UpdateStepResult::PostVerifyPassed);
        }

        tracing::info!(
            cluster = %cluster.metadata.name,
            version = %plan.target_version,
            "Running post-update verification"
        );

        let namespace = cluster.metadata.namespace.as_deref().unwrap_or("default");
        let pods = self
            .resource_manager
            .list_pods(namespace, &cluster.metadata.name)
            .await?;

        let unhealthy: Vec<_> = pods.iter().filter(|p| !p.ready).collect();
        if unhealthy.is_empty() {
            tracing::info!(
                cluster = %cluster.metadata.name,
                "Post-update verification passed: all {} pods healthy",
                pods.len()
            );
            Ok(UpdateStepResult::PostVerifyPassed)
        } else {
            let names: Vec<_> = unhealthy.iter().map(|p| p.name.as_str()).collect();
            let reason = format!(
                "Post-update verification failed: {} unhealthy pods: {}",
                unhealthy.len(),
                names.join(", ")
            );
            tracing::warn!(cluster = %cluster.metadata.name, %reason);
            Ok(UpdateStepResult::PostVerifyFailed { reason })
        }
    }
}

/// Extract the ordinal index from a StatefulSet pod name.
fn ordinal_from_name(name: &str) -> i32 {
    name.rsplit('-')
        .next()
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(i32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cluster(replicas: i32) -> FerriteCluster {
        let spec = FerriteClusterSpec {
            replicas,
            version: "1.0.0".to_string(),
            ..Default::default()
        };
        FerriteCluster::new("test", spec).with_namespace("default")
    }

    async fn setup_pods(rm: &ResourceManager, replicas: i32) {
        let spec = super::super::controller::StatefulSetSpec {
            name: "test".to_string(),
            namespace: "default".to_string(),
            replicas,
            service_name: "test-headless".to_string(),
            labels: std::collections::HashMap::new(),
            selector: std::collections::HashMap::new(),
            pod_spec: super::super::resources::PodSpec {
                containers: vec![],
                init_containers: vec![],
                volumes: vec![],
                service_account_name: None,
                security_context: None,
                node_selector: std::collections::HashMap::new(),
                tolerations: vec![],
                termination_grace_period_seconds: None,
            },
            volume_claim_templates: vec![],
            owner_reference: None,
        };
        rm.create_statefulset(&spec).await.unwrap();
    }

    #[tokio::test]
    async fn test_plan_update_order() {
        let rm = Arc::new(ResourceManager::new());
        setup_pods(&rm, 3).await;

        let rum = RollingUpdateManager::new(rm);
        let cluster = test_cluster(3);
        let plan = rum.plan_update(&cluster, "2.0.0").await.unwrap();

        // Expected order: replicas (highest ordinal first), then primary
        // Pods: test-0 (primary), test-1 (replica), test-2 (replica)
        // Update order: test-2, test-1, test-0
        assert_eq!(plan.pods_to_update.len(), 3);
        assert_eq!(plan.pods_to_update[0], "test-2");
        assert_eq!(plan.pods_to_update[1], "test-1");
        assert_eq!(plan.pods_to_update[2], "test-0"); // primary last
        assert_eq!(plan.target_version, "2.0.0");
        assert_eq!(plan.previous_version, "1.0.0");
    }

    #[tokio::test]
    async fn test_execute_step_success() {
        let rm = Arc::new(ResourceManager::new());
        setup_pods(&rm, 3).await;

        let rum = RollingUpdateManager::new(rm);
        let cluster = test_cluster(3);
        let plan = rum.plan_update(&cluster, "2.0.0").await.unwrap();
        let mut progress = RollingUpdateManager::new_progress(&plan);

        let result = rum.execute_step(&cluster, &plan, &mut progress).await.unwrap();
        // Simulated pods are always ready, so should continue
        assert!(matches!(result, UpdateStepResult::Continue));
        assert_eq!(progress.completed, 1);
    }

    #[tokio::test]
    async fn test_execute_all_steps() {
        let rm = Arc::new(ResourceManager::new());
        setup_pods(&rm, 3).await;

        let rum = RollingUpdateManager::new(rm);
        let cluster = test_cluster(3);
        let plan = rum.plan_update(&cluster, "2.0.0").await.unwrap();
        let mut progress = RollingUpdateManager::new_progress(&plan);

        for i in 0..3 {
            let result = rum
                .execute_step(&cluster, &plan, &mut progress)
                .await
                .unwrap();
            if i < 2 {
                assert!(matches!(result, UpdateStepResult::Continue));
            } else {
                assert!(matches!(result, UpdateStepResult::Complete));
            }
        }
        assert_eq!(progress.completed, 3);
        assert_eq!(progress.failed, 0);
    }

    #[tokio::test]
    async fn test_rollback() {
        let rm = Arc::new(ResourceManager::new());
        setup_pods(&rm, 3).await;

        let rum = RollingUpdateManager::new(rm);
        let cluster = test_cluster(3);
        let actions = rum.rollback(&cluster, "1.0.0").await.unwrap();

        // Should have: 1 status update + 3 pod deletions
        assert!(actions.len() >= 4);
    }

    #[tokio::test]
    async fn test_check_pod_health() {
        let rm = Arc::new(ResourceManager::new());
        setup_pods(&rm, 3).await;

        let rum = RollingUpdateManager::new(rm);
        let cluster = test_cluster(3);

        // All simulated pods are healthy
        assert!(rum.check_pod_health(&cluster, "test-0").await.unwrap());
        assert!(rum.check_pod_health(&cluster, "test-1").await.unwrap());
        // Non-existent pod is not healthy
        assert!(!rum.check_pod_health(&cluster, "test-99").await.unwrap());
    }

    #[test]
    fn test_new_progress() {
        let plan = UpdatePlan {
            pods_to_update: vec!["a".into(), "b".into(), "c".into()],
            target_version: "2.0.0".to_string(),
            previous_version: "1.0.0".to_string(),
            strategy: UpdateStrategy::RollingUpdate,
            pre_check_passed: true,
        };
        let progress = RollingUpdateManager::new_progress(&plan);
        assert_eq!(progress.total, 3);
        assert_eq!(progress.completed, 0);
        assert_eq!(progress.failed, 0);
        assert!(progress.current_pod.is_none());
    }

    #[test]
    fn test_custom_settings() {
        let rm = Arc::new(ResourceManager::new());
        let rum =
            RollingUpdateManager::with_settings(rm, Duration::from_secs(120), 3);
        assert_eq!(rum.health_check_timeout(), Duration::from_secs(120));
        assert_eq!(rum.max_failures(), 3);
    }

    #[test]
    fn test_from_config() {
        let rm = Arc::new(ResourceManager::new());
        let config = super::super::crd::RollingUpdateConfig {
            max_unavailable: "25%".to_string(),
            max_surge: "1".to_string(),
            health_check_timeout_secs: 120,
            max_consecutive_failures: 3,
            pre_update_health_check: false,
            post_update_verify: true,
        };
        let rum = RollingUpdateManager::from_config(rm, &config, 8);
        assert_eq!(rum.health_check_timeout(), Duration::from_secs(120));
        assert_eq!(rum.max_failures(), 3);
        assert_eq!(rum.max_unavailable(), 2); // 25% of 8
        assert_eq!(rum.max_surge(), 1);
    }

    #[tokio::test]
    async fn test_pre_update_health_check_passes() {
        let rm = Arc::new(ResourceManager::new());
        setup_pods(&rm, 3).await;

        let rum = RollingUpdateManager::new(rm);
        let cluster = test_cluster(3);
        let plan = rum.plan_update(&cluster, "2.0.0").await.unwrap();

        // All simulated pods are healthy, so pre-check should pass
        assert!(plan.pre_check_passed);
    }

    #[tokio::test]
    async fn test_post_update_verify_passes() {
        let rm = Arc::new(ResourceManager::new());
        setup_pods(&rm, 3).await;

        let rum = RollingUpdateManager::new(rm);
        let cluster = test_cluster(3);
        let plan = rum.plan_update(&cluster, "2.0.0").await.unwrap();

        let result = rum.post_update_verify(&cluster, &plan).await.unwrap();
        assert!(matches!(result, UpdateStepResult::PostVerifyPassed));
    }
}
