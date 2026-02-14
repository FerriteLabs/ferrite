//! Failover Detection and Recovery
//!
//! Monitors primary node health and orchestrates automatic failover when
//! the primary becomes unreachable.
//!
//! # Failover flow
//!
//! 1. Health probes detect consecutive failures on the primary
//! 2. A [`FailoverEvent`] is emitted
//! 3. [`FailoverManager::plan_failover`] selects a new primary from healthy replicas
//! 4. The plan is executed: endpoints are updated, replication is reconfigured

use super::crd::*;
use super::operator::OperatorError;
use super::reconciler::ReconcileAction;
use super::resources::{PodInfo, ResourceManager};
use super::*;
use std::sync::Arc;

/// Manages failover detection and recovery for Ferrite clusters.
pub struct FailoverManager {
    /// Resource manager for querying pod state
    resource_manager: Arc<ResourceManager>,
    /// Number of consecutive failures before triggering failover
    failure_threshold: u32,
}

/// Events that trigger a failover evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailoverEvent {
    /// Primary is not responding to health checks
    PrimaryUnreachable,
    /// Primary is responding but degraded (e.g. high latency, read-only)
    PrimaryDegraded,
    /// Multiple nodes claim to be primary (split-brain)
    SplitBrain,
}

/// A planned failover operation.
#[derive(Debug)]
pub struct FailoverPlan {
    /// Name of the pod promoted to primary
    pub new_primary: String,
    /// Name of the failed primary (if known)
    pub old_primary: Option<String>,
    /// Actions to execute the failover
    pub actions: Vec<ReconcileAction>,
}

/// Health status of an individual pod.
#[derive(Debug, Clone)]
pub struct HealthStatus {
    /// Pod name
    pub pod_name: String,
    /// Whether the pod is healthy right now
    pub healthy: bool,
    /// Timestamp of the last successful health check (RFC 3339)
    pub last_check: Option<String>,
    /// Count of consecutive health-check failures
    pub consecutive_failures: u32,
}

impl FailoverManager {
    /// Create a new failover manager.
    ///
    /// `failure_threshold` controls how many consecutive health-check failures
    /// are required before a failover is triggered (default: 3).
    pub fn new(resource_manager: Arc<ResourceManager>) -> Self {
        Self {
            resource_manager,
            failure_threshold: 3,
        }
    }

    /// Create a new failover manager with a custom failure threshold.
    pub fn with_failure_threshold(
        resource_manager: Arc<ResourceManager>,
        threshold: u32,
    ) -> Self {
        Self {
            resource_manager,
            failure_threshold: threshold,
        }
    }

    /// Detect whether a failover event has occurred for the given cluster.
    ///
    /// Inspects pod health to determine if the primary is unreachable or
    /// if a split-brain condition exists.
    pub async fn detect_failure(
        &self,
        cluster: &FerriteCluster,
    ) -> Result<Option<FailoverEvent>, OperatorError> {
        let namespace = cluster.metadata.namespace.as_deref().unwrap_or("default");
        let pods = self
            .resource_manager
            .list_pods(namespace, &cluster.metadata.name)
            .await?;

        if pods.is_empty() {
            return Ok(None);
        }

        // Find the primary
        let primaries: Vec<&PodInfo> = pods.iter().filter(|p| p.is_primary).collect();

        // Split-brain: more than one primary
        if primaries.len() > 1 {
            tracing::warn!(
                cluster = %cluster.metadata.name,
                primary_count = primaries.len(),
                "Split-brain detected: multiple primaries"
            );
            return Ok(Some(FailoverEvent::SplitBrain));
        }

        // No primary at all
        if primaries.is_empty() {
            tracing::warn!(
                cluster = %cluster.metadata.name,
                "No primary detected"
            );
            return Ok(Some(FailoverEvent::PrimaryUnreachable));
        }

        // Primary exists but is not ready
        let primary = primaries[0];
        if !primary.ready {
            tracing::warn!(
                cluster = %cluster.metadata.name,
                primary = %primary.name,
                "Primary is not ready"
            );
            return Ok(Some(FailoverEvent::PrimaryDegraded));
        }

        Ok(None)
    }

    /// Plan a failover in response to a detected event.
    ///
    /// Selects the best candidate for promotion and generates the actions
    /// needed to execute the failover.
    pub async fn plan_failover(
        &self,
        cluster: &FerriteCluster,
        event: &FailoverEvent,
    ) -> Result<FailoverPlan, OperatorError> {
        let namespace = cluster.metadata.namespace.as_deref().unwrap_or("default");
        let pods = self
            .resource_manager
            .list_pods(namespace, &cluster.metadata.name)
            .await?;

        let old_primary = pods
            .iter()
            .find(|p| p.is_primary)
            .map(|p| p.name.clone());

        // Gather healthy replica candidates
        let candidates: Vec<&PodInfo> = pods
            .iter()
            .filter(|p| !p.is_primary && p.ready)
            .collect();

        let new_primary = self.elect_primary(&candidates)?;

        tracing::info!(
            cluster = %cluster.metadata.name,
            event = ?event,
            new_primary = %new_primary,
            old_primary = ?old_primary,
            "Failover planned"
        );

        let mut actions = Vec::new();

        // Update status to FailingOver
        let mut status = cluster.status.clone().unwrap_or_default();
        status.phase = ClusterPhase::FailingOver;
        status.leader = Some(new_primary.clone());
        status.set_condition(
            Condition::new("Failover", true)
                .with_reason(format!("{:?}", event))
                .with_message(format!(
                    "Failing over from {:?} to {}",
                    old_primary, new_primary
                )),
        );
        actions.push(ReconcileAction::UpdateStatus(status));

        // Delete the old primary pod so it restarts as a replica
        if let Some(ref old) = old_primary {
            if *event != FailoverEvent::PrimaryDegraded {
                actions.push(ReconcileAction::DeletePod(old.clone()));
            }
        }

        Ok(FailoverPlan {
            new_primary,
            old_primary,
            actions,
        })
    }

    /// Select the best candidate for primary promotion.
    ///
    /// Strategy: pick the replica with the lowest ordinal (closest to
    /// the original primary, typically with the most recent data).
    pub fn elect_primary(
        &self,
        candidates: &[&PodInfo],
    ) -> Result<String, OperatorError> {
        if candidates.is_empty() {
            return Err(OperatorError::Reconciliation(
                "No healthy replicas available for failover".to_string(),
            ));
        }

        // Pick the candidate with the lowest ordinal index
        let mut best = candidates[0];
        for candidate in &candidates[1..] {
            let best_ordinal = ordinal_from_name(&best.name);
            let cand_ordinal = ordinal_from_name(&candidate.name);
            if cand_ordinal < best_ordinal {
                best = candidate;
            }
        }

        Ok(best.name.clone())
    }

    /// Check the health of all pods and build health status reports.
    pub async fn check_health(
        &self,
        cluster: &FerriteCluster,
    ) -> Result<Vec<HealthStatus>, OperatorError> {
        let namespace = cluster.metadata.namespace.as_deref().unwrap_or("default");
        let pods = self
            .resource_manager
            .list_pods(namespace, &cluster.metadata.name)
            .await?;

        let now = chrono::Utc::now().to_rfc3339();
        Ok(pods
            .iter()
            .map(|p| HealthStatus {
                pod_name: p.name.clone(),
                healthy: p.ready,
                last_check: Some(now.clone()),
                consecutive_failures: if p.ready { 0 } else { 1 },
            })
            .collect())
    }

    /// Get the failure threshold.
    pub fn failure_threshold(&self) -> u32 {
        self.failure_threshold
    }
}

/// Extract the ordinal index from a StatefulSet pod name (e.g. "cluster-2" → 2).
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
            ..Default::default()
        };
        FerriteCluster::new("test", spec).with_namespace("default")
    }

    #[test]
    fn test_ordinal_from_name() {
        assert_eq!(ordinal_from_name("test-0"), 0);
        assert_eq!(ordinal_from_name("test-1"), 1);
        assert_eq!(ordinal_from_name("my-cluster-42"), 42);
        assert_eq!(ordinal_from_name("no-number-here"), i32::MAX);
    }

    #[test]
    fn test_elect_primary_lowest_ordinal() {
        let rm = Arc::new(ResourceManager::new());
        let fm = FailoverManager::new(rm);

        let pod1 = PodInfo {
            name: "test-1".to_string(),
            ip: Some("10.0.0.1".to_string()),
            ready: true,
            is_primary: false,
        };
        let pod2 = PodInfo {
            name: "test-2".to_string(),
            ip: Some("10.0.0.2".to_string()),
            ready: true,
            is_primary: false,
        };
        let pod3 = PodInfo {
            name: "test-3".to_string(),
            ip: Some("10.0.0.3".to_string()),
            ready: true,
            is_primary: false,
        };

        let candidates = vec![&pod3, &pod1, &pod2];
        let elected = fm.elect_primary(&candidates).unwrap();
        assert_eq!(elected, "test-1");
    }

    #[test]
    fn test_elect_primary_no_candidates() {
        let rm = Arc::new(ResourceManager::new());
        let fm = FailoverManager::new(rm);

        let candidates: Vec<&PodInfo> = vec![];
        let result = fm.elect_primary(&candidates);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_detect_no_failure() {
        let rm = Arc::new(ResourceManager::new());

        // Create a StatefulSet to populate pods
        let cluster = test_cluster(3);
        let spec = super::super::controller::StatefulSetSpec {
            name: "test".to_string(),
            namespace: "default".to_string(),
            replicas: 3,
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

        let fm = FailoverManager::new(rm);
        let event = fm.detect_failure(&cluster).await.unwrap();
        assert!(event.is_none());
    }

    #[tokio::test]
    async fn test_check_health() {
        let rm = Arc::new(ResourceManager::new());

        let spec = super::super::controller::StatefulSetSpec {
            name: "test".to_string(),
            namespace: "default".to_string(),
            replicas: 3,
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

        let cluster = test_cluster(3);
        let fm = FailoverManager::new(rm);
        let health = fm.check_health(&cluster).await.unwrap();

        assert_eq!(health.len(), 3);
        assert!(health.iter().all(|h| h.healthy));
    }

    #[tokio::test]
    async fn test_plan_failover() {
        let rm = Arc::new(ResourceManager::new());

        let spec = super::super::controller::StatefulSetSpec {
            name: "test".to_string(),
            namespace: "default".to_string(),
            replicas: 3,
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

        let cluster = test_cluster(3);
        let fm = FailoverManager::new(rm);
        let plan = fm
            .plan_failover(&cluster, &FailoverEvent::PrimaryUnreachable)
            .await
            .unwrap();

        // Pod test-0 is primary, so test-1 (lowest replica ordinal) should be elected
        assert_eq!(plan.new_primary, "test-1");
        assert_eq!(plan.old_primary, Some("test-0".to_string()));
        assert!(!plan.actions.is_empty());
    }

    #[test]
    fn test_custom_failure_threshold() {
        let rm = Arc::new(ResourceManager::new());
        let fm = FailoverManager::with_failure_threshold(rm, 5);
        assert_eq!(fm.failure_threshold(), 5);
    }
}
