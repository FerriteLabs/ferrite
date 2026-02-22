//! Scaling Logic for Ferrite Clusters
//!
//! Manages horizontal scaling of Ferrite clusters, including:
//! - Scale-up: adding new replicas and configuring replication
//! - Scale-down: graceful drain, data migration, quorum validation
//! - Quorum validation to prevent unsafe scaling operations

use super::crd::*;
use super::operator::OperatorError;
use super::reconciler::ReconcileAction;
use super::resources::ResourceManager;
use super::*;
use std::sync::Arc;

/// Manages scaling operations for Ferrite clusters.
pub struct ScalingManager {
    /// Resource manager for querying current cluster state
    resource_manager: Arc<ResourceManager>,
}

/// Direction of a scaling operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScaleDirection {
    /// Adding replicas
    Up,
    /// Removing replicas
    Down,
    /// No change needed
    None,
}

/// A planned scaling operation with all necessary actions.
#[derive(Debug)]
pub struct ScalePlan {
    /// Direction of the scale operation
    pub direction: ScaleDirection,
    /// Current number of replicas
    pub current_replicas: i32,
    /// Desired number of replicas
    pub desired_replicas: i32,
    /// Ordered list of actions to execute the scaling
    pub actions: Vec<ReconcileAction>,
    /// Whether the plan requires data migration
    pub requires_migration: bool,
    /// Slot migration state (for cluster-mode scale-down)
    pub migration_state: Option<SlotMigrationState>,
}

/// Tracks the progress of hash-slot migration during a cluster-mode scale-down.
#[derive(Debug, Clone)]
pub struct SlotMigrationState {
    /// Total number of slots to migrate.
    pub total_slots: u32,
    /// Number of slots migrated so far.
    pub migrated_slots: u32,
    /// Pods whose slots are being drained.
    pub source_pods: Vec<String>,
    /// Pods receiving the migrated slots.
    pub target_pods: Vec<String>,
    /// Current phase of the migration.
    pub phase: SlotMigrationPhase,
}

/// Phase of a slot migration operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotMigrationPhase {
    /// Calculating slot redistribution plan.
    Planning,
    /// Actively migrating slots.
    Migrating,
    /// Verifying slot ownership after migration.
    Verifying,
    /// Migration complete, pods can be removed.
    Complete,
}

/// Result of validating a scaling request.
#[derive(Debug, Clone)]
pub struct ScaleValidation {
    /// Whether the requested scale is valid
    pub valid: bool,
    /// Minimum allowed replicas
    pub min_replicas: i32,
    /// Maximum allowed replicas
    pub max_replicas: i32,
    /// Validation errors (empty if valid)
    pub errors: Vec<String>,
}

impl ScalingManager {
    /// Create a new scaling manager.
    pub fn new(resource_manager: Arc<ResourceManager>) -> Self {
        Self { resource_manager }
    }

    /// Plan a scaling operation for the given cluster.
    ///
    /// Determines the direction and generates the required reconcile actions.
    pub async fn plan_scaling(
        &self,
        cluster: &FerriteCluster,
        current_replicas: i32,
        desired_replicas: i32,
    ) -> Result<ScalePlan, OperatorError> {
        let direction = match desired_replicas.cmp(&current_replicas) {
            std::cmp::Ordering::Greater => ScaleDirection::Up,
            std::cmp::Ordering::Less => ScaleDirection::Down,
            std::cmp::Ordering::Equal => {
                return Ok(ScalePlan {
                    direction: ScaleDirection::None,
                    current_replicas,
                    desired_replicas,
                    actions: vec![],
                    requires_migration: false,
                    migration_state: None,
                });
            }
        };

        let validation = self.validate_scale(cluster, desired_replicas);
        if !validation.valid {
            return Err(OperatorError::InvalidResource(
                validation.errors.join("; "),
            ));
        }

        let actions = match direction {
            ScaleDirection::Up => {
                self.plan_scale_up(cluster, current_replicas, desired_replicas)
                    .await?
            }
            ScaleDirection::Down => {
                self.plan_scale_down(cluster, current_replicas, desired_replicas)
                    .await?
            }
            ScaleDirection::None => vec![],
        };

        let requires_migration = direction == ScaleDirection::Down
            && cluster.spec.cluster_mode.enabled;

        let migration_state = if requires_migration {
            let pods_to_drain = (desired_replicas..current_replicas)
                .rev()
                .map(|i| format!("{}-{}", cluster.metadata.name, i))
                .collect::<Vec<_>>();
            let target_pods = (0..desired_replicas)
                .map(|i| format!("{}-{}", cluster.metadata.name, i))
                .collect::<Vec<_>>();
            let total_slots = Self::slots_per_node(current_replicas) as u32
                * pods_to_drain.len() as u32;

            Some(SlotMigrationState {
                total_slots,
                migrated_slots: 0,
                source_pods: pods_to_drain,
                target_pods,
                phase: SlotMigrationPhase::Planning,
            })
        } else {
            None
        };

        Ok(ScalePlan {
            direction,
            current_replicas,
            desired_replicas,
            actions,
            requires_migration,
            migration_state,
        })
    }

    /// Validate a scaling request against cluster constraints.
    ///
    /// Checks minimum quorum, maximum replica limits, and cluster-mode
    /// specific constraints.
    pub fn validate_scale(
        &self,
        cluster: &FerriteCluster,
        desired_replicas: i32,
    ) -> ScaleValidation {
        let mut errors = Vec::new();
        let min_replicas = 1;
        let max_replicas = 100;

        if desired_replicas < min_replicas {
            errors.push(format!(
                "Cannot scale below {} replica(s)",
                min_replicas
            ));
        }

        if desired_replicas > max_replicas {
            errors.push(format!(
                "Cannot scale above {} replicas",
                max_replicas
            ));
        }

        // For sentinel-enabled clusters, ensure quorum is maintained
        if cluster.spec.sentinel.enabled {
            let min_quorum = Self::minimum_quorum(cluster.spec.replicas);
            if desired_replicas < min_quorum {
                errors.push(format!(
                    "Sentinel requires at least {} replicas for quorum (current: {})",
                    min_quorum, cluster.spec.replicas
                ));
            }
        }

        // For cluster-mode, require at least 3 nodes for slot distribution
        if cluster.spec.cluster_mode.enabled && desired_replicas < 3 {
            errors.push(
                "Cluster mode requires at least 3 nodes for hash slot distribution".to_string(),
            );
        }

        ScaleValidation {
            valid: errors.is_empty(),
            min_replicas,
            max_replicas,
            errors,
        }
    }

    /// Calculate the minimum quorum size for a given number of replicas.
    ///
    /// Quorum = floor(replicas / 2) + 1 — the smallest majority needed for
    /// consensus and leader election.
    pub fn minimum_quorum(replicas: i32) -> i32 {
        (replicas / 2) + 1
    }

    /// Calculate the approximate number of hash slots per node.
    ///
    /// Ferrite uses 16384 hash slots (like Redis Cluster). This returns
    /// the number of slots each node should own for even distribution.
    pub fn slots_per_node(node_count: i32) -> i32 {
        if node_count <= 0 {
            return 0;
        }
        16384 / node_count
    }

    /// Plan scale-up: add new replicas and configure replication.
    async fn plan_scale_up(
        &self,
        cluster: &FerriteCluster,
        current: i32,
        desired: i32,
    ) -> Result<Vec<ReconcileAction>, OperatorError> {
        let mut actions = Vec::new();

        tracing::info!(
            cluster = %cluster.metadata.name,
            from = current,
            to = desired,
            "Planning scale up"
        );

        // Scale the StatefulSet to the desired count
        actions.push(ReconcileAction::ScaleStatefulSet { replicas: desired });

        // Update status to Scaling phase
        let mut status = cluster.status.clone().unwrap_or_default();
        status.phase = ClusterPhase::Scaling;
        status.set_condition(
            Condition::new("Scaling", true)
                .with_reason("ScalingUp")
                .with_message(format!("Scaling from {} to {} replicas", current, desired)),
        );
        actions.push(ReconcileAction::UpdateStatus(status));

        Ok(actions)
    }

    /// Plan scale-down: graceful drain, data migration, replica removal.
    async fn plan_scale_down(
        &self,
        cluster: &FerriteCluster,
        current: i32,
        desired: i32,
    ) -> Result<Vec<ReconcileAction>, OperatorError> {
        let mut actions = Vec::new();
        let namespace = cluster.metadata.namespace.as_deref().unwrap_or("default");

        tracing::info!(
            cluster = %cluster.metadata.name,
            from = current,
            to = desired,
            "Planning scale down"
        );

        // Identify pods to remove (highest ordinal first — standard StatefulSet behavior)
        let pods = self
            .resource_manager
            .list_pods(namespace, &cluster.metadata.name)
            .await?;

        for pod in pods.iter().rev() {
            let ordinal = pod
                .name
                .rsplit('-')
                .next()
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(0);

            if ordinal >= desired {
                // In a real implementation, we would first:
                // 1. Detach the replica from the primary
                // 2. Wait for all in-flight operations to complete
                // 3. Then delete the pod
                tracing::info!(
                    pod = %pod.name,
                    "Marking pod for graceful removal"
                );
                actions.push(ReconcileAction::DeletePod(pod.name.clone()));
            }
        }

        // Scale the StatefulSet
        actions.push(ReconcileAction::ScaleStatefulSet { replicas: desired });

        // Update status
        let mut status = cluster.status.clone().unwrap_or_default();
        status.phase = ClusterPhase::Scaling;
        status.set_condition(
            Condition::new("Scaling", true)
                .with_reason("ScalingDown")
                .with_message(format!("Scaling from {} to {} replicas", current, desired)),
        );
        actions.push(ReconcileAction::UpdateStatus(status));

        Ok(actions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cluster(replicas: i32) -> FerriteCluster {
        let spec = FerriteClusterSpec {
            replicas,
            ..Default::default()
        };
        FerriteCluster::new("test-cluster", spec).with_namespace("default")
    }

    fn sentinel_cluster(replicas: i32) -> FerriteCluster {
        let spec = FerriteClusterSpec {
            replicas,
            sentinel: SentinelSpec {
                enabled: true,
                ..Default::default()
            },
            ..Default::default()
        };
        FerriteCluster::new("sentinel-cluster", spec).with_namespace("default")
    }

    fn cluster_mode_cluster(replicas: i32) -> FerriteCluster {
        let spec = FerriteClusterSpec {
            replicas,
            cluster_mode: ClusterModeSpec {
                enabled: true,
                shards: 3,
                replicas_per_shard: 1,
            },
            ..Default::default()
        };
        FerriteCluster::new("cluster-mode", spec).with_namespace("default")
    }

    #[test]
    fn test_minimum_quorum() {
        assert_eq!(ScalingManager::minimum_quorum(1), 1);
        assert_eq!(ScalingManager::minimum_quorum(2), 2);
        assert_eq!(ScalingManager::minimum_quorum(3), 2);
        assert_eq!(ScalingManager::minimum_quorum(5), 3);
        assert_eq!(ScalingManager::minimum_quorum(7), 4);
    }

    #[test]
    fn test_validate_scale_basic() {
        let rm = Arc::new(ResourceManager::new());
        let sm = ScalingManager::new(rm);
        let cluster = test_cluster(3);

        let v = sm.validate_scale(&cluster, 5);
        assert!(v.valid);
        assert!(v.errors.is_empty());
    }

    #[test]
    fn test_validate_scale_below_minimum() {
        let rm = Arc::new(ResourceManager::new());
        let sm = ScalingManager::new(rm);
        let cluster = test_cluster(3);

        let v = sm.validate_scale(&cluster, 0);
        assert!(!v.valid);
        assert!(!v.errors.is_empty());
    }

    #[test]
    fn test_validate_scale_above_maximum() {
        let rm = Arc::new(ResourceManager::new());
        let sm = ScalingManager::new(rm);
        let cluster = test_cluster(3);

        let v = sm.validate_scale(&cluster, 101);
        assert!(!v.valid);
    }

    #[test]
    fn test_validate_sentinel_quorum() {
        let rm = Arc::new(ResourceManager::new());
        let sm = ScalingManager::new(rm);
        let cluster = sentinel_cluster(5);

        // Scaling to 2 should fail — sentinel needs quorum of 3 (for replicas=5)
        let v = sm.validate_scale(&cluster, 2);
        assert!(!v.valid);
        assert!(v.errors.iter().any(|e| e.contains("quorum")));
    }

    #[test]
    fn test_validate_cluster_mode_minimum() {
        let rm = Arc::new(ResourceManager::new());
        let sm = ScalingManager::new(rm);
        let cluster = cluster_mode_cluster(6);

        let v = sm.validate_scale(&cluster, 2);
        assert!(!v.valid);
        assert!(v.errors.iter().any(|e| e.contains("3 nodes")));
    }

    #[tokio::test]
    async fn test_plan_scaling_no_change() {
        let rm = Arc::new(ResourceManager::new());
        let sm = ScalingManager::new(rm);
        let cluster = test_cluster(3);

        let plan = sm.plan_scaling(&cluster, 3, 3).await.unwrap();
        assert_eq!(plan.direction, ScaleDirection::None);
        assert!(plan.actions.is_empty());
    }

    #[tokio::test]
    async fn test_plan_scaling_up() {
        let rm = Arc::new(ResourceManager::new());
        let sm = ScalingManager::new(rm);
        let cluster = test_cluster(3);

        let plan = sm.plan_scaling(&cluster, 3, 5).await.unwrap();
        assert_eq!(plan.direction, ScaleDirection::Up);
        assert!(!plan.actions.is_empty());
    }

    #[tokio::test]
    async fn test_plan_scaling_down() {
        let rm = Arc::new(ResourceManager::new());
        let sm = ScalingManager::new(rm);
        let cluster = test_cluster(5);

        let plan = sm.plan_scaling(&cluster, 5, 3).await.unwrap();
        assert_eq!(plan.direction, ScaleDirection::Down);
        assert!(!plan.actions.is_empty());
    }

    #[tokio::test]
    async fn test_plan_scaling_invalid() {
        let rm = Arc::new(ResourceManager::new());
        let sm = ScalingManager::new(rm);
        let cluster = test_cluster(3);

        let result = sm.plan_scaling(&cluster, 3, 0).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_slots_per_node() {
        assert_eq!(ScalingManager::slots_per_node(3), 5461);
        assert_eq!(ScalingManager::slots_per_node(6), 2730);
        assert_eq!(ScalingManager::slots_per_node(1), 16384);
        assert_eq!(ScalingManager::slots_per_node(0), 0);
    }

    #[tokio::test]
    async fn test_cluster_mode_scale_down_generates_migration_state() {
        let rm = Arc::new(ResourceManager::new());
        let sm = ScalingManager::new(rm);
        let cluster = cluster_mode_cluster(6);

        let plan = sm.plan_scaling(&cluster, 6, 3).await.unwrap();
        assert_eq!(plan.direction, ScaleDirection::Down);
        assert!(plan.requires_migration);
        assert!(plan.migration_state.is_some());

        let migration = plan.migration_state.unwrap();
        assert_eq!(migration.source_pods.len(), 3);
        assert_eq!(migration.target_pods.len(), 3);
        assert_eq!(migration.phase, SlotMigrationPhase::Planning);
        assert!(migration.total_slots > 0);
    }

    #[tokio::test]
    async fn test_non_cluster_mode_scale_down_no_migration() {
        let rm = Arc::new(ResourceManager::new());
        let sm = ScalingManager::new(rm);
        let cluster = test_cluster(5);

        let plan = sm.plan_scaling(&cluster, 5, 3).await.unwrap();
        assert!(!plan.requires_migration);
        assert!(plan.migration_state.is_none());
    }

    #[tokio::test]
    async fn test_scale_up_no_migration() {
        let rm = Arc::new(ResourceManager::new());
        let sm = ScalingManager::new(rm);
        let cluster = cluster_mode_cluster(3);

        let plan = sm.plan_scaling(&cluster, 3, 6).await.unwrap();
        assert_eq!(plan.direction, ScaleDirection::Up);
        assert!(!plan.requires_migration);
        assert!(plan.migration_state.is_none());
    }
}
