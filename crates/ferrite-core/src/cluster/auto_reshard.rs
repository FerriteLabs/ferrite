//! Auto-Resharding Engine
//!
//! Provides automatic, zero-downtime slot redistribution when cluster
//! topology changes (nodes join/leave). Monitors cluster state and
//! triggers rebalancing with configurable policies.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use super::migration::{MigrationState, SlotRebalancer};
use super::{ClusterManager, NodeId, CLUSTER_SLOTS};

/// Auto-reshard configuration
#[derive(Debug, Clone)]
pub struct AutoReshardConfig {
    /// Enable auto-resharding
    pub enabled: bool,
    /// Minimum time between rebalance checks
    pub check_interval: Duration,
    /// Cooldown after a rebalance before starting another
    pub cooldown: Duration,
    /// Maximum slot imbalance ratio before triggering rebalance (e.g., 1.2 = 20% imbalance)
    pub imbalance_threshold: f64,
    /// Maximum number of slots to migrate concurrently
    pub max_concurrent_migrations: usize,
    /// Maximum throughput degradation target (0.0-1.0) during migration
    pub max_throughput_degradation: f64,
    /// Rate limit: max keys migrated per second (0 = unlimited)
    pub rate_limit_keys_per_sec: u64,
    /// Whether to rebalance on node join
    pub rebalance_on_join: bool,
    /// Whether to rebalance on node leave
    pub rebalance_on_leave: bool,
    /// Minimum number of primary nodes before auto-resharding activates
    pub min_primaries: usize,
}

impl Default for AutoReshardConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            check_interval: Duration::from_secs(10),
            cooldown: Duration::from_secs(300),
            imbalance_threshold: 1.2,
            max_concurrent_migrations: 8,
            max_throughput_degradation: 0.05,
            rate_limit_keys_per_sec: 10_000,
            rebalance_on_join: true,
            rebalance_on_leave: true,
            min_primaries: 3,
        }
    }
}

/// Current state of the auto-resharding engine
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoReshardState {
    /// Idle, no rebalancing needed
    Idle,
    /// Analyzing cluster for imbalance
    Analyzing,
    /// Executing migration plan
    Migrating,
    /// In cooldown period after rebalance
    Cooldown,
    /// Disabled by configuration or operator
    Disabled,
}

/// Statistics for auto-resharding operations
#[derive(Debug, Clone)]
pub struct AutoReshardStats {
    /// Current engine state
    pub state: AutoReshardState,
    /// Total rebalances completed
    pub total_rebalances: u64,
    /// Total slots migrated across all rebalances
    pub total_slots_migrated: u64,
    /// Total keys migrated across all rebalances
    pub total_keys_migrated: u64,
    /// Last rebalance timestamp
    pub last_rebalance: Option<Instant>,
    /// Current imbalance ratio
    pub current_imbalance_ratio: f64,
    /// Active migrations count
    pub active_migrations: usize,
    /// Pending migrations in current plan
    pub pending_migrations: usize,
}

/// A single planned migration step
#[derive(Debug, Clone)]
pub struct PlannedMigration {
    /// Slot number to migrate.
    pub slot: u16,
    /// Node currently owning the slot.
    pub source_node: NodeId,
    /// Node that will receive the slot.
    pub target_node: NodeId,
    /// Estimated number of keys in this slot.
    pub estimated_keys: usize,
    /// Current state of this migration step.
    pub state: MigrationState,
}

/// Rebalance plan describing a full set of migrations to achieve balance
#[derive(Debug, Clone)]
pub struct RebalancePlan {
    /// When the plan was created.
    pub created_at: Instant,
    /// Ordered list of slot migrations.
    pub migrations: Vec<PlannedMigration>,
    /// Why the rebalance was triggered.
    pub reason: RebalanceReason,
    /// Estimated total keys across all migrations.
    pub estimated_total_keys: usize,
}

/// Why a rebalance was triggered
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebalanceReason {
    /// Node joined the cluster
    NodeJoined(NodeId),
    /// Node left the cluster
    NodeLeft(NodeId),
    /// Imbalance threshold exceeded
    ImbalanceDetected,
    /// Manually triggered by operator
    ManualTrigger,
}

impl std::fmt::Display for RebalanceReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NodeJoined(id) => write!(f, "node joined: {}", &id[..8.min(id.len())]),
            Self::NodeLeft(id) => write!(f, "node left: {}", &id[..8.min(id.len())]),
            Self::ImbalanceDetected => write!(f, "imbalance detected"),
            Self::ManualTrigger => write!(f, "manual trigger"),
        }
    }
}

/// Auto-resharding engine that monitors cluster topology and triggers rebalancing
pub struct AutoReshardEngine {
    config: AutoReshardConfig,
    cluster: Arc<ClusterManager>,
    state: RwLock<AutoReshardState>,
    stats: RwLock<AutoReshardStats>,
    current_plan: RwLock<Option<RebalancePlan>>,
    running: AtomicBool,
    total_rebalances: AtomicU64,
    total_slots_migrated: AtomicU64,
    total_keys_migrated: AtomicU64,
    last_known_primaries: RwLock<Vec<NodeId>>,
    shutdown_tx: broadcast::Sender<()>,
}

impl AutoReshardEngine {
    /// Create a new auto-resharding engine
    pub fn new(config: AutoReshardConfig, cluster: Arc<ClusterManager>) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        let initial_state = if config.enabled {
            AutoReshardState::Idle
        } else {
            AutoReshardState::Disabled
        };

        Self {
            config,
            cluster,
            state: RwLock::new(initial_state),
            stats: RwLock::new(AutoReshardStats {
                state: initial_state,
                total_rebalances: 0,
                total_slots_migrated: 0,
                total_keys_migrated: 0,
                last_rebalance: None,
                current_imbalance_ratio: 1.0,
                active_migrations: 0,
                pending_migrations: 0,
            }),
            current_plan: RwLock::new(None),
            running: AtomicBool::new(false),
            total_rebalances: AtomicU64::new(0),
            total_slots_migrated: AtomicU64::new(0),
            total_keys_migrated: AtomicU64::new(0),
            last_known_primaries: RwLock::new(Vec::new()),
            shutdown_tx,
        }
    }

    /// Get current engine state
    pub fn state(&self) -> AutoReshardState {
        *self.state.read()
    }

    /// Get current statistics
    pub fn stats(&self) -> AutoReshardStats {
        self.stats.read().clone()
    }

    /// Get the current rebalance plan, if any
    pub fn current_plan(&self) -> Option<RebalancePlan> {
        self.current_plan.read().clone()
    }

    /// Manually trigger a rebalance
    pub fn trigger_rebalance(&self) -> Result<RebalancePlan, AutoReshardError> {
        let current_state = *self.state.read();
        if current_state == AutoReshardState::Migrating {
            return Err(AutoReshardError::AlreadyRebalancing);
        }

        self.create_rebalance_plan(RebalanceReason::ManualTrigger)
    }

    /// Notify that a node has joined the cluster
    pub fn notify_node_joined(&self, node_id: &NodeId) {
        if !self.config.rebalance_on_join {
            return;
        }
        info!(
            "Auto-reshard: node joined {}, evaluating rebalance",
            &node_id[..8.min(node_id.len())]
        );
        self.update_known_primaries();
    }

    /// Notify that a node has left the cluster
    pub fn notify_node_left(&self, node_id: &NodeId) {
        if !self.config.rebalance_on_leave {
            return;
        }
        info!(
            "Auto-reshard: node left {}, evaluating rebalance",
            &node_id[..8.min(node_id.len())]
        );
        self.update_known_primaries();
    }

    /// Calculate the current slot imbalance ratio
    pub fn calculate_imbalance_ratio(&self) -> f64 {
        let assignments = self.get_current_assignments();
        if assignments.is_empty() {
            return 1.0;
        }

        let counts: Vec<usize> = assignments.values().map(|v| v.len()).collect();
        let max = *counts.iter().max().unwrap_or(&0) as f64;
        let min = *counts.iter().min().unwrap_or(&0) as f64;
        let avg = counts.iter().sum::<usize>() as f64 / counts.len() as f64;

        if avg == 0.0 {
            return f64::MAX;
        }

        // Ratio of max to average — 1.0 means perfectly balanced
        let ratio = if min == 0.0 && max > 0.0 {
            f64::MAX
        } else {
            max / avg
        };

        // Update stats
        self.stats.write().current_imbalance_ratio = ratio;
        ratio
    }

    /// Check if a rebalance is needed
    pub fn needs_rebalance(&self) -> bool {
        let primaries = self.cluster.get_primaries();
        if primaries.len() < self.config.min_primaries {
            return false;
        }

        let ratio = self.calculate_imbalance_ratio();
        ratio > self.config.imbalance_threshold || self.has_unassigned_slots()
    }

    /// Create a rebalance plan
    pub fn create_rebalance_plan(
        &self,
        reason: RebalanceReason,
    ) -> Result<RebalancePlan, AutoReshardError> {
        *self.state.write() = AutoReshardState::Analyzing;

        let primaries = self.cluster.get_primaries();
        if primaries.is_empty() {
            *self.state.write() = AutoReshardState::Idle;
            return Err(AutoReshardError::NoPrimaries);
        }

        let node_ids: Vec<NodeId> = primaries.iter().map(|n| n.id.clone()).collect();
        let assignments = self.get_current_assignments();

        let mut rebalancer = SlotRebalancer::new();
        let migration_plan = rebalancer.calculate_rebalance_plan(&node_ids, &assignments);

        if migration_plan.is_empty() {
            *self.state.write() = AutoReshardState::Idle;
            return Err(AutoReshardError::AlreadyBalanced);
        }

        let planned_migrations: Vec<PlannedMigration> = migration_plan
            .into_iter()
            .map(|(slot, source, target)| PlannedMigration {
                slot,
                source_node: source,
                target_node: target,
                estimated_keys: 0, // Will be populated during execution
                state: MigrationState::Pending,
            })
            .collect();

        let plan = RebalancePlan {
            created_at: Instant::now(),
            migrations: planned_migrations,
            reason,
            estimated_total_keys: 0,
        };

        info!(
            "Auto-reshard: created plan with {} slot migrations (reason: {})",
            plan.migrations.len(),
            plan.reason
        );

        *self.current_plan.write() = Some(plan.clone());
        *self.state.write() = AutoReshardState::Idle;

        Ok(plan)
    }

    /// Execute the current rebalance plan
    pub async fn execute_plan(&self) -> Result<RebalanceSummary, AutoReshardError> {
        let plan = self
            .current_plan
            .read()
            .clone()
            .ok_or(AutoReshardError::NoPlan)?;

        let current_state = *self.state.read();
        if current_state == AutoReshardState::Migrating {
            return Err(AutoReshardError::AlreadyRebalancing);
        }

        *self.state.write() = AutoReshardState::Migrating;
        self.running.store(true, Ordering::SeqCst);

        let start_time = Instant::now();
        let mut slots_migrated: u64 = 0;
        let mut slots_failed: u64 = 0;
        let total = plan.migrations.len() as u64;

        // Process migrations in batches
        let batch_size = self.config.max_concurrent_migrations;
        for chunk in plan.migrations.chunks(batch_size) {
            if !self.running.load(Ordering::SeqCst) {
                warn!("Auto-reshard: execution cancelled");
                break;
            }

            for migration in chunk {
                // Set the slot to migrating state on the source
                if let Err(e) = self
                    .cluster
                    .set_slot_migrating(migration.slot, migration.target_node.clone())
                {
                    warn!(
                        "Auto-reshard: failed to set slot {} migrating: {}",
                        migration.slot, e
                    );
                    slots_failed += 1;
                    continue;
                }

                // Set the slot to importing state on the target
                if let Err(e) = self
                    .cluster
                    .set_slot_importing(migration.slot, migration.source_node.clone())
                {
                    warn!(
                        "Auto-reshard: failed to set slot {} importing: {}",
                        migration.slot, e
                    );
                    self.cluster.set_slot_stable(migration.slot);
                    slots_failed += 1;
                    continue;
                }

                // Reassign slot ownership
                if let Err(e) = self
                    .cluster
                    .set_slot_node(migration.slot, migration.target_node.clone())
                {
                    warn!(
                        "Auto-reshard: failed to reassign slot {}: {}",
                        migration.slot, e
                    );
                    slots_failed += 1;
                    continue;
                }

                // Mark slot as stable after ownership transfer
                self.cluster.set_slot_stable(migration.slot);
                slots_migrated += 1;

                debug!(
                    "Auto-reshard: migrated slot {} from {} to {}",
                    migration.slot,
                    &migration.source_node[..8.min(migration.source_node.len())],
                    &migration.target_node[..8.min(migration.target_node.len())]
                );
            }

            // Update progress stats
            {
                let mut stats = self.stats.write();
                stats.active_migrations = 0;
                stats.pending_migrations = (total - slots_migrated - slots_failed) as usize;
            }

            // Rate limiting between batches
            if self.config.rate_limit_keys_per_sec > 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        // Increment epoch to propagate new topology
        self.cluster.increment_epoch();

        // Update engine state
        self.total_rebalances.fetch_add(1, Ordering::Relaxed);
        self.total_slots_migrated
            .fetch_add(slots_migrated, Ordering::Relaxed);

        let summary = RebalanceSummary {
            duration: start_time.elapsed(),
            slots_migrated,
            slots_failed,
            total_planned: total,
            reason: plan.reason.clone(),
        };

        info!(
            "Auto-reshard: completed rebalance - {}/{} slots migrated ({} failed) in {:?}",
            slots_migrated, total, slots_failed, summary.duration
        );

        // Enter cooldown
        *self.state.write() = AutoReshardState::Cooldown;
        {
            let mut stats = self.stats.write();
            stats.state = AutoReshardState::Cooldown;
            stats.last_rebalance = Some(Instant::now());
            stats.total_rebalances = self.total_rebalances.load(Ordering::Relaxed);
            stats.total_slots_migrated = self.total_slots_migrated.load(Ordering::Relaxed);
            stats.active_migrations = 0;
            stats.pending_migrations = 0;
        }
        *self.current_plan.write() = None;
        self.running.store(false, Ordering::SeqCst);

        Ok(summary)
    }

    /// Cancel an in-progress rebalance
    pub fn cancel(&self) {
        self.running.store(false, Ordering::SeqCst);
        warn!("Auto-reshard: cancellation requested");
    }

    /// Stop the engine
    pub fn shutdown(&self) {
        self.cancel();
        let _ = self.shutdown_tx.send(());
    }

    // ---- Internal helpers ----

    fn get_current_assignments(&self) -> HashMap<NodeId, Vec<u16>> {
        let mut assignments: HashMap<NodeId, Vec<u16>> = HashMap::new();
        let primaries = self.cluster.get_primaries();

        for primary in &primaries {
            assignments.insert(primary.id.clone(), Vec::new());
        }

        for slot in 0..CLUSTER_SLOTS {
            if let Some(owner) = self.cluster.get_slot_owner(slot) {
                assignments.entry(owner).or_default().push(slot);
            }
        }

        assignments
    }

    fn has_unassigned_slots(&self) -> bool {
        for slot in 0..CLUSTER_SLOTS {
            if self.cluster.get_slot_owner(slot).is_none() {
                return true;
            }
        }
        false
    }

    fn update_known_primaries(&self) {
        let primaries: Vec<NodeId> = self
            .cluster
            .get_primaries()
            .iter()
            .map(|n| n.id.clone())
            .collect();
        *self.last_known_primaries.write() = primaries;
    }
}

/// Summary of a completed rebalance operation
#[derive(Debug, Clone)]
pub struct RebalanceSummary {
    /// How long the rebalance took
    pub duration: Duration,
    /// Number of slots successfully migrated
    pub slots_migrated: u64,
    /// Number of slots that failed to migrate
    pub slots_failed: u64,
    /// Total number of migrations planned
    pub total_planned: u64,
    /// Reason for the rebalance
    pub reason: RebalanceReason,
}

/// Errors from the auto-resharding engine
#[derive(Debug, thiserror::Error)]
pub enum AutoReshardError {
    /// Auto-resharding is disabled by configuration.
    #[error("auto-resharding is disabled")]
    Disabled,
    /// A rebalance is already in progress.
    #[error("rebalance already in progress")]
    AlreadyRebalancing,
    /// The cluster is already balanced; no action needed.
    #[error("cluster is already balanced")]
    AlreadyBalanced,
    /// There are no primary nodes available for rebalancing.
    #[error("no primary nodes available")]
    NoPrimaries,
    /// There is no rebalance plan to execute.
    #[error("no rebalance plan to execute")]
    NoPlan,
    /// An error from the cluster state layer.
    #[error("cluster state error: {0}")]
    ClusterError(String),
    /// An error during slot migration.
    #[error("migration error: {0}")]
    MigrationError(String),
}

/// Start the auto-reshard monitoring loop
pub async fn start_auto_reshard_monitor(
    engine: Arc<AutoReshardEngine>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let interval = engine.config.check_interval;
    info!(
        "Auto-reshard monitor started (check interval: {:?})",
        interval
    );

    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {
                let state = engine.state();
                match state {
                    AutoReshardState::Disabled => continue,
                    AutoReshardState::Migrating => continue,
                    AutoReshardState::Cooldown => {
                        if let Some(last) = engine.stats().last_rebalance {
                            if last.elapsed() < engine.config.cooldown {
                                continue;
                            }
                        }
                        // Cooldown expired, go back to idle
                        *engine.state.write() = AutoReshardState::Idle;
                    }
                    AutoReshardState::Idle | AutoReshardState::Analyzing => {
                        if engine.needs_rebalance() {
                            info!("Auto-reshard: imbalance detected, creating plan");
                            match engine.create_rebalance_plan(RebalanceReason::ImbalanceDetected) {
                                Ok(_plan) => {
                                    if let Err(e) = engine.execute_plan().await {
                                        error!("Auto-reshard: execution failed: {}", e);
                                    }
                                }
                                Err(AutoReshardError::AlreadyBalanced) => {
                                    debug!("Auto-reshard: cluster is balanced");
                                }
                                Err(e) => {
                                    warn!("Auto-reshard: planning failed: {}", e);
                                }
                            }
                        }
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                info!("Auto-reshard monitor shutting down");
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::ClusterNode;
    use super::*;
    use std::net::SocketAddr;

    fn make_cluster() -> Arc<ClusterManager> {
        let config = super::super::ClusterConfig::default();
        let addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        Arc::new(ClusterManager::new(config, "node0".to_string(), addr))
    }

    fn add_primary(cluster: &ClusterManager, id: &str, port: u16) {
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        let node = ClusterNode::new(id.to_string(), addr);
        cluster.add_node(node);
    }

    #[test]
    fn test_default_config() {
        let config = AutoReshardConfig::default();
        assert!(config.enabled);
        assert_eq!(config.imbalance_threshold, 1.2);
        assert_eq!(config.max_concurrent_migrations, 8);
        assert_eq!(config.min_primaries, 3);
    }

    #[test]
    fn test_imbalance_ratio_empty() {
        let cluster = make_cluster();
        let engine = AutoReshardEngine::new(AutoReshardConfig::default(), cluster);
        let ratio = engine.calculate_imbalance_ratio();
        // Only self node, 1 primary with 0 slots — still ratio 1.0 (no slots assigned)
        assert!(ratio >= 1.0);
    }

    #[test]
    fn test_imbalance_detection_balanced() {
        let cluster = make_cluster();
        add_primary(&cluster, "node1", 7001);
        add_primary(&cluster, "node2", 7002);

        // Distribute slots evenly
        cluster.distribute_slots();

        let engine = AutoReshardEngine::new(AutoReshardConfig::default(), cluster);
        let ratio = engine.calculate_imbalance_ratio();
        // Should be close to 1.0 for 3 nodes (16384/3 = 5461 each, +/- 1)
        assert!(ratio < 1.1, "ratio={} should be < 1.1", ratio);
    }

    #[test]
    fn test_needs_rebalance_false_when_balanced() {
        let cluster = make_cluster();
        add_primary(&cluster, "node1", 7001);
        add_primary(&cluster, "node2", 7002);
        cluster.distribute_slots();

        let engine = AutoReshardEngine::new(AutoReshardConfig::default(), cluster);
        assert!(!engine.needs_rebalance());
    }

    #[test]
    fn test_create_plan_balanced_returns_error() {
        let cluster = make_cluster();
        add_primary(&cluster, "node1", 7001);
        add_primary(&cluster, "node2", 7002);
        cluster.distribute_slots();

        let engine = AutoReshardEngine::new(AutoReshardConfig::default(), cluster);
        match engine.create_rebalance_plan(RebalanceReason::ManualTrigger) {
            Err(AutoReshardError::AlreadyBalanced) => {} // expected
            other => panic!(
                "expected AlreadyBalanced, got {:?}",
                other.map(|p| p.migrations.len())
            ),
        }
    }

    #[test]
    fn test_create_plan_with_new_empty_node() {
        let cluster = make_cluster();
        add_primary(&cluster, "node1", 7001);
        add_primary(&cluster, "node2", 7002);
        cluster.distribute_slots();

        // Add a new node with no slots
        add_primary(&cluster, "node3", 7003);

        let engine = AutoReshardEngine::new(AutoReshardConfig::default(), cluster);
        let plan = engine
            .create_rebalance_plan(RebalanceReason::NodeJoined("node3".to_string()))
            .expect("should create plan");

        assert!(
            !plan.migrations.is_empty(),
            "should have migrations for the new node"
        );

        // All target nodes should be the new node (it needs slots)
        let targets_to_new: usize = plan
            .migrations
            .iter()
            .filter(|m| m.target_node == "node3")
            .count();
        assert!(targets_to_new > 0);
    }

    #[test]
    fn test_cancel() {
        let cluster = make_cluster();
        let engine = AutoReshardEngine::new(AutoReshardConfig::default(), cluster);
        engine.cancel();
        assert!(!engine.running.load(Ordering::SeqCst));
    }

    #[test]
    fn test_state_disabled() {
        let cluster = make_cluster();
        let mut config = AutoReshardConfig::default();
        config.enabled = false;
        let engine = AutoReshardEngine::new(config, cluster);
        assert_eq!(engine.state(), AutoReshardState::Disabled);
    }

    #[test]
    fn test_rebalance_reason_display() {
        assert_eq!(
            format!("{}", RebalanceReason::ImbalanceDetected),
            "imbalance detected"
        );
        assert!(
            format!("{}", RebalanceReason::NodeJoined("abc12345".to_string())).contains("abc12345")
        );
        assert_eq!(
            format!("{}", RebalanceReason::ManualTrigger),
            "manual trigger"
        );
    }

    #[tokio::test]
    async fn test_execute_plan_no_plan() {
        let cluster = make_cluster();
        let engine = AutoReshardEngine::new(AutoReshardConfig::default(), cluster);
        let result = engine.execute_plan().await;
        assert!(matches!(result, Err(AutoReshardError::NoPlan)));
    }

    #[tokio::test]
    async fn test_execute_plan_with_migrations() {
        let cluster = make_cluster();
        add_primary(&cluster, "node1", 7001);
        add_primary(&cluster, "node2", 7002);
        cluster.distribute_slots();

        // Add new empty node
        add_primary(&cluster, "node3", 7003);

        let engine = AutoReshardEngine::new(AutoReshardConfig::default(), Arc::clone(&cluster));

        // Create and execute plan
        let _plan = engine
            .create_rebalance_plan(RebalanceReason::NodeJoined("node3".to_string()))
            .expect("plan created");

        let summary = engine.execute_plan().await.expect("execution succeeds");
        assert!(summary.slots_migrated > 0);
        assert_eq!(
            summary.slots_migrated + summary.slots_failed,
            summary.total_planned
        );
    }
}
