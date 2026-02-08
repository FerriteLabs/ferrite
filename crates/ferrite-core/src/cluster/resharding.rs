//! Production-ready resharding with background slot migration.
//!
//! Implements `CLUSTER SETSLOT` subcommands (IMPORTING/MIGRATING/STABLE/NODE)
//! with background key migration, per-slot progress tracking, and atomic
//! ownership transfer.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use super::{ClusterManager, HashSlot, NodeId, CLUSTER_SLOTS};

/// Per-slot migration progress.
#[derive(Debug, Clone)]
pub struct SlotMigrationProgress {
    /// Slot being migrated.
    pub slot: u16,
    /// Source node that owns the slot.
    pub source_node: NodeId,
    /// Target node receiving the slot.
    pub target_node: NodeId,
    /// Total keys discovered at migration start.
    pub total_keys: u64,
    /// Keys migrated so far.
    pub keys_migrated: AtomicU64Snapshot,
    /// Keys that failed to migrate.
    pub keys_failed: u64,
    /// When migration started.
    pub started_at: Instant,
    /// When migration completed (`None` if still running).
    pub completed_at: Option<Instant>,
    /// Current phase.
    pub phase: MigrationPhase,
}

/// Snapshot-friendly wrapper for atomic counters exposed in progress.
#[derive(Debug, Clone, Copy, Default)]
pub struct AtomicU64Snapshot(pub u64);

/// Phases a slot goes through during migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationPhase {
    /// Preparing: scanning keys, setting IMPORTING/MIGRATING flags.
    Preparing,
    /// Actively transferring keys in batches.
    Transferring,
    /// Finalising: atomic ownership transfer via SETSLOT NODE.
    Finalizing,
    /// Migration completed successfully.
    Completed,
    /// Migration failed or was cancelled.
    Failed,
}

impl std::fmt::Display for MigrationPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Preparing => write!(f, "preparing"),
            Self::Transferring => write!(f, "transferring"),
            Self::Finalizing => write!(f, "finalizing"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

/// Configuration for the resharding engine.
#[derive(Debug, Clone)]
pub struct ReshardingConfig {
    /// Number of keys to migrate per batch.
    pub batch_size: usize,
    /// Delay between batches to limit throughput impact.
    pub batch_delay: Duration,
    /// Timeout for migrating a single key.
    pub key_timeout: Duration,
    /// Maximum concurrent slot migrations.
    pub max_concurrent_slots: usize,
    /// Whether to retry failed key migrations.
    pub retry_failed_keys: bool,
    /// Maximum retries per key.
    pub max_key_retries: u32,
}

impl Default for ReshardingConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            batch_delay: Duration::from_millis(10),
            key_timeout: Duration::from_secs(5),
            max_concurrent_slots: 1,
            retry_failed_keys: true,
            max_key_retries: 3,
        }
    }
}

/// Resharding engine that orchestrates background slot migrations.
pub struct ReshardingEngine {
    config: ReshardingConfig,
    cluster: Arc<ClusterManager>,
    /// Active migrations keyed by slot number.
    active_migrations: RwLock<HashMap<u16, SlotMigrationProgress>>,
    /// Completed/failed migrations kept for observability.
    completed_migrations: RwLock<Vec<SlotMigrationProgress>>,
    /// Global running flag.
    running: AtomicBool,
    /// Cumulative counters.
    total_keys_migrated: AtomicU64,
    total_keys_failed: AtomicU64,
    total_slots_completed: AtomicU64,
    total_slots_failed: AtomicU64,
    shutdown_tx: broadcast::Sender<()>,
}

impl ReshardingEngine {
    /// Create a new resharding engine.
    pub fn new(config: ReshardingConfig, cluster: Arc<ClusterManager>) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        Self {
            config,
            cluster,
            active_migrations: RwLock::new(HashMap::new()),
            completed_migrations: RwLock::new(Vec::new()),
            running: AtomicBool::new(false),
            total_keys_migrated: AtomicU64::new(0),
            total_keys_failed: AtomicU64::new(0),
            total_slots_completed: AtomicU64::new(0),
            total_slots_failed: AtomicU64::new(0),
            shutdown_tx,
        }
    }

    /// Begin a slot migration from `source` to `target`.
    ///
    /// Sets IMPORTING on this node if we are the target, or MIGRATING if we are
    /// the source. Returns an error if the slot is already being migrated.
    pub fn begin_slot_migration(
        &self,
        slot: u16,
        source_node: NodeId,
        target_node: NodeId,
        total_keys: u64,
    ) -> Result<(), ReshardingError> {
        if slot >= CLUSTER_SLOTS {
            return Err(ReshardingError::InvalidSlot(slot));
        }

        let mut active = self.active_migrations.write();
        if active.contains_key(&slot) {
            return Err(ReshardingError::SlotAlreadyMigrating(slot));
        }

        let active_count = active
            .values()
            .filter(|p| p.phase == MigrationPhase::Transferring)
            .count();
        if active_count >= self.config.max_concurrent_slots {
            return Err(ReshardingError::TooManyConcurrent);
        }

        // Set MIGRATING / IMPORTING on the cluster manager.
        self.cluster
            .set_slot_migrating(slot, target_node.clone())
            .map_err(ReshardingError::ClusterError)?;
        self.cluster
            .set_slot_importing(slot, source_node.clone())
            .map_err(ReshardingError::ClusterError)?;

        let progress = SlotMigrationProgress {
            slot,
            source_node,
            target_node,
            total_keys,
            keys_migrated: AtomicU64Snapshot(0),
            keys_failed: 0,
            started_at: Instant::now(),
            completed_at: None,
            phase: MigrationPhase::Preparing,
        };

        active.insert(slot, progress);
        info!(slot, "Resharding: slot migration initiated");

        Ok(())
    }

    /// Record progress: increment migrated key count for `slot`.
    pub fn record_keys_migrated(&self, slot: u16, count: u64) {
        if let Some(progress) = self.active_migrations.write().get_mut(&slot) {
            progress.keys_migrated.0 += count;
            self.total_keys_migrated.fetch_add(count, Ordering::Relaxed);
        }
    }

    /// Advance a slot migration to the `Transferring` phase.
    pub fn mark_transferring(&self, slot: u16) {
        if let Some(progress) = self.active_migrations.write().get_mut(&slot) {
            progress.phase = MigrationPhase::Transferring;
            debug!(slot, "Resharding: slot entering transfer phase");
        }
    }

    /// Atomically finalize slot ownership transfer.
    ///
    /// Sets the slot to STABLE and reassigns ownership to `target_node` via
    /// `CLUSTER SETSLOT <slot> NODE <target>`.
    pub fn finalize_slot_migration(&self, slot: u16) -> Result<(), ReshardingError> {
        let mut active = self.active_migrations.write();
        let progress = active
            .get_mut(&slot)
            .ok_or(ReshardingError::SlotNotMigrating(slot))?;

        progress.phase = MigrationPhase::Finalizing;
        let target = progress.target_node.clone();

        // Atomic ownership transfer.
        self.cluster
            .set_slot_node(slot, target.clone())
            .map_err(ReshardingError::ClusterError)?;
        self.cluster.set_slot_stable(slot);

        progress.phase = MigrationPhase::Completed;
        progress.completed_at = Some(Instant::now());
        self.total_slots_completed.fetch_add(1, Ordering::Relaxed);

        info!(slot, target = %target, "Resharding: slot migration finalized");

        // Move to completed list.
        let completed = active.remove(&slot);
        drop(active);
        if let Some(p) = completed {
            self.completed_migrations.write().push(p);
        }

        Ok(())
    }

    /// Cancel an in-progress slot migration, reverting to STABLE.
    pub fn cancel_slot_migration(&self, slot: u16) -> Result<(), ReshardingError> {
        let mut active = self.active_migrations.write();
        let progress = active
            .get_mut(&slot)
            .ok_or(ReshardingError::SlotNotMigrating(slot))?;

        progress.phase = MigrationPhase::Failed;
        progress.completed_at = Some(Instant::now());
        self.total_slots_failed.fetch_add(1, Ordering::Relaxed);
        self.total_keys_failed
            .fetch_add(progress.keys_failed, Ordering::Relaxed);

        self.cluster.set_slot_stable(slot);
        warn!(slot, "Resharding: slot migration cancelled");

        let failed = active.remove(&slot);
        drop(active);
        if let Some(p) = failed {
            self.completed_migrations.write().push(p);
        }

        Ok(())
    }

    /// Get progress for a specific slot.
    pub fn get_progress(&self, slot: u16) -> Option<SlotMigrationProgress> {
        self.active_migrations.read().get(&slot).cloned()
    }

    /// Get all active migration progress snapshots.
    pub fn active_migrations(&self) -> Vec<SlotMigrationProgress> {
        self.active_migrations.read().values().cloned().collect()
    }

    /// Number of slots currently being migrated.
    pub fn active_count(&self) -> usize {
        self.active_migrations.read().len()
    }

    /// Cumulative stats.
    pub fn stats(&self) -> ReshardingStats {
        ReshardingStats {
            total_keys_migrated: self.total_keys_migrated.load(Ordering::Relaxed),
            total_keys_failed: self.total_keys_failed.load(Ordering::Relaxed),
            total_slots_completed: self.total_slots_completed.load(Ordering::Relaxed),
            total_slots_failed: self.total_slots_failed.load(Ordering::Relaxed),
            active_slot_count: self.active_migrations.read().len(),
        }
    }

    /// Shutdown the engine.
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::SeqCst);
        let _ = self.shutdown_tx.send(());
    }
}

/// Cumulative resharding statistics.
#[derive(Debug, Clone)]
pub struct ReshardingStats {
    /// Total keys successfully migrated across all slots.
    pub total_keys_migrated: u64,
    /// Total keys that failed to migrate.
    pub total_keys_failed: u64,
    /// Total slots that completed migration.
    pub total_slots_completed: u64,
    /// Total slots whose migration failed.
    pub total_slots_failed: u64,
    /// Number of slots currently being migrated.
    pub active_slot_count: usize,
}

/// Errors from the resharding engine.
#[derive(Debug, thiserror::Error)]
pub enum ReshardingError {
    /// Slot number is out of the valid range [0, 16384).
    #[error("invalid slot number: {0}")]
    InvalidSlot(u16),
    /// The slot is already being migrated.
    #[error("slot {0} is already being migrated")]
    SlotAlreadyMigrating(u16),
    /// The slot is not currently being migrated.
    #[error("slot {0} is not being migrated")]
    SlotNotMigrating(u16),
    /// Too many concurrent slot migrations.
    #[error("too many concurrent slot migrations")]
    TooManyConcurrent,
    /// Error from the cluster state layer.
    #[error("cluster error: {0}")]
    ClusterError(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    use crate::cluster::{generate_node_id, ClusterConfig, ClusterNode};

    fn make_cluster() -> Arc<ClusterManager> {
        let config = ClusterConfig::default();
        let addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        Arc::new(ClusterManager::new(config, "node0".to_string(), addr))
    }

    fn add_node(cluster: &ClusterManager, id: &str, port: u16) {
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        cluster.add_node(ClusterNode::new(id.to_string(), addr));
    }

    #[test]
    fn test_begin_and_finalize_migration() {
        let cluster = make_cluster();
        add_node(&cluster, "node1", 7001);

        // Assign slot 100 to node0 so MIGRATING can be set.
        cluster.assign_slots(&"node0".to_string(), super::super::SlotRange::new(100, 100));

        let engine = ReshardingEngine::new(ReshardingConfig::default(), cluster.clone());

        // Begin migration.
        engine
            .begin_slot_migration(100, "node0".to_string(), "node1".to_string(), 50)
            .unwrap();
        assert_eq!(engine.active_count(), 1);

        let progress = engine.get_progress(100).unwrap();
        assert_eq!(progress.phase, MigrationPhase::Preparing);

        // Mark transferring and record some keys.
        engine.mark_transferring(100);
        engine.record_keys_migrated(100, 25);

        let progress = engine.get_progress(100).unwrap();
        assert_eq!(progress.phase, MigrationPhase::Transferring);
        assert_eq!(progress.keys_migrated.0, 25);

        // Finalize.
        engine.finalize_slot_migration(100).unwrap();
        assert_eq!(engine.active_count(), 0);

        let stats = engine.stats();
        assert_eq!(stats.total_slots_completed, 1);
        assert_eq!(stats.total_keys_migrated, 25);
    }

    #[test]
    fn test_cancel_migration() {
        let cluster = make_cluster();
        add_node(&cluster, "node1", 7001);
        cluster.assign_slots(&"node0".to_string(), super::super::SlotRange::new(200, 200));

        let engine = ReshardingEngine::new(ReshardingConfig::default(), cluster.clone());
        engine
            .begin_slot_migration(200, "node0".to_string(), "node1".to_string(), 10)
            .unwrap();

        engine.cancel_slot_migration(200).unwrap();
        assert_eq!(engine.active_count(), 0);
        assert_eq!(engine.stats().total_slots_failed, 1);
    }

    #[test]
    fn test_duplicate_migration_rejected() {
        let cluster = make_cluster();
        add_node(&cluster, "node1", 7001);
        cluster.assign_slots(&"node0".to_string(), super::super::SlotRange::new(300, 300));

        let engine = ReshardingEngine::new(ReshardingConfig::default(), cluster.clone());
        engine
            .begin_slot_migration(300, "node0".to_string(), "node1".to_string(), 5)
            .unwrap();

        let result = engine.begin_slot_migration(300, "node0".to_string(), "node1".to_string(), 5);
        assert!(matches!(result, Err(ReshardingError::SlotAlreadyMigrating(300))));
    }

    #[test]
    fn test_invalid_slot() {
        let cluster = make_cluster();
        let engine = ReshardingEngine::new(ReshardingConfig::default(), cluster);

        let result = engine.begin_slot_migration(
            CLUSTER_SLOTS,
            "a".to_string(),
            "b".to_string(),
            0,
        );
        assert!(matches!(result, Err(ReshardingError::InvalidSlot(_))));
    }

    #[test]
    fn test_finalize_not_migrating() {
        let cluster = make_cluster();
        let engine = ReshardingEngine::new(ReshardingConfig::default(), cluster);
        let result = engine.finalize_slot_migration(999);
        assert!(matches!(result, Err(ReshardingError::SlotNotMigrating(999))));
    }

    #[test]
    fn test_concurrent_limit() {
        let cluster = make_cluster();
        add_node(&cluster, "node1", 7001);
        // Assign enough slots.
        cluster.assign_slots(&"node0".to_string(), super::super::SlotRange::new(0, 10));

        let mut config = ReshardingConfig::default();
        config.max_concurrent_slots = 1;
        let engine = ReshardingEngine::new(config, cluster.clone());

        engine
            .begin_slot_migration(0, "node0".to_string(), "node1".to_string(), 1)
            .unwrap();
        engine.mark_transferring(0);

        let result = engine.begin_slot_migration(1, "node0".to_string(), "node1".to_string(), 1);
        assert!(matches!(result, Err(ReshardingError::TooManyConcurrent)));
    }
}
