//! Multi-region active-active replication with conflict resolution.
//!
//! Provides vector clocks for causal ordering, conflict detection/resolution,
//! region management, and a cross-region replication engine.
//!
//! The [`ActiveActiveManager`] provides a high-level API that coordinates
//! region topology, replication, and health monitoring.

pub mod conflict;
pub mod region;
pub mod replicator;
pub mod vector_clock;

pub use conflict::{ConflictResolution, ConflictResolver, ConflictStrategy, ConflictWinner};
pub use region::{Region, RegionStatus};
pub use replicator::{
    ActiveActiveReplicator, ConsistencyLevel, ReplicationConfig, ReplicationStats,
};
pub use vector_clock::{ClockOrdering, ConflictRecord, ConflictResolutionKind, VectorClock};

use std::sync::Arc;

// ── Active-Active Manager ────────────────────────────────────────────────

/// High-level manager for active-active replication.
///
/// Wraps [`ActiveActiveReplicator`] with topology awareness, health
/// monitoring, and region lifecycle management.
pub struct ActiveActiveManager {
    replicator: Arc<ActiveActiveReplicator>,
    /// Regions that this local node should replicate *to* (downstream peers).
    topology: dashmap::DashMap<String, Vec<String>>,
}

impl ActiveActiveManager {
    /// Create a new manager for the given local region.
    pub fn new(local_region: String, config: ReplicationConfig) -> Self {
        Self {
            replicator: Arc::new(ActiveActiveReplicator::new(local_region, config)),
            topology: dashmap::DashMap::new(),
        }
    }

    /// Create with default configuration.
    pub fn with_defaults(local_region: String) -> Self {
        Self::new(local_region, ReplicationConfig::default())
    }

    /// Access the underlying replicator.
    pub fn replicator(&self) -> &ActiveActiveReplicator {
        &self.replicator
    }

    // ── Topology ─────────────────────────────────────────────────────

    /// Register a peer region and configure replication links.
    ///
    /// `replicate_to` lists the region ids that `region_id` should push
    /// updates to.
    pub fn add_peer(
        &self,
        region_id: String,
        name: String,
        endpoint: String,
        replicate_to: Vec<String>,
    ) -> Result<(), String> {
        self.replicator
            .add_region(region_id.clone(), name, endpoint)?;
        self.topology.insert(region_id, replicate_to);
        Ok(())
    }

    /// Remove a peer region and its topology links.
    pub fn remove_peer(&self, region_id: &str) -> Result<(), String> {
        self.replicator.remove_region(region_id)?;
        self.topology.remove(region_id);
        // Also remove this peer from other regions' replication targets.
        for mut entry in self.topology.iter_mut() {
            entry.value_mut().retain(|id| id != region_id);
        }
        Ok(())
    }

    /// Return the replication targets for a given region.
    pub fn replication_targets(&self, region_id: &str) -> Vec<String> {
        self.topology
            .get(region_id)
            .map(|r| r.value().clone())
            .unwrap_or_default()
    }

    // ── Replication ──────────────────────────────────────────────────

    /// Replicate a write to all configured downstream peers.
    ///
    /// Returns the list of region ids that were successfully updated.
    pub fn replicate_write(&self, key: &str, value: &[u8]) -> Vec<String> {
        self.replicator
            .record_write(key, value, self.replicator.local_region());

        let local = self.replicator.local_region().to_string();
        let targets = self.replication_targets(&local);

        let clock = VectorClock::new(); // in production, fetch current clock for key
        let mut succeeded = Vec::new();
        for target in &targets {
            if self
                .replicator
                .replicate_to(target, key, value, &clock)
                .is_ok()
            {
                succeeded.push(target.clone());
            }
        }
        succeeded
    }

    // ── Health ────────────────────────────────────────────────────────

    /// Identify regions whose replication lag exceeds `threshold_ms`.
    pub fn lagging_regions(&self, threshold_ms: u64) -> Vec<Region> {
        self.replicator
            .list_regions()
            .into_iter()
            .filter(|r| r.replication_lag_ms > threshold_ms)
            .collect()
    }

    /// Identify offline regions.
    pub fn offline_regions(&self) -> Vec<Region> {
        self.replicator
            .list_regions()
            .into_iter()
            .filter(|r| r.status == RegionStatus::Offline)
            .collect()
    }

    /// Return a summary suitable for a CLUSTER INFO-style response.
    pub fn cluster_summary(&self) -> ClusterSummary {
        let regions = self.replicator.list_regions();
        let stats = self.replicator.stats();
        ClusterSummary {
            local_region: self.replicator.local_region().to_string(),
            total_regions: regions.len(),
            active_regions: regions
                .iter()
                .filter(|r| r.status == RegionStatus::Active)
                .count(),
            ops_replicated: stats.ops_replicated,
            conflicts_detected: stats.conflicts_detected,
            conflicts_resolved: stats.conflicts_resolved,
            avg_lag_ms: stats.avg_lag_ms,
        }
    }
}

/// High-level cluster summary for monitoring and diagnostics.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ClusterSummary {
    pub local_region: String,
    pub total_regions: usize,
    pub active_regions: usize,
    pub ops_replicated: u64,
    pub conflicts_detected: u64,
    pub conflicts_resolved: u64,
    pub avg_lag_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manager_add_and_remove_peer() {
        let mgr = ActiveActiveManager::with_defaults("us-east".into());
        mgr.add_peer(
            "eu-west".into(),
            "EU West".into(),
            "10.0.2.1:6379".into(),
            vec!["us-east".into()],
        )
        .unwrap();
        assert_eq!(mgr.replicator().list_regions().len(), 1);
        assert_eq!(mgr.replication_targets("eu-west"), vec!["us-east"]);

        mgr.remove_peer("eu-west").unwrap();
        assert!(mgr.replicator().list_regions().is_empty());
    }

    #[test]
    fn manager_replicate_write() {
        let mgr = ActiveActiveManager::with_defaults("us-east".into());
        mgr.add_peer(
            "eu-west".into(),
            "EU West".into(),
            "10.0.2.1:6379".into(),
            vec![],
        )
        .unwrap();
        // Set topology: us-east replicates to eu-west
        mgr.topology
            .insert("us-east".into(), vec!["eu-west".into()]);

        let succeeded = mgr.replicate_write("key1", b"hello");
        assert_eq!(succeeded, vec!["eu-west"]);
    }

    #[test]
    fn manager_cluster_summary() {
        let mgr = ActiveActiveManager::with_defaults("us-east".into());
        let summary = mgr.cluster_summary();
        assert_eq!(summary.local_region, "us-east");
        assert_eq!(summary.total_regions, 0);
    }

    #[test]
    fn manager_lagging_regions_empty() {
        let mgr = ActiveActiveManager::with_defaults("us-east".into());
        assert!(mgr.lagging_regions(100).is_empty());
        assert!(mgr.offline_regions().is_empty());
    }
}
