#![forbid(unsafe_code)]
//! Cross-region replication engine for active-active deployments.

use chrono::Utc;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tracing::info;

use super::conflict::{
    Conflict, ConflictResolution, ConflictResolver, ConflictStrategy, ConflictWinner,
};
use super::region::{Region, RegionStatus};
use super::vector_clock::{ClockOrdering, VectorClock};

/// Consistency level for cross-region replication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsistencyLevel {
    /// Updates are replicated asynchronously.
    Eventual,
    /// Causal ordering is preserved via vector clocks.
    Causal,
    /// All regions must acknowledge before returning.
    Strong,
}

impl std::fmt::Display for ConsistencyLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eventual => write!(f, "eventual"),
            Self::Causal => write!(f, "causal"),
            Self::Strong => write!(f, "strong"),
        }
    }
}

/// Configuration for the replication engine.
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub consistency_level: ConsistencyLevel,
    pub sync_interval_ms: u64,
    pub max_batch_size: usize,
    pub conflict_strategy: ConflictStrategy,
    pub conflict_log_size: usize,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            consistency_level: ConsistencyLevel::Eventual,
            sync_interval_ms: 1000,
            max_batch_size: 1000,
            conflict_strategy: ConflictStrategy::LastWriterWins,
            conflict_log_size: 1000,
        }
    }
}

/// Statistics about the replication engine.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReplicationStats {
    pub ops_replicated: u64,
    pub conflicts_detected: u64,
    pub conflicts_resolved: u64,
    pub regions_active: usize,
    pub avg_lag_ms: u64,
}

/// Cross-region active-active replication engine.
pub struct ActiveActiveReplicator {
    local_region: String,
    regions: DashMap<String, Region>,
    conflict_strategy: ConflictStrategy,
    vector_clocks: DashMap<String, VectorClock>,
    conflict_log: parking_lot::Mutex<Vec<Conflict>>,
    config: ReplicationConfig,
    stats: parking_lot::Mutex<ReplicationStats>,
}

impl ActiveActiveReplicator {
    /// Create a new replicator for the given local region.
    pub fn new(local_region: String, config: ReplicationConfig) -> Self {
        let strategy = config.conflict_strategy.clone();
        Self {
            local_region,
            regions: DashMap::new(),
            conflict_strategy: strategy,
            vector_clocks: DashMap::new(),
            conflict_log: parking_lot::Mutex::new(Vec::new()),
            config,
            stats: parking_lot::Mutex::new(ReplicationStats::default()),
        }
    }

    /// Create a replicator with default configuration.
    pub fn with_defaults(local_region: String) -> Self {
        Self::new(local_region, ReplicationConfig::default())
    }

    /// Add a region to the replication cluster.
    pub fn add_region(&self, id: String, name: String, endpoint: String) -> Result<(), String> {
        if self.regions.contains_key(&id) {
            return Err(format!("Region '{id}' already exists"));
        }
        let region = Region::new(id.clone(), name.clone(), endpoint);
        self.regions.insert(id.clone(), region);
        self.stats.lock().regions_active = self.regions.len();
        info!("Region added: {id} ({name})");
        Ok(())
    }

    /// Remove a region from the replication cluster.
    pub fn remove_region(&self, id: &str) -> Result<(), String> {
        if self.regions.remove(id).is_none() {
            return Err(format!("Region '{id}' not found"));
        }
        self.stats.lock().regions_active = self.regions.len();
        info!("Region removed: {id}");
        Ok(())
    }

    /// List all regions.
    pub fn list_regions(&self) -> Vec<Region> {
        self.regions.iter().map(|r| r.value().clone()).collect()
    }

    /// Get a specific region's status.
    pub fn get_region(&self, id: &str) -> Option<Region> {
        self.regions.get(id).map(|r| r.value().clone())
    }

    /// Record a local write and update the vector clock.
    pub fn record_write(&self, key: &str, _value: &[u8], _region_id: &str) {
        let mut entry = self
            .vector_clocks
            .entry(key.to_string())
            .or_insert_with(VectorClock::new);
        entry.increment(&self.local_region);
    }

    /// Simulate sending an update to a remote region.
    pub fn replicate_to(
        &self,
        region_id: &str,
        _key: &str,
        _value: &[u8],
        _clock: &VectorClock,
    ) -> Result<(), String> {
        if let Some(mut region) = self.regions.get_mut(region_id) {
            region.keys_synced += 1;
            region.last_sync = Some(Utc::now());
            region.status = RegionStatus::Active;
            self.stats.lock().ops_replicated += 1;
            Ok(())
        } else {
            Err(format!("Region '{region_id}' not found"))
        }
    }

    /// Handle an incoming update from a remote region. Detects and resolves conflicts.
    pub fn receive_update(
        &self,
        key: &str,
        remote_value: &[u8],
        remote_clock: &VectorClock,
    ) -> Result<ConflictResolution, String> {
        let local_clock = self
            .vector_clocks
            .get(key)
            .map(|c| c.clone())
            .unwrap_or_default();

        let ordering = local_clock.compare(remote_clock);

        match ordering {
            ClockOrdering::Before | ClockOrdering::Equal => {
                // Remote is newer or equal — accept remote
                self.vector_clocks
                    .entry(key.to_string())
                    .or_default()
                    .merge(remote_clock);

                Ok(ConflictResolution {
                    strategy_used: self.conflict_strategy.clone(),
                    winner: ConflictWinner::Remote,
                    resolved_value: remote_value.to_vec(),
                    resolved_at: Utc::now(),
                })
            }
            ClockOrdering::After => {
                // Local is newer — keep local
                Ok(ConflictResolution {
                    strategy_used: self.conflict_strategy.clone(),
                    winner: ConflictWinner::Local,
                    resolved_value: Vec::new(), // caller uses existing local value
                    resolved_at: Utc::now(),
                })
            }
            ClockOrdering::Concurrent => {
                // Conflict! Use conflict resolution strategy
                let conflict = Conflict {
                    key: key.to_string(),
                    local_value: Vec::new(), // in production, fetch from store
                    remote_value: remote_value.to_vec(),
                    local_clock: local_clock.clone(),
                    remote_clock: remote_clock.clone(),
                    local_timestamp: Utc::now(),
                    remote_timestamp: Utc::now(),
                    resolution: None,
                };

                let resolution =
                    ConflictResolver::resolve(&conflict, &self.conflict_strategy);

                // Log the conflict
                {
                    let mut log = self.conflict_log.lock();
                    let max = self.config.conflict_log_size;
                    if log.len() >= max {
                        let drain_count = log.len() - max + 1;
                        log.drain(..drain_count);
                    }
                    let mut logged = conflict;
                    logged.resolution = Some(resolution.clone());
                    log.push(logged);
                }

                // Update stats
                {
                    let mut stats = self.stats.lock();
                    stats.conflicts_detected += 1;
                    stats.conflicts_resolved += 1;
                }

                // Merge clocks after resolution
                self.vector_clocks
                    .entry(key.to_string())
                    .or_default()
                    .merge(remote_clock);

                Ok(resolution)
            }
        }
    }

    /// Get recent conflicts from the log.
    pub fn get_conflicts(&self, limit: usize) -> Vec<ConflictSummary> {
        let log = self.conflict_log.lock();
        log.iter()
            .rev()
            .take(limit)
            .map(|c| ConflictSummary {
                key: c.key.clone(),
                strategy: c
                    .resolution
                    .as_ref()
                    .map(|r| r.strategy_used.to_string())
                    .unwrap_or_default(),
                winner: c
                    .resolution
                    .as_ref()
                    .map(|r| format!("{:?}", r.winner))
                    .unwrap_or_default(),
                resolved_at: c
                    .resolution
                    .as_ref()
                    .map(|r| r.resolved_at.to_rfc3339())
                    .unwrap_or_default(),
            })
            .collect()
    }

    /// Get current replication statistics.
    pub fn stats(&self) -> ReplicationStats {
        let mut s = self.stats.lock().clone();
        s.regions_active = self.regions.len();

        // Compute average lag
        let total_lag: u64 = self.regions.iter().map(|r| r.replication_lag_ms).sum();
        let count = self.regions.len().max(1);
        s.avg_lag_ms = total_lag / count as u64;

        s
    }

    /// Get the current conflict strategy.
    pub fn conflict_strategy(&self) -> &ConflictStrategy {
        &self.conflict_strategy
    }

    /// Set the conflict resolution strategy.
    pub fn set_conflict_strategy(&mut self, strategy: ConflictStrategy) {
        self.conflict_strategy = strategy;
    }

    /// Get the local region ID.
    pub fn local_region(&self) -> &str {
        &self.local_region
    }
}

/// Serializable conflict summary for command responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictSummary {
    pub key: String,
    pub strategy: String,
    pub winner: String,
    pub resolved_at: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_remove_region() {
        let r = ActiveActiveReplicator::with_defaults("us-east".to_string());
        assert!(r.add_region("eu-west".to_string(), "EU West".to_string(), "10.0.2.1:6379".to_string()).is_ok());
        assert_eq!(r.list_regions().len(), 1);

        assert!(r.add_region("eu-west".to_string(), "dup".to_string(), "x".to_string()).is_err());

        assert!(r.remove_region("eu-west").is_ok());
        assert!(r.list_regions().is_empty());

        assert!(r.remove_region("no-exist").is_err());
    }

    #[test]
    fn test_record_write_updates_clock() {
        let r = ActiveActiveReplicator::with_defaults("us-east".to_string());
        r.record_write("key1", b"val", "us-east");
        r.record_write("key1", b"val2", "us-east");

        let clock = r.vector_clocks.get("key1").unwrap();
        assert_eq!(clock.get("us-east"), 2);
    }

    #[test]
    fn test_receive_update_no_conflict() {
        let r = ActiveActiveReplicator::with_defaults("us-east".to_string());
        let mut remote_clock = VectorClock::new();
        remote_clock.increment("eu-west");

        let res = r.receive_update("key1", b"remote-val", &remote_clock).unwrap();
        assert_eq!(res.winner, ConflictWinner::Remote);
    }

    #[test]
    fn test_receive_update_with_conflict() {
        let r = ActiveActiveReplicator::with_defaults("us-east".to_string());
        // Local write
        r.record_write("key1", b"local-val", "us-east");

        // Remote write on a different region (concurrent)
        let mut remote_clock = VectorClock::new();
        remote_clock.increment("eu-west");

        let res = r.receive_update("key1", b"remote-val", &remote_clock).unwrap();
        // LWW is default — resolution should exist
        assert!(matches!(res.winner, ConflictWinner::Local | ConflictWinner::Remote));

        assert_eq!(r.get_conflicts(10).len(), 1);
        assert_eq!(r.stats().conflicts_detected, 1);
    }

    #[test]
    fn test_replicate_to() {
        let r = ActiveActiveReplicator::with_defaults("us-east".to_string());
        r.add_region("eu-west".to_string(), "EU West".to_string(), "10.0.2.1:6379".to_string()).unwrap();

        let clock = VectorClock::new();
        assert!(r.replicate_to("eu-west", "key1", b"val", &clock).is_ok());
        assert!(r.replicate_to("no-exist", "key1", b"val", &clock).is_err());

        let region = r.get_region("eu-west").unwrap();
        assert_eq!(region.keys_synced, 1);
        assert_eq!(region.status, RegionStatus::Active);
    }

    #[test]
    fn test_stats() {
        let r = ActiveActiveReplicator::with_defaults("us-east".to_string());
        let s = r.stats();
        assert_eq!(s.regions_active, 0);
        assert_eq!(s.ops_replicated, 0);
    }
}
