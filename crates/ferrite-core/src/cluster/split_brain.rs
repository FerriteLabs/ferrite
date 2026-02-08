//! Split-brain detection and recovery for Ferrite clusters.
//!
//! Detects network partitions through gossip heartbeat timeouts, forces
//! nodes in a minority partition into read-only mode, and provides
//! recovery procedures to reunite the cluster.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tracing::{error, info, warn};

use super::{ClusterManager, NodeId, NodeState};

/// Configuration for split-brain detection.
#[derive(Debug, Clone)]
pub struct SplitBrainConfig {
    /// How long without a PONG before a node is considered unreachable.
    pub heartbeat_timeout: Duration,
    /// How many consecutive misses before marking PFAIL.
    pub pfail_threshold: u32,
    /// Fraction of reachable primaries required to stay in majority (0.0–1.0).
    pub majority_fraction: f64,
    /// When `true` (the default), nodes in the minority partition become
    /// read-only instead of refusing all operations.
    pub minority_read_only: bool,
    /// Whether full slot coverage is required for the cluster to be "ok".
    /// Mirrors Redis `cluster-require-full-coverage`.
    pub require_full_coverage: bool,
    /// Interval for running the partition check.
    pub check_interval: Duration,
}

impl Default for SplitBrainConfig {
    fn default() -> Self {
        Self {
            heartbeat_timeout: Duration::from_secs(15),
            pfail_threshold: 3,
            majority_fraction: 0.5,
            minority_read_only: true,
            require_full_coverage: true,
            check_interval: Duration::from_secs(1),
        }
    }
}

/// Result of a partition analysis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionStatus {
    /// We can reach a majority of primaries — cluster is healthy.
    Majority,
    /// We can only reach a minority — restrict operations.
    Minority,
    /// We cannot determine the partition (e.g., no other primaries).
    Indeterminate,
}

impl std::fmt::Display for PartitionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Majority => write!(f, "majority"),
            Self::Minority => write!(f, "minority"),
            Self::Indeterminate => write!(f, "indeterminate"),
        }
    }
}

/// Per-node heartbeat tracking.
#[derive(Debug, Clone)]
pub struct HeartbeatState {
    /// Node identifier.
    pub node_id: NodeId,
    /// When we last received a heartbeat (PONG) from this node.
    pub last_heartbeat: Instant,
    /// Consecutive missed heartbeats.
    pub missed_count: u32,
    /// Whether we consider this node reachable.
    pub reachable: bool,
}

/// Split-brain detector.
pub struct SplitBrainDetector {
    config: SplitBrainConfig,
    cluster: Arc<ClusterManager>,
    /// Per-node heartbeat tracking.
    heartbeats: RwLock<HashMap<NodeId, HeartbeatState>>,
    /// Current partition status.
    partition_status: RwLock<PartitionStatus>,
    /// Whether this node is in read-only mode due to minority partition.
    read_only: AtomicBool,
    /// When the current partition was first detected.
    partition_detected_at: RwLock<Option<Instant>>,
    /// Recovery log for observability.
    recovery_log: RwLock<Vec<RecoveryEvent>>,
}

/// An event logged during partition detection/recovery.
#[derive(Debug, Clone)]
pub struct RecoveryEvent {
    /// When the event occurred.
    pub timestamp: Instant,
    /// Description.
    pub description: String,
    /// Nodes involved.
    pub nodes: Vec<NodeId>,
}

impl SplitBrainDetector {
    /// Create a new detector.
    pub fn new(config: SplitBrainConfig, cluster: Arc<ClusterManager>) -> Self {
        Self {
            config,
            cluster,
            heartbeats: RwLock::new(HashMap::new()),
            partition_status: RwLock::new(PartitionStatus::Indeterminate),
            read_only: AtomicBool::new(false),
            partition_detected_at: RwLock::new(None),
            recovery_log: RwLock::new(Vec::new()),
        }
    }

    /// Record a heartbeat received from `node_id`.
    pub fn record_heartbeat(&self, node_id: &NodeId) {
        let mut heartbeats = self.heartbeats.write();
        let entry = heartbeats.entry(node_id.clone()).or_insert(HeartbeatState {
            node_id: node_id.clone(),
            last_heartbeat: Instant::now(),
            missed_count: 0,
            reachable: true,
        });
        entry.last_heartbeat = Instant::now();
        entry.missed_count = 0;
        entry.reachable = true;
    }

    /// Run the periodic partition check.
    ///
    /// Should be called at `config.check_interval` cadence.
    pub fn check_partition(&self) {
        self.update_heartbeat_states();
        let new_status = self.evaluate_partition();

        let old_status = self.partition_status.read().clone();
        if new_status != old_status {
            info!(
                old = %old_status,
                new = %new_status,
                "Partition status changed"
            );

            match new_status {
                PartitionStatus::Minority => {
                    warn!("This node is in a MINORITY partition");
                    if self.config.minority_read_only {
                        self.read_only.store(true, Ordering::SeqCst);
                        info!("Entering read-only mode (minority partition)");
                    }
                    *self.partition_detected_at.write() = Some(Instant::now());
                    self.log_recovery_event(
                        "Minority partition detected".to_string(),
                        self.unreachable_nodes(),
                    );
                }
                PartitionStatus::Majority | PartitionStatus::Indeterminate => {
                    if old_status == PartitionStatus::Minority {
                        self.read_only.store(false, Ordering::SeqCst);
                        info!("Leaving read-only mode (partition resolved)");
                        self.log_recovery_event("Partition resolved".to_string(), vec![]);
                    }
                    *self.partition_detected_at.write() = None;
                }
            }
            *self.partition_status.write() = new_status;
        }
    }

    /// Whether this node is currently in read-only mode due to split-brain.
    pub fn is_read_only(&self) -> bool {
        self.read_only.load(Ordering::SeqCst)
    }

    /// Force exit read-only mode (manual recovery).
    pub fn force_recover(&self) {
        self.read_only.store(false, Ordering::SeqCst);
        *self.partition_status.write() = PartitionStatus::Indeterminate;
        *self.partition_detected_at.write() = None;
        info!("Manual split-brain recovery: exiting read-only mode");
        self.log_recovery_event("Manual recovery triggered".to_string(), vec![]);
    }

    /// Current partition status.
    pub fn status(&self) -> PartitionStatus {
        self.partition_status.read().clone()
    }

    /// Whether `require_full_coverage` is enabled.
    pub fn require_full_coverage(&self) -> bool {
        self.config.require_full_coverage
    }

    /// Nodes currently considered unreachable.
    pub fn unreachable_nodes(&self) -> Vec<NodeId> {
        self.heartbeats
            .read()
            .values()
            .filter(|h| !h.reachable)
            .map(|h| h.node_id.clone())
            .collect()
    }

    /// Reachable node count (excluding self).
    pub fn reachable_count(&self) -> usize {
        self.heartbeats
            .read()
            .values()
            .filter(|h| h.reachable)
            .count()
    }

    /// Get the recovery event log.
    pub fn recovery_log(&self) -> Vec<RecoveryEvent> {
        self.recovery_log.read().clone()
    }

    /// Duration since partition was detected, if any.
    pub fn partition_duration(&self) -> Option<Duration> {
        self.partition_detected_at.read().map(|t| t.elapsed())
    }

    // ── internals ────────────────────────────────────────────────────

    fn update_heartbeat_states(&self) {
        let now = Instant::now();
        let timeout = self.config.heartbeat_timeout;
        let mut heartbeats = self.heartbeats.write();

        for state in heartbeats.values_mut() {
            if now.duration_since(state.last_heartbeat) > timeout {
                state.missed_count += 1;
                if state.missed_count >= self.config.pfail_threshold && state.reachable {
                    warn!(node = %state.node_id, "Node marked unreachable after {} missed heartbeats", state.missed_count);
                    state.reachable = false;
                    self.cluster.mark_node_pfail(&state.node_id);
                }
            }
        }
    }

    fn evaluate_partition(&self) -> PartitionStatus {
        // Use all known nodes (including PFail/Fail) to evaluate partition.
        let all_nodes = self.cluster.get_all_nodes();
        let my_id = self.cluster.node_id();
        let remote_primaries: Vec<_> = all_nodes
            .iter()
            .filter(|n| n.is_primary() && &n.id != my_id)
            .collect();

        if remote_primaries.is_empty() {
            return PartitionStatus::Indeterminate;
        }

        let heartbeats = self.heartbeats.read();
        let reachable_count = remote_primaries
            .iter()
            .filter(|n| {
                heartbeats
                    .get(&n.id)
                    .map(|h| h.reachable)
                    .unwrap_or(false)
            })
            .count();

        let total = remote_primaries.len();
        let ratio = reachable_count as f64 / total as f64;

        if ratio > self.config.majority_fraction {
            PartitionStatus::Majority
        } else {
            PartitionStatus::Minority
        }
    }

    fn log_recovery_event(&self, description: String, nodes: Vec<NodeId>) {
        self.recovery_log.write().push(RecoveryEvent {
            timestamp: Instant::now(),
            description,
            nodes,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    use crate::cluster::{ClusterConfig, ClusterNode};

    fn make_cluster() -> Arc<ClusterManager> {
        let config = ClusterConfig::default();
        let addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        Arc::new(ClusterManager::new(config, "node0".to_string(), addr))
    }

    fn add_primary(cluster: &ClusterManager, id: &str, port: u16) {
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        cluster.add_node(ClusterNode::new(id.to_string(), addr));
    }

    #[test]
    fn test_default_config() {
        let config = SplitBrainConfig::default();
        assert!(config.minority_read_only);
        assert!(config.require_full_coverage);
        assert_eq!(config.pfail_threshold, 3);
    }

    #[test]
    fn test_indeterminate_with_no_peers() {
        let cluster = make_cluster();
        let detector = SplitBrainDetector::new(SplitBrainConfig::default(), cluster);
        detector.check_partition();
        assert_eq!(detector.status(), PartitionStatus::Indeterminate);
    }

    #[test]
    fn test_majority_when_all_reachable() {
        let cluster = make_cluster();
        add_primary(&cluster, "node1", 7001);
        add_primary(&cluster, "node2", 7002);

        let detector = SplitBrainDetector::new(SplitBrainConfig::default(), cluster);
        detector.record_heartbeat(&"node1".to_string());
        detector.record_heartbeat(&"node2".to_string());

        detector.check_partition();
        assert_eq!(detector.status(), PartitionStatus::Majority);
        assert!(!detector.is_read_only());
    }

    #[test]
    fn test_minority_triggers_read_only() {
        let cluster = make_cluster();
        add_primary(&cluster, "node1", 7001);
        add_primary(&cluster, "node2", 7002);
        add_primary(&cluster, "node3", 7003);

        let mut config = SplitBrainConfig::default();
        config.heartbeat_timeout = Duration::from_secs(5);
        config.pfail_threshold = 1;

        let detector = SplitBrainDetector::new(config, cluster);
        // Only record heartbeat for node1 — nodes 2 and 3 are unreachable.
        detector.record_heartbeat(&"node1".to_string());

        // Force the heartbeats for node2/node3 to be in the map but old.
        {
            let mut hb = detector.heartbeats.write();
            hb.insert(
                "node2".to_string(),
                HeartbeatState {
                    node_id: "node2".to_string(),
                    last_heartbeat: Instant::now() - Duration::from_secs(60),
                    missed_count: 0,
                    reachable: true,
                },
            );
            hb.insert(
                "node3".to_string(),
                HeartbeatState {
                    node_id: "node3".to_string(),
                    last_heartbeat: Instant::now() - Duration::from_secs(60),
                    missed_count: 0,
                    reachable: true,
                },
            );
        }

        detector.check_partition();
        assert_eq!(detector.status(), PartitionStatus::Minority);
        assert!(detector.is_read_only());
        assert_eq!(detector.unreachable_nodes().len(), 2);
    }

    #[test]
    fn test_force_recover() {
        let cluster = make_cluster();
        let detector = SplitBrainDetector::new(SplitBrainConfig::default(), cluster);

        // Simulate read-only state.
        detector.read_only.store(true, Ordering::SeqCst);
        *detector.partition_status.write() = PartitionStatus::Minority;

        detector.force_recover();
        assert!(!detector.is_read_only());
        assert_eq!(detector.status(), PartitionStatus::Indeterminate);
    }

    #[test]
    fn test_recovery_log() {
        let cluster = make_cluster();
        let detector = SplitBrainDetector::new(SplitBrainConfig::default(), cluster);
        detector.force_recover();
        assert!(!detector.recovery_log().is_empty());
    }

    #[test]
    fn test_partition_status_display() {
        assert_eq!(PartitionStatus::Majority.to_string(), "majority");
        assert_eq!(PartitionStatus::Minority.to_string(), "minority");
        assert_eq!(PartitionStatus::Indeterminate.to_string(), "indeterminate");
    }

    #[test]
    fn test_reachable_count() {
        let cluster = make_cluster();
        let detector = SplitBrainDetector::new(SplitBrainConfig::default(), cluster);
        detector.record_heartbeat(&"node1".to_string());
        detector.record_heartbeat(&"node2".to_string());
        assert_eq!(detector.reachable_count(), 2);
    }
}
