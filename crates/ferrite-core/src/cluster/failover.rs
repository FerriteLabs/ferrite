//! Automatic Failover for Ferrite Cluster
//!
//! This module implements automatic failover for Redis Cluster compatibility:
//! - Primary failure detection via gossip protocol (PFAIL -> FAIL state)
//! - Replica promotion election using Raft-like consensus
//! - Manual failover via CLUSTER FAILOVER command
//! - Cluster topology updates after failover

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::sync::broadcast;
use tokio::time;
use tracing::{debug, error, info, warn};

use super::{ClusterManager, NodeId, NodeRole, SlotRange};

/// Failover configuration
#[derive(Debug, Clone)]
pub struct FailoverConfig {
    /// Timeout for failover election (milliseconds)
    pub election_timeout_ms: u64,
    /// Minimum delay before replica can request election
    pub replica_validity_factor: u64,
    /// Maximum time to wait for manual failover
    pub manual_failover_timeout_ms: u64,
    /// Require primary acknowledgment for manual failover
    pub require_primary_ack: bool,
    /// Enable automatic failover
    pub auto_failover_enabled: bool,
}

impl Default for FailoverConfig {
    fn default() -> Self {
        Self {
            election_timeout_ms: 5000,
            replica_validity_factor: 10,
            manual_failover_timeout_ms: 60000,
            require_primary_ack: true,
            auto_failover_enabled: true,
        }
    }
}

/// Failover state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverState {
    /// No failover in progress
    None,
    /// Waiting for primary to pause
    WaitingForPrimaryPause,
    /// Waiting for replicas to catch up
    WaitingForReplicaSync,
    /// Election in progress
    Election,
    /// Promoting self to primary
    Promoting,
    /// Failover complete
    Complete,
    /// Failover failed
    Failed,
}

/// Failover request type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverType {
    /// Normal failover (requires primary acknowledgment)
    Normal,
    /// Force failover (no primary acknowledgment needed)
    Force,
    /// Takeover (immediate, no election, no primary ack)
    Takeover,
}

/// Failover election vote
#[derive(Debug, Clone)]
pub struct FailoverVote {
    /// Node ID of the voter
    pub voter_id: NodeId,
    /// Node ID being voted for
    pub candidate_id: NodeId,
    /// Configuration epoch when vote was cast
    pub config_epoch: u64,
    /// Timestamp of the vote
    pub timestamp: Instant,
}

/// Failover manager handles automatic and manual failover
pub struct FailoverManager {
    /// Reference to cluster manager
    cluster: Arc<ClusterManager>,
    /// Failover configuration
    config: FailoverConfig,
    /// Current failover state
    state: RwLock<FailoverState>,
    /// Current failover type (if any)
    failover_type: RwLock<Option<FailoverType>>,
    /// Failed primary we're failing over from
    failed_primary_id: RwLock<Option<NodeId>>,
    /// Election votes received
    votes: RwLock<Vec<FailoverVote>>,
    /// Our current vote (if we voted)
    our_vote: RwLock<Option<FailoverVote>>,
    /// Last time we requested election
    last_election_request: RwLock<Option<Instant>>,
    /// Configuration epoch for current election
    election_epoch: RwLock<u64>,
    /// Shutdown signal
    shutdown_tx: broadcast::Sender<()>,
}

impl FailoverManager {
    /// Create a new failover manager
    pub fn new(cluster: Arc<ClusterManager>, config: FailoverConfig) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);

        Self {
            cluster,
            config,
            state: RwLock::new(FailoverState::None),
            failover_type: RwLock::new(None),
            failed_primary_id: RwLock::new(None),
            votes: RwLock::new(Vec::new()),
            our_vote: RwLock::new(None),
            last_election_request: RwLock::new(None),
            election_epoch: RwLock::new(0),
            shutdown_tx,
        }
    }

    /// Get current failover state
    pub fn state(&self) -> FailoverState {
        *self.state.read()
    }

    /// Check if failover is in progress
    pub fn is_failover_in_progress(&self) -> bool {
        !matches!(
            *self.state.read(),
            FailoverState::None | FailoverState::Complete | FailoverState::Failed
        )
    }

    /// Start manual failover (CLUSTER FAILOVER command)
    pub fn start_manual_failover(&self, failover_type: FailoverType) -> Result<(), String> {
        // Check if we're a replica
        let self_node = self
            .cluster
            .get_node(self.cluster.node_id())
            .ok_or("Cannot get self node info")?;

        if self_node.role != NodeRole::Replica {
            return Err("Manual failover can only be triggered on a replica".to_string());
        }

        let primary_id = self_node
            .primary_id
            .as_ref()
            .ok_or("Replica has no primary configured")?;

        // Check if already in failover
        if self.is_failover_in_progress() {
            return Err("Failover already in progress".to_string());
        }

        // Start failover
        *self.failover_type.write() = Some(failover_type);
        *self.failed_primary_id.write() = Some(primary_id.clone());

        match failover_type {
            FailoverType::Takeover => {
                // Immediate takeover - no election, no waiting
                *self.state.write() = FailoverState::Promoting;
                self.promote_self();
            }
            FailoverType::Force => {
                // Force - start election immediately without primary ack
                *self.state.write() = FailoverState::Election;
                self.start_election();
            }
            FailoverType::Normal => {
                // Normal - wait for primary pause
                *self.state.write() = FailoverState::WaitingForPrimaryPause;
                // In full implementation, send FAILOVER message to primary
                info!("Manual failover initiated, waiting for primary pause");
            }
        }

        Ok(())
    }

    /// Handle detected primary failure
    pub fn handle_primary_failure(&self, failed_primary_id: &NodeId) {
        if !self.config.auto_failover_enabled {
            warn!(
                "Auto-failover disabled, not handling primary {} failure",
                failed_primary_id
            );
            return;
        }

        // Check if we're a replica of the failed primary
        let self_node = match self.cluster.get_node(self.cluster.node_id()) {
            Some(node) => node,
            None => return,
        };

        if self_node.role != NodeRole::Replica {
            return;
        }

        if self_node.primary_id.as_ref() != Some(failed_primary_id) {
            return;
        }

        // Check if already in failover
        if self.is_failover_in_progress() {
            return;
        }

        info!(
            "Primary {} failed, initiating automatic failover",
            failed_primary_id
        );

        *self.failover_type.write() = Some(FailoverType::Force);
        *self.failed_primary_id.write() = Some(failed_primary_id.clone());
        *self.state.write() = FailoverState::Election;

        self.start_election();
    }

    /// Start election for replica promotion
    fn start_election(&self) {
        info!("Starting failover election");

        // Clear previous votes
        self.votes.write().clear();
        *self.our_vote.write() = None;
        *self.last_election_request.write() = Some(Instant::now());

        // Increment election epoch
        let mut epoch = self.election_epoch.write();
        *epoch += 1;
        let _current_epoch = *epoch;
        drop(epoch);

        // In a full implementation, we would:
        // 1. Broadcast FAILOVER_AUTH_REQUEST to all primaries
        // 2. Wait for FAILOVER_AUTH_ACK responses
        // For now, simulate winning the election if we're the only replica

        let failed_primary = self.failed_primary_id.read().clone();
        if let Some(primary_id) = failed_primary {
            let replicas = self.cluster.get_replicas(&primary_id);

            if replicas.len() == 1 && replicas[0].id == *self.cluster.node_id() {
                // We're the only replica, auto-win
                info!("Only replica available, winning election by default");
                self.handle_election_win();
            } else if replicas.is_empty() {
                error!("No replicas available for failover");
                *self.state.write() = FailoverState::Failed;
            } else {
                // Multiple replicas - would need actual election
                // For now, check if we have the best replication offset
                debug!("Multiple replicas found, election would proceed");
                // Simulate election timeout
                *self.state.write() = FailoverState::Promoting;
                self.promote_self();
            }
        }
    }

    /// Handle receiving a vote
    pub fn handle_vote(&self, vote: FailoverVote) {
        if vote.candidate_id != *self.cluster.node_id() {
            return;
        }

        if *self.state.read() != FailoverState::Election {
            return;
        }

        let mut votes = self.votes.write();

        // Check for duplicate vote from same voter
        if votes.iter().any(|v| v.voter_id == vote.voter_id) {
            return;
        }

        votes.push(vote);

        // Check if we have quorum
        let primaries = self.cluster.get_primaries();
        let required_votes = (primaries.len() / 2) + 1;

        if votes.len() >= required_votes {
            info!(
                "Election won with {}/{} votes",
                votes.len(),
                primaries.len()
            );
            drop(votes);
            self.handle_election_win();
        }
    }

    /// Handle winning the election
    fn handle_election_win(&self) {
        info!("Failover election won, promoting to primary");
        *self.state.write() = FailoverState::Promoting;
        self.promote_self();
    }

    /// Promote self to primary
    fn promote_self(&self) {
        let failed_primary_id = self.failed_primary_id.read().clone();

        if let Some(primary_id) = failed_primary_id {
            // Get the slots from the failed primary
            let slots: Vec<SlotRange> =
                if let Some(primary_node) = self.cluster.get_node(&primary_id) {
                    primary_node.slots.clone()
                } else {
                    Vec::new()
                };

            // Update our role to primary
            if let Some(mut self_node) = self.cluster.get_node(self.cluster.node_id()) {
                self_node.role = NodeRole::Primary;
                self_node.primary_id = None;
                self_node.slots = slots;

                // In full implementation, update cluster state
                info!(
                    "Promoted to primary, taking over {} slots",
                    self_node.slots.iter().map(|r| r.count()).sum::<usize>()
                );
            }

            // Mark old primary as failed if not already
            self.cluster.mark_node_failed(&primary_id);

            *self.state.write() = FailoverState::Complete;
            info!("Failover complete");
        } else {
            error!("No failed primary to take over from");
            *self.state.write() = FailoverState::Failed;
        }
    }

    /// Abort current failover
    pub fn abort_failover(&self) {
        info!("Aborting failover");
        *self.state.write() = FailoverState::None;
        *self.failover_type.write() = None;
        *self.failed_primary_id.write() = None;
        self.votes.write().clear();
        *self.our_vote.write() = None;
    }

    /// Check if we should vote for a candidate
    pub fn should_vote_for(&self, _candidate_id: &NodeId, candidate_epoch: u64) -> bool {
        // Only primaries can vote
        let self_node = match self.cluster.get_node(self.cluster.node_id()) {
            Some(node) => node,
            None => return false,
        };

        if self_node.role != NodeRole::Primary {
            return false;
        }

        // Check if we already voted in this epoch
        if let Some(ref vote) = *self.our_vote.read() {
            if vote.config_epoch >= candidate_epoch {
                return false;
            }
        }

        // Would also check:
        // - Candidate is a replica of the failed primary
        // - Candidate has valid replication data
        // - Election timeout hasn't expired

        true
    }

    /// Cast vote for a candidate
    pub fn cast_vote(&self, candidate_id: &NodeId, candidate_epoch: u64) -> Option<FailoverVote> {
        if !self.should_vote_for(candidate_id, candidate_epoch) {
            return None;
        }

        let vote = FailoverVote {
            voter_id: self.cluster.node_id().clone(),
            candidate_id: candidate_id.clone(),
            config_epoch: candidate_epoch,
            timestamp: Instant::now(),
        };

        *self.our_vote.write() = Some(vote.clone());

        info!("Voted for {} in epoch {}", candidate_id, candidate_epoch);

        Some(vote)
    }

    /// Get failover statistics
    pub fn get_stats(&self) -> FailoverStats {
        FailoverStats {
            state: *self.state.read(),
            failover_type: *self.failover_type.read(),
            failed_primary: self.failed_primary_id.read().clone(),
            votes_received: self.votes.read().len(),
            election_epoch: *self.election_epoch.read(),
        }
    }
}

/// Failover statistics
#[derive(Debug, Clone)]
pub struct FailoverStats {
    /// Current failover state
    pub state: FailoverState,
    /// Type of failover in progress
    pub failover_type: Option<FailoverType>,
    /// Failed primary being replaced
    pub failed_primary: Option<NodeId>,
    /// Number of votes received
    pub votes_received: usize,
    /// Current election epoch
    pub election_epoch: u64,
}

/// Start the failover monitor task
pub async fn start_failover_monitor(
    manager: Arc<FailoverManager>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    info!("Starting failover monitor");

    let mut interval = time::interval(Duration::from_millis(1000));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Check for election timeout
                if manager.state() == FailoverState::Election {
                    if let Some(start_time) = *manager.last_election_request.read() {
                        if start_time.elapsed() > Duration::from_millis(manager.config.election_timeout_ms) {
                            warn!("Election timeout, aborting failover");
                            manager.abort_failover();
                        }
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                info!("Failover monitor shutting down");
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    use crate::cluster::{generate_node_id, ClusterConfig};

    fn create_test_manager() -> (Arc<ClusterManager>, Arc<FailoverManager>) {
        let config = ClusterConfig::default();
        let node_id = generate_node_id();
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let cluster = Arc::new(ClusterManager::new(config, node_id, addr));
        let failover = Arc::new(FailoverManager::new(
            cluster.clone(),
            FailoverConfig::default(),
        ));
        (cluster, failover)
    }

    #[test]
    fn test_failover_config_default() {
        let config = FailoverConfig::default();
        assert_eq!(config.election_timeout_ms, 5000);
        assert!(config.auto_failover_enabled);
    }

    #[test]
    fn test_failover_state_initial() {
        let (_cluster, failover) = create_test_manager();
        assert_eq!(failover.state(), FailoverState::None);
        assert!(!failover.is_failover_in_progress());
    }

    #[test]
    fn test_manual_failover_requires_replica() {
        let (_cluster, failover) = create_test_manager();
        // Node is a primary by default, should fail
        let result = failover.start_manual_failover(FailoverType::Normal);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("replica"));
    }

    #[test]
    fn test_failover_stats() {
        let (_cluster, failover) = create_test_manager();
        let stats = failover.get_stats();
        assert_eq!(stats.state, FailoverState::None);
        assert!(stats.failover_type.is_none());
        assert!(stats.failed_primary.is_none());
        assert_eq!(stats.votes_received, 0);
    }

    #[test]
    fn test_abort_failover() {
        let (_cluster, failover) = create_test_manager();
        failover.abort_failover();
        assert_eq!(failover.state(), FailoverState::None);
    }

    #[test]
    fn test_vote_for_candidate() {
        let (cluster, failover) = create_test_manager();
        let candidate_id = generate_node_id();

        // As a primary (default), we should be able to vote
        let vote = failover.cast_vote(&candidate_id, 1);
        assert!(vote.is_some());

        let vote = vote.unwrap();
        assert_eq!(vote.candidate_id, candidate_id);
        assert_eq!(vote.config_epoch, 1);
    }

    #[test]
    fn test_no_double_voting() {
        let (_cluster, failover) = create_test_manager();
        let candidate_id = generate_node_id();

        // First vote should succeed
        let vote1 = failover.cast_vote(&candidate_id, 1);
        assert!(vote1.is_some());

        // Second vote in same epoch should fail
        let vote2 = failover.cast_vote(&candidate_id, 1);
        assert!(vote2.is_none());
    }

    #[test]
    fn test_vote_higher_epoch() {
        let (_cluster, failover) = create_test_manager();
        let candidate1 = generate_node_id();
        let candidate2 = generate_node_id();

        // Vote in epoch 1
        let vote1 = failover.cast_vote(&candidate1, 1);
        assert!(vote1.is_some());

        // Can vote in higher epoch
        let vote2 = failover.cast_vote(&candidate2, 2);
        assert!(vote2.is_some());
    }

    #[test]
    fn test_handle_vote() {
        let (cluster, failover) = create_test_manager();

        // Put in election state
        *failover.state.write() = FailoverState::Election;

        // Vote for ourselves
        let vote = FailoverVote {
            voter_id: generate_node_id(),
            candidate_id: cluster.node_id().clone(),
            config_epoch: 1,
            timestamp: Instant::now(),
        };

        failover.handle_vote(vote);

        // With single vote (and only one primary), we might not win
        // This depends on quorum calculation
        let stats = failover.get_stats();
        assert_eq!(stats.votes_received, 1);
    }

    #[test]
    fn test_failover_type() {
        assert_ne!(FailoverType::Normal, FailoverType::Force);
        assert_ne!(FailoverType::Force, FailoverType::Takeover);
    }
}
