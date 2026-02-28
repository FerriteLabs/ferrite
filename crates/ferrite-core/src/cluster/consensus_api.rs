//! Consensus-as-a-Service API
//!
//! Exposes Raft consensus as a general-purpose distributed coordination
//! primitive. Clients can create consensus groups for leader election,
//! distributed config, and state machines.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the Consensus-as-a-Service engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusConfig {
    /// Maximum number of consensus groups.
    pub max_groups: usize,
    /// Election timeout before a new leader is chosen.
    pub election_timeout: Duration,
    /// Heartbeat interval between leader and followers.
    pub heartbeat_interval: Duration,
    /// Maximum log entries per group.
    pub max_log_entries: usize,
}

impl Default for ConsensusConfig {
    fn default() -> Self {
        Self {
            max_groups: 100,
            election_timeout: Duration::from_secs(1),
            heartbeat_interval: Duration::from_millis(200),
            max_log_entries: 10_000,
        }
    }
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// State of a consensus group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupState {
    /// Group is being formed (not yet active).
    Forming,
    /// Group is active and has a leader.
    Active,
    /// Election is in progress.
    Electing,
    /// Group is degraded (lost quorum).
    Degraded,
    /// Group has been dissolved.
    Dissolved,
}

impl std::fmt::Display for GroupState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Forming => write!(f, "Forming"),
            Self::Active => write!(f, "Active"),
            Self::Electing => write!(f, "Electing"),
            Self::Degraded => write!(f, "Degraded"),
            Self::Dissolved => write!(f, "Dissolved"),
        }
    }
}

/// A consensus group with leader election and replicated state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusGroup {
    /// Group name (unique identifier).
    pub name: String,
    /// Current group state.
    pub state: GroupState,
    /// Current leader node, if any.
    pub leader: Option<String>,
    /// Member node identifiers.
    pub members: Vec<String>,
    /// Monotonically increasing epoch for this group.
    pub epoch: u64,
    /// Creation timestamp (unix seconds).
    pub created_at: u64,
    /// Replicated key-value data.
    pub data: HashMap<String, Vec<u8>>,
}

/// A committed entry in the consensus log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusEntry {
    /// Key that was proposed.
    pub key: String,
    /// Value bytes.
    pub value: Vec<u8>,
    /// Epoch at which this entry was committed.
    pub epoch: u64,
    /// Node that proposed this entry.
    pub proposed_by: String,
    /// Commit timestamp (unix seconds).
    pub timestamp: u64,
}

/// Summary information about a consensus group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupInfo {
    /// Group name.
    pub name: String,
    /// Current state.
    pub state: GroupState,
    /// Current leader.
    pub leader: Option<String>,
    /// Number of members.
    pub members_count: usize,
    /// Current epoch.
    pub epoch: u64,
    /// Number of committed entries.
    pub entries: usize,
    /// Creation timestamp.
    pub created_at: u64,
}

/// Aggregate statistics for the consensus service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusStats {
    /// Number of active groups.
    pub groups: usize,
    /// Total proposals received.
    pub total_proposals: u64,
    /// Total linearizable reads.
    pub total_reads: u64,
    /// Total elections triggered.
    pub elections: u64,
    /// Average proposal latency in microseconds.
    pub avg_latency_us: f64,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors returned by the consensus API.
#[derive(Debug, thiserror::Error)]
pub enum ConsensusError {
    /// The requested group was not found.
    #[error("consensus group '{0}' not found")]
    GroupNotFound(String),
    /// A group with this name already exists.
    #[error("consensus group '{0}' already exists")]
    GroupExists(String),
    /// The current node is not the leader for this group.
    #[error("not the leader for group '{0}'")]
    NotLeader(String),
    /// The group does not have quorum.
    #[error("no quorum for group '{0}'")]
    NoQuorum(String),
    /// The proposal was rejected.
    #[error("proposal rejected for group '{0}'")]
    ProposalRejected(String),
    /// Maximum number of groups reached.
    #[error("maximum number of consensus groups reached ({0})")]
    MaxGroups(usize),
    /// The specified member was not found in the group.
    #[error("member '{0}' not found in group '{1}'")]
    MemberNotFound(String, String),
}

// ---------------------------------------------------------------------------
// Internal statistics (atomic counters)
// ---------------------------------------------------------------------------

struct InternalStats {
    total_proposals: AtomicU64,
    total_reads: AtomicU64,
    elections: AtomicU64,
    total_latency_us: AtomicU64,
}

impl InternalStats {
    fn new() -> Self {
        Self {
            total_proposals: AtomicU64::new(0),
            total_reads: AtomicU64::new(0),
            elections: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
        }
    }
}

// ---------------------------------------------------------------------------
// ConsensusManager
// ---------------------------------------------------------------------------

/// Manages multiple consensus groups.
///
/// In single-node mode the local node is always the leader and proposals are
/// committed immediately. This gives the correct API shape while real
/// multi-node Raft would be wired in production.
pub struct ConsensusManager {
    config: ConsensusConfig,
    groups: RwLock<HashMap<String, ConsensusGroup>>,
    /// Per-group committed entries (separate from the replicated data map).
    entries: RwLock<HashMap<String, Vec<ConsensusEntry>>>,
    stats: InternalStats,
    /// The local node identifier used as the default leader.
    local_node: String,
}

impl ConsensusManager {
    /// Create a new consensus manager.
    pub fn new(config: ConsensusConfig) -> Self {
        Self {
            config,
            groups: RwLock::new(HashMap::new()),
            entries: RwLock::new(HashMap::new()),
            stats: InternalStats::new(),
            local_node: "local-node-1".to_string(),
        }
    }

    // -- helpers --

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    // -- public API --

    /// Create a new consensus group with the given members.
    pub fn create_group(
        &self,
        name: &str,
        members: Vec<String>,
    ) -> Result<ConsensusGroup, ConsensusError> {
        let mut groups = self.groups.write();
        if groups.contains_key(name) {
            return Err(ConsensusError::GroupExists(name.to_string()));
        }
        if groups.len() >= self.config.max_groups {
            return Err(ConsensusError::MaxGroups(self.config.max_groups));
        }

        let group = ConsensusGroup {
            name: name.to_string(),
            state: GroupState::Active,
            leader: Some(self.local_node.clone()),
            members: if members.is_empty() {
                vec![self.local_node.clone()]
            } else {
                members
            },
            epoch: 1,
            created_at: Self::now_secs(),
            data: HashMap::new(),
        };

        let result = group.clone();
        groups.insert(name.to_string(), group);
        self.entries
            .write()
            .insert(name.to_string(), Vec::new());
        Ok(result)
    }

    /// Dissolve (remove) a consensus group.
    pub fn dissolve_group(&self, name: &str) -> Result<(), ConsensusError> {
        let mut groups = self.groups.write();
        if groups.remove(name).is_none() {
            return Err(ConsensusError::GroupNotFound(name.to_string()));
        }
        self.entries.write().remove(name);
        Ok(())
    }

    /// Propose a key-value pair to the consensus group. Returns the epoch.
    pub fn propose(
        &self,
        group: &str,
        key: &str,
        value: &[u8],
    ) -> Result<u64, ConsensusError> {
        let start = Instant::now();
        let mut groups = self.groups.write();
        let g = groups
            .get_mut(group)
            .ok_or_else(|| ConsensusError::GroupNotFound(group.to_string()))?;

        if g.state == GroupState::Dissolved {
            return Err(ConsensusError::GroupNotFound(group.to_string()));
        }

        // Single-node: always the leader, commit immediately.
        g.epoch += 1;
        g.data.insert(key.to_string(), value.to_vec());

        let entry = ConsensusEntry {
            key: key.to_string(),
            value: value.to_vec(),
            epoch: g.epoch,
            proposed_by: self.local_node.clone(),
            timestamp: Self::now_secs(),
        };

        let epoch = g.epoch;
        drop(groups);

        // Trim log if needed.
        let mut entries = self.entries.write();
        let log = entries.entry(group.to_string()).or_default();
        log.push(entry);
        if log.len() > self.config.max_log_entries {
            let excess = log.len() - self.config.max_log_entries;
            log.drain(..excess);
        }

        self.stats.total_proposals.fetch_add(1, Ordering::Relaxed);
        let elapsed_us = start.elapsed().as_micros() as u64;
        self.stats
            .total_latency_us
            .fetch_add(elapsed_us, Ordering::Relaxed);

        Ok(epoch)
    }

    /// Linearizable read of a key from a consensus group.
    pub fn read(
        &self,
        group: &str,
        key: &str,
    ) -> Result<Option<ConsensusEntry>, ConsensusError> {
        self.stats.total_reads.fetch_add(1, Ordering::Relaxed);
        let groups = self.groups.read();
        let g = groups
            .get(group)
            .ok_or_else(|| ConsensusError::GroupNotFound(group.to_string()))?;

        if let Some(value) = g.data.get(key) {
            Ok(Some(ConsensusEntry {
                key: key.to_string(),
                value: value.clone(),
                epoch: g.epoch,
                proposed_by: self.local_node.clone(),
                timestamp: Self::now_secs(),
            }))
        } else {
            Ok(None)
        }
    }

    /// Return the current leader of a consensus group.
    pub fn leader(&self, group: &str) -> Result<Option<String>, ConsensusError> {
        let groups = self.groups.read();
        let g = groups
            .get(group)
            .ok_or_else(|| ConsensusError::GroupNotFound(group.to_string()))?;
        Ok(g.leader.clone())
    }

    /// Trigger an election in the group. Returns the new leader.
    pub fn elect(&self, group: &str) -> Result<String, ConsensusError> {
        self.stats.elections.fetch_add(1, Ordering::Relaxed);
        let mut groups = self.groups.write();
        let g = groups
            .get_mut(group)
            .ok_or_else(|| ConsensusError::GroupNotFound(group.to_string()))?;

        // Single-node: local node always wins.
        g.state = GroupState::Active;
        g.leader = Some(self.local_node.clone());
        g.epoch += 1;
        Ok(self.local_node.clone())
    }

    /// Return the members of a consensus group.
    pub fn members(&self, group: &str) -> Result<Vec<String>, ConsensusError> {
        let groups = self.groups.read();
        let g = groups
            .get(group)
            .ok_or_else(|| ConsensusError::GroupNotFound(group.to_string()))?;
        Ok(g.members.clone())
    }

    /// Add a member to a consensus group.
    pub fn add_member(&self, group: &str, member: &str) -> Result<(), ConsensusError> {
        let mut groups = self.groups.write();
        let g = groups
            .get_mut(group)
            .ok_or_else(|| ConsensusError::GroupNotFound(group.to_string()))?;

        if !g.members.contains(&member.to_string()) {
            g.members.push(member.to_string());
        }
        Ok(())
    }

    /// Remove a member from a consensus group.
    pub fn remove_member(&self, group: &str, member: &str) -> Result<(), ConsensusError> {
        let mut groups = self.groups.write();
        let g = groups
            .get_mut(group)
            .ok_or_else(|| ConsensusError::GroupNotFound(group.to_string()))?;

        let pos = g
            .members
            .iter()
            .position(|m| m == member)
            .ok_or_else(|| {
                ConsensusError::MemberNotFound(member.to_string(), group.to_string())
            })?;
        g.members.remove(pos);
        Ok(())
    }

    /// Return summary information about a consensus group.
    pub fn group_info(&self, name: &str) -> Option<GroupInfo> {
        let groups = self.groups.read();
        let g = groups.get(name)?;
        let entries = self.entries.read();
        let entry_count = entries.get(name).map(|e| e.len()).unwrap_or(0);
        Some(GroupInfo {
            name: g.name.clone(),
            state: g.state,
            leader: g.leader.clone(),
            members_count: g.members.len(),
            epoch: g.epoch,
            entries: entry_count,
            created_at: g.created_at,
        })
    }

    /// List all consensus groups.
    pub fn list_groups(&self) -> Vec<GroupInfo> {
        let groups = self.groups.read();
        let entries = self.entries.read();
        groups
            .values()
            .map(|g| {
                let entry_count = entries.get(&g.name).map(|e| e.len()).unwrap_or(0);
                GroupInfo {
                    name: g.name.clone(),
                    state: g.state,
                    leader: g.leader.clone(),
                    members_count: g.members.len(),
                    epoch: g.epoch,
                    entries: entry_count,
                    created_at: g.created_at,
                }
            })
            .collect()
    }

    /// Return aggregate statistics for the consensus service.
    pub fn stats(&self) -> ConsensusStats {
        let groups = self.groups.read();
        let proposals = self.stats.total_proposals.load(Ordering::Relaxed);
        let latency = self.stats.total_latency_us.load(Ordering::Relaxed);
        let avg = if proposals > 0 {
            latency as f64 / proposals as f64
        } else {
            0.0
        };
        ConsensusStats {
            groups: groups.len(),
            total_proposals: proposals,
            total_reads: self.stats.total_reads.load(Ordering::Relaxed),
            elections: self.stats.elections.load(Ordering::Relaxed),
            avg_latency_us: avg,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_manager() -> ConsensusManager {
        ConsensusManager::new(ConsensusConfig::default())
    }

    #[test]
    fn test_create_group() {
        let mgr = default_manager();
        let group = mgr
            .create_group("test-group", vec!["node-a".into(), "node-b".into()])
            .expect("create should succeed");
        assert_eq!(group.name, "test-group");
        assert_eq!(group.state, GroupState::Active);
        assert!(group.leader.is_some());
        assert_eq!(group.members.len(), 2);
    }

    #[test]
    fn test_create_duplicate_group() {
        let mgr = default_manager();
        mgr.create_group("dup", vec![]).expect("first create");
        let err = mgr.create_group("dup", vec![]).unwrap_err();
        assert!(matches!(err, ConsensusError::GroupExists(_)));
    }

    #[test]
    fn test_dissolve_group() {
        let mgr = default_manager();
        mgr.create_group("to-remove", vec![]).expect("create");
        mgr.dissolve_group("to-remove").expect("dissolve");
        assert!(mgr.group_info("to-remove").is_none());
    }

    #[test]
    fn test_propose_and_read() {
        let mgr = default_manager();
        mgr.create_group("kv", vec![]).expect("create");
        let epoch = mgr.propose("kv", "key1", b"value1").expect("propose");
        assert!(epoch > 0);

        let entry = mgr.read("kv", "key1").expect("read").expect("entry");
        assert_eq!(entry.key, "key1");
        assert_eq!(entry.value, b"value1");
        assert_eq!(entry.epoch, epoch);
    }

    #[test]
    fn test_read_missing_key() {
        let mgr = default_manager();
        mgr.create_group("empty", vec![]).expect("create");
        let result = mgr.read("empty", "nope").expect("read");
        assert!(result.is_none());
    }

    #[test]
    fn test_leader_and_elect() {
        let mgr = default_manager();
        mgr.create_group("election", vec![]).expect("create");
        let leader = mgr.leader("election").expect("leader").expect("some");
        assert!(!leader.is_empty());

        let new_leader = mgr.elect("election").expect("elect");
        assert_eq!(new_leader, leader);
    }

    #[test]
    fn test_add_and_remove_member() {
        let mgr = default_manager();
        mgr.create_group("membership", vec!["node-1".into()])
            .expect("create");
        mgr.add_member("membership", "node-2").expect("add");
        let members = mgr.members("membership").expect("members");
        assert_eq!(members.len(), 2);

        mgr.remove_member("membership", "node-2").expect("remove");
        let members = mgr.members("membership").expect("members");
        assert_eq!(members.len(), 1);
    }

    #[test]
    fn test_remove_nonexistent_member() {
        let mgr = default_manager();
        mgr.create_group("rmtest", vec!["a".into()]).expect("create");
        let err = mgr.remove_member("rmtest", "ghost").unwrap_err();
        assert!(matches!(err, ConsensusError::MemberNotFound(_, _)));
    }

    #[test]
    fn test_list_groups_and_stats() {
        let mgr = default_manager();
        mgr.create_group("g1", vec![]).expect("create g1");
        mgr.create_group("g2", vec![]).expect("create g2");

        let list = mgr.list_groups();
        assert_eq!(list.len(), 2);

        mgr.propose("g1", "a", b"1").expect("propose");
        let stats = mgr.stats();
        assert_eq!(stats.groups, 2);
        assert_eq!(stats.total_proposals, 1);
    }

    #[test]
    fn test_max_groups_limit() {
        let mgr = ConsensusManager::new(ConsensusConfig {
            max_groups: 2,
            ..Default::default()
        });
        mgr.create_group("g1", vec![]).expect("g1");
        mgr.create_group("g2", vec![]).expect("g2");
        let err = mgr.create_group("g3", vec![]).unwrap_err();
        assert!(matches!(err, ConsensusError::MaxGroups(2)));
    }

    #[test]
    fn test_operations_on_missing_group() {
        let mgr = default_manager();
        assert!(mgr.propose("no-group", "k", b"v").is_err());
        assert!(mgr.read("no-group", "k").is_err());
        assert!(mgr.leader("no-group").is_err());
        assert!(mgr.elect("no-group").is_err());
        assert!(mgr.members("no-group").is_err());
        assert!(mgr.dissolve_group("no-group").is_err());
    }
}
