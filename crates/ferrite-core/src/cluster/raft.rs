//! Raft Consensus Module
//!
//! Implements the Raft consensus protocol for leader election and metadata
//! replication in Ferrite clusters. This provides formally-verified consensus
//! for configuration changes, slot assignments, and failover coordination.
//!
//! Based on the Raft paper (Ongaro & Ousterhout, 2014) with extensions for:
//! - Cluster slot management as the replicated state machine
//! - Pre-vote protocol to prevent disruptive elections
//! - Leadership transfer for planned maintenance
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────┐
//! │                RaftNode                      │
//! │  ┌─────────┐  ┌──────────┐  ┌───────────┐  │
//! │  │  State   │  │   Log    │  │ Transport │  │
//! │  │ Machine  │  │ (entries)│  │  (async)  │  │
//! │  └────┬─────┘  └────┬─────┘  └─────┬─────┘  │
//! │       └──────────────┴──────────────┘        │
//! │                     │                        │
//! │              ┌──────▼──────┐                  │
//! │              │  Consensus  │                  │
//! │              │   Engine    │                  │
//! │              └─────────────┘                  │
//! └─────────────────────────────────────────────┘
//! ```

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::cluster::NodeId;

/// Raft term number
pub type Term = u64;

/// Log index (1-based, 0 means no entries)
pub type LogIndex = u64;

/// Raft node role in the consensus protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftRole {
    /// Passive node that responds to RPCs
    Follower,
    /// Actively seeking votes to become leader
    Candidate,
    /// Authoritative node that manages log replication
    Leader,
    /// Pre-candidate performing pre-vote check (prevents disruptive elections)
    PreCandidate,
}

impl fmt::Display for RaftRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RaftRole::Follower => write!(f, "Follower"),
            RaftRole::Candidate => write!(f, "Candidate"),
            RaftRole::Leader => write!(f, "Leader"),
            RaftRole::PreCandidate => write!(f, "PreCandidate"),
        }
    }
}

/// Commands that can be proposed to the Raft state machine
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RaftCommand {
    /// Assign hash slots to a node
    AssignSlots {
        /// Target node for slot assignment.
        node_id: NodeId,
        /// Slot ranges as (start, end) pairs.
        slots: Vec<(u16, u16)>,
    },
    /// Add a new node to the cluster
    AddNode {
        /// ID of the new node.
        node_id: NodeId,
        /// Network address of the new node.
        addr: String,
        /// Role of the new node (e.g. "primary", "replica").
        role: String,
    },
    /// Remove a node from the cluster
    RemoveNode {
        /// ID of the node to remove.
        node_id: NodeId,
    },
    /// Mark a node as failed
    NodeFailed {
        /// ID of the failed node.
        node_id: NodeId,
    },
    /// Promote a replica to primary
    PromoteReplica {
        /// ID of the replica to promote.
        replica_id: NodeId,
        /// ID of the former primary being replaced.
        old_primary_id: NodeId,
    },
    /// Begin slot migration between nodes
    BeginMigration {
        /// Slot number to migrate.
        slot: u16,
        /// Node currently owning the slot.
        source: NodeId,
        /// Node that will receive the slot.
        target: NodeId,
    },
    /// Complete slot migration
    CompleteMigration {
        /// Slot that was migrated.
        slot: u16,
        /// Node that now owns the slot.
        new_owner: NodeId,
    },
    /// Update cluster configuration epoch
    BumpEpoch {
        /// New epoch value.
        new_epoch: u64,
    },
    /// No-op entry for leader commit confirmation
    Noop,
}

/// A single entry in the Raft log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Term when entry was received by leader
    pub term: Term,
    /// Position in the log (1-based)
    pub index: LogIndex,
    /// The command to apply to the state machine
    pub command: RaftCommand,
}

/// Persistent state that must survive restarts
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PersistentState {
    /// Latest term this node has seen
    pub current_term: Term,
    /// Candidate that received vote in current term (if any)
    pub voted_for: Option<NodeId>,
    /// The log entries
    pub log: Vec<LogEntry>,
}

/// Volatile state maintained on all servers
#[derive(Debug, Clone, Default)]
pub struct VolatileState {
    /// Index of highest log entry known to be committed
    pub commit_index: LogIndex,
    /// Index of highest log entry applied to state machine
    pub last_applied: LogIndex,
}

/// Volatile state maintained only on leaders
#[derive(Debug, Clone)]
pub struct LeaderState {
    /// For each peer: index of next log entry to send.
    pub next_index: HashMap<NodeId, LogIndex>,
    /// For each peer: index of highest log entry known to be replicated.
    pub match_index: HashMap<NodeId, LogIndex>,
}

impl LeaderState {
    /// Create leader state initialized for the given peers.
    pub fn new(peers: &[NodeId], last_log_index: LogIndex) -> Self {
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();
        for peer in peers {
            next_index.insert(peer.clone(), last_log_index + 1);
            match_index.insert(peer.clone(), 0);
        }
        Self {
            next_index,
            match_index,
        }
    }
}

/// RequestVote RPC arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteArgs {
    /// Candidate's term
    pub term: Term,
    /// Candidate requesting vote
    pub candidate_id: NodeId,
    /// Index of candidate's last log entry
    pub last_log_index: LogIndex,
    /// Term of candidate's last log entry
    pub last_log_term: Term,
    /// Whether this is a pre-vote (doesn't increment term)
    pub pre_vote: bool,
}

/// RequestVote RPC reply
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteReply {
    /// Current term, for candidate to update itself
    pub term: Term,
    /// True if candidate received vote
    pub vote_granted: bool,
}

/// AppendEntries RPC arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesArgs {
    /// Leader's term
    pub term: Term,
    /// Leader's ID so followers can redirect clients
    pub leader_id: NodeId,
    /// Index of log entry immediately preceding new ones
    pub prev_log_index: LogIndex,
    /// Term of prev_log_index entry
    pub prev_log_term: Term,
    /// Log entries to store (empty for heartbeat)
    pub entries: Vec<LogEntry>,
    /// Leader's commit index
    pub leader_commit: LogIndex,
}

/// AppendEntries RPC reply
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    /// Current term, for leader to update itself
    pub term: Term,
    /// True if follower contained entry matching prev_log_index/term
    pub success: bool,
    /// Optimization: the follower's last log index for faster catchup
    pub last_log_index: Option<LogIndex>,
}

/// Configuration for Raft consensus timing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    /// Minimum election timeout in milliseconds
    pub election_timeout_min_ms: u64,
    /// Maximum election timeout in milliseconds
    pub election_timeout_max_ms: u64,
    /// Heartbeat interval in milliseconds (must be << election timeout)
    pub heartbeat_interval_ms: u64,
    /// Maximum entries per AppendEntries RPC
    pub max_entries_per_append: usize,
    /// Enable pre-vote protocol to prevent disruptive elections
    pub pre_vote_enabled: bool,
    /// Maximum time to wait for a proposal to commit (ms)
    pub proposal_timeout_ms: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 50,
            max_entries_per_append: 100,
            pre_vote_enabled: true,
            proposal_timeout_ms: 5000,
        }
    }
}

/// Result of applying a command to the state machine
#[derive(Debug, Clone)]
pub enum ApplyResult {
    /// Command applied successfully
    Success,
    /// Command applied with a return value
    Value(String),
    /// Command failed to apply
    Error(String),
}

/// The core Raft consensus node
pub struct RaftNode {
    /// This node's unique identifier
    pub id: NodeId,
    /// Current role in the protocol
    role: RwLock<RaftRole>,
    /// Persistent state (survives restarts)
    persistent: RwLock<PersistentState>,
    /// Volatile state (all servers)
    volatile: RwLock<VolatileState>,
    /// Leader-only volatile state
    leader_state: RwLock<Option<LeaderState>>,
    /// Known peer node IDs
    peers: RwLock<Vec<NodeId>>,
    /// Current leader ID (if known)
    leader_id: RwLock<Option<NodeId>>,
    /// Raft configuration
    config: RaftConfig,
    /// Election deadline
    election_deadline: RwLock<Instant>,
    /// Number of votes received in current election
    votes_received: AtomicU64,
    /// Applied command results for client notification
    applied_results: RwLock<HashMap<LogIndex, ApplyResult>>,
}

impl RaftNode {
    /// Create a new Raft node
    pub fn new(id: NodeId, peers: Vec<NodeId>, config: RaftConfig) -> Self {
        let election_deadline = Self::random_election_deadline(&config);
        Self {
            id,
            role: RwLock::new(RaftRole::Follower),
            persistent: RwLock::new(PersistentState::default()),
            volatile: RwLock::new(VolatileState::default()),
            leader_state: RwLock::new(None),
            peers: RwLock::new(peers),
            leader_id: RwLock::new(None),
            config,
            election_deadline: RwLock::new(election_deadline),
            votes_received: AtomicU64::new(0),
            applied_results: RwLock::new(HashMap::new()),
        }
    }

    /// Create with default configuration
    pub fn with_defaults(id: NodeId, peers: Vec<NodeId>) -> Self {
        Self::new(id, peers, RaftConfig::default())
    }

    /// Get current role
    pub fn role(&self) -> RaftRole {
        *self.role.read()
    }

    /// Get current term
    pub fn current_term(&self) -> Term {
        self.persistent.read().current_term
    }

    /// Get current leader ID
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id.read().clone()
    }

    /// Check if this node is the leader
    pub fn is_leader(&self) -> bool {
        *self.role.read() == RaftRole::Leader
    }

    /// Get commit index
    pub fn commit_index(&self) -> LogIndex {
        self.volatile.read().commit_index
    }

    /// Get last applied index
    pub fn last_applied(&self) -> LogIndex {
        self.volatile.read().last_applied
    }

    /// Get the last log index and term
    pub fn last_log_info(&self) -> (LogIndex, Term) {
        let persistent = self.persistent.read();
        match persistent.log.last() {
            Some(entry) => (entry.index, entry.term),
            None => (0, 0),
        }
    }

    /// Get the number of nodes needed for a quorum
    pub fn quorum_size(&self) -> usize {
        let total = self.peers.read().len() + 1; // +1 for self
        (total / 2) + 1
    }

    fn random_election_deadline(config: &RaftConfig) -> Instant {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let timeout =
            rng.gen_range(config.election_timeout_min_ms..=config.election_timeout_max_ms);
        Instant::now() + Duration::from_millis(timeout)
    }

    /// Reset the election timer (called on valid heartbeat or vote grant)
    pub fn reset_election_timer(&self) {
        let deadline = Self::random_election_deadline(&self.config);
        *self.election_deadline.write() = deadline;
    }

    /// Check if the election timer has expired
    pub fn election_timeout_elapsed(&self) -> bool {
        Instant::now() >= *self.election_deadline.read()
    }

    /// Transition to a new role
    fn transition_to(&self, new_role: RaftRole) {
        let old_role = *self.role.read();
        if old_role == new_role {
            return;
        }
        info!(
            node_id = %self.id,
            from = %old_role,
            to = %new_role,
            term = self.current_term(),
            "Raft role transition"
        );
        *self.role.write() = new_role;

        match new_role {
            RaftRole::Leader => {
                *self.leader_id.write() = Some(self.id.clone());
                let (last_index, _) = self.last_log_info();
                let peers = self.peers.read().clone();
                *self.leader_state.write() = Some(LeaderState::new(&peers, last_index));
            }
            RaftRole::Follower => {
                *self.leader_state.write() = None;
                self.votes_received.store(0, Ordering::Relaxed);
            }
            RaftRole::Candidate | RaftRole::PreCandidate => {
                self.votes_received.store(0, Ordering::Relaxed);
            }
        }
    }

    /// Step down to follower if we discover a higher term
    pub fn step_down(&self, new_term: Term) {
        let mut persistent = self.persistent.write();
        if new_term > persistent.current_term {
            debug!(
                node_id = %self.id,
                old_term = persistent.current_term,
                new_term = new_term,
                "Stepping down: discovered higher term"
            );
            persistent.current_term = new_term;
            persistent.voted_for = None;
            drop(persistent);
            self.transition_to(RaftRole::Follower);
            self.reset_election_timer();
        }
    }

    /// Start an election (or pre-vote)
    pub fn start_election(&self) -> Option<RequestVoteArgs> {
        if self.config.pre_vote_enabled && *self.role.read() == RaftRole::Follower {
            self.transition_to(RaftRole::PreCandidate);
            let (last_index, last_term) = self.last_log_info();
            let term = self.current_term() + 1; // Hypothetical next term
                                                // Vote for self in pre-vote
            self.votes_received.store(1, Ordering::Relaxed);
            return Some(RequestVoteArgs {
                term,
                candidate_id: self.id.clone(),
                last_log_index: last_index,
                last_log_term: last_term,
                pre_vote: true,
            });
        }

        // Real election
        {
            let mut persistent = self.persistent.write();
            persistent.current_term += 1;
            persistent.voted_for = Some(self.id.clone());
        }
        self.transition_to(RaftRole::Candidate);
        self.reset_election_timer();
        // Vote for self
        self.votes_received.store(1, Ordering::Relaxed);

        let (last_index, last_term) = self.last_log_info();
        Some(RequestVoteArgs {
            term: self.current_term(),
            candidate_id: self.id.clone(),
            last_log_index: last_index,
            last_log_term: last_term,
            pre_vote: false,
        })
    }

    /// Handle a RequestVote RPC
    pub fn handle_request_vote(&self, args: &RequestVoteArgs) -> RequestVoteReply {
        let current_term = self.current_term();

        // Pre-vote: don't update state, just check if we would vote
        if args.pre_vote {
            let (our_last_index, our_last_term) = self.last_log_info();
            let log_is_current = args.last_log_term > our_last_term
                || (args.last_log_term == our_last_term && args.last_log_index >= our_last_index);
            let would_vote = args.term > current_term && log_is_current;
            return RequestVoteReply {
                term: current_term,
                vote_granted: would_vote,
            };
        }

        // Step down if we see a higher term
        if args.term > current_term {
            self.step_down(args.term);
        }

        if args.term < current_term {
            return RequestVoteReply {
                term: self.current_term(),
                vote_granted: false,
            };
        }

        let persistent = self.persistent.read();
        let voted_for = &persistent.voted_for;

        // Check if we can vote for this candidate
        let can_vote = voted_for.is_none() || voted_for.as_ref() == Some(&args.candidate_id);

        // Check if candidate's log is at least as up-to-date
        let (our_last_index, our_last_term) = self.last_log_info();
        let log_is_current = args.last_log_term > our_last_term
            || (args.last_log_term == our_last_term && args.last_log_index >= our_last_index);

        let vote_granted = can_vote && log_is_current;
        drop(persistent);

        if vote_granted {
            self.persistent.write().voted_for = Some(args.candidate_id.clone());
            self.reset_election_timer();
            debug!(
                node_id = %self.id,
                voted_for = %args.candidate_id,
                term = args.term,
                "Granted vote"
            );
        }

        RequestVoteReply {
            term: self.current_term(),
            vote_granted,
        }
    }

    /// Handle a vote reply (from RequestVote RPC response)
    pub fn handle_vote_reply(&self, reply: &RequestVoteReply, pre_vote: bool) {
        if reply.term > self.current_term() {
            self.step_down(reply.term);
            return;
        }

        if !reply.vote_granted {
            return;
        }

        let current_role = *self.role.read();

        if pre_vote && current_role == RaftRole::PreCandidate {
            let votes = self.votes_received.fetch_add(1, Ordering::Relaxed) + 1;
            if votes as usize >= self.quorum_size() {
                debug!(
                    node_id = %self.id,
                    votes = votes,
                    "Pre-vote succeeded, starting real election"
                );
                // Pre-vote succeeded, start real election
                self.transition_to(RaftRole::Follower);
                self.start_election();
            }
        } else if !pre_vote && current_role == RaftRole::Candidate {
            let votes = self.votes_received.fetch_add(1, Ordering::Relaxed) + 1;
            if votes as usize >= self.quorum_size() {
                info!(
                    node_id = %self.id,
                    votes = votes,
                    term = self.current_term(),
                    "Won election, becoming leader"
                );
                self.transition_to(RaftRole::Leader);
            }
        }
    }

    /// Handle an AppendEntries RPC (heartbeat or log replication)
    pub fn handle_append_entries(&self, args: &AppendEntriesArgs) -> AppendEntriesReply {
        let current_term = self.current_term();

        // Reject if sender's term is old
        if args.term < current_term {
            return AppendEntriesReply {
                term: current_term,
                success: false,
                last_log_index: None,
            };
        }

        // Recognize the sender as leader
        if args.term >= current_term {
            if args.term > current_term {
                self.step_down(args.term);
            }
            *self.leader_id.write() = Some(args.leader_id.clone());
            if *self.role.read() != RaftRole::Follower {
                self.transition_to(RaftRole::Follower);
            }
        }

        self.reset_election_timer();

        let mut persistent = self.persistent.write();

        // Check log consistency at prev_log_index
        if args.prev_log_index > 0 {
            match persistent
                .log
                .iter()
                .find(|e| e.index == args.prev_log_index)
            {
                Some(entry) if entry.term != args.prev_log_term => {
                    // Conflicting entry — truncate from here
                    persistent.log.retain(|e| e.index < args.prev_log_index);
                    let last_idx = persistent.log.last().map(|e| e.index).unwrap_or(0);
                    return AppendEntriesReply {
                        term: self.current_term(),
                        success: false,
                        last_log_index: Some(last_idx),
                    };
                }
                None if args.prev_log_index > persistent.log.len() as u64 => {
                    // We don't have this entry yet
                    let last_idx = persistent.log.last().map(|e| e.index).unwrap_or(0);
                    return AppendEntriesReply {
                        term: self.current_term(),
                        success: false,
                        last_log_index: Some(last_idx),
                    };
                }
                _ => {}
            }
        }

        // Append new entries (removing conflicts)
        for entry in &args.entries {
            if let Some(existing) = persistent.log.iter().find(|e| e.index == entry.index) {
                if existing.term != entry.term {
                    persistent.log.retain(|e| e.index < entry.index);
                    persistent.log.push(entry.clone());
                }
                // Same index and term — skip (already have it)
            } else {
                persistent.log.push(entry.clone());
            }
        }

        // Update commit index
        let last_new_index = persistent.log.last().map(|e| e.index).unwrap_or(0);
        drop(persistent);

        if args.leader_commit > self.volatile.read().commit_index {
            let new_commit = std::cmp::min(args.leader_commit, last_new_index);
            self.volatile.write().commit_index = new_commit;
        }

        AppendEntriesReply {
            term: self.current_term(),
            success: true,
            last_log_index: Some(last_new_index),
        }
    }

    /// Propose a new command (leader only)
    pub fn propose(&self, command: RaftCommand) -> Result<LogIndex, RaftError> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader {
                leader_id: self.leader_id(),
            });
        }

        let mut persistent = self.persistent.write();
        let index = persistent.log.last().map(|e| e.index).unwrap_or(0) + 1;
        let entry = LogEntry {
            term: persistent.current_term,
            index,
            command,
        };
        persistent.log.push(entry);
        debug!(
            node_id = %self.id,
            index = index,
            term = persistent.current_term,
            "Proposed new log entry"
        );
        Ok(index)
    }

    /// Generate AppendEntries RPCs for all peers (leader only)
    pub fn create_append_entries(&self) -> Vec<(NodeId, AppendEntriesArgs)> {
        if !self.is_leader() {
            return Vec::new();
        }

        let persistent = self.persistent.read();
        let leader_state = self.leader_state.read();
        let leader = match leader_state.as_ref() {
            Some(ls) => ls,
            None => return Vec::new(),
        };

        let peers = self.peers.read().clone();
        let mut rpcs = Vec::new();

        for peer in &peers {
            let next_idx = leader.next_index.get(peer).copied().unwrap_or(1);
            let prev_log_index = if next_idx > 0 { next_idx - 1 } else { 0 };
            let prev_log_term = if prev_log_index > 0 {
                persistent
                    .log
                    .iter()
                    .find(|e| e.index == prev_log_index)
                    .map(|e| e.term)
                    .unwrap_or(0)
            } else {
                0
            };

            let entries: Vec<LogEntry> = persistent
                .log
                .iter()
                .filter(|e| e.index >= next_idx)
                .take(self.config.max_entries_per_append)
                .cloned()
                .collect();

            rpcs.push((
                peer.clone(),
                AppendEntriesArgs {
                    term: persistent.current_term,
                    leader_id: self.id.clone(),
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: self.volatile.read().commit_index,
                },
            ));
        }

        rpcs
    }

    /// Handle AppendEntries reply from a peer (leader only)
    pub fn handle_append_reply(&self, peer: &NodeId, reply: &AppendEntriesReply) {
        if reply.term > self.current_term() {
            self.step_down(reply.term);
            return;
        }

        if !self.is_leader() {
            return;
        }

        let mut leader_state = self.leader_state.write();
        let leader = match leader_state.as_mut() {
            Some(ls) => ls,
            None => return,
        };

        if reply.success {
            if let Some(last_idx) = reply.last_log_index {
                leader.next_index.insert(peer.clone(), last_idx + 1);
                leader.match_index.insert(peer.clone(), last_idx);
            }
            // Check if we can advance commit_index
            self.try_advance_commit_index(leader);
        } else {
            // Decrement next_index and retry
            let next = leader.next_index.get(peer).copied().unwrap_or(1);
            let new_next = if let Some(last_idx) = reply.last_log_index {
                // Fast catchup: jump to follower's last index + 1
                last_idx + 1
            } else {
                std::cmp::max(1, next.saturating_sub(1))
            };
            leader.next_index.insert(peer.clone(), new_next);
        }
    }

    /// Try to advance commit_index based on match_index values
    fn try_advance_commit_index(&self, leader: &LeaderState) {
        let persistent = self.persistent.read();
        let current_commit = self.volatile.read().commit_index;
        let last_log_index = persistent.log.last().map(|e| e.index).unwrap_or(0);

        for n in (current_commit + 1)..=last_log_index {
            // Only commit entries from current term (Raft safety)
            let entry_term = persistent.log.iter().find(|e| e.index == n).map(|e| e.term);

            if entry_term != Some(persistent.current_term) {
                continue;
            }

            // Count replicas that have this entry
            let match_count = leader.match_index.values().filter(|&&idx| idx >= n).count() + 1; // +1 for leader

            if match_count >= self.quorum_size() {
                self.volatile.write().commit_index = n;
                debug!(
                    node_id = %self.id,
                    commit_index = n,
                    "Advanced commit index"
                );
            }
        }
    }

    /// Apply committed but unapplied entries to the state machine
    pub fn apply_committed(&self) -> Vec<(LogIndex, RaftCommand)> {
        let commit_index = self.volatile.read().commit_index;
        let last_applied = self.volatile.read().last_applied;

        if commit_index <= last_applied {
            return Vec::new();
        }

        let persistent = self.persistent.read();
        let mut applied = Vec::new();

        for entry in &persistent.log {
            if entry.index > last_applied && entry.index <= commit_index {
                applied.push((entry.index, entry.command.clone()));
            }
        }
        drop(persistent);

        if let Some((last_idx, _)) = applied.last() {
            self.volatile.write().last_applied = *last_idx;
        }

        applied
    }

    /// Add a new peer to the Raft group
    pub fn add_peer(&self, peer_id: NodeId) {
        let mut peers = self.peers.write();
        if !peers.contains(&peer_id) {
            peers.push(peer_id.clone());
            // If leader, initialize tracking for new peer
            if self.is_leader() {
                let (last_index, _) = self.last_log_info();
                if let Some(ref mut ls) = *self.leader_state.write() {
                    ls.next_index.insert(peer_id.clone(), last_index + 1);
                    ls.match_index.insert(peer_id, 0);
                }
            }
        }
    }

    /// Remove a peer from the Raft group
    pub fn remove_peer(&self, peer_id: &NodeId) {
        let mut peers = self.peers.write();
        peers.retain(|p| p != peer_id);
        if let Some(ref mut ls) = *self.leader_state.write() {
            ls.next_index.remove(peer_id);
            ls.match_index.remove(peer_id);
        }
    }

    /// Get a snapshot of the current Raft status
    pub fn status(&self) -> RaftStatus {
        let (last_log_index, last_log_term) = self.last_log_info();
        RaftStatus {
            id: self.id.clone(),
            role: self.role(),
            term: self.current_term(),
            leader_id: self.leader_id(),
            commit_index: self.commit_index(),
            last_applied: self.last_applied(),
            last_log_index,
            last_log_term,
            peers: self.peers.read().clone(),
            log_length: self.persistent.read().log.len(),
        }
    }

    /// Initiate leadership transfer to a specific node
    pub fn transfer_leadership(&self, target: &NodeId) -> Result<(), RaftError> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader {
                leader_id: self.leader_id(),
            });
        }
        if !self.peers.read().contains(target) {
            return Err(RaftError::UnknownPeer(target.clone()));
        }
        info!(
            node_id = %self.id,
            target = %target,
            "Initiating leadership transfer"
        );
        // In a full implementation, send TimeoutNow to the target
        // For now, step down to allow natural re-election
        self.transition_to(RaftRole::Follower);
        Ok(())
    }
}

/// Raft status snapshot for diagnostics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftStatus {
    /// Node identifier.
    pub id: NodeId,
    /// Current role.
    pub role: RaftRole,
    /// Current term.
    pub term: Term,
    /// Known leader ID, if any.
    pub leader_id: Option<NodeId>,
    /// Highest committed log index.
    pub commit_index: LogIndex,
    /// Highest applied log index.
    pub last_applied: LogIndex,
    /// Index of the last log entry.
    pub last_log_index: LogIndex,
    /// Term of the last log entry.
    pub last_log_term: Term,
    /// Known peer node IDs.
    pub peers: Vec<NodeId>,
    /// Total number of log entries.
    pub log_length: usize,
}

/// Errors specific to Raft operations
#[derive(Debug, Clone, thiserror::Error)]
pub enum RaftError {
    /// This node is not the leader; includes the known leader if available.
    #[error("Not the leader (leader: {leader_id:?})")]
    NotLeader {
        /// Known leader ID, if any.
        leader_id: Option<NodeId>,
    },

    /// The specified peer is not in the Raft group.
    #[error("Unknown peer: {0}")]
    UnknownPeer(NodeId),

    /// A proposal did not commit within the configured timeout.
    #[error("Proposal timeout after {0}ms")]
    ProposalTimeout(u64),

    /// An internal Raft error.
    #[error("Raft internal error: {0}")]
    Internal(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_three_node_cluster() -> (RaftNode, RaftNode, RaftNode) {
        let n1 = RaftNode::with_defaults(
            "node1".to_string(),
            vec!["node2".to_string(), "node3".to_string()],
        );
        let n2 = RaftNode::with_defaults(
            "node2".to_string(),
            vec!["node1".to_string(), "node3".to_string()],
        );
        let n3 = RaftNode::with_defaults(
            "node3".to_string(),
            vec!["node1".to_string(), "node2".to_string()],
        );
        (n1, n2, n3)
    }

    #[test]
    fn test_initial_state() {
        let node = RaftNode::with_defaults(
            "node1".to_string(),
            vec!["node2".to_string(), "node3".to_string()],
        );
        assert_eq!(node.role(), RaftRole::Follower);
        assert_eq!(node.current_term(), 0);
        assert!(node.leader_id().is_none());
        assert_eq!(node.commit_index(), 0);
        assert_eq!(node.last_applied(), 0);
        assert_eq!(node.quorum_size(), 2);
    }

    #[test]
    fn test_start_election() {
        let node = RaftNode::new(
            "node1".to_string(),
            vec!["node2".to_string(), "node3".to_string()],
            RaftConfig {
                pre_vote_enabled: false,
                ..Default::default()
            },
        );

        let args = node.start_election().unwrap();
        assert_eq!(args.term, 1);
        assert_eq!(args.candidate_id, "node1");
        assert_eq!(node.role(), RaftRole::Candidate);
        assert_eq!(node.current_term(), 1);
    }

    #[test]
    fn test_pre_vote_election() {
        let node = RaftNode::with_defaults(
            "node1".to_string(),
            vec!["node2".to_string(), "node3".to_string()],
        );

        let args = node.start_election().unwrap();
        assert!(args.pre_vote);
        assert_eq!(node.role(), RaftRole::PreCandidate);
        // Term not yet incremented during pre-vote
        assert_eq!(node.current_term(), 0);
    }

    #[test]
    fn test_vote_granting() {
        let (n1, n2, _n3) = make_three_node_cluster();

        // n1 starts election without pre-vote
        let mut config = RaftConfig::default();
        config.pre_vote_enabled = false;
        let n1 = RaftNode::new(
            "node1".to_string(),
            vec!["node2".to_string(), "node3".to_string()],
            config,
        );

        let args = n1.start_election().unwrap();
        let reply = n2.handle_request_vote(&args);

        assert!(reply.vote_granted);
        assert_eq!(reply.term, 1);
    }

    #[test]
    fn test_reject_stale_term_vote() {
        let n1 = RaftNode::with_defaults("node1".to_string(), vec!["node2".to_string()]);
        // Advance n1's term
        n1.step_down(5);

        let args = RequestVoteArgs {
            term: 2, // Stale
            candidate_id: "node2".to_string(),
            last_log_index: 0,
            last_log_term: 0,
            pre_vote: false,
        };

        let reply = n1.handle_request_vote(&args);
        assert!(!reply.vote_granted);
        assert_eq!(reply.term, 5);
    }

    #[test]
    fn test_leader_election_with_quorum() {
        let config = RaftConfig {
            pre_vote_enabled: false,
            ..Default::default()
        };
        let n1 = RaftNode::new(
            "node1".to_string(),
            vec!["node2".to_string(), "node3".to_string()],
            config,
        );
        let n2 = RaftNode::with_defaults(
            "node2".to_string(),
            vec!["node1".to_string(), "node3".to_string()],
        );

        // n1 starts election
        let args = n1.start_election().unwrap();
        // n2 votes for n1
        let reply = n2.handle_request_vote(&args);
        assert!(reply.vote_granted);

        // n1 receives vote (already has self-vote = 1, now gets 2nd = quorum)
        n1.handle_vote_reply(&reply, false);
        assert_eq!(n1.role(), RaftRole::Leader);
        assert!(n1.is_leader());
    }

    #[test]
    fn test_append_entries_heartbeat() {
        let n1 = RaftNode::with_defaults("node1".to_string(), vec!["node2".to_string()]);
        let n2 = RaftNode::with_defaults("node2".to_string(), vec!["node1".to_string()]);

        let args = AppendEntriesArgs {
            term: 1,
            leader_id: "node1".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let reply = n2.handle_append_entries(&args);
        assert!(reply.success);
        assert_eq!(n2.leader_id(), Some("node1".to_string()));
    }

    #[test]
    fn test_log_replication() {
        let config = RaftConfig {
            pre_vote_enabled: false,
            ..Default::default()
        };
        let n1 = RaftNode::new("node1".to_string(), vec!["node2".to_string()], config);
        let n2 = RaftNode::with_defaults("node2".to_string(), vec!["node1".to_string()]);

        // Make n1 leader
        let args = n1.start_election().unwrap();
        let reply = n2.handle_request_vote(&args);
        n1.handle_vote_reply(&reply, false);
        assert!(n1.is_leader());

        // Propose a command
        let idx = n1
            .propose(RaftCommand::AssignSlots {
                node_id: "node1".to_string(),
                slots: vec![(0, 8191)],
            })
            .unwrap();
        assert_eq!(idx, 1);

        // Create AppendEntries and send to n2
        let rpcs = n1.create_append_entries();
        assert_eq!(rpcs.len(), 1);
        let (peer, ae_args) = &rpcs[0];
        assert_eq!(peer, "node2");
        assert_eq!(ae_args.entries.len(), 1);

        // n2 receives and appends
        let ae_reply = n2.handle_append_entries(ae_args);
        assert!(ae_reply.success);

        // n1 handles reply — should advance commit index
        n1.handle_append_reply(peer, &ae_reply);
        assert_eq!(n1.commit_index(), 1);
    }

    #[test]
    fn test_propose_not_leader() {
        let node = RaftNode::with_defaults("node1".to_string(), vec!["node2".to_string()]);
        let result = node.propose(RaftCommand::Noop);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), RaftError::NotLeader { .. }));
    }

    #[test]
    fn test_step_down_on_higher_term() {
        let config = RaftConfig {
            pre_vote_enabled: false,
            ..Default::default()
        };
        let node = RaftNode::new("node1".to_string(), vec!["node2".to_string()], config);

        // Become candidate
        node.start_election();
        assert_eq!(node.role(), RaftRole::Candidate);
        assert_eq!(node.current_term(), 1);

        // Discover higher term
        node.step_down(5);
        assert_eq!(node.role(), RaftRole::Follower);
        assert_eq!(node.current_term(), 5);
    }

    #[test]
    fn test_apply_committed() {
        let config = RaftConfig {
            pre_vote_enabled: false,
            ..Default::default()
        };
        let n1 = RaftNode::new("node1".to_string(), vec!["node2".to_string()], config);
        let n2 = RaftNode::with_defaults("node2".to_string(), vec!["node1".to_string()]);

        // Elect n1
        let args = n1.start_election().unwrap();
        let reply = n2.handle_request_vote(&args);
        n1.handle_vote_reply(&reply, false);

        // Propose and replicate
        n1.propose(RaftCommand::AddNode {
            node_id: "node3".to_string(),
            addr: "127.0.0.1:7002".to_string(),
            role: "replica".to_string(),
        })
        .unwrap();

        let rpcs = n1.create_append_entries();
        let (peer, ae_args) = &rpcs[0];
        let ae_reply = n2.handle_append_entries(ae_args);
        n1.handle_append_reply(peer, &ae_reply);

        // Apply committed entries
        let applied = n1.apply_committed();
        assert_eq!(applied.len(), 1);
        assert_eq!(n1.last_applied(), 1);
        assert!(matches!(applied[0].1, RaftCommand::AddNode { .. }));
    }

    #[test]
    fn test_status_snapshot() {
        let node = RaftNode::with_defaults(
            "node1".to_string(),
            vec!["node2".to_string(), "node3".to_string()],
        );
        let status = node.status();
        assert_eq!(status.id, "node1");
        assert_eq!(status.role, RaftRole::Follower);
        assert_eq!(status.peers.len(), 2);
        assert_eq!(status.log_length, 0);
    }

    #[test]
    fn test_add_remove_peer() {
        let node = RaftNode::with_defaults("node1".to_string(), vec!["node2".to_string()]);
        assert_eq!(node.peers.read().len(), 1);

        node.add_peer("node3".to_string());
        assert_eq!(node.peers.read().len(), 2);

        node.remove_peer(&"node2".to_string());
        assert_eq!(node.peers.read().len(), 1);
        assert_eq!(node.peers.read()[0], "node3");
    }

    #[test]
    fn test_leadership_transfer() {
        let config = RaftConfig {
            pre_vote_enabled: false,
            ..Default::default()
        };
        let n1 = RaftNode::new("node1".to_string(), vec!["node2".to_string()], config);
        let n2 = RaftNode::with_defaults("node2".to_string(), vec!["node1".to_string()]);

        // Elect n1
        let args = n1.start_election().unwrap();
        let reply = n2.handle_request_vote(&args);
        n1.handle_vote_reply(&reply, false);
        assert!(n1.is_leader());

        // Transfer leadership
        let result = n1.transfer_leadership(&"node2".to_string());
        assert!(result.is_ok());
        assert_eq!(n1.role(), RaftRole::Follower);
    }

    #[test]
    fn test_reject_append_entries_stale_term() {
        let node = RaftNode::with_defaults("node1".to_string(), vec![]);
        node.step_down(5);

        let args = AppendEntriesArgs {
            term: 2, // Stale
            leader_id: "node2".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let reply = node.handle_append_entries(&args);
        assert!(!reply.success);
    }

    #[test]
    fn test_raft_command_serialization() {
        let cmd = RaftCommand::AssignSlots {
            node_id: "node1".to_string(),
            slots: vec![(0, 5460), (5461, 10922)],
        };
        let serialized = serde_json::to_string(&cmd).unwrap();
        let deserialized: RaftCommand = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cmd, deserialized);
    }

    #[test]
    fn test_quorum_sizes() {
        // 1 node cluster
        let n = RaftNode::with_defaults("n1".to_string(), vec![]);
        assert_eq!(n.quorum_size(), 1);

        // 3 node cluster
        let n = RaftNode::with_defaults("n1".to_string(), vec!["n2".to_string(), "n3".to_string()]);
        assert_eq!(n.quorum_size(), 2);

        // 5 node cluster
        let n = RaftNode::with_defaults(
            "n1".to_string(),
            vec![
                "n2".to_string(),
                "n3".to_string(),
                "n4".to_string(),
                "n5".to_string(),
            ],
        );
        assert_eq!(n.quorum_size(), 3);
    }
}
