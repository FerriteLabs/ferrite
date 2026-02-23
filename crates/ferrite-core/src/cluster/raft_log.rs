//! Raft Log Replication and State Machine
//!
//! Implements the append-only replicated log and state machine components
//! of the Raft consensus protocol. This module complements [`super::raft`]
//! by providing the durable log storage and deterministic state machine
//! that the consensus engine drives.
//!
//! # Architecture
//!
//! ```text
//! ┌───────────────────────────────────────────────┐
//! │                  RaftLog                       │
//! │  ┌─────────┐  ┌────────────┐  ┌────────────┐ │
//! │  │ Entries  │  │ Commit Idx │  │ Compaction  │ │
//! │  │ (append) │  │ (monotonic)│  │ (truncate)  │ │
//! │  └────┬─────┘  └─────┬──────┘  └──────┬─────┘ │
//! │       └───────────────┼────────────────┘       │
//! │                       ▼                        │
//! │              ┌────────────────┐                │
//! │              │ StateMachine   │                │
//! │              │ (apply committed│                │
//! │              │  entries)       │                │
//! │              └────────────────┘                │
//! └───────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! use ferrite_core::cluster::raft_log::{RaftLog, RaftLogConfig, RaftStateMachine, LogCommand};
//!
//! let log = RaftLog::new(RaftLogConfig::default());
//! let sm = RaftStateMachine::new();
//!
//! // Append a slot assignment
//! let entry = log.append_command(1, LogCommand::SlotAssignment {
//!     slot: 42,
//!     node_id: "node-1".to_string(),
//! }).unwrap();
//!
//! // Commit and apply
//! log.set_commit_index(entry).unwrap();
//! ```

use std::collections::HashMap;
use std::fmt;
use std::mem;
use std::ops::{Add, Sub};

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Newtypes: LogIndex & Term
// ---------------------------------------------------------------------------

/// Strongly-typed log index (1-based; 0 means "no entry").
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LogIndex(pub u64);

impl LogIndex {
    /// The sentinel value representing "no entry".
    pub const ZERO: LogIndex = LogIndex(0);

    /// Returns the raw `u64` value.
    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for LogIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Add<u64> for LogIndex {
    type Output = LogIndex;
    fn add(self, rhs: u64) -> Self::Output {
        LogIndex(self.0 + rhs)
    }
}

impl Sub<u64> for LogIndex {
    type Output = LogIndex;
    fn sub(self, rhs: u64) -> Self::Output {
        LogIndex(self.0.saturating_sub(rhs))
    }
}

impl From<u64> for LogIndex {
    fn from(v: u64) -> Self {
        LogIndex(v)
    }
}

/// Strongly-typed Raft term number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Term(pub u64);

impl Term {
    /// Term zero (no term).
    pub const ZERO: Term = Term(0);

    /// Returns the raw `u64` value.
    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for Term {
    fn from(v: u64) -> Self {
        Term(v)
    }
}

// ---------------------------------------------------------------------------
// LogCommand
// ---------------------------------------------------------------------------

/// Commands replicated through the Raft log.
///
/// These represent cluster metadata mutations that must be consistently
/// applied across all nodes in the same order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LogCommand {
    /// Assign a hash slot to a specific node.
    SlotAssignment {
        /// Hash slot number (0–16383).
        slot: u16,
        /// Node that owns this slot.
        node_id: String,
    },
    /// Migrate a slot from one node to another.
    SlotMigration {
        /// Hash slot number.
        slot: u16,
        /// Source node ID.
        from: String,
        /// Destination node ID.
        to: String,
    },
    /// A new node is joining the cluster.
    NodeJoin {
        /// ID of the joining node.
        node_id: String,
        /// Network address of the joining node.
        address: String,
    },
    /// A node is leaving the cluster.
    NodeLeave {
        /// ID of the departing node.
        node_id: String,
    },
    /// Update a cluster-wide configuration key.
    ConfigChange {
        /// Configuration key.
        key: String,
        /// New value.
        value: String,
    },
    /// Bump the cluster epoch.
    EpochBump {
        /// New epoch value.
        new_epoch: u64,
    },
    /// No-op entry used for leader commit confirmation.
    Noop,
}

// ---------------------------------------------------------------------------
// LogEntry
// ---------------------------------------------------------------------------

/// A single entry in the replicated Raft log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Position in the log (1-based).
    pub index: LogIndex,
    /// Term when the entry was created.
    pub term: Term,
    /// The command to apply to the state machine.
    pub command: LogCommand,
    /// Wall-clock timestamp when the entry was created.
    pub timestamp: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// RaftLogConfig
// ---------------------------------------------------------------------------

/// Configuration for the Raft log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftLogConfig {
    /// Maximum number of entries before rejecting appends.
    pub max_entries: usize,
    /// Number of compacted entries that triggers log compaction.
    pub compaction_threshold: usize,
    /// Interval (in entries) between automatic snapshots.
    pub snapshot_interval: usize,
}

impl Default for RaftLogConfig {
    fn default() -> Self {
        Self {
            max_entries: 100_000,
            compaction_threshold: 50_000,
            snapshot_interval: 10_000,
        }
    }
}

// ---------------------------------------------------------------------------
// LogStats
// ---------------------------------------------------------------------------

/// Statistics about the current state of the Raft log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogStats {
    /// Total entries currently stored (after compaction).
    pub total_entries: usize,
    /// Number of entries that have been committed.
    pub committed_entries: usize,
    /// Number of entries removed by compaction.
    pub compacted_entries: usize,
    /// Smallest index still present in the log.
    pub first_index: LogIndex,
    /// Largest index in the log.
    pub last_index: LogIndex,
    /// Term of the last entry.
    pub last_term: Term,
    /// Current commit index.
    pub commit_index: LogIndex,
    /// Approximate byte size of the log in memory.
    pub size_bytes: usize,
}

// ---------------------------------------------------------------------------
// RaftLogError
// ---------------------------------------------------------------------------

/// Errors produced by Raft log operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum RaftLogError {
    /// The requested index is outside the valid range.
    #[error("Index out of range: {0}")]
    IndexOutOfRange(LogIndex),

    /// Term mismatch during consistency check.
    #[error("Term mismatch at index {index}: expected {expected}, found {found}")]
    TermMismatch {
        /// Log index where the mismatch occurred.
        index: LogIndex,
        /// Expected term.
        expected: Term,
        /// Actual term found.
        found: Term,
    },

    /// The requested entry has already been compacted.
    #[error("Entry at index {0} has already been compacted")]
    AlreadyCompacted(LogIndex),

    /// The commit index is invalid (e.g. moving backwards).
    #[error("Invalid commit index: {requested} (current: {current})")]
    InvalidCommitIndex {
        /// The commit index that was requested.
        requested: LogIndex,
        /// The current commit index.
        current: LogIndex,
    },

    /// Snapshot operation failed.
    #[error("Snapshot failed: {0}")]
    SnapshotFailed(String),

    /// Catch-all internal error.
    #[error("Internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Internal log state
// ---------------------------------------------------------------------------

/// Mutable interior state of [`RaftLog`], protected by an `RwLock`.
#[derive(Debug)]
struct LogState {
    entries: Vec<LogEntry>,
    commit_index: LogIndex,
    /// Lowest index still present (entries before this were compacted).
    first_index: LogIndex,
    /// Running count of compacted entries.
    compacted_count: usize,
}

// ---------------------------------------------------------------------------
// RaftLog
// ---------------------------------------------------------------------------

/// Append-only replicated log for the Raft consensus protocol.
///
/// Thread-safe via `parking_lot::RwLock`. All indices are 1-based;
/// [`LogIndex::ZERO`] means "no entries".
pub struct RaftLog {
    state: RwLock<LogState>,
    config: RaftLogConfig,
}

impl RaftLog {
    /// Create a new, empty Raft log with the given configuration.
    pub fn new(config: RaftLogConfig) -> Self {
        Self {
            state: RwLock::new(LogState {
                entries: Vec::new(),
                commit_index: LogIndex::ZERO,
                first_index: LogIndex(1),
                compacted_count: 0,
            }),
            config,
        }
    }

    /// Append a [`LogEntry`] to the log.
    ///
    /// The entry's `index` is overwritten with the next sequential index.
    /// Returns the assigned [`LogIndex`] on success.
    pub fn append(&self, mut entry: LogEntry) -> Result<LogIndex, RaftLogError> {
        let mut state = self.state.write();
        if state.entries.len() >= self.config.max_entries {
            return Err(RaftLogError::Internal(format!(
                "Log full: {} entries (max {})",
                state.entries.len(),
                self.config.max_entries,
            )));
        }
        let next_index = if let Some(last) = state.entries.last() {
            LogIndex(last.index.0 + 1)
        } else {
            state.first_index
        };
        entry.index = next_index;
        state.entries.push(entry);
        Ok(next_index)
    }

    /// Retrieve the entry at the given index, or `None` if not present.
    pub fn get(&self, index: LogIndex) -> Option<LogEntry> {
        let state = self.state.read();
        self.resolve(&state, index)
            .map(|i| state.entries[i].clone())
    }

    /// Retrieve all entries in the half-open range `[from, to)`.
    pub fn get_range(&self, from: LogIndex, to: LogIndex) -> Vec<LogEntry> {
        let state = self.state.read();
        let mut result = Vec::new();
        let mut idx = from;
        while idx < to {
            if let Some(pos) = self.resolve(&state, idx) {
                result.push(state.entries[pos].clone());
            }
            idx = LogIndex(idx.0 + 1);
        }
        result
    }

    /// Remove all entries **after** `index`, returning the number removed.
    ///
    /// This is used when a leader discovers inconsistencies in a follower's log.
    pub fn truncate_after(&self, index: LogIndex) -> Result<usize, RaftLogError> {
        let mut state = self.state.write();
        if index < state.first_index && index != LogIndex::ZERO {
            return Err(RaftLogError::AlreadyCompacted(index));
        }
        let keep = if index == LogIndex::ZERO {
            0
        } else {
            match self.resolve(&state, index) {
                Some(pos) => pos + 1,
                None => return Err(RaftLogError::IndexOutOfRange(index)),
            }
        };
        let removed = state.entries.len() - keep;
        state.entries.truncate(keep);
        // Adjust commit index if it was past the truncation point.
        if state.commit_index > index {
            state.commit_index = index;
        }
        Ok(removed)
    }

    /// Return the index of the last entry, or [`LogIndex::ZERO`] if the log is empty.
    pub fn last_index(&self) -> LogIndex {
        let state = self.state.read();
        state
            .entries
            .last()
            .map(|e| e.index)
            .unwrap_or(LogIndex::ZERO)
    }

    /// Return the term of the last entry, or [`Term::ZERO`] if the log is empty.
    pub fn last_term(&self) -> Term {
        let state = self.state.read();
        state.entries.last().map(|e| e.term).unwrap_or(Term::ZERO)
    }

    /// Return the current commit index.
    pub fn commit_index(&self) -> LogIndex {
        self.state.read().commit_index
    }

    /// Advance the commit index.
    ///
    /// The new index must be ≥ the current commit index and ≤ the last log index.
    pub fn set_commit_index(&self, index: LogIndex) -> Result<(), RaftLogError> {
        let mut state = self.state.write();
        if index < state.commit_index {
            return Err(RaftLogError::InvalidCommitIndex {
                requested: index,
                current: state.commit_index,
            });
        }
        let last = state
            .entries
            .last()
            .map(|e| e.index)
            .unwrap_or(LogIndex::ZERO);
        if index > last {
            return Err(RaftLogError::InvalidCommitIndex {
                requested: index,
                current: state.commit_index,
            });
        }
        state.commit_index = index;
        Ok(())
    }

    /// Return all entries with index **strictly greater than** `index`.
    pub fn entries_since(&self, index: LogIndex) -> Vec<LogEntry> {
        let state = self.state.read();
        state
            .entries
            .iter()
            .filter(|e| e.index > index)
            .cloned()
            .collect()
    }

    /// Compact (discard) all entries up to and including `up_to`.
    ///
    /// Only committed entries may be compacted. Returns the number of
    /// entries removed.
    pub fn compact(&self, up_to: LogIndex) -> Result<usize, RaftLogError> {
        let mut state = self.state.write();
        if up_to > state.commit_index {
            return Err(RaftLogError::InvalidCommitIndex {
                requested: up_to,
                current: state.commit_index,
            });
        }
        if up_to < state.first_index {
            return Ok(0);
        }
        let remove_count = match self.resolve(&state, up_to) {
            Some(pos) => pos + 1,
            None => return Ok(0),
        };
        let remaining = state.entries.split_off(remove_count);
        let removed = mem::replace(&mut state.entries, remaining).len();
        state.compacted_count += removed;
        state.first_index = LogIndex(up_to.0 + 1);
        Ok(removed)
    }

    /// Return a snapshot of log statistics.
    pub fn get_stats(&self) -> LogStats {
        let state = self.state.read();
        let last_entry = state.entries.last();
        let committed = state
            .entries
            .iter()
            .filter(|e| e.index <= state.commit_index)
            .count();
        let size_bytes = state.entries.len() * mem::size_of::<LogEntry>();
        LogStats {
            total_entries: state.entries.len(),
            committed_entries: committed,
            compacted_entries: state.compacted_count,
            first_index: if state.entries.is_empty() {
                state.first_index
            } else {
                state.entries[0].index
            },
            last_index: last_entry.map(|e| e.index).unwrap_or(LogIndex::ZERO),
            last_term: last_entry.map(|e| e.term).unwrap_or(Term::ZERO),
            commit_index: state.commit_index,
            size_bytes,
        }
    }

    // -- helpers -----------------------------------------------------------

    /// Translate a [`LogIndex`] to a Vec position, accounting for compaction offset.
    fn resolve(&self, state: &LogState, index: LogIndex) -> Option<usize> {
        if index < state.first_index {
            return None;
        }
        let pos = (index.0 - state.first_index.0) as usize;
        if pos < state.entries.len() {
            Some(pos)
        } else {
            None
        }
    }
}

impl fmt::Debug for RaftLog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.read();
        f.debug_struct("RaftLog")
            .field("entries", &state.entries.len())
            .field("commit_index", &state.commit_index)
            .field("first_index", &state.first_index)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// State Machine types
// ---------------------------------------------------------------------------

/// Status of a cluster node tracked by the state machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Node is operating normally.
    Active,
    /// Node is gracefully leaving the cluster.
    Leaving,
    /// Node has been marked as failed.
    Failed,
}

impl fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeStatus::Active => write!(f, "Active"),
            NodeStatus::Leaving => write!(f, "Leaving"),
            NodeStatus::Failed => write!(f, "Failed"),
        }
    }
}

/// Information about a cluster node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Unique identifier for the node.
    pub node_id: String,
    /// Network address (e.g. `"10.0.1.5:6379"`).
    pub address: String,
    /// When the node joined the cluster.
    pub joined_at: DateTime<Utc>,
    /// Current status.
    pub status: NodeStatus,
}

/// Result of applying a single log entry to the state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyResult {
    /// Index of the applied entry.
    pub index: LogIndex,
    /// Whether the entry produced a state change.
    pub applied: bool,
    /// Human-readable description of what was applied.
    pub description: String,
}

/// Point-in-time snapshot of the state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMachineSnapshot {
    /// Slot → node-ID mapping.
    pub slot_assignments: HashMap<u16, String>,
    /// Known cluster nodes.
    pub nodes: Vec<NodeInfo>,
    /// Cluster configuration key-value pairs.
    pub config: HashMap<String, String>,
    /// Current cluster epoch.
    pub epoch: u64,
    /// Index of the last applied log entry.
    pub last_applied: LogIndex,
}

// ---------------------------------------------------------------------------
// Internal state machine state
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct StateMachineState {
    slot_assignments: HashMap<u16, String>,
    nodes: HashMap<String, NodeInfo>,
    config: HashMap<String, String>,
    epoch: u64,
    last_applied: LogIndex,
}

// ---------------------------------------------------------------------------
// RaftStateMachine
// ---------------------------------------------------------------------------

/// Deterministic state machine driven by committed Raft log entries.
///
/// Tracks slot assignments, cluster membership, configuration, and the
/// cluster epoch. All mutations are applied through [`RaftStateMachine::apply`]
/// to guarantee identical state across replicas.
pub struct RaftStateMachine {
    state: RwLock<StateMachineState>,
}

impl RaftStateMachine {
    /// Create a new, empty state machine.
    pub fn new() -> Self {
        Self {
            state: RwLock::new(StateMachineState {
                slot_assignments: HashMap::new(),
                nodes: HashMap::new(),
                config: HashMap::new(),
                epoch: 0,
                last_applied: LogIndex::ZERO,
            }),
        }
    }

    /// Apply a committed log entry, returning an [`ApplyResult`].
    ///
    /// Entries are idempotent: re-applying an already-applied index is a
    /// no-op that returns `applied: false`.
    pub fn apply(&self, entry: &LogEntry) -> Result<ApplyResult, RaftLogError> {
        let mut state = self.state.write();
        if entry.index <= state.last_applied {
            return Ok(ApplyResult {
                index: entry.index,
                applied: false,
                description: "Already applied".to_string(),
            });
        }
        let description = match &entry.command {
            LogCommand::SlotAssignment { slot, node_id } => {
                state.slot_assignments.insert(*slot, node_id.clone());
                format!("Assigned slot {} to {}", slot, node_id)
            }
            LogCommand::SlotMigration { slot, from, to } => {
                state.slot_assignments.insert(*slot, to.clone());
                format!("Migrated slot {} from {} to {}", slot, from, to)
            }
            LogCommand::NodeJoin { node_id, address } => {
                state.nodes.insert(
                    node_id.clone(),
                    NodeInfo {
                        node_id: node_id.clone(),
                        address: address.clone(),
                        joined_at: entry.timestamp,
                        status: NodeStatus::Active,
                    },
                );
                format!("Node {} joined at {}", node_id, address)
            }
            LogCommand::NodeLeave { node_id } => {
                if let Some(node) = state.nodes.get_mut(node_id) {
                    node.status = NodeStatus::Leaving;
                }
                format!("Node {} leaving", node_id)
            }
            LogCommand::ConfigChange { key, value } => {
                state.config.insert(key.clone(), value.clone());
                format!("Config {}={}", key, value)
            }
            LogCommand::EpochBump { new_epoch } => {
                state.epoch = *new_epoch;
                format!("Epoch bumped to {}", new_epoch)
            }
            LogCommand::Noop => "Noop".to_string(),
        };
        state.last_applied = entry.index;
        Ok(ApplyResult {
            index: entry.index,
            applied: true,
            description,
        })
    }

    /// Look up which node owns the given hash slot.
    pub fn get_slot_assignment(&self, slot: u16) -> Option<String> {
        self.state.read().slot_assignments.get(&slot).cloned()
    }

    /// Return a list of all known nodes.
    pub fn get_node_list(&self) -> Vec<NodeInfo> {
        self.state.read().nodes.values().cloned().collect()
    }

    /// Look up a cluster configuration value.
    pub fn get_config(&self, key: &str) -> Option<String> {
        self.state.read().config.get(key).cloned()
    }

    /// Return the current cluster epoch.
    pub fn get_current_epoch(&self) -> u64 {
        self.state.read().epoch
    }

    /// Take a point-in-time snapshot of the state machine.
    pub fn snapshot(&self) -> StateMachineSnapshot {
        let state = self.state.read();
        StateMachineSnapshot {
            slot_assignments: state.slot_assignments.clone(),
            nodes: state.nodes.values().cloned().collect(),
            config: state.config.clone(),
            epoch: state.epoch,
            last_applied: state.last_applied,
        }
    }

    /// Restore the state machine from a snapshot.
    pub fn restore(&self, snapshot: &StateMachineSnapshot) -> Result<(), RaftLogError> {
        let mut state = self.state.write();
        state.slot_assignments = snapshot.slot_assignments.clone();
        state.nodes = snapshot
            .nodes
            .iter()
            .map(|n| (n.node_id.clone(), n.clone()))
            .collect();
        state.config = snapshot.config.clone();
        state.epoch = snapshot.epoch;
        state.last_applied = snapshot.last_applied;
        Ok(())
    }
}

impl Default for RaftStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for RaftStateMachine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.read();
        f.debug_struct("RaftStateMachine")
            .field("slots", &state.slot_assignments.len())
            .field("nodes", &state.nodes.len())
            .field("epoch", &state.epoch)
            .field("last_applied", &state.last_applied)
            .finish()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- helpers -----------------------------------------------------------

    fn make_log() -> RaftLog {
        RaftLog::new(RaftLogConfig::default())
    }

    fn make_entry(term: u64, command: LogCommand) -> LogEntry {
        LogEntry {
            index: LogIndex::ZERO, // overwritten by append
            term: Term(term),
            command,
            timestamp: Utc::now(),
        }
    }

    fn append_noop(log: &RaftLog, term: u64) -> LogIndex {
        log.append(make_entry(term, LogCommand::Noop)).unwrap()
    }

    // -- LogIndex & Term ---------------------------------------------------

    #[test]
    fn log_index_arithmetic() {
        let idx = LogIndex(5);
        assert_eq!((idx + 3).0, 8);
        assert_eq!((idx - 2).0, 3);
        assert_eq!((idx - 10).0, 0); // saturating
        assert_eq!(format!("{}", idx), "5");
    }

    #[test]
    fn term_ordering() {
        assert!(Term(2) > Term(1));
        assert_eq!(Term(3), Term(3));
        assert!(Term(0) < Term(1));
        assert_eq!(format!("{}", Term(42)), "42");
    }

    // -- append & get ------------------------------------------------------

    #[test]
    fn append_and_get_entries() {
        let log = make_log();
        let i1 = log
            .append(make_entry(
                1,
                LogCommand::NodeJoin {
                    node_id: "n1".into(),
                    address: "127.0.0.1:6379".into(),
                },
            ))
            .unwrap();
        let i2 = append_noop(&log, 1);

        assert_eq!(i1, LogIndex(1));
        assert_eq!(i2, LogIndex(2));

        let e1 = log.get(i1).unwrap();
        assert_eq!(e1.term, Term(1));
        assert!(matches!(e1.command, LogCommand::NodeJoin { .. }));

        assert!(log.get(LogIndex(99)).is_none());
    }

    #[test]
    fn get_range_returns_half_open() {
        let log = make_log();
        for t in 1..=5 {
            append_noop(&log, t);
        }
        let range = log.get_range(LogIndex(2), LogIndex(4));
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].index, LogIndex(2));
        assert_eq!(range[1].index, LogIndex(3));
    }

    #[test]
    fn entries_since_returns_tail() {
        let log = make_log();
        for t in 1..=5 {
            append_noop(&log, t);
        }
        let tail = log.entries_since(LogIndex(3));
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].index, LogIndex(4));
        assert_eq!(tail[1].index, LogIndex(5));
    }

    #[test]
    fn last_index_and_term() {
        let log = make_log();
        assert_eq!(log.last_index(), LogIndex::ZERO);
        assert_eq!(log.last_term(), Term::ZERO);

        append_noop(&log, 3);
        assert_eq!(log.last_index(), LogIndex(1));
        assert_eq!(log.last_term(), Term(3));

        append_noop(&log, 5);
        assert_eq!(log.last_index(), LogIndex(2));
        assert_eq!(log.last_term(), Term(5));
    }

    // -- truncation --------------------------------------------------------

    #[test]
    fn truncate_after_removes_tail() {
        let log = make_log();
        for _ in 0..5 {
            append_noop(&log, 1);
        }
        let removed = log.truncate_after(LogIndex(3)).unwrap();
        assert_eq!(removed, 2);
        assert_eq!(log.last_index(), LogIndex(3));
        assert!(log.get(LogIndex(4)).is_none());
    }

    #[test]
    fn truncate_after_zero_clears_log() {
        let log = make_log();
        append_noop(&log, 1);
        append_noop(&log, 1);
        let removed = log.truncate_after(LogIndex::ZERO).unwrap();
        assert_eq!(removed, 2);
        assert_eq!(log.last_index(), LogIndex::ZERO);
    }

    #[test]
    fn truncate_adjusts_commit_index() {
        let log = make_log();
        for _ in 0..5 {
            append_noop(&log, 1);
        }
        log.set_commit_index(LogIndex(4)).unwrap();
        log.truncate_after(LogIndex(2)).unwrap();
        assert_eq!(log.commit_index(), LogIndex(2));
    }

    // -- commit index ------------------------------------------------------

    #[test]
    fn commit_index_advances_monotonically() {
        let log = make_log();
        for _ in 0..5 {
            append_noop(&log, 1);
        }
        log.set_commit_index(LogIndex(3)).unwrap();
        assert_eq!(log.commit_index(), LogIndex(3));

        log.set_commit_index(LogIndex(5)).unwrap();
        assert_eq!(log.commit_index(), LogIndex(5));
    }

    #[test]
    fn commit_index_cannot_go_backwards() {
        let log = make_log();
        for _ in 0..5 {
            append_noop(&log, 1);
        }
        log.set_commit_index(LogIndex(3)).unwrap();
        let err = log.set_commit_index(LogIndex(1)).unwrap_err();
        assert!(matches!(err, RaftLogError::InvalidCommitIndex { .. }));
    }

    #[test]
    fn commit_index_cannot_exceed_last_index() {
        let log = make_log();
        append_noop(&log, 1);
        let err = log.set_commit_index(LogIndex(99)).unwrap_err();
        assert!(matches!(err, RaftLogError::InvalidCommitIndex { .. }));
    }

    // -- compaction --------------------------------------------------------

    #[test]
    fn compact_removes_committed_prefix() {
        let log = make_log();
        for _ in 0..10 {
            append_noop(&log, 1);
        }
        log.set_commit_index(LogIndex(7)).unwrap();
        let removed = log.compact(LogIndex(5)).unwrap();
        assert_eq!(removed, 5);
        assert!(log.get(LogIndex(3)).is_none()); // compacted
        assert!(log.get(LogIndex(6)).is_some()); // still present
        assert_eq!(log.last_index(), LogIndex(10));
    }

    #[test]
    fn compact_cannot_exceed_commit_index() {
        let log = make_log();
        for _ in 0..5 {
            append_noop(&log, 1);
        }
        log.set_commit_index(LogIndex(3)).unwrap();
        let err = log.compact(LogIndex(4)).unwrap_err();
        assert!(matches!(err, RaftLogError::InvalidCommitIndex { .. }));
    }

    #[test]
    fn compact_updates_stats() {
        let log = make_log();
        for _ in 0..10 {
            append_noop(&log, 1);
        }
        log.set_commit_index(LogIndex(8)).unwrap();
        log.compact(LogIndex(5)).unwrap();

        let stats = log.get_stats();
        assert_eq!(stats.compacted_entries, 5);
        assert_eq!(stats.total_entries, 5);
        assert_eq!(stats.first_index, LogIndex(6));
        assert_eq!(stats.last_index, LogIndex(10));
    }

    // -- stats -------------------------------------------------------------

    #[test]
    fn stats_reflect_log_state() {
        let log = make_log();
        for _ in 0..5 {
            append_noop(&log, 2);
        }
        log.set_commit_index(LogIndex(3)).unwrap();

        let stats = log.get_stats();
        assert_eq!(stats.total_entries, 5);
        assert_eq!(stats.committed_entries, 3);
        assert_eq!(stats.compacted_entries, 0);
        assert_eq!(stats.first_index, LogIndex(1));
        assert_eq!(stats.last_index, LogIndex(5));
        assert_eq!(stats.last_term, Term(2));
        assert_eq!(stats.commit_index, LogIndex(3));
        assert!(stats.size_bytes > 0);
    }

    // -- state machine: slot assignment ------------------------------------

    #[test]
    fn sm_slot_assignment() {
        let sm = RaftStateMachine::new();
        let entry = LogEntry {
            index: LogIndex(1),
            term: Term(1),
            command: LogCommand::SlotAssignment {
                slot: 100,
                node_id: "n1".into(),
            },
            timestamp: Utc::now(),
        };
        let result = sm.apply(&entry).unwrap();
        assert!(result.applied);
        assert_eq!(sm.get_slot_assignment(100), Some("n1".to_string()));
        assert_eq!(sm.get_slot_assignment(200), None);
    }

    #[test]
    fn sm_slot_migration() {
        let sm = RaftStateMachine::new();
        // First assign slot to n1
        sm.apply(&LogEntry {
            index: LogIndex(1),
            term: Term(1),
            command: LogCommand::SlotAssignment {
                slot: 42,
                node_id: "n1".into(),
            },
            timestamp: Utc::now(),
        })
        .unwrap();
        // Migrate to n2
        sm.apply(&LogEntry {
            index: LogIndex(2),
            term: Term(1),
            command: LogCommand::SlotMigration {
                slot: 42,
                from: "n1".into(),
                to: "n2".into(),
            },
            timestamp: Utc::now(),
        })
        .unwrap();
        assert_eq!(sm.get_slot_assignment(42), Some("n2".to_string()));
    }

    // -- state machine: node join / leave ----------------------------------

    #[test]
    fn sm_node_join_and_leave() {
        let sm = RaftStateMachine::new();
        sm.apply(&LogEntry {
            index: LogIndex(1),
            term: Term(1),
            command: LogCommand::NodeJoin {
                node_id: "n1".into(),
                address: "10.0.0.1:6379".into(),
            },
            timestamp: Utc::now(),
        })
        .unwrap();

        let nodes = sm.get_node_list();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].node_id, "n1");
        assert_eq!(nodes[0].status, NodeStatus::Active);

        sm.apply(&LogEntry {
            index: LogIndex(2),
            term: Term(1),
            command: LogCommand::NodeLeave {
                node_id: "n1".into(),
            },
            timestamp: Utc::now(),
        })
        .unwrap();

        let nodes = sm.get_node_list();
        assert_eq!(nodes[0].status, NodeStatus::Leaving);
    }

    // -- state machine: config change --------------------------------------

    #[test]
    fn sm_config_change() {
        let sm = RaftStateMachine::new();
        sm.apply(&LogEntry {
            index: LogIndex(1),
            term: Term(1),
            command: LogCommand::ConfigChange {
                key: "max-memory".into(),
                value: "4gb".into(),
            },
            timestamp: Utc::now(),
        })
        .unwrap();
        assert_eq!(sm.get_config("max-memory"), Some("4gb".to_string()));
        assert_eq!(sm.get_config("missing"), None);
    }

    // -- state machine: epoch bump -----------------------------------------

    #[test]
    fn sm_epoch_bump() {
        let sm = RaftStateMachine::new();
        assert_eq!(sm.get_current_epoch(), 0);
        sm.apply(&LogEntry {
            index: LogIndex(1),
            term: Term(1),
            command: LogCommand::EpochBump { new_epoch: 7 },
            timestamp: Utc::now(),
        })
        .unwrap();
        assert_eq!(sm.get_current_epoch(), 7);
    }

    // -- state machine: idempotent apply -----------------------------------

    #[test]
    fn sm_idempotent_apply() {
        let sm = RaftStateMachine::new();
        let entry = LogEntry {
            index: LogIndex(1),
            term: Term(1),
            command: LogCommand::Noop,
            timestamp: Utc::now(),
        };
        let r1 = sm.apply(&entry).unwrap();
        assert!(r1.applied);
        let r2 = sm.apply(&entry).unwrap();
        assert!(!r2.applied);
    }

    // -- snapshot & restore ------------------------------------------------

    #[test]
    fn snapshot_and_restore() {
        let sm = RaftStateMachine::new();
        sm.apply(&LogEntry {
            index: LogIndex(1),
            term: Term(1),
            command: LogCommand::NodeJoin {
                node_id: "n1".into(),
                address: "10.0.0.1:6379".into(),
            },
            timestamp: Utc::now(),
        })
        .unwrap();
        sm.apply(&LogEntry {
            index: LogIndex(2),
            term: Term(1),
            command: LogCommand::SlotAssignment {
                slot: 0,
                node_id: "n1".into(),
            },
            timestamp: Utc::now(),
        })
        .unwrap();
        sm.apply(&LogEntry {
            index: LogIndex(3),
            term: Term(1),
            command: LogCommand::EpochBump { new_epoch: 5 },
            timestamp: Utc::now(),
        })
        .unwrap();

        let snap = sm.snapshot();
        assert_eq!(snap.epoch, 5);
        assert_eq!(snap.last_applied, LogIndex(3));
        assert_eq!(snap.nodes.len(), 1);
        assert_eq!(snap.slot_assignments.get(&0), Some(&"n1".to_string()));

        // Restore into a fresh state machine
        let sm2 = RaftStateMachine::new();
        sm2.restore(&snap).unwrap();
        assert_eq!(sm2.get_current_epoch(), 5);
        assert_eq!(sm2.get_slot_assignment(0), Some("n1".to_string()));
        assert_eq!(sm2.get_node_list().len(), 1);
    }

    // -- error cases -------------------------------------------------------

    #[test]
    fn truncate_compacted_index_errors() {
        let log = make_log();
        for _ in 0..10 {
            append_noop(&log, 1);
        }
        log.set_commit_index(LogIndex(5)).unwrap();
        log.compact(LogIndex(5)).unwrap();

        let err = log.truncate_after(LogIndex(3)).unwrap_err();
        assert!(matches!(err, RaftLogError::AlreadyCompacted(_)));
    }

    #[test]
    fn log_full_error() {
        let log = RaftLog::new(RaftLogConfig {
            max_entries: 3,
            ..Default::default()
        });
        append_noop(&log, 1);
        append_noop(&log, 1);
        append_noop(&log, 1);
        let err = log.append(make_entry(1, LogCommand::Noop)).unwrap_err();
        assert!(matches!(err, RaftLogError::Internal(_)));
    }

    #[test]
    fn display_impls() {
        assert_eq!(format!("{}", NodeStatus::Active), "Active");
        assert_eq!(format!("{}", NodeStatus::Leaving), "Leaving");
        assert_eq!(format!("{}", NodeStatus::Failed), "Failed");
    }
}
