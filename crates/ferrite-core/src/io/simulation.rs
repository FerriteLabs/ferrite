//! Deterministic Simulation Testing Framework
//!
//! Provides a fully deterministic execution environment for testing
//! distributed system behavior under controlled conditions. Inspired
//! by FoundationDB's simulation testing and Jepsen's fault injection.
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────┐
//! │              Deterministic Simulator                       │
//! │  ┌──────────────┐  ┌──────────────┐  ┌─────────────────┐ │
//! │  │  Seed-based  │  │ Fault        │  │ Virtual Clock   │ │
//! │  │  Scheduler   │  │ Injector     │  │ (no real time)  │ │
//! │  └──────────────┘  └──────────────┘  └─────────────────┘ │
//! │  ┌──────────────┐  ┌──────────────┐  ┌─────────────────┐ │
//! │  │  Network     │  │ Disk         │  │ Event           │ │
//! │  │  Simulation  │  │ Simulation   │  │ History Log     │ │
//! │  └──────────────┘  └──────────────┘  └─────────────────┘ │
//! └────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Features
//!
//! - **Deterministic scheduling**: Seed-based PRNG ensures reproducible runs
//! - **Virtual clock**: No dependency on wall clock time
//! - **Fault injection**: Network partitions, disk errors, message delays
//! - **Event history**: Full linearizability checking support
//! - **Property-based**: Generate random workloads with invariant checks

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Virtual Clock
// ---------------------------------------------------------------------------

/// A virtual clock that advances only when explicitly ticked.
/// Ensures fully deterministic timing across simulation runs.
#[derive(Debug)]
pub struct VirtualClock {
    /// Current virtual time in microseconds
    now_us: AtomicU64,
}

impl VirtualClock {
    /// Creates a new virtual clock starting at time zero.
    pub fn new() -> Self {
        Self {
            now_us: AtomicU64::new(0),
        }
    }

    /// Returns the current virtual time in microseconds.
    pub fn now_us(&self) -> u64 {
        self.now_us.load(Ordering::Relaxed)
    }

    /// Returns the current virtual time in milliseconds.
    pub fn now_ms(&self) -> u64 {
        self.now_us() / 1_000
    }

    /// Advances the clock by the given number of microseconds.
    pub fn advance(&self, delta_us: u64) {
        self.now_us.fetch_add(delta_us, Ordering::Relaxed);
    }

    /// Sets the clock to an absolute time in microseconds.
    pub fn set(&self, time_us: u64) {
        self.now_us.store(time_us, Ordering::Relaxed);
    }
}

impl Default for VirtualClock {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Deterministic RNG
// ---------------------------------------------------------------------------

/// Simple xorshift64 PRNG for deterministic simulation.
/// Not cryptographically secure — designed for reproducibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimRng {
    state: u64,
}

impl SimRng {
    /// Creates a new PRNG with the given seed.
    pub fn new(seed: u64) -> Self {
        // Ensure non-zero state
        Self {
            state: if seed == 0 { 1 } else { seed },
        }
    }

    /// Returns the next pseudo-random `u64` value.
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    /// Returns a value in [0, bound)
    pub fn next_bounded(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            return 0;
        }
        self.next_u64() % bound
    }

    /// Returns true with probability `p` (0.0 to 1.0)
    pub fn chance(&mut self, p: f64) -> bool {
        let threshold = (p * u64::MAX as f64) as u64;
        self.next_u64() < threshold
    }

    /// Shuffle a slice in-place (Fisher-Yates)
    pub fn shuffle<T>(&mut self, slice: &mut [T]) {
        for i in (1..slice.len()).rev() {
            let j = self.next_bounded(i as u64 + 1) as usize;
            slice.swap(i, j);
        }
    }
}

// ---------------------------------------------------------------------------
// Fault Types
// ---------------------------------------------------------------------------

/// Types of faults that can be injected during simulation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FaultKind {
    /// Drop network messages between two nodes
    NetworkPartition {
        /// First node in the partition
        node_a: String,
        /// Second node in the partition
        node_b: String,
    },
    /// Delay messages by a fixed amount
    NetworkDelay {
        /// Node to apply the delay to
        node: String,
        /// Delay duration in microseconds
        delay_us: u64,
    },
    /// Simulate disk I/O error
    DiskError {
        /// Node experiencing the disk error
        node: String,
        /// Description of the error
        error_msg: String,
    },
    /// Simulate a node crash (stops processing)
    NodeCrash {
        /// Node to crash
        node: String,
    },
    /// Simulate a node restart after crash
    NodeRestart {
        /// Node to restart
        node: String,
    },
    /// Simulate clock skew on a node
    ClockSkew {
        /// Node experiencing clock skew
        node: String,
        /// Clock skew amount in microseconds (may be negative)
        skew_us: i64,
    },
    /// Simulate slow disk I/O
    SlowDisk {
        /// Node experiencing slow disk
        node: String,
        /// Additional disk latency in microseconds
        latency_us: u64,
    },
}

impl fmt::Display for FaultKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FaultKind::NetworkPartition { node_a, node_b } => {
                write!(f, "NetworkPartition({} <-> {})", node_a, node_b)
            }
            FaultKind::NetworkDelay { node, delay_us } => {
                write!(f, "NetworkDelay({}, {}us)", node, delay_us)
            }
            FaultKind::DiskError { node, .. } => write!(f, "DiskError({})", node),
            FaultKind::NodeCrash { node } => write!(f, "NodeCrash({})", node),
            FaultKind::NodeRestart { node } => write!(f, "NodeRestart({})", node),
            FaultKind::ClockSkew { node, skew_us } => {
                write!(f, "ClockSkew({}, {}us)", node, skew_us)
            }
            FaultKind::SlowDisk { node, latency_us } => {
                write!(f, "SlowDisk({}, {}us)", node, latency_us)
            }
        }
    }
}

/// A scheduled fault with timing information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledFault {
    /// The fault to inject.
    pub fault: FaultKind,
    /// When to inject this fault (virtual time in microseconds)
    pub inject_at_us: u64,
    /// When to heal this fault (None = permanent)
    pub heal_at_us: Option<u64>,
}

// ---------------------------------------------------------------------------
// Fault Injector
// ---------------------------------------------------------------------------

/// Manages fault injection scheduling and active faults.
#[derive(Debug)]
pub struct FaultInjector {
    scheduled: Vec<ScheduledFault>,
    active_faults: Vec<FaultKind>,
    history: Vec<FaultEvent>,
}

/// Record of a fault event for post-run analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaultEvent {
    /// Virtual time when this event occurred.
    pub time_us: u64,
    /// The fault that was injected or healed.
    pub fault: FaultKind,
    /// Whether the fault was injected or healed.
    pub action: FaultAction,
}

/// Whether a fault was injected or healed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FaultAction {
    /// The fault was activated.
    Injected,
    /// The fault was removed.
    Healed,
}

impl fmt::Display for FaultAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FaultAction::Injected => write!(f, "INJECTED"),
            FaultAction::Healed => write!(f, "HEALED"),
        }
    }
}

impl FaultInjector {
    /// Creates a new empty fault injector.
    pub fn new() -> Self {
        Self {
            scheduled: Vec::new(),
            active_faults: Vec::new(),
            history: Vec::new(),
        }
    }

    /// Adds a fault to the schedule, sorted by injection time.
    pub fn schedule(&mut self, fault: ScheduledFault) {
        self.scheduled.push(fault);
        self.scheduled.sort_by_key(|f| f.inject_at_us);
    }

    /// Process faults up to the given virtual time, returning newly injected/healed faults.
    pub fn tick(&mut self, now_us: u64) -> Vec<FaultEvent> {
        let mut events = Vec::new();

        // Inject faults whose time has come
        let mut i = 0;
        while i < self.scheduled.len() {
            if self.scheduled[i].inject_at_us <= now_us {
                let sf = self.scheduled.remove(i);
                let event = FaultEvent {
                    time_us: now_us,
                    fault: sf.fault.clone(),
                    action: FaultAction::Injected,
                };
                events.push(event.clone());
                self.history.push(event);
                self.active_faults.push(sf.fault.clone());

                // Schedule healing if duration is specified
                if let Some(heal_at) = sf.heal_at_us {
                    self.scheduled.push(ScheduledFault {
                        fault: self.heal_fault(&sf.fault),
                        inject_at_us: heal_at,
                        heal_at_us: None,
                    });
                    self.scheduled.sort_by_key(|f| f.inject_at_us);
                }
            } else {
                i += 1;
            }
        }

        // Remove healed faults from active list
        self.active_faults.retain(|f| {
            !events
                .iter()
                .any(|e| e.action == FaultAction::Healed && e.fault == *f)
        });

        events
    }

    fn heal_fault(&self, fault: &FaultKind) -> FaultKind {
        match fault {
            FaultKind::NodeCrash { node } => FaultKind::NodeRestart { node: node.clone() },
            other => other.clone(),
        }
    }

    /// Check if a network partition exists between two nodes.
    pub fn is_partitioned(&self, node_a: &str, node_b: &str) -> bool {
        self.active_faults.iter().any(|f| match f {
            FaultKind::NetworkPartition {
                node_a: a,
                node_b: b,
            } => (a == node_a && b == node_b) || (a == node_b && b == node_a),
            _ => false,
        })
    }

    /// Check if a node is crashed.
    pub fn is_crashed(&self, node: &str) -> bool {
        self.active_faults.iter().any(|f| match f {
            FaultKind::NodeCrash { node: n } => n == node,
            _ => false,
        })
    }

    /// Get the network delay for a node (0 if none).
    pub fn get_delay(&self, node: &str) -> u64 {
        self.active_faults
            .iter()
            .filter_map(|f| match f {
                FaultKind::NetworkDelay {
                    node: n,
                    delay_us: d,
                } if n == node => Some(*d),
                _ => None,
            })
            .max()
            .unwrap_or(0)
    }

    /// Returns the currently active faults.
    pub fn active_faults(&self) -> &[FaultKind] {
        &self.active_faults
    }

    /// Returns the full history of fault events.
    pub fn history(&self) -> &[FaultEvent] {
        &self.history
    }
}

impl Default for FaultInjector {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Simulated Network
// ---------------------------------------------------------------------------

/// A message in the simulated network.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimMessage {
    /// Source node identifier.
    pub from: String,
    /// Destination node identifier.
    pub to: String,
    /// Message payload bytes.
    pub payload: Vec<u8>,
    /// Virtual time when the message should be delivered
    pub deliver_at_us: u64,
    /// Unique message identifier.
    pub id: u64,
}

/// Simulated network layer that models message passing between nodes.
#[derive(Debug)]
pub struct SimNetwork {
    /// Messages in flight, keyed by delivery time
    in_flight: BTreeMap<u64, Vec<SimMessage>>,
    /// Messages delivered and waiting to be consumed
    delivered: HashMap<String, VecDeque<SimMessage>>,
    /// Dropped message count
    dropped: u64,
    next_msg_id: u64,
    /// Base network latency in microseconds
    base_latency_us: u64,
}

impl SimNetwork {
    /// Creates a new simulated network with the given base latency.
    pub fn new(base_latency_us: u64) -> Self {
        Self {
            in_flight: BTreeMap::new(),
            delivered: HashMap::new(),
            dropped: 0,
            next_msg_id: 0,
            base_latency_us,
        }
    }

    /// Send a message from one node to another.
    pub fn send(
        &mut self,
        from: &str,
        to: &str,
        payload: Vec<u8>,
        now_us: u64,
        injector: &FaultInjector,
        rng: &mut SimRng,
    ) {
        // Check for partition
        if injector.is_partitioned(from, to) {
            tracing::debug!(from = from, to = to, "Message dropped: partition");
            self.dropped += 1;
            return;
        }

        // Check if sender is crashed
        if injector.is_crashed(from) {
            tracing::debug!(from = from, "Message dropped: sender crashed");
            self.dropped += 1;
            return;
        }

        // Calculate delivery time with jitter
        let delay = self.base_latency_us + injector.get_delay(from) + injector.get_delay(to);
        let jitter = rng.next_bounded(delay / 4 + 1);
        let deliver_at = now_us + delay + jitter;

        let msg = SimMessage {
            from: from.to_string(),
            to: to.to_string(),
            payload,
            deliver_at_us: deliver_at,
            id: self.next_msg_id,
        };
        self.next_msg_id += 1;

        self.in_flight.entry(deliver_at).or_default().push(msg);
    }

    /// Deliver all messages up to the given time.
    pub fn deliver_up_to(&mut self, now_us: u64, injector: &FaultInjector) -> usize {
        let mut delivered_count = 0;
        let to_deliver: Vec<_> = self
            .in_flight
            .range(..=now_us)
            .map(|(k, v)| (*k, v.clone()))
            .collect();

        for (time, messages) in to_deliver {
            self.in_flight.remove(&time);
            for msg in messages {
                // Check if receiver is crashed
                if injector.is_crashed(&msg.to) {
                    self.dropped += 1;
                    continue;
                }
                self.delivered
                    .entry(msg.to.clone())
                    .or_default()
                    .push_back(msg);
                delivered_count += 1;
            }
        }

        delivered_count
    }

    /// Receive the next message for a given node.
    pub fn recv(&mut self, node: &str) -> Option<SimMessage> {
        self.delivered.get_mut(node)?.pop_front()
    }

    /// Receive all pending messages for a node.
    pub fn recv_all(&mut self, node: &str) -> Vec<SimMessage> {
        self.delivered
            .get_mut(node)
            .map(|q| q.drain(..).collect())
            .unwrap_or_default()
    }

    /// Returns the number of messages currently in flight.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.values().map(|v| v.len()).sum()
    }

    /// Returns the total number of dropped messages.
    pub fn dropped_count(&self) -> u64 {
        self.dropped
    }
}

// ---------------------------------------------------------------------------
// Simulated Disk
// ---------------------------------------------------------------------------

/// Simulated disk that models persistent storage with fault injection.
#[derive(Debug)]
pub struct SimDisk {
    /// In-memory representation of disk contents
    files: HashMap<String, Vec<u8>>,
    /// Write-ahead log of all operations for verification
    wal: Vec<DiskOp>,
    /// Whether the disk is in a failed state
    failed: bool,
}

/// A recorded disk operation for post-run verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskOp {
    /// Virtual time when this operation occurred.
    pub time_us: u64,
    /// The kind of disk operation performed.
    pub kind: DiskOpKind,
    /// File path targeted by this operation.
    pub path: String,
}

/// The type of disk operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DiskOpKind {
    /// A write operation with the given byte count.
    Write {
        /// Number of bytes written
        size: usize,
    },
    /// A read operation with the given byte count.
    Read {
        /// Number of bytes read
        size: usize,
    },
    /// A file deletion.
    Delete,
    /// A sync/flush to disk.
    Sync,
}

impl SimDisk {
    /// Creates a new empty simulated disk.
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
            wal: Vec::new(),
            failed: false,
        }
    }

    /// Writes data to a file path, recording the operation in the WAL.
    pub fn write(&mut self, path: &str, data: &[u8], now_us: u64) -> Result<(), SimulationError> {
        if self.failed {
            return Err(SimulationError::DiskFailure {
                path: path.to_string(),
                detail: "disk is in failed state".to_string(),
            });
        }

        self.wal.push(DiskOp {
            time_us: now_us,
            kind: DiskOpKind::Write { size: data.len() },
            path: path.to_string(),
        });
        self.files.insert(path.to_string(), data.to_vec());
        Ok(())
    }

    /// Reads data from a file path, recording the operation in the WAL.
    pub fn read(&mut self, path: &str, now_us: u64) -> Result<Vec<u8>, SimulationError> {
        if self.failed {
            return Err(SimulationError::DiskFailure {
                path: path.to_string(),
                detail: "disk is in failed state".to_string(),
            });
        }

        let data = self
            .files
            .get(path)
            .cloned()
            .ok_or_else(|| SimulationError::DiskFailure {
                path: path.to_string(),
                detail: "file not found".to_string(),
            })?;

        self.wal.push(DiskOp {
            time_us: now_us,
            kind: DiskOpKind::Read { size: data.len() },
            path: path.to_string(),
        });
        Ok(data)
    }

    /// Deletes a file at the given path, recording the operation in the WAL.
    pub fn delete(&mut self, path: &str, now_us: u64) -> Result<(), SimulationError> {
        if self.failed {
            return Err(SimulationError::DiskFailure {
                path: path.to_string(),
                detail: "disk is in failed state".to_string(),
            });
        }

        self.wal.push(DiskOp {
            time_us: now_us,
            kind: DiskOpKind::Delete,
            path: path.to_string(),
        });
        self.files.remove(path);
        Ok(())
    }

    /// Sets or clears the disk failure state.
    pub fn set_failed(&mut self, failed: bool) {
        self.failed = failed;
    }

    /// Returns whether a file exists at the given path.
    pub fn exists(&self, path: &str) -> bool {
        self.files.contains_key(path)
    }

    /// Returns all recorded disk operations.
    pub fn operations(&self) -> &[DiskOp] {
        &self.wal
    }
}

impl Default for SimDisk {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Event History & Linearizability Checking
// ---------------------------------------------------------------------------

/// An operation recorded in the simulation history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEvent {
    /// Virtual time when this event was recorded.
    pub time_us: u64,
    /// Node that performed the operation.
    pub node: String,
    /// The operation that was performed.
    pub op: HistoryOp,
}

/// The type of operation in the history.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum HistoryOp {
    /// Client invoked an operation
    Invoke {
        /// Unique operation identifier
        op_id: u64,
        /// Key being operated on
        key: String,
        /// Kind of operation invoked
        kind: OpKind,
    },
    /// Operation completed successfully
    Ok {
        /// Unique operation identifier
        op_id: u64,
        /// Key that was operated on
        key: String,
        /// Resulting value, if any
        value: Option<String>,
    },
    /// Operation failed
    Fail {
        /// Unique operation identifier
        op_id: u64,
        /// Key that was operated on
        key: String,
        /// Error description
        error: String,
    },
}

/// The kind of key-value operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum OpKind {
    /// A read operation.
    Read,
    /// A write operation with a value.
    Write {
        /// Value to write
        value: String,
    },
    /// A compare-and-swap operation.
    Cas {
        /// Expected current value
        expected: String,
        /// New value to set if expected matches
        new_value: String,
    },
    /// A delete operation.
    Delete,
}

impl fmt::Display for OpKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpKind::Read => write!(f, "READ"),
            OpKind::Write { value } => write!(f, "WRITE({})", value),
            OpKind::Cas {
                expected,
                new_value,
            } => write!(f, "CAS({} -> {})", expected, new_value),
            OpKind::Delete => write!(f, "DELETE"),
        }
    }
}

/// Event history recorder for linearizability verification.
#[derive(Debug)]
pub struct EventHistory {
    events: Vec<HistoryEvent>,
    next_op_id: AtomicU64,
}

impl EventHistory {
    /// Creates a new empty event history.
    pub fn new() -> Self {
        Self {
            events: Vec::new(),
            next_op_id: AtomicU64::new(0),
        }
    }

    /// Returns the next unique operation identifier.
    pub fn next_op_id(&self) -> u64 {
        self.next_op_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Records a history event.
    pub fn record(&mut self, event: HistoryEvent) {
        self.events.push(event);
    }

    /// Records an invoke event and returns the assigned operation ID.
    pub fn invoke(&mut self, time_us: u64, node: &str, key: &str, kind: OpKind) -> u64 {
        let op_id = self.next_op_id();
        self.record(HistoryEvent {
            time_us,
            node: node.to_string(),
            op: HistoryOp::Invoke {
                op_id,
                key: key.to_string(),
                kind,
            },
        });
        op_id
    }

    /// Records a successful completion event for an operation.
    pub fn ok(&mut self, time_us: u64, node: &str, op_id: u64, key: &str, value: Option<String>) {
        self.record(HistoryEvent {
            time_us,
            node: node.to_string(),
            op: HistoryOp::Ok {
                op_id,
                key: key.to_string(),
                value,
            },
        });
    }

    /// Records a failure event for an operation.
    pub fn fail(&mut self, time_us: u64, node: &str, op_id: u64, key: &str, error: &str) {
        self.record(HistoryEvent {
            time_us,
            node: node.to_string(),
            op: HistoryOp::Fail {
                op_id,
                key: key.to_string(),
                error: error.to_string(),
            },
        });
    }

    /// Returns all recorded history events.
    pub fn events(&self) -> &[HistoryEvent] {
        &self.events
    }

    /// Basic sequential consistency check:
    /// For every completed read, the value should match the last completed write.
    pub fn check_sequential_consistency(&self) -> Result<(), Vec<String>> {
        let mut last_write: HashMap<String, String> = HashMap::new();
        let mut violations = Vec::new();

        // Build a map of completed operations
        let mut completed: HashMap<u64, (OpKind, Option<String>)> = HashMap::new();

        for event in &self.events {
            match &event.op {
                HistoryOp::Invoke { op_id, kind, .. } => {
                    completed.insert(*op_id, (kind.clone(), None));
                }
                HistoryOp::Ok {
                    op_id, key, value, ..
                } => {
                    if let Some(entry) = completed.get_mut(op_id) {
                        entry.1 = value.clone();

                        match &entry.0 {
                            OpKind::Write { value: v } => {
                                last_write.insert(key.clone(), v.clone());
                            }
                            OpKind::Read => {
                                if let Some(read_val) = value {
                                    if let Some(expected) = last_write.get(key) {
                                        if read_val != expected {
                                            violations.push(format!(
                                                "op_id={}: read key='{}' got '{}' expected '{}'",
                                                op_id, key, read_val, expected
                                            ));
                                        }
                                    }
                                }
                            }
                            OpKind::Delete => {
                                last_write.remove(key);
                            }
                            _ => {}
                        }
                    }
                }
                HistoryOp::Fail { .. } => {}
            }
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }
}

impl Default for EventHistory {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Simulation Config & Runner
// ---------------------------------------------------------------------------

/// Configuration for a simulation run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationConfig {
    /// PRNG seed for deterministic execution
    pub seed: u64,
    /// Number of simulated nodes
    pub num_nodes: usize,
    /// Maximum simulation steps
    pub max_steps: u64,
    /// Base network latency in microseconds
    pub network_latency_us: u64,
    /// Time step per tick in microseconds
    pub tick_us: u64,
    /// Maximum virtual runtime in microseconds
    pub max_runtime_us: u64,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            seed: 42,
            num_nodes: 3,
            max_steps: 10_000,
            network_latency_us: 1_000,
            tick_us: 100,
            max_runtime_us: 10_000_000, // 10 seconds virtual time
        }
    }
}

/// A simulated node in the cluster.
#[derive(Debug)]
pub struct SimNode {
    /// Unique node identifier.
    pub id: String,
    /// Simulated disk storage for this node.
    pub disk: SimDisk,
    /// In-memory key-value state
    pub store: HashMap<String, String>,
    /// Whether this node is in a crashed state.
    pub crashed: bool,
}

impl SimNode {
    /// Creates a new simulated node with the given identifier.
    pub fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            disk: SimDisk::new(),
            store: HashMap::new(),
            crashed: false,
        }
    }

    /// Writes a key-value pair to this node's in-memory store.
    pub fn apply_write(&mut self, key: &str, value: &str) -> Result<(), SimulationError> {
        if self.crashed {
            return Err(SimulationError::NodeCrashed {
                node: self.id.clone(),
            });
        }
        self.store.insert(key.to_string(), value.to_string());
        Ok(())
    }

    /// Reads a value by key from this node's in-memory store.
    pub fn apply_read(&self, key: &str) -> Result<Option<String>, SimulationError> {
        if self.crashed {
            return Err(SimulationError::NodeCrashed {
                node: self.id.clone(),
            });
        }
        Ok(self.store.get(key).cloned())
    }

    /// Deletes a key from this node's in-memory store, returning whether it existed.
    pub fn apply_delete(&mut self, key: &str) -> Result<bool, SimulationError> {
        if self.crashed {
            return Err(SimulationError::NodeCrashed {
                node: self.id.clone(),
            });
        }
        Ok(self.store.remove(key).is_some())
    }
}

/// The main deterministic simulator.
pub struct Simulator {
    /// Simulation configuration parameters.
    pub config: SimulationConfig,
    /// Virtual clock for deterministic time progression.
    pub clock: VirtualClock,
    /// Deterministic pseudo-random number generator.
    pub rng: SimRng,
    /// Simulated network for message passing between nodes.
    pub network: SimNetwork,
    /// Fault injector for scheduling and managing faults.
    pub injector: FaultInjector,
    /// Event history for linearizability checking.
    pub history: EventHistory,
    /// Map of node identifiers to simulated nodes.
    pub nodes: HashMap<String, SimNode>,
    /// Current simulation step count.
    pub step: u64,
}

impl Simulator {
    /// Creates a new simulator from the given configuration.
    pub fn new(config: SimulationConfig) -> Self {
        let mut nodes = HashMap::new();
        for i in 0..config.num_nodes {
            let id = format!("node-{}", i);
            nodes.insert(id.clone(), SimNode::new(&id));
        }

        Self {
            rng: SimRng::new(config.seed),
            network: SimNetwork::new(config.network_latency_us),
            clock: VirtualClock::new(),
            injector: FaultInjector::new(),
            history: EventHistory::new(),
            nodes,
            step: 0,
            config,
        }
    }

    /// Run one simulation tick: advance clock, process faults, deliver messages.
    pub fn tick(&mut self) -> SimTickResult {
        self.step += 1;
        self.clock.advance(self.config.tick_us);
        let now = self.clock.now_us();

        // Process fault schedule
        let fault_events = self.injector.tick(now);

        // Apply node crash/restart faults
        for event in &fault_events {
            match &event.fault {
                FaultKind::NodeCrash { node } => {
                    if let Some(n) = self.nodes.get_mut(node) {
                        n.crashed = true;
                    }
                }
                FaultKind::NodeRestart { node } => {
                    if let Some(n) = self.nodes.get_mut(node) {
                        n.crashed = false;
                    }
                }
                FaultKind::DiskError { node, .. } => {
                    if let Some(n) = self.nodes.get_mut(node) {
                        n.disk.set_failed(true);
                    }
                }
                _ => {}
            }
        }

        // Deliver network messages
        let messages_delivered = self.network.deliver_up_to(now, &self.injector);

        SimTickResult {
            step: self.step,
            time_us: now,
            fault_events,
            messages_delivered,
        }
    }

    /// Run until max_steps or max_runtime is reached.
    pub fn run(&mut self) -> SimulationResult {
        while self.step < self.config.max_steps && self.clock.now_us() < self.config.max_runtime_us
        {
            self.tick();
        }

        let consistency_check = self.history.check_sequential_consistency();

        SimulationResult {
            seed: self.config.seed,
            total_steps: self.step,
            final_time_us: self.clock.now_us(),
            messages_dropped: self.network.dropped_count(),
            fault_history: self.injector.history().to_vec(),
            consistency_violations: consistency_check.err().unwrap_or_default(),
        }
    }

    /// Execute a write operation on a specific node with history recording.
    pub fn write_op(
        &mut self,
        node_id: &str,
        key: &str,
        value: &str,
    ) -> Result<(), SimulationError> {
        let now = self.clock.now_us();
        let op_id = self.history.invoke(
            now,
            node_id,
            key,
            OpKind::Write {
                value: value.to_string(),
            },
        );

        let result = self
            .nodes
            .get_mut(node_id)
            .ok_or_else(|| SimulationError::NodeNotFound {
                node: node_id.to_string(),
            })?
            .apply_write(key, value);

        match result {
            Ok(()) => {
                self.history
                    .ok(now, node_id, op_id, key, Some(value.to_string()));
                Ok(())
            }
            Err(e) => {
                self.history.fail(now, node_id, op_id, key, &e.to_string());
                Err(e)
            }
        }
    }

    /// Execute a read operation on a specific node with history recording.
    pub fn read_op(&mut self, node_id: &str, key: &str) -> Result<Option<String>, SimulationError> {
        let now = self.clock.now_us();
        let op_id = self.history.invoke(now, node_id, key, OpKind::Read);

        let result = self
            .nodes
            .get(node_id)
            .ok_or_else(|| SimulationError::NodeNotFound {
                node: node_id.to_string(),
            })?
            .apply_read(key);

        match result {
            Ok(val) => {
                self.history.ok(now, node_id, op_id, key, val.clone());
                Ok(val)
            }
            Err(e) => {
                self.history.fail(now, node_id, op_id, key, &e.to_string());
                Err(e)
            }
        }
    }

    /// Schedule a fault for injection at a specific time.
    pub fn schedule_fault(
        &mut self,
        fault: FaultKind,
        inject_at_us: u64,
        duration_us: Option<u64>,
    ) {
        self.injector.schedule(ScheduledFault {
            fault,
            inject_at_us,
            heal_at_us: duration_us.map(|d| inject_at_us + d),
        });
    }

    /// Returns all node identifiers in the simulation.
    pub fn node_ids(&self) -> Vec<String> {
        self.nodes.keys().cloned().collect()
    }
}

impl fmt::Debug for Simulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Simulator")
            .field("seed", &self.config.seed)
            .field("step", &self.step)
            .field("time_us", &self.clock.now_us())
            .field("nodes", &self.nodes.len())
            .finish()
    }
}

/// Result of a single simulation tick.
#[derive(Debug)]
pub struct SimTickResult {
    /// The simulation step number after this tick.
    pub step: u64,
    /// The virtual time in microseconds after this tick.
    pub time_us: u64,
    /// Fault events that occurred during this tick.
    pub fault_events: Vec<FaultEvent>,
    /// Number of network messages delivered during this tick.
    pub messages_delivered: usize,
}

/// Result of a complete simulation run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationResult {
    /// PRNG seed used for this simulation run.
    pub seed: u64,
    /// Total number of simulation steps executed.
    pub total_steps: u64,
    /// Final virtual time in microseconds.
    pub final_time_us: u64,
    /// Total number of network messages dropped.
    pub messages_dropped: u64,
    /// Complete history of fault injection and healing events.
    pub fault_history: Vec<FaultEvent>,
    /// List of sequential consistency violations detected.
    pub consistency_violations: Vec<String>,
}

impl SimulationResult {
    /// Returns `true` if no consistency violations were detected.
    pub fn passed(&self) -> bool {
        self.consistency_violations.is_empty()
    }
}

impl fmt::Display for SimulationResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Simulation(seed={}, steps={}, time={}us, dropped={}, violations={})",
            self.seed,
            self.total_steps,
            self.final_time_us,
            self.messages_dropped,
            self.consistency_violations.len()
        )
    }
}

// ---------------------------------------------------------------------------
// Multi-Seed Runner
// ---------------------------------------------------------------------------

/// Run the same simulation scenario across many seeds to find bugs.
pub struct MultiSeedRunner {
    base_config: SimulationConfig,
    #[allow(clippy::type_complexity)]
    fault_schedule_fn: Option<Box<dyn Fn(&mut Simulator) + Send + Sync>>,
    #[allow(clippy::type_complexity)]
    workload_fn: Option<Box<dyn Fn(&mut Simulator) + Send + Sync>>,
}

impl MultiSeedRunner {
    /// Creates a new multi-seed runner with the given base configuration.
    pub fn new(config: SimulationConfig) -> Self {
        Self {
            base_config: config,
            fault_schedule_fn: None,
            workload_fn: None,
        }
    }

    /// Sets the fault scheduling function applied to each simulation run.
    pub fn with_faults<F>(mut self, f: F) -> Self
    where
        F: Fn(&mut Simulator) + Send + Sync + 'static,
    {
        self.fault_schedule_fn = Some(Box::new(f));
        self
    }

    /// Sets the workload function applied to each simulation run.
    pub fn with_workload<F>(mut self, f: F) -> Self
    where
        F: Fn(&mut Simulator) + Send + Sync + 'static,
    {
        self.workload_fn = Some(Box::new(f));
        self
    }

    /// Run simulation with the given number of different seeds.
    pub fn run(&self, num_seeds: usize) -> Vec<SimulationResult> {
        let mut results = Vec::with_capacity(num_seeds);

        for i in 0..num_seeds {
            let mut config = self.base_config.clone();
            config.seed = self.base_config.seed.wrapping_add(i as u64);

            let mut sim = Simulator::new(config);

            if let Some(ref fault_fn) = self.fault_schedule_fn {
                fault_fn(&mut sim);
            }

            if let Some(ref workload_fn) = self.workload_fn {
                workload_fn(&mut sim);
            }

            let result = sim.run();
            tracing::debug!(
                seed = result.seed,
                steps = result.total_steps,
                passed = result.passed(),
                "Simulation complete"
            );
            results.push(result);
        }

        results
    }
}

impl fmt::Debug for MultiSeedRunner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiSeedRunner")
            .field("base_config", &self.base_config)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Invariant Checker
// ---------------------------------------------------------------------------

/// A composable invariant checker for post-simulation verification.
pub struct InvariantChecker {
    checks: Vec<InvariantCheck>,
}

struct InvariantCheck {
    name: String,
    #[allow(clippy::type_complexity)]
    check_fn: Box<dyn Fn(&Simulator) -> Result<(), String>>,
}

impl InvariantChecker {
    /// Creates a new empty invariant checker.
    pub fn new() -> Self {
        Self { checks: Vec::new() }
    }

    /// Adds a named invariant check function.
    pub fn add<F>(&mut self, name: &str, check: F)
    where
        F: Fn(&Simulator) -> Result<(), String> + 'static,
    {
        self.checks.push(InvariantCheck {
            name: name.to_string(),
            check_fn: Box::new(check),
        });
    }

    /// Runs all invariant checks against the simulator, returning any violations.
    pub fn check_all(&self, sim: &Simulator) -> Vec<InvariantViolation> {
        let mut violations = Vec::new();
        for check in &self.checks {
            if let Err(msg) = (check.check_fn)(sim) {
                violations.push(InvariantViolation {
                    invariant: check.name.clone(),
                    message: msg,
                });
            }
        }
        violations
    }
}

impl Default for InvariantChecker {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for InvariantChecker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names: Vec<_> = self.checks.iter().map(|c| c.name.as_str()).collect();
        f.debug_struct("InvariantChecker")
            .field("checks", &names)
            .finish()
    }
}

/// A detected invariant violation with context.
#[derive(Debug, Clone)]
pub struct InvariantViolation {
    /// Name of the invariant that was violated.
    pub invariant: String,
    /// Description of the violation.
    pub message: String,
}

impl fmt::Display for InvariantViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Invariant '{}' violated: {}",
            self.invariant, self.message
        )
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur during simulation execution.
#[derive(Debug, thiserror::Error)]
pub enum SimulationError {
    #[error("node not found: {node}")]
    /// The specified node was not found in the simulation.
    NodeNotFound {
        /// Node identifier that was not found
        node: String,
    },

    #[error("node crashed: {node}")]
    /// The target node is in a crashed state.
    NodeCrashed {
        /// Node identifier that is crashed
        node: String,
    },

    #[error("disk failure on {path}: {detail}")]
    /// A disk I/O operation failed.
    DiskFailure {
        /// File path where the failure occurred
        path: String,
        /// Description of the disk failure
        detail: String,
    },

    #[error("simulation timeout after {steps} steps")]
    /// The simulation exceeded its maximum step count.
    Timeout {
        /// Number of steps executed before timeout
        steps: u64,
    },

    #[error("invariant violation: {0}")]
    /// An invariant check failed during simulation.
    InvariantViolation(String),
}

// ---------------------------------------------------------------------------
// Thread-safe wrapper for shared simulator state
// ---------------------------------------------------------------------------

/// Thread-safe handle for sharing simulation state across components.
#[derive(Debug, Clone)]
pub struct SharedSimulator {
    inner: Arc<RwLock<SimulatorState>>,
}

#[derive(Debug)]
struct SimulatorState {
    clock: Arc<VirtualClock>,
    rng: SimRng,
    _step: u64,
}

impl SharedSimulator {
    /// Creates a new shared simulator handle with the given PRNG seed.
    pub fn new(seed: u64) -> Self {
        Self {
            inner: Arc::new(RwLock::new(SimulatorState {
                clock: Arc::new(VirtualClock::new()),
                rng: SimRng::new(seed),
                _step: 0,
            })),
        }
    }

    /// Returns the current virtual time in microseconds.
    pub fn now_us(&self) -> u64 {
        self.inner.read().clock.now_us()
    }

    /// Advances the virtual clock by the given number of microseconds.
    pub fn advance(&self, delta_us: u64) {
        self.inner.read().clock.advance(delta_us);
    }

    /// Returns the next pseudo-random value from the shared PRNG.
    pub fn next_random(&self) -> u64 {
        self.inner.write().rng.next_u64()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_virtual_clock() {
        let clock = VirtualClock::new();
        assert_eq!(clock.now_us(), 0);

        clock.advance(1_000);
        assert_eq!(clock.now_us(), 1_000);
        assert_eq!(clock.now_ms(), 1);

        clock.advance(500);
        assert_eq!(clock.now_us(), 1_500);

        clock.set(5_000);
        assert_eq!(clock.now_us(), 5_000);
    }

    #[test]
    fn test_sim_rng_deterministic() {
        let mut rng1 = SimRng::new(42);
        let mut rng2 = SimRng::new(42);

        let seq1: Vec<u64> = (0..100).map(|_| rng1.next_u64()).collect();
        let seq2: Vec<u64> = (0..100).map(|_| rng2.next_u64()).collect();
        assert_eq!(seq1, seq2);
    }

    #[test]
    fn test_sim_rng_different_seeds() {
        let mut rng1 = SimRng::new(42);
        let mut rng2 = SimRng::new(99);
        assert_ne!(rng1.next_u64(), rng2.next_u64());
    }

    #[test]
    fn test_sim_rng_bounded() {
        let mut rng = SimRng::new(42);
        for _ in 0..100 {
            let v = rng.next_bounded(10);
            assert!(v < 10);
        }
        assert_eq!(rng.next_bounded(0), 0);
    }

    #[test]
    fn test_sim_rng_shuffle() {
        let mut rng = SimRng::new(42);
        let mut data = vec![1, 2, 3, 4, 5];
        rng.shuffle(&mut data);
        // After shuffling, should still contain same elements
        let mut sorted = data.clone();
        sorted.sort();
        assert_eq!(sorted, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_fault_injector_schedule_and_tick() {
        let mut injector = FaultInjector::new();
        injector.schedule(ScheduledFault {
            fault: FaultKind::NetworkPartition {
                node_a: "n1".to_string(),
                node_b: "n2".to_string(),
            },
            inject_at_us: 1_000,
            heal_at_us: Some(5_000),
        });

        // Before injection time
        let events = injector.tick(500);
        assert!(events.is_empty());
        assert!(!injector.is_partitioned("n1", "n2"));

        // At injection time
        let events = injector.tick(1_000);
        assert_eq!(events.len(), 1);
        assert!(injector.is_partitioned("n1", "n2"));
        assert!(injector.is_partitioned("n2", "n1")); // symmetric

        // At heal time
        let events = injector.tick(5_000);
        assert!(!events.is_empty());
    }

    #[test]
    fn test_fault_injector_node_crash() {
        let mut injector = FaultInjector::new();
        injector.schedule(ScheduledFault {
            fault: FaultKind::NodeCrash {
                node: "n1".to_string(),
            },
            inject_at_us: 100,
            heal_at_us: None,
        });

        injector.tick(100);
        assert!(injector.is_crashed("n1"));
        assert!(!injector.is_crashed("n2"));
    }

    #[test]
    fn test_fault_injector_network_delay() {
        let mut injector = FaultInjector::new();
        injector.schedule(ScheduledFault {
            fault: FaultKind::NetworkDelay {
                node: "n1".to_string(),
                delay_us: 5_000,
            },
            inject_at_us: 0,
            heal_at_us: None,
        });

        injector.tick(0);
        assert_eq!(injector.get_delay("n1"), 5_000);
        assert_eq!(injector.get_delay("n2"), 0);
    }

    #[test]
    fn test_sim_network_send_recv() {
        let mut net = SimNetwork::new(100);
        let injector = FaultInjector::new();
        let mut rng = SimRng::new(42);

        net.send("n1", "n2", b"hello".to_vec(), 0, &injector, &mut rng);
        assert_eq!(net.in_flight_count(), 1);

        // Deliver at appropriate time
        let delivered = net.deliver_up_to(200, &injector);
        assert!(delivered > 0);

        let msg = net.recv("n2");
        assert!(msg.is_some());
        assert_eq!(msg.unwrap().payload, b"hello");
    }

    #[test]
    fn test_sim_network_partition_drops() {
        let mut net = SimNetwork::new(100);
        let mut injector = FaultInjector::new();
        let mut rng = SimRng::new(42);

        injector.schedule(ScheduledFault {
            fault: FaultKind::NetworkPartition {
                node_a: "n1".to_string(),
                node_b: "n2".to_string(),
            },
            inject_at_us: 0,
            heal_at_us: None,
        });
        injector.tick(0);

        net.send("n1", "n2", b"hello".to_vec(), 0, &injector, &mut rng);
        assert_eq!(net.in_flight_count(), 0);
        assert_eq!(net.dropped_count(), 1);
    }

    #[test]
    fn test_sim_disk_operations() {
        let mut disk = SimDisk::new();

        disk.write("data.bin", b"hello world", 100).unwrap();
        assert!(disk.exists("data.bin"));

        let data = disk.read("data.bin", 200).unwrap();
        assert_eq!(data, b"hello world");

        disk.delete("data.bin", 300).unwrap();
        assert!(!disk.exists("data.bin"));

        assert_eq!(disk.operations().len(), 3);
    }

    #[test]
    fn test_sim_disk_failure() {
        let mut disk = SimDisk::new();
        disk.set_failed(true);

        let err = disk.write("data.bin", b"test", 0);
        assert!(err.is_err());

        let err = disk.read("data.bin", 0);
        assert!(err.is_err());
    }

    #[test]
    fn test_event_history_consistency() {
        let mut history = EventHistory::new();

        // Write then read the same key
        let op1 = history.invoke(
            0,
            "n1",
            "key1",
            OpKind::Write {
                value: "v1".to_string(),
            },
        );
        history.ok(0, "n1", op1, "key1", Some("v1".to_string()));

        let op2 = history.invoke(1, "n1", "key1", OpKind::Read);
        history.ok(1, "n1", op2, "key1", Some("v1".to_string()));

        assert!(history.check_sequential_consistency().is_ok());
    }

    #[test]
    fn test_event_history_violation() {
        let mut history = EventHistory::new();

        let op1 = history.invoke(
            0,
            "n1",
            "key1",
            OpKind::Write {
                value: "v1".to_string(),
            },
        );
        history.ok(0, "n1", op1, "key1", Some("v1".to_string()));

        // Read returns wrong value
        let op2 = history.invoke(1, "n1", "key1", OpKind::Read);
        history.ok(1, "n1", op2, "key1", Some("v2".to_string()));

        let result = history.check_sequential_consistency();
        assert!(result.is_err());
    }

    #[test]
    fn test_simulator_basic() {
        let config = SimulationConfig {
            seed: 42,
            num_nodes: 3,
            max_steps: 100,
            max_runtime_us: 100_000,
            ..Default::default()
        };
        let mut sim = Simulator::new(config);

        assert_eq!(sim.nodes.len(), 3);
        assert!(sim.nodes.contains_key("node-0"));

        sim.write_op("node-0", "key1", "value1").unwrap();
        let val = sim.read_op("node-0", "key1").unwrap();
        assert_eq!(val, Some("value1".to_string()));
    }

    #[test]
    fn test_simulator_node_crash() {
        let config = SimulationConfig {
            seed: 42,
            num_nodes: 2,
            max_steps: 100,
            ..Default::default()
        };
        let mut sim = Simulator::new(config);

        sim.write_op("node-0", "key1", "val1").unwrap();

        // Crash node-0
        sim.schedule_fault(
            FaultKind::NodeCrash {
                node: "node-0".to_string(),
            },
            0,
            None,
        );
        sim.tick();

        let result = sim.write_op("node-0", "key2", "val2");
        assert!(result.is_err());
    }

    #[test]
    fn test_simulator_run() {
        let config = SimulationConfig {
            seed: 42,
            num_nodes: 3,
            max_steps: 50,
            max_runtime_us: 5_000,
            tick_us: 100,
            ..Default::default()
        };
        let mut sim = Simulator::new(config);
        let result = sim.run();

        assert!(result.total_steps <= 50);
        assert!(result.passed());
    }

    #[test]
    fn test_multi_seed_runner() {
        let config = SimulationConfig {
            seed: 1,
            num_nodes: 2,
            max_steps: 20,
            max_runtime_us: 2_000,
            tick_us: 100,
            ..Default::default()
        };

        let runner = MultiSeedRunner::new(config).with_workload(|sim| {
            sim.write_op("node-0", "k", "v").ok();
        });

        let results = runner.run(5);
        assert_eq!(results.len(), 5);
        // Each seed should be different
        let seeds: Vec<u64> = results.iter().map(|r| r.seed).collect();
        assert_eq!(seeds, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_invariant_checker() {
        let config = SimulationConfig {
            seed: 42,
            num_nodes: 2,
            max_steps: 10,
            ..Default::default()
        };
        let sim = Simulator::new(config);

        let mut checker = InvariantChecker::new();
        checker.add("has_nodes", |s| {
            if s.nodes.is_empty() {
                Err("no nodes".to_string())
            } else {
                Ok(())
            }
        });
        checker.add("reasonable_step", |s| {
            if s.step > 1_000_000 {
                Err("too many steps".to_string())
            } else {
                Ok(())
            }
        });

        let violations = checker.check_all(&sim);
        assert!(violations.is_empty());
    }

    #[test]
    fn test_invariant_checker_violation() {
        let config = SimulationConfig {
            seed: 42,
            num_nodes: 0, // no nodes
            max_steps: 10,
            ..Default::default()
        };
        let sim = Simulator::new(config);

        let mut checker = InvariantChecker::new();
        checker.add("has_nodes", |s| {
            if s.nodes.is_empty() {
                Err("no nodes found".to_string())
            } else {
                Ok(())
            }
        });

        let violations = checker.check_all(&sim);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].message.contains("no nodes"));
    }

    #[test]
    fn test_sim_network_message_ordering() {
        let mut net = SimNetwork::new(100);
        let injector = FaultInjector::new();
        let mut rng = SimRng::new(42);

        // Send multiple messages
        net.send("n1", "n2", b"msg1".to_vec(), 0, &injector, &mut rng);
        net.send("n1", "n2", b"msg2".to_vec(), 0, &injector, &mut rng);
        net.send("n1", "n2", b"msg3".to_vec(), 0, &injector, &mut rng);

        // Deliver all
        net.deliver_up_to(1_000_000, &injector);

        let messages = net.recv_all("n2");
        assert_eq!(messages.len(), 3);
    }

    #[test]
    fn test_shared_simulator() {
        let shared = SharedSimulator::new(42);

        assert_eq!(shared.now_us(), 0);
        shared.advance(1_000);
        assert_eq!(shared.now_us(), 1_000);

        let r1 = shared.next_random();
        let r2 = shared.next_random();
        assert_ne!(r1, r2);
    }

    #[test]
    fn test_simulation_result_display() {
        let result = SimulationResult {
            seed: 42,
            total_steps: 100,
            final_time_us: 10_000,
            messages_dropped: 5,
            fault_history: vec![],
            consistency_violations: vec![],
        };
        let s = format!("{}", result);
        assert!(s.contains("seed=42"));
        assert!(s.contains("steps=100"));
        assert!(result.passed());
    }

    #[test]
    fn test_fault_kind_display() {
        let partition = FaultKind::NetworkPartition {
            node_a: "a".to_string(),
            node_b: "b".to_string(),
        };
        assert!(format!("{}", partition).contains("a <-> b"));

        let crash = FaultKind::NodeCrash {
            node: "n1".to_string(),
        };
        assert!(format!("{}", crash).contains("n1"));
    }

    #[test]
    fn test_sim_node_operations() {
        let mut node = SimNode::new("test-node");
        node.apply_write("k1", "v1").unwrap();
        assert_eq!(node.apply_read("k1").unwrap(), Some("v1".to_string()));

        let deleted = node.apply_delete("k1").unwrap();
        assert!(deleted);
        assert_eq!(node.apply_read("k1").unwrap(), None);

        // Crashed node rejects all ops
        node.crashed = true;
        assert!(node.apply_write("k2", "v2").is_err());
        assert!(node.apply_read("k2").is_err());
        assert!(node.apply_delete("k2").is_err());
    }
}
