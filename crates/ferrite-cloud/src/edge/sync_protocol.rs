//! Delta-state CRDT synchronization protocol for Edge nodes.
//!
//! Implements a pull-push sync protocol where edge nodes periodically
//! exchange delta states with the upstream cluster. Uses vector clocks
//! for causal consistency and delta compression for bandwidth efficiency.
//!
//! # Protocol Flow
//!
//! ```text
//! Edge Node                    Upstream Cluster
//!     │                              │
//!     │── SYNC_REQUEST (vc, deltas)─►│
//!     │                              │  merge deltas
//!     │◄─ SYNC_RESPONSE (vc, deltas)─│  compute response deltas
//!     │  merge response              │
//!     │  update local vc             │
//!     │                              │
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

// ---------------------------------------------------------------------------
// Legacy message types (preserved for backward compatibility)
// ---------------------------------------------------------------------------

/// A sync message exchanged between edge and cloud.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncMessage {
    /// Source node ID.
    pub source_node: String,
    /// Message type.
    pub msg_type: SyncMessageType,
    /// Vector clock for causal ordering.
    pub vector_clock: HashMap<String, u64>,
    /// Payload operations.
    pub operations: Vec<SyncOperation>,
    /// Timestamp of the message.
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Type of sync message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncMessageType {
    /// Full state sync.
    FullSync,
    /// Delta/incremental sync.
    DeltaSync,
    /// Acknowledgement.
    Ack,
    /// Conflict report.
    ConflictReport,
}

/// A single sync operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncOperation {
    pub key: String,
    pub value: Option<Vec<u8>>,
    pub operation: SyncOpType,
    pub timestamp: u64,
    pub node_id: String,
}

/// Type of sync operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncOpType {
    Set,
    Delete,
    Merge,
}

/// Result of a sync session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResult {
    pub operations_sent: u64,
    pub operations_received: u64,
    pub conflicts_detected: u64,
    pub conflicts_resolved: u64,
    pub bytes_transferred: u64,
    pub duration_ms: u64,
}

// ---------------------------------------------------------------------------
// Vector clock
// ---------------------------------------------------------------------------

/// Vector clock for causal ordering across nodes.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VectorClock {
    clocks: HashMap<String, u64>,
}

impl VectorClock {
    pub fn new() -> Self {
        Self::default()
    }

    /// Increments the clock for the given node.
    pub fn increment(&mut self, node_id: &str) {
        let counter = self.clocks.entry(node_id.to_string()).or_insert(0);
        *counter += 1;
    }

    /// Returns the clock value for a node.
    pub fn get(&self, node_id: &str) -> u64 {
        self.clocks.get(node_id).copied().unwrap_or(0)
    }

    /// Merges another vector clock (takes max of each component).
    pub fn merge(&mut self, other: &VectorClock) {
        for (node, &value) in &other.clocks {
            let entry = self.clocks.entry(node.clone()).or_insert(0);
            *entry = (*entry).max(value);
        }
    }

    /// Returns true if this clock is causally before or concurrent with another.
    pub fn happens_before(&self, other: &VectorClock) -> bool {
        let mut at_least_one_less = false;
        for (node, &value) in &self.clocks {
            let other_value = other.get(node);
            if value > other_value {
                return false;
            }
            if value < other_value {
                at_least_one_less = true;
            }
        }
        // Check nodes in other but not in self
        for node in other.clocks.keys() {
            if !self.clocks.contains_key(node) {
                at_least_one_less = true;
            }
        }
        at_least_one_less
    }

    /// Returns true if the two clocks are concurrent (neither happens before the other).
    pub fn is_concurrent(&self, other: &VectorClock) -> bool {
        !self.happens_before(other) && !other.happens_before(self)
    }

    /// Returns a clone of the internal map.
    pub fn to_map(&self) -> HashMap<String, u64> {
        self.clocks.clone()
    }

    /// Create a VectorClock from a map.
    pub fn from_map(clocks: HashMap<String, u64>) -> Self {
        Self { clocks }
    }
}

// ---------------------------------------------------------------------------
// Delta-state sync protocol types
// ---------------------------------------------------------------------------

/// A delta operation to be synced between nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncDelta {
    /// Key affected by this delta.
    pub key: String,
    /// Type of CRDT operation.
    pub operation: DeltaOperation,
    /// Vector clock at the time of the operation.
    pub vector_clock: HashMap<String, u64>,
    /// Timestamp of the operation.
    pub timestamp: SystemTime,
    /// Originating node ID.
    pub origin_node: String,
}

/// Types of delta operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeltaOperation {
    /// Set a value (LWW semantics).
    Set { value: Vec<u8> },
    /// Delete a key.
    Delete,
    /// Increment a counter.
    Increment { amount: i64 },
    /// Add to a set.
    SetAdd { member: String },
    /// Remove from a set.
    SetRemove { member: String },
    /// Merge a full CRDT state (for initial sync).
    FullState { data: Vec<u8> },
}

/// Sync request sent from edge to upstream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncRequest {
    /// Edge node identifier.
    pub node_id: String,
    /// Edge node's current vector clock.
    pub vector_clock: HashMap<String, u64>,
    /// Deltas accumulated since last sync.
    pub deltas: Vec<SyncDelta>,
    /// Whether this is an initial sync (full state requested).
    pub initial_sync: bool,
    /// Compression used for deltas.
    pub compression: Compression,
    /// Sequence number for ordering.
    pub sequence: u64,
}

/// Sync response from upstream to edge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResponse {
    /// Upstream's vector clock after merge.
    pub vector_clock: HashMap<String, u64>,
    /// Deltas from upstream that edge needs.
    pub deltas: Vec<SyncDelta>,
    /// Number of conflicts resolved.
    pub conflicts_resolved: u64,
    /// Acknowledged sequence number.
    pub ack_sequence: u64,
    /// Status of the sync.
    pub status: SyncStatus,
}

/// Compression type for sync traffic.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Compression {
    None,
    Lz4,
    Zstd,
}

impl Default for Compression {
    fn default() -> Self {
        Self::Lz4
    }
}

/// Status of a sync operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SyncStatus {
    Ok,
    PartialSync,
    ConflictsDetected,
    Error(String),
}

// ---------------------------------------------------------------------------
// Delta buffer
// ---------------------------------------------------------------------------

/// Delta buffer that accumulates changes between sync cycles.
pub struct DeltaBuffer {
    /// Pending deltas to send.
    deltas: Vec<SyncDelta>,
    /// Maximum buffer size before forced sync.
    max_buffer_size: usize,
    /// Local vector clock.
    vector_clock: HashMap<String, u64>,
    /// Node ID for this buffer.
    node_id: String,
    /// Next sequence number.
    sequence: u64,
}

impl DeltaBuffer {
    pub fn new(node_id: String, max_buffer_size: usize) -> Self {
        Self {
            deltas: Vec::new(),
            max_buffer_size,
            vector_clock: HashMap::new(),
            node_id,
            sequence: 0,
        }
    }

    /// Record a SET operation.
    pub fn record_set(&mut self, key: &str, value: Vec<u8>) {
        self.increment_clock();
        self.deltas.push(SyncDelta {
            key: key.to_string(),
            operation: DeltaOperation::Set { value },
            vector_clock: self.vector_clock.clone(),
            timestamp: SystemTime::now(),
            origin_node: self.node_id.clone(),
        });
    }

    /// Record a DELETE operation.
    pub fn record_delete(&mut self, key: &str) {
        self.increment_clock();
        self.deltas.push(SyncDelta {
            key: key.to_string(),
            operation: DeltaOperation::Delete,
            vector_clock: self.vector_clock.clone(),
            timestamp: SystemTime::now(),
            origin_node: self.node_id.clone(),
        });
    }

    /// Record a counter increment.
    pub fn record_increment(&mut self, key: &str, amount: i64) {
        self.increment_clock();
        self.deltas.push(SyncDelta {
            key: key.to_string(),
            operation: DeltaOperation::Increment { amount },
            vector_clock: self.vector_clock.clone(),
            timestamp: SystemTime::now(),
            origin_node: self.node_id.clone(),
        });
    }

    /// Record a set add.
    pub fn record_set_add(&mut self, key: &str, member: String) {
        self.increment_clock();
        self.deltas.push(SyncDelta {
            key: key.to_string(),
            operation: DeltaOperation::SetAdd { member },
            vector_clock: self.vector_clock.clone(),
            timestamp: SystemTime::now(),
            origin_node: self.node_id.clone(),
        });
    }

    /// Check if buffer is full and needs sync.
    pub fn needs_sync(&self) -> bool {
        self.deltas.len() >= self.max_buffer_size
    }

    /// Take all deltas and create a sync request.
    pub fn drain_to_request(&mut self) -> SyncRequest {
        self.sequence += 1;
        SyncRequest {
            node_id: self.node_id.clone(),
            vector_clock: self.vector_clock.clone(),
            deltas: std::mem::take(&mut self.deltas),
            initial_sync: self.sequence == 1,
            compression: Compression::default(),
            sequence: self.sequence,
        }
    }

    /// Apply a sync response (merge upstream's vector clock and deltas).
    pub fn apply_response(&mut self, response: &SyncResponse) {
        // Merge vector clocks (element-wise max)
        for (region, &clock) in &response.vector_clock {
            let entry = self.vector_clock.entry(region.clone()).or_insert(0);
            *entry = (*entry).max(clock);
        }
    }

    /// Get current buffer size.
    pub fn pending_count(&self) -> usize {
        self.deltas.len()
    }

    /// Get current vector clock.
    pub fn vector_clock(&self) -> &HashMap<String, u64> {
        &self.vector_clock
    }

    fn increment_clock(&mut self) {
        let counter = self.vector_clock.entry(self.node_id.clone()).or_insert(0);
        *counter += 1;
    }
}

// ---------------------------------------------------------------------------
// Sync session
// ---------------------------------------------------------------------------

/// Sync session managing the connection to upstream.
pub struct SyncSession {
    /// Node ID.
    pub node_id: String,
    /// Upstream endpoint.
    pub upstream_endpoint: String,
    /// Delta buffer.
    pub buffer: DeltaBuffer,
    /// Sync interval.
    pub interval: Duration,
    /// Statistics.
    pub total_syncs: u64,
    pub failed_syncs: u64,
    pub total_deltas_sent: u64,
    pub total_deltas_received: u64,
    pub total_conflicts: u64,
    pub last_sync: Option<SystemTime>,
}

impl SyncSession {
    pub fn new(node_id: String, upstream_endpoint: String, interval: Duration) -> Self {
        Self {
            node_id: node_id.clone(),
            upstream_endpoint,
            buffer: DeltaBuffer::new(node_id, 1000),
            interval,
            total_syncs: 0,
            failed_syncs: 0,
            total_deltas_sent: 0,
            total_deltas_received: 0,
            total_conflicts: 0,
            last_sync: None,
        }
    }

    /// Execute one sync cycle.
    ///
    /// In production, this would:
    /// 1. Serialize the sync request
    /// 2. Send it over TCP/QUIC to the upstream
    /// 3. Receive and deserialize the response
    /// 4. Apply the response deltas locally
    ///
    /// For now, returns a simulated successful response.
    pub fn sync_cycle(&mut self) -> Result<SyncResponse, SyncError> {
        let request = self.buffer.drain_to_request();
        let deltas_sent = request.deltas.len() as u64;

        // Simulate upstream response
        let response = SyncResponse {
            vector_clock: request.vector_clock.clone(),
            deltas: vec![],
            conflicts_resolved: 0,
            ack_sequence: request.sequence,
            status: SyncStatus::Ok,
        };

        self.buffer.apply_response(&response);
        self.total_syncs += 1;
        self.total_deltas_sent += deltas_sent;
        self.total_deltas_received += response.deltas.len() as u64;
        self.total_conflicts += response.conflicts_resolved;
        self.last_sync = Some(SystemTime::now());

        Ok(response)
    }

    /// Get session statistics.
    pub fn stats(&self) -> SyncSessionStats {
        SyncSessionStats {
            node_id: self.node_id.clone(),
            upstream: self.upstream_endpoint.clone(),
            total_syncs: self.total_syncs,
            failed_syncs: self.failed_syncs,
            total_deltas_sent: self.total_deltas_sent,
            total_deltas_received: self.total_deltas_received,
            total_conflicts: self.total_conflicts,
            pending_deltas: self.buffer.pending_count() as u64,
            last_sync: self.last_sync,
            interval: self.interval,
        }
    }
}

/// Sync session statistics.
#[derive(Debug, Clone)]
pub struct SyncSessionStats {
    pub node_id: String,
    pub upstream: String,
    pub total_syncs: u64,
    pub failed_syncs: u64,
    pub total_deltas_sent: u64,
    pub total_deltas_received: u64,
    pub total_conflicts: u64,
    pub pending_deltas: u64,
    pub last_sync: Option<SystemTime>,
    pub interval: Duration,
}

// ---------------------------------------------------------------------------
// Sync error
// ---------------------------------------------------------------------------

/// Sync error type.
#[derive(Debug, Clone)]
pub enum SyncError {
    ConnectionFailed(String),
    Timeout,
    SerializationError(String),
    UpstreamError(String),
}

impl std::fmt::Display for SyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectionFailed(msg) => write!(f, "connection failed: {}", msg),
            Self::Timeout => write!(f, "sync timeout"),
            Self::SerializationError(msg) => write!(f, "serialization error: {}", msg),
            Self::UpstreamError(msg) => write!(f, "upstream error: {}", msg),
        }
    }
}

impl std::error::Error for SyncError {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- Vector clock tests ---

    #[test]
    fn test_vector_clock_increment() {
        let mut vc = VectorClock::new();
        vc.increment("node-1");
        vc.increment("node-1");
        vc.increment("node-2");

        assert_eq!(vc.get("node-1"), 2);
        assert_eq!(vc.get("node-2"), 1);
        assert_eq!(vc.get("node-3"), 0);
    }

    #[test]
    fn test_vector_clock_merge() {
        let mut vc1 = VectorClock::new();
        vc1.increment("a");
        vc1.increment("a");

        let mut vc2 = VectorClock::new();
        vc2.increment("a");
        vc2.increment("b");
        vc2.increment("b");

        vc1.merge(&vc2);
        assert_eq!(vc1.get("a"), 2);
        assert_eq!(vc1.get("b"), 2);
    }

    #[test]
    fn test_happens_before() {
        let mut vc1 = VectorClock::new();
        vc1.increment("a");

        let mut vc2 = VectorClock::new();
        vc2.increment("a");
        vc2.increment("a");

        assert!(vc1.happens_before(&vc2));
        assert!(!vc2.happens_before(&vc1));
    }

    #[test]
    fn test_concurrent() {
        let mut vc1 = VectorClock::new();
        vc1.increment("a");

        let mut vc2 = VectorClock::new();
        vc2.increment("b");

        assert!(vc1.is_concurrent(&vc2));
    }

    #[test]
    fn test_sync_message_serialization() {
        let msg = SyncMessage {
            source_node: "edge-1".to_string(),
            msg_type: SyncMessageType::DeltaSync,
            vector_clock: HashMap::new(),
            operations: vec![SyncOperation {
                key: "k1".to_string(),
                value: Some(b"v1".to_vec()),
                operation: SyncOpType::Set,
                timestamp: 12345,
                node_id: "edge-1".to_string(),
            }],
            timestamp: chrono::Utc::now(),
        };

        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: SyncMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.source_node, "edge-1");
        assert_eq!(deserialized.operations.len(), 1);
    }

    // --- Delta buffer tests ---

    #[test]
    fn test_delta_buffer_record_set() {
        let mut buf = DeltaBuffer::new("edge-1".into(), 100);
        buf.record_set("key1", b"value1".to_vec());
        assert_eq!(buf.pending_count(), 1);
        assert_eq!(buf.vector_clock()["edge-1"], 1);
    }

    #[test]
    fn test_delta_buffer_drain() {
        let mut buf = DeltaBuffer::new("edge-1".into(), 100);
        buf.record_set("k1", b"v1".to_vec());
        buf.record_delete("k2");
        let req = buf.drain_to_request();
        assert_eq!(req.deltas.len(), 2);
        assert_eq!(buf.pending_count(), 0);
        assert_eq!(req.sequence, 1);
    }

    #[test]
    fn test_delta_buffer_needs_sync() {
        let mut buf = DeltaBuffer::new("edge-1".into(), 2);
        assert!(!buf.needs_sync());
        buf.record_set("k1", b"v1".to_vec());
        assert!(!buf.needs_sync());
        buf.record_set("k2", b"v2".to_vec());
        assert!(buf.needs_sync());
    }

    #[test]
    fn test_sync_session_cycle() {
        let mut session = SyncSession::new(
            "edge-1".into(),
            "ferrite://upstream:6379".into(),
            Duration::from_secs(5),
        );
        session.buffer.record_set("key1", b"val".to_vec());
        let result = session.sync_cycle();
        assert!(result.is_ok());
        assert_eq!(session.total_syncs, 1);
        assert_eq!(session.total_deltas_sent, 1);
    }

    #[test]
    fn test_sync_session_stats() {
        let session = SyncSession::new(
            "edge-1".into(),
            "ferrite://upstream:6379".into(),
            Duration::from_secs(5),
        );
        let stats = session.stats();
        assert_eq!(stats.total_syncs, 0);
        assert_eq!(stats.pending_deltas, 0);
    }

    #[test]
    fn test_apply_response_merges_clocks() {
        let mut buf = DeltaBuffer::new("edge-1".into(), 100);
        buf.record_set("k1", b"v1".to_vec());
        let response = SyncResponse {
            vector_clock: HashMap::from([
                ("upstream".into(), 5),
                ("edge-1".into(), 0),
            ]),
            deltas: vec![],
            conflicts_resolved: 0,
            ack_sequence: 1,
            status: SyncStatus::Ok,
        };
        buf.apply_response(&response);
        assert_eq!(buf.vector_clock()["edge-1"], 1); // max(1, 0)
        assert_eq!(buf.vector_clock()["upstream"], 5);
    }

    #[test]
    fn test_compression_default() {
        assert_eq!(Compression::default(), Compression::Lz4);
    }

    #[test]
    fn test_sync_error_display() {
        let e = SyncError::Timeout;
        assert!(e.to_string().contains("timeout"));
    }
}
