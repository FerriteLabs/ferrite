//! CRDT-based synchronization protocol for edge-cloud data sync.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
