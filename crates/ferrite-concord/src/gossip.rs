//! Gossip transport for CRDT state propagation.
//!
//! Production: replaces in-process channels with TCP/UDP gossip.
//! This implementation uses channels for local simulation and testing.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

/// A gossip message carrying CRDT delta state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipMessage {
    pub source_node: String,
    pub target_node: String,
    pub key: String,
    pub crdt_type: CrdtType,
    pub payload: Vec<u8>,
    pub timestamp_ms: u64,
    pub sequence: u64,
}

/// CRDT type tag carried inside gossip messages.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CrdtType {
    GCounter,
    PnCounter,
    OrSet,
    LwwRegister,
    MvRegister,
}

/// Gossip configuration.
#[derive(Clone, Copy)]
pub struct GossipConfig {
    /// Number of peers to gossip to per round.
    pub fan_out: usize,
    /// Gossip interval in milliseconds.
    pub interval_ms: u64,
    /// Maximum message payload size in bytes.
    pub max_payload_bytes: usize,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            fan_out: 3,
            interval_ms: 1000,
            max_payload_bytes: 65536,
        }
    }
}

/// In-process gossip node for testing multi-node CRDT convergence.
pub struct GossipNode {
    id: String,
    inbox: Arc<RwLock<Vec<GossipMessage>>>,
    outbox: Arc<RwLock<Vec<GossipMessage>>>,
    peers: Vec<String>,
    config: GossipConfig,
    sequence: AtomicU64,
}

impl GossipNode {
    pub fn new(id: String, config: GossipConfig) -> Self {
        Self {
            id,
            inbox: Arc::new(RwLock::new(Vec::new())),
            outbox: Arc::new(RwLock::new(Vec::new())),
            peers: Vec::new(),
            config,
            sequence: AtomicU64::new(0),
        }
    }

    /// Register a peer node for gossip dissemination.
    pub fn add_peer(&mut self, peer_id: String) {
        if peer_id != self.id && !self.peers.contains(&peer_id) {
            self.peers.push(peer_id);
        }
    }

    /// Enqueue a message to a fan-out subset of peers.
    pub fn send(&self, key: &str, crdt_type: CrdtType, payload: Vec<u8>, now_ms: u64) {
        let seq = self.sequence.fetch_add(1, Ordering::Relaxed);
        let targets: Vec<_> = self
            .peers
            .iter()
            .take(self.config.fan_out)
            .cloned()
            .collect();

        let mut outbox = self.outbox.write().expect("outbox lock poisoned");
        for target in targets {
            let truncated = if payload.len() > self.config.max_payload_bytes {
                payload[..self.config.max_payload_bytes].to_vec()
            } else {
                payload.clone()
            };
            outbox.push(GossipMessage {
                source_node: self.id.clone(),
                target_node: target,
                key: key.to_owned(),
                crdt_type,
                payload: truncated,
                timestamp_ms: now_ms,
                sequence: seq,
            });
        }
    }

    /// Drain all pending outbound messages.
    pub fn drain_outbox(&self) -> Vec<GossipMessage> {
        let mut outbox = self.outbox.write().expect("outbox lock poisoned");
        outbox.drain(..).collect()
    }

    /// Deliver a message to this node's inbox.
    pub fn receive(&self, msg: GossipMessage) {
        let mut inbox = self.inbox.write().expect("inbox lock poisoned");
        inbox.push(msg);
    }

    /// Drain all pending inbound messages.
    pub fn drain_inbox(&self) -> Vec<GossipMessage> {
        let mut inbox = self.inbox.write().expect("inbox lock poisoned");
        inbox.drain(..).collect()
    }

    /// Number of messages waiting in the inbox.
    pub fn pending_count(&self) -> usize {
        self.inbox.read().expect("inbox lock poisoned").len()
    }

    /// Node identifier.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Current peer list.
    pub fn peers(&self) -> &[String] {
        &self.peers
    }
}

/// A gossip cluster for in-process testing.
pub struct GossipCluster {
    nodes: HashMap<String, GossipNode>,
}

impl GossipCluster {
    /// Create a fully-meshed cluster of gossip nodes.
    pub fn new(node_ids: Vec<String>, config: GossipConfig) -> Self {
        let mut nodes: HashMap<String, GossipNode> = node_ids
            .iter()
            .map(|id| (id.clone(), GossipNode::new(id.clone(), config)))
            .collect();

        // Wire full-mesh peers.
        let all_ids: Vec<String> = node_ids.clone();
        for node in nodes.values_mut() {
            for peer_id in &all_ids {
                node.add_peer(peer_id.clone());
            }
        }

        Self { nodes }
    }

    /// Run one gossip round: drain outboxes, deliver to inboxes.
    /// Returns the number of messages delivered.
    pub fn tick(&self) -> usize {
        let mut all_msgs = Vec::new();
        for node in self.nodes.values() {
            all_msgs.extend(node.drain_outbox());
        }
        let count = all_msgs.len();
        for msg in all_msgs {
            if let Some(target_node) = self.nodes.get(&msg.target_node) {
                target_node.receive(msg);
            }
        }
        count
    }

    /// Get a reference to a node by ID.
    pub fn node(&self, id: &str) -> Option<&GossipNode> {
        self.nodes.get(id)
    }

    /// Get a mutable reference to a node by ID.
    pub fn node_mut(&mut self, id: &str) -> Option<&mut GossipNode> {
        self.nodes.get_mut(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cluster_ids() -> Vec<String> {
        vec!["n1".into(), "n2".into(), "n3".into()]
    }

    #[test]
    fn three_node_send_tick_deliver() {
        let cluster = GossipCluster::new(cluster_ids(), GossipConfig::default());

        // n1 sends a message.
        let n1 = cluster.node("n1").unwrap();
        n1.send("key1", CrdtType::GCounter, vec![1, 2, 3], 1000);

        // Tick delivers to peers.
        let delivered = cluster.tick();
        assert_eq!(delivered, 2); // fan_out=3 but only 2 peers

        // n2 and n3 each got the message.
        assert_eq!(cluster.node("n2").unwrap().pending_count(), 1);
        assert_eq!(cluster.node("n3").unwrap().pending_count(), 1);

        // n1 should NOT receive its own message.
        assert_eq!(cluster.node("n1").unwrap().pending_count(), 0);
    }

    #[test]
    fn fan_out_limits_messages() {
        let config = GossipConfig {
            fan_out: 1,
            ..Default::default()
        };
        let ids: Vec<String> = (0..5).map(|i| format!("n{i}")).collect();
        let cluster = GossipCluster::new(ids, config);

        cluster
            .node("n0")
            .unwrap()
            .send("k", CrdtType::PnCounter, vec![42], 1000);

        let delivered = cluster.tick();
        assert_eq!(delivered, 1); // fan_out = 1
    }

    #[test]
    fn drain_inbox_outbox() {
        let cluster = GossipCluster::new(cluster_ids(), GossipConfig::default());

        let n1 = cluster.node("n1").unwrap();
        n1.send("k", CrdtType::OrSet, vec![10], 500);

        // Outbox has messages before tick.
        let out = n1.drain_outbox();
        assert_eq!(out.len(), 2);

        // After drain, outbox is empty.
        assert!(n1.drain_outbox().is_empty());

        // Manually deliver one message.
        let n2 = cluster.node("n2").unwrap();
        n2.receive(out[0].clone());
        assert_eq!(n2.pending_count(), 1);

        // Drain inbox.
        let inbox = n2.drain_inbox();
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].key, "k");
        assert_eq!(inbox[0].crdt_type, CrdtType::OrSet);

        // Inbox is empty after drain.
        assert_eq!(n2.pending_count(), 0);
    }

    #[test]
    fn message_payload_truncated() {
        let config = GossipConfig {
            max_payload_bytes: 4,
            ..Default::default()
        };
        let cluster = GossipCluster::new(cluster_ids(), config);
        let n1 = cluster.node("n1").unwrap();
        n1.send("k", CrdtType::LwwRegister, vec![0; 100], 1);

        let msgs = n1.drain_outbox();
        for m in &msgs {
            assert_eq!(m.payload.len(), 4);
        }
    }

    #[test]
    fn add_peer_ignores_self_and_duplicates() {
        let mut node = GossipNode::new("n1".into(), GossipConfig::default());
        node.add_peer("n1".into()); // self — ignored
        node.add_peer("n2".into());
        node.add_peer("n2".into()); // duplicate — ignored
        assert_eq!(node.peers().len(), 1);
        assert_eq!(node.peers()[0], "n2");
    }
}
