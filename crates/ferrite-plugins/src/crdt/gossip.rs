//! # Gossip Protocol for CRDT State Propagation
//!
//! Implements an efficient gossip protocol for delta-state CRDT propagation
//! across regions. Peers exchange compact delta messages filtered by vector
//! clock comparison, ensuring each node only receives updates it hasn't seen.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────┐       gossip        ┌──────────────────────┐
//! │  Node A              │◄───────────────────►│  Node B              │
//! │  ┌────────────────┐  │                     │  ┌────────────────┐  │
//! │  │ Pending Deltas  │  │   GossipMessage     │  │ Pending Deltas  │  │
//! │  ├────────────────┤  │   (filtered by VC)  │  ├────────────────┤  │
//! │  │ Vector Clocks   │  │                     │  │ Vector Clocks   │  │
//! │  ├────────────────┤  │                     │  ├────────────────┤  │
//! │  │ Peer State      │  │                     │  │ Peer State      │  │
//! │  └────────────────┘  │                     │  └────────────────┘  │
//! └──────────────────────┘                     └──────────────────────┘
//! ```
//!
//! # Example
//!
//! ```no_run
//! use ferrite_plugins::crdt::gossip::{GossipProtocol, GossipConfig};
//! use ferrite_plugins::crdt::CrdtType;
//!
//! let config = GossipConfig::default();
//! let node = GossipProtocol::new("node-1".to_string(), config);
//!
//! // Register peers
//! node.add_peer("node-2", "10.0.0.2:6380");
//! node.add_peer("node-3", "10.0.0.3:6380");
//!
//! // Record a local mutation
//! node.record_local_mutation("key:1", CrdtType::PNCounter, vec![1, 2, 3]);
//!
//! // Build a gossip message for a peer
//! if let Some(msg) = node.prepare_gossip_message("node-2") {
//!     // Send msg over the network...
//! }
//! ```

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::sync::VectorClock;
use super::types::CrdtType;

// Re-export CrdtType so downstream can use it via gossip module
pub use super::types::CrdtType as GossipCrdtType;

/// Configuration for the gossip protocol.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GossipConfig {
    /// Number of peers to gossip with per round.
    pub fanout: usize,
    /// Interval between gossip rounds in milliseconds.
    pub gossip_interval_ms: u64,
    /// Maximum number of deltas buffered before oldest are dropped.
    pub max_delta_buffer: usize,
    /// Maximum gossip message size in bytes.
    pub max_message_size: usize,
    /// Peer timeout in milliseconds — peers not heard from within this
    /// window are marked as dead.
    pub peer_timeout_ms: u64,
    /// Whether to enable delta compression.
    pub enable_delta_compression: bool,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            fanout: 3,
            gossip_interval_ms: 1000,
            max_delta_buffer: 10_000,
            max_message_size: 1_048_576, // 1 MB
            peer_timeout_ms: 30_000,
            enable_delta_compression: true,
        }
    }
}

/// A delta representing a single CRDT mutation to propagate.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CrdtDelta {
    /// Key the mutation applies to.
    pub key: String,
    /// Type of the CRDT.
    pub crdt_type: CrdtType,
    /// Serialized delta payload.
    pub delta_bytes: Vec<u8>,
    /// Node that originated the mutation.
    pub origin_node: String,
    /// Wall-clock timestamp (epoch millis).
    pub timestamp: u64,
    /// Vector clock at the time of the mutation.
    pub version: VectorClock,
}

/// A gossip message exchanged between peers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GossipMessage {
    /// Sender node ID.
    pub from_node: String,
    /// Intended recipient node ID.
    pub to_node: String,
    /// Delta mutations included in this message.
    pub deltas: Vec<CrdtDelta>,
    /// Sender's current vector clock.
    pub sender_version: VectorClock,
    /// Gossip round number.
    pub round: u64,
    /// Wall-clock timestamp (epoch millis).
    pub timestamp: u64,
}

/// Result of processing a received gossip message.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GossipReceiveResult {
    /// Number of deltas successfully applied.
    pub applied: usize,
    /// Number of deltas skipped (already seen).
    pub skipped: usize,
    /// Keys that were seen for the first time.
    pub new_keys: Vec<String>,
    /// Number of concurrent/conflicting deltas detected.
    pub conflicts: usize,
}

/// State tracked per known peer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerState {
    /// Peer's node ID.
    pub peer_id: String,
    /// Network endpoint for this peer.
    pub endpoint: String,
    /// Last time we heard from this peer (epoch millis).
    pub last_seen: u64,
    /// Last gossip round involving this peer.
    pub last_gossip_round: u64,
    /// Total messages sent to this peer.
    pub messages_sent: u64,
    /// Total messages received from this peer.
    pub messages_received: u64,
    /// Whether the peer is considered alive.
    pub is_alive: bool,
}

/// Atomic counters for gossip protocol statistics.
pub struct GossipStats {
    rounds_completed: AtomicU64,
    messages_sent: AtomicU64,
    messages_received: AtomicU64,
    deltas_sent: AtomicU64,
    deltas_received: AtomicU64,
    deltas_applied: AtomicU64,
    deltas_skipped: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
}

impl GossipStats {
    fn new() -> Self {
        Self {
            rounds_completed: AtomicU64::new(0),
            messages_sent: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            deltas_sent: AtomicU64::new(0),
            deltas_received: AtomicU64::new(0),
            deltas_applied: AtomicU64::new(0),
            deltas_skipped: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> GossipStatsSnapshot {
        GossipStatsSnapshot {
            rounds_completed: self.rounds_completed.load(Ordering::Relaxed),
            messages_sent: self.messages_sent.load(Ordering::Relaxed),
            messages_received: self.messages_received.load(Ordering::Relaxed),
            deltas_sent: self.deltas_sent.load(Ordering::Relaxed),
            deltas_received: self.deltas_received.load(Ordering::Relaxed),
            deltas_applied: self.deltas_applied.load(Ordering::Relaxed),
            deltas_skipped: self.deltas_skipped.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of gossip statistics.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GossipStatsSnapshot {
    /// Total gossip rounds completed.
    pub rounds_completed: u64,
    /// Total messages sent.
    pub messages_sent: u64,
    /// Total messages received.
    pub messages_received: u64,
    /// Total deltas sent.
    pub deltas_sent: u64,
    /// Total deltas received.
    pub deltas_received: u64,
    /// Total deltas applied locally.
    pub deltas_applied: u64,
    /// Total deltas skipped (already seen).
    pub deltas_skipped: u64,
    /// Total bytes sent.
    pub bytes_sent: u64,
    /// Total bytes received.
    pub bytes_received: u64,
}

/// Gossip protocol manager for CRDT delta-state propagation.
///
/// Maintains peer state, buffers pending deltas, and constructs/processes
/// gossip messages filtered by vector clock comparison so each peer only
/// receives mutations it has not yet seen.
pub struct GossipProtocol {
    /// Protocol configuration.
    config: GossipConfig,
    /// This node's unique identifier.
    node_id: String,
    /// Known peers and their tracked state.
    peers: RwLock<HashMap<String, PeerState>>,
    /// Queue of deltas waiting to be propagated.
    pending_deltas: RwLock<VecDeque<CrdtDelta>>,
    /// Last known vector clock per peer (what they've acknowledged).
    received_versions: RwLock<HashMap<String, VectorClock>>,
    /// This node's own vector clock.
    local_version: RwLock<VectorClock>,
    /// Current gossip round counter.
    current_round: AtomicU64,
    /// Atomic statistics counters.
    stats: GossipStats,
}

impl GossipProtocol {
    /// Create a new gossip protocol instance for the given node.
    pub fn new(node_id: String, config: GossipConfig) -> Self {
        Self {
            config,
            node_id,
            peers: RwLock::new(HashMap::new()),
            pending_deltas: RwLock::new(VecDeque::new()),
            received_versions: RwLock::new(HashMap::new()),
            local_version: RwLock::new(VectorClock::new()),
            current_round: AtomicU64::new(0),
            stats: GossipStats::new(),
        }
    }

    /// Register a new peer with the given endpoint.
    pub fn add_peer(&self, peer_id: &str, endpoint: &str) {
        let state = PeerState {
            peer_id: peer_id.to_string(),
            endpoint: endpoint.to_string(),
            last_seen: 0,
            last_gossip_round: 0,
            messages_sent: 0,
            messages_received: 0,
            is_alive: true,
        };
        self.peers.write().insert(peer_id.to_string(), state);
        self.received_versions
            .write()
            .insert(peer_id.to_string(), VectorClock::new());
    }

    /// Remove a peer by ID.
    pub fn remove_peer(&self, peer_id: &str) {
        self.peers.write().remove(peer_id);
        self.received_versions.write().remove(peer_id);
    }

    /// Record a local CRDT mutation as a delta to be propagated.
    ///
    /// The delta is appended to the pending buffer and the local vector
    /// clock is incremented. If the buffer exceeds `max_delta_buffer`,
    /// the oldest delta is dropped.
    pub fn record_local_mutation(&self, key: &str, crdt_type: CrdtType, delta: Vec<u8>) {
        let mut local_version = self.local_version.write();
        local_version.increment(&self.node_id);

        let crdt_delta = CrdtDelta {
            key: key.to_string(),
            crdt_type,
            delta_bytes: delta,
            origin_node: self.node_id.clone(),
            timestamp: current_timestamp_ms(),
            version: local_version.clone(),
        };

        let mut pending = self.pending_deltas.write();
        pending.push_back(crdt_delta);

        // Evict oldest deltas when buffer is full
        while pending.len() > self.config.max_delta_buffer {
            pending.pop_front();
        }
    }

    /// Build a gossip message for the target peer containing only deltas
    /// the peer has not yet seen (based on vector clock comparison).
    ///
    /// Returns `None` if there are no new deltas for the peer.
    pub fn prepare_gossip_message(&self, target_peer: &str) -> Option<GossipMessage> {
        let peers = self.peers.read();
        if !peers.contains_key(target_peer) {
            return None;
        }
        drop(peers);

        let received_versions = self.received_versions.read();
        let peer_version = received_versions
            .get(target_peer)
            .cloned()
            .unwrap_or_default();
        drop(received_versions);

        let pending = self.pending_deltas.read();
        let mut deltas: Vec<CrdtDelta> = Vec::new();
        let mut total_size: usize = 0;

        for delta in pending.iter() {
            // Only include deltas the peer hasn't seen
            if !peer_version_dominates(&peer_version, &delta.version) {
                let delta_size = delta.delta_bytes.len();
                if total_size + delta_size > self.config.max_message_size {
                    break;
                }
                total_size += delta_size;
                deltas.push(delta.clone());
            }
        }

        if deltas.is_empty() {
            return None;
        }

        let round = self.current_round.fetch_add(1, Ordering::Relaxed) + 1;
        let local_version = self.local_version.read().clone();

        let delta_count = deltas.len() as u64;
        self.stats
            .messages_sent
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .deltas_sent
            .fetch_add(delta_count, Ordering::Relaxed);
        self.stats
            .bytes_sent
            .fetch_add(total_size as u64, Ordering::Relaxed);
        self.stats
            .rounds_completed
            .fetch_add(1, Ordering::Relaxed);

        // Update peer state
        let mut peers = self.peers.write();
        if let Some(peer) = peers.get_mut(target_peer) {
            peer.last_gossip_round = round;
            peer.messages_sent += 1;
        }

        Some(GossipMessage {
            from_node: self.node_id.clone(),
            to_node: target_peer.to_string(),
            deltas,
            sender_version: local_version,
            round,
            timestamp: current_timestamp_ms(),
        })
    }

    /// Process an incoming gossip message from a peer.
    ///
    /// Deltas whose version is already dominated by our local vector clock
    /// are skipped. New deltas are applied and the local clock is merged.
    pub fn receive_gossip_message(&self, message: GossipMessage) -> GossipReceiveResult {
        let mut result = GossipReceiveResult::default();
        let mut local_version = self.local_version.write();
        let mut bytes_received: u64 = 0;

        // Track keys we've seen before this message
        let known_keys: std::collections::HashSet<String> = {
            let pending = self.pending_deltas.read();
            pending.iter().map(|d| d.key.clone()).collect()
        };

        for delta in &message.deltas {
            bytes_received += delta.delta_bytes.len() as u64;

            if peer_version_dominates(&local_version, &delta.version) {
                // We've already seen this delta
                result.skipped += 1;
            } else if local_version.is_concurrent(&delta.version) {
                // Concurrent update — apply but record conflict
                result.applied += 1;
                result.conflicts += 1;
                if !known_keys.contains(&delta.key) {
                    result.new_keys.push(delta.key.clone());
                }
                local_version.merge(&delta.version);
                self.pending_deltas.write().push_back(delta.clone());
            } else {
                // Causally newer — apply normally
                result.applied += 1;
                if !known_keys.contains(&delta.key) {
                    result.new_keys.push(delta.key.clone());
                }
                local_version.merge(&delta.version);
                self.pending_deltas.write().push_back(delta.clone());
            }
        }

        // Evict oldest deltas when buffer is full
        {
            let mut pending = self.pending_deltas.write();
            while pending.len() > self.config.max_delta_buffer {
                pending.pop_front();
            }
        }

        // Update sender's known version
        self.received_versions
            .write()
            .insert(message.from_node.clone(), message.sender_version);

        // Update peer state
        {
            let mut peers = self.peers.write();
            if let Some(peer) = peers.get_mut(&message.from_node) {
                peer.last_seen = current_timestamp_ms();
                peer.last_gossip_round = message.round;
                peer.messages_received += 1;
                peer.is_alive = true;
            }
        }

        // Update stats
        self.stats
            .messages_received
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .deltas_received
            .fetch_add(message.deltas.len() as u64, Ordering::Relaxed);
        self.stats
            .deltas_applied
            .fetch_add(result.applied as u64, Ordering::Relaxed);
        self.stats
            .deltas_skipped
            .fetch_add(result.skipped as u64, Ordering::Relaxed);
        self.stats
            .bytes_received
            .fetch_add(bytes_received, Ordering::Relaxed);

        result
    }

    /// Select peers to gossip with using deterministic hash-based selection.
    ///
    /// Peers are sorted by a hash of `(peer_id, current_round)` and the
    /// first `fanout` alive peers are returned.
    pub fn select_gossip_targets(&self, fanout: usize) -> Vec<String> {
        let peers = self.peers.read();
        let round = self.current_round.load(Ordering::Relaxed);

        let mut alive: Vec<(&String, u64)> = peers
            .keys()
            .filter(|id| peers.get(*id).is_some_and(|p| p.is_alive))
            .map(|id| {
                let hash = deterministic_hash(id, round);
                (id, hash)
            })
            .collect();

        alive.sort_by_key(|(_, h)| *h);
        alive
            .into_iter()
            .take(fanout)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Return the number of known peers.
    pub fn peer_count(&self) -> usize {
        self.peers.read().len()
    }

    /// Return the number of pending deltas in the buffer.
    pub fn pending_delta_count(&self) -> usize {
        self.pending_deltas.read().len()
    }

    /// Take a point-in-time snapshot of the gossip statistics.
    pub fn stats(&self) -> GossipStatsSnapshot {
        self.stats.snapshot()
    }

    /// Mark peers as dead if they haven't been heard from within the
    /// configured timeout window.
    pub fn detect_peer_timeouts(&self) {
        let now = current_timestamp_ms();
        let timeout = self.config.peer_timeout_ms;
        let mut peers = self.peers.write();
        for peer in peers.values_mut() {
            if peer.last_seen > 0 && now.saturating_sub(peer.last_seen) > timeout {
                peer.is_alive = false;
            }
        }
    }
}

/// Check whether `known` dominates `candidate` — i.e., all entries in
/// `candidate` are ≤ the corresponding entries in `known`.
fn peer_version_dominates(known: &VectorClock, candidate: &VectorClock) -> bool {
    candidate.happened_before(known) || known == candidate
}

/// Deterministic hash for peer selection (FNV-1a inspired).
fn deterministic_hash(peer_id: &str, round: u64) -> u64 {
    let mut hash: u64 = 14695981039346656037;
    for byte in peer_id.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    // Mix in round number
    hash ^= round;
    hash = hash.wrapping_mul(1099511628211);
    hash
}

/// Current wall-clock time in milliseconds since epoch.
fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_protocol(node_id: &str) -> GossipProtocol {
        GossipProtocol::new(node_id.to_string(), GossipConfig::default())
    }

    #[test]
    fn test_add_and_remove_peers() {
        let proto = make_protocol("node-1");

        proto.add_peer("node-2", "10.0.0.2:6380");
        proto.add_peer("node-3", "10.0.0.3:6380");
        assert_eq!(proto.peer_count(), 2);

        proto.remove_peer("node-2");
        assert_eq!(proto.peer_count(), 1);

        // Removing non-existent peer is a no-op
        proto.remove_peer("node-99");
        assert_eq!(proto.peer_count(), 1);
    }

    #[test]
    fn test_record_local_mutation_adds_to_delta_buffer() {
        let proto = make_protocol("node-1");

        assert_eq!(proto.pending_delta_count(), 0);
        proto.record_local_mutation("key:1", CrdtType::GCounter, vec![1, 2, 3]);
        assert_eq!(proto.pending_delta_count(), 1);

        proto.record_local_mutation("key:2", CrdtType::PNCounter, vec![4, 5]);
        assert_eq!(proto.pending_delta_count(), 2);

        // Verify delta contents
        let pending = proto.pending_deltas.read();
        assert_eq!(pending[0].key, "key:1");
        assert_eq!(pending[0].crdt_type, CrdtType::GCounter);
        assert_eq!(pending[0].origin_node, "node-1");
        assert_eq!(pending[1].key, "key:2");
    }

    #[test]
    fn test_prepare_gossip_message_builds_valid_message() {
        let proto = make_protocol("node-1");
        proto.add_peer("node-2", "10.0.0.2:6380");

        proto.record_local_mutation("key:1", CrdtType::OrSet, vec![10, 20]);
        proto.record_local_mutation("key:2", CrdtType::LwwRegister, vec![30]);

        let msg = proto.prepare_gossip_message("node-2");
        assert!(msg.is_some());

        let msg = msg.unwrap();
        assert_eq!(msg.from_node, "node-1");
        assert_eq!(msg.to_node, "node-2");
        assert_eq!(msg.deltas.len(), 2);
        assert!(msg.round > 0);
        assert!(msg.timestamp > 0);

        // Unknown peer returns None
        assert!(proto.prepare_gossip_message("node-99").is_none());
    }

    #[test]
    fn test_receive_gossip_message_applies_deltas() {
        let node1 = make_protocol("node-1");
        let node2 = make_protocol("node-2");
        node1.add_peer("node-2", "10.0.0.2:6380");
        node2.add_peer("node-1", "10.0.0.1:6380");

        // Node 1 records mutations
        node1.record_local_mutation("key:1", CrdtType::GCounter, vec![1]);
        node1.record_local_mutation("key:2", CrdtType::PNCounter, vec![2]);

        // Node 1 prepares message for node 2
        let msg = node1.prepare_gossip_message("node-2").unwrap();

        // Node 2 receives the message
        let result = node2.receive_gossip_message(msg);
        assert_eq!(result.applied, 2);
        assert_eq!(result.skipped, 0);
        assert_eq!(result.new_keys.len(), 2);
        assert!(result.new_keys.contains(&"key:1".to_string()));
        assert!(result.new_keys.contains(&"key:2".to_string()));
    }

    #[test]
    fn test_skips_already_seen_deltas() {
        let node1 = make_protocol("node-1");
        let node2 = make_protocol("node-2");
        node1.add_peer("node-2", "10.0.0.2:6380");
        node2.add_peer("node-1", "10.0.0.1:6380");

        // Node 1 records a mutation
        node1.record_local_mutation("key:1", CrdtType::GCounter, vec![1]);

        // Send first time — should be applied
        let msg1 = node1.prepare_gossip_message("node-2").unwrap();
        let result1 = node2.receive_gossip_message(msg1.clone());
        assert_eq!(result1.applied, 1);
        assert_eq!(result1.skipped, 0);

        // Send same message again — should be skipped
        let result2 = node2.receive_gossip_message(msg1);
        assert_eq!(result2.applied, 0);
        assert_eq!(result2.skipped, 1);
    }

    #[test]
    fn test_select_gossip_targets_respects_fanout() {
        let proto = make_protocol("node-1");
        proto.add_peer("node-2", "10.0.0.2:6380");
        proto.add_peer("node-3", "10.0.0.3:6380");
        proto.add_peer("node-4", "10.0.0.4:6380");
        proto.add_peer("node-5", "10.0.0.5:6380");

        // Default fanout is 3, but we explicitly request 2
        let targets = proto.select_gossip_targets(2);
        assert_eq!(targets.len(), 2);

        // Request more than available
        let targets = proto.select_gossip_targets(10);
        assert_eq!(targets.len(), 4);

        // All returned targets should be known peers
        for t in &targets {
            assert!(proto.peers.read().contains_key(t));
        }
    }

    #[test]
    fn test_stats_tracking() {
        let node1 = make_protocol("node-1");
        let node2 = make_protocol("node-2");
        node1.add_peer("node-2", "10.0.0.2:6380");
        node2.add_peer("node-1", "10.0.0.1:6380");

        // Initial stats should be zero
        let s = node1.stats();
        assert_eq!(s.messages_sent, 0);
        assert_eq!(s.deltas_sent, 0);

        // Record and send
        node1.record_local_mutation("key:1", CrdtType::OrMap, vec![1, 2]);
        let msg = node1.prepare_gossip_message("node-2").unwrap();

        let s = node1.stats();
        assert_eq!(s.messages_sent, 1);
        assert_eq!(s.deltas_sent, 1);
        assert!(s.bytes_sent > 0);
        assert_eq!(s.rounds_completed, 1);

        // Receive on node 2
        node2.receive_gossip_message(msg);
        let s2 = node2.stats();
        assert_eq!(s2.messages_received, 1);
        assert_eq!(s2.deltas_received, 1);
        assert_eq!(s2.deltas_applied, 1);
        assert!(s2.bytes_received > 0);
    }

    #[test]
    fn test_peer_timeout_detection() {
        let config = GossipConfig {
            peer_timeout_ms: 100,
            ..Default::default()
        };
        let proto = GossipProtocol::new("node-1".to_string(), config);
        proto.add_peer("node-2", "10.0.0.2:6380");

        // Peer starts as alive with last_seen=0, timeout only applies when last_seen > 0
        proto.detect_peer_timeouts();
        assert!(proto.peers.read().get("node-2").unwrap().is_alive);

        // Simulate a peer that was seen long ago
        {
            let mut peers = proto.peers.write();
            let peer = peers.get_mut("node-2").unwrap();
            peer.last_seen = 1; // epoch millis = 1 (way in the past)
        }

        proto.detect_peer_timeouts();
        assert!(!proto.peers.read().get("node-2").unwrap().is_alive);

        // Dead peers should be excluded from gossip targets
        let targets = proto.select_gossip_targets(3);
        assert!(targets.is_empty());
    }

    #[test]
    fn test_delta_buffer_eviction() {
        let config = GossipConfig {
            max_delta_buffer: 3,
            ..Default::default()
        };
        let proto = GossipProtocol::new("node-1".to_string(), config);

        proto.record_local_mutation("key:1", CrdtType::GCounter, vec![1]);
        proto.record_local_mutation("key:2", CrdtType::GCounter, vec![2]);
        proto.record_local_mutation("key:3", CrdtType::GCounter, vec![3]);
        assert_eq!(proto.pending_delta_count(), 3);

        // Adding one more should evict the oldest
        proto.record_local_mutation("key:4", CrdtType::GCounter, vec![4]);
        assert_eq!(proto.pending_delta_count(), 3);

        let pending = proto.pending_deltas.read();
        assert_eq!(pending[0].key, "key:2");
        assert_eq!(pending[2].key, "key:4");
    }

    #[test]
    fn test_gossip_config_defaults() {
        let config = GossipConfig::default();
        assert_eq!(config.fanout, 3);
        assert_eq!(config.gossip_interval_ms, 1000);
        assert_eq!(config.max_delta_buffer, 10_000);
        assert_eq!(config.max_message_size, 1_048_576);
        assert_eq!(config.peer_timeout_ms, 30_000);
        assert!(config.enable_delta_compression);
    }
}
