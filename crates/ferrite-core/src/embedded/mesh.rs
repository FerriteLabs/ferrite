//! # Edge Mesh Networking
//!
//! Peer-to-peer mesh synchronization for edge Ferrite nodes.
//! Enables multiple Ferrite Lite/Edge instances to discover each other
//! and replicate data using CRDT-based eventual consistency.
//!
//! # Features
//!
//! - Peer discovery (static, mDNS-style, seed nodes)
//! - Gossip-based state dissemination
//! - CRDT merge for conflict-free replication
//! - Anti-entropy protocol for consistency repair
//! - Deployment profiles for different hardware targets
//!
//! # Example
//!
//! ```ignore
//! use ferrite::embedded::mesh::{MeshConfig, MeshNode, DeploymentProfile};
//!
//! let config = MeshConfig {
//!     node_id: "edge-01".to_string(),
//!     listen_addr: "0.0.0.0:7379".to_string(),
//!     profile: DeploymentProfile::arm64(),
//!     ..Default::default()
//! };
//!
//! let node = MeshNode::new(config);
//! node.add_seed("192.168.1.10:7379");
//! node.start();
//! ```

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Deployment profiles
// ---------------------------------------------------------------------------

/// Hardware deployment profile for edge nodes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeploymentProfile {
    /// Human-readable profile name.
    pub name: String,
    /// Target architecture (e.g. aarch64, x86_64, riscv64).
    pub arch: String,
    /// Recommended max memory in bytes.
    pub recommended_memory: usize,
    /// Recommended max keys.
    pub recommended_max_keys: usize,
    /// Whether to enable compression by default.
    pub compression: bool,
    /// Gossip interval for mesh sync.
    pub gossip_interval: Duration,
    /// Anti-entropy repair interval.
    pub anti_entropy_interval: Duration,
    /// Max peers to gossip with per round.
    pub gossip_fanout: usize,
    /// Whether persistence is recommended.
    pub persistence: bool,
    /// Feature flags to enable.
    pub features: Vec<String>,
}

impl DeploymentProfile {
    /// ARM64 server / Raspberry Pi 4+ profile.
    pub fn arm64() -> Self {
        Self {
            name: "arm64-server".to_string(),
            arch: "aarch64".to_string(),
            recommended_memory: 128 * 1024 * 1024,
            recommended_max_keys: 500_000,
            compression: true,
            gossip_interval: Duration::from_secs(2),
            anti_entropy_interval: Duration::from_secs(30),
            gossip_fanout: 3,
            persistence: true,
            features: vec!["lite".to_string(), "compression".to_string()],
        }
    }

    /// x86_64 edge server profile.
    pub fn x86_64() -> Self {
        Self {
            name: "x86_64-edge".to_string(),
            arch: "x86_64".to_string(),
            recommended_memory: 512 * 1024 * 1024,
            recommended_max_keys: 2_000_000,
            compression: true,
            gossip_interval: Duration::from_secs(1),
            anti_entropy_interval: Duration::from_secs(15),
            gossip_fanout: 4,
            persistence: true,
            features: vec![
                "lite".to_string(),
                "compression".to_string(),
                "full-types".to_string(),
            ],
        }
    }

    /// Constrained IoT device profile (e.g. ESP32, RPi Zero).
    pub fn iot() -> Self {
        Self {
            name: "iot-constrained".to_string(),
            arch: "arm".to_string(),
            recommended_memory: 2 * 1024 * 1024,
            recommended_max_keys: 1_000,
            compression: true,
            gossip_interval: Duration::from_secs(30),
            anti_entropy_interval: Duration::from_secs(300),
            gossip_fanout: 1,
            persistence: true,
            features: vec!["lite".to_string()],
        }
    }

    /// WASM/browser edge profile.
    pub fn wasm() -> Self {
        Self {
            name: "wasm-browser".to_string(),
            arch: "wasm32".to_string(),
            recommended_memory: 32 * 1024 * 1024,
            recommended_max_keys: 50_000,
            compression: false, // limited WASM support
            gossip_interval: Duration::from_secs(5),
            anti_entropy_interval: Duration::from_secs(60),
            gossip_fanout: 2,
            persistence: false,
            features: vec!["lite".to_string(), "wasm".to_string()],
        }
    }

    /// Mobile device profile (iOS/Android).
    pub fn mobile() -> Self {
        Self {
            name: "mobile".to_string(),
            arch: "aarch64".to_string(),
            recommended_memory: 64 * 1024 * 1024,
            recommended_max_keys: 100_000,
            compression: true,
            gossip_interval: Duration::from_secs(10),
            anti_entropy_interval: Duration::from_secs(120),
            gossip_fanout: 2,
            persistence: true,
            features: vec!["lite".to_string(), "compression".to_string()],
        }
    }
}

impl Default for DeploymentProfile {
    fn default() -> Self {
        Self::x86_64()
    }
}

// ---------------------------------------------------------------------------
// CRDT types for conflict-free replication
// ---------------------------------------------------------------------------

/// A Last-Writer-Wins register for a single key-value pair.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LwwRegister {
    /// The value (None = tombstone).
    pub value: Option<Vec<u8>>,
    /// Lamport timestamp of the write.
    pub timestamp: u64,
    /// Node that performed the write.
    pub origin: String,
}

impl LwwRegister {
    /// Create a new LWW register with the given value, timestamp, and origin node.
    pub fn new(value: Option<Vec<u8>>, timestamp: u64, origin: String) -> Self {
        Self {
            value,
            timestamp,
            origin,
        }
    }

    /// Merge with another register. The higher timestamp wins; ties broken by origin.
    pub fn merge(&mut self, other: &LwwRegister) -> bool {
        if other.timestamp > self.timestamp
            || (other.timestamp == self.timestamp && other.origin > self.origin)
        {
            self.value = other.value.clone();
            self.timestamp = other.timestamp;
            self.origin = other.origin.clone();
            true
        } else {
            false
        }
    }
}

/// Grow-only counter for distributed counting.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GCounter {
    /// Per-node counts.
    counts: BTreeMap<String, u64>,
}

impl GCounter {
    /// Create a new empty grow-only counter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment the counter for the given node.
    pub fn increment(&mut self, node_id: &str, amount: u64) {
        let entry = self.counts.entry(node_id.to_string()).or_insert(0);
        *entry += amount;
    }

    /// Get the total counter value across all nodes.
    pub fn value(&self) -> u64 {
        self.counts.values().sum()
    }

    /// Merge with another counter, taking the maximum count per node.
    pub fn merge(&mut self, other: &GCounter) {
        for (node, &count) in &other.counts {
            let entry = self.counts.entry(node.clone()).or_insert(0);
            *entry = (*entry).max(count);
        }
    }
}

// ---------------------------------------------------------------------------
// Mesh configuration and state
// ---------------------------------------------------------------------------

/// Configuration for a mesh node.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MeshConfig {
    /// Unique node identifier.
    pub node_id: String,
    /// Address to listen on (e.g. "0.0.0.0:7379").
    pub listen_addr: String,
    /// Seed node addresses for bootstrapping.
    pub seed_nodes: Vec<String>,
    /// Deployment profile.
    pub profile: DeploymentProfile,
    /// Maximum number of peers.
    pub max_peers: usize,
    /// Time after which a peer is considered dead.
    pub peer_timeout: Duration,
    /// Replication factor (how many nodes should hold each key).
    pub replication_factor: usize,
    /// Namespace/cluster name to partition mesh networks.
    pub namespace: String,
}

impl Default for MeshConfig {
    fn default() -> Self {
        Self {
            node_id: default_node_id(),
            listen_addr: "0.0.0.0:7379".to_string(),
            seed_nodes: Vec::new(),
            profile: DeploymentProfile::default(),
            max_peers: 32,
            peer_timeout: Duration::from_secs(30),
            replication_factor: 2,
            namespace: "default".to_string(),
        }
    }
}

fn default_node_id() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let random: u64 = rand::random();
    format!("node-{:08x}{:08x}", ts as u32, random as u32)
}

/// Peer state in the mesh.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Unique peer identifier.
    pub node_id: String,
    /// Network address of the peer.
    pub addr: String,
    /// Current lifecycle state.
    pub state: PeerState,
    /// Timestamp of last contact (seconds since epoch).
    pub last_seen: u64,
    /// Timestamp when the peer joined (seconds since epoch).
    pub joined_at: u64,
    /// Namespace the peer belongs to.
    pub namespace: String,
    /// Version vector — sequence number for this peer's state.
    pub version: u64,
    /// Profile name.
    pub profile: String,
}

/// Peer lifecycle states.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerState {
    /// Peer is healthy and responsive.
    Alive,
    /// Peer has not responded recently; suspected down.
    Suspect,
    /// Peer confirmed dead / left the mesh.
    Dead,
}

/// Gossip digest for anti-entropy.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GossipDigest {
    /// ID of the node that produced this digest.
    pub node_id: String,
    /// Map of key → timestamp for each key this node knows about.
    pub key_versions: HashMap<String, u64>,
    /// Sequence number of this digest.
    pub sequence: u64,
}

/// A batch of key updates exchanged between peers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MeshDelta {
    /// ID of the node that produced this delta.
    pub source: String,
    /// Key updates in this delta.
    pub entries: Vec<MeshEntry>,
    /// Lamport sequence number.
    pub sequence: u64,
}

/// A single key update.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MeshEntry {
    /// The key that was updated.
    pub key: String,
    /// The LWW register value for this key.
    pub register: LwwRegister,
}

/// Statistics for the mesh node.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MeshStats {
    /// Number of alive peers.
    pub peers_alive: usize,
    /// Number of suspect peers.
    pub peers_suspect: usize,
    /// Number of dead peers.
    pub peers_dead: usize,
    /// Number of live (non-tombstoned) local keys.
    pub keys_local: usize,
    /// Total gossip rounds executed.
    pub gossip_rounds: u64,
    /// Total anti-entropy rounds executed.
    pub anti_entropy_rounds: u64,
    /// Total keys received from peers.
    pub keys_received: u64,
    /// Total keys sent to peers.
    pub keys_sent: u64,
    /// Total conflicts resolved during merges.
    pub conflicts_resolved: u64,
    /// Total bytes sent (placeholder).
    pub bytes_sent: u64,
    /// Total bytes received (placeholder).
    pub bytes_received: u64,
}

// ---------------------------------------------------------------------------
// Mesh node
// ---------------------------------------------------------------------------

/// A mesh-networked edge node that participates in gossip-based replication.
pub struct MeshNode {
    config: MeshConfig,
    /// Local CRDT state (key → LWW register).
    state: RwLock<HashMap<String, LwwRegister>>,
    /// Known peers.
    peers: RwLock<HashMap<String, PeerInfo>>,
    /// Logical clock for ordering local writes.
    lamport: AtomicU64,
    /// Gossip round counter.
    gossip_rounds: AtomicU64,
    anti_entropy_rounds: AtomicU64,
    keys_received: AtomicU64,
    keys_sent: AtomicU64,
    conflicts_resolved: AtomicU64,
    running: AtomicBool,
    started_at: Instant,
}

impl MeshNode {
    /// Create a new mesh node.
    pub fn new(config: MeshConfig) -> Self {
        Self {
            config,
            state: RwLock::new(HashMap::new()),
            peers: RwLock::new(HashMap::new()),
            lamport: AtomicU64::new(0),
            gossip_rounds: AtomicU64::new(0),
            anti_entropy_rounds: AtomicU64::new(0),
            keys_received: AtomicU64::new(0),
            keys_sent: AtomicU64::new(0),
            conflicts_resolved: AtomicU64::new(0),
            running: AtomicBool::new(false),
            started_at: Instant::now(),
        }
    }

    /// Create with default config.
    pub fn with_defaults() -> Self {
        Self::new(MeshConfig::default())
    }

    /// Mark the node as running.
    pub fn start(&self) {
        self.running.store(true, Ordering::Release);
    }

    /// Stop the node.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    /// Check if node is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    // -- Data operations --

    /// Set a key locally and prepare for replication.
    pub fn set(&self, key: &str, value: Vec<u8>) {
        let ts = self.tick();
        let register = LwwRegister::new(Some(value), ts, self.config.node_id.clone());
        self.state.write().insert(key.to_string(), register);
    }

    /// Get a key from local state.
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let state = self.state.read();
        state.get(key).and_then(|r| r.value.clone())
    }

    /// Delete a key (tombstone).
    pub fn delete(&self, key: &str) -> bool {
        let ts = self.tick();
        let mut state = self.state.write();
        if state.contains_key(key) {
            state.insert(
                key.to_string(),
                LwwRegister::new(None, ts, self.config.node_id.clone()),
            );
            true
        } else {
            false
        }
    }

    /// Number of live (non-tombstoned) keys.
    pub fn key_count(&self) -> usize {
        self.state
            .read()
            .values()
            .filter(|r| r.value.is_some())
            .count()
    }

    // -- Peer management --

    /// Add a seed peer address.
    pub fn add_seed(&self, addr: &str) {
        let now_ts = now_secs();
        let peer = PeerInfo {
            node_id: format!("seed-{}", addr),
            addr: addr.to_string(),
            state: PeerState::Alive,
            last_seen: now_ts,
            joined_at: now_ts,
            namespace: self.config.namespace.clone(),
            version: 0,
            profile: "unknown".to_string(),
        };
        self.peers.write().insert(peer.node_id.clone(), peer);
    }

    /// Register a peer (e.g. after receiving a gossip message).
    pub fn register_peer(&self, info: PeerInfo) {
        if info.node_id == self.config.node_id {
            return; // Don't register self
        }
        if self.peers.read().len() >= self.config.max_peers {
            return; // At capacity
        }
        if info.namespace != self.config.namespace {
            return; // Different namespace
        }
        self.peers.write().insert(info.node_id.clone(), info);
    }

    /// Update a peer's liveness.
    pub fn touch_peer(&self, node_id: &str) {
        if let Some(peer) = self.peers.write().get_mut(node_id) {
            peer.last_seen = now_secs();
            peer.state = PeerState::Alive;
        }
    }

    /// Run peer failure detection based on timeouts.
    pub fn detect_failures(&self) -> Vec<String> {
        let timeout = self.config.peer_timeout.as_secs();
        let now = now_secs();
        let mut dead = Vec::new();

        let mut peers = self.peers.write();
        for (id, peer) in peers.iter_mut() {
            if peer.state == PeerState::Dead {
                continue;
            }
            let elapsed = now.saturating_sub(peer.last_seen);
            if elapsed > timeout * 2 {
                peer.state = PeerState::Dead;
                dead.push(id.clone());
            } else if elapsed > timeout {
                peer.state = PeerState::Suspect;
            }
        }
        dead
    }

    /// Remove dead peers.
    pub fn prune_dead_peers(&self) -> usize {
        let mut peers = self.peers.write();
        let before = peers.len();
        peers.retain(|_, p| p.state != PeerState::Dead);
        before - peers.len()
    }

    /// Get alive peers.
    pub fn alive_peers(&self) -> Vec<PeerInfo> {
        self.peers
            .read()
            .values()
            .filter(|p| p.state == PeerState::Alive)
            .cloned()
            .collect()
    }

    // -- Gossip / replication --

    /// Produce a gossip digest of local state for a peer.
    pub fn gossip_digest(&self) -> GossipDigest {
        let state = self.state.read();
        let key_versions: HashMap<String, u64> = state
            .iter()
            .map(|(k, r)| (k.clone(), r.timestamp))
            .collect();

        GossipDigest {
            node_id: self.config.node_id.clone(),
            key_versions,
            sequence: self.lamport.load(Ordering::Relaxed),
        }
    }

    /// Produce a delta containing keys that the remote peer is missing or has
    /// older versions of, based on their digest.
    pub fn compute_delta(&self, remote_digest: &GossipDigest) -> MeshDelta {
        let state = self.state.read();
        let mut entries = Vec::new();

        for (key, register) in state.iter() {
            let remote_ts = remote_digest.key_versions.get(key).copied().unwrap_or(0);
            if register.timestamp > remote_ts
                || (register.timestamp == remote_ts && register.origin != remote_digest.node_id)
            {
                entries.push(MeshEntry {
                    key: key.clone(),
                    register: register.clone(),
                });
            }
        }

        self.keys_sent
            .fetch_add(entries.len() as u64, Ordering::Relaxed);

        MeshDelta {
            source: self.config.node_id.clone(),
            entries,
            sequence: self.lamport.load(Ordering::Relaxed),
        }
    }

    /// Apply a delta received from a peer.
    pub fn apply_delta(&self, delta: &MeshDelta) -> MergeSummary {
        let mut state = self.state.write();
        let mut applied = 0u64;
        let mut conflicts = 0u64;
        let mut skipped = 0u64;

        for entry in &delta.entries {
            match state.get_mut(&entry.key) {
                Some(existing) => {
                    if existing.timestamp == entry.register.timestamp
                        && existing.origin != entry.register.origin
                    {
                        conflicts += 1;
                    }
                    if existing.merge(&entry.register) {
                        applied += 1;
                    } else {
                        skipped += 1;
                    }
                }
                None => {
                    state.insert(entry.key.clone(), entry.register.clone());
                    applied += 1;
                }
            }
        }

        self.keys_received.fetch_add(applied, Ordering::Relaxed);
        self.conflicts_resolved
            .fetch_add(conflicts, Ordering::Relaxed);

        // Advance lamport clock
        let remote_ts = delta.sequence;
        let _ = self
            .lamport
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                if remote_ts >= current {
                    Some(remote_ts + 1)
                } else {
                    None
                }
            });

        MergeSummary {
            applied,
            conflicts,
            skipped,
        }
    }

    /// Simulate one round of gossip to a set of peers.
    /// Returns how many entries were sent.
    pub fn gossip_round(&self, peer_digests: &[GossipDigest]) -> u64 {
        let mut total_sent = 0;

        let fanout = self.config.profile.gossip_fanout.min(peer_digests.len());
        for digest in peer_digests.iter().take(fanout) {
            let delta = self.compute_delta(digest);
            total_sent += delta.entries.len() as u64;
        }

        self.gossip_rounds.fetch_add(1, Ordering::Relaxed);
        total_sent
    }

    /// Run anti-entropy: compare digest against local state to find keys
    /// this node is missing.
    pub fn anti_entropy_request(&self, remote_digest: &GossipDigest) -> Vec<String> {
        let state = self.state.read();
        let mut missing = Vec::new();

        for (key, &remote_ts) in &remote_digest.key_versions {
            match state.get(key) {
                Some(local) if local.timestamp < remote_ts => {
                    missing.push(key.clone());
                }
                None => {
                    missing.push(key.clone());
                }
                _ => {}
            }
        }

        self.anti_entropy_rounds.fetch_add(1, Ordering::Relaxed);
        missing
    }

    // -- Statistics --

    /// Get mesh node statistics.
    pub fn stats(&self) -> MeshStats {
        let peers = self.peers.read();
        MeshStats {
            peers_alive: peers
                .values()
                .filter(|p| p.state == PeerState::Alive)
                .count(),
            peers_suspect: peers
                .values()
                .filter(|p| p.state == PeerState::Suspect)
                .count(),
            peers_dead: peers
                .values()
                .filter(|p| p.state == PeerState::Dead)
                .count(),
            keys_local: self.key_count(),
            gossip_rounds: self.gossip_rounds.load(Ordering::Relaxed),
            anti_entropy_rounds: self.anti_entropy_rounds.load(Ordering::Relaxed),
            keys_received: self.keys_received.load(Ordering::Relaxed),
            keys_sent: self.keys_sent.load(Ordering::Relaxed),
            conflicts_resolved: self.conflicts_resolved.load(Ordering::Relaxed),
            bytes_sent: 0,
            bytes_received: 0,
        }
    }

    /// Node ID.
    pub fn node_id(&self) -> &str {
        &self.config.node_id
    }

    /// Current Lamport clock.
    pub fn lamport_clock(&self) -> u64 {
        self.lamport.load(Ordering::Relaxed)
    }

    // -- Internal --

    fn tick(&self) -> u64 {
        self.lamport.fetch_add(1, Ordering::SeqCst) + 1
    }
}

/// Summary of a delta merge operation.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MergeSummary {
    /// Number of entries applied.
    pub applied: u64,
    /// Number of conflicts detected and resolved.
    pub conflicts: u64,
    /// Number of entries skipped (already up-to-date).
    pub skipped: u64,
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// SAFETY: All mutable state is behind RwLock or Atomic types.
unsafe impl Send for MeshNode {}
unsafe impl Sync for MeshNode {}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str) -> MeshNode {
        MeshNode::new(MeshConfig {
            node_id: id.to_string(),
            ..Default::default()
        })
    }

    #[test]
    fn test_deployment_profiles() {
        let arm = DeploymentProfile::arm64();
        assert_eq!(arm.arch, "aarch64");
        assert!(arm.compression);

        let iot = DeploymentProfile::iot();
        assert_eq!(iot.recommended_max_keys, 1_000);

        let wasm = DeploymentProfile::wasm();
        assert!(!wasm.compression);

        let mobile = DeploymentProfile::mobile();
        assert_eq!(mobile.name, "mobile");
    }

    #[test]
    fn test_lww_register_merge() {
        let mut r1 = LwwRegister::new(Some(b"v1".to_vec()), 1, "a".into());
        let r2 = LwwRegister::new(Some(b"v2".to_vec()), 2, "b".into());

        assert!(r1.merge(&r2));
        assert_eq!(r1.value, Some(b"v2".to_vec()));

        // Lower timestamp doesn't overwrite
        let r3 = LwwRegister::new(Some(b"v3".to_vec()), 1, "c".into());
        assert!(!r1.merge(&r3));
        assert_eq!(r1.value, Some(b"v2".to_vec()));
    }

    #[test]
    fn test_lww_tiebreak_by_origin() {
        let mut r1 = LwwRegister::new(Some(b"v1".to_vec()), 5, "a".into());
        let r2 = LwwRegister::new(Some(b"v2".to_vec()), 5, "b".into());

        // Same timestamp, "b" > "a" so r2 wins
        assert!(r1.merge(&r2));
        assert_eq!(r1.origin, "b");
    }

    #[test]
    fn test_gcounter() {
        let mut c1 = GCounter::new();
        c1.increment("a", 3);
        c1.increment("a", 2);
        assert_eq!(c1.value(), 5);

        let mut c2 = GCounter::new();
        c2.increment("b", 10);

        c1.merge(&c2);
        assert_eq!(c1.value(), 15);
    }

    #[test]
    fn test_mesh_set_get_delete() {
        let n = node("n1");
        n.set("k1", b"hello".to_vec());
        assert_eq!(n.get("k1"), Some(b"hello".to_vec()));
        assert_eq!(n.key_count(), 1);

        assert!(n.delete("k1"));
        assert_eq!(n.get("k1"), None);
        // Tombstone still exists in state
        assert_eq!(n.key_count(), 0);
    }

    #[test]
    fn test_mesh_gossip_replication() {
        let n1 = node("n1");
        let n2 = node("n2");

        // n1 writes some data
        n1.set("a", b"1".to_vec());
        n1.set("b", b"2".to_vec());

        // n2 sends its digest to n1
        let digest = n2.gossip_digest();
        let delta = n1.compute_delta(&digest);

        assert_eq!(delta.entries.len(), 2);

        // n2 applies the delta
        let summary = n2.apply_delta(&delta);
        assert_eq!(summary.applied, 2);
        assert_eq!(n2.get("a"), Some(b"1".to_vec()));
        assert_eq!(n2.get("b"), Some(b"2".to_vec()));
    }

    #[test]
    fn test_mesh_conflict_resolution() {
        let n1 = node("n1");
        let n2 = node("n2");

        // Both write same key, n2 writes later (higher lamport)
        n1.set("key", b"from-n1".to_vec());
        n2.set("key", b"from-n2".to_vec());

        // n2's timestamp is 1, n1's is 1, but n2 > n1 lexicographically
        let digest = n1.gossip_digest();
        let delta = n2.compute_delta(&digest);
        let summary = n1.apply_delta(&delta);

        // n2's value should win (same timestamp, "n2" > "n1")
        assert_eq!(n1.get("key"), Some(b"from-n2".to_vec()));
        assert!(summary.applied >= 1);
    }

    #[test]
    fn test_mesh_anti_entropy() {
        let n1 = node("n1");
        let n2 = node("n2");

        n1.set("x", b"val".to_vec());

        // n2 checks what it's missing
        let digest = n1.gossip_digest();
        let missing = n2.anti_entropy_request(&digest);
        assert_eq!(missing, vec!["x"]);

        // After replication, nothing missing
        let delta = n1.compute_delta(&n2.gossip_digest());
        n2.apply_delta(&delta);
        let missing2 = n2.anti_entropy_request(&n1.gossip_digest());
        assert!(missing2.is_empty());
    }

    #[test]
    fn test_peer_management() {
        let n = node("main");
        n.add_seed("192.168.1.1:7379");
        assert_eq!(n.alive_peers().len(), 1);

        n.register_peer(PeerInfo {
            node_id: "peer-1".to_string(),
            addr: "10.0.0.1:7379".to_string(),
            state: PeerState::Alive,
            last_seen: now_secs(),
            joined_at: now_secs(),
            namespace: "default".to_string(),
            version: 0,
            profile: "x86_64".to_string(),
        });
        assert_eq!(n.alive_peers().len(), 2);
    }

    #[test]
    fn test_peer_failure_detection() {
        let n = MeshNode::new(MeshConfig {
            node_id: "detector".to_string(),
            peer_timeout: Duration::from_secs(0), // immediate timeout
            ..Default::default()
        });

        n.register_peer(PeerInfo {
            node_id: "old-peer".to_string(),
            addr: "1.2.3.4:7379".to_string(),
            state: PeerState::Alive,
            last_seen: 0, // epoch = very old
            joined_at: 0,
            namespace: "default".to_string(),
            version: 0,
            profile: "test".to_string(),
        });

        let dead = n.detect_failures();
        assert!(dead.contains(&"old-peer".to_string()));

        let pruned = n.prune_dead_peers();
        assert_eq!(pruned, 1);
        assert!(n.alive_peers().is_empty());
    }

    #[test]
    fn test_self_registration_ignored() {
        let n = node("self-node");
        n.register_peer(PeerInfo {
            node_id: "self-node".to_string(),
            addr: "127.0.0.1:7379".to_string(),
            state: PeerState::Alive,
            last_seen: now_secs(),
            joined_at: now_secs(),
            namespace: "default".to_string(),
            version: 0,
            profile: "test".to_string(),
        });
        assert!(n.alive_peers().is_empty());
    }

    #[test]
    fn test_namespace_isolation() {
        let n = node("ns-node");
        n.register_peer(PeerInfo {
            node_id: "other".to_string(),
            addr: "1.2.3.4:7379".to_string(),
            state: PeerState::Alive,
            last_seen: now_secs(),
            joined_at: now_secs(),
            namespace: "different-ns".to_string(),
            version: 0,
            profile: "test".to_string(),
        });
        // Different namespace, should not be registered
        assert!(n.alive_peers().is_empty());
    }

    #[test]
    fn test_mesh_stats() {
        let n = node("stats-node");
        n.set("a", b"1".to_vec());
        n.set("b", b"2".to_vec());

        let stats = n.stats();
        assert_eq!(stats.keys_local, 2);
        assert_eq!(stats.peers_alive, 0);
    }

    #[test]
    fn test_gossip_round() {
        let n1 = node("g1");
        let n2 = node("g2");

        n1.set("x", b"1".to_vec());

        let digests = vec![n2.gossip_digest()];
        let sent = n1.gossip_round(&digests);
        assert_eq!(sent, 1);

        let stats = n1.stats();
        assert_eq!(stats.gossip_rounds, 1);
    }

    #[test]
    fn test_lamport_clock_advancement() {
        let n1 = node("lc1");
        let n2 = node("lc2");

        // n1 makes 5 writes
        for i in 0..5 {
            n1.set(&format!("k{i}"), b"v".to_vec());
        }
        assert_eq!(n1.lamport_clock(), 5);

        // n2 applies n1's delta, its clock should advance past 5
        let delta = n1.compute_delta(&n2.gossip_digest());
        n2.apply_delta(&delta);
        assert!(n2.lamport_clock() > 5);
    }
}
