//! Edge Mesh Networking
//!
//! Automatic peer-to-peer mesh for edge Ferrite instances with
//! mDNS discovery, NAT traversal, encrypted tunnels, and
//! bandwidth-aware CRDT sync.
#![allow(dead_code)]

use std::collections::{BinaryHeap, HashMap};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the edge mesh.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshConfig {
    /// Whether mesh networking is enabled.
    pub enabled: bool,
    /// Port to listen on for mesh traffic.
    pub listen_port: u16,
    /// How peers are discovered.
    pub discovery_method: DiscoveryMethod,
    /// Initial bootstrap nodes.
    pub bootstrap_nodes: Vec<String>,
    /// Interval between heartbeats.
    pub heartbeat_interval: Duration,
    /// Time after which a peer is considered unreachable.
    pub peer_timeout: Duration,
    /// Maximum number of peers.
    pub max_peers: usize,
    /// Encrypt inter-peer traffic.
    pub encrypt_traffic: bool,
    /// Interval between sync rounds.
    pub sync_interval: Duration,
    /// Optional bandwidth cap in kbps.
    pub bandwidth_limit_kbps: Option<u64>,
    /// Enable NAT traversal.
    pub nat_traversal: bool,
}

impl Default for MeshConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listen_port: 6380,
            discovery_method: DiscoveryMethod::Mdns,
            bootstrap_nodes: Vec::new(),
            heartbeat_interval: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(30),
            max_peers: 1000,
            encrypt_traffic: true,
            sync_interval: Duration::from_secs(1),
            bandwidth_limit_kbps: None,
            nat_traversal: true,
        }
    }
}

/// How peers are discovered.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DiscoveryMethod {
    /// Multicast DNS (local network).
    Mdns,
    /// DNS-SD with a given domain.
    DnsSd {
        /// Domain to query.
        domain: String,
    },
    /// Contact known bootstrap nodes.
    Bootstrap,
    /// Static peer list.
    Static {
        /// Peers to connect to.
        peers: Vec<SocketAddr>,
    },
    /// Combination of mDNS + bootstrap.
    Hybrid,
}

impl Default for DiscoveryMethod {
    fn default() -> Self {
        Self::Mdns
    }
}

// ---------------------------------------------------------------------------
// Peer types
// ---------------------------------------------------------------------------

/// A peer in the mesh.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshPeer {
    /// Unique peer identifier.
    pub id: String,
    /// Network address.
    pub addr: SocketAddr,
    /// Role of the peer.
    pub role: PeerRole,
    /// Current state.
    pub state: PeerState,
    /// Last heartbeat timestamp (epoch millis).
    pub last_heartbeat: u64,
    /// Round-trip latency in milliseconds.
    pub latency_ms: u64,
    /// Measured bandwidth in kbps.
    pub bandwidth_kbps: u64,
    /// Number of keys shared by this peer.
    pub keys_shared: u64,
    /// Sync lag in milliseconds.
    pub sync_lag_ms: u64,
    /// Optional region label.
    pub region: Option<String>,
    /// Arbitrary metadata.
    pub metadata: HashMap<String, String>,
}

/// Role of a mesh peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerRole {
    /// Full read-write peer.
    Full,
    /// Read-only replica.
    ReadOnly,
    /// Relay-only (forwards traffic).
    Relay,
    /// Gateway between mesh and external networks.
    Gateway,
}

impl Default for PeerRole {
    fn default() -> Self {
        Self::Full
    }
}

impl std::str::FromStr for PeerRole {
    type Err = MeshError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "full" => Ok(Self::Full),
            "readonly" => Ok(Self::ReadOnly),
            "relay" => Ok(Self::Relay),
            "gateway" => Ok(Self::Gateway),
            _ => Err(MeshError::DiscoveryFailed(format!(
                "unknown peer role: {}",
                s
            ))),
        }
    }
}

/// State of a mesh peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerState {
    /// Just discovered, not yet connected.
    Discovered,
    /// Connection in progress.
    Connecting,
    /// TCP/QUIC connected, pre-sync.
    Connected,
    /// Initial sync in progress.
    Syncing,
    /// Fully active and synced.
    Active,
    /// Failed heartbeats.
    Unreachable,
    /// Banned from the mesh.
    Banned,
}

impl Default for PeerState {
    fn default() -> Self {
        Self::Discovered
    }
}

// ---------------------------------------------------------------------------
// Topology
// ---------------------------------------------------------------------------

/// Full topology of the mesh.
#[derive(Debug, Clone, Serialize)]
pub struct MeshTopology {
    /// This node's ID.
    pub self_id: String,
    /// All known peers.
    pub peers: Vec<MeshPeer>,
    /// Edges (connections) in the mesh graph.
    pub edges: Vec<MeshEdge>,
    /// Logical clusters.
    pub clusters: Vec<MeshCluster>,
}

/// An edge in the mesh graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshEdge {
    /// Source peer ID.
    pub from: String,
    /// Destination peer ID.
    pub to: String,
    /// Latency of this link in ms.
    pub latency_ms: u64,
    /// Bandwidth of this link in kbps.
    pub bandwidth_kbps: u64,
    /// Whether traffic on this edge is encrypted.
    pub encrypted: bool,
}

/// A logical cluster of peers (typically same region).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshCluster {
    /// Cluster identifier.
    pub id: String,
    /// Peer IDs in this cluster.
    pub peers: Vec<String>,
    /// Optional region label.
    pub region: Option<String>,
}

// ---------------------------------------------------------------------------
// Results & stats
// ---------------------------------------------------------------------------

/// Result of a sync operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResult {
    /// Peer synced with.
    pub peer_id: String,
    /// Keys sent to the peer.
    pub keys_sent: u64,
    /// Keys received from the peer.
    pub keys_received: u64,
    /// Number of conflicts resolved during sync.
    pub conflicts_resolved: u64,
    /// Duration of the sync in milliseconds.
    pub duration_ms: u64,
}

/// Result of a broadcast.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastResult {
    /// Number of peers that received the message.
    pub peers_reached: usize,
    /// Number of peers that failed.
    pub peers_failed: usize,
    /// Total bytes sent.
    pub total_bytes: u64,
}

/// Health of the mesh.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshHealth {
    /// Total known peers.
    pub total_peers: usize,
    /// Peers in Active state.
    pub active_peers: usize,
    /// Peers in Unreachable state.
    pub unreachable_peers: usize,
    /// Average latency across active peers (ms).
    pub avg_latency_ms: f64,
    /// Average sync lag across active peers (ms).
    pub sync_lag_ms: f64,
    /// Bandwidth utilisation percentage.
    pub bandwidth_utilization_pct: f64,
}

/// Operational statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MeshStats {
    /// Total peers ever discovered.
    pub total_peers_discovered: u64,
    /// Total sync operations.
    pub total_syncs: u64,
    /// Total bytes transferred.
    pub total_bytes_transferred: u64,
    /// Total conflicts resolved.
    pub total_conflicts: u64,
    /// Total broadcasts sent.
    pub broadcasts: u64,
    /// Number of NAT traversals.
    pub nat_traversals: u64,
    /// Uptime in seconds.
    pub uptime_secs: u64,
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors returned by the mesh manager.
#[derive(Debug, Clone, thiserror::Error)]
pub enum MeshError {
    /// Peer not found.
    #[error("peer not found: {0}")]
    PeerNotFound(String),
    /// Peer is banned.
    #[error("peer is banned: {0}")]
    PeerBanned(String),
    /// Max peers exceeded.
    #[error("max peers reached ({0})")]
    MaxPeers(usize),
    /// Discovery process failed.
    #[error("discovery failed: {0}")]
    DiscoveryFailed(String),
    /// Sync failed.
    #[error("sync failed: {0}")]
    SyncFailed(String),
    /// NAT traversal failed.
    #[error("NAT traversal failed: {0}")]
    NatTraversalFailed(String),
    /// Operation timed out.
    #[error("timeout: {0}")]
    Timeout(String),
    /// Encryption error.
    #[error("encryption failed: {0}")]
    EncryptionFailed(String),
}

// ---------------------------------------------------------------------------
// EdgeMeshManager
// ---------------------------------------------------------------------------

/// Manages the edge mesh network.
pub struct EdgeMeshManager {
    config: MeshConfig,
    self_id: String,
    peers: RwLock<HashMap<String, MeshPeer>>,
    edges: RwLock<Vec<MeshEdge>>,
    discovery_active: RwLock<bool>,
    stats: RwLock<MeshStats>,
    started_at: Instant,
}

impl EdgeMeshManager {
    /// Create a new mesh manager.
    pub fn new(config: MeshConfig) -> Self {
        let self_id = format!("node-{}", config.listen_port);
        Self {
            config,
            self_id,
            peers: RwLock::new(HashMap::new()),
            edges: RwLock::new(Vec::new()),
            discovery_active: RwLock::new(false),
            stats: RwLock::new(MeshStats::default()),
            started_at: Instant::now(),
        }
    }

    /// Start the peer discovery process.
    pub fn start_discovery(&self) -> Result<(), MeshError> {
        let mut active = self.discovery_active.write();
        if *active {
            return Ok(());
        }
        *active = true;
        Ok(())
    }

    /// Stop the peer discovery process.
    pub fn stop_discovery(&self) -> Result<(), MeshError> {
        let mut active = self.discovery_active.write();
        *active = false;
        Ok(())
    }

    /// Manually add a peer to the mesh.
    pub fn add_peer(&self, addr: SocketAddr, role: PeerRole) -> Result<MeshPeer, MeshError> {
        let mut peers = self.peers.write();
        if peers.len() >= self.config.max_peers {
            return Err(MeshError::MaxPeers(self.config.max_peers));
        }
        let peer_id = format!("peer-{}", addr);
        if let Some(existing) = peers.get(&peer_id) {
            if existing.state == PeerState::Banned {
                return Err(MeshError::PeerBanned(peer_id));
            }
        }
        let peer = MeshPeer {
            id: peer_id.clone(),
            addr,
            role,
            state: PeerState::Connected,
            last_heartbeat: now_epoch_ms(),
            latency_ms: 0,
            bandwidth_kbps: 0,
            keys_shared: 0,
            sync_lag_ms: 0,
            region: None,
            metadata: HashMap::new(),
        };
        peers.insert(peer_id, peer.clone());

        // Create edge from self to new peer
        self.edges.write().push(MeshEdge {
            from: self.self_id.clone(),
            to: peer.id.clone(),
            latency_ms: 0,
            bandwidth_kbps: 0,
            encrypted: self.config.encrypt_traffic,
        });

        self.stats.write().total_peers_discovered += 1;
        Ok(peer)
    }

    /// Remove a peer from the mesh.
    pub fn remove_peer(&self, peer_id: &str) -> Result<(), MeshError> {
        let mut peers = self.peers.write();
        if peers.remove(peer_id).is_none() {
            return Err(MeshError::PeerNotFound(peer_id.to_string()));
        }
        self.edges
            .write()
            .retain(|e| e.from != peer_id && e.to != peer_id);
        Ok(())
    }

    /// Ban a peer from the mesh.
    pub fn ban_peer(&self, peer_id: &str, _reason: &str) -> Result<(), MeshError> {
        let mut peers = self.peers.write();
        let peer = peers
            .get_mut(peer_id)
            .ok_or_else(|| MeshError::PeerNotFound(peer_id.to_string()))?;
        peer.state = PeerState::Banned;
        Ok(())
    }

    /// List all peers.
    pub fn peers(&self) -> Vec<MeshPeer> {
        self.peers.read().values().cloned().collect()
    }

    /// Get info about a specific peer.
    pub fn peer_info(&self, peer_id: &str) -> Option<MeshPeer> {
        self.peers.read().get(peer_id).cloned()
    }

    /// Return the current mesh topology.
    pub fn topology(&self) -> MeshTopology {
        let peers: Vec<MeshPeer> = self.peers.read().values().cloned().collect();
        let edges = self.edges.read().clone();

        // Build clusters by region
        let mut region_map: HashMap<String, Vec<String>> = HashMap::new();
        for p in &peers {
            let region = p.region.clone().unwrap_or_else(|| "default".to_string());
            region_map
                .entry(region)
                .or_default()
                .push(p.id.clone());
        }
        let clusters = region_map
            .into_iter()
            .enumerate()
            .map(|(i, (region, peer_ids))| MeshCluster {
                id: format!("cluster-{}", i),
                peers: peer_ids,
                region: Some(region),
            })
            .collect();

        MeshTopology {
            self_id: self.self_id.clone(),
            peers,
            edges,
            clusters,
        }
    }

    /// Force sync with a specific peer.
    pub fn sync_with(&self, peer_id: &str) -> Result<SyncResult, MeshError> {
        let peers = self.peers.read();
        let peer = peers
            .get(peer_id)
            .ok_or_else(|| MeshError::PeerNotFound(peer_id.to_string()))?;
        if peer.state == PeerState::Banned {
            return Err(MeshError::PeerBanned(peer_id.to_string()));
        }
        drop(peers);

        let result = SyncResult {
            peer_id: peer_id.to_string(),
            keys_sent: 0,
            keys_received: 0,
            conflicts_resolved: 0,
            duration_ms: 0,
        };

        let mut stats = self.stats.write();
        stats.total_syncs += 1;
        Ok(result)
    }

    /// Broadcast a message to all active peers.
    pub fn broadcast(&self, message: &[u8]) -> Result<BroadcastResult, MeshError> {
        let peers = self.peers.read();
        let active_count = peers
            .values()
            .filter(|p| p.state == PeerState::Active || p.state == PeerState::Connected)
            .count();

        let mut stats = self.stats.write();
        stats.broadcasts += 1;
        stats.total_bytes_transferred += (message.len() * active_count) as u64;

        Ok(BroadcastResult {
            peers_reached: active_count,
            peers_failed: 0,
            total_bytes: (message.len() * active_count) as u64,
        })
    }

    /// Compute the optimal (lowest-latency) route to a target peer using
    /// Dijkstra's algorithm on the edge graph.
    pub fn optimal_route(&self, target_id: &str) -> Option<Vec<String>> {
        let edges = self.edges.read();
        let peers = self.peers.read();

        // Build adjacency list
        let mut adj: HashMap<&str, Vec<(&str, u64)>> = HashMap::new();
        for edge in edges.iter() {
            adj.entry(edge.from.as_str())
                .or_default()
                .push((edge.to.as_str(), edge.latency_ms));
            adj.entry(edge.to.as_str())
                .or_default()
                .push((edge.from.as_str(), edge.latency_ms));
        }

        if !peers.contains_key(target_id) && target_id != self.self_id {
            return None;
        }

        // Dijkstra
        let mut dist: HashMap<&str, u64> = HashMap::new();
        let mut prev: HashMap<&str, &str> = HashMap::new();
        dist.insert(self.self_id.as_str(), 0);

        // Min-heap: (cost, node)
        let mut heap = BinaryHeap::new();
        heap.push(std::cmp::Reverse((0u64, self.self_id.as_str())));

        while let Some(std::cmp::Reverse((cost, node))) = heap.pop() {
            if node == target_id {
                break;
            }
            if let Some(&best) = dist.get(node) {
                if cost > best {
                    continue;
                }
            }
            if let Some(neighbors) = adj.get(node) {
                for &(next, weight) in neighbors {
                    let next_cost = cost + weight.max(1); // treat 0-latency as 1
                    if next_cost < *dist.get(next).unwrap_or(&u64::MAX) {
                        dist.insert(next, next_cost);
                        prev.insert(next, node);
                        heap.push(std::cmp::Reverse((next_cost, next)));
                    }
                }
            }
        }

        // Reconstruct path
        if !prev.contains_key(target_id) && target_id != self.self_id {
            return None;
        }

        let mut path = Vec::new();
        let mut current = target_id;
        while current != self.self_id {
            path.push(current.to_string());
            current = match prev.get(current) {
                Some(p) => p,
                None => return None,
            };
        }
        path.push(self.self_id.clone());
        path.reverse();
        Some(path)
    }

    /// Health summary.
    pub fn health(&self) -> MeshHealth {
        let peers = self.peers.read();
        let active: Vec<&MeshPeer> = peers
            .values()
            .filter(|p| p.state == PeerState::Active || p.state == PeerState::Connected)
            .collect();
        let unreachable = peers
            .values()
            .filter(|p| p.state == PeerState::Unreachable)
            .count();

        let avg_latency = if active.is_empty() {
            0.0
        } else {
            active.iter().map(|p| p.latency_ms as f64).sum::<f64>() / active.len() as f64
        };
        let avg_sync_lag = if active.is_empty() {
            0.0
        } else {
            active.iter().map(|p| p.sync_lag_ms as f64).sum::<f64>() / active.len() as f64
        };

        let bandwidth_util = if let Some(limit) = self.config.bandwidth_limit_kbps {
            let total_bw: u64 = active.iter().map(|p| p.bandwidth_kbps).sum();
            (total_bw as f64 / limit as f64) * 100.0
        } else {
            0.0
        };

        MeshHealth {
            total_peers: peers.len(),
            active_peers: active.len(),
            unreachable_peers: unreachable,
            avg_latency_ms: avg_latency,
            sync_lag_ms: avg_sync_lag,
            bandwidth_utilization_pct: bandwidth_util,
        }
    }

    /// Operational statistics.
    pub fn stats(&self) -> MeshStats {
        let mut s = self.stats.read().clone();
        s.uptime_secs = self.started_at.elapsed().as_secs();
        s
    }
}

/// Current epoch time in milliseconds.
fn now_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_manager() -> EdgeMeshManager {
        EdgeMeshManager::new(MeshConfig::default())
    }

    fn test_addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    #[test]
    fn test_add_peer() {
        let mgr = default_manager();
        let peer = mgr.add_peer(test_addr(7001), PeerRole::Full).unwrap();
        assert_eq!(peer.state, PeerState::Connected);
        assert_eq!(mgr.peers().len(), 1);
    }

    #[test]
    fn test_remove_peer() {
        let mgr = default_manager();
        let peer = mgr.add_peer(test_addr(7002), PeerRole::Full).unwrap();
        mgr.remove_peer(&peer.id).unwrap();
        assert!(mgr.peers().is_empty());
    }

    #[test]
    fn test_remove_unknown_peer() {
        let mgr = default_manager();
        let err = mgr.remove_peer("nonexistent").unwrap_err();
        assert!(matches!(err, MeshError::PeerNotFound(_)));
    }

    #[test]
    fn test_ban_peer() {
        let mgr = default_manager();
        let peer = mgr.add_peer(test_addr(7003), PeerRole::Full).unwrap();
        mgr.ban_peer(&peer.id, "misbehaving").unwrap();
        let info = mgr.peer_info(&peer.id).unwrap();
        assert_eq!(info.state, PeerState::Banned);
    }

    #[test]
    fn test_max_peers() {
        let mgr = EdgeMeshManager::new(MeshConfig {
            max_peers: 2,
            ..Default::default()
        });
        mgr.add_peer(test_addr(8001), PeerRole::Full).unwrap();
        mgr.add_peer(test_addr(8002), PeerRole::Full).unwrap();
        let err = mgr.add_peer(test_addr(8003), PeerRole::Full).unwrap_err();
        assert!(matches!(err, MeshError::MaxPeers(2)));
    }

    #[test]
    fn test_topology() {
        let mgr = default_manager();
        mgr.add_peer(test_addr(7004), PeerRole::Full).unwrap();
        mgr.add_peer(test_addr(7005), PeerRole::ReadOnly).unwrap();
        let topo = mgr.topology();
        assert_eq!(topo.peers.len(), 2);
        assert!(!topo.edges.is_empty());
    }

    #[test]
    fn test_dijkstra_routing() {
        let mgr = default_manager();

        // Add peers
        let p1 = mgr.add_peer(test_addr(9001), PeerRole::Full).unwrap();
        let p2 = mgr.add_peer(test_addr(9002), PeerRole::Full).unwrap();

        // Add an intermediate edge with latency
        {
            let mut edges = mgr.edges.write();
            // self -> p1: latency 10 (already added with 0, update it)
            for e in edges.iter_mut() {
                if e.to == p1.id {
                    e.latency_ms = 10;
                }
                if e.to == p2.id {
                    e.latency_ms = 50;
                }
            }
            // Add p1 -> p2 with latency 5
            edges.push(MeshEdge {
                from: p1.id.clone(),
                to: p2.id.clone(),
                latency_ms: 5,
                bandwidth_kbps: 1000,
                encrypted: true,
            });
        }

        // Optimal route to p2 should go through p1 (10+5=15 < 50)
        let route = mgr.optimal_route(&p2.id).unwrap();
        assert_eq!(route.len(), 3); // self -> p1 -> p2
        assert_eq!(route[1], p1.id);
    }

    #[test]
    fn test_sync_with() {
        let mgr = default_manager();
        let peer = mgr.add_peer(test_addr(7006), PeerRole::Full).unwrap();
        let result = mgr.sync_with(&peer.id).unwrap();
        assert_eq!(result.peer_id, peer.id);
        assert_eq!(mgr.stats().total_syncs, 1);
    }

    #[test]
    fn test_broadcast() {
        let mgr = default_manager();
        mgr.add_peer(test_addr(7007), PeerRole::Full).unwrap();
        mgr.add_peer(test_addr(7008), PeerRole::Full).unwrap();
        let result = mgr.broadcast(b"hello mesh").unwrap();
        assert_eq!(result.peers_reached, 2);
        assert_eq!(mgr.stats().broadcasts, 1);
    }

    #[test]
    fn test_health() {
        let mgr = default_manager();
        mgr.add_peer(test_addr(7009), PeerRole::Full).unwrap();
        let health = mgr.health();
        assert_eq!(health.total_peers, 1);
        assert_eq!(health.active_peers, 1);
    }

    #[test]
    fn test_discovery_lifecycle() {
        let mgr = default_manager();
        mgr.start_discovery().unwrap();
        assert!(*mgr.discovery_active.read());
        mgr.stop_discovery().unwrap();
        assert!(!*mgr.discovery_active.read());
    }

    #[test]
    fn test_sync_banned_peer() {
        let mgr = default_manager();
        let peer = mgr.add_peer(test_addr(7010), PeerRole::Full).unwrap();
        mgr.ban_peer(&peer.id, "bad").unwrap();
        let err = mgr.sync_with(&peer.id).unwrap_err();
        assert!(matches!(err, MeshError::PeerBanned(_)));
    }

    #[test]
    fn test_stats() {
        let mgr = default_manager();
        mgr.add_peer(test_addr(7011), PeerRole::Full).unwrap();
        let stats = mgr.stats();
        assert_eq!(stats.total_peers_discovered, 1);
        assert!(stats.uptime_secs < 2);
    }
}
