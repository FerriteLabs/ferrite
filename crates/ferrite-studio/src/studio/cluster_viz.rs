//! Cluster Topology Visualizer for Ferrite Studio
//!
//! Generates cluster topology data for the web UI including node layout,
//! slot distribution, replication chains, and real-time health information.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur during cluster visualization.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ClusterVizError {
    /// The requested node was not found.
    #[error("node not found: {0}")]
    NodeNotFound(String),

    /// Cluster information is unavailable (e.g. standalone mode).
    #[error("cluster not available")]
    ClusterNotAvailable,

    /// An internal error occurred.
    #[error("internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// Role of a node in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    /// Primary node that owns hash slots and accepts writes.
    Primary,
    /// Replica that replicates a primary.
    Replica,
    /// Arbiter node used for tie-breaking during elections.
    Arbiter,
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Primary => write!(f, "primary"),
            Self::Replica => write!(f, "replica"),
            Self::Arbiter => write!(f, "arbiter"),
        }
    }
}

/// Health status of a cluster node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Node is fully operational.
    Online,
    /// Node is unreachable.
    Offline,
    /// Node is synchronising data from its primary.
    Syncing,
    /// Node is loading data from disk.
    Loading,
    /// Node is experiencing intermittent failures.
    Failing,
}

impl std::fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Online => write!(f, "online"),
            Self::Offline => write!(f, "offline"),
            Self::Syncing => write!(f, "syncing"),
            Self::Loading => write!(f, "loading"),
            Self::Failing => write!(f, "failing"),
        }
    }
}

/// Overall cluster health state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterState {
    /// All slots assigned and all nodes healthy.
    Ok,
    /// Degraded — some replicas down or slots migrating.
    Warn,
    /// Cluster cannot serve all slots.
    Fail,
    /// State cannot be determined.
    Unknown,
}

impl std::fmt::Display for ClusterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ok => write!(f, "ok"),
            Self::Warn => write!(f, "warn"),
            Self::Fail => write!(f, "fail"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// Type of connection between two nodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EdgeType {
    /// Primary → Replica replication link.
    Replication,
    /// Cluster gossip / heartbeat.
    Gossip,
    /// Client redirect (MOVED / ASK).
    ClientRedirect,
}

/// Migration status of a slot range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SlotStatus {
    /// Slot range is stable and fully owned.
    Stable,
    /// Slot range is being migrated out.
    Migrating,
    /// Slot range is being imported from another node.
    Importing,
}

// ---------------------------------------------------------------------------
// Topology types
// ---------------------------------------------------------------------------

/// Full cluster topology snapshot for the UI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterTopology {
    /// All nodes in the cluster.
    pub nodes: Vec<NodeInfo>,
    /// Connections between nodes.
    pub edges: Vec<NodeEdge>,
    /// Total hash slots in the cluster (usually 16384).
    pub total_slots: u32,
    /// Number of slots currently assigned to nodes.
    pub assigned_slots: u32,
    /// Overall cluster health state.
    pub cluster_state: ClusterState,
}

/// Summary information for a single cluster node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Unique node identifier.
    pub id: String,
    /// Network address (host:port).
    pub address: String,
    /// Role of this node.
    pub role: NodeRole,
    /// Current health status.
    pub status: NodeStatus,
    /// Hash slot ranges owned by this node.
    pub slots: Vec<(u16, u16)>,
    /// Memory currently in use (bytes).
    pub memory_used_bytes: u64,
    /// Total memory available (bytes).
    pub memory_total_bytes: u64,
    /// Number of connected clients.
    pub connected_clients: u64,
    /// Operations per second.
    pub ops_per_sec: u64,
    /// Time since node started (seconds).
    pub uptime_secs: u64,
}

/// A directed edge between two nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeEdge {
    /// Source node id.
    pub source: String,
    /// Target node id.
    pub target: String,
    /// Type of connection.
    pub edge_type: EdgeType,
    /// Measured latency between the nodes in milliseconds.
    pub latency_ms: f64,
}

// ---------------------------------------------------------------------------
// Node detail types
// ---------------------------------------------------------------------------

/// Detailed information for a single node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDetail {
    /// Core node information.
    pub info: NodeInfo,
    /// Selected configuration parameters.
    pub config: HashMap<String, String>,
    /// Currently connected clients.
    pub clients: Vec<ClientInfo>,
    /// Recent slow log entries.
    pub slow_log: Vec<SlowLogEntry>,
    /// Per-database keyspace statistics.
    pub keyspace: Vec<KeyspaceInfo>,
}

/// Information about a connected client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientInfo {
    /// Client identifier.
    pub id: String,
    /// Client address (ip:port).
    pub addr: String,
    /// Connection age in seconds.
    pub age_secs: u64,
    /// Seconds since last command.
    pub idle_secs: u64,
    /// Currently selected database.
    pub db: u8,
    /// Last executed command.
    pub cmd: String,
}

/// A single slow log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlowLogEntry {
    /// Unique slow log entry id.
    pub id: u64,
    /// Unix timestamp when the command was executed.
    pub timestamp: u64,
    /// Execution duration in microseconds.
    pub duration_us: u64,
    /// The command that was executed.
    pub command: String,
    /// Address of the client that issued the command.
    pub client_addr: String,
}

/// Keyspace statistics for a single database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyspaceInfo {
    /// Database index.
    pub db: u8,
    /// Total number of keys.
    pub keys: u64,
    /// Number of keys with an expiry set.
    pub expires: u64,
    /// Average TTL of keys in milliseconds.
    pub avg_ttl: u64,
}

// ---------------------------------------------------------------------------
// Slot map types
// ---------------------------------------------------------------------------

/// Complete slot assignment map.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotMap {
    /// Assigned slot ranges.
    pub slots: Vec<SlotRange>,
    /// Slot ranges that are not assigned to any node.
    pub unassigned: Vec<(u16, u16)>,
}

/// A contiguous range of hash slots assigned to a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotRange {
    /// Start of the range (inclusive).
    pub start: u16,
    /// End of the range (inclusive).
    pub end: u16,
    /// Node that owns this range.
    pub node_id: String,
    /// Current migration status.
    pub status: SlotStatus,
}

// ---------------------------------------------------------------------------
// Replication types
// ---------------------------------------------------------------------------

/// Cluster-wide replication topology.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationTopology {
    /// Replication chains (one per primary).
    pub chains: Vec<ReplicationChain>,
    /// Total replication offset across the cluster.
    pub total_offset: u64,
}

/// A single replication chain: one primary and its replicas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationChain {
    /// Node id of the primary.
    pub primary: String,
    /// Replicas attached to this primary.
    pub replicas: Vec<ReplicaInfo>,
}

/// Status of an individual replica.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaInfo {
    /// Replica node id.
    pub node_id: String,
    /// Current replication offset.
    pub offset: u64,
    /// Replication lag in bytes.
    pub lag_bytes: u64,
    /// Current health status.
    pub status: NodeStatus,
}

// ---------------------------------------------------------------------------
// ClusterVisualizer implementation
// ---------------------------------------------------------------------------

/// Generates cluster topology data for the Ferrite Studio web UI.
///
/// Provides a full topology snapshot, per-node details, slot distribution,
/// and replication chain information suitable for rendering interactive
/// cluster diagrams.
pub struct ClusterVisualizer {
    _private: (),
}

impl ClusterVisualizer {
    /// Create a new cluster visualizer.
    pub fn new() -> Self {
        Self { _private: () }
    }

    /// Generate a full cluster topology snapshot.
    pub fn get_topology(&self) -> ClusterTopology {
        let nodes = self.build_sample_nodes();
        let edges = self.build_sample_edges(&nodes);
        let assigned: u32 = nodes
            .iter()
            .filter(|n| n.role == NodeRole::Primary)
            .flat_map(|n| &n.slots)
            .map(|(start, end)| (*end as u32) - (*start as u32) + 1)
            .sum();

        ClusterTopology {
            nodes,
            edges,
            total_slots: 16384,
            assigned_slots: assigned,
            cluster_state: ClusterState::Ok,
        }
    }

    /// Get detailed information for a specific node.
    pub fn get_node_details(&self, node_id: &str) -> Option<NodeDetail> {
        let nodes = self.build_sample_nodes();
        let info = nodes.into_iter().find(|n| n.id == node_id)?;

        let mut config = HashMap::new();
        config.insert("maxmemory".to_string(), "4gb".to_string());
        config.insert("maxmemory-policy".to_string(), "allkeys-lru".to_string());
        config.insert("cluster-enabled".to_string(), "yes".to_string());

        Some(NodeDetail {
            info,
            config,
            clients: vec![
                ClientInfo {
                    id: "client-1".to_string(),
                    addr: "10.0.0.50:52310".to_string(),
                    age_secs: 120,
                    idle_secs: 2,
                    db: 0,
                    cmd: "GET".to_string(),
                },
                ClientInfo {
                    id: "client-2".to_string(),
                    addr: "10.0.0.51:48721".to_string(),
                    age_secs: 3600,
                    idle_secs: 30,
                    db: 0,
                    cmd: "SUBSCRIBE".to_string(),
                },
            ],
            slow_log: vec![SlowLogEntry {
                id: 1,
                timestamp: 1_700_000_000,
                duration_us: 15_000,
                command: "KEYS *".to_string(),
                client_addr: "10.0.0.50:52310".to_string(),
            }],
            keyspace: vec![
                KeyspaceInfo {
                    db: 0,
                    keys: 125_000,
                    expires: 40_000,
                    avg_ttl: 3_600_000,
                },
                KeyspaceInfo {
                    db: 1,
                    keys: 5_000,
                    expires: 1_000,
                    avg_ttl: 86_400_000,
                },
            ],
        })
    }

    /// Get the full slot assignment map.
    pub fn get_slot_map(&self) -> SlotMap {
        SlotMap {
            slots: vec![
                SlotRange {
                    start: 0,
                    end: 5460,
                    node_id: "node-1".to_string(),
                    status: SlotStatus::Stable,
                },
                SlotRange {
                    start: 5461,
                    end: 10922,
                    node_id: "node-2".to_string(),
                    status: SlotStatus::Stable,
                },
                SlotRange {
                    start: 10923,
                    end: 16383,
                    node_id: "node-3".to_string(),
                    status: SlotStatus::Stable,
                },
            ],
            unassigned: vec![],
        }
    }

    /// Get the replication topology.
    pub fn get_replication_status(&self) -> ReplicationTopology {
        ReplicationTopology {
            chains: vec![
                ReplicationChain {
                    primary: "node-1".to_string(),
                    replicas: vec![ReplicaInfo {
                        node_id: "node-4".to_string(),
                        offset: 1_000_000,
                        lag_bytes: 256,
                        status: NodeStatus::Online,
                    }],
                },
                ReplicationChain {
                    primary: "node-2".to_string(),
                    replicas: vec![ReplicaInfo {
                        node_id: "node-5".to_string(),
                        offset: 980_000,
                        lag_bytes: 1024,
                        status: NodeStatus::Online,
                    }],
                },
                ReplicationChain {
                    primary: "node-3".to_string(),
                    replicas: vec![ReplicaInfo {
                        node_id: "node-6".to_string(),
                        offset: 950_000,
                        lag_bytes: 512,
                        status: NodeStatus::Syncing,
                    }],
                },
            ],
            total_offset: 2_930_000,
        }
    }

    // -- private helpers ----------------------------------------------------

    fn build_sample_nodes(&self) -> Vec<NodeInfo> {
        vec![
            NodeInfo {
                id: "node-1".to_string(),
                address: "10.0.0.1:6379".to_string(),
                role: NodeRole::Primary,
                status: NodeStatus::Online,
                slots: vec![(0, 5460)],
                memory_used_bytes: 2_147_483_648,
                memory_total_bytes: 4_294_967_296,
                connected_clients: 150,
                ops_per_sec: 45_000,
                uptime_secs: 864_000,
            },
            NodeInfo {
                id: "node-2".to_string(),
                address: "10.0.0.2:6379".to_string(),
                role: NodeRole::Primary,
                status: NodeStatus::Online,
                slots: vec![(5461, 10922)],
                memory_used_bytes: 1_932_735_283,
                memory_total_bytes: 4_294_967_296,
                connected_clients: 120,
                ops_per_sec: 38_000,
                uptime_secs: 864_000,
            },
            NodeInfo {
                id: "node-3".to_string(),
                address: "10.0.0.3:6379".to_string(),
                role: NodeRole::Primary,
                status: NodeStatus::Online,
                slots: vec![(10923, 16383)],
                memory_used_bytes: 2_362_232_012,
                memory_total_bytes: 4_294_967_296,
                connected_clients: 130,
                ops_per_sec: 42_000,
                uptime_secs: 864_000,
            },
            NodeInfo {
                id: "node-4".to_string(),
                address: "10.0.0.4:6379".to_string(),
                role: NodeRole::Replica,
                status: NodeStatus::Online,
                slots: vec![],
                memory_used_bytes: 2_147_483_648,
                memory_total_bytes: 4_294_967_296,
                connected_clients: 10,
                ops_per_sec: 5_000,
                uptime_secs: 864_000,
            },
            NodeInfo {
                id: "node-5".to_string(),
                address: "10.0.0.5:6379".to_string(),
                role: NodeRole::Replica,
                status: NodeStatus::Online,
                slots: vec![],
                memory_used_bytes: 1_932_735_283,
                memory_total_bytes: 4_294_967_296,
                connected_clients: 8,
                ops_per_sec: 4_000,
                uptime_secs: 864_000,
            },
            NodeInfo {
                id: "node-6".to_string(),
                address: "10.0.0.6:6379".to_string(),
                role: NodeRole::Replica,
                status: NodeStatus::Syncing,
                slots: vec![],
                memory_used_bytes: 1_073_741_824,
                memory_total_bytes: 4_294_967_296,
                connected_clients: 5,
                ops_per_sec: 2_000,
                uptime_secs: 3_600,
            },
        ]
    }

    fn build_sample_edges(&self, nodes: &[NodeInfo]) -> Vec<NodeEdge> {
        let mut edges = Vec::new();

        // Replication edges: primary → replica.
        let repl_pairs = [
            ("node-1", "node-4"),
            ("node-2", "node-5"),
            ("node-3", "node-6"),
        ];
        for (src, tgt) in &repl_pairs {
            edges.push(NodeEdge {
                source: src.to_string(),
                target: tgt.to_string(),
                edge_type: EdgeType::Replication,
                latency_ms: 0.5,
            });
        }

        // Gossip edges between all primaries.
        let primaries: Vec<&NodeInfo> = nodes
            .iter()
            .filter(|n| n.role == NodeRole::Primary)
            .collect();
        for i in 0..primaries.len() {
            for j in (i + 1)..primaries.len() {
                edges.push(NodeEdge {
                    source: primaries[i].id.clone(),
                    target: primaries[j].id.clone(),
                    edge_type: EdgeType::Gossip,
                    latency_ms: 0.3,
                });
            }
        }

        edges
    }
}

impl Default for ClusterVisualizer {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn visualizer() -> ClusterVisualizer {
        ClusterVisualizer::new()
    }

    // -- topology -----------------------------------------------------------

    #[test]
    fn test_topology_nodes() {
        let topo = visualizer().get_topology();
        assert_eq!(topo.nodes.len(), 6);
        assert_eq!(topo.total_slots, 16384);
        assert_eq!(topo.cluster_state, ClusterState::Ok);
    }

    #[test]
    fn test_topology_slots_fully_assigned() {
        let topo = visualizer().get_topology();
        assert_eq!(topo.assigned_slots, 16384);
    }

    #[test]
    fn test_topology_has_primaries_and_replicas() {
        let topo = visualizer().get_topology();
        let primaries = topo
            .nodes
            .iter()
            .filter(|n| n.role == NodeRole::Primary)
            .count();
        let replicas = topo
            .nodes
            .iter()
            .filter(|n| n.role == NodeRole::Replica)
            .count();
        assert_eq!(primaries, 3);
        assert_eq!(replicas, 3);
    }

    #[test]
    fn test_topology_edges() {
        let topo = visualizer().get_topology();
        let repl = topo
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::Replication)
            .count();
        let gossip = topo
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::Gossip)
            .count();
        assert_eq!(repl, 3);
        assert_eq!(gossip, 3);
    }

    #[test]
    fn test_topology_serialization() {
        let topo = visualizer().get_topology();
        let json = serde_json::to_string(&topo).unwrap();
        let restored: ClusterTopology = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.nodes.len(), topo.nodes.len());
        assert_eq!(restored.cluster_state, topo.cluster_state);
    }

    // -- node details -------------------------------------------------------

    #[test]
    fn test_node_details_existing() {
        let detail = visualizer().get_node_details("node-1");
        assert!(detail.is_some());
        let detail = detail.unwrap();
        assert_eq!(detail.info.id, "node-1");
        assert_eq!(detail.info.role, NodeRole::Primary);
        assert!(!detail.clients.is_empty());
        assert!(!detail.keyspace.is_empty());
    }

    #[test]
    fn test_node_details_missing() {
        assert!(visualizer().get_node_details("nonexistent").is_none());
    }

    #[test]
    fn test_node_detail_serialization() {
        let detail = visualizer().get_node_details("node-2").unwrap();
        let json = serde_json::to_string(&detail).unwrap();
        let restored: NodeDetail = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.info.id, "node-2");
    }

    // -- slot map -----------------------------------------------------------

    #[test]
    fn test_slot_map_coverage() {
        let map = visualizer().get_slot_map();
        assert_eq!(map.slots.len(), 3);
        assert!(map.unassigned.is_empty());

        let total: u32 = map
            .slots
            .iter()
            .map(|r| (r.end as u32) - (r.start as u32) + 1)
            .sum();
        assert_eq!(total, 16384);
    }

    #[test]
    fn test_slot_map_all_stable() {
        let map = visualizer().get_slot_map();
        for range in &map.slots {
            assert_eq!(range.status, SlotStatus::Stable);
        }
    }

    #[test]
    fn test_slot_map_serialization() {
        let map = visualizer().get_slot_map();
        let json = serde_json::to_string(&map).unwrap();
        let restored: SlotMap = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.slots.len(), map.slots.len());
    }

    // -- replication --------------------------------------------------------

    #[test]
    fn test_replication_chains() {
        let repl = visualizer().get_replication_status();
        assert_eq!(repl.chains.len(), 3);
        assert!(repl.total_offset > 0);
    }

    #[test]
    fn test_replication_lag() {
        let repl = visualizer().get_replication_status();
        for chain in &repl.chains {
            for replica in &chain.replicas {
                assert!(replica.offset > 0);
            }
        }
    }

    #[test]
    fn test_replication_serialization() {
        let repl = visualizer().get_replication_status();
        let json = serde_json::to_string(&repl).unwrap();
        let restored: ReplicationTopology = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.chains.len(), repl.chains.len());
    }

    // -- enum display -------------------------------------------------------

    #[test]
    fn test_node_role_display() {
        assert_eq!(NodeRole::Primary.to_string(), "primary");
        assert_eq!(NodeRole::Replica.to_string(), "replica");
        assert_eq!(NodeRole::Arbiter.to_string(), "arbiter");
    }

    #[test]
    fn test_node_status_display() {
        assert_eq!(NodeStatus::Online.to_string(), "online");
        assert_eq!(NodeStatus::Syncing.to_string(), "syncing");
    }

    #[test]
    fn test_cluster_state_display() {
        assert_eq!(ClusterState::Ok.to_string(), "ok");
        assert_eq!(ClusterState::Fail.to_string(), "fail");
    }

    // -- error display ------------------------------------------------------

    #[test]
    fn test_error_display() {
        let e = ClusterVizError::NodeNotFound("node-99".to_string());
        assert_eq!(e.to_string(), "node not found: node-99");

        let e = ClusterVizError::ClusterNotAvailable;
        assert_eq!(e.to_string(), "cluster not available");
    }

    // -- default impl -------------------------------------------------------

    #[test]
    fn test_default_visualizer() {
        let viz = ClusterVisualizer::default();
        let topo = viz.get_topology();
        assert!(!topo.nodes.is_empty());
    }

    // -- client / slow log --------------------------------------------------

    #[test]
    fn test_client_info_serialization() {
        let client = ClientInfo {
            id: "c1".to_string(),
            addr: "127.0.0.1:1234".to_string(),
            age_secs: 60,
            idle_secs: 5,
            db: 0,
            cmd: "PING".to_string(),
        };
        let json = serde_json::to_string(&client).unwrap();
        let restored: ClientInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.id, "c1");
    }

    #[test]
    fn test_slow_log_entry_serialization() {
        let entry = SlowLogEntry {
            id: 42,
            timestamp: 1_700_000_000,
            duration_us: 10_000,
            command: "KEYS *".to_string(),
            client_addr: "10.0.0.1:5000".to_string(),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let restored: SlowLogEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.id, 42);
    }

    #[test]
    fn test_keyspace_info_serialization() {
        let ks = KeyspaceInfo {
            db: 0,
            keys: 1000,
            expires: 500,
            avg_ttl: 3600,
        };
        let json = serde_json::to_string(&ks).unwrap();
        let restored: KeyspaceInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.keys, 1000);
    }
}
