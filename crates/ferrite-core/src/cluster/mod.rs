//! Cluster Mode for Ferrite
//!
//! This module implements Redis Cluster-compatible clustering with:
//! - 16384 hash slots for key distribution
//! - MOVED/ASK redirects for client-side routing
//! - Cluster topology management
//! - Automatic failover handling
//! - Gossip protocol for node discovery and failure detection
//! - Slot migration for cluster resharding

#![allow(dead_code, unused_imports, unused_variables)]
pub mod auto_reshard;
pub mod cluster_metrics;
pub mod failover;
pub mod gossip;
pub mod health;
pub mod migration;
pub mod raft;
pub mod resharding;
pub mod rolling_upgrade;
mod routing;
mod slots;
pub mod split_brain;
pub mod state_manager;

pub use auto_reshard::{
    start_auto_reshard_monitor, AutoReshardConfig, AutoReshardEngine, AutoReshardError,
    AutoReshardState, AutoReshardStats, PlannedMigration, RebalancePlan, RebalanceReason,
    RebalanceSummary,
};
pub use failover::{
    start_failover_monitor, FailoverConfig, FailoverManager, FailoverState, FailoverStats,
    FailoverType, FailoverVote,
};
pub use gossip::{
    start_gossip_service, GossipConfig, GossipHandle, GossipManager, GossipMessage,
    GossipMessageType, GossipNodeInfo, GossipStats,
};
pub use migration::{
    MigrateOptions, MigrateResult, MigrationConfig, MigrationState, SlotMigrationManager,
    SlotMigrationStatus, SlotRebalancer,
};
pub use raft::{
    AppendEntriesArgs, AppendEntriesReply, LogEntry, RaftCommand, RaftConfig, RaftError, RaftNode,
    RaftRole, RaftStatus, RequestVoteArgs, RequestVoteReply, Term,
};
pub use routing::{ClusterRouter, ClusterState, RouteResult, SharedRouter};
pub use slots::{HashSlot, Redirect, SlotInfo, SlotMap, SlotMigrationState, SlotRange, CLUSTER_SLOTS};
pub use state_manager::{ClusterNodeInfo, ClusterStateManager, RedirectResult};
pub use cluster_metrics::{MigrationMetrics, MigrationMetricsSnapshot};
pub use health::{
    ClusterHealthSummary, HealthConfig, HealthMonitor, HealthScore, NodeHealthState, NodeMetrics,
};
pub use resharding::{
    MigrationPhase, ReshardingConfig, ReshardingEngine, ReshardingError, ReshardingStats,
    SlotMigrationProgress,
};
pub use rolling_upgrade::{
    ClusterVersion, RollingUpgradeConfig, RollingUpgradeManager, ShutdownPhase, VersionError,
};
pub use split_brain::{
    PartitionStatus, SplitBrainConfig, SplitBrainDetector,
};

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Instant;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

/// Node ID (40-character hex string in Redis)
pub type NodeId = String;

/// Typed representation of `CLUSTER <subcommand>` for type-safe dispatch.
///
/// Parsing a raw `CLUSTER` invocation into this enum centralises argument
/// validation and makes downstream handlers exhaustive via `match`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterCommand {
    /// `CLUSTER INFO` – cluster state summary.
    ClusterInfo,
    /// `CLUSTER NODES` – node topology.
    ClusterNodes,
    /// `CLUSTER SLOTS` – slot-to-node mapping.
    ClusterSlots,
    /// `CLUSTER SHARDS` – shard information (Redis 7.0+).
    ClusterShards,
    /// `CLUSTER MYID` – this node's 40-char hex identifier.
    ClusterMyId,
    /// `CLUSTER KEYSLOT <key>` – hash slot for a key.
    ClusterKeySlot(String),
    /// `CLUSTER COUNTKEYSINSLOT <slot>` – number of keys in a slot.
    ClusterCountKeysInSlot(u16),
    /// `CLUSTER GETKEYSINSLOT <slot> <count>` – keys in a slot.
    ClusterGetKeysInSlot(u16, u32),
    /// `CLUSTER MEET <ip> <port>` – introduce a node.
    ClusterMeet(SocketAddr),
    /// `CLUSTER FORGET <node-id>` – remove a node.
    ClusterForget(String),
    /// `CLUSTER ADDSLOTS <slot> [slot ...]` – assign slots to this node.
    ClusterAddSlots(Vec<u16>),
    /// `CLUSTER DELSLOTS <slot> [slot ...]` – remove slot assignments.
    ClusterDelSlots(Vec<u16>),
    /// `CLUSTER SETSLOT <slot> IMPORTING|MIGRATING|STABLE|NODE <node-id>`.
    ClusterSetSlot {
        /// Target slot.
        slot: u16,
        /// Sub-action.
        action: SetSlotAction,
    },
    /// `CLUSTER FAILOVER [FORCE|TAKEOVER]`.
    ClusterFailover(Option<FailoverOption>),
    /// `CLUSTER REPLICATE <node-id>`.
    ClusterReplicate(String),
    /// `CLUSTER RESET [HARD|SOFT]`.
    ClusterReset(bool),
    /// `CLUSTER SAVECONFIG`.
    ClusterSaveConfig,
    /// `CLUSTER FLUSHSLOTS`.
    ClusterFlushSlots,
    /// `CLUSTER SET-CONFIG-EPOCH <epoch>`.
    ClusterSetConfigEpoch(u64),
    /// `CLUSTER HELP`.
    ClusterHelp,
}

/// Sub-actions for `CLUSTER SETSLOT`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetSlotAction {
    /// Mark slot as importing from `source_node`.
    Importing(String),
    /// Mark slot as migrating to `target_node`.
    Migrating(String),
    /// Clear migration state.
    Stable,
    /// Assign slot to `node_id`.
    Node(String),
}

/// Options for `CLUSTER FAILOVER`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverOption {
    /// Force failover without primary agreement.
    Force,
    /// Immediate takeover without election.
    Takeover,
}

/// Errors produced when parsing a raw `CLUSTER` command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterCommandParseError(pub String);

impl std::fmt::Display for ClusterCommandParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ClusterCommandParseError {}

impl ClusterCommand {
    /// Parse a `CLUSTER` invocation from its subcommand and arguments.
    pub fn parse(subcommand: &str, args: &[String]) -> Result<Self, ClusterCommandParseError> {
        let err = |msg: &str| Err(ClusterCommandParseError(msg.to_string()));

        match subcommand.to_ascii_uppercase().as_str() {
            "INFO" => Ok(Self::ClusterInfo),
            "NODES" => Ok(Self::ClusterNodes),
            "SLOTS" => Ok(Self::ClusterSlots),
            "SHARDS" => Ok(Self::ClusterShards),
            "MYID" => Ok(Self::ClusterMyId),
            "HELP" => Ok(Self::ClusterHelp),
            "SAVECONFIG" => Ok(Self::ClusterSaveConfig),
            "FLUSHSLOTS" => Ok(Self::ClusterFlushSlots),
            "KEYSLOT" => {
                if args.is_empty() {
                    return err("ERR wrong number of arguments for 'cluster|keyslot' command");
                }
                Ok(Self::ClusterKeySlot(args[0].clone()))
            }
            "COUNTKEYSINSLOT" => {
                if args.is_empty() {
                    return err(
                        "ERR wrong number of arguments for 'cluster|countkeysinslot' command",
                    );
                }
                let slot: u16 = args[0].parse().map_err(|_| {
                    ClusterCommandParseError("ERR Invalid or out of range slot".to_string())
                })?;
                if slot >= CLUSTER_SLOTS {
                    return err("ERR Invalid or out of range slot");
                }
                Ok(Self::ClusterCountKeysInSlot(slot))
            }
            "GETKEYSINSLOT" => {
                if args.len() < 2 {
                    return err(
                        "ERR wrong number of arguments for 'cluster|getkeysinslot' command",
                    );
                }
                let slot: u16 = args[0].parse().map_err(|_| {
                    ClusterCommandParseError("ERR Invalid or out of range slot".to_string())
                })?;
                if slot >= CLUSTER_SLOTS {
                    return err("ERR Invalid or out of range slot");
                }
                let count: u32 = args[1].parse().map_err(|_| {
                    ClusterCommandParseError("ERR Invalid count".to_string())
                })?;
                Ok(Self::ClusterGetKeysInSlot(slot, count))
            }
            "MEET" => {
                if args.len() < 2 {
                    return err("ERR wrong number of arguments for 'cluster|meet' command");
                }
                let ip: std::net::IpAddr = args[0].parse().map_err(|_| {
                    ClusterCommandParseError("ERR Invalid IP address".to_string())
                })?;
                let port: u16 = args[1].parse().map_err(|_| {
                    ClusterCommandParseError("ERR Invalid port number".to_string())
                })?;
                Ok(Self::ClusterMeet(SocketAddr::new(ip, port)))
            }
            "FORGET" => {
                if args.is_empty() {
                    return err("ERR wrong number of arguments for 'cluster|forget' command");
                }
                Ok(Self::ClusterForget(args[0].clone()))
            }
            "ADDSLOTS" => {
                if args.is_empty() {
                    return err("ERR wrong number of arguments for 'cluster|addslots' command");
                }
                let mut slots = Vec::with_capacity(args.len());
                for arg in args {
                    let slot: u16 = arg.parse().map_err(|_| {
                        ClusterCommandParseError("ERR Invalid or out of range slot".to_string())
                    })?;
                    if slot >= CLUSTER_SLOTS {
                        return err("ERR Invalid or out of range slot");
                    }
                    slots.push(slot);
                }
                Ok(Self::ClusterAddSlots(slots))
            }
            "DELSLOTS" => {
                if args.is_empty() {
                    return err("ERR wrong number of arguments for 'cluster|delslots' command");
                }
                let mut slots = Vec::with_capacity(args.len());
                for arg in args {
                    let slot: u16 = arg.parse().map_err(|_| {
                        ClusterCommandParseError("ERR Invalid or out of range slot".to_string())
                    })?;
                    if slot >= CLUSTER_SLOTS {
                        return err("ERR Invalid or out of range slot");
                    }
                    slots.push(slot);
                }
                Ok(Self::ClusterDelSlots(slots))
            }
            "SETSLOT" => {
                if args.len() < 2 {
                    return err("ERR wrong number of arguments for 'cluster|setslot' command");
                }
                let slot: u16 = args[0].parse().map_err(|_| {
                    ClusterCommandParseError("ERR Invalid or out of range slot".to_string())
                })?;
                if slot >= CLUSTER_SLOTS {
                    return err("ERR Invalid or out of range slot");
                }
                let action = match args[1].to_ascii_uppercase().as_str() {
                    "IMPORTING" => {
                        if args.len() < 3 {
                            return err("ERR Please specify the source node ID");
                        }
                        SetSlotAction::Importing(args[2].clone())
                    }
                    "MIGRATING" => {
                        if args.len() < 3 {
                            return err("ERR Please specify the target node ID");
                        }
                        SetSlotAction::Migrating(args[2].clone())
                    }
                    "STABLE" => SetSlotAction::Stable,
                    "NODE" => {
                        if args.len() < 3 {
                            return err("ERR Please specify the node ID");
                        }
                        SetSlotAction::Node(args[2].clone())
                    }
                    _ => return err("ERR Invalid CLUSTER SETSLOT action"),
                };
                Ok(Self::ClusterSetSlot { slot, action })
            }
            "FAILOVER" => {
                let opt = if args.is_empty() {
                    None
                } else {
                    match args[0].to_ascii_uppercase().as_str() {
                        "FORCE" => Some(FailoverOption::Force),
                        "TAKEOVER" => Some(FailoverOption::Takeover),
                        _ => {
                            return err(
                                "ERR Invalid CLUSTER FAILOVER option. Valid: FORCE, TAKEOVER",
                            )
                        }
                    }
                };
                Ok(Self::ClusterFailover(opt))
            }
            "REPLICATE" => {
                if args.is_empty() {
                    return err("ERR wrong number of arguments for 'cluster|replicate' command");
                }
                Ok(Self::ClusterReplicate(args[0].clone()))
            }
            "RESET" => {
                let hard = !args.is_empty() && args[0].eq_ignore_ascii_case("HARD");
                Ok(Self::ClusterReset(hard))
            }
            "SET-CONFIG-EPOCH" => {
                if args.is_empty() {
                    return err(
                        "ERR wrong number of arguments for 'cluster|set-config-epoch' command",
                    );
                }
                let epoch: u64 = args[0].parse().map_err(|_| {
                    ClusterCommandParseError("ERR Invalid config epoch".to_string())
                })?;
                if epoch == 0 {
                    return err("ERR Invalid config epoch specified: 0");
                }
                Ok(Self::ClusterSetConfigEpoch(epoch))
            }
            other => err(&format!(
                "ERR Unknown subcommand or wrong number of arguments for '{}'",
                other,
            )),
        }
    }
}

/// Cluster node state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    /// Node is online and healthy
    Online,
    /// Node is suspected to be failing (PFAIL)
    PFail,
    /// Node has failed (FAIL)
    Fail,
    /// Node is in handshake state
    Handshake,
    /// Node has no address
    NoAddr,
}

impl Default for NodeState {
    fn default() -> Self {
        Self::Online
    }
}

/// Node role in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    /// Primary node serving hash slots
    Primary,
    /// Replica of a primary node
    Replica,
}

impl Default for NodeRole {
    fn default() -> Self {
        Self::Primary
    }
}

/// Cluster node information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNode {
    /// Unique node identifier
    pub id: NodeId,
    /// Node address (host:port)
    pub addr: SocketAddr,
    /// Node role
    pub role: NodeRole,
    /// Node state
    pub state: NodeState,
    /// ID of the primary (if this is a replica)
    pub primary_id: Option<NodeId>,
    /// Hash slots assigned to this node (if primary)
    pub slots: Vec<SlotRange>,
    /// Last ping time (not serialized)
    #[serde(skip)]
    pub last_ping: Option<Instant>,
    /// Last pong time (not serialized)
    #[serde(skip)]
    pub last_pong: Option<Instant>,
    /// Configuration epoch
    pub config_epoch: u64,
    /// Node flags
    pub flags: NodeFlags,
}

/// Node flags
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeFlags {
    /// This node is ourselves
    pub myself: bool,
    /// Node is not communicating with us
    pub noaddr: bool,
    /// Node is currently performing a failover
    pub failover_in_progress: bool,
    /// Node was migrated
    pub migrated: bool,
}

impl ClusterNode {
    /// Create a new cluster node
    pub fn new(id: NodeId, addr: SocketAddr) -> Self {
        Self {
            id,
            addr,
            role: NodeRole::Primary,
            state: NodeState::Online,
            primary_id: None,
            slots: Vec::new(),
            last_ping: None,
            last_pong: None,
            config_epoch: 0,
            flags: NodeFlags::default(),
        }
    }

    /// Create this node (ourselves)
    pub fn new_self(id: NodeId, addr: SocketAddr) -> Self {
        let mut node = Self::new(id, addr);
        node.flags.myself = true;
        node
    }

    /// Check if this node is a primary
    pub fn is_primary(&self) -> bool {
        self.role == NodeRole::Primary
    }

    /// Check if this node is a replica
    pub fn is_replica(&self) -> bool {
        self.role == NodeRole::Replica
    }

    /// Check if this node is healthy
    pub fn is_healthy(&self) -> bool {
        self.state == NodeState::Online
    }

    /// Set node as replica of another node
    pub fn set_replica_of(&mut self, primary_id: NodeId) {
        self.role = NodeRole::Replica;
        self.primary_id = Some(primary_id);
        self.slots.clear();
    }

    /// Assign a slot range to this node
    pub fn assign_slots(&mut self, range: SlotRange) {
        if self.is_primary() {
            self.slots.push(range);
        }
    }

    /// Check if this node serves a given slot
    pub fn serves_slot(&self, slot: u16) -> bool {
        self.slots.iter().any(|range| range.contains(slot))
    }

    /// Get total number of slots served
    pub fn slot_count(&self) -> usize {
        self.slots.iter().map(|r| r.count()).sum()
    }

    /// Consolidate adjacent slot ranges into single ranges
    pub fn consolidate_slot_ranges(&mut self) {
        if self.slots.len() <= 1 {
            return;
        }

        // Sort ranges by start
        self.slots.sort_by_key(|r| r.start);

        // Merge adjacent/overlapping ranges
        let mut consolidated = Vec::new();
        let mut current = self.slots[0].clone();

        for range in self.slots.iter().skip(1) {
            // Check if can merge (adjacent or overlapping)
            if range.start <= current.end + 1 {
                current.end = current.end.max(range.end);
            } else {
                consolidated.push(current);
                current = range.clone();
            }
        }
        consolidated.push(current);

        self.slots = consolidated;
    }
}

/// Cluster configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Whether cluster mode is enabled
    pub enabled: bool,
    /// This node's address (for cluster bus)
    pub node_addr: Option<SocketAddr>,
    /// Cluster bus port offset (default: 10000)
    pub bus_port_offset: u16,
    /// Cluster node timeout in milliseconds
    pub node_timeout: u64,
    /// Number of replicas per primary
    pub replica_count: u8,
    /// Whether to enable automatic failover
    pub failover_enabled: bool,
    /// Minimum number of primaries for cluster to be operational
    pub min_primaries: usize,
    /// Whether to require full slot coverage
    pub require_full_coverage: bool,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_addr: None,
            bus_port_offset: 10000,
            node_timeout: 15000,
            replica_count: 1,
            failover_enabled: true,
            min_primaries: 1,
            require_full_coverage: true,
        }
    }
}

/// Slot migration info
#[derive(Debug, Clone)]
pub struct SlotMigrationInfo {
    /// Migration state
    pub state: SlotMigrationState,
    /// Target node (for MIGRATING) or source node (for IMPORTING)
    pub node_id: NodeId,
}

/// Cluster manager
pub struct ClusterManager {
    /// Cluster configuration
    config: ClusterConfig,
    /// This node's ID
    node_id: NodeId,
    /// All known nodes
    nodes: RwLock<HashMap<NodeId, ClusterNode>>,
    /// Slot assignments (slot -> node_id)
    slot_map: RwLock<[Option<NodeId>; CLUSTER_SLOTS as usize]>,
    /// Current configuration epoch
    current_epoch: RwLock<u64>,
    /// Cluster state
    state: RwLock<ClusterState>,
    /// Shutdown signal sender
    shutdown_tx: broadcast::Sender<()>,
    /// Slot migration state (slot -> migration info)
    migrating_slots: RwLock<HashMap<u16, SlotMigrationInfo>>,
    /// Slot importing state (slot -> source node)
    importing_slots: RwLock<HashMap<u16, SlotMigrationInfo>>,
}

impl ClusterManager {
    /// Create a new cluster manager
    pub fn new(config: ClusterConfig, node_id: NodeId, node_addr: SocketAddr) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);

        let manager = Self {
            config,
            node_id: node_id.clone(),
            nodes: RwLock::new(HashMap::new()),
            slot_map: RwLock::new([const { None }; CLUSTER_SLOTS as usize]),
            current_epoch: RwLock::new(0),
            state: RwLock::new(ClusterState::Unknown),
            shutdown_tx,
            migrating_slots: RwLock::new(HashMap::new()),
            importing_slots: RwLock::new(HashMap::new()),
        };

        // Add ourselves
        let self_node = ClusterNode::new_self(node_id, node_addr);
        manager
            .nodes
            .write()
            .insert(self_node.id.clone(), self_node);

        manager
    }

    /// Get this node's ID
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Get cluster state
    pub fn state(&self) -> ClusterState {
        *self.state.read()
    }

    /// Get current cluster configuration epoch
    pub fn current_epoch(&self) -> u64 {
        *self.current_epoch.read()
    }

    /// Increment and return the new epoch
    pub fn increment_epoch(&self) -> u64 {
        let mut epoch = self.current_epoch.write();
        *epoch += 1;
        *epoch
    }

    /// Add a node to the cluster
    pub fn add_node(&self, node: ClusterNode) {
        let node_id = node.id.clone();
        self.nodes.write().insert(node_id.clone(), node);
        info!("Added node {} to cluster", node_id);
    }

    /// Remove a node from the cluster
    pub fn remove_node(&self, node_id: &NodeId) -> Option<ClusterNode> {
        let removed = self.nodes.write().remove(node_id);
        if removed.is_some() {
            info!("Removed node {} from cluster", node_id);
            // Clear slot assignments for this node
            let mut slot_map = self.slot_map.write();
            for slot_ref in slot_map.iter_mut() {
                if slot_ref.as_ref() == Some(node_id) {
                    *slot_ref = None;
                }
            }
        }
        removed
    }

    /// Get a node by ID
    pub fn get_node(&self, node_id: &NodeId) -> Option<ClusterNode> {
        self.nodes.read().get(node_id).cloned()
    }

    /// Get all nodes
    pub fn get_all_nodes(&self) -> Vec<ClusterNode> {
        self.nodes.read().values().cloned().collect()
    }

    /// Get primary nodes
    pub fn get_primaries(&self) -> Vec<ClusterNode> {
        self.nodes
            .read()
            .values()
            .filter(|n| n.is_primary() && n.is_healthy())
            .cloned()
            .collect()
    }

    /// Get replicas for a primary
    pub fn get_replicas(&self, primary_id: &NodeId) -> Vec<ClusterNode> {
        self.nodes
            .read()
            .values()
            .filter(|n| n.is_replica() && n.primary_id.as_ref() == Some(primary_id))
            .cloned()
            .collect()
    }

    /// Assign slots to a node
    pub fn assign_slots(&self, node_id: &NodeId, range: SlotRange) {
        // Update node's slot list
        if let Some(node) = self.nodes.write().get_mut(node_id) {
            node.assign_slots(range.clone());
        }

        // Update slot map
        let mut slot_map = self.slot_map.write();
        for slot in range.start..=range.end {
            slot_map[slot as usize] = Some(node_id.clone());
        }

        info!("Assigned slots {:?} to node {}", range, node_id);
    }

    /// Get the node serving a given slot
    pub fn get_slot_owner(&self, slot: u16) -> Option<NodeId> {
        self.slot_map.read()[slot as usize].clone()
    }

    /// Get the address of the node serving a given slot
    pub fn get_slot_address(&self, slot: u16) -> Option<SocketAddr> {
        let node_id = self.get_slot_owner(slot)?;
        self.get_node(&node_id).map(|n| n.addr)
    }

    /// Route a key to the appropriate node
    pub fn route_key(&self, key: &[u8]) -> RouteResult {
        let slot = HashSlot::for_key(key);
        self.route_slot(slot)
    }

    /// Route a slot to the appropriate node
    pub fn route_slot(&self, slot: u16) -> RouteResult {
        let slot_map = self.slot_map.read();

        match &slot_map[slot as usize] {
            Some(node_id) => {
                // Check if it's us
                if node_id == &self.node_id {
                    // Check if we're migrating this slot
                    if let Some(_migration) = self.migrating_slots.read().get(&slot) {
                        // We own the slot but it's being migrated
                        // Return Local - client should check for ASK on MOVED
                        RouteResult::Local
                    } else {
                        RouteResult::Local
                    }
                } else {
                    // Get the node's address
                    if let Some(node) = self.nodes.read().get(node_id) {
                        RouteResult::Moved {
                            slot,
                            addr: node.addr,
                        }
                    } else {
                        RouteResult::Error("Node not found".to_string())
                    }
                }
            }
            None => {
                // Check if we're importing this slot
                if self.importing_slots.read().contains_key(&slot) {
                    // We're importing - allow if ASK flag is set
                    RouteResult::Importing { slot }
                } else if self.config.require_full_coverage {
                    RouteResult::ClusterDown
                } else {
                    RouteResult::Error(format!("Slot {} not covered", slot))
                }
            }
        }
    }

    /// Route a slot, checking for ASK redirect during migration
    pub fn route_slot_with_asking(&self, slot: u16, asking: bool) -> RouteResult {
        // Check if we're importing this slot and client sent ASKING
        if asking && self.importing_slots.read().contains_key(&slot) {
            return RouteResult::Local;
        }

        // Check if we own this slot but it's migrating
        let slot_map = self.slot_map.read();
        if let Some(node_id) = &slot_map[slot as usize] {
            if node_id == &self.node_id {
                if let Some(migration) = self.migrating_slots.read().get(&slot) {
                    // Key might have been migrated, check if it exists locally first
                    // If not, return ASK redirect
                    if let Some(target_node) = self.nodes.read().get(&migration.node_id) {
                        return RouteResult::Ask {
                            slot,
                            addr: target_node.addr,
                        };
                    }
                }
            }
        }
        drop(slot_map);

        self.route_slot(slot)
    }

    /// Update cluster state based on current configuration
    pub fn update_state(&self) {
        let nodes = self.nodes.read();
        let slot_map = self.slot_map.read();

        // Check if we have minimum primaries
        let primary_count = nodes
            .values()
            .filter(|n| n.is_primary() && n.is_healthy())
            .count();

        if primary_count < self.config.min_primaries {
            *self.state.write() = ClusterState::Fail;
            return;
        }

        // Check slot coverage
        if self.config.require_full_coverage {
            let covered = slot_map.iter().filter(|s| s.is_some()).count();
            if covered < CLUSTER_SLOTS as usize {
                *self.state.write() = ClusterState::Fail;
                return;
            }
        }

        // Check for failed nodes
        let failed_count = nodes
            .values()
            .filter(|n| n.state == NodeState::Fail)
            .count();

        if failed_count > 0 {
            *self.state.write() = ClusterState::Degraded;
        } else {
            *self.state.write() = ClusterState::Ok;
        }
    }

    /// Mark a node as failed
    pub fn mark_node_failed(&self, node_id: &NodeId) {
        if let Some(node) = self.nodes.write().get_mut(node_id) {
            node.state = NodeState::Fail;
            warn!("Node {} marked as FAIL", node_id);
        }
        self.update_state();
    }

    /// Mark a node as potentially failed
    pub fn mark_node_pfail(&self, node_id: &NodeId) {
        if let Some(node) = self.nodes.write().get_mut(node_id) {
            if node.state == NodeState::Online {
                node.state = NodeState::PFail;
                debug!("Node {} marked as PFAIL", node_id);
            }
        }
    }

    /// Mark a node as online
    pub fn mark_node_online(&self, node_id: &NodeId) {
        if let Some(node) = self.nodes.write().get_mut(node_id) {
            node.state = NodeState::Online;
            info!("Node {} marked as ONLINE", node_id);
        }
        self.update_state();
    }

    /// Generate cluster nodes info (CLUSTER NODES format)
    pub fn cluster_nodes_info(&self) -> String {
        let nodes = self.nodes.read();
        let mut lines = Vec::new();

        for node in nodes.values() {
            let flags = self.node_flags_string(node);
            let primary = node.primary_id.as_deref().unwrap_or("-");
            let slots = self.node_slots_string(node);

            lines.push(format!(
                "{} {}:{} {} {} {} {} {} {}",
                node.id,
                node.addr.ip(),
                node.addr.port(),
                flags,
                primary,
                node.last_ping.map(|_| 0).unwrap_or(0),
                node.last_pong.map(|_| 0).unwrap_or(0),
                node.config_epoch,
                slots
            ));
        }

        lines.join("\n")
    }

    fn node_flags_string(&self, node: &ClusterNode) -> String {
        let mut flags = Vec::new();
        if node.flags.myself {
            flags.push("myself");
        }
        if node.is_primary() {
            flags.push("master");
        } else {
            flags.push("slave");
        }
        match node.state {
            NodeState::Fail => flags.push("fail"),
            NodeState::PFail => flags.push("fail?"),
            NodeState::Handshake => flags.push("handshake"),
            NodeState::NoAddr => flags.push("noaddr"),
            _ => {}
        }
        if flags.is_empty() {
            "-".to_string()
        } else {
            flags.join(",")
        }
    }

    fn node_slots_string(&self, node: &ClusterNode) -> String {
        if node.slots.is_empty() {
            return String::new();
        }

        node.slots
            .iter()
            .map(|range| {
                if range.start == range.end {
                    format!("{}", range.start)
                } else {
                    format!("{}-{}", range.start, range.end)
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Distribute slots evenly across primaries
    pub fn distribute_slots(&self) {
        let primaries: Vec<_> = self.get_primaries();
        if primaries.is_empty() {
            warn!("No primary nodes to distribute slots");
            return;
        }

        let slots_per_node = CLUSTER_SLOTS / primaries.len() as u16;
        let remainder = CLUSTER_SLOTS % primaries.len() as u16;

        let mut current_slot: u16 = 0;
        for (i, primary) in primaries.iter().enumerate() {
            let mut node_slots = slots_per_node;
            if (i as u16) < remainder {
                node_slots += 1;
            }

            let start = current_slot;
            let end = current_slot + node_slots - 1;
            self.assign_slots(&primary.id, SlotRange::new(start, end));

            current_slot = end + 1;
        }

        self.update_state();
    }

    // ========== Slot Migration Methods ==========

    /// Set slot to MIGRATING state (source node)
    /// This node owns the slot and is migrating it to target_node
    pub fn set_slot_migrating(&self, slot: u16, target_node_id: NodeId) -> Result<(), String> {
        // Verify we own this slot
        let slot_map = self.slot_map.read();
        if slot_map[slot as usize].as_ref() != Some(&self.node_id) {
            return Err(format!("Cannot set MIGRATING: we don't own slot {}", slot));
        }
        drop(slot_map);

        // Verify target node exists
        if !self.nodes.read().contains_key(&target_node_id) {
            return Err(format!("Unknown target node {}", target_node_id));
        }

        self.migrating_slots.write().insert(
            slot,
            SlotMigrationInfo {
                state: SlotMigrationState::Migrating,
                node_id: target_node_id.clone(),
            },
        );

        info!("Slot {} set to MIGRATING to node {}", slot, target_node_id);
        Ok(())
    }

    /// Set slot to IMPORTING state (target node)
    /// This node will import the slot from source_node
    pub fn set_slot_importing(&self, slot: u16, source_node_id: NodeId) -> Result<(), String> {
        // Verify source node exists
        if !self.nodes.read().contains_key(&source_node_id) {
            return Err(format!("Unknown source node {}", source_node_id));
        }

        self.importing_slots.write().insert(
            slot,
            SlotMigrationInfo {
                state: SlotMigrationState::Importing,
                node_id: source_node_id.clone(),
            },
        );

        info!(
            "Slot {} set to IMPORTING from node {}",
            slot, source_node_id
        );
        Ok(())
    }

    /// Set slot to STABLE state (cancel migration)
    pub fn set_slot_stable(&self, slot: u16) {
        self.migrating_slots.write().remove(&slot);
        self.importing_slots.write().remove(&slot);
        info!("Slot {} set to STABLE", slot);
    }

    /// Set slot owner (finalize migration with SETSLOT NODE)
    pub fn set_slot_node(&self, slot: u16, node_id: NodeId) -> Result<(), String> {
        // Verify node exists
        if !self.nodes.read().contains_key(&node_id) {
            return Err(format!("Unknown node {}", node_id));
        }

        // Clear any migration state
        self.migrating_slots.write().remove(&slot);
        self.importing_slots.write().remove(&slot);

        // Update slot assignment
        self.slot_map.write()[slot as usize] = Some(node_id.clone());

        // Update node's slot list
        if let Some(node) = self.nodes.write().get_mut(&node_id) {
            // Check if this slot is already in node's ranges
            let already_has = node.slots.iter().any(|r| r.contains(slot));
            if !already_has {
                node.slots.push(SlotRange::single(slot));
                // Consolidate adjacent ranges
                node.consolidate_slot_ranges();
            }
        }

        info!("Slot {} assigned to node {}", slot, node_id);
        Ok(())
    }

    /// Check if a slot is being migrated from this node
    pub fn is_slot_migrating(&self, slot: u16) -> bool {
        self.migrating_slots.read().contains_key(&slot)
    }

    /// Check if a slot is being imported to this node
    pub fn is_slot_importing(&self, slot: u16) -> bool {
        self.importing_slots.read().contains_key(&slot)
    }

    /// Get migration target for a slot (if migrating)
    pub fn get_migration_target(&self, slot: u16) -> Option<NodeId> {
        self.migrating_slots
            .read()
            .get(&slot)
            .map(|m| m.node_id.clone())
    }

    /// Get import source for a slot (if importing)
    pub fn get_import_source(&self, slot: u16) -> Option<NodeId> {
        self.importing_slots
            .read()
            .get(&slot)
            .map(|m| m.node_id.clone())
    }

    /// Get all migrating slots
    pub fn get_migrating_slots(&self) -> Vec<(u16, NodeId)> {
        self.migrating_slots
            .read()
            .iter()
            .map(|(slot, info)| (*slot, info.node_id.clone()))
            .collect()
    }

    /// Get all importing slots
    pub fn get_importing_slots(&self) -> Vec<(u16, NodeId)> {
        self.importing_slots
            .read()
            .iter()
            .map(|(slot, info)| (*slot, info.node_id.clone()))
            .collect()
    }
}

/// Generate a random node ID (40 hex characters)
pub fn generate_node_id() -> NodeId {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let bytes: [u8; 20] = rng.gen();
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_manager() -> ClusterManager {
        let config = ClusterConfig::default();
        let node_id = generate_node_id();
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        ClusterManager::new(config, node_id, addr)
    }

    #[test]
    fn test_generate_node_id() {
        let id = generate_node_id();
        assert_eq!(id.len(), 40);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_cluster_node() {
        let id = generate_node_id();
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let node = ClusterNode::new(id.clone(), addr);

        assert_eq!(node.id, id);
        assert_eq!(node.addr, addr);
        assert!(node.is_primary());
        assert!(node.is_healthy());
        assert_eq!(node.slot_count(), 0);
    }

    #[test]
    fn test_add_remove_node() {
        let manager = create_test_manager();

        let id = generate_node_id();
        let addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        let node = ClusterNode::new(id.clone(), addr);

        manager.add_node(node);
        assert!(manager.get_node(&id).is_some());

        manager.remove_node(&id);
        assert!(manager.get_node(&id).is_none());
    }

    #[test]
    fn test_slot_assignment() {
        let manager = create_test_manager();
        let node_id = manager.node_id().clone();

        // Assign slots 0-5461 to ourselves
        manager.assign_slots(&node_id, SlotRange::new(0, 5461));

        assert_eq!(manager.get_slot_owner(0), Some(node_id.clone()));
        assert_eq!(manager.get_slot_owner(5461), Some(node_id.clone()));
        assert!(manager.get_slot_owner(5462).is_none());
    }

    #[test]
    fn test_route_key() {
        let manager = create_test_manager();
        let node_id = manager.node_id().clone();

        // Assign all slots to ourselves
        manager.assign_slots(&node_id, SlotRange::new(0, CLUSTER_SLOTS - 1));

        // Any key should route locally
        let result = manager.route_key(b"test");
        assert!(matches!(result, RouteResult::Local));
    }

    #[test]
    fn test_cluster_state() {
        let manager = create_test_manager();

        // Initial state should be Unknown
        assert_eq!(manager.state(), ClusterState::Unknown);

        // After assigning all slots, should be Ok
        let node_id = manager.node_id().clone();
        manager.assign_slots(&node_id, SlotRange::new(0, CLUSTER_SLOTS - 1));
        manager.update_state();

        assert_eq!(manager.state(), ClusterState::Ok);
    }

    #[test]
    fn test_distribute_slots() {
        let config = ClusterConfig::default();
        let node1_id = generate_node_id();
        let node1_addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let manager = ClusterManager::new(config, node1_id.clone(), node1_addr);

        // Add two more primaries
        let node2_id = generate_node_id();
        let node2_addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        manager.add_node(ClusterNode::new(node2_id.clone(), node2_addr));

        let node3_id = generate_node_id();
        let node3_addr: SocketAddr = "127.0.0.1:6381".parse().unwrap();
        manager.add_node(ClusterNode::new(node3_id.clone(), node3_addr));

        // Distribute slots
        manager.distribute_slots();

        // Each node should have roughly 1/3 of slots
        let nodes = manager.get_all_nodes();
        let total_slots: usize = nodes.iter().map(|n| n.slot_count()).sum();
        assert_eq!(total_slots, CLUSTER_SLOTS as usize);

        // All slots should be covered
        for slot in 0..CLUSTER_SLOTS {
            assert!(manager.get_slot_owner(slot).is_some());
        }
    }

    #[test]
    fn test_epoch_initial_value() {
        let manager = create_test_manager();
        // Initial epoch should be 0
        assert_eq!(manager.current_epoch(), 0);
    }

    #[test]
    fn test_epoch_increment() {
        let manager = create_test_manager();

        // Initial epoch is 0
        assert_eq!(manager.current_epoch(), 0);

        // Increment epoch
        let new_epoch = manager.increment_epoch();
        assert_eq!(new_epoch, 1);
        assert_eq!(manager.current_epoch(), 1);

        // Increment again
        let new_epoch = manager.increment_epoch();
        assert_eq!(new_epoch, 2);
        assert_eq!(manager.current_epoch(), 2);
    }

    #[test]
    fn test_epoch_increment_multiple_times() {
        let manager = create_test_manager();

        // Increment 100 times
        for i in 1..=100 {
            let epoch = manager.increment_epoch();
            assert_eq!(epoch, i);
        }
        assert_eq!(manager.current_epoch(), 100);
    }

    #[test]
    fn test_node_role_transitions() {
        let manager = create_test_manager();
        let id = generate_node_id();
        let addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();

        let mut node = ClusterNode::new(id.clone(), addr);
        assert!(node.is_primary());

        // Make it a replica
        let primary_id = generate_node_id();
        node.set_replica_of(primary_id.clone());
        assert!(node.is_replica());
        assert_eq!(node.primary_id, Some(primary_id));

        // Promote back to primary (manually clear primary_id and role)
        node.role = NodeRole::Primary;
        node.primary_id = None;
        assert!(node.is_primary());
        assert_eq!(node.primary_id, None);
    }

    #[test]
    fn test_node_state_transitions() {
        let id = generate_node_id();
        let addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        let mut node = ClusterNode::new(id, addr);

        // Initial state is online
        assert!(node.is_healthy());
        assert_eq!(node.state, NodeState::Online);

        // Mark as failing
        node.state = NodeState::PFail;
        assert!(!node.is_healthy());

        // Mark as failed
        node.state = NodeState::Fail;
        assert!(!node.is_healthy());

        // Mark as handshaking
        node.state = NodeState::Handshake;
        assert!(!node.is_healthy());

        // Mark as online again
        node.state = NodeState::Online;
        assert!(node.is_healthy());
    }

    #[test]
    fn test_slot_range() {
        // Valid range
        let range = SlotRange::new(0, 5461);
        assert_eq!(range.start, 0);
        assert_eq!(range.end, 5461);
        assert!(range.contains(0));
        assert!(range.contains(5461));
        assert!(!range.contains(5462));

        // Single slot
        let single = SlotRange::new(100, 100);
        assert!(single.contains(100));
        assert!(!single.contains(99));
        assert!(!single.contains(101));
    }

    #[test]
    fn test_cluster_nodes_info() {
        let manager = create_test_manager();
        let node_id = manager.node_id().clone();

        // Assign all slots
        manager.assign_slots(&node_id, SlotRange::new(0, CLUSTER_SLOTS - 1));
        manager.update_state();

        let info = manager.cluster_nodes_info();

        // cluster_nodes_info returns CLUSTER NODES format
        // Each line: <id> <ip:port> <flags> <master> <ping> <pong> <config-epoch> <slots>
        assert!(info.contains("myself"));
        assert!(info.contains("master"));
        assert!(info.contains("0-16383")); // All slots assigned
    }

    // ── ClusterCommand parse tests ──────────────────────────────────

    #[test]
    fn test_parse_cluster_info() {
        let cmd = ClusterCommand::parse("INFO", &[]).unwrap();
        assert_eq!(cmd, ClusterCommand::ClusterInfo);
    }

    #[test]
    fn test_parse_cluster_keyslot() {
        let cmd = ClusterCommand::parse("keyslot", &["foo".into()]).unwrap();
        assert_eq!(cmd, ClusterCommand::ClusterKeySlot("foo".into()));
    }

    #[test]
    fn test_parse_cluster_keyslot_missing_arg() {
        assert!(ClusterCommand::parse("KEYSLOT", &[]).is_err());
    }

    #[test]
    fn test_parse_cluster_meet() {
        let cmd = ClusterCommand::parse("MEET", &["127.0.0.1".into(), "6380".into()]).unwrap();
        assert_eq!(
            cmd,
            ClusterCommand::ClusterMeet("127.0.0.1:6380".parse().unwrap())
        );
    }

    #[test]
    fn test_parse_cluster_addslots() {
        let cmd = ClusterCommand::parse("ADDSLOTS", &["0".into(), "1".into(), "2".into()]).unwrap();
        assert_eq!(cmd, ClusterCommand::ClusterAddSlots(vec![0, 1, 2]));
    }

    #[test]
    fn test_parse_cluster_setslot_migrating() {
        let cmd = ClusterCommand::parse(
            "SETSLOT",
            &["100".into(), "MIGRATING".into(), "node-xyz".into()],
        )
        .unwrap();
        assert_eq!(
            cmd,
            ClusterCommand::ClusterSetSlot {
                slot: 100,
                action: SetSlotAction::Migrating("node-xyz".into()),
            }
        );
    }

    #[test]
    fn test_parse_cluster_setslot_stable() {
        let cmd = ClusterCommand::parse("SETSLOT", &["100".into(), "STABLE".into()]).unwrap();
        assert_eq!(
            cmd,
            ClusterCommand::ClusterSetSlot {
                slot: 100,
                action: SetSlotAction::Stable,
            }
        );
    }

    #[test]
    fn test_parse_cluster_failover_force() {
        let cmd = ClusterCommand::parse("FAILOVER", &["FORCE".into()]).unwrap();
        assert_eq!(
            cmd,
            ClusterCommand::ClusterFailover(Some(FailoverOption::Force))
        );
    }

    #[test]
    fn test_parse_cluster_failover_normal() {
        let cmd = ClusterCommand::parse("FAILOVER", &[]).unwrap();
        assert_eq!(cmd, ClusterCommand::ClusterFailover(None));
    }

    #[test]
    fn test_parse_cluster_reset_hard() {
        let cmd = ClusterCommand::parse("RESET", &["HARD".into()]).unwrap();
        assert_eq!(cmd, ClusterCommand::ClusterReset(true));
    }

    #[test]
    fn test_parse_cluster_reset_soft() {
        let cmd = ClusterCommand::parse("RESET", &[]).unwrap();
        assert_eq!(cmd, ClusterCommand::ClusterReset(false));
    }

    #[test]
    fn test_parse_cluster_unknown_subcommand() {
        assert!(ClusterCommand::parse("BOGUS", &[]).is_err());
    }

    #[test]
    fn test_parse_cluster_countkeysinslot_out_of_range() {
        assert!(ClusterCommand::parse("COUNTKEYSINSLOT", &["20000".into()]).is_err());
    }

    #[test]
    fn test_parse_cluster_set_config_epoch() {
        let cmd = ClusterCommand::parse("SET-CONFIG-EPOCH", &["42".into()]).unwrap();
        assert_eq!(cmd, ClusterCommand::ClusterSetConfigEpoch(42));
    }

    #[test]
    fn test_parse_cluster_set_config_epoch_zero() {
        assert!(ClusterCommand::parse("SET-CONFIG-EPOCH", &["0".into()]).is_err());
    }

    #[test]
    fn test_parse_cluster_getkeysinslot() {
        let cmd = ClusterCommand::parse("GETKEYSINSLOT", &["100".into(), "10".into()]).unwrap();
        assert_eq!(cmd, ClusterCommand::ClusterGetKeysInSlot(100, 10));
    }
}
