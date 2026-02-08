//! Cluster Gossip Protocol
//!
//! This module implements the Redis Cluster gossip protocol for:
//! - Node discovery and membership
//! - Failure detection
//! - Cluster state propagation
//!
//! The gossip protocol uses a dedicated cluster bus on port+10000.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use parking_lot::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use super::{ClusterManager, ClusterNode, NodeId, NodeRole, NodeState, SlotRange};

/// Gossip message types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum GossipMessageType {
    /// Ping message - "are you alive?"
    Ping = 0,
    /// Pong message - response to ping
    Pong = 1,
    /// Meet message - introduce a new node
    Meet = 2,
    /// Fail message - announce a node failure
    Fail = 3,
    /// Update message - update cluster topology
    Update = 4,
    /// Failover auth request
    FailoverAuth = 5,
    /// Failover auth ack
    FailoverAuthAck = 6,
}

impl TryFrom<u8> for GossipMessageType {
    type Error = io::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Ping),
            1 => Ok(Self::Pong),
            2 => Ok(Self::Meet),
            3 => Ok(Self::Fail),
            4 => Ok(Self::Update),
            5 => Ok(Self::FailoverAuth),
            6 => Ok(Self::FailoverAuthAck),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid gossip message type: {}", value),
            )),
        }
    }
}

/// Gossip node info sent in messages
#[derive(Debug, Clone)]
pub struct GossipNodeInfo {
    /// Node ID
    pub id: NodeId,
    /// Node address
    pub addr: SocketAddr,
    /// Node role
    pub role: NodeRole,
    /// Node state
    pub state: NodeState,
    /// Configuration epoch
    pub config_epoch: u64,
    /// Primary ID (for replicas)
    pub primary_id: Option<NodeId>,
    /// Slots served (for primaries)
    pub slots: Vec<SlotRange>,
    /// Last ping time (milliseconds since epoch)
    pub last_ping_ms: u64,
}

impl GossipNodeInfo {
    /// Create from a ClusterNode
    pub fn from_node(node: &ClusterNode) -> Self {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            id: node.id.clone(),
            addr: node.addr,
            role: node.role,
            state: node.state,
            config_epoch: node.config_epoch,
            primary_id: node.primary_id.clone(),
            slots: node.slots.clone(),
            last_ping_ms: now_ms,
        }
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        // Node ID (length + data)
        buf.put_u16(self.id.len() as u16);
        buf.put_slice(self.id.as_bytes());

        // Address (IP as 4 bytes for IPv4 or 16 for IPv6, plus 2 bytes port)
        match self.addr.ip() {
            std::net::IpAddr::V4(ip) => {
                buf.put_u8(4);
                buf.put_slice(&ip.octets());
            }
            std::net::IpAddr::V6(ip) => {
                buf.put_u8(16);
                buf.put_slice(&ip.octets());
            }
        }
        buf.put_u16(self.addr.port());

        // Role and state
        buf.put_u8(self.role as u8);
        buf.put_u8(self.state as u8);

        // Config epoch
        buf.put_u64(self.config_epoch);

        // Primary ID (optional)
        if let Some(ref primary_id) = self.primary_id {
            buf.put_u8(1);
            buf.put_u16(primary_id.len() as u16);
            buf.put_slice(primary_id.as_bytes());
        } else {
            buf.put_u8(0);
        }

        // Slots
        buf.put_u16(self.slots.len() as u16);
        for slot in &self.slots {
            buf.put_u16(slot.start);
            buf.put_u16(slot.end);
        }

        // Last ping
        buf.put_u64(self.last_ping_ms);

        buf.freeze()
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &mut Bytes) -> io::Result<Self> {
        if data.remaining() < 2 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough data",
            ));
        }

        // Node ID
        let id_len = data.get_u16() as usize;
        if data.remaining() < id_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough data for node ID",
            ));
        }
        let id = String::from_utf8(data.copy_to_bytes(id_len).to_vec())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // Address
        if data.remaining() < 1 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough data for IP version",
            ));
        }
        let ip_len = data.get_u8() as usize;
        if data.remaining() < ip_len + 2 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough data for address",
            ));
        }

        let ip = if ip_len == 4 {
            let octets: [u8; 4] = data.copy_to_bytes(4).as_ref().try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "Invalid IPv4 address bytes")
            })?;
            std::net::IpAddr::V4(std::net::Ipv4Addr::from(octets))
        } else if ip_len == 16 {
            let octets: [u8; 16] = data.copy_to_bytes(16).as_ref().try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "Invalid IPv6 address bytes")
            })?;
            std::net::IpAddr::V6(std::net::Ipv6Addr::from(octets))
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid IP length",
            ));
        };
        let port = data.get_u16();
        let addr = SocketAddr::new(ip, port);

        // Role and state
        if data.remaining() < 2 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough data for role/state",
            ));
        }
        let role = match data.get_u8() {
            0 => NodeRole::Primary,
            _ => NodeRole::Replica,
        };
        let state = match data.get_u8() {
            0 => NodeState::Online,
            1 => NodeState::PFail,
            2 => NodeState::Fail,
            3 => NodeState::Handshake,
            _ => NodeState::NoAddr,
        };

        // Config epoch
        if data.remaining() < 8 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough data for epoch",
            ));
        }
        let config_epoch = data.get_u64();

        // Primary ID
        if data.remaining() < 1 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough data for primary ID flag",
            ));
        }
        let has_primary = data.get_u8() == 1;
        let primary_id = if has_primary {
            if data.remaining() < 2 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Not enough data for primary ID",
                ));
            }
            let len = data.get_u16() as usize;
            if data.remaining() < len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Not enough data for primary ID",
                ));
            }
            Some(
                String::from_utf8(data.copy_to_bytes(len).to_vec())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
            )
        } else {
            None
        };

        // Slots
        if data.remaining() < 2 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough data for slots count",
            ));
        }
        let slot_count = data.get_u16() as usize;
        let mut slots = Vec::with_capacity(slot_count);
        for _ in 0..slot_count {
            if data.remaining() < 4 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Not enough data for slot range",
                ));
            }
            let start = data.get_u16();
            let end = data.get_u16();
            slots.push(SlotRange::new(start, end));
        }

        // Last ping
        if data.remaining() < 8 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough data for last ping",
            ));
        }
        let last_ping_ms = data.get_u64();

        Ok(Self {
            id,
            addr,
            role,
            state,
            config_epoch,
            primary_id,
            slots,
            last_ping_ms,
        })
    }
}

/// Gossip message
#[derive(Debug, Clone)]
pub struct GossipMessage {
    /// Message type
    pub msg_type: GossipMessageType,
    /// Sender node ID
    pub sender_id: NodeId,
    /// Current epoch of sender
    pub current_epoch: u64,
    /// Target node ID (for directed messages)
    pub target_id: Option<NodeId>,
    /// Gossip entries (node info)
    pub gossip_entries: Vec<GossipNodeInfo>,
}

impl GossipMessage {
    /// Create a new ping message
    pub fn ping(sender_id: NodeId, current_epoch: u64, entries: Vec<GossipNodeInfo>) -> Self {
        Self {
            msg_type: GossipMessageType::Ping,
            sender_id,
            current_epoch,
            target_id: None,
            gossip_entries: entries,
        }
    }

    /// Create a new pong message
    pub fn pong(sender_id: NodeId, current_epoch: u64, entries: Vec<GossipNodeInfo>) -> Self {
        Self {
            msg_type: GossipMessageType::Pong,
            sender_id,
            current_epoch,
            target_id: None,
            gossip_entries: entries,
        }
    }

    /// Create a meet message
    pub fn meet(sender_id: NodeId, current_epoch: u64, sender_info: GossipNodeInfo) -> Self {
        Self {
            msg_type: GossipMessageType::Meet,
            sender_id,
            current_epoch,
            target_id: None,
            gossip_entries: vec![sender_info],
        }
    }

    /// Create a fail message
    pub fn fail(sender_id: NodeId, current_epoch: u64, failed_node_id: NodeId) -> Self {
        Self {
            msg_type: GossipMessageType::Fail,
            sender_id,
            current_epoch,
            target_id: Some(failed_node_id),
            gossip_entries: vec![],
        }
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        // Message type
        buf.put_u8(self.msg_type as u8);

        // Sender ID
        buf.put_u16(self.sender_id.len() as u16);
        buf.put_slice(self.sender_id.as_bytes());

        // Current epoch
        buf.put_u64(self.current_epoch);

        // Target ID
        if let Some(ref target_id) = self.target_id {
            buf.put_u8(1);
            buf.put_u16(target_id.len() as u16);
            buf.put_slice(target_id.as_bytes());
        } else {
            buf.put_u8(0);
        }

        // Gossip entries
        buf.put_u16(self.gossip_entries.len() as u16);
        for entry in &self.gossip_entries {
            let entry_bytes = entry.to_bytes();
            buf.put_u32(entry_bytes.len() as u32);
            buf.put_slice(&entry_bytes);
        }

        buf.freeze()
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &mut Bytes) -> io::Result<Self> {
        if data.remaining() < 1 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough data",
            ));
        }

        // Message type
        let msg_type = GossipMessageType::try_from(data.get_u8())?;

        // Sender ID
        if data.remaining() < 2 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough data for sender ID",
            ));
        }
        let sender_len = data.get_u16() as usize;
        if data.remaining() < sender_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough data for sender ID",
            ));
        }
        let sender_id = String::from_utf8(data.copy_to_bytes(sender_len).to_vec())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // Current epoch
        if data.remaining() < 8 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough data for epoch",
            ));
        }
        let current_epoch = data.get_u64();

        // Target ID
        if data.remaining() < 1 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough data for target ID flag",
            ));
        }
        let has_target = data.get_u8() == 1;
        let target_id = if has_target {
            if data.remaining() < 2 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Not enough data for target ID",
                ));
            }
            let len = data.get_u16() as usize;
            if data.remaining() < len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Not enough data for target ID",
                ));
            }
            Some(
                String::from_utf8(data.copy_to_bytes(len).to_vec())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
            )
        } else {
            None
        };

        // Gossip entries
        if data.remaining() < 2 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough data for entry count",
            ));
        }
        let entry_count = data.get_u16() as usize;
        let mut gossip_entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            if data.remaining() < 4 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Not enough data for entry length",
                ));
            }
            let entry_len = data.get_u32() as usize;
            if data.remaining() < entry_len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Not enough data for entry",
                ));
            }
            let mut entry_data = data.copy_to_bytes(entry_len);
            gossip_entries.push(GossipNodeInfo::from_bytes(&mut entry_data)?);
        }

        Ok(Self {
            msg_type,
            sender_id,
            current_epoch,
            target_id,
            gossip_entries,
        })
    }
}

/// Gossip protocol configuration
#[derive(Debug, Clone)]
pub struct GossipConfig {
    /// Interval between gossip rounds
    pub gossip_interval: Duration,
    /// Node timeout before marking as PFail
    pub node_timeout: Duration,
    /// Number of nodes to gossip with per round
    pub fanout: usize,
    /// Port offset for cluster bus
    pub bus_port_offset: u16,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            gossip_interval: Duration::from_millis(100),
            node_timeout: Duration::from_secs(15),
            fanout: 3,
            bus_port_offset: 10000,
        }
    }
}

/// Gossip statistics
#[derive(Debug, Clone, Default)]
pub struct GossipStats {
    /// Total pings sent
    pub pings_sent: u64,
    /// Total pongs sent
    pub pongs_sent: u64,
    /// Total pings received
    pub pings_received: u64,
    /// Total pongs received
    pub pongs_received: u64,
    /// Total meet messages sent
    pub meets_sent: u64,
    /// Total fail messages sent
    pub fails_sent: u64,
    /// Connection errors
    pub connection_errors: u64,
}

/// Gossip protocol manager
pub struct GossipManager {
    /// Configuration
    config: GossipConfig,
    /// Reference to cluster manager
    cluster: Arc<ClusterManager>,
    /// Statistics
    stats: GossipStats,
    /// Ping counters per node
    ping_counts: RwLock<HashMap<NodeId, u64>>,
    /// Last ping sent time per node
    last_ping_sent: RwLock<HashMap<NodeId, Instant>>,
    /// Last pong received time per node
    last_pong_received: RwLock<HashMap<NodeId, Instant>>,
    /// PFail votes (node_id -> list of voters)
    pfail_votes: RwLock<HashMap<NodeId, Vec<NodeId>>>,
}

impl GossipManager {
    /// Create a new gossip manager
    pub fn new(config: GossipConfig, cluster: Arc<ClusterManager>) -> Self {
        Self {
            config,
            cluster,
            stats: GossipStats::default(),
            ping_counts: RwLock::new(HashMap::new()),
            last_ping_sent: RwLock::new(HashMap::new()),
            last_pong_received: RwLock::new(HashMap::new()),
            pfail_votes: RwLock::new(HashMap::new()),
        }
    }

    /// Get the cluster bus address for a node
    pub fn bus_addr(&self, node_addr: SocketAddr) -> SocketAddr {
        SocketAddr::new(
            node_addr.ip(),
            node_addr.port() + self.config.bus_port_offset,
        )
    }

    /// Select random nodes for gossip
    fn select_gossip_targets(&self) -> Vec<ClusterNode> {
        use rand::seq::SliceRandom;

        let nodes = self.cluster.get_all_nodes();
        let my_id = self.cluster.node_id();

        // Filter out ourselves and select random nodes
        let mut candidates: Vec<_> = nodes
            .into_iter()
            .filter(|n| &n.id != my_id && n.state != NodeState::Fail)
            .collect();

        candidates.shuffle(&mut rand::thread_rng());
        candidates.truncate(self.config.fanout);
        candidates
    }

    /// Build gossip entries to send
    fn build_gossip_entries(&self) -> Vec<GossipNodeInfo> {
        let nodes = self.cluster.get_all_nodes();
        nodes.iter().map(GossipNodeInfo::from_node).collect()
    }

    /// Send a ping to a node
    pub async fn send_ping(&self, target: &ClusterNode) -> io::Result<()> {
        let bus_addr = self.bus_addr(target.addr);
        debug!("Sending PING to {} at {}", target.id, bus_addr);

        let mut stream = TcpStream::connect(bus_addr).await?;

        let entries = self.build_gossip_entries();
        let msg = GossipMessage::ping(
            self.cluster.node_id().clone(),
            self.cluster.current_epoch(),
            entries,
        );

        let data = msg.to_bytes();
        let len = data.len() as u32;
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(&data).await?;

        // Record ping sent
        self.last_ping_sent
            .write()
            .insert(target.id.clone(), Instant::now());

        Ok(())
    }

    /// Send a meet message to a node
    pub async fn send_meet(&self, addr: SocketAddr) -> io::Result<()> {
        let bus_addr = self.bus_addr(addr);
        info!("Sending MEET to {}", bus_addr);

        let mut stream = TcpStream::connect(bus_addr).await?;

        // Get our own node info
        let my_id = self.cluster.node_id();
        let my_node = self
            .cluster
            .get_node(my_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Own node not found"))?;

        let sender_info = GossipNodeInfo::from_node(&my_node);
        let msg = GossipMessage::meet(my_id.clone(), self.cluster.current_epoch(), sender_info);

        let data = msg.to_bytes();
        let len = data.len() as u32;
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(&data).await?;

        Ok(())
    }

    /// Handle an incoming gossip message
    pub async fn handle_message(&self, msg: GossipMessage) -> io::Result<Option<GossipMessage>> {
        debug!("Received {:?} from {}", msg.msg_type, msg.sender_id);

        match msg.msg_type {
            GossipMessageType::Ping => {
                // Process gossip entries
                self.process_gossip_entries(&msg.gossip_entries);

                // Send pong response
                let entries = self.build_gossip_entries();
                let response = GossipMessage::pong(
                    self.cluster.node_id().clone(),
                    self.cluster.current_epoch(),
                    entries,
                );
                Ok(Some(response))
            }
            GossipMessageType::Pong => {
                // Record pong received
                self.last_pong_received
                    .write()
                    .insert(msg.sender_id.clone(), Instant::now());

                // Process gossip entries
                self.process_gossip_entries(&msg.gossip_entries);
                Ok(None)
            }
            GossipMessageType::Meet => {
                // Add the sender as a new node
                if let Some(sender_info) = msg.gossip_entries.first() {
                    let node = ClusterNode::new(sender_info.id.clone(), sender_info.addr);
                    self.cluster.add_node(node);
                    info!("Added node {} via MEET", sender_info.id);
                }

                // Send pong response
                let entries = self.build_gossip_entries();
                let response = GossipMessage::pong(
                    self.cluster.node_id().clone(),
                    self.cluster.current_epoch(),
                    entries,
                );
                Ok(Some(response))
            }
            GossipMessageType::Fail => {
                // Mark the target node as failed
                if let Some(ref failed_id) = msg.target_id {
                    self.cluster.mark_node_failed(failed_id);
                    warn!("Node {} marked as FAIL via gossip", failed_id);
                }
                Ok(None)
            }
            GossipMessageType::Update => {
                // Update cluster topology
                self.process_gossip_entries(&msg.gossip_entries);
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    /// Process gossip entries from a message
    fn process_gossip_entries(&self, entries: &[GossipNodeInfo]) {
        let my_id = self.cluster.node_id();

        for entry in entries {
            // Skip ourselves
            if &entry.id == my_id {
                continue;
            }

            // Check if we know this node
            if let Some(existing) = self.cluster.get_node(&entry.id) {
                // Update if the incoming info has a higher epoch
                if entry.config_epoch > existing.config_epoch {
                    // Update node info
                    let mut updated_node = existing.clone();
                    updated_node.config_epoch = entry.config_epoch;
                    updated_node.state = entry.state;
                    updated_node.slots = entry.slots.clone();
                    self.cluster.add_node(updated_node);
                }

                // Check failure state propagation
                match entry.state {
                    NodeState::PFail => {
                        // Record PFail vote
                        self.record_pfail_vote(&entry.id, &entry.id);
                    }
                    NodeState::Fail => {
                        self.cluster.mark_node_failed(&entry.id);
                    }
                    _ => {}
                }
            } else {
                // New node - add it
                let new_node = ClusterNode {
                    id: entry.id.clone(),
                    addr: entry.addr,
                    role: entry.role,
                    state: entry.state,
                    primary_id: entry.primary_id.clone(),
                    slots: entry.slots.clone(),
                    last_ping: None,
                    last_pong: None,
                    config_epoch: entry.config_epoch,
                    flags: Default::default(),
                };
                self.cluster.add_node(new_node);
                debug!("Discovered new node {} via gossip", entry.id);
            }
        }
    }

    /// Record a PFail vote for a node
    fn record_pfail_vote(&self, node_id: &NodeId, voter_id: &NodeId) {
        let should_mark_fail = {
            let mut votes = self.pfail_votes.write();
            let node_votes = votes.entry(node_id.clone()).or_default();

            if !node_votes.contains(voter_id) {
                node_votes.push(voter_id.clone());

                // Check if we have quorum for FAIL
                let total_nodes = self.cluster.get_all_nodes().len();
                let required_votes = (total_nodes / 2) + 1;

                if node_votes.len() >= required_votes {
                    Some((node_votes.len(), required_votes))
                } else {
                    None
                }
            } else {
                None
            }
        };

        if let Some((vote_count, required_votes)) = should_mark_fail {
            self.cluster.mark_node_failed(node_id);
            warn!(
                "Node {} marked as FAIL (quorum reached: {}/{})",
                node_id, vote_count, required_votes
            );
        }
    }

    /// Check for node timeouts and mark as PFail
    pub fn check_node_timeouts(&self) {
        let now = Instant::now();
        let timeout = self.config.node_timeout;
        let my_id = self.cluster.node_id();

        let last_pong = self.last_pong_received.read();

        for node in self.cluster.get_all_nodes() {
            if &node.id == my_id {
                continue;
            }

            // Check if we've received a pong recently
            if let Some(last) = last_pong.get(&node.id) {
                if now.duration_since(*last) > timeout && node.state == NodeState::Online {
                    self.cluster.mark_node_pfail(&node.id);
                    debug!(
                        "Node {} marked as PFAIL (no pong for {:?})",
                        node.id,
                        now.duration_since(*last)
                    );
                }
            }
        }
    }

    /// Get gossip statistics
    pub fn stats(&self) -> GossipStats {
        self.stats.clone()
    }
}

/// Handle for controlling the gossip service
pub struct GossipHandle {
    /// Shutdown sender
    shutdown_tx: broadcast::Sender<()>,
    /// Listener task handle
    listener_handle: Option<tokio::task::JoinHandle<()>>,
    /// Gossip task handle
    gossip_handle: Option<tokio::task::JoinHandle<()>>,
}

impl GossipHandle {
    /// Signal the gossip service to stop
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }

    /// Wait for all gossip tasks to complete
    pub async fn wait(self) {
        if let Some(handle) = self.listener_handle {
            let _ = handle.await;
        }
        if let Some(handle) = self.gossip_handle {
            let _ = handle.await;
        }
    }
}

/// Start the gossip service
///
/// This spawns:
/// - A TCP listener for the cluster bus
/// - A periodic gossip task that pings other nodes
pub async fn start_gossip_service(
    gossip_manager: Arc<GossipManager>,
    bind_addr: SocketAddr,
) -> io::Result<GossipHandle> {
    let (shutdown_tx, _) = broadcast::channel(1);

    // Calculate cluster bus address
    let bus_addr = gossip_manager.bus_addr(bind_addr);

    // Start cluster bus listener
    let listener = TcpListener::bind(bus_addr).await?;
    info!("Cluster bus listening on {}", bus_addr);

    // Spawn listener task
    let gm_listener = gossip_manager.clone();
    let mut rx_listener = shutdown_tx.subscribe();
    let listener_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            debug!("Cluster bus connection from {}", addr);
                            let gm = gm_listener.clone();
                            tokio::spawn(handle_cluster_connection(stream, gm));
                        }
                        Err(e) => {
                            error!("Cluster bus accept error: {}", e);
                        }
                    }
                }
                _ = rx_listener.recv() => {
                    info!("Gossip listener shutting down");
                    break;
                }
            }
        }
    });

    // Spawn periodic gossip task
    let gm_gossip = gossip_manager.clone();
    let mut rx_gossip = shutdown_tx.subscribe();
    let gossip_interval = gossip_manager.config.gossip_interval;
    let gossip_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(gossip_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Select nodes to gossip with
                    let targets = gm_gossip.select_gossip_targets();

                    // Send pings
                    for target in targets {
                        if let Err(e) = gm_gossip.send_ping(&target).await {
                            debug!("Failed to ping {}: {}", target.id, e);
                        }
                    }

                    // Check for node timeouts
                    gm_gossip.check_node_timeouts();
                }
                _ = rx_gossip.recv() => {
                    info!("Gossip task shutting down");
                    break;
                }
            }
        }
    });

    Ok(GossipHandle {
        shutdown_tx,
        listener_handle: Some(listener_handle),
        gossip_handle: Some(gossip_handle),
    })
}

/// Handle a cluster bus connection
async fn handle_cluster_connection(mut stream: TcpStream, gossip_manager: Arc<GossipManager>) {
    loop {
        // Read message length
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => {
                debug!("Cluster connection read error: {}", e);
                break;
            }
        }

        let len = u32::from_be_bytes(len_buf) as usize;
        if len > 1024 * 1024 {
            // 1MB limit
            warn!("Gossip message too large: {} bytes", len);
            break;
        }

        // Read message data
        let mut data = vec![0u8; len];
        if let Err(e) = stream.read_exact(&mut data).await {
            debug!("Failed to read gossip message: {}", e);
            break;
        }

        // Parse and handle message
        let mut bytes = Bytes::from(data);
        match GossipMessage::from_bytes(&mut bytes) {
            Ok(msg) => {
                match gossip_manager.handle_message(msg).await {
                    Ok(Some(response)) => {
                        // Send response
                        let response_data = response.to_bytes();
                        let response_len = response_data.len() as u32;
                        if stream.write_all(&response_len.to_be_bytes()).await.is_err() {
                            break;
                        }
                        if stream.write_all(&response_data).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        debug!("Failed to handle gossip message: {}", e);
                    }
                }
            }
            Err(e) => {
                debug!("Failed to parse gossip message: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_node_info() -> GossipNodeInfo {
        GossipNodeInfo {
            id: "test-node-123".to_string(),
            addr: "127.0.0.1:6379".parse().unwrap(),
            role: NodeRole::Primary,
            state: NodeState::Online,
            config_epoch: 1,
            primary_id: None,
            slots: vec![SlotRange::new(0, 5461)],
            last_ping_ms: 1234567890,
        }
    }

    #[test]
    fn test_gossip_node_info_serialization() {
        let info = create_test_node_info();
        let bytes = info.to_bytes();
        let mut data = bytes;
        let decoded = GossipNodeInfo::from_bytes(&mut data).unwrap();

        assert_eq!(decoded.id, info.id);
        assert_eq!(decoded.addr, info.addr);
        assert_eq!(decoded.role as u8, info.role as u8);
        assert_eq!(decoded.state as u8, info.state as u8);
        assert_eq!(decoded.config_epoch, info.config_epoch);
        assert_eq!(decoded.slots.len(), info.slots.len());
    }

    #[test]
    fn test_gossip_message_ping() {
        let entries = vec![create_test_node_info()];
        let msg = GossipMessage::ping("sender-123".to_string(), 5, entries);

        let bytes = msg.to_bytes();
        let mut data = bytes;
        let decoded = GossipMessage::from_bytes(&mut data).unwrap();

        assert_eq!(decoded.msg_type, GossipMessageType::Ping);
        assert_eq!(decoded.sender_id, "sender-123");
        assert_eq!(decoded.current_epoch, 5);
        assert_eq!(decoded.gossip_entries.len(), 1);
    }

    #[test]
    fn test_gossip_message_pong() {
        let entries = vec![create_test_node_info()];
        let msg = GossipMessage::pong("sender-456".to_string(), 10, entries);

        let bytes = msg.to_bytes();
        let mut data = bytes;
        let decoded = GossipMessage::from_bytes(&mut data).unwrap();

        assert_eq!(decoded.msg_type, GossipMessageType::Pong);
        assert_eq!(decoded.sender_id, "sender-456");
        assert_eq!(decoded.current_epoch, 10);
    }

    #[test]
    fn test_gossip_message_meet() {
        let info = create_test_node_info();
        let msg = GossipMessage::meet("sender-789".to_string(), 1, info);

        let bytes = msg.to_bytes();
        let mut data = bytes;
        let decoded = GossipMessage::from_bytes(&mut data).unwrap();

        assert_eq!(decoded.msg_type, GossipMessageType::Meet);
        assert_eq!(decoded.sender_id, "sender-789");
        assert_eq!(decoded.gossip_entries.len(), 1);
    }

    #[test]
    fn test_gossip_message_fail() {
        let msg = GossipMessage::fail("sender-aaa".to_string(), 100, "failed-node".to_string());

        let bytes = msg.to_bytes();
        let mut data = bytes;
        let decoded = GossipMessage::from_bytes(&mut data).unwrap();

        assert_eq!(decoded.msg_type, GossipMessageType::Fail);
        assert_eq!(decoded.sender_id, "sender-aaa");
        assert_eq!(decoded.target_id, Some("failed-node".to_string()));
    }

    #[test]
    fn test_gossip_message_type_conversion() {
        assert_eq!(
            GossipMessageType::try_from(0).unwrap(),
            GossipMessageType::Ping
        );
        assert_eq!(
            GossipMessageType::try_from(1).unwrap(),
            GossipMessageType::Pong
        );
        assert_eq!(
            GossipMessageType::try_from(2).unwrap(),
            GossipMessageType::Meet
        );
        assert_eq!(
            GossipMessageType::try_from(3).unwrap(),
            GossipMessageType::Fail
        );
        assert!(GossipMessageType::try_from(99).is_err());
    }

    #[test]
    fn test_gossip_config_default() {
        let config = GossipConfig::default();
        assert_eq!(config.gossip_interval, Duration::from_millis(100));
        assert_eq!(config.node_timeout, Duration::from_secs(15));
        assert_eq!(config.fanout, 3);
        assert_eq!(config.bus_port_offset, 10000);
    }

    #[test]
    fn test_bus_addr_calculation() {
        use super::super::{generate_node_id, ClusterConfig};

        let node_id = generate_node_id();
        let node_addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let config = ClusterConfig::default();
        let cluster = Arc::new(ClusterManager::new(config, node_id, node_addr));

        let gossip_config = GossipConfig::default();
        let gossip_manager = GossipManager::new(gossip_config, cluster);

        let bus_addr = gossip_manager.bus_addr(node_addr);
        assert_eq!(bus_addr.port(), 16379);
    }

    #[test]
    fn test_gossip_manager_creation() {
        use super::super::{generate_node_id, ClusterConfig};

        let node_id = generate_node_id();
        let node_addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let config = ClusterConfig::default();
        let cluster = Arc::new(ClusterManager::new(config, node_id, node_addr));

        let gossip_config = GossipConfig {
            gossip_interval: Duration::from_millis(50),
            node_timeout: Duration::from_secs(5),
            fanout: 2,
            bus_port_offset: 10000,
        };
        let gossip_manager = GossipManager::new(gossip_config, cluster);

        // Initially no ping/pong activity
        let stats = gossip_manager.stats();
        assert_eq!(stats.pings_sent, 0);
        assert_eq!(stats.pongs_sent, 0);
    }

    #[test]
    fn test_gossip_node_info_with_replica() {
        let info = GossipNodeInfo {
            id: "replica-node-123".to_string(),
            addr: "127.0.0.1:6380".parse().unwrap(),
            role: NodeRole::Replica,
            state: NodeState::Online,
            config_epoch: 5,
            primary_id: Some("primary-node-456".to_string()),
            slots: vec![],
            last_ping_ms: 9876543210,
        };

        let bytes = info.to_bytes();
        let mut data = bytes;
        let decoded = GossipNodeInfo::from_bytes(&mut data).unwrap();

        assert_eq!(decoded.id, info.id);
        assert_eq!(decoded.role as u8, NodeRole::Replica as u8);
        assert_eq!(decoded.primary_id, info.primary_id);
        assert!(decoded.slots.is_empty());
    }

    #[test]
    fn test_gossip_node_info_ipv6() {
        let info = GossipNodeInfo {
            id: "ipv6-node".to_string(),
            addr: "[::1]:6379".parse().unwrap(),
            role: NodeRole::Primary,
            state: NodeState::Online,
            config_epoch: 1,
            primary_id: None,
            slots: vec![],
            last_ping_ms: 0,
        };

        let bytes = info.to_bytes();
        let mut data = bytes;
        let decoded = GossipNodeInfo::from_bytes(&mut data).unwrap();

        assert_eq!(decoded.addr, "[::1]:6379".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn test_gossip_entries_multiple_slots() {
        let info = GossipNodeInfo {
            id: "multi-slot-node".to_string(),
            addr: "127.0.0.1:6379".parse().unwrap(),
            role: NodeRole::Primary,
            state: NodeState::Online,
            config_epoch: 10,
            primary_id: None,
            slots: vec![SlotRange::new(0, 5461), SlotRange::new(10923, 16383)],
            last_ping_ms: 1234567890,
        };

        let bytes = info.to_bytes();
        let mut data = bytes;
        let decoded = GossipNodeInfo::from_bytes(&mut data).unwrap();

        assert_eq!(decoded.slots.len(), 2);
        assert_eq!(decoded.slots[0].start, 0);
        assert_eq!(decoded.slots[0].end, 5461);
        assert_eq!(decoded.slots[1].start, 10923);
        assert_eq!(decoded.slots[1].end, 16383);
    }

    #[tokio::test]
    async fn test_gossip_service_startup_shutdown() {
        use super::super::{generate_node_id, ClusterConfig};

        let node_id = generate_node_id();
        let node_addr: SocketAddr = "127.0.0.1:16500".parse().unwrap();
        let config = ClusterConfig::default();
        let cluster = Arc::new(ClusterManager::new(config, node_id, node_addr));

        let gossip_config = GossipConfig {
            gossip_interval: Duration::from_millis(1000), // Slow for test
            node_timeout: Duration::from_secs(15),
            fanout: 3,
            bus_port_offset: 10000,
        };
        let gossip_manager = Arc::new(GossipManager::new(gossip_config, cluster));

        // Start the gossip service
        let handle = start_gossip_service(gossip_manager.clone(), node_addr)
            .await
            .expect("Failed to start gossip service");

        // Let it run briefly
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Shutdown
        handle.shutdown();
        handle.wait().await;
    }

    #[tokio::test]
    async fn test_handle_meet_message() {
        use super::super::{generate_node_id, ClusterConfig};

        let node_id = generate_node_id();
        let node_addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let config = ClusterConfig::default();
        let cluster = Arc::new(ClusterManager::new(config, node_id, node_addr));

        let gossip_config = GossipConfig::default();
        let gossip_manager = GossipManager::new(gossip_config, cluster.clone());

        // Create a MEET message from a new node
        let new_node_info = GossipNodeInfo {
            id: "new-node-xyz".to_string(),
            addr: "127.0.0.1:6380".parse().unwrap(),
            role: NodeRole::Primary,
            state: NodeState::Online,
            config_epoch: 1,
            primary_id: None,
            slots: vec![],
            last_ping_ms: 0,
        };
        let meet_msg = GossipMessage::meet("new-node-xyz".to_string(), 1, new_node_info);

        // Handle the MEET message
        let response = gossip_manager.handle_message(meet_msg).await.unwrap();

        // Should get a PONG response
        assert!(response.is_some());
        let pong = response.unwrap();
        assert_eq!(pong.msg_type, GossipMessageType::Pong);

        // The new node should be added to the cluster
        let new_node = cluster.get_node(&"new-node-xyz".to_string());
        assert!(new_node.is_some());
        assert_eq!(
            new_node.unwrap().addr,
            "127.0.0.1:6380".parse::<SocketAddr>().unwrap()
        );
    }

    #[tokio::test]
    async fn test_handle_ping_message() {
        use super::super::{generate_node_id, ClusterConfig};

        let node_id = generate_node_id();
        let node_addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let config = ClusterConfig::default();
        let cluster = Arc::new(ClusterManager::new(config, node_id.clone(), node_addr));

        let gossip_config = GossipConfig::default();
        let gossip_manager = GossipManager::new(gossip_config, cluster.clone());

        // Create a PING message with gossip entries
        let entries = vec![GossipNodeInfo {
            id: "other-node".to_string(),
            addr: "127.0.0.1:6380".parse().unwrap(),
            role: NodeRole::Primary,
            state: NodeState::Online,
            config_epoch: 2,
            primary_id: None,
            slots: vec![SlotRange::new(0, 5461)],
            last_ping_ms: 0,
        }];
        let ping_msg = GossipMessage::ping("other-node".to_string(), 1, entries);

        // Handle the PING message
        let response = gossip_manager.handle_message(ping_msg).await.unwrap();

        // Should get a PONG response
        assert!(response.is_some());
        let pong = response.unwrap();
        assert_eq!(pong.msg_type, GossipMessageType::Pong);
        assert_eq!(pong.sender_id, node_id);

        // The other node should be discovered via gossip
        let other_node = cluster.get_node(&"other-node".to_string());
        assert!(other_node.is_some());
    }

    #[tokio::test]
    async fn test_handle_fail_message() {
        use super::super::{generate_node_id, ClusterConfig, ClusterNode};

        let node_id = generate_node_id();
        let node_addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let config = ClusterConfig::default();
        let cluster = Arc::new(ClusterManager::new(config, node_id, node_addr));

        // Add a node that will be marked as failed
        let target_node =
            ClusterNode::new("target-node".to_string(), "127.0.0.1:6380".parse().unwrap());
        cluster.add_node(target_node);

        let gossip_config = GossipConfig::default();
        let gossip_manager = GossipManager::new(gossip_config, cluster.clone());

        // Create a FAIL message
        let fail_msg =
            GossipMessage::fail("reporter-node".to_string(), 1, "target-node".to_string());

        // Handle the FAIL message
        let response = gossip_manager.handle_message(fail_msg).await.unwrap();

        // No response expected for FAIL
        assert!(response.is_none());

        // The target node should be marked as failed
        let target = cluster.get_node(&"target-node".to_string()).unwrap();
        assert_eq!(target.state, NodeState::Fail);
    }
}
