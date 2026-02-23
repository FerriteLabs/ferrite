// Redis Cluster Bus protocol implementation
//
// Implements the binary protocol used for node-to-node communication on the
// cluster bus port (base port + 10000 by default). This is the transport layer
// that carries gossip, failure detection, failover voting, configuration
// updates, and slot migration coordination between Ferrite cluster nodes.
//
// Wire format (70-byte fixed header):
//   sig(4B) | ver(2B) | type(2B) | sender(40B) | flags(2B) |
//   config_epoch(8B) | current_epoch(8B) | total_length(4B)
// followed by variable-length payload.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tracing::{debug, warn};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Magic signature bytes for the cluster bus protocol.
const SIGNATURE: [u8; 4] = *b"RCmb";

/// Current protocol version.
const PROTOCOL_VERSION: u16 = 1;

/// Fixed header size in bytes.
const HEADER_SIZE: usize = 70;

/// Length of a Redis cluster node ID in bytes (40 hex characters).
const NODE_ID_LEN: usize = 40;

/// Total number of hash slots in a Redis / Ferrite cluster.
const TOTAL_SLOTS: usize = 16384;

/// Size of the slots bitmap (one bit per slot).
const SLOTS_BITMAP_SIZE: usize = TOTAL_SLOTS / 8; // 2048 bytes

// ---------------------------------------------------------------------------
// BusError
// ---------------------------------------------------------------------------

/// Errors that can occur during cluster bus operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum BusError {
    /// Failed to establish a connection to a peer node.
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// A send or receive operation exceeded the configured timeout.
    #[error("operation timed out")]
    Timeout,

    /// The received message is malformed or cannot be decoded.
    #[error("invalid message: {0}")]
    InvalidMessage(String),

    /// The target node is not known to the cluster bus.
    #[error("node not found")]
    NodeNotFound,

    /// Failed to encode a message for transmission.
    #[error("encoding error: {0}")]
    EncodingError(String),

    /// The connection pool has reached its configured limit.
    #[error("max connections reached")]
    MaxConnectionsReached,

    /// An unclassified internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// NodeId
// ---------------------------------------------------------------------------

/// A 40-byte Redis-compatible cluster node identifier.
///
/// In Redis, node IDs are 40-character lowercase hexadecimal strings. This
/// type stores the raw bytes for zero-copy header encoding and provides
/// helpers for hex conversion.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub [u8; NODE_ID_LEN]);

impl Serialize for NodeId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for NodeId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::from_hex(&s).map_err(serde::de::Error::custom)
    }
}

impl NodeId {
    /// Generate a random node ID.
    pub fn random() -> Self {
        let mut id = [0u8; NODE_ID_LEN];
        let hex_chars = b"0123456789abcdef";
        for byte in &mut id {
            *byte = hex_chars[rand::random::<usize>() % 16];
        }
        Self(id)
    }

    /// Parse a node ID from a 40-character hex string.
    pub fn from_hex(s: &str) -> Result<Self, BusError> {
        let bytes = s.as_bytes();
        if bytes.len() != NODE_ID_LEN {
            return Err(BusError::InvalidMessage(format!(
                "node ID must be {} hex characters, got {}",
                NODE_ID_LEN,
                bytes.len()
            )));
        }
        for &b in bytes {
            if !b.is_ascii_hexdigit() {
                return Err(BusError::InvalidMessage(format!(
                    "invalid hex character in node ID: '{}'",
                    b as char
                )));
            }
        }
        let mut id = [0u8; NODE_ID_LEN];
        id.copy_from_slice(bytes);
        Ok(Self(id))
    }

    /// Return the 40-character hex representation.
    pub fn to_hex(&self) -> String {
        String::from_utf8_lossy(&self.0).to_string()
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({})", self.to_hex())
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

// ---------------------------------------------------------------------------
// NodeAddress
// ---------------------------------------------------------------------------

/// Network address of a cluster node (client port + bus port).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeAddress {
    /// Hostname or IP address.
    pub host: String,
    /// Client-facing port (e.g. 6379).
    pub port: u16,
    /// Cluster bus port (typically `port + port_offset`).
    pub bus_port: u16,
}

impl fmt::Display for NodeAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{} (bus {})", self.host, self.port, self.bus_port)
    }
}

// ---------------------------------------------------------------------------
// ClusterBusState
// ---------------------------------------------------------------------------

/// High-level cluster health state carried in ping/pong payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterBusState {
    /// Cluster is operating normally — all slots covered.
    Ok = 0,
    /// Cluster is in a degraded state — some slots are uncovered.
    Fail = 1,
}

impl ClusterBusState {
    fn from_u8(v: u8) -> Result<Self, BusError> {
        match v {
            0 => Ok(Self::Ok),
            1 => Ok(Self::Fail),
            _ => Err(BusError::InvalidMessage(format!(
                "unknown cluster bus state: {v}"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// MessageType
// ---------------------------------------------------------------------------

/// Discriminant for cluster bus message types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u16)]
pub enum MessageType {
    /// Heartbeat request.
    Ping = 0,
    /// Heartbeat reply.
    Pong = 1,
    /// Introduce a new node to the cluster.
    Meet = 2,
    /// Report a node as failed (PFAIL → FAIL promotion).
    Fail = 3,
    /// Pub/Sub message forwarded across nodes.
    Publish = 4,
    /// Slot ownership update after a config epoch change.
    Update = 5,
    /// Request a failover authorization vote.
    FailAuth = 6,
    /// Grant a failover authorization vote.
    FailAuthAck = 7,
    /// Manual failover initiation.
    MFStart = 8,
    /// Module-specific extension message.
    Module = 9,
}

impl MessageType {
    fn from_u16(v: u16) -> Result<Self, BusError> {
        match v {
            0 => Ok(Self::Ping),
            1 => Ok(Self::Pong),
            2 => Ok(Self::Meet),
            3 => Ok(Self::Fail),
            4 => Ok(Self::Publish),
            5 => Ok(Self::Update),
            6 => Ok(Self::FailAuth),
            7 => Ok(Self::FailAuthAck),
            8 => Ok(Self::MFStart),
            9 => Ok(Self::Module),
            _ => Err(BusError::InvalidMessage(format!(
                "unknown message type: {v}"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// MessageHeader
// ---------------------------------------------------------------------------

/// Fixed-size binary header for every cluster bus message.
///
/// Total encoded size: [`HEADER_SIZE`] (70 bytes).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageHeader {
    /// Magic bytes (`RCmb`).
    pub signature: [u8; 4],
    /// Protocol version (currently 1).
    pub version: u16,
    /// Type of the message.
    pub message_type: MessageType,
    /// ID of the sending node.
    pub sender: NodeId,
    /// Bitfield flags (reserved for future use).
    pub flags: u16,
    /// The sender's current config epoch for its slot ownership.
    pub config_epoch: u64,
    /// The highest epoch known to the sender.
    pub current_epoch: u64,
    /// Total message length including header and payload.
    pub total_length: u32,
}

impl MessageHeader {
    /// Create a new header with the given type and sender.
    pub fn new(message_type: MessageType, sender: NodeId) -> Self {
        Self {
            signature: SIGNATURE,
            version: PROTOCOL_VERSION,
            message_type,
            sender,
            flags: 0,
            config_epoch: 0,
            current_epoch: 0,
            total_length: 0, // filled during encoding
        }
    }

    /// Encode the header into `buf`.
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_slice(&self.signature);
        buf.put_u16_le(self.version);
        buf.put_u16_le(self.message_type as u16);
        buf.put_slice(&self.sender.0);
        buf.put_u16_le(self.flags);
        buf.put_u64_le(self.config_epoch);
        buf.put_u64_le(self.current_epoch);
        buf.put_u32_le(self.total_length);
    }

    /// Decode a header from the start of `buf`.
    fn decode(buf: &[u8]) -> Result<Self, BusError> {
        if buf.len() < HEADER_SIZE {
            return Err(BusError::InvalidMessage(format!(
                "buffer too short for header: need {HEADER_SIZE}, got {}",
                buf.len()
            )));
        }
        let mut cursor = &buf[..HEADER_SIZE];

        let mut sig = [0u8; 4];
        cursor.copy_to_slice(&mut sig);
        if sig != SIGNATURE {
            return Err(BusError::InvalidMessage(format!(
                "invalid signature: expected {:?}, got {sig:?}",
                SIGNATURE
            )));
        }

        let version = cursor.get_u16_le();
        let msg_type = MessageType::from_u16(cursor.get_u16_le())?;

        let mut sender_bytes = [0u8; NODE_ID_LEN];
        cursor.copy_to_slice(&mut sender_bytes);
        let sender = NodeId(sender_bytes);

        let flags = cursor.get_u16_le();
        let config_epoch = cursor.get_u64_le();
        let current_epoch = cursor.get_u64_le();
        let total_length = cursor.get_u32_le();

        Ok(Self {
            signature: sig,
            version,
            message_type: msg_type,
            sender,
            flags,
            config_epoch,
            current_epoch,
            total_length,
        })
    }
}

// ---------------------------------------------------------------------------
// MessagePayload
// ---------------------------------------------------------------------------

/// Variable-length payload portion of a cluster bus message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessagePayload {
    /// Heartbeat request carrying slot ownership and cluster state.
    Ping {
        /// Bitmap of the 16384 hash slots owned by this node.
        slots_bitmap: Vec<u8>,
        /// Current cluster health as seen by the sender.
        cluster_state: ClusterBusState,
        /// Number of slots this node is serving.
        served_slots: u16,
    },
    /// Heartbeat reply (same structure as Ping).
    Pong {
        /// Bitmap of the 16384 hash slots owned by this node.
        slots_bitmap: Vec<u8>,
        /// Current cluster health as seen by the sender.
        cluster_state: ClusterBusState,
        /// Number of slots this node is serving.
        served_slots: u16,
    },
    /// Request to add a new node to the cluster.
    Meet {
        /// IP address or hostname of the node to meet.
        ip: String,
        /// Client-facing port.
        port: u16,
        /// Cluster bus port.
        bus_port: u16,
    },
    /// Broadcast that a node has entered FAIL state.
    Fail {
        /// ID of the failed node.
        node_id: NodeId,
    },
    /// Slot ownership update after an epoch change.
    Update {
        /// ID of the node whose slots changed.
        node_id: NodeId,
        /// The config epoch that authorizes this change.
        config_epoch: u64,
        /// New slots bitmap for `node_id`.
        slots: Vec<u8>,
    },
    /// Pub/Sub message forwarded through the cluster bus.
    Publish {
        /// Channel name.
        channel: String,
        /// Payload bytes.
        message: Bytes,
    },
    /// Request a failover authorization vote for a node.
    FailAuth {
        /// ID of the node requesting authorization.
        node_id: NodeId,
    },
    /// Grant a failover authorization vote.
    FailAuthAck {
        /// ID of the node being acknowledged.
        node_id: NodeId,
    },
    /// Manual failover start signal.
    MFStart,
    /// Placeholder for message types that carry no payload.
    Empty,
}

impl MessagePayload {
    /// Encode the payload into `buf`.
    fn encode(&self, buf: &mut BytesMut) {
        match self {
            Self::Ping {
                slots_bitmap,
                cluster_state,
                served_slots,
            }
            | Self::Pong {
                slots_bitmap,
                cluster_state,
                served_slots,
            } => {
                encode_ping_pong(buf, slots_bitmap, *cluster_state, *served_slots);
            }
            Self::Meet { ip, port, bus_port } => {
                let ip_bytes = ip.as_bytes();
                buf.put_u16_le(ip_bytes.len() as u16);
                buf.put_slice(ip_bytes);
                buf.put_u16_le(*port);
                buf.put_u16_le(*bus_port);
            }
            Self::Fail { node_id } => {
                buf.put_slice(&node_id.0);
            }
            Self::Update {
                node_id,
                config_epoch,
                slots,
            } => {
                buf.put_slice(&node_id.0);
                buf.put_u64_le(*config_epoch);
                let slot_bytes = slots.as_slice();
                buf.put_u16_le(slot_bytes.len() as u16);
                buf.put_slice(slot_bytes);
            }
            Self::Publish { channel, message } => {
                let ch_bytes = channel.as_bytes();
                buf.put_u32_le(ch_bytes.len() as u32);
                buf.put_slice(ch_bytes);
                buf.put_u32_le(message.len() as u32);
                buf.put_slice(message);
            }
            Self::FailAuth { node_id } | Self::FailAuthAck { node_id } => {
                buf.put_slice(&node_id.0);
            }
            Self::MFStart | Self::Empty => { /* no payload bytes */ }
        }
    }

    /// Decode the payload from `buf` given the message type.
    fn decode(msg_type: MessageType, mut buf: &[u8]) -> Result<Self, BusError> {
        match msg_type {
            MessageType::Ping => {
                let (bitmap, state, served) = decode_ping_pong(&mut buf)?;
                Ok(Self::Ping {
                    slots_bitmap: bitmap,
                    cluster_state: state,
                    served_slots: served,
                })
            }
            MessageType::Pong => {
                let (bitmap, state, served) = decode_ping_pong(&mut buf)?;
                Ok(Self::Pong {
                    slots_bitmap: bitmap,
                    cluster_state: state,
                    served_slots: served,
                })
            }
            MessageType::Meet => {
                if buf.remaining() < 6 {
                    return Err(BusError::InvalidMessage(
                        "meet payload too short".into(),
                    ));
                }
                let ip_len = buf.get_u16_le() as usize;
                if buf.remaining() < ip_len + 4 {
                    return Err(BusError::InvalidMessage(
                        "meet payload truncated".into(),
                    ));
                }
                let mut ip_bytes = vec![0u8; ip_len];
                buf.copy_to_slice(&mut ip_bytes);
                let ip = String::from_utf8(ip_bytes).map_err(|e| {
                    BusError::InvalidMessage(format!("invalid IP string: {e}"))
                })?;
                let port = buf.get_u16_le();
                let bus_port = buf.get_u16_le();
                Ok(Self::Meet { ip, port, bus_port })
            }
            MessageType::Fail => {
                if buf.remaining() < NODE_ID_LEN {
                    return Err(BusError::InvalidMessage(
                        "fail payload too short".into(),
                    ));
                }
                let mut id = [0u8; NODE_ID_LEN];
                buf.copy_to_slice(&mut id);
                Ok(Self::Fail {
                    node_id: NodeId(id),
                })
            }
            MessageType::Update => {
                if buf.remaining() < NODE_ID_LEN + 8 + 2 {
                    return Err(BusError::InvalidMessage(
                        "update payload too short".into(),
                    ));
                }
                let mut id = [0u8; NODE_ID_LEN];
                buf.copy_to_slice(&mut id);
                let config_epoch = buf.get_u64_le();
                let slots_len = buf.get_u16_le() as usize;
                if buf.remaining() < slots_len {
                    return Err(BusError::InvalidMessage(
                        "update payload truncated".into(),
                    ));
                }
                let mut slots = vec![0u8; slots_len];
                buf.copy_to_slice(&mut slots);
                Ok(Self::Update {
                    node_id: NodeId(id),
                    config_epoch,
                    slots,
                })
            }
            MessageType::Publish => {
                if buf.remaining() < 8 {
                    return Err(BusError::InvalidMessage(
                        "publish payload too short".into(),
                    ));
                }
                let ch_len = buf.get_u32_le() as usize;
                if buf.remaining() < ch_len + 4 {
                    return Err(BusError::InvalidMessage(
                        "publish payload truncated".into(),
                    ));
                }
                let mut ch_bytes = vec![0u8; ch_len];
                buf.copy_to_slice(&mut ch_bytes);
                let channel = String::from_utf8(ch_bytes).map_err(|e| {
                    BusError::InvalidMessage(format!("invalid channel: {e}"))
                })?;
                let msg_len = buf.get_u32_le() as usize;
                if buf.remaining() < msg_len {
                    return Err(BusError::InvalidMessage(
                        "publish payload truncated".into(),
                    ));
                }
                let mut msg_bytes = vec![0u8; msg_len];
                buf.copy_to_slice(&mut msg_bytes);
                Ok(Self::Publish {
                    channel,
                    message: Bytes::from(msg_bytes),
                })
            }
            MessageType::FailAuth => {
                if buf.remaining() < NODE_ID_LEN {
                    return Err(BusError::InvalidMessage(
                        "fail_auth payload too short".into(),
                    ));
                }
                let mut id = [0u8; NODE_ID_LEN];
                buf.copy_to_slice(&mut id);
                Ok(Self::FailAuth {
                    node_id: NodeId(id),
                })
            }
            MessageType::FailAuthAck => {
                if buf.remaining() < NODE_ID_LEN {
                    return Err(BusError::InvalidMessage(
                        "fail_auth_ack payload too short".into(),
                    ));
                }
                let mut id = [0u8; NODE_ID_LEN];
                buf.copy_to_slice(&mut id);
                Ok(Self::FailAuthAck {
                    node_id: NodeId(id),
                })
            }
            MessageType::MFStart => Ok(Self::MFStart),
            MessageType::Module => Ok(Self::Empty),
        }
    }
}

// Helper: encode ping/pong body
fn encode_ping_pong(
    buf: &mut BytesMut,
    slots_bitmap: &[u8],
    cluster_state: ClusterBusState,
    served_slots: u16,
) {
    // Pad or truncate bitmap to exactly SLOTS_BITMAP_SIZE bytes.
    if slots_bitmap.len() >= SLOTS_BITMAP_SIZE {
        buf.put_slice(&slots_bitmap[..SLOTS_BITMAP_SIZE]);
    } else {
        buf.put_slice(slots_bitmap);
        buf.put_bytes(0, SLOTS_BITMAP_SIZE - slots_bitmap.len());
    }
    buf.put_u8(cluster_state as u8);
    buf.put_u16_le(served_slots);
}

// Helper: decode ping/pong body
fn decode_ping_pong(
    buf: &mut &[u8],
) -> Result<(Vec<u8>, ClusterBusState, u16), BusError> {
    if buf.remaining() < SLOTS_BITMAP_SIZE + 3 {
        return Err(BusError::InvalidMessage(
            "ping/pong payload too short".into(),
        ));
    }
    let mut bitmap = vec![0u8; SLOTS_BITMAP_SIZE];
    buf.copy_to_slice(&mut bitmap);
    let state = ClusterBusState::from_u8(buf.get_u8())?;
    let served = buf.get_u16_le();
    Ok((bitmap, state, served))
}

// ---------------------------------------------------------------------------
// ClusterMessage
// ---------------------------------------------------------------------------

/// A complete cluster bus message (header + payload).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterMessage {
    /// Fixed-size header.
    pub header: MessageHeader,
    /// Variable-length payload.
    pub payload: MessagePayload,
}

impl ClusterMessage {
    /// Create a new cluster message.
    pub fn new(
        message_type: MessageType,
        sender: NodeId,
        payload: MessagePayload,
    ) -> Self {
        Self {
            header: MessageHeader::new(message_type, sender),
            payload,
        }
    }

    /// Encode this message into a byte buffer suitable for transmission.
    pub fn encode(&self) -> BytesMut {
        // Encode payload first to learn its size.
        let mut payload_buf = BytesMut::new();
        self.payload.encode(&mut payload_buf);

        let total_length = (HEADER_SIZE + payload_buf.len()) as u32;

        let mut buf = BytesMut::with_capacity(total_length as usize);
        let mut header = self.header.clone();
        header.total_length = total_length;
        header.encode(&mut buf);
        buf.extend_from_slice(&payload_buf);
        buf
    }

    /// Decode a message from a raw byte slice.
    pub fn decode(buf: &[u8]) -> Result<Self, BusError> {
        let header = MessageHeader::decode(buf)?;
        let payload_start = HEADER_SIZE;
        if buf.len() < header.total_length as usize {
            return Err(BusError::InvalidMessage(format!(
                "buffer shorter than total_length: {} < {}",
                buf.len(),
                header.total_length
            )));
        }
        let payload_bytes = &buf[payload_start..header.total_length as usize];
        let payload = MessagePayload::decode(header.message_type, payload_bytes)?;
        Ok(Self { header, payload })
    }
}

// ---------------------------------------------------------------------------
// Slots bitmap helpers
// ---------------------------------------------------------------------------

/// Set slot `slot` in a 2048-byte bitmap.
pub fn bitmap_set_slot(bitmap: &mut [u8], slot: u16) {
    let idx = slot as usize;
    if idx < TOTAL_SLOTS && bitmap.len() >= SLOTS_BITMAP_SIZE {
        bitmap[idx / 8] |= 1 << (idx % 8);
    }
}

/// Test whether slot `slot` is set in a 2048-byte bitmap.
pub fn bitmap_get_slot(bitmap: &[u8], slot: u16) -> bool {
    let idx = slot as usize;
    if idx < TOTAL_SLOTS && bitmap.len() >= SLOTS_BITMAP_SIZE {
        (bitmap[idx / 8] & (1 << (idx % 8))) != 0
    } else {
        false
    }
}

// ---------------------------------------------------------------------------
// MessageHandler trait
// ---------------------------------------------------------------------------

/// Trait for components that react to incoming cluster bus messages.
///
/// Implementations can optionally return a response message (e.g. a Pong for
/// every Ping). Returning `None` means no reply is sent.
#[async_trait]
pub trait MessageHandler: Send + Sync {
    /// Process an inbound message and optionally produce a reply.
    async fn handle_message(
        &self,
        sender: &NodeAddress,
        msg: &ClusterMessage,
    ) -> Option<ClusterMessage>;
}

// ---------------------------------------------------------------------------
// BusStats
// ---------------------------------------------------------------------------

/// Runtime statistics for the cluster bus.
#[derive(Debug, Default)]
pub struct BusStats {
    /// Total messages sent.
    pub messages_sent: u64,
    /// Total messages received.
    pub messages_received: u64,
    /// Ping messages sent.
    pub ping_sent: u64,
    /// Pong messages received.
    pub pong_received: u64,
    /// Meet messages sent.
    pub meet_sent: u64,
    /// Fail messages sent.
    pub fail_sent: u64,
    /// Total bytes written to the network.
    pub bytes_sent: u64,
    /// Total bytes read from the network.
    pub bytes_received: u64,
    /// Number of errors encountered.
    pub errors: u64,
    /// Number of currently connected peer nodes.
    pub connected_nodes: u64,
}

/// Atomic counters backing [`BusStats`].
#[derive(Debug, Default)]
struct AtomicBusStats {
    messages_sent: AtomicU64,
    messages_received: AtomicU64,
    ping_sent: AtomicU64,
    pong_received: AtomicU64,
    meet_sent: AtomicU64,
    fail_sent: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
    errors: AtomicU64,
}

impl AtomicBusStats {
    fn snapshot(&self, connected_nodes: u64) -> BusStats {
        BusStats {
            messages_sent: self.messages_sent.load(Ordering::Relaxed),
            messages_received: self.messages_received.load(Ordering::Relaxed),
            ping_sent: self.ping_sent.load(Ordering::Relaxed),
            pong_received: self.pong_received.load(Ordering::Relaxed),
            meet_sent: self.meet_sent.load(Ordering::Relaxed),
            fail_sent: self.fail_sent.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            connected_nodes,
        }
    }
}

// ---------------------------------------------------------------------------
// ClusterBusConfig
// ---------------------------------------------------------------------------

/// Configuration for the cluster bus transport.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterBusConfig {
    /// Offset added to the client port to derive the bus port (default 10000).
    pub port_offset: u16,
    /// TCP connect timeout in milliseconds.
    pub connect_timeout_ms: u64,
    /// Per-message send/receive timeout in milliseconds.
    pub message_timeout_ms: u64,
    /// Maximum number of outbound connections to maintain.
    pub max_connections: usize,
    /// Interval between automatic Ping rounds in milliseconds.
    pub ping_interval_ms: u64,
}

impl Default for ClusterBusConfig {
    fn default() -> Self {
        Self {
            port_offset: 10000,
            connect_timeout_ms: 5000,
            message_timeout_ms: 10000,
            max_connections: 128,
            ping_interval_ms: 1000,
        }
    }
}

impl ClusterBusConfig {
    /// Validate the configuration, returning an error for invalid values.
    pub fn validate(&self) -> Result<(), BusError> {
        if self.connect_timeout_ms == 0 {
            return Err(BusError::Internal(
                "connect_timeout_ms must be > 0".into(),
            ));
        }
        if self.message_timeout_ms == 0 {
            return Err(BusError::Internal(
                "message_timeout_ms must be > 0".into(),
            ));
        }
        if self.max_connections == 0 {
            return Err(BusError::Internal(
                "max_connections must be > 0".into(),
            ));
        }
        if self.ping_interval_ms == 0 {
            return Err(BusError::Internal(
                "ping_interval_ms must be > 0".into(),
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ClusterBus
// ---------------------------------------------------------------------------

/// Main cluster bus manager.
///
/// Manages outbound connections to peer nodes, dispatches inbound messages to
/// registered handlers, and tracks per-type statistics.
pub struct ClusterBus {
    /// Bus configuration.
    config: ClusterBusConfig,
    /// Known peer addresses keyed by a string identifier (`host:bus_port`).
    peers: DashMap<String, NodeAddress>,
    /// Registered message handlers.
    handlers: RwLock<Vec<Arc<dyn MessageHandler>>>,
    /// Atomic statistics counters.
    stats: Arc<AtomicBusStats>,
}

impl ClusterBus {
    /// Create a new cluster bus with the given configuration.
    pub fn new(config: ClusterBusConfig) -> Self {
        Self {
            config,
            peers: DashMap::new(),
            handlers: RwLock::new(Vec::new()),
            stats: Arc::new(AtomicBusStats::default()),
        }
    }

    /// Register a peer node so it can be targeted by [`send_message`](Self::send_message).
    pub fn add_peer(&self, addr: NodeAddress) {
        let key = format!("{}:{}", addr.host, addr.bus_port);
        debug!(peer = %key, "adding cluster bus peer");
        self.peers.insert(key, addr);
    }

    /// Remove a peer from the known set.
    pub fn remove_peer(&self, addr: &NodeAddress) {
        let key = format!("{}:{}", addr.host, addr.bus_port);
        self.peers.remove(&key);
    }

    /// Send a single message to a target node.
    pub async fn send_message(
        &self,
        target: &NodeAddress,
        msg: ClusterMessage,
    ) -> Result<(), BusError> {
        let encoded = msg.encode();
        let encoded_len = encoded.len() as u64;

        let addr = format!("{}:{}", target.host, target.bus_port);

        let connect_result = tokio::time::timeout(
            std::time::Duration::from_millis(self.config.connect_timeout_ms),
            TcpStream::connect(&addr),
        )
        .await;

        let mut stream = match connect_result {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                return Err(BusError::ConnectionFailed(format!(
                    "TCP connect to {addr}: {e}"
                )));
            }
            Err(_) => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                return Err(BusError::Timeout);
            }
        };

        let write_result = tokio::time::timeout(
            std::time::Duration::from_millis(self.config.message_timeout_ms),
            stream.write_all(&encoded),
        )
        .await;

        match write_result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                return Err(BusError::ConnectionFailed(format!(
                    "write to {addr}: {e}"
                )));
            }
            Err(_) => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                return Err(BusError::Timeout);
            }
        }

        // Update stats per message type.
        self.stats.messages_sent.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes_sent.fetch_add(encoded_len, Ordering::Relaxed);

        match msg.header.message_type {
            MessageType::Ping => {
                self.stats.ping_sent.fetch_add(1, Ordering::Relaxed);
            }
            MessageType::Meet => {
                self.stats.meet_sent.fetch_add(1, Ordering::Relaxed);
            }
            MessageType::Fail => {
                self.stats.fail_sent.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }

        debug!(target = %addr, msg_type = ?msg.header.message_type, "sent cluster bus message");
        Ok(())
    }

    /// Broadcast a message to every known peer.
    ///
    /// Returns a vector of `(NodeAddress, Result)` pairs — one per peer.
    pub async fn broadcast(
        &self,
        msg: ClusterMessage,
    ) -> Vec<(NodeAddress, Result<(), BusError>)> {
        let peers: Vec<NodeAddress> =
            self.peers.iter().map(|r| r.value().clone()).collect();

        let mut results = Vec::with_capacity(peers.len());
        for peer in peers {
            let result = self.send_message(&peer, msg.clone()).await;
            results.push((peer, result));
        }
        results
    }

    /// Register a handler that will be invoked for every inbound message.
    pub async fn register_handler(&self, handler: Arc<dyn MessageHandler>) {
        self.handlers.write().await.push(handler);
    }

    /// Return a snapshot of the current bus statistics.
    pub fn get_stats(&self) -> BusStats {
        let connected = self.peers.len() as u64;
        self.stats.snapshot(connected)
    }

    /// Dispatch an inbound message to all registered handlers.
    ///
    /// Returns the first non-`None` response produced by a handler, if any.
    pub async fn dispatch(
        &self,
        sender: &NodeAddress,
        msg: &ClusterMessage,
    ) -> Option<ClusterMessage> {
        self.stats
            .messages_received
            .fetch_add(1, Ordering::Relaxed);

        if msg.header.message_type == MessageType::Pong {
            self.stats.pong_received.fetch_add(1, Ordering::Relaxed);
        }

        let handlers = self.handlers.read().await;
        for handler in handlers.iter() {
            if let Some(response) = handler.handle_message(sender, msg).await {
                return Some(response);
            }
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── helpers ──────────────────────────────────────────────────────────

    fn test_node_id() -> NodeId {
        NodeId::from_hex("a]bcdef0123456789abcdef0123456789abcdef01"
            .replace(']', "")
            .trim())
        .unwrap_or_else(|_| {
            let mut id = [b'0'; NODE_ID_LEN];
            for (i, b) in b"abcdef0123456789abcdef0123456789abcdef01"
                .iter()
                .enumerate()
            {
                id[i] = *b;
            }
            NodeId(id)
        })
    }

    fn make_node_id(hex: &str) -> NodeId {
        let padded = format!("{:0<40}", hex);
        NodeId::from_hex(&padded).unwrap()
    }

    fn test_address() -> NodeAddress {
        NodeAddress {
            host: "127.0.0.1".into(),
            port: 6379,
            bus_port: 16379,
        }
    }

    // ── NodeId tests ────────────────────────────────────────────────────

    #[test]
    fn node_id_random_is_valid_hex() {
        let id = NodeId::random();
        let hex = id.to_hex();
        assert_eq!(hex.len(), 40);
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn node_id_hex_roundtrip() {
        let hex_str = "abcdef0123456789abcdef0123456789abcdef01";
        let id = NodeId::from_hex(hex_str).unwrap();
        assert_eq!(id.to_hex(), hex_str);
    }

    #[test]
    fn node_id_from_hex_wrong_length() {
        assert!(NodeId::from_hex("abc").is_err());
    }

    #[test]
    fn node_id_from_hex_invalid_char() {
        let bad = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
        assert!(NodeId::from_hex(bad).is_err());
    }

    // ── Message encode/decode roundtrip ─────────────────────────────────

    #[test]
    fn encode_decode_ping_roundtrip() {
        let sender = make_node_id("aabbccdd");
        let mut bitmap = vec![0u8; SLOTS_BITMAP_SIZE];
        bitmap_set_slot(&mut bitmap, 0);
        bitmap_set_slot(&mut bitmap, 100);
        bitmap_set_slot(&mut bitmap, 16383);

        let msg = ClusterMessage::new(
            MessageType::Ping,
            sender,
            MessagePayload::Ping {
                slots_bitmap: bitmap.clone(),
                cluster_state: ClusterBusState::Ok,
                served_slots: 3,
            },
        );

        let encoded = msg.encode();
        let decoded = ClusterMessage::decode(&encoded).unwrap();

        assert_eq!(decoded.header.message_type, MessageType::Ping);
        assert_eq!(decoded.header.sender, sender);
        if let MessagePayload::Ping {
            slots_bitmap,
            cluster_state,
            served_slots,
        } = &decoded.payload
        {
            assert_eq!(slots_bitmap, &bitmap);
            assert_eq!(*cluster_state, ClusterBusState::Ok);
            assert_eq!(*served_slots, 3);
        } else {
            panic!("expected Ping payload");
        }
    }

    #[test]
    fn encode_decode_pong_roundtrip() {
        let sender = make_node_id("11223344");
        let bitmap = vec![0u8; SLOTS_BITMAP_SIZE];
        let msg = ClusterMessage::new(
            MessageType::Pong,
            sender,
            MessagePayload::Pong {
                slots_bitmap: bitmap.clone(),
                cluster_state: ClusterBusState::Fail,
                served_slots: 0,
            },
        );

        let encoded = msg.encode();
        let decoded = ClusterMessage::decode(&encoded).unwrap();
        assert_eq!(decoded.header.message_type, MessageType::Pong);
        if let MessagePayload::Pong { cluster_state, .. } = &decoded.payload {
            assert_eq!(*cluster_state, ClusterBusState::Fail);
        } else {
            panic!("expected Pong payload");
        }
    }

    #[test]
    fn encode_decode_meet_roundtrip() {
        let sender = make_node_id("deadbeef");
        let msg = ClusterMessage::new(
            MessageType::Meet,
            sender,
            MessagePayload::Meet {
                ip: "192.168.1.100".into(),
                port: 6380,
                bus_port: 16380,
            },
        );

        let encoded = msg.encode();
        let decoded = ClusterMessage::decode(&encoded).unwrap();

        if let MessagePayload::Meet { ip, port, bus_port } = &decoded.payload {
            assert_eq!(ip, "192.168.1.100");
            assert_eq!(*port, 6380);
            assert_eq!(*bus_port, 16380);
        } else {
            panic!("expected Meet payload");
        }
    }

    #[test]
    fn encode_decode_fail_roundtrip() {
        let sender = make_node_id("aa");
        let failed = make_node_id("bb");
        let msg = ClusterMessage::new(
            MessageType::Fail,
            sender,
            MessagePayload::Fail { node_id: failed },
        );

        let encoded = msg.encode();
        let decoded = ClusterMessage::decode(&encoded).unwrap();

        if let MessagePayload::Fail { node_id } = &decoded.payload {
            assert_eq!(*node_id, failed);
        } else {
            panic!("expected Fail payload");
        }
    }

    #[test]
    fn encode_decode_update_roundtrip() {
        let sender = make_node_id("1111");
        let target = make_node_id("2222");
        let slots = vec![0xFFu8; SLOTS_BITMAP_SIZE];
        let msg = ClusterMessage::new(
            MessageType::Update,
            sender,
            MessagePayload::Update {
                node_id: target,
                config_epoch: 42,
                slots: slots.clone(),
            },
        );

        let encoded = msg.encode();
        let decoded = ClusterMessage::decode(&encoded).unwrap();

        if let MessagePayload::Update {
            node_id,
            config_epoch,
            slots: s,
        } = &decoded.payload
        {
            assert_eq!(*node_id, target);
            assert_eq!(*config_epoch, 42);
            assert_eq!(s, &slots);
        } else {
            panic!("expected Update payload");
        }
    }

    #[test]
    fn encode_decode_publish_roundtrip() {
        let sender = make_node_id("cafe");
        let msg = ClusterMessage::new(
            MessageType::Publish,
            sender,
            MessagePayload::Publish {
                channel: "events".into(),
                message: Bytes::from_static(b"hello world"),
            },
        );

        let encoded = msg.encode();
        let decoded = ClusterMessage::decode(&encoded).unwrap();

        if let MessagePayload::Publish { channel, message } = &decoded.payload {
            assert_eq!(channel, "events");
            assert_eq!(message.as_ref(), b"hello world");
        } else {
            panic!("expected Publish payload");
        }
    }

    #[test]
    fn encode_decode_fail_auth_roundtrip() {
        let sender = make_node_id("aaaa");
        let target = make_node_id("bbbb");
        let msg = ClusterMessage::new(
            MessageType::FailAuth,
            sender,
            MessagePayload::FailAuth { node_id: target },
        );

        let encoded = msg.encode();
        let decoded = ClusterMessage::decode(&encoded).unwrap();

        if let MessagePayload::FailAuth { node_id } = &decoded.payload {
            assert_eq!(*node_id, target);
        } else {
            panic!("expected FailAuth payload");
        }
    }

    #[test]
    fn encode_decode_fail_auth_ack_roundtrip() {
        let sender = make_node_id("cccc");
        let target = make_node_id("dddd");
        let msg = ClusterMessage::new(
            MessageType::FailAuthAck,
            sender,
            MessagePayload::FailAuthAck { node_id: target },
        );

        let encoded = msg.encode();
        let decoded = ClusterMessage::decode(&encoded).unwrap();

        if let MessagePayload::FailAuthAck { node_id } = &decoded.payload {
            assert_eq!(*node_id, target);
        } else {
            panic!("expected FailAuthAck payload");
        }
    }

    #[test]
    fn encode_decode_mfstart_roundtrip() {
        let sender = make_node_id("eeee");
        let msg = ClusterMessage::new(
            MessageType::MFStart,
            sender,
            MessagePayload::MFStart,
        );

        let encoded = msg.encode();
        let decoded = ClusterMessage::decode(&encoded).unwrap();
        assert_eq!(decoded.header.message_type, MessageType::MFStart);
        assert_eq!(decoded.payload, MessagePayload::MFStart);
    }

    // ── Slots bitmap ────────────────────────────────────────────────────

    #[test]
    fn bitmap_set_and_get() {
        let mut bitmap = vec![0u8; SLOTS_BITMAP_SIZE];

        assert!(!bitmap_get_slot(&bitmap, 0));
        bitmap_set_slot(&mut bitmap, 0);
        assert!(bitmap_get_slot(&bitmap, 0));

        bitmap_set_slot(&mut bitmap, 16383);
        assert!(bitmap_get_slot(&bitmap, 16383));

        assert!(!bitmap_get_slot(&bitmap, 1));
        assert!(!bitmap_get_slot(&bitmap, 8000));
    }

    #[test]
    fn bitmap_all_slots() {
        let mut bitmap = vec![0u8; SLOTS_BITMAP_SIZE];
        for slot in 0..16384u16 {
            bitmap_set_slot(&mut bitmap, slot);
        }
        for slot in 0..16384u16 {
            assert!(bitmap_get_slot(&bitmap, slot), "slot {slot} not set");
        }
    }

    #[test]
    fn bitmap_out_of_range_is_noop() {
        let mut bitmap = vec![0u8; SLOTS_BITMAP_SIZE];
        bitmap_set_slot(&mut bitmap, 16384); // out of range
        assert!(!bitmap_get_slot(&bitmap, 16384));
    }

    // ── Header ──────────────────────────────────────────────────────────

    #[test]
    fn header_encode_decode_roundtrip() {
        let sender = make_node_id("face");
        let mut header = MessageHeader::new(MessageType::Meet, sender);
        header.config_epoch = 7;
        header.current_epoch = 12;
        header.flags = 0x0003;
        header.total_length = HEADER_SIZE as u32;

        let mut buf = BytesMut::new();
        header.encode(&mut buf);
        assert_eq!(buf.len(), HEADER_SIZE);

        let decoded = MessageHeader::decode(&buf).unwrap();
        assert_eq!(decoded.signature, SIGNATURE);
        assert_eq!(decoded.version, PROTOCOL_VERSION);
        assert_eq!(decoded.message_type, MessageType::Meet);
        assert_eq!(decoded.sender, sender);
        assert_eq!(decoded.flags, 0x0003);
        assert_eq!(decoded.config_epoch, 7);
        assert_eq!(decoded.current_epoch, 12);
        assert_eq!(decoded.total_length, HEADER_SIZE as u32);
    }

    #[test]
    fn header_decode_bad_signature() {
        let mut buf = vec![0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(b"XXXX");
        assert!(MessageHeader::decode(&buf).is_err());
    }

    #[test]
    fn header_decode_buffer_too_short() {
        let buf = vec![0u8; 10];
        assert!(MessageHeader::decode(&buf).is_err());
    }

    // ── Config validation ───────────────────────────────────────────────

    #[test]
    fn default_config_is_valid() {
        ClusterBusConfig::default().validate().unwrap();
    }

    #[test]
    fn config_zero_timeout_rejected() {
        let mut cfg = ClusterBusConfig::default();
        cfg.connect_timeout_ms = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn config_zero_connections_rejected() {
        let mut cfg = ClusterBusConfig::default();
        cfg.max_connections = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn config_zero_ping_interval_rejected() {
        let mut cfg = ClusterBusConfig::default();
        cfg.ping_interval_ms = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn config_zero_message_timeout_rejected() {
        let mut cfg = ClusterBusConfig::default();
        cfg.message_timeout_ms = 0;
        assert!(cfg.validate().is_err());
    }

    // ── Stats ───────────────────────────────────────────────────────────

    #[test]
    fn stats_initial_values() {
        let bus = ClusterBus::new(ClusterBusConfig::default());
        let stats = bus.get_stats();
        assert_eq!(stats.messages_sent, 0);
        assert_eq!(stats.messages_received, 0);
        assert_eq!(stats.ping_sent, 0);
        assert_eq!(stats.pong_received, 0);
        assert_eq!(stats.meet_sent, 0);
        assert_eq!(stats.fail_sent, 0);
        assert_eq!(stats.bytes_sent, 0);
        assert_eq!(stats.bytes_received, 0);
        assert_eq!(stats.errors, 0);
        assert_eq!(stats.connected_nodes, 0);
    }

    #[test]
    fn stats_connected_nodes_tracks_peers() {
        let bus = ClusterBus::new(ClusterBusConfig::default());
        let addr = test_address();
        bus.add_peer(addr.clone());
        assert_eq!(bus.get_stats().connected_nodes, 1);

        bus.remove_peer(&addr);
        assert_eq!(bus.get_stats().connected_nodes, 0);
    }

    // ── Dispatch ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn dispatch_increments_received_counter() {
        let bus = ClusterBus::new(ClusterBusConfig::default());
        let sender_addr = test_address();
        let sender_id = make_node_id("1234");
        let msg = ClusterMessage::new(
            MessageType::Ping,
            sender_id,
            MessagePayload::Ping {
                slots_bitmap: vec![0u8; SLOTS_BITMAP_SIZE],
                cluster_state: ClusterBusState::Ok,
                served_slots: 0,
            },
        );

        bus.dispatch(&sender_addr, &msg).await;
        assert_eq!(bus.get_stats().messages_received, 1);
    }

    #[tokio::test]
    async fn dispatch_pong_increments_pong_counter() {
        let bus = ClusterBus::new(ClusterBusConfig::default());
        let sender_addr = test_address();
        let sender_id = make_node_id("5678");
        let msg = ClusterMessage::new(
            MessageType::Pong,
            sender_id,
            MessagePayload::Pong {
                slots_bitmap: vec![0u8; SLOTS_BITMAP_SIZE],
                cluster_state: ClusterBusState::Ok,
                served_slots: 0,
            },
        );

        bus.dispatch(&sender_addr, &msg).await;
        assert_eq!(bus.get_stats().pong_received, 1);
    }

    // ── Error display ───────────────────────────────────────────────────

    #[test]
    fn error_display_messages() {
        assert_eq!(
            BusError::ConnectionFailed("refused".into()).to_string(),
            "connection failed: refused"
        );
        assert_eq!(BusError::Timeout.to_string(), "operation timed out");
        assert_eq!(
            BusError::InvalidMessage("bad".into()).to_string(),
            "invalid message: bad"
        );
        assert_eq!(BusError::NodeNotFound.to_string(), "node not found");
        assert_eq!(
            BusError::EncodingError("oops".into()).to_string(),
            "encoding error: oops"
        );
        assert_eq!(
            BusError::MaxConnectionsReached.to_string(),
            "max connections reached"
        );
        assert_eq!(
            BusError::Internal("fail".into()).to_string(),
            "internal error: fail"
        );
    }

    // ── MessageType conversion ──────────────────────────────────────────

    #[test]
    fn message_type_roundtrip() {
        let types = [
            MessageType::Ping,
            MessageType::Pong,
            MessageType::Meet,
            MessageType::Fail,
            MessageType::Publish,
            MessageType::Update,
            MessageType::FailAuth,
            MessageType::FailAuthAck,
            MessageType::MFStart,
            MessageType::Module,
        ];
        for t in types {
            let val = t as u16;
            let back = MessageType::from_u16(val).unwrap();
            assert_eq!(back, t);
        }
    }

    #[test]
    fn message_type_unknown_value() {
        assert!(MessageType::from_u16(999).is_err());
    }

    // ── ClusterBusState conversion ──────────────────────────────────────

    #[test]
    fn cluster_bus_state_roundtrip() {
        assert_eq!(ClusterBusState::from_u8(0).unwrap(), ClusterBusState::Ok);
        assert_eq!(ClusterBusState::from_u8(1).unwrap(), ClusterBusState::Fail);
        assert!(ClusterBusState::from_u8(2).is_err());
    }

    // ── Total length in header ──────────────────────────────────────────

    #[test]
    fn encoded_message_total_length_matches() {
        let sender = make_node_id("f00d");
        let msg = ClusterMessage::new(
            MessageType::Meet,
            sender,
            MessagePayload::Meet {
                ip: "10.0.0.1".into(),
                port: 7000,
                bus_port: 17000,
            },
        );
        let encoded = msg.encode();
        let total_len =
            u32::from_le_bytes(encoded[66..70].try_into().unwrap());
        assert_eq!(total_len as usize, encoded.len());
    }

    // ── Handler registration ────────────────────────────────────────────

    #[tokio::test]
    async fn register_handler_and_dispatch() {
        struct EchoHandler;

        #[async_trait]
        impl MessageHandler for EchoHandler {
            async fn handle_message(
                &self,
                _sender: &NodeAddress,
                msg: &ClusterMessage,
            ) -> Option<ClusterMessage> {
                if msg.header.message_type == MessageType::Ping {
                    Some(ClusterMessage::new(
                        MessageType::Pong,
                        msg.header.sender,
                        MessagePayload::Pong {
                            slots_bitmap: vec![0u8; SLOTS_BITMAP_SIZE],
                            cluster_state: ClusterBusState::Ok,
                            served_slots: 0,
                        },
                    ))
                } else {
                    None
                }
            }
        }

        let bus = ClusterBus::new(ClusterBusConfig::default());
        bus.register_handler(Arc::new(EchoHandler)).await;

        let sender_id = make_node_id("abcd");
        let ping = ClusterMessage::new(
            MessageType::Ping,
            sender_id,
            MessagePayload::Ping {
                slots_bitmap: vec![0u8; SLOTS_BITMAP_SIZE],
                cluster_state: ClusterBusState::Ok,
                served_slots: 0,
            },
        );

        let response = bus.dispatch(&test_address(), &ping).await;
        assert!(response.is_some());
        let resp = response.unwrap();
        assert_eq!(resp.header.message_type, MessageType::Pong);
    }
}
