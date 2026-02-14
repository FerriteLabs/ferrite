//! Real-Time Sync Protocol
//!
//! Enables bidirectional real-time synchronization between Ferrite and
//! client applications using CRDT-based conflict resolution and delta
//! state transfer over WebSocket connections.
//!
//! # Architecture
//!
//! ```text
//! ┌───────────────────┐          ┌─────────────────────────┐
//! │  Client SDK       │ ◄──WS──►│   Sync Gateway          │
//! │  (JS/TS/Rust)     │         │   ┌─────────────────┐    │
//! │  ┌──────────────┐ │         │   │  Session Manager │    │
//! │  │ Local State   │ │         │   └────────┬────────┘    │
//! │  │ (CRDT)       │ │         │            │             │
//! │  ├──────────────┤ │         │   ┌────────▼────────┐    │
//! │  │ Delta Queue   │ │         │   │  Delta Router   │    │
//! │  ├──────────────┤ │         │   └────────┬────────┘    │
//! │  │ Offline Queue │ │         │            │             │
//! │  └──────────────┘ │         │   ┌────────▼────────┐    │
//! └───────────────────┘         │   │  Ferrite Store   │    │
//!                               │   └─────────────────┘    │
//!                               └─────────────────────────┘
//! ```
//!
//! # Features
//!
//! - Delta-state CRDTs for efficient sync
//! - Offline support with mutation queue
//! - Subscription-based change notifications
//! - Causal ordering via vector clocks
//! - Automatic reconnection with state reconciliation

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::debug;

/// A vector clock for causal ordering
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorClock {
    entries: HashMap<String, u64>,
}

impl VectorClock {
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment the clock for a given node
    pub fn increment(&mut self, node_id: &str) {
        let counter = self.entries.entry(node_id.to_string()).or_insert(0);
        *counter += 1;
    }

    /// Get the counter for a node
    pub fn get(&self, node_id: &str) -> u64 {
        self.entries.get(node_id).copied().unwrap_or(0)
    }

    /// Merge with another vector clock (take max of each entry)
    pub fn merge(&mut self, other: &VectorClock) {
        for (node, &count) in &other.entries {
            let entry = self.entries.entry(node.clone()).or_insert(0);
            *entry = (*entry).max(count);
        }
    }

    /// Check if this clock happened-before another
    pub fn happened_before(&self, other: &VectorClock) -> bool {
        let mut at_least_one_less = false;
        for (node, &count) in &self.entries {
            let other_count = other.get(node);
            if count > other_count {
                return false;
            }
            if count < other_count {
                at_least_one_less = true;
            }
        }
        // Check for entries in other that we don't have
        for (node, &count) in &other.entries {
            if count > 0 && self.get(node) == 0 {
                at_least_one_less = true;
            }
        }
        at_least_one_less
    }

    /// Check if two clocks are concurrent (neither happened-before the other)
    pub fn is_concurrent(&self, other: &VectorClock) -> bool {
        !self.happened_before(other) && !other.happened_before(self) && self != other
    }
}

/// A delta operation representing a state change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncDelta {
    /// Unique delta ID
    pub id: String,
    /// Client that produced this delta
    pub client_id: String,
    /// Key affected
    pub key: String,
    /// The operation
    pub operation: SyncOperation,
    /// Vector clock at time of operation
    pub clock: VectorClock,
    /// Timestamp (wall clock, for display/debugging)
    pub timestamp: u64,
    /// Whether this delta originated offline
    pub from_offline: bool,
}

/// Operations that can be synced
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncOperation {
    /// Set a string value (LWW register semantics)
    Set { value: String },
    /// Delete a key
    Delete,
    /// Increment a counter
    Increment { amount: i64 },
    /// Add to a set (OR-Set semantics)
    SetAdd { member: String },
    /// Remove from a set
    SetRemove { member: String },
    /// Set a hash field
    HashSet { field: String, value: String },
    /// Remove a hash field
    HashDelete { field: String },
    /// Append to a list
    ListAppend { value: String },
}

/// A subscription to key changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    /// Unique subscription ID
    pub id: String,
    /// Client that subscribed
    pub client_id: String,
    /// Key patterns to watch
    pub patterns: Vec<String>,
    /// Whether to include the full value or just the key
    pub include_value: bool,
    /// Created timestamp
    pub created_at: u64,
}

/// Connection state for a sync client
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientState {
    /// Initial connection handshake
    Connecting,
    /// Connected and syncing
    Connected,
    /// Temporarily disconnected (will retry)
    Disconnected,
    /// Reconnecting with state reconciliation
    Reconnecting,
    /// Permanently closed
    Closed,
}

impl fmt::Display for ClientState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientState::Connecting => write!(f, "Connecting"),
            ClientState::Connected => write!(f, "Connected"),
            ClientState::Disconnected => write!(f, "Disconnected"),
            ClientState::Reconnecting => write!(f, "Reconnecting"),
            ClientState::Closed => write!(f, "Closed"),
        }
    }
}

/// A connected sync client session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncSession {
    /// Client ID
    pub client_id: String,
    /// Current connection state
    pub state: ClientState,
    /// Client's vector clock (last known)
    pub clock: VectorClock,
    /// Active subscriptions
    pub subscriptions: Vec<Subscription>,
    /// Pending deltas to send to this client
    pub pending_deltas: Vec<SyncDelta>,
    /// Number of deltas sent
    pub deltas_sent: u64,
    /// Number of deltas received
    pub deltas_received: u64,
    /// Last activity timestamp
    pub last_activity: u64,
}

/// Configuration for the sync gateway
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
    /// Maximum number of concurrent sync clients
    pub max_clients: usize,
    /// Delta batch size (max deltas per message)
    pub batch_size: usize,
    /// Heartbeat interval for connection keepalive
    pub heartbeat_interval: Duration,
    /// Maximum offline queue size per client
    pub max_offline_queue: usize,
    /// Timeout for idle connections
    pub idle_timeout: Duration,
    /// Enable compression for delta transfer
    pub compression_enabled: bool,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            max_clients: 10_000,
            batch_size: 100,
            heartbeat_interval: Duration::from_secs(30),
            max_offline_queue: 10_000,
            idle_timeout: Duration::from_secs(300),
            compression_enabled: true,
        }
    }
}

/// The sync gateway that manages client connections and delta routing
pub struct SyncGateway {
    config: SyncConfig,
    /// Active client sessions
    sessions: RwLock<HashMap<String, SyncSession>>,
    /// Global vector clock (merged from all clients)
    global_clock: RwLock<VectorClock>,
    /// Delta log (ordered history)
    delta_log: RwLock<Vec<SyncDelta>>,
    /// Statistics
    stats: SyncStats,
}

/// Sync gateway statistics
pub struct SyncStats {
    pub total_deltas_processed: AtomicU64,
    pub total_deltas_sent: AtomicU64,
    pub total_connections: AtomicU64,
    pub active_connections: AtomicU64,
    pub reconnections: AtomicU64,
    pub conflicts_resolved: AtomicU64,
}

impl Default for SyncStats {
    fn default() -> Self {
        Self {
            total_deltas_processed: AtomicU64::new(0),
            total_deltas_sent: AtomicU64::new(0),
            total_connections: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            reconnections: AtomicU64::new(0),
            conflicts_resolved: AtomicU64::new(0),
        }
    }
}

impl SyncGateway {
    /// Create a new sync gateway
    pub fn new(config: SyncConfig) -> Self {
        Self {
            config,
            sessions: RwLock::new(HashMap::new()),
            global_clock: RwLock::new(VectorClock::new()),
            delta_log: RwLock::new(Vec::new()),
            stats: SyncStats::default(),
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(SyncConfig::default())
    }

    /// Register a new client connection
    pub fn connect(&self, client_id: &str) -> Result<(), SyncError> {
        let mut sessions = self.sessions.write();
        if sessions.len() >= self.config.max_clients {
            return Err(SyncError::MaxClientsReached);
        }

        let session = SyncSession {
            client_id: client_id.to_string(),
            state: ClientState::Connected,
            clock: VectorClock::new(),
            subscriptions: Vec::new(),
            pending_deltas: Vec::new(),
            deltas_sent: 0,
            deltas_received: 0,
            last_activity: now_epoch_ms(),
        };

        sessions.insert(client_id.to_string(), session);
        self.stats.total_connections.fetch_add(1, Ordering::Relaxed);
        self.stats
            .active_connections
            .fetch_add(1, Ordering::Relaxed);

        debug!(client_id = client_id, "Client connected to sync gateway");
        Ok(())
    }

    /// Disconnect a client
    pub fn disconnect(&self, client_id: &str) {
        let mut sessions = self.sessions.write();
        if let Some(session) = sessions.get_mut(client_id) {
            session.state = ClientState::Closed;
            self.stats
                .active_connections
                .fetch_sub(1, Ordering::Relaxed);
        }
        sessions.remove(client_id);
    }

    /// Handle a reconnection (merge offline deltas)
    pub fn reconnect(
        &self,
        client_id: &str,
        client_clock: VectorClock,
        offline_deltas: Vec<SyncDelta>,
    ) -> Result<Vec<SyncDelta>, SyncError> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(client_id)
            .ok_or(SyncError::SessionNotFound)?;

        session.state = ClientState::Reconnecting;
        session.clock = client_clock.clone();

        // Process offline deltas
        drop(sessions);
        for delta in &offline_deltas {
            self.apply_delta(delta.clone())?;
        }

        // Find deltas the client missed while offline
        let missed = self.get_deltas_since(&client_clock, client_id);

        let mut sessions = self.sessions.write();
        if let Some(session) = sessions.get_mut(client_id) {
            session.state = ClientState::Connected;
            session.last_activity = now_epoch_ms();
        }

        self.stats.reconnections.fetch_add(1, Ordering::Relaxed);
        Ok(missed)
    }

    /// Apply a delta from a client
    pub fn apply_delta(&self, delta: SyncDelta) -> Result<(), SyncError> {
        // Update global clock
        self.global_clock.write().merge(&delta.clock);

        // Route to subscribers
        let sessions = self.sessions.read();
        let mut notifications = Vec::new();

        for (cid, session) in sessions.iter() {
            if cid == &delta.client_id {
                continue; // Don't echo back to sender
            }
            for sub in &session.subscriptions {
                if Self::matches_pattern(&delta.key, &sub.patterns) {
                    notifications.push(cid.clone());
                    break;
                }
            }
        }
        drop(sessions);

        // Queue delta for matching subscribers
        let mut sessions = self.sessions.write();
        for cid in &notifications {
            if let Some(session) = sessions.get_mut(cid) {
                if session.pending_deltas.len() < self.config.max_offline_queue {
                    session.pending_deltas.push(delta.clone());
                }
            }
        }

        // Update sender stats
        if let Some(session) = sessions.get_mut(&delta.client_id) {
            session.deltas_received += 1;
            session.clock.merge(&delta.clock);
            session.last_activity = now_epoch_ms();
        }

        // Add to delta log
        self.delta_log.write().push(delta);
        self.stats
            .total_deltas_processed
            .fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Add a subscription for a client
    pub fn subscribe(
        &self,
        client_id: &str,
        patterns: Vec<String>,
        include_value: bool,
    ) -> Result<String, SyncError> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(client_id)
            .ok_or(SyncError::SessionNotFound)?;

        let sub_id = format!("sub-{}", uuid::Uuid::new_v4());
        session.subscriptions.push(Subscription {
            id: sub_id.clone(),
            client_id: client_id.to_string(),
            patterns,
            include_value,
            created_at: now_epoch_ms(),
        });

        Ok(sub_id)
    }

    /// Remove a subscription
    pub fn unsubscribe(&self, client_id: &str, subscription_id: &str) -> Result<(), SyncError> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(client_id)
            .ok_or(SyncError::SessionNotFound)?;

        session.subscriptions.retain(|s| s.id != subscription_id);
        Ok(())
    }

    /// Drain pending deltas for a client (returns batch)
    pub fn drain_pending(&self, client_id: &str) -> Vec<SyncDelta> {
        let mut sessions = self.sessions.write();
        if let Some(session) = sessions.get_mut(client_id) {
            let batch_size = self.config.batch_size.min(session.pending_deltas.len());
            let drained: Vec<SyncDelta> = session.pending_deltas.drain(..batch_size).collect();
            session.deltas_sent += drained.len() as u64;
            self.stats
                .total_deltas_sent
                .fetch_add(drained.len() as u64, Ordering::Relaxed);
            drained
        } else {
            Vec::new()
        }
    }

    /// Get deltas that happened after the given clock (for a specific client)
    fn get_deltas_since(&self, since: &VectorClock, exclude_client: &str) -> Vec<SyncDelta> {
        self.delta_log
            .read()
            .iter()
            .filter(|d| d.client_id != exclude_client && since.happened_before(&d.clock))
            .cloned()
            .collect()
    }

    /// Get session info for a client
    pub fn session_info(&self, client_id: &str) -> Option<SyncSession> {
        self.sessions.read().get(client_id).cloned()
    }

    /// Get the number of active connections
    pub fn active_connections(&self) -> usize {
        self.sessions
            .read()
            .values()
            .filter(|s| s.state == ClientState::Connected)
            .count()
    }

    /// Get a stats snapshot
    pub fn stats_snapshot(&self) -> SyncStatsSnapshot {
        SyncStatsSnapshot {
            total_deltas_processed: self.stats.total_deltas_processed.load(Ordering::Relaxed),
            total_deltas_sent: self.stats.total_deltas_sent.load(Ordering::Relaxed),
            total_connections: self.stats.total_connections.load(Ordering::Relaxed),
            active_connections: self.active_connections(),
            reconnections: self.stats.reconnections.load(Ordering::Relaxed),
            conflicts_resolved: self.stats.conflicts_resolved.load(Ordering::Relaxed),
            delta_log_size: self.delta_log.read().len(),
        }
    }

    fn matches_pattern(key: &str, patterns: &[String]) -> bool {
        patterns.iter().any(|p| {
            if p.ends_with('*') {
                key.starts_with(&p[..p.len() - 1])
            } else {
                key == p
            }
        })
    }
}

/// Serializable stats snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncStatsSnapshot {
    pub total_deltas_processed: u64,
    pub total_deltas_sent: u64,
    pub total_connections: u64,
    pub active_connections: usize,
    pub reconnections: u64,
    pub conflicts_resolved: u64,
    pub delta_log_size: usize,
}

/// Errors from sync operations
#[derive(Debug, Clone, thiserror::Error)]
pub enum SyncError {
    #[error("Maximum number of sync clients reached")]
    MaxClientsReached,

    #[error("Session not found")]
    SessionNotFound,

    #[error("Client already connected")]
    AlreadyConnected,

    #[error("Sync protocol error: {0}")]
    ProtocolError(String),
}

fn now_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_clock_basic() {
        let mut vc = VectorClock::new();
        vc.increment("node1");
        vc.increment("node1");
        vc.increment("node2");

        assert_eq!(vc.get("node1"), 2);
        assert_eq!(vc.get("node2"), 1);
        assert_eq!(vc.get("node3"), 0);
    }

    #[test]
    fn test_vector_clock_merge() {
        let mut vc1 = VectorClock::new();
        vc1.increment("a");
        vc1.increment("a");
        vc1.increment("b");

        let mut vc2 = VectorClock::new();
        vc2.increment("a");
        vc2.increment("c");
        vc2.increment("c");

        vc1.merge(&vc2);
        assert_eq!(vc1.get("a"), 2); // max(2,1)
        assert_eq!(vc1.get("b"), 1); // only in vc1
        assert_eq!(vc1.get("c"), 2); // only in vc2
    }

    #[test]
    fn test_vector_clock_happened_before() {
        let mut vc1 = VectorClock::new();
        vc1.increment("a");

        let mut vc2 = VectorClock::new();
        vc2.increment("a");
        vc2.increment("a");

        assert!(vc1.happened_before(&vc2));
        assert!(!vc2.happened_before(&vc1));
    }

    #[test]
    fn test_vector_clock_concurrent() {
        let mut vc1 = VectorClock::new();
        vc1.increment("a");

        let mut vc2 = VectorClock::new();
        vc2.increment("b");

        assert!(vc1.is_concurrent(&vc2));
        assert!(vc2.is_concurrent(&vc1));
    }

    #[test]
    fn test_vector_clock_equal() {
        let mut vc1 = VectorClock::new();
        vc1.increment("a");

        let mut vc2 = VectorClock::new();
        vc2.increment("a");

        assert!(!vc1.is_concurrent(&vc2));
        assert!(!vc1.happened_before(&vc2));
        assert_eq!(vc1, vc2);
    }

    #[test]
    fn test_connect_disconnect() {
        let gw = SyncGateway::with_defaults();

        gw.connect("client1").unwrap();
        assert_eq!(gw.active_connections(), 1);

        gw.disconnect("client1");
        assert_eq!(gw.active_connections(), 0);
    }

    #[test]
    fn test_max_clients() {
        let config = SyncConfig {
            max_clients: 2,
            ..Default::default()
        };
        let gw = SyncGateway::new(config);

        gw.connect("c1").unwrap();
        gw.connect("c2").unwrap();
        let result = gw.connect("c3");
        assert!(matches!(result, Err(SyncError::MaxClientsReached)));
    }

    #[test]
    fn test_subscribe_and_delta_routing() {
        let gw = SyncGateway::with_defaults();
        gw.connect("sender").unwrap();
        gw.connect("receiver").unwrap();

        // Receiver subscribes to user:* pattern
        gw.subscribe("receiver", vec!["user:*".to_string()], true)
            .unwrap();

        // Sender produces a delta
        let mut clock = VectorClock::new();
        clock.increment("sender");
        let delta = SyncDelta {
            id: "d1".to_string(),
            client_id: "sender".to_string(),
            key: "user:123".to_string(),
            operation: SyncOperation::Set {
                value: "Alice".to_string(),
            },
            clock,
            timestamp: now_epoch_ms(),
            from_offline: false,
        };

        gw.apply_delta(delta).unwrap();

        // Receiver should have pending delta
        let pending = gw.drain_pending("receiver");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].key, "user:123");
    }

    #[test]
    fn test_no_echo_back() {
        let gw = SyncGateway::with_defaults();
        gw.connect("sender").unwrap();
        gw.subscribe("sender", vec!["*".to_string()], true).unwrap();

        let mut clock = VectorClock::new();
        clock.increment("sender");
        let delta = SyncDelta {
            id: "d1".to_string(),
            client_id: "sender".to_string(),
            key: "test".to_string(),
            operation: SyncOperation::Set {
                value: "v".to_string(),
            },
            clock,
            timestamp: now_epoch_ms(),
            from_offline: false,
        };

        gw.apply_delta(delta).unwrap();

        // Sender should NOT receive their own delta
        let pending = gw.drain_pending("sender");
        assert!(pending.is_empty());
    }

    #[test]
    fn test_unsubscribe() {
        let gw = SyncGateway::with_defaults();
        gw.connect("c1").unwrap();
        let sub_id = gw
            .subscribe("c1", vec!["key:*".to_string()], false)
            .unwrap();

        let session = gw.session_info("c1").unwrap();
        assert_eq!(session.subscriptions.len(), 1);

        gw.unsubscribe("c1", &sub_id).unwrap();

        let session = gw.session_info("c1").unwrap();
        assert_eq!(session.subscriptions.len(), 0);
    }

    #[test]
    fn test_reconnect_with_offline_deltas() {
        let gw = SyncGateway::with_defaults();
        gw.connect("c1").unwrap();
        gw.connect("c2").unwrap();
        gw.subscribe("c1", vec!["*".to_string()], true).unwrap();

        // c2 produces a delta while c1 is "connected"
        let mut clock = VectorClock::new();
        clock.increment("c2");
        gw.apply_delta(SyncDelta {
            id: "d1".to_string(),
            client_id: "c2".to_string(),
            key: "key1".to_string(),
            operation: SyncOperation::Set {
                value: "v1".to_string(),
            },
            clock: clock.clone(),
            timestamp: now_epoch_ms(),
            from_offline: false,
        })
        .unwrap();

        // Simulate c1 disconnect and reconnect with offline delta
        let c1_clock = VectorClock::new(); // c1 knows nothing

        let mut offline_clock = VectorClock::new();
        offline_clock.increment("c1");
        let offline = vec![SyncDelta {
            id: "d2".to_string(),
            client_id: "c1".to_string(),
            key: "key2".to_string(),
            operation: SyncOperation::Set {
                value: "v2".to_string(),
            },
            clock: offline_clock,
            timestamp: now_epoch_ms(),
            from_offline: true,
        }];

        let missed = gw.reconnect("c1", c1_clock, offline).unwrap();
        // c1 should get the delta from c2
        assert!(missed.iter().any(|d| d.key == "key1"));
    }

    #[test]
    fn test_pattern_matching() {
        assert!(SyncGateway::matches_pattern(
            "user:123",
            &["user:*".to_string()]
        ));
        assert!(!SyncGateway::matches_pattern(
            "order:123",
            &["user:*".to_string()]
        ));
        assert!(SyncGateway::matches_pattern(
            "exact",
            &["exact".to_string()]
        ));
    }

    #[test]
    fn test_stats() {
        let gw = SyncGateway::with_defaults();
        gw.connect("c1").unwrap();

        let stats = gw.stats_snapshot();
        assert_eq!(stats.total_connections, 1);
        assert_eq!(stats.active_connections, 1);
    }

    #[test]
    fn test_client_state_display() {
        assert_eq!(ClientState::Connected.to_string(), "Connected");
        assert_eq!(ClientState::Disconnected.to_string(), "Disconnected");
    }
}
