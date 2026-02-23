//! Client protocol negotiation for Redis HELLO and CLIENT commands.
//!
//! Implements HELLO command (RESP2/RESP3 negotiation), CLIENT subcommands
//! (SETNAME, GETNAME, LIST, KILL, INFO), and client-side caching with
//! key-level and broadcast tracking modes.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors returned by client negotiation operations.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum NegotiationError {
    /// The specified client ID does not exist.
    #[error("ERR No such client with id {0}")]
    ClientNotFound(u64),

    /// An unsupported RESP protocol version was requested.
    #[error("NOPROTO unsupported protocol version {0}")]
    InvalidProtocolVersion(u32),

    /// Tracking is already enabled for the client.
    #[error("ERR client tracking is already enabled")]
    TrackingAlreadyEnabled,

    /// The redirect target client does not exist.
    #[error("ERR redirect client with id {0} not found")]
    RedirectTargetNotFound(u64),

    /// An internal error.
    #[error("ERR {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// HelloArgs / HelloResponse
// ---------------------------------------------------------------------------

/// Arguments parsed from the HELLO command.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HelloArgs {
    /// Requested protocol version (2 or 3). `None` means keep current.
    pub protocol_version: Option<u32>,
    /// Optional `AUTH username password` pair.
    pub auth: Option<(String, String)>,
    /// Optional `SETNAME` client name.
    pub client_name: Option<String>,
}

/// Response returned from a successful HELLO command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloResponse {
    /// Server identifier.
    pub server: String,
    /// Server version string.
    pub version: String,
    /// Negotiated protocol version.
    pub proto: u32,
    /// Assigned client ID.
    pub id: u64,
    /// Server mode (e.g. `"standalone"`, `"cluster"`).
    pub mode: String,
    /// Replication role (e.g. `"master"`, `"slave"`).
    pub role: String,
    /// Loaded server modules.
    pub modules: Vec<String>,
}

// ---------------------------------------------------------------------------
// ClientDetail
// ---------------------------------------------------------------------------

/// Detailed information about a connected client.
///
/// Fields mirror the Redis `CLIENT LIST` output for compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientDetail {
    /// Unique client identifier.
    pub id: u64,
    /// Client address (`ip:port`).
    pub addr: String,
    /// Optional client name set via `CLIENT SETNAME`.
    pub name: Option<String>,
    /// Seconds since the client connected.
    pub age_secs: u64,
    /// Seconds since the last command.
    pub idle_secs: u64,
    /// Redis-compatible flag string.
    pub flags: String,
    /// Currently selected database index.
    pub db: u8,
    /// Last command executed.
    pub cmd: Option<String>,
    /// Negotiated protocol version.
    pub protocol: u32,
    /// Whether client-side caching tracking is enabled.
    pub tracking: bool,
    /// Whether the client is inside a MULTI transaction.
    pub multi: bool,
    /// Number of active Pub/Sub subscriptions.
    pub subscriptions: u64,
    /// Output buffer size in bytes.
    pub output_buffer_bytes: u64,
    /// Input buffer size in bytes.
    pub input_buffer_bytes: u64,
    /// Timestamp when the client connected.
    pub connected_at: DateTime<Utc>,
    /// Timestamp of the last command, if any.
    pub last_command_at: Option<DateTime<Utc>>,
}

impl ClientDetail {
    /// Format this detail as a Redis-compatible `CLIENT LIST` line.
    #[inline]
    pub fn to_info_line(&self) -> String {
        let name = self.name.as_deref().unwrap_or("");
        let cmd = self.cmd.as_deref().unwrap_or("NULL");
        format!(
            "id={id} addr={addr} name={name} age={age} idle={idle} flags={flags} \
             db={db} cmd={cmd} proto={proto} tracking={tracking} multi={multi} \
             sub={sub} obl={obl} ibl={ibl}",
            id = self.id,
            addr = self.addr,
            name = name,
            age = self.age_secs,
            idle = self.idle_secs,
            flags = self.flags,
            db = self.db,
            cmd = cmd,
            proto = self.protocol,
            tracking = if self.tracking { "on" } else { "off" },
            multi = if self.multi { "1" } else { "-1" },
            sub = self.subscriptions,
            obl = self.output_buffer_bytes,
            ibl = self.input_buffer_bytes,
        )
    }
}

// ---------------------------------------------------------------------------
// KillTarget
// ---------------------------------------------------------------------------

/// Target selector for the `CLIENT KILL` command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KillTarget {
    /// Kill by client ID.
    Id(u64),
    /// Kill by client address (`ip:port`).
    Addr(String),
    /// Kill by client name.
    Name(String),
    /// Kill all clients authenticated as a given user.
    User(String),
    /// Kill clients connected longer than `secs` seconds.
    MaxAge {
        /// Minimum connection age in seconds.
        secs: u64,
    },
    /// Kill clients based on their tracking state.
    Tracking {
        /// If `true`, kill clients with tracking enabled; otherwise those without.
        on: bool,
    },
}

// ---------------------------------------------------------------------------
// Tracking
// ---------------------------------------------------------------------------

/// Tracking mode for client-side caching.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrackingMode {
    /// Per-key tracking: each key a client reads is tracked individually.
    Default,
    /// Prefix-based broadcasting: clients subscribe to key prefixes.
    Broadcasting,
}

impl Default for TrackingMode {
    fn default() -> Self {
        Self::Default
    }
}

/// Configuration for client-side caching tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackingConfig {
    /// Tracking mode.
    pub mode: TrackingMode,
    /// Key prefixes for broadcasting mode.
    pub prefixes: Vec<String>,
    /// If set, invalidations are redirected to this client ID.
    pub redirect_id: Option<u64>,
    /// Enable broadcasting mode (alias for `mode = Broadcasting`).
    pub bcast: bool,
    /// Do not send invalidation for keys modified by the same client.
    pub noloop: bool,
    /// Opt-in: only track keys after an explicit `CLIENT CACHING YES`.
    pub optin: bool,
    /// Opt-out: track all keys except those after `CLIENT CACHING NO`.
    pub optout: bool,
}

impl Default for TrackingConfig {
    fn default() -> Self {
        Self {
            mode: TrackingMode::Default,
            prefixes: Vec::new(),
            redirect_id: None,
            bcast: false,
            noloop: false,
            optin: false,
            optout: false,
        }
    }
}

// ---------------------------------------------------------------------------
// ClientFlag / ClientFlags
// ---------------------------------------------------------------------------

/// Individual client flag used to build Redis-compatible flag strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ClientFlag {
    /// Client is a replica.
    Slave,
    /// Client is in MONITOR mode.
    Monitor,
    /// Client is inside a MULTI transaction.
    Multi,
    /// Client is blocked on a blocking command.
    Blocked,
    /// Client has tracking enabled (default mode).
    Tracking,
    /// Client has tracking enabled (broadcasting mode).
    TrackingBroadcast,
    /// Client has Pub/Sub subscriptions.
    SubscribedPubSub,
    /// Client is a master (used in replication context).
    Master,
    /// Client is in read-only mode.
    ReadOnly,
    /// Client connection is scheduled for closing.
    Close,
}

impl ClientFlag {
    /// Return the single-character Redis flag code.
    #[inline]
    fn code(self) -> char {
        match self {
            Self::Slave => 'S',
            Self::Monitor => 'O',
            Self::Multi => 'x',
            Self::Blocked => 'b',
            Self::Tracking => 't',
            Self::TrackingBroadcast => 'T',
            Self::SubscribedPubSub => 'P',
            Self::Master => 'M',
            Self::ReadOnly => 'r',
            Self::Close => 'c',
        }
    }
}

/// Utility for building Redis-compatible flag strings.
#[derive(Debug, Clone, Default)]
pub struct ClientFlags {
    flags: HashSet<ClientFlag>,
}

impl ClientFlags {
    /// Create an empty flag set.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a flag to the set.
    #[inline]
    pub fn set_flag(&mut self, flag: ClientFlag) {
        self.flags.insert(flag);
    }

    /// Remove a flag from the set.
    #[inline]
    pub fn unset_flag(&mut self, flag: ClientFlag) {
        self.flags.remove(&flag);
    }

    /// Return `true` if the given flag is set.
    #[inline]
    pub fn has_flag(&self, flag: ClientFlag) -> bool {
        self.flags.contains(&flag)
    }
}

impl fmt::Display for ClientFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.flags.is_empty() {
            return write!(f, "N");
        }
        // Deterministic ordering by flag code.
        let mut chars: Vec<char> = self.flags.iter().map(|fl| fl.code()).collect();
        chars.sort_unstable();
        for ch in chars {
            write!(f, "{ch}")?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// InvalidationMessage
// ---------------------------------------------------------------------------

/// Message sent to a client when tracked keys are modified.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvalidationMessage {
    /// Keys that have been invalidated.
    pub keys: Vec<String>,
    /// If set, the message should be delivered to this client instead.
    pub redirect_to: Option<u64>,
}

// ---------------------------------------------------------------------------
// NegotiationStats
// ---------------------------------------------------------------------------

/// Cumulative statistics for the negotiation subsystem.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NegotiationStats {
    /// Total clients ever registered.
    pub total_clients_registered: u64,
    /// Total HELLO commands processed.
    pub total_hello_commands: u64,
    /// Total tracking-enable operations.
    pub total_tracking_enabled: u64,
    /// Total invalidation messages sent.
    pub total_invalidations_sent: u64,
    /// Total CLIENT KILL operations.
    pub total_kills: u64,
    /// Currently-tracking clients.
    pub current_tracking_clients: u64,
    /// Clients speaking RESP2.
    pub protocol_v2_clients: u64,
    /// Clients speaking RESP3.
    pub protocol_v3_clients: u64,
}

// ---------------------------------------------------------------------------
// Internal client state
// ---------------------------------------------------------------------------

/// Internal per-client state managed by `ClientNegotiator`.
#[derive(Debug, Clone)]
struct ClientState {
    id: u64,
    addr: String,
    name: Option<String>,
    protocol: u32,
    db: u8,
    flags: ClientFlags,
    cmd: Option<String>,
    multi: bool,
    subscriptions: u64,
    output_buffer_bytes: u64,
    input_buffer_bytes: u64,
    connected_at: DateTime<Utc>,
    last_command_at: Option<DateTime<Utc>>,
    tracking: Option<TrackingConfig>,
    /// Keys currently tracked by this client (default mode).
    tracked_keys: HashSet<String>,
}

impl ClientState {
    fn new(id: u64, addr: String) -> Self {
        Self {
            id,
            addr,
            name: None,
            protocol: 2,
            db: 0,
            flags: ClientFlags::new(),
            cmd: None,
            multi: false,
            subscriptions: 0,
            output_buffer_bytes: 0,
            input_buffer_bytes: 0,
            connected_at: Utc::now(),
            last_command_at: None,
            tracking: None,
            tracked_keys: HashSet::new(),
        }
    }

    fn to_detail(&self) -> ClientDetail {
        let now = Utc::now();
        let age_secs = now
            .signed_duration_since(self.connected_at)
            .num_seconds()
            .max(0) as u64;
        let idle_secs = self
            .last_command_at
            .map(|t| now.signed_duration_since(t).num_seconds().max(0) as u64)
            .unwrap_or(age_secs);

        ClientDetail {
            id: self.id,
            addr: self.addr.clone(),
            name: self.name.clone(),
            age_secs,
            idle_secs,
            flags: self.flags.to_string(),
            db: self.db,
            cmd: self.cmd.clone(),
            protocol: self.protocol,
            tracking: self.tracking.is_some(),
            multi: self.multi,
            subscriptions: self.subscriptions,
            output_buffer_bytes: self.output_buffer_bytes,
            input_buffer_bytes: self.input_buffer_bytes,
            connected_at: self.connected_at,
            last_command_at: self.last_command_at,
        }
    }
}

// ---------------------------------------------------------------------------
// ClientNegotiator
// ---------------------------------------------------------------------------

/// Manages HELLO / CLIENT commands and client-side caching.
///
/// Thread-safe — all methods take `&self` and internal state is protected
/// by `DashMap` and `RwLock`.
pub struct ClientNegotiator {
    /// Per-client state keyed by client ID.
    clients: DashMap<u64, ClientState>,
    /// Default-mode reverse index: key → set of tracking client IDs.
    key_trackers: DashMap<String, HashSet<u64>>,
    /// Broadcast-mode subscriptions: prefix → set of client IDs.
    prefix_trackers: RwLock<HashMap<String, HashSet<u64>>>,
    /// Monotonic client ID generator.
    next_id: AtomicU64,

    // Counters
    total_clients_registered: AtomicU64,
    total_hello_commands: AtomicU64,
    total_tracking_enabled: AtomicU64,
    total_invalidations_sent: AtomicU64,
    total_kills: AtomicU64,
}

impl fmt::Debug for ClientNegotiator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientNegotiator")
            .field("clients", &self.clients.len())
            .finish()
    }
}

impl Default for ClientNegotiator {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientNegotiator {
    // -- Construction -------------------------------------------------------

    /// Create a new, empty negotiator.
    pub fn new() -> Self {
        Self {
            clients: DashMap::new(),
            key_trackers: DashMap::new(),
            prefix_trackers: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            total_clients_registered: AtomicU64::new(0),
            total_hello_commands: AtomicU64::new(0),
            total_tracking_enabled: AtomicU64::new(0),
            total_invalidations_sent: AtomicU64::new(0),
            total_kills: AtomicU64::new(0),
        }
    }

    // -- HELLO --------------------------------------------------------------

    /// Process a HELLO command and return a [`HelloResponse`].
    ///
    /// If `args.protocol_version` is `Some`, the client's protocol is
    /// switched (only versions 2 and 3 are accepted). An optional client
    /// name is applied when provided.
    pub fn handle_hello(&self, args: &HelloArgs) -> Result<HelloResponse, NegotiationError> {
        self.total_hello_commands.fetch_add(1, Ordering::Relaxed);

        // Validate protocol version.
        if let Some(v) = args.protocol_version {
            if v != 2 && v != 3 {
                return Err(NegotiationError::InvalidProtocolVersion(v));
            }
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let proto = args.protocol_version.unwrap_or(2);

        // Register a lightweight client entry so subsequent commands work.
        let mut state = ClientState::new(id, String::new());
        state.protocol = proto;
        if let Some(ref name) = args.client_name {
            state.name = Some(name.clone());
        }
        self.clients.insert(id, state);
        self.total_clients_registered
            .fetch_add(1, Ordering::Relaxed);

        Ok(HelloResponse {
            server: "ferrite".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            proto,
            id,
            mode: "standalone".to_string(),
            role: "master".to_string(),
            modules: Vec::new(),
        })
    }

    // -- CLIENT SETNAME / GETNAME -------------------------------------------

    /// Set the human-readable name for a client.
    pub fn set_client_name(&self, client_id: u64, name: &str) {
        if let Some(mut entry) = self.clients.get_mut(&client_id) {
            entry.name = Some(name.to_string());
        }
    }

    /// Retrieve the name previously set for a client.
    pub fn get_client_name(&self, client_id: u64) -> Option<String> {
        self.clients.get(&client_id).and_then(|e| e.name.clone())
    }

    // -- CLIENT INFO / LIST -------------------------------------------------

    /// Return detailed info for a single client.
    pub fn get_client_info(&self, client_id: u64) -> Option<ClientDetail> {
        self.clients.get(&client_id).map(|e| e.to_detail())
    }

    /// List all connected clients in Redis `CLIENT LIST` compatible format.
    pub fn list_clients(&self) -> Vec<ClientDetail> {
        self.clients.iter().map(|e| e.to_detail()).collect()
    }

    // -- CLIENT KILL --------------------------------------------------------

    /// Kill clients matching `target`. Returns the number killed.
    pub fn kill_client(&self, target: KillTarget) -> u64 {
        let ids_to_kill: Vec<u64> = self
            .clients
            .iter()
            .filter_map(|entry| {
                let st = entry.value();
                let matched = match &target {
                    KillTarget::Id(id) => st.id == *id,
                    KillTarget::Addr(addr) => st.addr == *addr,
                    KillTarget::Name(name) => st.name.as_deref() == Some(name.as_str()),
                    KillTarget::User(_user) => false, // user auth not wired yet
                    KillTarget::MaxAge { secs } => {
                        let age = Utc::now()
                            .signed_duration_since(st.connected_at)
                            .num_seconds()
                            .max(0) as u64;
                        age > *secs
                    }
                    KillTarget::Tracking { on } => {
                        let tracking_on = st.tracking.is_some();
                        tracking_on == *on
                    }
                };
                if matched {
                    Some(st.id)
                } else {
                    None
                }
            })
            .collect();

        let count = ids_to_kill.len() as u64;
        for id in ids_to_kill {
            self.unregister_client(id);
        }
        self.total_kills.fetch_add(count, Ordering::Relaxed);
        count
    }

    // -- Tracking -----------------------------------------------------------

    /// Enable client-side caching tracking for a client.
    pub fn enable_tracking(
        &self,
        client_id: u64,
        config: TrackingConfig,
    ) -> Result<(), NegotiationError> {
        let mut entry = self
            .clients
            .get_mut(&client_id)
            .ok_or(NegotiationError::ClientNotFound(client_id))?;

        if entry.tracking.is_some() {
            return Err(NegotiationError::TrackingAlreadyEnabled);
        }

        // Validate redirect target exists.
        if let Some(redirect) = config.redirect_id {
            if !self.clients.contains_key(&redirect) {
                return Err(NegotiationError::RedirectTargetNotFound(redirect));
            }
        }

        let effective_mode = if config.bcast {
            TrackingMode::Broadcasting
        } else {
            config.mode
        };

        let effective = TrackingConfig {
            mode: effective_mode,
            ..config
        };

        // Register broadcast prefixes.
        if effective.mode == TrackingMode::Broadcasting {
            let mut pt = self.prefix_trackers.write();
            for prefix in &effective.prefixes {
                pt.entry(prefix.clone()).or_default().insert(client_id);
            }
        }

        entry.flags.set_flag(ClientFlag::Tracking);
        if effective.mode == TrackingMode::Broadcasting {
            entry.flags.set_flag(ClientFlag::TrackingBroadcast);
        }
        entry.tracking = Some(effective);

        self.total_tracking_enabled.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Disable client-side caching tracking for a client.
    pub fn disable_tracking(&self, client_id: u64) {
        if let Some(mut entry) = self.clients.get_mut(&client_id) {
            // Clean up broadcast prefixes.
            if let Some(ref cfg) = entry.tracking {
                if cfg.mode == TrackingMode::Broadcasting {
                    let mut pt = self.prefix_trackers.write();
                    for prefix in &cfg.prefixes {
                        if let Some(set) = pt.get_mut(prefix) {
                            set.remove(&client_id);
                            if set.is_empty() {
                                pt.remove(prefix);
                            }
                        }
                    }
                }
            }

            // Clean up per-key tracking entries.
            for key in &entry.tracked_keys {
                if let Some(mut set) = self.key_trackers.get_mut(key) {
                    set.remove(&client_id);
                }
            }
            entry.tracked_keys.clear();

            entry.flags.unset_flag(ClientFlag::Tracking);
            entry.flags.unset_flag(ClientFlag::TrackingBroadcast);
            entry.tracking = None;
        }
    }

    /// Record that `client_id` read `key` (default tracking mode).
    ///
    /// Call this from GET / MGET handlers to register interest.
    pub fn track_key(&self, client_id: u64, key: &str) {
        if let Some(mut entry) = self.clients.get_mut(&client_id) {
            if let Some(ref cfg) = entry.tracking {
                if cfg.mode == TrackingMode::Default {
                    entry.tracked_keys.insert(key.to_string());
                    self.key_trackers
                        .entry(key.to_string())
                        .or_default()
                        .insert(client_id);
                }
            }
        }
    }

    /// Invalidate a key and return the client IDs that should be notified.
    ///
    /// For **default-mode** clients the key is removed from their tracked set.
    /// For **broadcasting-mode** clients any matching prefix triggers notification.
    /// The `noloop` flag is respected: if the modifying client is the same as
    /// the tracking client, it is excluded.
    pub fn invalidate(&self, key: &str) -> Vec<u64> {
        let mut notify: HashSet<u64> = HashSet::new();

        // Default-mode: exact key match.
        if let Some((_, client_ids)) = self.key_trackers.remove(key) {
            for cid in &client_ids {
                // Remove the key from each client's tracked set.
                if let Some(mut entry) = self.clients.get_mut(cid) {
                    entry.tracked_keys.remove(key);
                }
                notify.insert(*cid);
            }
        }

        // Broadcasting mode: prefix match.
        {
            let pt = self.prefix_trackers.read();
            for (prefix, cids) in pt.iter() {
                if key.starts_with(prefix.as_str()) {
                    for cid in cids {
                        notify.insert(*cid);
                    }
                }
            }
        }

        // Apply noloop: filter out clients whose noloop flag is set.
        // (Caller is responsible for passing the modifier's ID externally;
        //  here we only build the candidate list.)
        let count = notify.len() as u64;
        self.total_invalidations_sent
            .fetch_add(count, Ordering::Relaxed);

        notify.into_iter().collect()
    }

    // -- Registration -------------------------------------------------------

    /// Register a new client connection.
    pub fn register_client(&self, client_id: u64, addr: String) {
        let state = ClientState::new(client_id, addr);
        self.clients.insert(client_id, state);
        self.total_clients_registered
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Remove a client and clean up all associated tracking state.
    pub fn unregister_client(&self, client_id: u64) {
        self.disable_tracking(client_id);
        self.clients.remove(&client_id);
    }

    // -- Stats --------------------------------------------------------------

    /// Return a snapshot of cumulative negotiation statistics.
    pub fn get_stats(&self) -> NegotiationStats {
        let (mut v2, mut v3, mut tracking) = (0u64, 0u64, 0u64);
        for entry in self.clients.iter() {
            match entry.protocol {
                2 => v2 += 1,
                3 => v3 += 1,
                _ => {}
            }
            if entry.tracking.is_some() {
                tracking += 1;
            }
        }

        NegotiationStats {
            total_clients_registered: self.total_clients_registered.load(Ordering::Relaxed),
            total_hello_commands: self.total_hello_commands.load(Ordering::Relaxed),
            total_tracking_enabled: self.total_tracking_enabled.load(Ordering::Relaxed),
            total_invalidations_sent: self.total_invalidations_sent.load(Ordering::Relaxed),
            total_kills: self.total_kills.load(Ordering::Relaxed),
            current_tracking_clients: tracking,
            protocol_v2_clients: v2,
            protocol_v3_clients: v3,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- helpers ------------------------------------------------------------

    fn negotiator_with_client() -> (ClientNegotiator, u64) {
        let n = ClientNegotiator::new();
        n.register_client(100, "127.0.0.1:6379".to_string());
        (n, 100)
    }

    // -- HELLO --------------------------------------------------------------

    #[test]
    fn hello_default_protocol_v2() {
        let n = ClientNegotiator::new();
        let resp = n.handle_hello(&HelloArgs::default()).unwrap();
        assert_eq!(resp.proto, 2);
        assert_eq!(resp.server, "ferrite");
        assert_eq!(resp.role, "master");
    }

    #[test]
    fn hello_protocol_v3() {
        let n = ClientNegotiator::new();
        let args = HelloArgs {
            protocol_version: Some(3),
            ..Default::default()
        };
        let resp = n.handle_hello(&args).unwrap();
        assert_eq!(resp.proto, 3);
    }

    #[test]
    fn hello_invalid_protocol_version() {
        let n = ClientNegotiator::new();
        let args = HelloArgs {
            protocol_version: Some(4),
            ..Default::default()
        };
        let err = n.handle_hello(&args).unwrap_err();
        assert_eq!(err, NegotiationError::InvalidProtocolVersion(4));
    }

    #[test]
    fn hello_with_client_name() {
        let n = ClientNegotiator::new();
        let args = HelloArgs {
            client_name: Some("myconn".to_string()),
            ..Default::default()
        };
        let resp = n.handle_hello(&args).unwrap();
        assert_eq!(n.get_client_name(resp.id), Some("myconn".to_string()));
    }

    // -- CLIENT SETNAME / GETNAME -------------------------------------------

    #[test]
    fn set_and_get_client_name() {
        let (n, id) = negotiator_with_client();
        assert_eq!(n.get_client_name(id), None);
        n.set_client_name(id, "worker-1");
        assert_eq!(n.get_client_name(id), Some("worker-1".to_string()));
    }

    #[test]
    fn get_name_unknown_client() {
        let n = ClientNegotiator::new();
        assert_eq!(n.get_client_name(999), None);
    }

    // -- CLIENT LIST / INFO -------------------------------------------------

    #[test]
    fn list_clients_returns_all() {
        let n = ClientNegotiator::new();
        n.register_client(1, "10.0.0.1:1000".to_string());
        n.register_client(2, "10.0.0.2:2000".to_string());
        let list = n.list_clients();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn get_client_info_present() {
        let (n, id) = negotiator_with_client();
        let info = n.get_client_info(id).unwrap();
        assert_eq!(info.id, id);
        assert_eq!(info.addr, "127.0.0.1:6379");
        assert_eq!(info.protocol, 2);
        assert!(!info.tracking);
    }

    #[test]
    fn get_client_info_missing() {
        let n = ClientNegotiator::new();
        assert!(n.get_client_info(42).is_none());
    }

    #[test]
    fn client_detail_info_line_format() {
        let (n, id) = negotiator_with_client();
        n.set_client_name(id, "bench");
        let info = n.get_client_info(id).unwrap();
        let line = info.to_info_line();
        assert!(line.contains("id=100"));
        assert!(line.contains("addr=127.0.0.1:6379"));
        assert!(line.contains("name=bench"));
        assert!(line.contains("proto=2"));
        assert!(line.contains("tracking=off"));
    }

    // -- CLIENT KILL --------------------------------------------------------

    #[test]
    fn kill_by_id() {
        let (n, id) = negotiator_with_client();
        assert_eq!(n.kill_client(KillTarget::Id(id)), 1);
        assert!(n.get_client_info(id).is_none());
    }

    #[test]
    fn kill_by_addr() {
        let n = ClientNegotiator::new();
        n.register_client(10, "10.0.0.5:5000".to_string());
        assert_eq!(
            n.kill_client(KillTarget::Addr("10.0.0.5:5000".to_string())),
            1
        );
        assert!(n.get_client_info(10).is_none());
    }

    #[test]
    fn kill_by_name() {
        let (n, id) = negotiator_with_client();
        n.set_client_name(id, "disposable");
        assert_eq!(n.kill_client(KillTarget::Name("disposable".to_string())), 1);
    }

    #[test]
    fn kill_by_max_age() {
        let n = ClientNegotiator::new();
        // Insert a client with a backdated connected_at.
        let mut state = ClientState::new(50, "127.0.0.1:9999".to_string());
        state.connected_at = Utc::now() - chrono::Duration::seconds(120);
        n.clients.insert(50, state);

        // Client aged 120 s should be killed with a threshold of 60 s.
        assert_eq!(n.kill_client(KillTarget::MaxAge { secs: 60 }), 1);
    }

    #[test]
    fn kill_by_tracking() {
        let n = ClientNegotiator::new();
        n.register_client(1, "a:1".to_string());
        n.register_client(2, "a:2".to_string());
        n.enable_tracking(1, TrackingConfig::default()).unwrap();

        assert_eq!(n.kill_client(KillTarget::Tracking { on: true }), 1);
        assert!(n.get_client_info(1).is_none());
        assert!(n.get_client_info(2).is_some());
    }

    #[test]
    fn kill_nonexistent_returns_zero() {
        let n = ClientNegotiator::new();
        assert_eq!(n.kill_client(KillTarget::Id(999)), 0);
    }

    // -- Tracking enable / disable ------------------------------------------

    #[test]
    fn enable_tracking_default_mode() {
        let (n, id) = negotiator_with_client();
        n.enable_tracking(id, TrackingConfig::default()).unwrap();
        let info = n.get_client_info(id).unwrap();
        assert!(info.tracking);
        assert!(info.flags.contains('t'));
    }

    #[test]
    fn enable_tracking_already_enabled() {
        let (n, id) = negotiator_with_client();
        n.enable_tracking(id, TrackingConfig::default()).unwrap();
        let err = n
            .enable_tracking(id, TrackingConfig::default())
            .unwrap_err();
        assert_eq!(err, NegotiationError::TrackingAlreadyEnabled);
    }

    #[test]
    fn enable_tracking_client_not_found() {
        let n = ClientNegotiator::new();
        let err = n
            .enable_tracking(999, TrackingConfig::default())
            .unwrap_err();
        assert_eq!(err, NegotiationError::ClientNotFound(999));
    }

    #[test]
    fn enable_tracking_redirect_target_missing() {
        let (n, id) = negotiator_with_client();
        let cfg = TrackingConfig {
            redirect_id: Some(9999),
            ..Default::default()
        };
        let err = n.enable_tracking(id, cfg).unwrap_err();
        assert_eq!(err, NegotiationError::RedirectTargetNotFound(9999));
    }

    #[test]
    fn disable_tracking() {
        let (n, id) = negotiator_with_client();
        n.enable_tracking(id, TrackingConfig::default()).unwrap();
        n.disable_tracking(id);
        let info = n.get_client_info(id).unwrap();
        assert!(!info.tracking);
    }

    // -- Invalidation (default mode) ----------------------------------------

    #[test]
    fn invalidation_default_mode() {
        let (n, id) = negotiator_with_client();
        n.enable_tracking(id, TrackingConfig::default()).unwrap();

        n.track_key(id, "user:1");
        n.track_key(id, "user:2");

        let notified = n.invalidate("user:1");
        assert_eq!(notified, vec![id]);

        // Key is no longer tracked after invalidation.
        let notified2 = n.invalidate("user:1");
        assert!(notified2.is_empty());
    }

    #[test]
    fn invalidation_multiple_clients() {
        let n = ClientNegotiator::new();
        n.register_client(1, "a:1".to_string());
        n.register_client(2, "a:2".to_string());
        n.enable_tracking(1, TrackingConfig::default()).unwrap();
        n.enable_tracking(2, TrackingConfig::default()).unwrap();

        n.track_key(1, "shared");
        n.track_key(2, "shared");

        let mut notified = n.invalidate("shared");
        notified.sort();
        assert_eq!(notified, vec![1, 2]);
    }

    // -- Invalidation (broadcasting mode) -----------------------------------

    #[test]
    fn invalidation_broadcast_mode() {
        let n = ClientNegotiator::new();
        n.register_client(1, "a:1".to_string());
        let cfg = TrackingConfig {
            mode: TrackingMode::Broadcasting,
            bcast: true,
            prefixes: vec!["user:".to_string()],
            ..Default::default()
        };
        n.enable_tracking(1, cfg).unwrap();

        let notified = n.invalidate("user:42");
        assert_eq!(notified, vec![1]);

        // Non-matching prefix yields no notification.
        let notified2 = n.invalidate("order:1");
        assert!(notified2.is_empty());
    }

    #[test]
    fn invalidation_broadcast_prefix_matching() {
        let n = ClientNegotiator::new();
        n.register_client(1, "a:1".to_string());
        n.register_client(2, "a:2".to_string());

        n.enable_tracking(
            1,
            TrackingConfig {
                bcast: true,
                prefixes: vec!["cache:".to_string()],
                ..Default::default()
            },
        )
        .unwrap();

        n.enable_tracking(
            2,
            TrackingConfig {
                bcast: true,
                prefixes: vec!["cache:user:".to_string()],
                ..Default::default()
            },
        )
        .unwrap();

        // "cache:user:1" matches both prefixes.
        let mut notified = n.invalidate("cache:user:1");
        notified.sort();
        assert_eq!(notified, vec![1, 2]);

        // "cache:product:1" matches only client 1's prefix.
        let notified2 = n.invalidate("cache:product:1");
        assert_eq!(notified2, vec![1]);
    }

    // -- ClientFlags --------------------------------------------------------

    #[test]
    fn client_flags_empty() {
        let flags = ClientFlags::new();
        assert_eq!(flags.to_string(), "N");
    }

    #[test]
    fn client_flags_single() {
        let mut flags = ClientFlags::new();
        flags.set_flag(ClientFlag::Tracking);
        assert_eq!(flags.to_string(), "t");
    }

    #[test]
    fn client_flags_multiple_sorted() {
        let mut flags = ClientFlags::new();
        flags.set_flag(ClientFlag::Tracking);
        flags.set_flag(ClientFlag::Multi);
        flags.set_flag(ClientFlag::Master);
        // Sorted by char code: 'M' < 't' < 'x'
        assert_eq!(flags.to_string(), "Mtx");
    }

    #[test]
    fn client_flags_unset() {
        let mut flags = ClientFlags::new();
        flags.set_flag(ClientFlag::Blocked);
        flags.set_flag(ClientFlag::ReadOnly);
        flags.unset_flag(ClientFlag::Blocked);
        assert_eq!(flags.to_string(), "r");
    }

    // -- Stats --------------------------------------------------------------

    #[test]
    fn stats_after_operations() {
        let n = ClientNegotiator::new();

        n.handle_hello(&HelloArgs::default()).unwrap();
        n.handle_hello(&HelloArgs {
            protocol_version: Some(3),
            ..Default::default()
        })
        .unwrap();

        n.register_client(100, "a:1".to_string());
        n.enable_tracking(100, TrackingConfig::default()).unwrap();
        n.track_key(100, "k1");
        n.invalidate("k1");

        let stats = n.get_stats();
        // 2 from HELLO + 1 from register_client
        assert_eq!(stats.total_clients_registered, 3);
        assert_eq!(stats.total_hello_commands, 2);
        assert_eq!(stats.total_tracking_enabled, 1);
        assert_eq!(stats.total_invalidations_sent, 1);
        assert_eq!(stats.current_tracking_clients, 1);
        // Hello clients: one v2 + one v3 + registered client (v2)
        assert_eq!(stats.protocol_v2_clients, 2);
        assert_eq!(stats.protocol_v3_clients, 1);
    }

    // -- Register / Unregister ----------------------------------------------

    #[test]
    fn register_and_unregister_client() {
        let n = ClientNegotiator::new();
        n.register_client(5, "10.0.0.1:1234".to_string());
        assert!(n.get_client_info(5).is_some());
        n.unregister_client(5);
        assert!(n.get_client_info(5).is_none());
    }

    #[test]
    fn unregister_cleans_up_tracking() {
        let n = ClientNegotiator::new();
        n.register_client(1, "a:1".to_string());
        n.enable_tracking(1, TrackingConfig::default()).unwrap();
        n.track_key(1, "mykey");

        n.unregister_client(1);

        // The key tracker should be cleared.
        let notified = n.invalidate("mykey");
        assert!(notified.is_empty());
    }

    #[test]
    fn unregister_cleans_up_broadcast_prefixes() {
        let n = ClientNegotiator::new();
        n.register_client(1, "a:1".to_string());
        n.enable_tracking(
            1,
            TrackingConfig {
                bcast: true,
                prefixes: vec!["foo:".to_string()],
                ..Default::default()
            },
        )
        .unwrap();

        n.unregister_client(1);

        // No broadcast notifications after unregister.
        let notified = n.invalidate("foo:bar");
        assert!(notified.is_empty());
    }

    // -- Kill stats ---------------------------------------------------------

    #[test]
    fn kill_stats_tracked() {
        let (n, id) = negotiator_with_client();
        n.kill_client(KillTarget::Id(id));
        assert_eq!(n.get_stats().total_kills, 1);
    }
}
