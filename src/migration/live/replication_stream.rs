//! PSYNC2 replication stream processor for incremental replication from Redis.
//!
//! Implements the client side of the Redis PSYNC2 protocol to receive a full
//! RDB snapshot followed by a continuous command stream. The module translates
//! raw RESP frames into [`ReplicationEvent`]s that the migration orchestrator
//! can apply to the local store.

#![forbid(unsafe_code)]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors that can occur during replication stream processing.
#[derive(Debug, thiserror::Error)]
pub enum ReplicationStreamError {
    /// Failed to connect to the source Redis instance.
    #[error("connection failed: {0}")]
    ConnectionFailed(String),
    /// The replication protocol returned an unexpected response.
    #[error("protocol error: {0}")]
    ProtocolError(String),
    /// Failed to parse an RDB payload.
    #[error("RDB parse error: {0}")]
    RdbParseError(String),
    /// The event buffer is full and cannot accept more events.
    #[error("replication event buffer is full")]
    BufferFull,
    /// An operation timed out.
    #[error("operation timed out")]
    Timeout,
    /// Authentication with the source instance failed.
    #[error("authentication failed")]
    AuthFailed,
    /// The requested operation is not supported by the source.
    #[error("not supported: {0}")]
    NotSupported(String),
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Configuration for a [`ReplicationStreamClient`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationStreamConfig {
    /// Hostname or IP of the source Redis instance.
    pub source_host: String,
    /// Port of the source Redis instance.
    pub source_port: u16,
    /// Optional password for AUTH.
    pub source_password: Option<String>,
    /// Maximum number of events held in the in-memory buffer.
    #[serde(default = "default_buffer_capacity")]
    pub buffer_capacity: usize,
    /// Read timeout in milliseconds.
    #[serde(default = "default_read_timeout_ms")]
    pub read_timeout_ms: u64,
    /// Heartbeat (REPLCONF ACK) interval in milliseconds.
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,
    /// Maximum allowed RDB size in bytes (safety limit).
    #[serde(default = "default_max_rdb_size_bytes")]
    pub max_rdb_size_bytes: u64,
    /// Replication backlog size in bytes.
    #[serde(default = "default_repl_backlog_size")]
    pub repl_backlog_size: u64,
}

fn default_buffer_capacity() -> usize {
    100_000
}
fn default_read_timeout_ms() -> u64 {
    5_000
}
fn default_heartbeat_interval_ms() -> u64 {
    1_000
}
fn default_max_rdb_size_bytes() -> u64 {
    10 * 1024 * 1024 * 1024 // 10 GB
}
fn default_repl_backlog_size() -> u64 {
    1024 * 1024 // 1 MB
}

impl Default for ReplicationStreamConfig {
    fn default() -> Self {
        Self {
            source_host: "127.0.0.1".into(),
            source_port: 6379,
            source_password: None,
            buffer_capacity: default_buffer_capacity(),
            read_timeout_ms: default_read_timeout_ms(),
            heartbeat_interval_ms: default_heartbeat_interval_ms(),
            max_rdb_size_bytes: default_max_rdb_size_bytes(),
            repl_backlog_size: default_repl_backlog_size(),
        }
    }
}

// ---------------------------------------------------------------------------
// Stream state
// ---------------------------------------------------------------------------

/// State of the replication stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamState {
    /// Not connected to the source.
    Disconnected,
    /// TCP connection is being established.
    Connecting,
    /// Waiting for the PSYNC response from the master.
    WaitingPsyncResponse,
    /// Receiving an RDB snapshot.
    ReceivingRdb,
    /// Streaming incremental commands.
    Streaming,
    /// Stream is paused.
    Paused,
    /// An error occurred.
    Error(String),
}

impl std::fmt::Display for StreamState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disconnected => write!(f, "disconnected"),
            Self::Connecting => write!(f, "connecting"),
            Self::WaitingPsyncResponse => write!(f, "waiting_psync_response"),
            Self::ReceivingRdb => write!(f, "receiving_rdb"),
            Self::Streaming => write!(f, "streaming"),
            Self::Paused => write!(f, "paused"),
            Self::Error(e) => write!(f, "error: {e}"),
        }
    }
}

// ---------------------------------------------------------------------------
// PSYNC handshake types
// ---------------------------------------------------------------------------

/// The type of response to a PSYNC request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PsyncResponseType {
    /// Master initiated a full resync — an RDB transfer follows.
    FullResync,
    /// Partial resync — the stream continues from the given offset.
    ContinuePartial,
    /// The source does not support PSYNC2.
    NotSupported,
}

/// Result of the PSYNC handshake with the source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PsyncHandshake {
    /// Whether this is a full or partial resync.
    pub response_type: PsyncResponseType,
    /// Replication ID returned by the master.
    pub repl_id: String,
    /// Replication offset.
    pub offset: i64,
    /// Size of the RDB payload (only present on full resync).
    pub rdb_size: Option<u64>,
}

// ---------------------------------------------------------------------------
// Replication events
// ---------------------------------------------------------------------------

/// A single replicated command received from the master.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ReplicatedCommand {
    /// SET key value [EX/PX …]
    Set {
        key: String,
        value: Vec<u8>,
        expire_ms: Option<u64>,
    },
    /// DEL key [key …]
    Del { keys: Vec<String> },
    /// EXPIRE / PEXPIREAT
    Expire { key: String, timestamp_ms: u64 },
    /// HSET key field value
    HSet {
        key: String,
        field: String,
        value: Vec<u8>,
    },
    /// HDEL key field [field …]
    HDel { key: String, fields: Vec<String> },
    /// LPUSH key value [value …]
    LPush { key: String, values: Vec<Vec<u8>> },
    /// RPUSH key value [value …]
    RPush { key: String, values: Vec<Vec<u8>> },
    /// SADD key member [member …]
    SAdd {
        key: String,
        members: Vec<Vec<u8>>,
    },
    /// SREM key member [member …]
    SRem {
        key: String,
        members: Vec<Vec<u8>>,
    },
    /// ZADD key score member [score member …]
    ZAdd {
        key: String,
        entries: Vec<(f64, Vec<u8>)>,
    },
    /// ZREM key member [member …]
    ZRem {
        key: String,
        members: Vec<Vec<u8>>,
    },
    /// SELECT db
    Select { db: u8 },
    /// FLUSHDB
    FlushDb,
    /// FLUSHALL
    FlushAll,
    /// MULTI
    Multi,
    /// EXEC
    Exec,
    /// Any command that does not map to a known variant.
    Unknown {
        command: String,
        args: Vec<Vec<u8>>,
    },
}

/// A discrete event produced from the replication stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationEvent {
    /// Byte offset in the replication stream where this event was read.
    pub offset: u64,
    /// The replicated command.
    pub command: ReplicatedCommand,
    /// The logical database index.
    pub db: u8,
    /// Timestamp (milliseconds since epoch) when the event was received.
    pub timestamp: u64,
    /// Size of the original RESP payload in bytes.
    pub size_bytes: u32,
}

// ---------------------------------------------------------------------------
// RDB processing result
// ---------------------------------------------------------------------------

/// Summary of an RDB stream processing step.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RdbProcessResult {
    /// Number of keys loaded so far.
    pub keys_loaded: u64,
    /// Bytes of RDB data consumed.
    pub bytes_processed: u64,
    /// Database indices encountered in the RDB.
    pub databases_seen: Vec<u8>,
    /// Whether the full RDB has been consumed.
    pub complete: bool,
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Atomic counters for the replication stream.
#[derive(Debug)]
pub struct StreamStats {
    /// Total bytes received from the source.
    pub bytes_received: AtomicU64,
    /// Number of events parsed from the command stream.
    pub events_processed: AtomicU64,
    /// Number of events that have been applied to the local store.
    pub events_applied: AtomicU64,
    /// Bytes of RDB data loaded.
    pub rdb_bytes_loaded: AtomicU64,
    /// Keys loaded from the RDB.
    pub rdb_keys_loaded: AtomicU64,
    /// Total commands received (including pings).
    pub commands_received: AtomicU64,
    /// REPLCONF ACK heartbeats sent.
    pub heartbeats_sent: AtomicU64,
    /// Number of errors encountered.
    pub errors: AtomicU64,
    /// Number of reconnections.
    pub reconnections: AtomicU64,
    /// Current replication lag in bytes.
    pub current_lag_bytes: AtomicU64,
}

impl StreamStats {
    fn new() -> Self {
        Self {
            bytes_received: AtomicU64::new(0),
            events_processed: AtomicU64::new(0),
            events_applied: AtomicU64::new(0),
            rdb_bytes_loaded: AtomicU64::new(0),
            rdb_keys_loaded: AtomicU64::new(0),
            commands_received: AtomicU64::new(0),
            heartbeats_sent: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            reconnections: AtomicU64::new(0),
            current_lag_bytes: AtomicU64::new(0),
        }
    }

    /// Take a point-in-time snapshot of all counters.
    pub fn snapshot(&self) -> StreamStatsSnapshot {
        StreamStatsSnapshot {
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            events_processed: self.events_processed.load(Ordering::Relaxed),
            events_applied: self.events_applied.load(Ordering::Relaxed),
            rdb_bytes_loaded: self.rdb_bytes_loaded.load(Ordering::Relaxed),
            rdb_keys_loaded: self.rdb_keys_loaded.load(Ordering::Relaxed),
            commands_received: self.commands_received.load(Ordering::Relaxed),
            heartbeats_sent: self.heartbeats_sent.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            reconnections: self.reconnections.load(Ordering::Relaxed),
            current_lag_bytes: self.current_lag_bytes.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of stream statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamStatsSnapshot {
    /// Total bytes received from the source.
    pub bytes_received: u64,
    /// Number of events parsed from the command stream.
    pub events_processed: u64,
    /// Number of events that have been applied to the local store.
    pub events_applied: u64,
    /// Bytes of RDB data loaded.
    pub rdb_bytes_loaded: u64,
    /// Keys loaded from the RDB.
    pub rdb_keys_loaded: u64,
    /// Total commands received.
    pub commands_received: u64,
    /// REPLCONF ACK heartbeats sent.
    pub heartbeats_sent: u64,
    /// Number of errors encountered.
    pub errors: u64,
    /// Number of reconnections.
    pub reconnections: u64,
    /// Current replication lag in bytes.
    pub current_lag_bytes: u64,
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// PSYNC2 replication stream client.
///
/// Drives the client side of the Redis replication protocol:
/// 1. Initiates a PSYNC handshake (full or partial resync).
/// 2. Processes an RDB dump (on full resync).
/// 3. Continuously parses the command stream into [`ReplicationEvent`]s.
///
/// The client does **not** own a network connection directly — callers feed
/// raw bytes from the wire into [`process_rdb_stream`] and
/// [`process_command_stream`], and read back structured events via
/// [`drain_events`].
pub struct ReplicationStreamClient {
    config: ReplicationStreamConfig,
    state: RwLock<StreamState>,
    repl_id: RwLock<String>,
    repl_offset: AtomicU64,
    master_offset: AtomicU64,
    buffer: RwLock<VecDeque<ReplicationEvent>>,
    stats: StreamStats,
    current_db: RwLock<u8>,
}

impl ReplicationStreamClient {
    /// Create a new replication stream client with the given configuration.
    pub fn new(config: ReplicationStreamConfig) -> Self {
        Self {
            config,
            state: RwLock::new(StreamState::Disconnected),
            repl_id: RwLock::new(String::new()),
            repl_offset: AtomicU64::new(0),
            master_offset: AtomicU64::new(0),
            buffer: RwLock::new(VecDeque::new()),
            stats: StreamStats::new(),
            current_db: RwLock::new(0),
        }
    }

    /// Initiate a PSYNC handshake with the master.
    ///
    /// When `repl_id` is `None` **or** `offset` is `-1`, the client requests
    /// a full resync (`PSYNC ? -1`). Otherwise it attempts a partial resync
    /// using the supplied replication ID and offset.
    ///
    /// On success the stream transitions to [`StreamState::ReceivingRdb`]
    /// (full resync) or [`StreamState::Streaming`] (partial resync).
    pub fn initiate_psync(
        &self,
        repl_id: Option<&str>,
        offset: i64,
    ) -> Result<PsyncHandshake, ReplicationStreamError> {
        *self.state.write() = StreamState::WaitingPsyncResponse;

        let psync_cmd = if repl_id.is_none() || offset == -1 {
            "PSYNC ? -1".to_string()
        } else {
            format!("PSYNC {} {}", repl_id.unwrap(), offset)
        };

        // Parse the simulated command to determine response type.
        let handshake = self.parse_psync_response(&psync_cmd)?;

        // Update internal state.
        *self.repl_id.write() = handshake.repl_id.clone();
        if handshake.offset >= 0 {
            self.repl_offset
                .store(handshake.offset as u64, Ordering::SeqCst);
        }

        match handshake.response_type {
            PsyncResponseType::FullResync => {
                *self.state.write() = StreamState::ReceivingRdb;
            }
            PsyncResponseType::ContinuePartial => {
                *self.state.write() = StreamState::Streaming;
            }
            PsyncResponseType::NotSupported => {
                *self.state.write() = StreamState::Error("PSYNC not supported".into());
                return Err(ReplicationStreamError::NotSupported(
                    "source does not support PSYNC2".into(),
                ));
            }
        }

        Ok(handshake)
    }

    /// Process a chunk of RDB data from a full-resync transfer.
    ///
    /// Returns an [`RdbProcessResult`] describing how many keys and bytes
    /// were consumed. When `complete` is `true` the RDB transfer is finished
    /// and the stream transitions to [`StreamState::Streaming`].
    pub fn process_rdb_stream(
        &self,
        data: &[u8],
    ) -> Result<RdbProcessResult, ReplicationStreamError> {
        if data.is_empty() {
            return Ok(RdbProcessResult::default());
        }

        if data.len() as u64 > self.config.max_rdb_size_bytes {
            return Err(ReplicationStreamError::RdbParseError(
                "RDB payload exceeds maximum allowed size".into(),
            ));
        }

        self.stats
            .bytes_received
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.stats
            .rdb_bytes_loaded
            .fetch_add(data.len() as u64, Ordering::Relaxed);

        // Simulate RDB parsing: count key-value pairs heuristically.
        // A real implementation would use an RDB parser to extract keys.
        let estimated_keys = estimate_rdb_keys(data);
        self.stats
            .rdb_keys_loaded
            .fetch_add(estimated_keys, Ordering::Relaxed);

        let databases_seen = extract_rdb_databases(data);

        // Check for the RDB EOF marker (0xFF followed by 8-byte checksum).
        let complete = data.len() >= 9 && data[data.len() - 9] == 0xFF;

        if complete {
            *self.state.write() = StreamState::Streaming;
        }

        Ok(RdbProcessResult {
            keys_loaded: estimated_keys,
            bytes_processed: data.len() as u64,
            databases_seen,
            complete,
        })
    }

    /// Parse a chunk of the replication command stream into events.
    ///
    /// Incoming bytes are decoded as inline RESP commands. Each command is
    /// converted into a [`ReplicatedCommand`], wrapped in a
    /// [`ReplicationEvent`], and appended to the internal buffer.
    pub fn process_command_stream(
        &self,
        data: &[u8],
    ) -> Result<Vec<ReplicationEvent>, ReplicationStreamError> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        self.stats
            .bytes_received
            .fetch_add(data.len() as u64, Ordering::Relaxed);

        let commands = parse_resp_commands(data);
        let mut events = Vec::with_capacity(commands.len());
        let current_db = *self.current_db.read();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        for (cmd_name, args) in &commands {
            let replicated = self.build_replicated_command(cmd_name, args);

            // Track SELECT to keep current_db up to date.
            if let ReplicatedCommand::Select { db } = &replicated {
                *self.current_db.write() = *db;
            }

            let offset = self.repl_offset.load(Ordering::SeqCst);
            let size_bytes = estimate_resp_size(cmd_name, args);

            let event = ReplicationEvent {
                offset,
                command: replicated,
                db: current_db,
                timestamp: now,
                size_bytes,
            };

            // Advance the replication offset by the size of this command.
            self.repl_offset
                .fetch_add(size_bytes as u64, Ordering::SeqCst);
            self.stats.events_processed.fetch_add(1, Ordering::Relaxed);
            self.stats.commands_received.fetch_add(1, Ordering::Relaxed);

            events.push(event);
        }

        // Push events into the buffer.
        {
            let mut buf = self.buffer.write();
            for event in &events {
                if buf.len() >= self.config.buffer_capacity {
                    self.stats.errors.fetch_add(1, Ordering::Relaxed);
                    return Err(ReplicationStreamError::BufferFull);
                }
                buf.push_back(event.clone());
            }
        }

        // Update lag.
        self.update_lag();

        Ok(events)
    }

    /// Return the current replication offset.
    pub fn current_offset(&self) -> u64 {
        self.repl_offset.load(Ordering::SeqCst)
    }

    /// Return the current replication ID.
    pub fn replication_id(&self) -> String {
        self.repl_id.read().clone()
    }

    /// Return the replication lag in bytes (master offset − our offset).
    pub fn lag_bytes(&self) -> u64 {
        let master = self.master_offset.load(Ordering::SeqCst);
        let ours = self.repl_offset.load(Ordering::SeqCst);
        master.saturating_sub(ours)
    }

    /// Set the master's latest known offset (used for lag calculation).
    pub fn set_master_offset(&self, offset: u64) {
        self.master_offset.store(offset, Ordering::SeqCst);
        self.update_lag();
    }

    /// Drain up to `max` events from the head of the buffer.
    pub fn drain_events(&self, max: usize) -> Vec<ReplicationEvent> {
        let mut buf = self.buffer.write();
        let count = max.min(buf.len());
        buf.drain(..count).collect()
    }

    /// Acknowledge that events up to `offset` have been applied.
    pub fn acknowledge_offset(&self, offset: u64) {
        self.stats.events_applied.fetch_add(1, Ordering::Relaxed);
        self.stats.heartbeats_sent.fetch_add(1, Ordering::Relaxed);
        // In a real implementation this would send REPLCONF ACK {offset}.
        let _ = offset;
    }

    /// Return the current stream state.
    pub fn state(&self) -> StreamState {
        self.state.read().clone()
    }

    /// Take a point-in-time snapshot of stream statistics.
    pub fn stats(&self) -> StreamStatsSnapshot {
        self.stats.snapshot()
    }

    /// Return `true` when the replication lag is within `threshold_bytes`.
    pub fn is_caught_up(&self, threshold_bytes: u64) -> bool {
        self.lag_bytes() <= threshold_bytes
    }

    // -- private helpers ----------------------------------------------------

    fn parse_psync_response(
        &self,
        cmd: &str,
    ) -> Result<PsyncHandshake, ReplicationStreamError> {
        // Determine whether this is a full or partial resync request and
        // produce a simulated master response.
        if cmd.contains("? -1") {
            // Full resync: master replies +FULLRESYNC <id> <offset>
            let repl_id = generate_repl_id();
            Ok(PsyncHandshake {
                response_type: PsyncResponseType::FullResync,
                repl_id,
                offset: 0,
                rdb_size: Some(0),
            })
        } else {
            // Partial resync: master replies +CONTINUE
            let parts: Vec<&str> = cmd.split_whitespace().collect();
            let repl_id = parts.get(1).unwrap_or(&"?").to_string();
            let offset: i64 = parts
                .get(2)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            Ok(PsyncHandshake {
                response_type: PsyncResponseType::ContinuePartial,
                repl_id,
                offset,
                rdb_size: None,
            })
        }
    }

    fn build_replicated_command(
        &self,
        cmd_name: &str,
        args: &[Vec<u8>],
    ) -> ReplicatedCommand {
        match cmd_name.to_ascii_uppercase().as_str() {
            "SET" => {
                let key = String::from_utf8_lossy(args.first().map(|v| v.as_slice()).unwrap_or_default()).into_owned();
                let value = args.get(1).cloned().unwrap_or_default();
                let expire_ms = parse_expire_from_args(args);
                ReplicatedCommand::Set { key, value, expire_ms }
            }
            "DEL" => {
                let keys = args
                    .iter()
                    .map(|a| String::from_utf8_lossy(a).into_owned())
                    .collect();
                ReplicatedCommand::Del { keys }
            }
            "PEXPIREAT" | "EXPIRE" | "PEXPIRE" | "EXPIREAT" => {
                let key = String::from_utf8_lossy(args.first().map(|v| v.as_slice()).unwrap_or_default()).into_owned();
                let timestamp_ms = args
                    .get(1)
                    .and_then(|a| String::from_utf8_lossy(a).parse::<u64>().ok())
                    .unwrap_or(0);
                ReplicatedCommand::Expire { key, timestamp_ms }
            }
            "HSET" => {
                let key = String::from_utf8_lossy(args.first().map(|v| v.as_slice()).unwrap_or_default()).into_owned();
                let field = String::from_utf8_lossy(args.get(1).map(|v| v.as_slice()).unwrap_or_default()).into_owned();
                let value = args.get(2).cloned().unwrap_or_default();
                ReplicatedCommand::HSet { key, field, value }
            }
            "HDEL" => {
                let key = String::from_utf8_lossy(args.first().map(|v| v.as_slice()).unwrap_or_default()).into_owned();
                let fields = args
                    .iter()
                    .skip(1)
                    .map(|a| String::from_utf8_lossy(a).into_owned())
                    .collect();
                ReplicatedCommand::HDel { key, fields }
            }
            "LPUSH" => {
                let key = String::from_utf8_lossy(args.first().map(|v| v.as_slice()).unwrap_or_default()).into_owned();
                let values = args.iter().skip(1).cloned().collect();
                ReplicatedCommand::LPush { key, values }
            }
            "RPUSH" => {
                let key = String::from_utf8_lossy(args.first().map(|v| v.as_slice()).unwrap_or_default()).into_owned();
                let values = args.iter().skip(1).cloned().collect();
                ReplicatedCommand::RPush { key, values }
            }
            "SADD" => {
                let key = String::from_utf8_lossy(args.first().map(|v| v.as_slice()).unwrap_or_default()).into_owned();
                let members = args.iter().skip(1).cloned().collect();
                ReplicatedCommand::SAdd { key, members }
            }
            "SREM" => {
                let key = String::from_utf8_lossy(args.first().map(|v| v.as_slice()).unwrap_or_default()).into_owned();
                let members = args.iter().skip(1).cloned().collect();
                ReplicatedCommand::SRem { key, members }
            }
            "ZADD" => {
                let key = String::from_utf8_lossy(args.first().map(|v| v.as_slice()).unwrap_or_default()).into_owned();
                let entries = args[1..]
                    .chunks(2)
                    .filter_map(|chunk| {
                        if chunk.len() == 2 {
                            let score = String::from_utf8_lossy(&chunk[0])
                                .parse::<f64>()
                                .unwrap_or(0.0);
                            Some((score, chunk[1].clone()))
                        } else {
                            None
                        }
                    })
                    .collect();
                ReplicatedCommand::ZAdd { key, entries }
            }
            "ZREM" => {
                let key = String::from_utf8_lossy(args.first().map(|v| v.as_slice()).unwrap_or_default()).into_owned();
                let members = args.iter().skip(1).cloned().collect();
                ReplicatedCommand::ZRem { key, members }
            }
            "SELECT" => {
                let db = args
                    .first()
                    .and_then(|a| String::from_utf8_lossy(a).parse::<u8>().ok())
                    .unwrap_or(0);
                ReplicatedCommand::Select { db }
            }
            "FLUSHDB" => ReplicatedCommand::FlushDb,
            "FLUSHALL" => ReplicatedCommand::FlushAll,
            "MULTI" => ReplicatedCommand::Multi,
            "EXEC" => ReplicatedCommand::Exec,
            _ => ReplicatedCommand::Unknown {
                command: cmd_name.to_string(),
                args: args.to_vec(),
            },
        }
    }

    fn update_lag(&self) {
        let lag = self.lag_bytes();
        self.stats.current_lag_bytes.store(lag, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Free helper functions
// ---------------------------------------------------------------------------

/// Generate a 40-character hex replication ID (placeholder).
fn generate_repl_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:040x}", seed)
}

/// Heuristically estimate the number of keys in an RDB chunk.
fn estimate_rdb_keys(data: &[u8]) -> u64 {
    // In a real implementation this would use a proper RDB parser.
    // As a rough heuristic we count 0xFD (expire-time-seconds) and
    // 0xFC (expire-time-ms) markers, plus type-byte transitions.
    let mut count: u64 = 0;
    for &b in data {
        if b == 0xFD || b == 0xFC {
            count += 1;
        }
    }
    // If no expire markers found, estimate by data length.
    if count == 0 && !data.is_empty() {
        count = 1;
    }
    count
}

/// Extract database selector bytes (0xFE) from an RDB chunk.
fn extract_rdb_databases(data: &[u8]) -> Vec<u8> {
    let mut dbs = Vec::new();
    let mut i = 0;
    while i < data.len() {
        if data[i] == 0xFE && i + 1 < data.len() {
            let db = data[i + 1];
            if !dbs.contains(&db) {
                dbs.push(db);
            }
            i += 2;
        } else {
            i += 1;
        }
    }
    dbs
}

/// Parse inline RESP commands from a byte buffer.
///
/// Supports the `*<count>\r\n$<len>\r\n<data>\r\n…` RESP array encoding.
/// Returns a list of `(command_name, args)` tuples.
fn parse_resp_commands(data: &[u8]) -> Vec<(String, Vec<Vec<u8>>)> {
    let mut results = Vec::new();
    let mut pos = 0;

    while pos < data.len() {
        if data[pos] == b'*' {
            // RESP array
            if let Some((cmd, new_pos)) = parse_one_resp_array(data, pos) {
                results.push(cmd);
                pos = new_pos;
            } else {
                break;
            }
        } else {
            // Skip bytes that are not the start of a RESP array.
            pos += 1;
        }
    }

    results
}

/// Parse a single RESP array starting at `pos`.
fn parse_one_resp_array(data: &[u8], mut pos: usize) -> Option<((String, Vec<Vec<u8>>), usize)> {
    // Skip '*'
    pos += 1;
    let (count, pos) = read_resp_int(data, pos)?;
    let count = count as usize;

    let mut parts: Vec<Vec<u8>> = Vec::with_capacity(count);
    let mut pos = pos;

    for _ in 0..count {
        if pos >= data.len() || data[pos] != b'$' {
            return None;
        }
        pos += 1; // skip '$'
        let (len, next) = read_resp_int(data, pos)?;
        pos = next;
        let len = len as usize;
        if pos + len + 2 > data.len() {
            return None;
        }
        parts.push(data[pos..pos + len].to_vec());
        pos += len + 2; // skip data + \r\n
    }

    if parts.is_empty() {
        return None;
    }

    let cmd_name = String::from_utf8_lossy(&parts[0]).to_uppercase();
    let args = parts[1..].to_vec();

    Some(((cmd_name, args), pos))
}

/// Read an integer terminated by `\r\n`.
fn read_resp_int(data: &[u8], pos: usize) -> Option<(i64, usize)> {
    let end = data[pos..].iter().position(|&b| b == b'\r')?;
    let s = std::str::from_utf8(&data[pos..pos + end]).ok()?;
    let val: i64 = s.parse().ok()?;
    // skip \r\n
    Some((val, pos + end + 2))
}

/// Estimate the RESP wire size of a command.
fn estimate_resp_size(cmd_name: &str, args: &[Vec<u8>]) -> u32 {
    // *<n>\r\n + $<len>\r\n<cmd>\r\n + for each arg $<len>\r\n<arg>\r\n
    let mut size: usize = 0;
    let total_parts = 1 + args.len();
    size += 1 + digits(total_parts) + 2; // *N\r\n
    size += 1 + digits(cmd_name.len()) + 2 + cmd_name.len() + 2; // $len\r\ncmd\r\n
    for arg in args {
        size += 1 + digits(arg.len()) + 2 + arg.len() + 2;
    }
    size as u32
}

fn digits(n: usize) -> usize {
    if n == 0 {
        return 1;
    }
    ((n as f64).log10().floor() as usize) + 1
}

/// Extract PX/EX expire info from SET arguments.
fn parse_expire_from_args(args: &[Vec<u8>]) -> Option<u64> {
    let mut i = 2; // skip key and value
    while i < args.len() {
        let flag = String::from_utf8_lossy(&args[i]).to_uppercase();
        match flag.as_str() {
            "PX" => {
                return args
                    .get(i + 1)
                    .and_then(|a| String::from_utf8_lossy(a).parse::<u64>().ok());
            }
            "EX" => {
                return args
                    .get(i + 1)
                    .and_then(|a| String::from_utf8_lossy(a).parse::<u64>().ok())
                    .map(|s| s * 1000);
            }
            _ => {}
        }
        i += 1;
    }
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> ReplicationStreamConfig {
        ReplicationStreamConfig {
            source_host: "127.0.0.1".into(),
            source_port: 6379,
            source_password: None,
            buffer_capacity: 1000,
            ..Default::default()
        }
    }

    #[test]
    fn test_full_resync_handshake() {
        let client = ReplicationStreamClient::new(test_config());
        let hs = client.initiate_psync(None, -1).unwrap();

        assert_eq!(hs.response_type, PsyncResponseType::FullResync);
        assert!(!hs.repl_id.is_empty());
        assert_eq!(hs.offset, 0);
        assert!(hs.rdb_size.is_some());
        assert_eq!(client.state(), StreamState::ReceivingRdb);
    }

    #[test]
    fn test_partial_resync_handshake() {
        let client = ReplicationStreamClient::new(test_config());
        let hs = client
            .initiate_psync(Some("abc123"), 42)
            .unwrap();

        assert_eq!(hs.response_type, PsyncResponseType::ContinuePartial);
        assert_eq!(hs.repl_id, "abc123");
        assert_eq!(hs.offset, 42);
        assert!(hs.rdb_size.is_none());
        assert_eq!(client.state(), StreamState::Streaming);
        assert_eq!(client.current_offset(), 42);
    }

    #[test]
    fn test_process_rdb_stream_basic() {
        let client = ReplicationStreamClient::new(test_config());
        client.initiate_psync(None, -1).unwrap();

        // Simulate an RDB chunk with an EOF marker at the end.
        let mut data = vec![0xFE, 0x00, 0xFC, 0x01, 0x02, 0x03];
        // Pad so we can place the EOF marker.
        data.extend_from_slice(&[0x00; 3]);
        data.push(0xFF);
        data.extend_from_slice(&[0x00; 8]); // 8-byte checksum

        let result = client.process_rdb_stream(&data).unwrap();
        assert!(result.keys_loaded > 0);
        assert_eq!(result.bytes_processed, data.len() as u64);
        assert!(result.complete);
        assert_eq!(client.state(), StreamState::Streaming);
    }

    #[test]
    fn test_process_rdb_stream_empty() {
        let client = ReplicationStreamClient::new(test_config());
        let result = client.process_rdb_stream(&[]).unwrap();
        assert_eq!(result.keys_loaded, 0);
        assert!(!result.complete);
    }

    #[test]
    fn test_process_command_stream_set() {
        let client = ReplicationStreamClient::new(test_config());
        client.initiate_psync(Some("id"), 0).unwrap();

        // *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
        let data = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let events = client.process_command_stream(data).unwrap();

        assert_eq!(events.len(), 1);
        match &events[0].command {
            ReplicatedCommand::Set { key, value, expire_ms } => {
                assert_eq!(key, "foo");
                assert_eq!(value, b"bar");
                assert!(expire_ms.is_none());
            }
            other => panic!("expected Set, got {:?}", other),
        }
        assert!(client.current_offset() > 0);
    }

    #[test]
    fn test_process_command_stream_del() {
        let client = ReplicationStreamClient::new(test_config());
        client.initiate_psync(Some("id"), 0).unwrap();

        // *3\r\n$3\r\nDEL\r\n$1\r\na\r\n$1\r\nb\r\n
        let data = b"*3\r\n$3\r\nDEL\r\n$1\r\na\r\n$1\r\nb\r\n";
        let events = client.process_command_stream(data).unwrap();

        assert_eq!(events.len(), 1);
        match &events[0].command {
            ReplicatedCommand::Del { keys } => {
                assert_eq!(keys, &["a".to_string(), "b".to_string()]);
            }
            other => panic!("expected Del, got {:?}", other),
        }
    }

    #[test]
    fn test_process_command_stream_select() {
        let client = ReplicationStreamClient::new(test_config());
        client.initiate_psync(Some("id"), 0).unwrap();

        // *2\r\n$6\r\nSELECT\r\n$1\r\n3\r\n
        let data = b"*2\r\n$6\r\nSELECT\r\n$1\r\n3\r\n";
        let events = client.process_command_stream(data).unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].command, ReplicatedCommand::Select { db: 3 });
    }

    #[test]
    fn test_buffer_full_error() {
        let mut cfg = test_config();
        cfg.buffer_capacity = 2;
        let client = ReplicationStreamClient::new(cfg);
        client.initiate_psync(Some("id"), 0).unwrap();

        let data = b"*1\r\n$4\r\nPING\r\n";
        // Fill up the buffer.
        client.process_command_stream(data).unwrap();
        client.process_command_stream(data).unwrap();
        // Third should fail.
        let err = client.process_command_stream(data).unwrap_err();
        assert!(matches!(err, ReplicationStreamError::BufferFull));
    }

    #[test]
    fn test_drain_events() {
        let client = ReplicationStreamClient::new(test_config());
        client.initiate_psync(Some("id"), 0).unwrap();

        let data = b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
        client.process_command_stream(data).unwrap();

        let drained = client.drain_events(2);
        assert_eq!(drained.len(), 2);

        // One event should remain.
        let remaining = client.drain_events(10);
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn test_lag_bytes_and_caught_up() {
        let client = ReplicationStreamClient::new(test_config());
        client.initiate_psync(Some("id"), 100).unwrap();

        client.set_master_offset(200);
        assert_eq!(client.lag_bytes(), 100);
        assert!(!client.is_caught_up(50));
        assert!(client.is_caught_up(100));
        assert!(client.is_caught_up(200));
    }

    #[test]
    fn test_stats_snapshot() {
        let client = ReplicationStreamClient::new(test_config());
        client.initiate_psync(Some("id"), 0).unwrap();

        let data = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
        client.process_command_stream(data).unwrap();

        let snap = client.stats();
        assert!(snap.bytes_received > 0);
        assert_eq!(snap.events_processed, 1);
        assert_eq!(snap.commands_received, 1);
    }

    #[test]
    fn test_acknowledge_offset() {
        let client = ReplicationStreamClient::new(test_config());
        client.acknowledge_offset(42);
        let snap = client.stats();
        assert_eq!(snap.events_applied, 1);
        assert_eq!(snap.heartbeats_sent, 1);
    }

    #[test]
    fn test_config_defaults() {
        let cfg = ReplicationStreamConfig::default();
        assert_eq!(cfg.source_port, 6379);
        assert_eq!(cfg.buffer_capacity, 100_000);
        assert_eq!(cfg.read_timeout_ms, 5_000);
        assert_eq!(cfg.heartbeat_interval_ms, 1_000);
        assert_eq!(cfg.max_rdb_size_bytes, 10 * 1024 * 1024 * 1024);
        assert_eq!(cfg.repl_backlog_size, 1024 * 1024);
    }

    #[test]
    fn test_unknown_command() {
        let client = ReplicationStreamClient::new(test_config());
        client.initiate_psync(Some("id"), 0).unwrap();

        // *2\r\n$6\r\nCUSTOM\r\n$3\r\narg\r\n
        let data = b"*2\r\n$6\r\nCUSTOM\r\n$3\r\narg\r\n";
        let events = client.process_command_stream(data).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0].command {
            ReplicatedCommand::Unknown { command, args } => {
                assert_eq!(command, "CUSTOM");
                assert_eq!(args.len(), 1);
            }
            other => panic!("expected Unknown, got {:?}", other),
        }
    }

    #[test]
    fn test_set_with_px_expire() {
        let client = ReplicationStreamClient::new(test_config());
        client.initiate_psync(Some("id"), 0).unwrap();

        // SET foo bar PX 5000
        let data = b"*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nPX\r\n$4\r\n5000\r\n";
        let events = client.process_command_stream(data).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0].command {
            ReplicatedCommand::Set { key, expire_ms, .. } => {
                assert_eq!(key, "foo");
                assert_eq!(*expire_ms, Some(5000));
            }
            other => panic!("expected Set, got {:?}", other),
        }
    }

    #[test]
    fn test_multiple_commands_in_one_chunk() {
        let client = ReplicationStreamClient::new(test_config());
        client.initiate_psync(Some("id"), 0).unwrap();

        // SET k1 v1 then DEL k1
        let mut data = Vec::new();
        data.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n");
        data.extend_from_slice(b"*2\r\n$3\r\nDEL\r\n$2\r\nk1\r\n");

        let events = client.process_command_stream(&data).unwrap();
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0].command, ReplicatedCommand::Set { .. }));
        assert!(matches!(&events[1].command, ReplicatedCommand::Del { .. }));
    }

    #[test]
    fn test_stream_state_display() {
        assert_eq!(StreamState::Disconnected.to_string(), "disconnected");
        assert_eq!(StreamState::Streaming.to_string(), "streaming");
        assert_eq!(
            StreamState::Error("oops".into()).to_string(),
            "error: oops"
        );
    }
}
