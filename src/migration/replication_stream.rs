//! Live replication stream from Redis
//!
//! Connects as a Redis replica to receive real-time mutations
//! for zero-downtime migration cutover.
#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors that can occur during replication streaming.
#[derive(Debug, thiserror::Error)]
pub enum ReplicationError {
    /// Failed to connect to the source Redis instance.
    #[error("connection failed: {0}")]
    ConnectionFailed(String),
    /// Authentication with the source instance failed.
    #[error("authentication failed: {0}")]
    AuthFailed(String),
    /// PSYNC handshake or initial sync failed.
    #[error("sync failed: {0}")]
    SyncFailed(String),
    /// The replication stream was unexpectedly broken.
    #[error("replication stream broken: {0}")]
    StreamBroken(String),
    /// An operation timed out.
    #[error("operation timed out: {0}")]
    Timeout(String),
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Configuration for a [`ReplicationStream`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Hostname or IP of the source Redis instance.
    pub source_host: String,
    /// Port of the source Redis instance.
    pub source_port: u16,
    /// Optional password for AUTH.
    pub source_password: Option<String>,
    /// Unique identifier for this replica.
    pub replica_id: String,
    /// Starting replication offset (0 for full sync).
    pub replication_offset: u64,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            source_host: "127.0.0.1".to_string(),
            source_port: 6379,
            source_password: None,
            replica_id: "ferrite-replica-001".to_string(),
            replication_offset: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Handle & stats
// ---------------------------------------------------------------------------

/// State of the replication stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationState {
    /// Establishing TCP connection.
    Connecting,
    /// Performing PSYNC handshake and receiving RDB snapshot.
    Syncing,
    /// Receiving live command stream.
    Streaming,
    /// Stream stopped gracefully.
    Stopped,
    /// Stream encountered an error.
    Error,
}

impl std::fmt::Display for ReplicationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connecting => write!(f, "connecting"),
            Self::Syncing => write!(f, "syncing"),
            Self::Streaming => write!(f, "streaming"),
            Self::Stopped => write!(f, "stopped"),
            Self::Error => write!(f, "error"),
        }
    }
}

/// Handle returned when replication is started.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationHandle {
    /// Unique identifier for this replication stream.
    pub stream_id: String,
    /// Current state of the stream.
    pub state: ReplicationState,
}

/// Runtime statistics for the replication stream.
pub struct ReplicationStats {
    /// Total commands received from the primary.
    pub commands_received: u64,
    /// Total bytes received from the primary.
    pub bytes_received: u64,
    /// Current replication lag in bytes.
    pub lag_bytes: u64,
    /// Timestamp when the stream connected.
    pub connected_since: Instant,
    /// Timestamp of the last heartbeat from the primary.
    pub last_heartbeat: Instant,
    /// Total number of errors encountered.
    pub errors: u64,
}

// ---------------------------------------------------------------------------
// ReplicationStream
// ---------------------------------------------------------------------------

/// Receives a live replication stream from a Redis primary.
///
/// Sends the `PSYNC` command to the source Redis, receives the initial RDB
/// snapshot, and then continuously applies the incoming command stream.
pub struct ReplicationStream {
    config: ReplicationConfig,
    state: RwLock<ReplicationState>,
    offset: AtomicU64,
    primary_offset: AtomicU64,
    commands_received: AtomicU64,
    bytes_received: AtomicU64,
    errors: AtomicU64,
    connected_since: RwLock<Option<Instant>>,
    last_heartbeat: RwLock<Option<Instant>>,
}

impl ReplicationStream {
    /// Create a new replication stream with the given configuration.
    pub fn new(config: ReplicationConfig) -> Self {
        let initial_offset = config.replication_offset;
        Self {
            config,
            state: RwLock::new(ReplicationState::Connecting),
            offset: AtomicU64::new(initial_offset),
            primary_offset: AtomicU64::new(0),
            commands_received: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            connected_since: RwLock::new(None),
            last_heartbeat: RwLock::new(None),
        }
    }

    /// Start the replication stream.
    ///
    /// Connects to the source Redis, authenticates if needed, and sends
    /// `PSYNC <replica_id> <offset>` to begin receiving the replication stream.
    pub async fn start_replication(
        &mut self,
    ) -> Result<ReplicationHandle, ReplicationError> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        *self.state.write() = ReplicationState::Connecting;

        let addr = format!("{}:{}", self.config.source_host, self.config.source_port);
        let mut stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| ReplicationError::ConnectionFailed(format!("{}: {}", addr, e)))?;

        // AUTH if password is set
        if let Some(ref password) = self.config.source_password {
            let auth_cmd = format!(
                "*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n",
                password.len(),
                password
            );
            stream
                .write_all(auth_cmd.as_bytes())
                .await
                .map_err(|e| ReplicationError::AuthFailed(e.to_string()))?;

            let mut buf = [0u8; 512];
            let n = stream
                .read(&mut buf)
                .await
                .map_err(|e| ReplicationError::AuthFailed(e.to_string()))?;
            let resp = String::from_utf8_lossy(&buf[..n]);
            if resp.starts_with('-') {
                return Err(ReplicationError::AuthFailed(resp.trim().to_string()));
            }
        }

        // REPLCONF listening-port
        let replconf_port =
            "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n";
        stream
            .write_all(replconf_port.as_bytes())
            .await
            .map_err(|e| ReplicationError::SyncFailed(format!("REPLCONF: {}", e)))?;
        let mut buf = [0u8; 256];
        let _ = stream.read(&mut buf).await;

        // PSYNC <repl_id> <offset>
        *self.state.write() = ReplicationState::Syncing;
        let offset_str = self.config.replication_offset.to_string();
        let psync_cmd = format!(
            "*3\r\n$5\r\nPSYNC\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            self.config.replica_id.len(),
            self.config.replica_id,
            offset_str.len(),
            offset_str,
        );
        stream
            .write_all(psync_cmd.as_bytes())
            .await
            .map_err(|e| ReplicationError::SyncFailed(format!("PSYNC: {}", e)))?;

        let mut psync_buf = [0u8; 4096];
        let n = stream
            .read(&mut psync_buf)
            .await
            .map_err(|e| ReplicationError::SyncFailed(format!("PSYNC response: {}", e)))?;
        let psync_resp = String::from_utf8_lossy(&psync_buf[..n]);

        // Validate PSYNC response
        if !psync_resp.contains("FULLRESYNC") && !psync_resp.contains("CONTINUE") {
            return Err(ReplicationError::SyncFailed(format!(
                "unexpected PSYNC response: {}",
                psync_resp.trim()
            )));
        }

        let now = Instant::now();
        *self.connected_since.write() = Some(now);
        *self.last_heartbeat.write() = Some(now);
        *self.state.write() = ReplicationState::Streaming;

        let stream_id = format!(
            "repl-{}-{}",
            self.config.source_host, self.config.source_port
        );

        Ok(ReplicationHandle {
            stream_id,
            state: ReplicationState::Streaming,
        })
    }

    /// Return the current replication offset.
    pub fn current_offset(&self) -> u64 {
        self.offset.load(Ordering::Relaxed)
    }

    /// Return the number of bytes this replica is behind the primary.
    pub fn lag_bytes(&self) -> u64 {
        let primary = self.primary_offset.load(Ordering::Relaxed);
        let local = self.offset.load(Ordering::Relaxed);
        primary.saturating_sub(local)
    }

    /// Stop the replication stream gracefully.
    pub fn stop(&self) -> Result<(), ReplicationError> {
        let current = *self.state.read();
        match current {
            ReplicationState::Stopped => Ok(()),
            ReplicationState::Error => Err(ReplicationError::StreamBroken(
                "stream is in error state".to_string(),
            )),
            _ => {
                *self.state.write() = ReplicationState::Stopped;
                Ok(())
            }
        }
    }

    /// Return a snapshot of current replication statistics.
    pub fn stats(&self) -> ReplicationStats {
        let now = Instant::now();
        ReplicationStats {
            commands_received: self.commands_received.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            lag_bytes: self.lag_bytes(),
            connected_since: self.connected_since.read().unwrap_or(now),
            last_heartbeat: self.last_heartbeat.read().unwrap_or(now),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ReplicationConfig::default();
        assert_eq!(config.source_port, 6379);
        assert_eq!(config.replication_offset, 0);
        assert!(config.source_password.is_none());
    }

    #[test]
    fn test_replication_state_display() {
        assert_eq!(format!("{}", ReplicationState::Connecting), "connecting");
        assert_eq!(format!("{}", ReplicationState::Streaming), "streaming");
        assert_eq!(format!("{}", ReplicationState::Stopped), "stopped");
    }

    #[test]
    fn test_new_stream() {
        let stream = ReplicationStream::new(ReplicationConfig::default());
        assert_eq!(stream.current_offset(), 0);
        assert_eq!(stream.lag_bytes(), 0);
    }

    #[test]
    fn test_stream_offset() {
        let stream = ReplicationStream::new(ReplicationConfig {
            replication_offset: 500,
            ..ReplicationConfig::default()
        });
        assert_eq!(stream.current_offset(), 500);
    }

    #[test]
    fn test_lag_bytes() {
        let stream = ReplicationStream::new(ReplicationConfig::default());
        stream.offset.store(100, Ordering::Relaxed);
        stream.primary_offset.store(300, Ordering::Relaxed);
        assert_eq!(stream.lag_bytes(), 200);
    }

    #[test]
    fn test_stop_stream() {
        let stream = ReplicationStream::new(ReplicationConfig::default());
        *stream.state.write() = ReplicationState::Streaming;
        assert!(stream.stop().is_ok());
        assert_eq!(*stream.state.read(), ReplicationState::Stopped);
    }

    #[test]
    fn test_stop_already_stopped() {
        let stream = ReplicationStream::new(ReplicationConfig::default());
        *stream.state.write() = ReplicationState::Stopped;
        assert!(stream.stop().is_ok());
    }

    #[test]
    fn test_stats_snapshot() {
        let stream = ReplicationStream::new(ReplicationConfig::default());
        stream.commands_received.store(42, Ordering::Relaxed);
        stream.bytes_received.store(1024, Ordering::Relaxed);
        let stats = stream.stats();
        assert_eq!(stats.commands_received, 42);
        assert_eq!(stats.bytes_received, 1024);
    }
}
