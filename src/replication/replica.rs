//! Replica (slave) replication logic
//!
//! This module implements the replica side of replication.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, error, info, trace, warn};

use crate::auth::SharedAcl;
use crate::commands::{Command, CommandExecutor, SharedBlockingListManager};
use crate::persistence::load_rdb;
use crate::protocol::{encode_frame, parse_frame, Frame};
use crate::runtime::SharedSubscriptionManager;
use crate::storage::Store;

use super::primary::{ReplicationError, HEARTBEAT_INTERVAL};
use super::{ReplicationId, SharedReplicationState};

/// Replica configuration
#[derive(Debug, Clone)]
pub struct ReplicaConfig {
    /// Master address
    pub master_addr: SocketAddr,
    /// Replica listening port (for REPLCONF)
    pub listening_port: u16,
    /// Reconnection delay
    pub reconnect_delay: Duration,
    /// Maximum reconnection attempts (0 = unlimited)
    pub max_reconnect_attempts: u32,
    /// Base delay for exponential backoff
    pub backoff_base: Duration,
    /// Maximum backoff delay cap
    pub backoff_max: Duration,
}

/// Replica connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaState {
    /// Not connected to master
    Disconnected,
    /// Connecting to master
    Connecting,
    /// Performing handshake
    Handshake,
    /// Waiting for RDB transfer
    WaitingRdb,
    /// Receiving RDB
    ReceivingRdb,
    /// Connected and streaming
    Streaming,
}

/// Replica replication handler
pub struct ReplicationReplica {
    /// Shared store for applying commands
    store: Arc<Store>,
    /// Replication state
    state: SharedReplicationState,
    /// Replica configuration
    config: ReplicaConfig,
    /// Current connection state
    connection_state: RwLock<ReplicaState>,
    /// Command executor for applying replicated commands
    executor: CommandExecutor,
    /// Master replication ID
    master_replid: RwLock<Option<ReplicationId>>,
}

impl ReplicationReplica {
    /// Create a new replica handler
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: Arc<Store>,
        state: SharedReplicationState,
        config: ReplicaConfig,
        pubsub_manager: SharedSubscriptionManager,
        acl: SharedAcl,
        blocking_manager: SharedBlockingListManager,
        blocking_stream_manager: crate::commands::blocking::SharedBlockingStreamManager,
        blocking_zset_manager: crate::commands::blocking::SharedBlockingSortedSetManager,
    ) -> Self {
        let executor = CommandExecutor::new(
            store.clone(),
            pubsub_manager,
            acl,
            blocking_manager,
            blocking_stream_manager,
            blocking_zset_manager,
        );
        Self {
            store,
            state,
            config,
            connection_state: RwLock::new(ReplicaState::Disconnected),
            executor,
            master_replid: RwLock::new(None),
        }
    }

    /// Start the replication loop
    pub async fn run(&self) -> Result<(), ReplicationError> {
        let mut consecutive_failures: u32 = 0;
        loop {
            *self.connection_state.write().await = ReplicaState::Connecting;

            match self.connect_and_sync().await {
                Ok(()) => {
                    info!("Replication stream ended normally");
                    consecutive_failures = 0;
                }
                Err(e) => {
                    consecutive_failures += 1;
                    error!(
                        error = %e,
                        attempt = consecutive_failures,
                        "Replication error"
                    );
                    *self.connection_state.write().await = ReplicaState::Disconnected;

                    // Check retry limit
                    if self.config.max_reconnect_attempts > 0
                        && consecutive_failures >= self.config.max_reconnect_attempts
                    {
                        error!(
                            attempts = consecutive_failures,
                            "Exceeded maximum reconnection attempts, giving up"
                        );
                        return Err(e);
                    }
                }
            }

            // Exponential backoff with jitter
            let delay = self.backoff_delay(consecutive_failures);
            info!(
                delay_ms = delay.as_millis(),
                attempt = consecutive_failures,
                "Waiting before reconnecting"
            );
            tokio::time::sleep(delay).await;
        }
    }

    /// Calculate backoff delay with exponential increase and jitter.
    fn backoff_delay(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return self.config.reconnect_delay;
        }
        let base_ms = self.config.backoff_base.as_millis() as u64;
        let max_ms = self.config.backoff_max.as_millis() as u64;
        // 2^(attempt-1) * base, capped at max
        let shift = attempt.min(20) - 1;
        let exp = base_ms.saturating_mul(1u64.checked_shl(shift).unwrap_or(u64::MAX));
        let capped = exp.min(max_ms);
        // Add ±25% jitter
        let jitter = (capped / 4) as i64;
        let offset = if jitter > 0 {
            (rand::random::<u64>() % (jitter as u64 * 2)) as i64 - jitter
        } else {
            0
        };
        let final_ms = (capped as i64 + offset).max(100) as u64;
        Duration::from_millis(final_ms)
    }

    /// Connect to master and start sync
    async fn connect_and_sync(&self) -> Result<(), ReplicationError> {
        info!("Connecting to master at {}", self.config.master_addr);

        let mut stream = TcpStream::connect(self.config.master_addr).await?;
        let mut buf = BytesMut::with_capacity(64 * 1024);

        // Perform handshake
        *self.connection_state.write().await = ReplicaState::Handshake;
        self.handshake(&mut stream, &mut buf).await?;

        // Start PSYNC
        *self.connection_state.write().await = ReplicaState::WaitingRdb;
        let (replid, offset) = self.psync(&mut stream, &mut buf).await?;

        // Store master replid
        *self.master_replid.write().await = Some(replid.clone());
        self.state.set_master_replid(replid).await;
        self.state.set_offset(offset);

        // Stream commands
        *self.connection_state.write().await = ReplicaState::Streaming;
        self.stream_commands(&mut stream, &mut buf).await?;

        Ok(())
    }

    /// Perform initial handshake with master
    async fn handshake(
        &self,
        stream: &mut TcpStream,
        buf: &mut BytesMut,
    ) -> Result<(), ReplicationError> {
        // PING
        let ping = Frame::array(vec![Frame::bulk("PING")]);
        self.send_command(stream, &ping).await?;
        let response = self.read_response(stream, buf).await?;
        debug!("PING response: {:?}", response);

        // REPLCONF listening-port
        let replconf_port = Frame::array(vec![
            Frame::bulk("REPLCONF"),
            Frame::bulk("listening-port"),
            Frame::bulk(self.config.listening_port.to_string()),
        ]);
        self.send_command(stream, &replconf_port).await?;
        let response = self.read_response(stream, buf).await?;
        debug!("REPLCONF listening-port response: {:?}", response);

        // REPLCONF capa eof capa psync2
        let replconf_capa = Frame::array(vec![
            Frame::bulk("REPLCONF"),
            Frame::bulk("capa"),
            Frame::bulk("eof"),
            Frame::bulk("capa"),
            Frame::bulk("psync2"),
        ]);
        self.send_command(stream, &replconf_capa).await?;
        let response = self.read_response(stream, buf).await?;
        debug!("REPLCONF capa response: {:?}", response);

        Ok(())
    }

    /// Send PSYNC and handle response
    async fn psync(
        &self,
        stream: &mut TcpStream,
        buf: &mut BytesMut,
    ) -> Result<(ReplicationId, u64), ReplicationError> {
        // Get current replid and offset (or ? and -1 for first sync)
        let (replid, offset) = {
            let master_replid = self.master_replid.read().await;
            match &*master_replid {
                Some(id) => (id.as_str().to_string(), self.state.repl_offset() as i64),
                None => ("?".to_string(), -1_i64),
            }
        };

        // Send PSYNC
        let psync = Frame::array(vec![
            Frame::bulk("PSYNC"),
            Frame::bulk(replid),
            Frame::bulk(offset.to_string()),
        ]);
        self.send_command(stream, &psync).await?;

        // Read PSYNC response
        let response = self.read_response(stream, buf).await?;

        match response {
            Frame::Simple(ref b) if b.starts_with(b"FULLRESYNC") => {
                // Parse FULLRESYNC <replid> <offset>
                let s = std::str::from_utf8(b).map_err(|_| {
                    ReplicationError::Protocol("Invalid UTF-8 in FULLRESYNC response".to_string())
                })?;
                let parts: Vec<&str> = s.split_whitespace().collect();
                if parts.len() != 3 {
                    return Err(ReplicationError::Protocol(
                        "Invalid FULLRESYNC response".to_string(),
                    ));
                }
                let replid = ReplicationId::from_string(parts[1]);
                let offset: u64 = parts[2].parse().map_err(|_| {
                    ReplicationError::Protocol("Invalid offset in FULLRESYNC".to_string())
                })?;

                // Receive RDB
                *self.connection_state.write().await = ReplicaState::ReceivingRdb;
                self.receive_rdb(stream, buf).await?;

                Ok((replid, offset))
            }
            Frame::Simple(ref b) if b.starts_with(b"CONTINUE") => {
                // Parse CONTINUE [<replid>]
                let s = std::str::from_utf8(b).map_err(|_| {
                    ReplicationError::Protocol("Invalid UTF-8 in CONTINUE response".to_string())
                })?;
                let parts: Vec<&str> = s.split_whitespace().collect();
                let replid = if parts.len() > 1 {
                    ReplicationId::from_string(parts[1])
                } else {
                    self.master_replid
                        .read()
                        .await
                        .clone()
                        .unwrap_or_else(ReplicationId::new)
                };

                Ok((replid, self.state.repl_offset()))
            }
            _ => Err(ReplicationError::Protocol(format!(
                "Unexpected PSYNC response: {:?}",
                response
            ))),
        }
    }

    /// Receive RDB snapshot from master
    async fn receive_rdb(
        &self,
        stream: &mut TcpStream,
        buf: &mut BytesMut,
    ) -> Result<(), ReplicationError> {
        // Read RDB data - format is $<length>\r\n<data>
        // First ensure we have the length line
        loop {
            if buf.starts_with(b"$") {
                // Find the end of the length line
                if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                    // Parse length (need to copy to avoid borrow issues)
                    let len_str = std::str::from_utf8(&buf[1..pos - 1])
                        .map_err(|_| ReplicationError::Protocol("Invalid RDB length".to_string()))?
                        .to_string();

                    // Handle EOF format ($EOF:<marker>)
                    if let Some(stripped) = len_str.strip_prefix("EOF:") {
                        // Streaming RDB - read until marker
                        let marker = stripped.as_bytes().to_vec();
                        buf.advance(pos + 1);
                        self.receive_rdb_streaming(stream, buf, &marker).await?;
                        return Ok(());
                    }

                    let len: usize = len_str.parse().map_err(|_| {
                        ReplicationError::Protocol("Invalid RDB length".to_string())
                    })?;

                    buf.advance(pos + 1);

                    // Read RDB data
                    while buf.len() < len {
                        let n = stream.read_buf(buf).await?;
                        if n == 0 {
                            return Err(ReplicationError::Protocol(
                                "Connection closed during RDB transfer".to_string(),
                            ));
                        }
                    }

                    // Process RDB - load into store
                    info!("Received RDB snapshot ({} bytes), loading...", len);
                    let rdb_data = &buf[..len];
                    match load_rdb(rdb_data, &self.store) {
                        Ok(count) => {
                            info!("Loaded {} keys from RDB snapshot", count);
                        }
                        Err(e) => {
                            warn!("Failed to load RDB snapshot: {}, starting fresh", e);
                        }
                    }
                    buf.advance(len);
                    return Ok(());
                }
            }

            // Read more data
            let n = stream.read_buf(buf).await?;
            if n == 0 {
                return Err(ReplicationError::Protocol(
                    "Connection closed waiting for RDB".to_string(),
                ));
            }
        }
    }

    /// Receive streaming RDB with EOF marker
    async fn receive_rdb_streaming(
        &self,
        stream: &mut TcpStream,
        buf: &mut BytesMut,
        marker: &[u8],
    ) -> Result<(), ReplicationError> {
        // Read until we see the marker
        loop {
            if let Some(pos) = buf.windows(marker.len()).position(|w| w == marker) {
                info!("Received streaming RDB (found EOF marker)");
                buf.advance(pos + marker.len());
                return Ok(());
            }

            let n = stream.read_buf(buf).await?;
            if n == 0 {
                return Err(ReplicationError::Protocol(
                    "Connection closed during streaming RDB".to_string(),
                ));
            }
        }
    }

    /// Stream and apply commands from master
    async fn stream_commands(
        &self,
        stream: &mut TcpStream,
        buf: &mut BytesMut,
    ) -> Result<(), ReplicationError> {
        info!("Starting replication stream");

        // Create heartbeat interval (send REPLCONF ACK periodically)
        let mut heartbeat = interval(HEARTBEAT_INTERVAL);

        loop {
            tokio::select! {
                // Heartbeat: send REPLCONF ACK periodically
                _ = heartbeat.tick() => {
                    let offset = self.state.repl_offset();
                    self.send_ack(stream, offset).await?;
                    trace!("Sent heartbeat ACK with offset {}", offset);
                }

                // Read data from master
                result = stream.read_buf(buf) => {
                    let n = result?;
                    if n == 0 {
                        return Err(ReplicationError::Protocol(
                            "Master closed connection".to_string(),
                        ));
                    }

                    // Track bytes read for offset calculation
                    let bytes_before = buf.len();

                    // Parse and apply commands
                    while let Some(frame) = parse_frame(buf).map_err(|e| {
                        ReplicationError::Protocol(format!("Parse error: {}", e))
                    })? {
                        // Calculate the actual byte size of this command
                        let bytes_after = buf.len();
                        let command_size = bytes_before - bytes_after;

                        // Handle the command (may be REPLCONF GETACK or regular command)
                        self.handle_streamed_command(stream, frame, command_size as u64).await?;
                    }
                }
            }
        }
    }

    /// Handle a command from the replication stream
    async fn handle_streamed_command(
        &self,
        stream: &mut TcpStream,
        frame: Frame,
        command_size: u64,
    ) -> Result<(), ReplicationError> {
        // Check if this is a REPLCONF GETACK command
        if let Frame::Array(Some(ref arr)) = frame {
            if arr.len() >= 2 {
                if let (Frame::Bulk(Some(cmd)), Frame::Bulk(Some(subcmd))) = (&arr[0], &arr[1]) {
                    let cmd_upper = String::from_utf8_lossy(cmd).to_uppercase();
                    let subcmd_upper = String::from_utf8_lossy(subcmd).to_uppercase();

                    if cmd_upper == "REPLCONF" && subcmd_upper == "GETACK" {
                        // Respond with REPLCONF ACK <offset>
                        let offset = self.state.repl_offset();
                        self.send_ack(stream, offset).await?;
                        debug!("Responded to GETACK with offset {}", offset);
                        // Note: GETACK doesn't count towards offset
                        return Ok(());
                    }
                }
            }
        }

        // Track offset for regular commands
        self.state.increment_offset(command_size);

        // Apply the command
        self.apply_command(frame).await?;

        Ok(())
    }

    /// Send REPLCONF ACK to master
    async fn send_ack(&self, stream: &mut TcpStream, offset: u64) -> Result<(), ReplicationError> {
        let ack = Frame::array(vec![
            Frame::bulk("REPLCONF"),
            Frame::bulk("ACK"),
            Frame::bulk(offset.to_string()),
        ]);
        self.send_command(stream, &ack).await?;
        Ok(())
    }

    /// Apply a replicated command
    ///
    /// Note: Offset tracking is done in handle_streamed_command before calling this.
    async fn apply_command(&self, frame: Frame) -> Result<(), ReplicationError> {
        // Parse and execute command
        match Command::from_frame(frame.clone()) {
            Ok(cmd) => {
                // Execute on database 0 (SELECT commands will update this)
                let _response = self.executor.execute(cmd, 0).await;
                debug!("Applied replicated command");
            }
            Err(e) => {
                warn!("Failed to parse replicated command: {}", e);
            }
        }

        Ok(())
    }

    /// Send a command to the master
    async fn send_command(
        &self,
        stream: &mut TcpStream,
        frame: &Frame,
    ) -> Result<(), ReplicationError> {
        let mut buf = BytesMut::new();
        encode_frame(frame, &mut buf);
        stream.write_all(&buf).await?;
        Ok(())
    }

    /// Read a response from the master
    async fn read_response(
        &self,
        stream: &mut TcpStream,
        buf: &mut BytesMut,
    ) -> Result<Frame, ReplicationError> {
        loop {
            if let Some(frame) = parse_frame(buf)
                .map_err(|e| ReplicationError::Protocol(format!("Parse error: {}", e)))?
            {
                return Ok(frame);
            }

            let n = stream.read_buf(buf).await?;
            if n == 0 {
                return Err(ReplicationError::Protocol("Connection closed".to_string()));
            }
        }
    }

    /// Get current connection state
    pub async fn connection_state(&self) -> ReplicaState {
        *self.connection_state.read().await
    }

    /// Get replication info for INFO command
    pub async fn get_info(&self) -> String {
        let state = self.connection_state().await;
        let master_replid = self.master_replid.read().await;
        let offset = self.state.repl_offset();

        format!(
            "role:slave\r\n\
             master_host:{}\r\n\
             master_port:{}\r\n\
             master_link_status:{}\r\n\
             master_replid:{}\r\n\
             master_repl_offset:{}\r\n",
            self.config.master_addr.ip(),
            self.config.master_addr.port(),
            if state == ReplicaState::Streaming {
                "up"
            } else {
                "down"
            },
            master_replid.as_ref().map(|r| r.as_str()).unwrap_or("?"),
            offset
        )
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::auth::Acl;
    use crate::commands::BlockingListManager;
    use crate::runtime::SubscriptionManager;
    use bytes::Bytes;

    /// Helper to create a test replica instance
    fn create_test_replica() -> (ReplicationReplica, Arc<super::super::ReplicationState>) {
        let store = Arc::new(Store::new(16));
        let state = Arc::new(super::super::ReplicationState::new());
        let pubsub = Arc::new(SubscriptionManager::new());
        let acl = Arc::new(Acl::new());
        let blocking_manager = Arc::new(BlockingListManager::new());
        let blocking_stream_manager =
            Arc::new(crate::commands::blocking::BlockingStreamManager::new());
        let blocking_zset_manager =
            Arc::new(crate::commands::blocking::BlockingSortedSetManager::new());
        let config = ReplicaConfig {
            master_addr: "127.0.0.1:6379".parse().unwrap(),
            listening_port: 6380,
            reconnect_delay: Duration::from_secs(1),
            max_reconnect_attempts: 0,
            backoff_base: Duration::from_millis(1000),
            backoff_max: Duration::from_millis(30000),
        };

        let replica = ReplicationReplica::new(
            store,
            state.clone(),
            config,
            pubsub,
            acl,
            blocking_manager,
            blocking_stream_manager,
            blocking_zset_manager,
        );
        (replica, state)
    }

    // ============================================================================
    // ReplicaState Tests
    // ============================================================================

    #[test]
    fn test_replica_state_enum_equality() {
        assert_eq!(ReplicaState::Disconnected, ReplicaState::Disconnected);
        assert_eq!(ReplicaState::Connecting, ReplicaState::Connecting);
        assert_eq!(ReplicaState::Handshake, ReplicaState::Handshake);
        assert_eq!(ReplicaState::WaitingRdb, ReplicaState::WaitingRdb);
        assert_eq!(ReplicaState::ReceivingRdb, ReplicaState::ReceivingRdb);
        assert_eq!(ReplicaState::Streaming, ReplicaState::Streaming);
        assert_ne!(ReplicaState::Disconnected, ReplicaState::Streaming);
    }

    #[test]
    fn test_replica_state_debug_format() {
        assert_eq!(format!("{:?}", ReplicaState::Disconnected), "Disconnected");
        assert_eq!(format!("{:?}", ReplicaState::Connecting), "Connecting");
        assert_eq!(format!("{:?}", ReplicaState::Handshake), "Handshake");
        assert_eq!(format!("{:?}", ReplicaState::WaitingRdb), "WaitingRdb");
        assert_eq!(format!("{:?}", ReplicaState::ReceivingRdb), "ReceivingRdb");
        assert_eq!(format!("{:?}", ReplicaState::Streaming), "Streaming");
    }

    #[test]
    fn test_replica_state_clone() {
        let state = ReplicaState::Streaming;
        let cloned = state;
        assert_eq!(state, cloned);
    }

    #[test]
    fn test_replica_state_copy() {
        let state = ReplicaState::Handshake;
        let copied: ReplicaState = state;
        assert_eq!(state, copied);
    }

    // ============================================================================
    // ReplicaConfig Tests
    // ============================================================================

    #[test]
    fn test_replica_config_creation() {
        let config = ReplicaConfig {
            master_addr: "192.168.1.100:6379".parse().unwrap(),
            listening_port: 6380,
            reconnect_delay: Duration::from_secs(5),
            max_reconnect_attempts: 0,
            backoff_base: Duration::from_millis(1000),
            backoff_max: Duration::from_millis(30000),
        };

        assert_eq!(config.master_addr.port(), 6379);
        assert_eq!(config.listening_port, 6380);
        assert_eq!(config.reconnect_delay, Duration::from_secs(5));
    }

    #[test]
    fn test_replica_config_debug() {
        let config = ReplicaConfig {
            master_addr: "127.0.0.1:6379".parse().unwrap(),
            listening_port: 6380,
            reconnect_delay: Duration::from_millis(500),
            max_reconnect_attempts: 0,
            backoff_base: Duration::from_millis(1000),
            backoff_max: Duration::from_millis(30000),
        };
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("127.0.0.1:6379"));
        assert!(debug_str.contains("6380"));
    }

    #[test]
    fn test_replica_config_clone() {
        let config = ReplicaConfig {
            master_addr: "127.0.0.1:6379".parse().unwrap(),
            listening_port: 6380,
            reconnect_delay: Duration::from_secs(1),
            max_reconnect_attempts: 0,
            backoff_base: Duration::from_millis(1000),
            backoff_max: Duration::from_millis(30000),
        };
        let cloned = config.clone();
        assert_eq!(config.master_addr, cloned.master_addr);
        assert_eq!(config.listening_port, cloned.listening_port);
        assert_eq!(config.reconnect_delay, cloned.reconnect_delay);
    }

    // ============================================================================
    // ReplicationReplica Initial State Tests
    // ============================================================================

    #[tokio::test]
    async fn test_replica_state() {
        let (replica, _) = create_test_replica();
        assert_eq!(replica.connection_state().await, ReplicaState::Disconnected);
    }

    #[tokio::test]
    async fn test_replica_initial_master_replid_none() {
        let (replica, _) = create_test_replica();
        let replid = replica.master_replid.read().await;
        assert!(replid.is_none());
    }

    #[tokio::test]
    async fn test_replica_initial_offset_zero() {
        let (_, state) = create_test_replica();
        assert_eq!(state.repl_offset(), 0);
    }

    // ============================================================================
    // get_info Tests
    // ============================================================================

    #[tokio::test]
    async fn test_get_info_disconnected() {
        let (replica, _) = create_test_replica();
        let info = replica.get_info().await;

        assert!(info.contains("role:slave"));
        assert!(info.contains("master_host:127.0.0.1"));
        assert!(info.contains("master_port:6379"));
        assert!(info.contains("master_link_status:down"));
        assert!(info.contains("master_replid:?"));
        assert!(info.contains("master_repl_offset:0"));
    }

    #[tokio::test]
    async fn test_get_info_streaming() {
        let (replica, state) = create_test_replica();

        // Simulate connected state
        *replica.connection_state.write().await = ReplicaState::Streaming;
        let replid = ReplicationId::new();
        *replica.master_replid.write().await = Some(replid.clone());
        state.set_offset(12345);

        let info = replica.get_info().await;

        assert!(info.contains("role:slave"));
        assert!(info.contains("master_link_status:up"));
        assert!(info.contains(&format!("master_replid:{}", replid.as_str())));
        assert!(info.contains("master_repl_offset:12345"));
    }

    #[tokio::test]
    async fn test_get_info_format() {
        let (replica, _) = create_test_replica();
        let info = replica.get_info().await;

        // Verify each line ends with \r\n
        for line in info.lines() {
            assert!(line.contains(':'), "Each line should have key:value format");
        }

        // Verify all expected fields are present
        assert!(info.contains("role:"));
        assert!(info.contains("master_host:"));
        assert!(info.contains("master_port:"));
        assert!(info.contains("master_link_status:"));
        assert!(info.contains("master_replid:"));
        assert!(info.contains("master_repl_offset:"));
    }

    // ============================================================================
    // State Transition Tests
    // ============================================================================

    #[tokio::test]
    async fn test_connection_state_transition_to_connecting() {
        let (replica, _) = create_test_replica();

        assert_eq!(replica.connection_state().await, ReplicaState::Disconnected);
        *replica.connection_state.write().await = ReplicaState::Connecting;
        assert_eq!(replica.connection_state().await, ReplicaState::Connecting);
    }

    #[tokio::test]
    async fn test_connection_state_transition_to_handshake() {
        let (replica, _) = create_test_replica();

        *replica.connection_state.write().await = ReplicaState::Handshake;
        assert_eq!(replica.connection_state().await, ReplicaState::Handshake);
    }

    #[tokio::test]
    async fn test_connection_state_transition_to_waiting_rdb() {
        let (replica, _) = create_test_replica();

        *replica.connection_state.write().await = ReplicaState::WaitingRdb;
        assert_eq!(replica.connection_state().await, ReplicaState::WaitingRdb);
    }

    #[tokio::test]
    async fn test_connection_state_transition_to_receiving_rdb() {
        let (replica, _) = create_test_replica();

        *replica.connection_state.write().await = ReplicaState::ReceivingRdb;
        assert_eq!(replica.connection_state().await, ReplicaState::ReceivingRdb);
    }

    #[tokio::test]
    async fn test_connection_state_transition_to_streaming() {
        let (replica, _) = create_test_replica();

        *replica.connection_state.write().await = ReplicaState::Streaming;
        assert_eq!(replica.connection_state().await, ReplicaState::Streaming);
    }

    #[tokio::test]
    async fn test_connection_state_transition_back_to_disconnected() {
        let (replica, _) = create_test_replica();

        *replica.connection_state.write().await = ReplicaState::Streaming;
        *replica.connection_state.write().await = ReplicaState::Disconnected;
        assert_eq!(replica.connection_state().await, ReplicaState::Disconnected);
    }

    // ============================================================================
    // Replication Offset Tests
    // ============================================================================

    #[tokio::test]
    async fn test_offset_increment() {
        let (_, state) = create_test_replica();

        assert_eq!(state.repl_offset(), 0);
        state.increment_offset(100);
        assert_eq!(state.repl_offset(), 100);
        state.increment_offset(50);
        assert_eq!(state.repl_offset(), 150);
    }

    #[tokio::test]
    async fn test_offset_set() {
        let (_, state) = create_test_replica();

        state.set_offset(5000);
        assert_eq!(state.repl_offset(), 5000);
    }

    // ============================================================================
    // Master Replid Tests
    // ============================================================================

    #[tokio::test]
    async fn test_master_replid_set() {
        let (replica, state) = create_test_replica();

        let replid = ReplicationId::new();
        *replica.master_replid.write().await = Some(replid.clone());
        state.set_master_replid(replid.clone()).await;

        let stored = replica.master_replid.read().await;
        assert!(stored.is_some());
        assert_eq!(stored.as_ref().unwrap().as_str(), replid.as_str());
    }

    #[tokio::test]
    async fn test_master_replid_format() {
        let replid = ReplicationId::new();
        let id_str = replid.as_str();

        // Ferrite uses 48-character replid: 32 hex (timestamp) + 16 hex (random)
        assert_eq!(id_str.len(), 48);
        // Should be hexadecimal
        assert!(id_str.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[tokio::test]
    async fn test_master_replid_from_string() {
        let original = "abcdef0123456789abcdef0123456789abcdef01";
        let replid = ReplicationId::from_string(original);
        assert_eq!(replid.as_str(), original);
    }

    // ============================================================================
    // Frame Parsing Tests (for PSYNC responses)
    // ============================================================================

    #[test]
    fn test_fullresync_response_format() {
        let response = "FULLRESYNC abc123def456abc123def456abc123def456abc1 0";
        let parts: Vec<&str> = response.split_whitespace().collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0], "FULLRESYNC");
        assert_eq!(parts[1].len(), 40);
        assert!(parts[2].parse::<u64>().is_ok());
    }

    #[test]
    fn test_continue_response_format() {
        let response = "CONTINUE abc123def456abc123def456abc123def456abc1";
        let parts: Vec<&str> = response.split_whitespace().collect();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "CONTINUE");
    }

    #[test]
    fn test_continue_response_without_replid() {
        let response = "CONTINUE";
        let parts: Vec<&str> = response.split_whitespace().collect();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0], "CONTINUE");
    }

    // ============================================================================
    // RDB Header Parsing Tests
    // ============================================================================

    #[test]
    fn test_rdb_length_header_parse() {
        let header = "$1024\r\n";
        assert!(header.starts_with('$'));

        let len_part = &header[1..header.len() - 2]; // Remove $ and \r\n
        let len: usize = len_part.parse().unwrap();
        assert_eq!(len, 1024);
    }

    #[test]
    fn test_rdb_eof_marker_header() {
        let header = "$EOF:abc123\r\n";
        assert!(header.starts_with('$'));

        let marker_part = &header[1..header.len() - 2];
        assert!(marker_part.starts_with("EOF:"));
        let marker = &marker_part[4..];
        assert_eq!(marker, "abc123");
    }

    // ============================================================================
    // REPLCONF Command Tests
    // ============================================================================

    #[test]
    fn test_replconf_ack_frame_format() {
        let offset = 12345_u64;
        let ack = Frame::array(vec![
            Frame::bulk("REPLCONF"),
            Frame::bulk("ACK"),
            Frame::bulk(offset.to_string()),
        ]);

        if let Frame::Array(Some(arr)) = ack {
            assert_eq!(arr.len(), 3);
            if let Frame::Bulk(Some(cmd)) = &arr[0] {
                assert_eq!(cmd.as_ref(), b"REPLCONF");
            }
            if let Frame::Bulk(Some(subcmd)) = &arr[1] {
                assert_eq!(subcmd.as_ref(), b"ACK");
            }
            if let Frame::Bulk(Some(off)) = &arr[2] {
                assert_eq!(off.as_ref(), b"12345");
            }
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_replconf_getack_detection() {
        let getack = Frame::array(vec![
            Frame::bulk("REPLCONF"),
            Frame::bulk("GETACK"),
            Frame::bulk("*"),
        ]);

        if let Frame::Array(Some(ref arr)) = getack {
            if let (Frame::Bulk(Some(cmd)), Frame::Bulk(Some(subcmd))) = (&arr[0], &arr[1]) {
                let cmd_upper = String::from_utf8_lossy(cmd).to_uppercase();
                let subcmd_upper = String::from_utf8_lossy(subcmd).to_uppercase();
                assert_eq!(cmd_upper, "REPLCONF");
                assert_eq!(subcmd_upper, "GETACK");
            }
        }
    }

    // ============================================================================
    // Concurrency Tests
    // ============================================================================

    #[tokio::test]
    async fn test_concurrent_state_reads() {
        let (replica, _) = create_test_replica();
        let replica = Arc::new(replica);

        // Spawn multiple concurrent reads
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let r = replica.clone();
                tokio::spawn(async move { r.connection_state().await })
            })
            .collect();

        for handle in handles {
            let state = handle.await.unwrap();
            assert_eq!(state, ReplicaState::Disconnected);
        }
    }

    #[tokio::test]
    async fn test_concurrent_offset_increments() {
        let (_, state) = create_test_replica();
        let state = Arc::new(state);

        // Spawn multiple concurrent increments
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let s = state.clone();
                tokio::spawn(async move {
                    s.increment_offset(10);
                })
            })
            .collect();

        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(state.repl_offset(), 100);
    }

    // ============================================================================
    // Edge Cases
    // ============================================================================

    #[tokio::test]
    async fn test_get_info_with_ipv4_address() {
        let store = Arc::new(Store::new(16));
        let state = Arc::new(super::super::ReplicationState::new());
        let pubsub = Arc::new(SubscriptionManager::new());
        let acl = Arc::new(Acl::new());
        let blocking_manager = Arc::new(BlockingListManager::new());
        let blocking_stream_manager =
            Arc::new(crate::commands::blocking::BlockingStreamManager::new());
        let blocking_zset_manager =
            Arc::new(crate::commands::blocking::BlockingSortedSetManager::new());
        let config = ReplicaConfig {
            master_addr: "10.0.0.1:6379".parse().unwrap(),
            listening_port: 6380,
            reconnect_delay: Duration::from_secs(1),
            max_reconnect_attempts: 0,
            backoff_base: Duration::from_millis(1000),
            backoff_max: Duration::from_millis(30000),
        };

        let replica = ReplicationReplica::new(
            store,
            state,
            config,
            pubsub,
            acl,
            blocking_manager,
            blocking_stream_manager,
            blocking_zset_manager,
        );

        let info = replica.get_info().await;
        assert!(info.contains("master_host:10.0.0.1"));
    }

    #[tokio::test]
    async fn test_large_offset_value() {
        let (_, state) = create_test_replica();

        let large_offset = u64::MAX - 1000;
        state.set_offset(large_offset);
        assert_eq!(state.repl_offset(), large_offset);

        // Increment without overflow check (should wrap or handle gracefully)
        state.increment_offset(500);
        assert_eq!(state.repl_offset(), large_offset + 500);
    }

    #[test]
    fn test_replica_config_with_different_ports() {
        let config1 = ReplicaConfig {
            master_addr: "127.0.0.1:6379".parse().unwrap(),
            listening_port: 6380,
            reconnect_delay: Duration::from_secs(1),
            max_reconnect_attempts: 0,
            backoff_base: Duration::from_millis(1000),
            backoff_max: Duration::from_millis(30000),
        };

        let config2 = ReplicaConfig {
            master_addr: "127.0.0.1:7000".parse().unwrap(),
            listening_port: 7001,
            reconnect_delay: Duration::from_millis(100),
            max_reconnect_attempts: 0,
            backoff_base: Duration::from_millis(1000),
            backoff_max: Duration::from_millis(30000),
        };

        assert_ne!(config1.master_addr.port(), config2.master_addr.port());
        assert_ne!(config1.listening_port, config2.listening_port);
    }

    #[test]
    fn test_handshake_ping_frame() {
        let ping = Frame::array(vec![Frame::bulk("PING")]);

        if let Frame::Array(Some(arr)) = ping {
            assert_eq!(arr.len(), 1);
            if let Frame::Bulk(Some(cmd)) = &arr[0] {
                assert_eq!(cmd.as_ref(), b"PING");
            }
        }
    }

    #[test]
    fn test_handshake_replconf_listening_port_frame() {
        let port = 6380_u16;
        let replconf = Frame::array(vec![
            Frame::bulk("REPLCONF"),
            Frame::bulk("listening-port"),
            Frame::bulk(port.to_string()),
        ]);

        if let Frame::Array(Some(arr)) = replconf {
            assert_eq!(arr.len(), 3);
            if let Frame::Bulk(Some(port_str)) = &arr[2] {
                assert_eq!(port_str.as_ref(), b"6380");
            }
        }
    }

    #[test]
    fn test_handshake_replconf_capa_frame() {
        let replconf_capa = Frame::array(vec![
            Frame::bulk("REPLCONF"),
            Frame::bulk("capa"),
            Frame::bulk("eof"),
            Frame::bulk("capa"),
            Frame::bulk("psync2"),
        ]);

        if let Frame::Array(Some(arr)) = replconf_capa {
            assert_eq!(arr.len(), 5);
        }
    }

    #[test]
    fn test_psync_first_sync_frame() {
        let psync = Frame::array(vec![
            Frame::bulk("PSYNC"),
            Frame::bulk("?"),
            Frame::bulk("-1"),
        ]);

        if let Frame::Array(Some(arr)) = psync {
            assert_eq!(arr.len(), 3);
            if let Frame::Bulk(Some(replid)) = &arr[1] {
                assert_eq!(replid.as_ref(), b"?");
            }
            if let Frame::Bulk(Some(offset)) = &arr[2] {
                assert_eq!(offset.as_ref(), b"-1");
            }
        }
    }

    #[test]
    fn test_psync_resync_frame() {
        let replid = "abcdef0123456789abcdef0123456789abcdef01";
        let offset = 12345_i64;
        let psync = Frame::array(vec![
            Frame::bulk("PSYNC"),
            Frame::bulk(replid),
            Frame::bulk(offset.to_string()),
        ]);

        if let Frame::Array(Some(arr)) = psync {
            assert_eq!(arr.len(), 3);
            if let Frame::Bulk(Some(id)) = &arr[1] {
                assert_eq!(id.len(), 40);
            }
        }
    }

    // ============================================================================
    // State Machine Comprehensive Tests
    // ============================================================================

    #[tokio::test]
    async fn test_state_machine_full_progression() {
        let (replica, _) = create_test_replica();

        // Start disconnected
        assert_eq!(replica.connection_state().await, ReplicaState::Disconnected);

        // Progress through connection states
        *replica.connection_state.write().await = ReplicaState::Connecting;
        assert_eq!(replica.connection_state().await, ReplicaState::Connecting);

        *replica.connection_state.write().await = ReplicaState::Handshake;
        assert_eq!(replica.connection_state().await, ReplicaState::Handshake);

        *replica.connection_state.write().await = ReplicaState::WaitingRdb;
        assert_eq!(replica.connection_state().await, ReplicaState::WaitingRdb);

        *replica.connection_state.write().await = ReplicaState::ReceivingRdb;
        assert_eq!(replica.connection_state().await, ReplicaState::ReceivingRdb);

        *replica.connection_state.write().await = ReplicaState::Streaming;
        assert_eq!(replica.connection_state().await, ReplicaState::Streaming);
    }

    #[tokio::test]
    async fn test_state_machine_failure_recovery() {
        let (replica, _) = create_test_replica();

        // Simulate connection failure during handshake
        *replica.connection_state.write().await = ReplicaState::Handshake;
        assert_eq!(replica.connection_state().await, ReplicaState::Handshake);

        // Failed - go back to disconnected
        *replica.connection_state.write().await = ReplicaState::Disconnected;
        assert_eq!(replica.connection_state().await, ReplicaState::Disconnected);

        // Retry connection
        *replica.connection_state.write().await = ReplicaState::Connecting;
        assert_eq!(replica.connection_state().await, ReplicaState::Connecting);
    }

    #[tokio::test]
    async fn test_state_machine_rdb_transfer_interruption() {
        let (replica, _) = create_test_replica();

        // Start RDB transfer
        *replica.connection_state.write().await = ReplicaState::ReceivingRdb;
        assert_eq!(replica.connection_state().await, ReplicaState::ReceivingRdb);

        // Interrupted - back to disconnected
        *replica.connection_state.write().await = ReplicaState::Disconnected;
        assert_eq!(replica.connection_state().await, ReplicaState::Disconnected);
    }

    #[tokio::test]
    async fn test_state_machine_streaming_reconnect() {
        let (replica, _) = create_test_replica();

        // Establish streaming connection
        *replica.connection_state.write().await = ReplicaState::Streaming;
        assert_eq!(replica.connection_state().await, ReplicaState::Streaming);

        // Connection lost during streaming
        *replica.connection_state.write().await = ReplicaState::Disconnected;
        assert_eq!(replica.connection_state().await, ReplicaState::Disconnected);

        // Reconnect and try to resume
        *replica.connection_state.write().await = ReplicaState::Connecting;
        assert_eq!(replica.connection_state().await, ReplicaState::Connecting);
    }

    // ============================================================================
    // RDB Transfer Handling Tests
    // ============================================================================

    #[test]
    fn test_rdb_length_parsing_valid() {
        let data = b"$2048\r\n";
        let s = std::str::from_utf8(&data[1..data.len() - 2]).unwrap();
        let len: usize = s.parse().unwrap();
        assert_eq!(len, 2048);
    }

    #[test]
    fn test_rdb_length_parsing_zero() {
        let data = b"$0\r\n";
        let s = std::str::from_utf8(&data[1..data.len() - 2]).unwrap();
        let len: usize = s.parse().unwrap();
        assert_eq!(len, 0);
    }

    #[test]
    fn test_rdb_length_parsing_large() {
        let data = b"$10485760\r\n"; // 10MB
        let s = std::str::from_utf8(&data[1..data.len() - 2]).unwrap();
        let len: usize = s.parse().unwrap();
        assert_eq!(len, 10485760);
    }

    #[test]
    fn test_rdb_eof_marker_detection() {
        let header = b"$EOF:xyz789abc\r\n";
        let s = std::str::from_utf8(&header[1..header.len() - 2]).unwrap();
        assert!(s.starts_with("EOF:"));
        let marker = &s[4..];
        assert_eq!(marker, "xyz789abc");
    }

    #[test]
    fn test_rdb_eof_marker_empty() {
        let header = b"$EOF:\r\n";
        let s = std::str::from_utf8(&header[1..header.len() - 2]).unwrap();
        assert!(s.starts_with("EOF:"));
        let marker = &s[4..];
        assert_eq!(marker, "");
    }

    #[test]
    fn test_rdb_streaming_marker_match() {
        let buffer = b"some data here xyz789abc more data";
        let marker = b"xyz789abc";
        let pos = buffer.windows(marker.len()).position(|w| w == marker);
        assert!(pos.is_some());
        assert_eq!(pos.unwrap(), 15);
    }

    #[test]
    fn test_rdb_streaming_marker_not_found() {
        let buffer = b"some data here without marker";
        let marker = b"xyz789abc";
        let pos = buffer.windows(marker.len()).position(|w| w == marker);
        assert!(pos.is_none());
    }

    // ============================================================================
    // Command Replication Tests
    // ============================================================================

    #[tokio::test]
    async fn test_apply_command_set() {
        let (replica, _) = create_test_replica();

        let set_cmd = Frame::array(vec![
            Frame::bulk("SET"),
            Frame::bulk("mykey"),
            Frame::bulk("myvalue"),
        ]);

        let result = replica.apply_command(set_cmd).await;
        assert!(result.is_ok());

        // Verify the key was set in the store
        let value = replica.store.get(0, &Bytes::from("mykey"));
        assert!(value.is_some());
    }

    #[tokio::test]
    async fn test_apply_command_del() {
        let (replica, _) = create_test_replica();

        // First set a key
        let set_cmd = Frame::array(vec![
            Frame::bulk("SET"),
            Frame::bulk("delkey"),
            Frame::bulk("value"),
        ]);
        replica.apply_command(set_cmd).await.unwrap();

        // Then delete it
        let del_cmd = Frame::array(vec![Frame::bulk("DEL"), Frame::bulk("delkey")]);
        let result = replica.apply_command(del_cmd).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_apply_command_invalid() {
        let (replica, _) = create_test_replica();

        // Invalid command structure
        let invalid_cmd = Frame::array(vec![]);
        let result = replica.apply_command(invalid_cmd).await;
        // Should not error - just warn and continue
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_replconf_getack_detection_uppercase() {
        let getack = Frame::array(vec![
            Frame::bulk("REPLCONF"),
            Frame::bulk("GETACK"),
            Frame::bulk("*"),
        ]);

        if let Frame::Array(Some(ref arr)) = getack {
            if arr.len() >= 2 {
                if let (Frame::Bulk(Some(cmd)), Frame::Bulk(Some(subcmd))) = (&arr[0], &arr[1]) {
                    let cmd_upper = String::from_utf8_lossy(cmd).to_uppercase();
                    let subcmd_upper = String::from_utf8_lossy(subcmd).to_uppercase();
                    assert_eq!(cmd_upper, "REPLCONF");
                    assert_eq!(subcmd_upper, "GETACK");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_replconf_getack_detection_lowercase() {
        let getack = Frame::array(vec![
            Frame::bulk("replconf"),
            Frame::bulk("getack"),
            Frame::bulk("*"),
        ]);

        if let Frame::Array(Some(ref arr)) = getack {
            if arr.len() >= 2 {
                if let (Frame::Bulk(Some(cmd)), Frame::Bulk(Some(subcmd))) = (&arr[0], &arr[1]) {
                    let cmd_upper = String::from_utf8_lossy(cmd).to_uppercase();
                    let subcmd_upper = String::from_utf8_lossy(subcmd).to_uppercase();
                    assert_eq!(cmd_upper, "REPLCONF");
                    assert_eq!(subcmd_upper, "GETACK");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_replconf_getack_detection_mixed_case() {
        let getack = Frame::array(vec![
            Frame::bulk("RePlConF"),
            Frame::bulk("GetAcK"),
            Frame::bulk("*"),
        ]);

        if let Frame::Array(Some(ref arr)) = getack {
            if arr.len() >= 2 {
                if let (Frame::Bulk(Some(cmd)), Frame::Bulk(Some(subcmd))) = (&arr[0], &arr[1]) {
                    let cmd_upper = String::from_utf8_lossy(cmd).to_uppercase();
                    let subcmd_upper = String::from_utf8_lossy(subcmd).to_uppercase();
                    assert_eq!(cmd_upper, "REPLCONF");
                    assert_eq!(subcmd_upper, "GETACK");
                }
            }
        }
    }

    // ============================================================================
    // Network Failure Recovery Tests
    // ============================================================================

    #[tokio::test]
    async fn test_network_failure_during_handshake() {
        let (replica, _) = create_test_replica();

        // Start handshake
        *replica.connection_state.write().await = ReplicaState::Handshake;

        // Simulate failure
        *replica.connection_state.write().await = ReplicaState::Disconnected;

        // Verify state is disconnected
        assert_eq!(replica.connection_state().await, ReplicaState::Disconnected);
    }

    #[tokio::test]
    async fn test_network_failure_during_rdb_transfer() {
        let (replica, state) = create_test_replica();

        // Start RDB transfer with some offset
        *replica.connection_state.write().await = ReplicaState::ReceivingRdb;
        state.set_offset(1000);

        // Simulate failure
        *replica.connection_state.write().await = ReplicaState::Disconnected;

        // Offset should remain (for potential resume)
        assert_eq!(state.repl_offset(), 1000);
    }

    #[tokio::test]
    async fn test_network_failure_during_streaming() {
        let (replica, state) = create_test_replica();

        // Establish streaming with replid and offset
        *replica.connection_state.write().await = ReplicaState::Streaming;
        let replid = ReplicationId::new();
        *replica.master_replid.write().await = Some(replid.clone());
        state.set_offset(5000);

        // Simulate failure
        *replica.connection_state.write().await = ReplicaState::Disconnected;

        // State should be preserved for reconnection
        assert_eq!(state.repl_offset(), 5000);
        let stored_replid = replica.master_replid.read().await;
        assert!(stored_replid.is_some());
    }

    // ============================================================================
    // Reconnection Logic Tests
    // ============================================================================

    #[test]
    fn test_reconnect_delay_configuration() {
        let config = ReplicaConfig {
            master_addr: "127.0.0.1:6379".parse().unwrap(),
            listening_port: 6380,
            reconnect_delay: Duration::from_millis(100),
            max_reconnect_attempts: 0,
            backoff_base: Duration::from_millis(1000),
            backoff_max: Duration::from_millis(30000),
        };
        assert_eq!(config.reconnect_delay, Duration::from_millis(100));
    }

    #[test]
    fn test_reconnect_delay_default() {
        let config = ReplicaConfig {
            master_addr: "127.0.0.1:6379".parse().unwrap(),
            listening_port: 6380,
            reconnect_delay: Duration::from_secs(1),
            max_reconnect_attempts: 0,
            backoff_base: Duration::from_millis(1000),
            backoff_max: Duration::from_millis(30000),
        };
        assert_eq!(config.reconnect_delay, Duration::from_secs(1));
    }

    #[test]
    fn test_reconnect_delay_long() {
        let config = ReplicaConfig {
            master_addr: "127.0.0.1:6379".parse().unwrap(),
            listening_port: 6380,
            reconnect_delay: Duration::from_secs(30),
            max_reconnect_attempts: 0,
            backoff_base: Duration::from_millis(1000),
            backoff_max: Duration::from_millis(30000),
        };
        assert_eq!(config.reconnect_delay, Duration::from_secs(30));
    }

    // ============================================================================
    // Offset Tracking Tests (Advanced)
    // ============================================================================

    #[tokio::test]
    async fn test_offset_tracking_multiple_commands() {
        let (_, state) = create_test_replica();

        // Simulate multiple command replications
        state.increment_offset(50); // First command
        state.increment_offset(75); // Second command
        state.increment_offset(100); // Third command

        assert_eq!(state.repl_offset(), 225);
    }

    #[tokio::test]
    async fn test_offset_tracking_getack_not_counted() {
        let (_, state) = create_test_replica();

        state.set_offset(1000);

        // GETACK should not increment offset
        // (This is handled in handle_streamed_command by early return)
        let initial = state.repl_offset();
        assert_eq!(initial, 1000);

        // Regular command increments
        state.increment_offset(50);
        assert_eq!(state.repl_offset(), 1050);
    }

    #[tokio::test]
    async fn test_offset_tracking_after_full_resync() {
        let (_, state) = create_test_replica();

        // After FULLRESYNC, offset is set to master's value
        state.set_offset(0);
        assert_eq!(state.repl_offset(), 0);

        // Then commands increment from there
        state.increment_offset(100);
        assert_eq!(state.repl_offset(), 100);
    }

    #[tokio::test]
    async fn test_offset_tracking_after_partial_resync() {
        let (_, state) = create_test_replica();

        // After CONTINUE, offset continues from previous value
        state.set_offset(5000);
        assert_eq!(state.repl_offset(), 5000);

        // Commands continue incrementing
        state.increment_offset(50);
        assert_eq!(state.repl_offset(), 5050);
    }

    #[tokio::test]
    async fn test_offset_tracking_zero() {
        let (_, state) = create_test_replica();

        state.increment_offset(0);
        assert_eq!(state.repl_offset(), 0);
    }

    #[tokio::test]
    async fn test_offset_tracking_large_increment() {
        let (_, state) = create_test_replica();

        // Large bulk transfer
        state.increment_offset(1_000_000);
        assert_eq!(state.repl_offset(), 1_000_000);
    }

    // ============================================================================
    // PSYNC Response Parsing Tests
    // ============================================================================

    #[test]
    fn test_fullresync_parse_valid() {
        let response = "FULLRESYNC abc123def456abc123def456abc123def456abc1 12345";
        let parts: Vec<&str> = response.split_whitespace().collect();

        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0], "FULLRESYNC");

        let replid = parts[1];
        assert_eq!(replid.len(), 40);

        let offset: u64 = parts[2].parse().unwrap();
        assert_eq!(offset, 12345);
    }

    #[test]
    fn test_fullresync_parse_zero_offset() {
        let response = "FULLRESYNC abc123def456abc123def456abc123def456abc1 0";
        let parts: Vec<&str> = response.split_whitespace().collect();

        assert_eq!(parts.len(), 3);
        let offset: u64 = parts[2].parse().unwrap();
        assert_eq!(offset, 0);
    }

    #[test]
    fn test_continue_parse_with_replid() {
        let response = "CONTINUE abc123def456abc123def456abc123def456abc1";
        let parts: Vec<&str> = response.split_whitespace().collect();

        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "CONTINUE");
        assert_eq!(parts[1].len(), 40);
    }

    #[test]
    fn test_continue_parse_without_replid() {
        let response = "CONTINUE";
        let parts: Vec<&str> = response.split_whitespace().collect();

        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0], "CONTINUE");
    }

    #[test]
    fn test_fullresync_invalid_format() {
        let response = "FULLRESYNC abc123"; // Missing offset
        let parts: Vec<&str> = response.split_whitespace().collect();

        assert_eq!(parts.len(), 2); // Should detect invalid format
    }

    // ============================================================================
    // Master Replid Tracking Tests
    // ============================================================================

    #[tokio::test]
    async fn test_master_replid_initial_none() {
        let (replica, _) = create_test_replica();
        let replid = replica.master_replid.read().await;
        assert!(replid.is_none());
    }

    #[tokio::test]
    async fn test_master_replid_set_and_retrieve() {
        let (replica, state) = create_test_replica();

        let replid = ReplicationId::new();
        *replica.master_replid.write().await = Some(replid.clone());
        state.set_master_replid(replid.clone()).await;

        let stored = replica.master_replid.read().await;
        assert!(stored.is_some());
        assert_eq!(stored.as_ref().unwrap().as_str(), replid.as_str());
    }

    #[tokio::test]
    async fn test_master_replid_update() {
        let (replica, state) = create_test_replica();

        // Set initial replid
        let replid1 = ReplicationId::new();
        *replica.master_replid.write().await = Some(replid1.clone());
        state.set_master_replid(replid1).await;

        // Update to new replid (after master failover)
        let replid2 = ReplicationId::new();
        *replica.master_replid.write().await = Some(replid2.clone());
        state.set_master_replid(replid2.clone()).await;

        let stored = replica.master_replid.read().await;
        assert_eq!(stored.as_ref().unwrap().as_str(), replid2.as_str());
    }

    #[tokio::test]
    async fn test_master_replid_from_string_custom() {
        let custom_id = "0000000000000000000000000000000000000000";
        let replid = ReplicationId::from_string(custom_id);
        assert_eq!(replid.as_str(), custom_id);
    }

    // ============================================================================
    // Concurrency and Thread Safety Tests
    // ============================================================================

    #[tokio::test]
    async fn test_concurrent_state_and_offset_access() {
        let (replica, state) = create_test_replica();
        let replica = Arc::new(replica);
        let state = Arc::new(state);

        let mut handles = vec![];

        // Spawn readers
        for _ in 0..5 {
            let r = replica.clone();
            let s = state.clone();
            handles.push(tokio::spawn(async move {
                let _ = r.connection_state().await;
                let _ = s.repl_offset();
            }));
        }

        // Spawn writers
        for i in 0..5 {
            let r = replica.clone();
            let s = state.clone();
            handles.push(tokio::spawn(async move {
                *r.connection_state.write().await = ReplicaState::Connecting;
                s.increment_offset(10 * i);
            }));
        }

        // Wait for all tasks
        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_concurrent_master_replid_updates() {
        let (replica, state) = create_test_replica();
        let replica = Arc::new(replica);
        let state = Arc::new(state);

        let mut handles = vec![];

        // Multiple tasks trying to update replid
        for _ in 0..10 {
            let r = replica.clone();
            let s = state.clone();
            handles.push(tokio::spawn(async move {
                let replid = ReplicationId::new();
                *r.master_replid.write().await = Some(replid.clone());
                s.set_master_replid(replid).await;
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // Should have some replid set (last writer wins)
        let replid = replica.master_replid.read().await;
        assert!(replid.is_some());
    }

    // ============================================================================
    // get_info Comprehensive Tests
    // ============================================================================

    #[tokio::test]
    async fn test_get_info_all_states() {
        let (replica, _) = create_test_replica();

        let states = vec![
            (ReplicaState::Disconnected, "down"),
            (ReplicaState::Connecting, "down"),
            (ReplicaState::Handshake, "down"),
            (ReplicaState::WaitingRdb, "down"),
            (ReplicaState::ReceivingRdb, "down"),
            (ReplicaState::Streaming, "up"),
        ];

        for (state, expected_status) in states {
            *replica.connection_state.write().await = state;
            let info = replica.get_info().await;
            assert!(
                info.contains(&format!("master_link_status:{}", expected_status)),
                "State {:?} should have status {}",
                state,
                expected_status
            );
        }
    }

    #[tokio::test]
    async fn test_get_info_with_offset_values() {
        let (replica, state) = create_test_replica();

        let offsets = vec![0, 100, 1000, 10000, 1_000_000];

        for offset in offsets {
            state.set_offset(offset);
            let info = replica.get_info().await;
            assert!(
                info.contains(&format!("master_repl_offset:{}", offset)),
                "Info should contain offset {}",
                offset
            );
        }
    }

    #[tokio::test]
    async fn test_get_info_redis_format_compliance() {
        let (replica, _) = create_test_replica();
        let info = replica.get_info().await;

        // Should use \r\n line endings
        assert!(info.contains("\r\n"));

        // Should have role field
        assert!(info.starts_with("role:"));

        // Check all required fields exist
        let required_fields = vec![
            "role:",
            "master_host:",
            "master_port:",
            "master_link_status:",
            "master_replid:",
            "master_repl_offset:",
        ];

        for field in required_fields {
            assert!(info.contains(field), "Missing required field: {}", field);
        }
    }

    // ============================================================================
    // Edge Cases and Error Scenarios
    // ============================================================================

    #[tokio::test]
    async fn test_empty_command_array() {
        let (replica, _) = create_test_replica();
        let empty_cmd = Frame::array(vec![]);
        let result = replica.apply_command(empty_cmd).await;
        // Should handle gracefully
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_null_frame_handling() {
        let (replica, _) = create_test_replica();
        let null_cmd = Frame::Null;
        let result = replica.apply_command(null_cmd).await;
        // Should handle gracefully
        assert!(result.is_ok());
    }

    #[test]
    fn test_replid_uniqueness() {
        let replid1 = ReplicationId::new();
        let replid2 = ReplicationId::new();

        // Two newly generated replids should be different
        assert_ne!(replid1.as_str(), replid2.as_str());
    }

    #[test]
    fn test_replid_hex_format() {
        let replid = ReplicationId::new();
        let id_str = replid.as_str();

        // Should be valid hexadecimal
        for c in id_str.chars() {
            assert!(
                c.is_ascii_hexdigit(),
                "Character '{}' is not a hex digit",
                c
            );
        }
    }

    #[tokio::test]
    async fn test_replica_with_different_master_addresses() {
        let addresses = vec![
            "127.0.0.1:6379",
            "192.168.1.1:6379",
            "10.0.0.1:7000",
            "172.16.0.1:6380",
        ];

        for addr in addresses {
            let config = ReplicaConfig {
                master_addr: addr.parse().unwrap(),
                listening_port: 6380,
                reconnect_delay: Duration::from_secs(1),
                max_reconnect_attempts: 0,
                backoff_base: Duration::from_millis(1000),
                backoff_max: Duration::from_millis(30000),
            };

            assert_eq!(config.master_addr.to_string(), addr);
        }
    }

    #[test]
    fn test_psync_frame_with_valid_replid() {
        let replid = ReplicationId::new();
        let offset = 1000_i64;

        let psync = Frame::array(vec![
            Frame::bulk("PSYNC"),
            Frame::bulk(replid.as_str().to_string()),
            Frame::bulk(offset.to_string()),
        ]);

        if let Frame::Array(Some(arr)) = psync {
            assert_eq!(arr.len(), 3);
        }
    }

    #[test]
    fn test_command_size_calculation() {
        // Simulating command byte size calculation
        // *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n = 33 bytes
        let command_bytes = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let command_size = command_bytes.len();

        assert_eq!(command_size, 33);
    }

    #[tokio::test]
    async fn test_offset_increment_matches_command_size() {
        let (_, state) = create_test_replica();

        let command_size = 36_u64;
        state.increment_offset(command_size);

        assert_eq!(state.repl_offset(), command_size);
    }

    // ============================================================================
    // Exponential Backoff Tests
    // ============================================================================

    #[test]
    fn test_backoff_delay_attempt_zero() {
        let (replica, _) = create_test_replica();
        let delay = replica.backoff_delay(0);
        assert_eq!(delay, Duration::from_secs(1)); // reconnect_delay
    }

    #[test]
    fn test_backoff_delay_increases() {
        let (replica, _) = create_test_replica();
        let d1 = replica.backoff_delay(1);
        let d3 = replica.backoff_delay(3);
        // d3 should generally be larger than d1 (exponential growth)
        // Account for jitter by checking approximate range
        assert!(d3.as_millis() >= d1.as_millis());
    }

    #[test]
    fn test_backoff_delay_capped() {
        let (replica, _) = create_test_replica();
        let delay = replica.backoff_delay(20);
        // Should be capped at backoff_max (30000ms) plus some jitter
        assert!(delay.as_millis() <= 40000); // generous upper bound with jitter
    }

    #[test]
    fn test_backoff_config_fields() {
        let config = ReplicaConfig {
            master_addr: "127.0.0.1:6379".parse().unwrap(),
            listening_port: 6380,
            reconnect_delay: Duration::from_secs(1),
            max_reconnect_attempts: 5,
            backoff_base: Duration::from_millis(500),
            backoff_max: Duration::from_millis(10000),
        };
        assert_eq!(config.max_reconnect_attempts, 5);
        assert_eq!(config.backoff_base, Duration::from_millis(500));
        assert_eq!(config.backoff_max, Duration::from_millis(10000));
    }
}
