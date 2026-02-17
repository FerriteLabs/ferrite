//! Primary (master) replication logic
//!
//! This module implements the primary side of replication.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use bytes::Bytes;

use crate::persistence::generate_rdb;
use crate::protocol::Frame;
use crate::storage::Store;

use super::stream::{ReplicationCommand, SharedReplicationStream};
use super::{ReplicationId, ReplicationOffset, SharedReplicationState};

/// Default heartbeat interval (10 seconds as per Redis)
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// Default replica timeout (60 seconds)
pub const REPLICA_TIMEOUT: Duration = Duration::from_secs(60);

/// Information about a connected replica
#[derive(Debug)]
pub struct ReplicaInfo {
    /// Replica address
    pub addr: SocketAddr,
    /// Replica's current offset (from ACK)
    pub offset: ReplicationOffset,
    /// Replica's listening port
    pub listening_port: u16,
    /// Whether the replica is online
    pub online: bool,
    /// Last interaction timestamp
    pub last_interaction: Instant,
    /// Last ACK timestamp
    pub last_ack: Instant,
    /// Replica capabilities
    pub capabilities: Vec<String>,
    /// Replication state
    pub state: ReplicaConnectionState,
}

/// Replica connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaConnectionState {
    /// Waiting for PSYNC
    WaitingPsync,
    /// Sending RDB
    SendingRdb,
    /// Online and streaming
    Online,
}

impl ReplicaInfo {
    /// Calculate the replication lag in bytes
    pub fn lag_bytes(&self, master_offset: u64) -> u64 {
        master_offset.saturating_sub(self.offset)
    }

    /// Calculate the replication lag in seconds since last ACK
    pub fn lag_seconds(&self) -> u64 {
        self.last_ack.elapsed().as_secs()
    }

    /// Check if replica is considered stale (no ACK for too long)
    pub fn is_stale(&self) -> bool {
        self.last_ack.elapsed() > REPLICA_TIMEOUT
    }
}

/// Primary replication handler
pub struct ReplicationPrimary {
    /// Shared store for RDB generation
    store: Arc<Store>,
    /// Replication state
    state: SharedReplicationState,
    /// Replication stream for broadcasting
    stream: SharedReplicationStream,
    /// Connected replicas
    replicas: RwLock<HashMap<SocketAddr, ReplicaInfo>>,
}

impl ReplicationPrimary {
    /// Create a new primary replication handler
    pub fn new(
        store: Arc<Store>,
        state: SharedReplicationState,
        stream: SharedReplicationStream,
    ) -> Self {
        Self {
            store,
            state,
            stream,
            replicas: RwLock::new(HashMap::new()),
        }
    }

    /// Handle PSYNC command from a replica
    pub async fn handle_psync(
        &self,
        replid: &str,
        offset: i64,
    ) -> Result<PsyncResponse, ReplicationError> {
        let current_replid = self.state.master_replid().await;
        let current_offset = self.state.repl_offset();

        // Check if we can do a partial resync
        if replid == current_replid.as_str() && offset >= 0 {
            let requested_offset = offset as u64;
            if self.stream.can_partial_sync(requested_offset).await {
                // Partial resync is possible
                return Ok(PsyncResponse::Continue {
                    replid: current_replid,
                    offset: current_offset,
                });
            }
        }

        // Full resync required
        Ok(PsyncResponse::FullResync {
            replid: current_replid,
            offset: current_offset,
        })
    }

    /// Register a new replica
    pub async fn register_replica(&self, addr: SocketAddr, listening_port: u16) {
        self.register_replica_with_caps(addr, listening_port, vec![])
            .await;
    }

    /// Register a new replica with capabilities
    pub async fn register_replica_with_caps(
        &self,
        addr: SocketAddr,
        listening_port: u16,
        capabilities: Vec<String>,
    ) {
        let now = Instant::now();
        let info = ReplicaInfo {
            addr,
            offset: 0,
            listening_port,
            online: true,
            last_interaction: now,
            last_ack: now,
            capabilities,
            state: ReplicaConnectionState::WaitingPsync,
        };
        self.replicas.write().await.insert(addr, info);
        self.state.add_replica();
    }

    /// Update replica state
    pub async fn set_replica_state(&self, addr: &SocketAddr, state: ReplicaConnectionState) {
        if let Some(info) = self.replicas.write().await.get_mut(addr) {
            info.state = state;
            info.last_interaction = Instant::now();
            if state == ReplicaConnectionState::Online {
                info.online = true;
            }
        }
    }

    /// Unregister a replica
    pub async fn unregister_replica(&self, addr: &SocketAddr) {
        if self.replicas.write().await.remove(addr).is_some() {
            self.state.remove_replica();
        }
    }

    /// Update replica offset (ACK)
    pub async fn update_replica_offset(&self, addr: &SocketAddr, offset: u64) {
        if let Some(info) = self.replicas.write().await.get_mut(addr) {
            info.offset = offset;
            let now = Instant::now();
            info.last_interaction = now;
            info.last_ack = now;
        }
    }

    /// Get the number of replicas that have acknowledged up to a given offset
    pub async fn count_replicas_at_offset(&self, min_offset: u64) -> usize {
        self.replicas
            .read()
            .await
            .values()
            .filter(|r| r.online && r.offset >= min_offset)
            .count()
    }

    /// Get minimum replication lag across all replicas (in bytes)
    pub async fn min_replica_lag(&self) -> u64 {
        let master_offset = self.state.repl_offset();
        self.replicas
            .read()
            .await
            .values()
            .filter(|r| r.online)
            .map(|r| r.lag_bytes(master_offset))
            .min()
            .unwrap_or(0)
    }

    /// Get maximum replication lag across all replicas (in bytes)
    pub async fn max_replica_lag(&self) -> u64 {
        let master_offset = self.state.repl_offset();
        self.replicas
            .read()
            .await
            .values()
            .filter(|r| r.online)
            .map(|r| r.lag_bytes(master_offset))
            .max()
            .unwrap_or(0)
    }

    /// Check for stale replicas and mark them offline
    pub async fn check_stale_replicas(&self) {
        let mut replicas = self.replicas.write().await;
        for info in replicas.values_mut() {
            if info.online && info.is_stale() {
                tracing::warn!(
                    "Replica {} is stale (no ACK for {} seconds), marking offline",
                    info.addr,
                    info.lag_seconds()
                );
                info.online = false;
            }
        }
    }

    /// Generate a REPLCONF GETACK command frame
    pub fn make_getack_command() -> Frame {
        Frame::array(vec![
            Frame::bulk("REPLCONF"),
            Frame::bulk("GETACK"),
            Frame::bulk("*"),
        ])
    }

    /// Propagate a command to all replicas
    pub async fn propagate(&self, command: ReplicationCommand) {
        let size = command.encoded_size();
        let offset = self.state.repl_offset();
        self.state.increment_offset(size);
        self.stream.broadcast(command, offset).await;
    }

    /// Generate RDB snapshot for full sync
    pub fn generate_rdb(&self) -> Bytes {
        generate_rdb(&self.store)
    }

    /// Get list of connected replicas
    pub async fn get_replicas(&self) -> Vec<ReplicaInfo> {
        self.replicas.read().await.values().cloned().collect()
    }

    /// Get replication info for INFO command
    pub async fn get_info(&self) -> String {
        let replicas = self.replicas.read().await;
        let replid = self.state.master_replid().await;
        let offset = self.state.repl_offset();

        let online_replicas = replicas.values().filter(|r| r.online).count();

        let mut info = format!(
            "role:master\r\n\
             connected_slaves:{}\r\n\
             master_replid:{}\r\n\
             master_repl_offset:{}\r\n\
             repl_backlog_active:1\r\n\
             repl_backlog_size:{}\r\n\
             repl_backlog_first_byte_offset:0\r\n\
             repl_backlog_histlen:{}\r\n",
            online_replicas,
            replid.as_str(),
            offset,
            1048576, // 1MB default backlog size
            offset
        );

        for (i, (addr, replica)) in replicas.iter().enumerate() {
            let state_str = match replica.state {
                ReplicaConnectionState::WaitingPsync => "wait_bgsave",
                ReplicaConnectionState::SendingRdb => "send_bulk",
                ReplicaConnectionState::Online => "online",
            };
            info.push_str(&format!(
                "slave{}:ip={},port={},state={},offset={},lag={}\r\n",
                i,
                addr.ip(),
                replica.listening_port,
                state_str,
                replica.offset,
                replica.lag_seconds()
            ));
        }

        info
    }
}

/// Response to PSYNC command
#[derive(Debug)]
#[allow(missing_docs)]
pub enum PsyncResponse {
    /// Full resync required
    FullResync { replid: ReplicationId, offset: u64 },
    /// Partial resync possible
    Continue { replid: ReplicationId, offset: u64 },
}

impl PsyncResponse {
    /// Convert to RESP frame
    pub fn to_frame(&self) -> Frame {
        match self {
            PsyncResponse::FullResync { replid, offset } => {
                Frame::simple(format!("FULLRESYNC {} {}", replid.as_str(), offset))
            }
            PsyncResponse::Continue { replid, offset } => {
                Frame::simple(format!("CONTINUE {} {}", replid.as_str(), offset))
            }
        }
    }
}

/// Replication errors
#[derive(Debug, thiserror::Error)]
pub enum ReplicationError {
    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Protocol error
    #[error("Protocol error: {0}")]
    Protocol(String),
}

impl Clone for ReplicaInfo {
    fn clone(&self) -> Self {
        Self {
            addr: self.addr,
            offset: self.offset,
            listening_port: self.listening_port,
            online: self.online,
            last_interaction: self.last_interaction,
            last_ack: self.last_ack,
            capabilities: self.capabilities.clone(),
            state: self.state,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::replication::{ReplicationState, ReplicationStream};

    #[tokio::test]
    async fn test_psync_full_resync() {
        let store = Arc::new(Store::new(16));
        let state = Arc::new(ReplicationState::new());
        let stream = Arc::new(ReplicationStream::new(10000));
        let primary = ReplicationPrimary::new(store, state, stream);

        // First sync should always be full resync
        let response = primary.handle_psync("?", -1).await.unwrap();
        match response {
            PsyncResponse::FullResync { .. } => {}
            _ => panic!("Expected full resync"),
        }
    }

    #[test]
    fn test_generate_rdb() {
        let store = Arc::new(Store::new(16));
        let state = Arc::new(ReplicationState::new());
        let stream = Arc::new(ReplicationStream::new(10000));
        let primary = ReplicationPrimary::new(store, state, stream);

        let rdb = primary.generate_rdb();
        assert!(rdb.starts_with(b"REDIS"));
        assert!(rdb.len() > 9); // At least magic + version
    }
}
