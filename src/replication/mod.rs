//! Replication module
//!
//! This module implements Redis-style replication with primary/replica topology,
//! as well as global active-active geo-replication for multi-region deployments.

pub mod geo;
pub mod failover;
pub mod health;
pub mod offset_persistence;
mod primary;
pub mod psync2;
mod replica;
pub mod slots;
mod stream;

pub use geo::{
    Conflict, ConflictResolution, ConflictResolutionResult, GeoReplicationCommand,
    GeoReplicationConfig, GeoReplicationError, GeoReplicationManager, GeoReplicationMessage,
    GeoReplicationMetrics, GeoReplicationMetricsSnapshot, GeoReplicationMode,
    GeoReplicationSnapshot, PeerRegion, ReadPreference, RegionId, RegionInfo, RegionState,
    RegionStatus, ReplicationOp, ReplicationOpType, SequenceNumber, SiteIdentifier,
};
pub use primary::{
    PsyncResponse, ReplicaConnectionState, ReplicaInfo, ReplicationError, ReplicationPrimary,
    HEARTBEAT_INTERVAL, REPLICA_TIMEOUT,
};
pub use psync2::{
    Psync2BacklogConfig, Psync2Error, Psync2Handler, Psync2Response, SharedPsync2Handler,
    DEFAULT_BACKLOG_SIZE,
};
pub use replica::{ReplicaConfig, ReplicaState, ReplicationReplica};
pub use failover::{FailoverCoordinator, FailoverError, FailoverResult};
pub use health::ReplicationHealthMonitor;
pub use offset_persistence::{
    OffsetPersistenceError, PersistedReplicationMeta, ReplicationOffsetPersistence,
};
pub use slots::{
    ChangeEvent, ChangeOperation, GlobalSlotStats, ReplicationType, SlotConfig, SlotId,
    SlotManager, SlotState, SlotStats, LSN,
};
pub use stream::{
    BacklogEntry, ReplicationBacklog, ReplicationCommand, ReplicationStream,
    SharedReplicationStream,
};

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Replication offset - tracks position in the replication stream
pub type ReplicationOffset = u64;

/// Replication state for a server
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(Default)]
pub enum ReplicationRole {
    /// This server is a primary (master)
    #[default]
    Primary,
    /// This server is a replica (slave)
    Replica,
}


/// Replication ID - unique identifier for a replication stream
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationId(String);

impl ReplicationId {
    /// Generate a new random replication ID
    pub fn new() -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let random: u64 = rand::random();
        Self(format!("{:032x}{:016x}", timestamp, random))
    }

    /// Create from a string
    pub fn from_string(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Get the ID as a string
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for ReplicationId {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared replication state
pub struct ReplicationState {
    /// Current role
    role: RwLock<ReplicationRole>,
    /// Master replication ID
    master_replid: RwLock<ReplicationId>,
    /// Secondary replication ID (for PSYNC2)
    master_replid2: RwLock<Option<ReplicationId>>,
    /// Current replication offset
    master_repl_offset: AtomicU64,
    /// Secondary offset (for PSYNC2)
    second_repl_offset: AtomicU64,
    /// Connected replicas count
    connected_replicas: AtomicU64,
}

impl ReplicationState {
    /// Create new replication state as primary
    pub fn new() -> Self {
        Self {
            role: RwLock::new(ReplicationRole::Primary),
            master_replid: RwLock::new(ReplicationId::new()),
            master_replid2: RwLock::new(None),
            master_repl_offset: AtomicU64::new(0),
            second_repl_offset: AtomicU64::new(-1_i64 as u64),
            connected_replicas: AtomicU64::new(0),
        }
    }

    /// Get current role
    pub async fn role(&self) -> ReplicationRole {
        *self.role.read().await
    }

    /// Set role
    pub async fn set_role(&self, role: ReplicationRole) {
        *self.role.write().await = role;
    }

    /// Get master replication ID
    pub async fn master_replid(&self) -> ReplicationId {
        self.master_replid.read().await.clone()
    }

    /// Set master replication ID
    pub async fn set_master_replid(&self, id: ReplicationId) {
        *self.master_replid.write().await = id;
    }

    /// Get current replication offset
    pub fn repl_offset(&self) -> u64 {
        self.master_repl_offset.load(Ordering::SeqCst)
    }

    /// Increment replication offset
    pub fn increment_offset(&self, bytes: u64) {
        self.master_repl_offset.fetch_add(bytes, Ordering::SeqCst);
    }

    /// Set replication offset
    pub fn set_offset(&self, offset: u64) {
        self.master_repl_offset.store(offset, Ordering::SeqCst);
    }

    /// Get connected replicas count
    pub fn connected_replicas(&self) -> u64 {
        self.connected_replicas.load(Ordering::SeqCst)
    }

    /// Increment connected replicas
    pub fn add_replica(&self) {
        self.connected_replicas.fetch_add(1, Ordering::SeqCst);
    }

    /// Decrement connected replicas
    pub fn remove_replica(&self) {
        self.connected_replicas.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Default for ReplicationState {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared replication state type
pub type SharedReplicationState = Arc<ReplicationState>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_id() {
        let id1 = ReplicationId::new();
        let id2 = ReplicationId::new();
        assert_ne!(id1, id2);
        assert!(!id1.as_str().is_empty());
    }

    #[tokio::test]
    async fn test_replication_state() {
        let state = ReplicationState::new();
        assert_eq!(state.role().await, ReplicationRole::Primary);
        assert_eq!(state.repl_offset(), 0);

        state.increment_offset(100);
        assert_eq!(state.repl_offset(), 100);

        state.set_role(ReplicationRole::Replica).await;
        assert_eq!(state.role().await, ReplicationRole::Replica);
    }
}
