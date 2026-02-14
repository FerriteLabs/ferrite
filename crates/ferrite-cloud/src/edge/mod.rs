//! Edge Deployment Kit
//!
//! Optimized configuration and sync protocol for deploying Ferrite on
//! resource-constrained edge devices. Supports offline-first operation,
//! automatic CRDT-based synchronization with cloud instances, and
//! minimal resource footprint.
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────┐     ┌─────────────────────┐
//! │       Edge Node                │     │   Cloud Ferrite      │
//! │  ┌──────────┐ ┌────────────┐  │     │                     │
//! │  │ Ferrite  │ │  Sync      │◄─┼────►│  Sync Endpoint      │
//! │  │ Embedded │ │  Engine    │  │     │                     │
//! │  └──────────┘ └────────────┘  │     └─────────────────────┘
//! │  ┌──────────┐ ┌────────────┐  │
//! │  │ Offline  │ │  Resource  │  │
//! │  │ Queue    │ │  Monitor   │  │
//! │  └──────────┘ └────────────┘  │
//! └────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use ferrite::edge::{EdgeNode, EdgeConfig, SyncPolicy};
//!
//! let node = EdgeNode::new(EdgeConfig {
//!     node_id: "edge-001".to_string(),
//!     max_memory: 50 * 1024 * 1024, // 50MB
//!     sync_policy: SyncPolicy::Opportunistic,
//!     ..Default::default()
//! });
//!
//! // Works fully offline
//! node.set("sensor:temp", "22.5")?;
//! let temp = node.get("sensor:temp")?;
//!
//! // Syncs when connectivity is available
//! node.sync().await?;
//! ```

pub mod offline_sync;
pub mod resource_monitor;
pub mod sync_protocol;

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::RwLock;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for an edge deployment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeConfig {
    /// Unique node identifier.
    pub node_id: String,
    /// Maximum memory budget (bytes).
    pub max_memory: u64,
    /// Maximum disk storage (bytes).
    pub max_disk: u64,
    /// Maximum number of keys.
    pub max_keys: u64,
    /// Sync policy with cloud.
    pub sync_policy: SyncPolicy,
    /// Cloud endpoint for synchronization.
    pub cloud_endpoint: Option<String>,
    /// Sync interval for periodic policy.
    pub sync_interval: Duration,
    /// Maximum offline queue size (operations).
    pub max_offline_queue: usize,
    /// Enable data compression for storage.
    pub compression: bool,
    /// Target CPU architectures for this deployment.
    pub target_arch: TargetArch,
    /// Resource monitoring interval.
    pub monitor_interval: Duration,
    /// Conflict resolution strategy for sync.
    pub conflict_resolution: ConflictResolution,
}

impl Default for EdgeConfig {
    fn default() -> Self {
        Self {
            node_id: "edge-default".to_string(),
            max_memory: 50 * 1024 * 1024, // 50MB
            max_disk: 500 * 1024 * 1024,  // 500MB
            max_keys: 100_000,
            sync_policy: SyncPolicy::Opportunistic,
            cloud_endpoint: None,
            sync_interval: Duration::from_secs(60),
            max_offline_queue: 10_000,
            compression: true,
            target_arch: TargetArch::Current,
            monitor_interval: Duration::from_secs(30),
            conflict_resolution: ConflictResolution::CrdtMerge,
        }
    }
}

/// When and how to sync with the cloud.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncPolicy {
    /// Sync immediately on every write.
    Immediate,
    /// Sync periodically at configured intervals.
    Periodic,
    /// Sync when connectivity is available and idle.
    Opportunistic,
    /// Never sync (fully standalone).
    NeverSync,
}

impl Default for SyncPolicy {
    fn default() -> Self {
        Self::Opportunistic
    }
}

/// Target CPU architecture for edge binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TargetArch {
    /// Use current compilation target.
    Current,
    /// ARM 64-bit (aarch64).
    Arm64,
    /// ARM 32-bit (armv7).
    Arm32,
    /// x86-64.
    X86_64,
    /// RISC-V 64-bit.
    RiscV64,
}

impl Default for TargetArch {
    fn default() -> Self {
        Self::Current
    }
}

/// How to resolve conflicts when syncing with cloud.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictResolution {
    /// Use CRDTs for automatic merge (preferred).
    CrdtMerge,
    /// Last-writer-wins based on timestamp.
    LastWriteWins,
    /// Cloud always wins.
    CloudWins,
    /// Edge always wins.
    EdgeWins,
}

impl Default for ConflictResolution {
    fn default() -> Self {
        Self::CrdtMerge
    }
}

// ---------------------------------------------------------------------------
// Edge Node
// ---------------------------------------------------------------------------

/// An edge deployment node with offline-first operation.
pub struct EdgeNode {
    config: EdgeConfig,
    is_online: AtomicBool,
    offline_queue: RwLock<VecDeque<OfflineOperation>>,
    stats: EdgeStats,
}

/// An operation queued while offline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OfflineOperation {
    /// Type of operation.
    pub op_type: OfflineOpType,
    /// Key involved.
    pub key: String,
    /// Value (if applicable).
    pub value: Option<Vec<u8>>,
    /// When the operation was queued.
    pub queued_at: chrono::DateTime<chrono::Utc>,
}

/// Type of offline operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OfflineOpType {
    Set,
    Delete,
    Increment,
    ListPush,
    SetAdd,
}

/// Edge node statistics.
#[derive(Debug, Default)]
pub struct EdgeStats {
    pub ops_total: AtomicU64,
    pub ops_offline: AtomicU64,
    pub syncs_completed: AtomicU64,
    pub syncs_failed: AtomicU64,
    pub conflicts_resolved: AtomicU64,
    pub bytes_synced: AtomicU64,
}

impl EdgeNode {
    /// Creates a new edge node with the given configuration.
    pub fn new(config: EdgeConfig) -> Self {
        Self {
            config,
            is_online: AtomicBool::new(false),
            offline_queue: RwLock::new(VecDeque::new()),
            stats: EdgeStats::default(),
        }
    }

    /// Returns the node ID.
    pub fn node_id(&self) -> &str {
        &self.config.node_id
    }

    /// Returns whether the node believes it has connectivity.
    pub fn is_online(&self) -> bool {
        self.is_online.load(Ordering::Relaxed)
    }

    /// Sets the online status.
    pub fn set_online(&self, online: bool) {
        self.is_online.store(online, Ordering::Relaxed);
    }

    /// Queues an operation for later sync when offline.
    pub fn queue_offline_operation(&self, op: OfflineOperation) -> Result<(), EdgeError> {
        let mut queue = self.offline_queue.write();
        if queue.len() >= self.config.max_offline_queue {
            return Err(EdgeError::OfflineQueueFull(self.config.max_offline_queue));
        }
        queue.push_back(op);
        self.stats.ops_offline.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Returns the number of pending offline operations.
    pub fn offline_queue_len(&self) -> usize {
        self.offline_queue.read().len()
    }

    /// Drains the offline queue (for sync processing).
    pub fn drain_offline_queue(&self) -> Vec<OfflineOperation> {
        self.offline_queue.write().drain(..).collect()
    }

    /// Records a completed sync.
    pub fn record_sync_success(&self, bytes: u64) {
        self.stats.syncs_completed.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes_synced.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records a failed sync.
    pub fn record_sync_failure(&self) {
        self.stats.syncs_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the sync policy.
    pub fn sync_policy(&self) -> SyncPolicy {
        self.config.sync_policy
    }

    /// Returns the conflict resolution strategy.
    pub fn conflict_resolution(&self) -> ConflictResolution {
        self.config.conflict_resolution
    }

    /// Returns a stats snapshot.
    pub fn stats_snapshot(&self) -> EdgeStatsSnapshot {
        EdgeStatsSnapshot {
            ops_total: self.stats.ops_total.load(Ordering::Relaxed),
            ops_offline: self.stats.ops_offline.load(Ordering::Relaxed),
            syncs_completed: self.stats.syncs_completed.load(Ordering::Relaxed),
            syncs_failed: self.stats.syncs_failed.load(Ordering::Relaxed),
            conflicts_resolved: self.stats.conflicts_resolved.load(Ordering::Relaxed),
            bytes_synced: self.stats.bytes_synced.load(Ordering::Relaxed),
            offline_queue_len: self.offline_queue_len() as u64,
            is_online: self.is_online(),
        }
    }

    /// Returns the configuration.
    pub fn config(&self) -> &EdgeConfig {
        &self.config
    }
}

/// Serializable stats snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeStatsSnapshot {
    pub ops_total: u64,
    pub ops_offline: u64,
    pub syncs_completed: u64,
    pub syncs_failed: u64,
    pub conflicts_resolved: u64,
    pub bytes_synced: u64,
    pub offline_queue_len: u64,
    pub is_online: bool,
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, thiserror::Error)]
pub enum EdgeError {
    #[error("offline queue full: max {0} operations")]
    OfflineQueueFull(usize),

    #[error("sync failed: {0}")]
    SyncFailed(String),

    #[error("cloud endpoint not configured")]
    NoCloudEndpoint,

    #[error("resource limit exceeded: {0}")]
    ResourceLimitExceeded(String),

    #[error("conflict resolution failed: {0}")]
    ConflictResolutionFailed(String),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = EdgeConfig::default();
        assert_eq!(config.max_memory, 50 * 1024 * 1024);
        assert_eq!(config.sync_policy, SyncPolicy::Opportunistic);
        assert_eq!(config.conflict_resolution, ConflictResolution::CrdtMerge);
        assert!(config.compression);
    }

    #[test]
    fn test_edge_node_creation() {
        let node = EdgeNode::new(EdgeConfig::default());
        assert_eq!(node.node_id(), "edge-default");
        assert!(!node.is_online());
    }

    #[test]
    fn test_online_status() {
        let node = EdgeNode::new(EdgeConfig::default());
        node.set_online(true);
        assert!(node.is_online());
        node.set_online(false);
        assert!(!node.is_online());
    }

    #[test]
    fn test_offline_queue() {
        let node = EdgeNode::new(EdgeConfig {
            max_offline_queue: 3,
            ..Default::default()
        });

        for i in 0..3 {
            node.queue_offline_operation(OfflineOperation {
                op_type: OfflineOpType::Set,
                key: format!("key:{}", i),
                value: Some(b"value".to_vec()),
                queued_at: chrono::Utc::now(),
            })
            .unwrap();
        }
        assert_eq!(node.offline_queue_len(), 3);

        // Queue full
        let result = node.queue_offline_operation(OfflineOperation {
            op_type: OfflineOpType::Set,
            key: "overflow".to_string(),
            value: None,
            queued_at: chrono::Utc::now(),
        });
        assert!(matches!(result, Err(EdgeError::OfflineQueueFull(3))));
    }

    #[test]
    fn test_drain_queue() {
        let node = EdgeNode::new(EdgeConfig::default());
        node.queue_offline_operation(OfflineOperation {
            op_type: OfflineOpType::Set,
            key: "k1".to_string(),
            value: None,
            queued_at: chrono::Utc::now(),
        })
        .unwrap();

        let ops = node.drain_offline_queue();
        assert_eq!(ops.len(), 1);
        assert_eq!(node.offline_queue_len(), 0);
    }

    #[test]
    fn test_stats_snapshot() {
        let node = EdgeNode::new(EdgeConfig::default());
        node.queue_offline_operation(OfflineOperation {
            op_type: OfflineOpType::Set,
            key: "k1".to_string(),
            value: None,
            queued_at: chrono::Utc::now(),
        })
        .unwrap();

        node.record_sync_success(1024);

        let stats = node.stats_snapshot();
        assert_eq!(stats.ops_offline, 1);
        assert_eq!(stats.syncs_completed, 1);
        assert_eq!(stats.bytes_synced, 1024);
        assert_eq!(stats.offline_queue_len, 1);
    }
}
