//! Offline-first queue and sync engine for edge deployments.
//!
//! Buffers mutations while the device is disconnected and replays them
//! to a remote Ferrite instance when connectivity resumes. Uses vector
//! clocks and configurable conflict resolution policies.

use super::sync_protocol::{SyncMessage, SyncMessageType, SyncOpType, SyncOperation, VectorClock};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tracing::{info, warn};

/// Conflict resolution strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictStrategy {
    /// Last writer wins based on wall-clock timestamp.
    LastWriteWins,
    /// Cloud value always wins.
    CloudWins,
    /// Edge value always wins.
    EdgeWins,
    /// Use CRDT merge semantics (requires CRDT-wrapped values).
    CrdtMerge,
}

impl Default for ConflictStrategy {
    fn default() -> Self {
        Self::LastWriteWins
    }
}

/// Configuration for the offline sync engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OfflineSyncConfig {
    /// Maximum number of operations to buffer.
    pub max_queue_size: usize,
    /// Conflict resolution strategy.
    pub conflict_strategy: ConflictStrategy,
    /// Local node identifier.
    pub node_id: String,
    /// Remote cloud endpoint (e.g. "ferrite://cloud:6379").
    pub remote_endpoint: String,
    /// How often to attempt sync (when online).
    pub sync_interval: Duration,
    /// Path to persist the offline queue on disk.
    pub queue_path: Option<PathBuf>,
}

impl Default for OfflineSyncConfig {
    fn default() -> Self {
        Self {
            max_queue_size: 100_000,
            conflict_strategy: ConflictStrategy::LastWriteWins,
            node_id: format!(
                "edge-{}",
                uuid::Uuid::new_v4()
                    .to_string()
                    .split('-')
                    .next()
                    .unwrap_or("0000")
            ),
            remote_endpoint: "ferrite://127.0.0.1:6379".to_string(),
            sync_interval: Duration::from_secs(30),
            queue_path: None,
        }
    }
}

/// A queued operation waiting to be synced.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedOp {
    pub key: String,
    pub value: Option<Vec<u8>>,
    pub op_type: SyncOpType,
    pub timestamp_ms: u64,
}

/// The offline sync engine.
pub struct OfflineSyncEngine {
    config: OfflineSyncConfig,
    queue: Mutex<VecDeque<QueuedOp>>,
    vector_clock: Mutex<VectorClock>,
    is_online: Mutex<bool>,
    stats: SyncStats,
}

/// Sync engine statistics.
#[derive(Debug, Default)]
pub struct SyncStats {
    pub ops_queued: std::sync::atomic::AtomicU64,
    pub ops_synced: std::sync::atomic::AtomicU64,
    pub ops_dropped: std::sync::atomic::AtomicU64,
    pub conflicts: std::sync::atomic::AtomicU64,
    pub sync_attempts: std::sync::atomic::AtomicU64,
    pub sync_successes: std::sync::atomic::AtomicU64,
}

impl OfflineSyncEngine {
    /// Create a new sync engine.
    pub fn new(config: OfflineSyncConfig) -> Self {
        let queue = if let Some(ref path) = config.queue_path {
            Self::load_queue(path)
        } else {
            VecDeque::new()
        };

        Self {
            config,
            queue: Mutex::new(queue),
            vector_clock: Mutex::new(VectorClock::new()),
            is_online: Mutex::new(false),
            stats: SyncStats::default(),
        }
    }

    /// Set online/offline status.
    pub fn set_online(&self, online: bool) {
        *self.is_online.lock() = online;
        if online {
            info!(node = %self.config.node_id, "edge node is online");
        } else {
            warn!(node = %self.config.node_id, "edge node is offline");
        }
    }

    /// Whether the engine currently considers itself online.
    pub fn is_online(&self) -> bool {
        *self.is_online.lock()
    }

    /// Enqueue a mutation. When offline this is buffered; when online it could
    /// be sent immediately (but we always buffer for reliability).
    pub fn enqueue(&self, key: String, value: Option<Vec<u8>>, op_type: SyncOpType) {
        use std::sync::atomic::Ordering;

        let mut queue = self.queue.lock();
        if queue.len() >= self.config.max_queue_size {
            // Drop oldest to make room.
            queue.pop_front();
            self.stats.ops_dropped.fetch_add(1, Ordering::Relaxed);
        }

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        queue.push_back(QueuedOp {
            key,
            value,
            op_type,
            timestamp_ms: now,
        });

        self.vector_clock.lock().increment(&self.config.node_id);
        self.stats.ops_queued.fetch_add(1, Ordering::Relaxed);
    }

    /// Build a sync message from the current queue contents.
    pub fn build_sync_message(&self) -> SyncMessage {
        let queue = self.queue.lock();
        let vc = self.vector_clock.lock();

        let operations = queue
            .iter()
            .map(|op| SyncOperation {
                key: op.key.clone(),
                value: op.value.clone(),
                operation: op.op_type,
                timestamp: op.timestamp_ms,
                node_id: self.config.node_id.clone(),
            })
            .collect();

        SyncMessage {
            source_node: self.config.node_id.clone(),
            msg_type: SyncMessageType::DeltaSync,
            vector_clock: vc.to_map(),
            operations,
            timestamp: chrono::Utc::now(),
        }
    }

    /// Acknowledge that a sync was successful, clearing the synced operations
    /// from the queue.
    pub fn acknowledge_sync(&self, ops_count: usize) {
        use std::sync::atomic::Ordering;
        let mut queue = self.queue.lock();
        let to_drain = ops_count.min(queue.len());
        queue.drain(..to_drain);
        self.stats
            .ops_synced
            .fetch_add(to_drain as u64, Ordering::Relaxed);
        self.stats.sync_successes.fetch_add(1, Ordering::Relaxed);
    }

    /// Apply incoming operations from the cloud, resolving conflicts.
    pub fn apply_incoming(&self, message: &SyncMessage) -> Vec<ResolvedConflict> {
        use std::sync::atomic::Ordering;
        let mut conflicts = Vec::new();

        // Merge vector clocks.
        let other_vc = VectorClock::from_map(message.vector_clock.clone());
        self.vector_clock.lock().merge(&other_vc);

        for op in &message.operations {
            // Check if we have a concurrent local operation on the same key.
            let local_op = {
                let queue = self.queue.lock();
                queue.iter().find(|q| q.key == op.key).cloned()
            };

            if let Some(local) = local_op {
                // Conflict detected.
                self.stats.conflicts.fetch_add(1, Ordering::Relaxed);
                let winner = self.resolve_conflict(&local, op);
                conflicts.push(ResolvedConflict {
                    key: op.key.clone(),
                    winner,
                    strategy: self.config.conflict_strategy,
                });
            }
        }

        conflicts
    }

    /// Resolve a conflict between a local queued op and an incoming remote op.
    fn resolve_conflict(&self, local: &QueuedOp, remote: &SyncOperation) -> ConflictWinner {
        match self.config.conflict_strategy {
            ConflictStrategy::LastWriteWins => {
                if local.timestamp_ms >= remote.timestamp {
                    ConflictWinner::Local
                } else {
                    ConflictWinner::Remote
                }
            }
            ConflictStrategy::CloudWins => ConflictWinner::Remote,
            ConflictStrategy::EdgeWins => ConflictWinner::Local,
            ConflictStrategy::CrdtMerge => ConflictWinner::Merged,
        }
    }

    /// Number of operations in the queue.
    pub fn queue_len(&self) -> usize {
        self.queue.lock().len()
    }

    /// Persist the queue to disk (if a path is configured).
    pub fn persist_queue(&self) -> std::io::Result<()> {
        if let Some(ref path) = self.config.queue_path {
            let queue = self.queue.lock();
            let json = serde_json::to_string(&*queue)
                .map_err(std::io::Error::other)?;
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(path, json)?;
        }
        Ok(())
    }

    fn load_queue(path: &Path) -> VecDeque<QueuedOp> {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }
}

/// Result of conflict resolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedConflict {
    pub key: String,
    pub winner: ConflictWinner,
    pub strategy: ConflictStrategy,
}

/// Who won a conflict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictWinner {
    Local,
    Remote,
    Merged,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> OfflineSyncConfig {
        OfflineSyncConfig {
            max_queue_size: 100,
            node_id: "test-node".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn enqueue_and_build_message() {
        let engine = OfflineSyncEngine::new(test_config());
        engine.enqueue("k1".into(), Some(b"v1".to_vec()), SyncOpType::Set);
        engine.enqueue("k2".into(), None, SyncOpType::Delete);

        assert_eq!(engine.queue_len(), 2);

        let msg = engine.build_sync_message();
        assert_eq!(msg.operations.len(), 2);
        assert_eq!(msg.source_node, "test-node");
    }

    #[test]
    fn acknowledge_clears_queue() {
        let engine = OfflineSyncEngine::new(test_config());
        engine.enqueue("k1".into(), Some(b"v1".to_vec()), SyncOpType::Set);
        engine.enqueue("k2".into(), Some(b"v2".to_vec()), SyncOpType::Set);
        engine.enqueue("k3".into(), Some(b"v3".to_vec()), SyncOpType::Set);

        engine.acknowledge_sync(2);
        assert_eq!(engine.queue_len(), 1);
    }

    #[test]
    fn queue_drops_oldest_when_full() {
        let mut config = test_config();
        config.max_queue_size = 2;
        let engine = OfflineSyncEngine::new(config);

        engine.enqueue("k1".into(), None, SyncOpType::Set);
        engine.enqueue("k2".into(), None, SyncOpType::Set);
        engine.enqueue("k3".into(), None, SyncOpType::Set);

        assert_eq!(engine.queue_len(), 2);
        // k1 should have been dropped
    }

    #[test]
    fn online_offline_toggle() {
        let engine = OfflineSyncEngine::new(test_config());
        assert!(!engine.is_online());
        engine.set_online(true);
        assert!(engine.is_online());
        engine.set_online(false);
        assert!(!engine.is_online());
    }

    #[test]
    fn conflict_resolution_lww() {
        let config = OfflineSyncConfig {
            conflict_strategy: ConflictStrategy::LastWriteWins,
            ..test_config()
        };
        let engine = OfflineSyncEngine::new(config);
        engine.enqueue("k1".into(), Some(b"local".to_vec()), SyncOpType::Set);

        let msg = SyncMessage {
            source_node: "cloud".into(),
            msg_type: SyncMessageType::DeltaSync,
            vector_clock: Default::default(),
            operations: vec![SyncOperation {
                key: "k1".into(),
                value: Some(b"remote".to_vec()),
                operation: SyncOpType::Set,
                timestamp: u64::MAX, // very far future
                node_id: "cloud".into(),
            }],
            timestamp: chrono::Utc::now(),
        };

        let conflicts = engine.apply_incoming(&msg);
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].winner, ConflictWinner::Remote);
    }

    #[test]
    fn persist_and_reload_queue() {
        let tmp = tempfile::TempDir::new().unwrap();
        let queue_path = tmp.path().join("queue.json");

        let config = OfflineSyncConfig {
            queue_path: Some(queue_path.clone()),
            ..test_config()
        };

        // Enqueue and persist.
        {
            let engine = OfflineSyncEngine::new(config.clone());
            engine.enqueue("k1".into(), Some(b"v1".to_vec()), SyncOpType::Set);
            engine.persist_queue().unwrap();
        }

        // Reload.
        {
            let engine = OfflineSyncEngine::new(config);
            assert_eq!(engine.queue_len(), 1);
        }
    }
}
