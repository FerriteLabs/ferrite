//! Distributed ACID Transaction System for Ferrite
//!
//! Provides full distributed transaction support with:
//! - MVCC (Multi-Version Concurrency Control) for snapshot isolation
//! - 2PC (Two-Phase Commit) for distributed atomicity
//! - Distributed lock manager for pessimistic concurrency
//! - Transaction WAL for durability
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    Transaction Coordinator                       │
//! │  ┌───────────────┐  ┌───────────────┐  ┌───────────────────┐   │
//! │  │   2PC State   │  │   Lock Mgr    │  │    Version Mgr     │   │
//! │  │   Machine     │  │   (DLM)       │  │    (MVCC)          │   │
//! │  └───────┬───────┘  └───────┬───────┘  └─────────┬─────────┘   │
//! │          │                  │                    │              │
//! │  ┌───────▼──────────────────▼────────────────────▼─────────┐   │
//! │  │                  Transaction Log (WAL)                   │   │
//! │  └──────────────────────────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────────────────────┘
//!                              │
//!                              ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     Storage Engine                               │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Isolation Levels
//!
//! - **ReadUncommitted**: No isolation, may read uncommitted data
//! - **ReadCommitted**: Only reads committed data
//! - **RepeatableRead**: Snapshot at first read, prevents phantom reads
//! - **Serializable**: Full serializability via 2PL + MVCC
//!
//! # Example
//!
//! ```ignore
//! use ferrite::transaction::{TransactionManager, IsolationLevel};
//!
//! let tm = TransactionManager::new(store, config);
//!
//! // Begin a distributed transaction
//! let txn = tm.begin(IsolationLevel::Serializable).await?;
//!
//! // Perform operations within transaction
//! txn.set("key1", "value1").await?;
//! txn.set("key2", "value2").await?;
//!
//! // Commit atomically across all nodes
//! txn.commit().await?;
//! ```

#![allow(dead_code, unused_imports, unused_variables)]
pub mod coordinator;
pub mod isolation;
pub mod lock;
pub mod mvcc;
pub mod percolator;
pub mod savepoint;
pub mod txn;
pub mod wal;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;

pub use coordinator::{CoordinatorConfig, ParticipantState, TwoPhaseCoordinator};
pub use isolation::IsolationLevel;
pub use lock::{LockError, LockGrant, LockManager, LockMode};
pub use mvcc::{ReadSet, Version, VersionManager, VersionedValue, WriteSet};
pub use percolator::{
    KeyRef, LockManagerStats, LockRecord, LockType, PercolatorConfig, PercolatorManager,
    PercolatorStats, PercolatorTransaction, PercolatorTxnState, Timestamp, TimestampOracle,
    WriteRecord, WriteType,
};
pub use savepoint::{Savepoint, SavepointError, SavepointManager, SavepointRollback};
pub use txn::{Transaction, TransactionConfig, TransactionId, TransactionState};
pub use wal::{LogEntry, LogEntryType, TransactionLog};

use crate::storage::Store;

/// Transaction manager configuration
#[derive(Clone, Debug)]
pub struct TransactionManagerConfig {
    /// Default isolation level
    pub default_isolation: IsolationLevel,
    /// Transaction timeout
    pub timeout: Duration,
    /// Maximum concurrent transactions
    pub max_concurrent: usize,
    /// Enable distributed transactions (2PC)
    pub distributed_enabled: bool,
    /// Lock wait timeout
    pub lock_timeout: Duration,
    /// WAL sync mode
    pub wal_sync_mode: WalSyncMode,
    /// Checkpoint interval (number of transactions)
    pub checkpoint_interval: u64,
    /// Enable deadlock detection
    pub deadlock_detection: bool,
}

impl Default for TransactionManagerConfig {
    fn default() -> Self {
        Self {
            default_isolation: IsolationLevel::RepeatableRead,
            timeout: Duration::from_secs(30),
            max_concurrent: 10_000,
            distributed_enabled: true,
            lock_timeout: Duration::from_secs(10),
            wal_sync_mode: WalSyncMode::Immediate,
            checkpoint_interval: 1000,
            deadlock_detection: true,
        }
    }
}

/// WAL synchronization mode
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WalSyncMode {
    /// Sync after every transaction (safest)
    Immediate,
    /// Sync after N transactions (batch size)
    Batched(u32),
    /// Background sync (fastest, less durable)
    Background,
}

/// Transaction manager - central coordinator for all transactions
pub struct TransactionManager {
    /// Storage engine reference
    store: Arc<Store>,
    /// Configuration
    config: TransactionManagerConfig,
    /// Next transaction ID
    next_txn_id: AtomicU64,
    /// Active transactions
    active_txns: Arc<RwLock<HashMap<TransactionId, Arc<Transaction>>>>,
    /// Version manager (MVCC)
    version_manager: Arc<VersionManager>,
    /// Lock manager
    lock_manager: Arc<LockManager>,
    /// 2PC coordinator
    coordinator: Arc<TwoPhaseCoordinator>,
    /// Transaction log (WAL)
    wal: Arc<TransactionLog>,
    /// Committed transaction watermark (for GC)
    committed_watermark: AtomicU64,
}

impl TransactionManager {
    /// Create a new transaction manager
    pub async fn new(
        store: Arc<Store>,
        config: TransactionManagerConfig,
    ) -> Result<Self, TransactionError> {
        let version_manager = Arc::new(VersionManager::new());
        let lock_manager = Arc::new(LockManager::new(
            config.lock_timeout,
            config.deadlock_detection,
        ));
        let coordinator = Arc::new(TwoPhaseCoordinator::new(CoordinatorConfig {
            prepare_timeout: config.timeout,
            commit_timeout: config.timeout,
        }));
        let wal = Arc::new(TransactionLog::new(config.wal_sync_mode.clone()).await?);

        Ok(Self {
            store,
            config,
            next_txn_id: AtomicU64::new(1),
            active_txns: Arc::new(RwLock::new(HashMap::new())),
            version_manager,
            lock_manager,
            coordinator,
            wal,
            committed_watermark: AtomicU64::new(0),
        })
    }

    /// Begin a new transaction
    pub async fn begin(
        &self,
        isolation: Option<IsolationLevel>,
    ) -> Result<Arc<Transaction>, TransactionError> {
        let isolation = isolation.unwrap_or(self.config.default_isolation);

        // Check concurrent transaction limit
        let active_count = self.active_txns.read().await.len();
        if active_count >= self.config.max_concurrent {
            return Err(TransactionError::TooManyTransactions);
        }

        // Allocate transaction ID
        let txn_id = TransactionId(self.next_txn_id.fetch_add(1, Ordering::SeqCst));

        // Get snapshot timestamp for MVCC
        let snapshot_ts = self.version_manager.current_timestamp();

        // Create transaction
        let txn = Arc::new(Transaction::new(
            txn_id,
            isolation,
            snapshot_ts,
            self.store.clone(),
            self.version_manager.clone(),
            self.lock_manager.clone(),
            self.wal.clone(),
            self.config.timeout,
        ));

        // Log transaction begin
        self.wal.log_begin(txn_id).await?;

        // Register in active transactions
        self.active_txns.write().await.insert(txn_id, txn.clone());

        Ok(txn)
    }

    /// Begin a distributed transaction (spans multiple nodes)
    pub async fn begin_distributed(
        &self,
        isolation: Option<IsolationLevel>,
        participants: Vec<String>,
    ) -> Result<Arc<Transaction>, TransactionError> {
        if !self.config.distributed_enabled {
            return Err(TransactionError::DistributedNotEnabled);
        }

        let txn = self.begin(isolation).await?;

        // Register with 2PC coordinator
        self.coordinator
            .register_transaction(txn.id(), participants)
            .await?;

        Ok(txn)
    }

    /// Commit a transaction
    pub async fn commit(&self, txn_id: TransactionId) -> Result<(), TransactionError> {
        let txn = {
            let active = self.active_txns.read().await;
            active
                .get(&txn_id)
                .cloned()
                .ok_or(TransactionError::NotFound(txn_id))?
        };

        // Check if distributed
        if self.coordinator.is_distributed(txn_id).await {
            // Use 2PC
            self.coordinator.prepare(txn_id).await?;
            self.coordinator.commit(txn_id).await?;
        }

        // Apply writes to storage
        txn.apply_writes().await?;

        // Log commit
        self.wal.log_commit(txn_id).await?;

        // Release locks
        self.lock_manager.release_all(txn_id).await;

        // Remove from active transactions
        self.active_txns.write().await.remove(&txn_id);

        // Update watermark
        self.update_watermark(txn_id);

        Ok(())
    }

    /// Rollback a transaction
    pub async fn rollback(&self, txn_id: TransactionId) -> Result<(), TransactionError> {
        let txn = {
            let active = self.active_txns.read().await;
            active
                .get(&txn_id)
                .cloned()
                .ok_or(TransactionError::NotFound(txn_id))?
        };

        // If distributed, abort on all participants
        if self.coordinator.is_distributed(txn_id).await {
            self.coordinator.abort(txn_id).await?;
        }

        // Discard writes
        txn.discard_writes().await;

        // Log abort
        self.wal.log_abort(txn_id).await?;

        // Release locks
        self.lock_manager.release_all(txn_id).await;

        // Remove from active transactions
        self.active_txns.write().await.remove(&txn_id);

        Ok(())
    }

    /// Get an active transaction by ID
    pub async fn get(&self, txn_id: TransactionId) -> Option<Arc<Transaction>> {
        self.active_txns.read().await.get(&txn_id).cloned()
    }

    /// Check if a transaction is active
    pub async fn is_active(&self, txn_id: TransactionId) -> bool {
        self.active_txns.read().await.contains_key(&txn_id)
    }

    /// Get the number of active transactions
    pub async fn active_count(&self) -> usize {
        self.active_txns.read().await.len()
    }

    /// Get the committed watermark for garbage collection
    pub fn committed_watermark(&self) -> u64 {
        self.committed_watermark.load(Ordering::SeqCst)
    }

    /// Run garbage collection on old versions
    pub async fn gc(&self) -> Result<u64, TransactionError> {
        let watermark = self.committed_watermark();
        let cleaned = self.version_manager.gc(watermark).await;
        Ok(cleaned)
    }

    /// Recover from WAL after crash
    pub async fn recover(&self) -> Result<RecoveryStats, TransactionError> {
        let entries = self.wal.read_all().await?;

        let mut stats = RecoveryStats::default();
        let mut pending_txns: HashMap<TransactionId, Vec<LogEntry>> = HashMap::new();

        for entry in entries {
            match entry.entry_type {
                LogEntryType::Begin => {
                    pending_txns.insert(entry.txn_id, vec![entry]);
                }
                LogEntryType::Write { .. } => {
                    if let Some(entries) = pending_txns.get_mut(&entry.txn_id) {
                        entries.push(entry);
                    }
                }
                LogEntryType::Prepare => {
                    // Mark as prepared but not committed
                    if let Some(entries) = pending_txns.get_mut(&entry.txn_id) {
                        entries.push(entry);
                    }
                }
                LogEntryType::Commit => {
                    // Replay this transaction
                    if let Some(entries) = pending_txns.remove(&entry.txn_id) {
                        self.replay_transaction(entries).await?;
                        stats.committed += 1;
                    }
                }
                LogEntryType::Abort => {
                    // Discard this transaction
                    pending_txns.remove(&entry.txn_id);
                    stats.aborted += 1;
                }
                LogEntryType::Checkpoint { .. } => {
                    // Use as recovery point
                    stats.checkpoints += 1;
                }
            }
        }

        // Any remaining transactions without commit/abort are rolled back
        for (txn_id, _) in pending_txns {
            self.wal.log_abort(txn_id).await?;
            stats.rolled_back += 1;
        }

        Ok(stats)
    }

    /// Replay a committed transaction during recovery
    async fn replay_transaction(&self, entries: Vec<LogEntry>) -> Result<(), TransactionError> {
        for entry in entries {
            if let LogEntryType::Write { db, key, value } = entry.entry_type {
                match value {
                    Some(v) => {
                        self.store.set(db, key, crate::storage::Value::String(v));
                    }
                    None => {
                        self.store.del(db, &[key]);
                    }
                }
            }
        }
        Ok(())
    }

    /// Update committed watermark
    fn update_watermark(&self, txn_id: TransactionId) {
        let current = self.committed_watermark.load(Ordering::SeqCst);
        if txn_id.0 > current {
            self.committed_watermark.store(txn_id.0, Ordering::SeqCst);
        }
    }

    /// Create a checkpoint
    /// Create a checkpoint of the current WAL state.
    pub async fn checkpoint(&self) -> Result<(), TransactionError> {
        let active_txns: Vec<TransactionId> =
            self.active_txns.read().await.keys().copied().collect();
        self.wal.log_checkpoint(active_txns).await?;
        Ok(())
    }
}

/// Recovery statistics
#[derive(Debug, Default)]
pub struct RecoveryStats {
    /// Number of committed transactions replayed
    pub committed: u64,
    /// Number of aborted transactions found
    pub aborted: u64,
    /// Number of transactions rolled back
    pub rolled_back: u64,
    /// Number of checkpoints found
    pub checkpoints: u64,
}

/// Transaction errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum TransactionError {
    /// Transaction with the given ID was not found.
    #[error("transaction not found: {0}")]
    NotFound(TransactionId),

    /// Transaction was already committed or aborted.
    #[error("transaction already committed or aborted")]
    AlreadyFinished,

    /// Transaction exceeded its time limit.
    #[error("transaction timed out")]
    Timeout,

    /// Maximum concurrent transaction limit reached.
    #[error("too many concurrent transactions")]
    TooManyTransactions,

    /// Write-write conflict on the specified key.
    #[error("write conflict on key: {0}")]
    WriteConflict(String),

    /// Serialization failure due to concurrent modifications.
    #[error("serialization failure")]
    SerializationFailure,

    /// Lock acquisition timed out on the specified key.
    #[error("lock timeout on key: {0}")]
    LockTimeout(String),

    /// A deadlock was detected among transactions.
    #[error("deadlock detected")]
    Deadlock,

    /// Two-phase commit prepare phase failed.
    #[error("2PC prepare failed: {0}")]
    PrepareFailed(String),

    /// Two-phase commit commit phase failed.
    #[error("2PC commit failed: {0}")]
    CommitFailed(String),

    /// Two-phase commit abort phase failed.
    #[error("2PC abort failed: {0}")]
    AbortFailed(String),

    /// Distributed transactions are not enabled in config.
    #[error("distributed transactions not enabled")]
    DistributedNotEnabled,

    /// Error from the write-ahead log.
    #[error("WAL error: {0}")]
    WalError(String),

    /// Error from the storage engine.
    #[error("storage error: {0}")]
    StorageError(String),

    /// Transaction is in an invalid state for the operation.
    #[error("invalid state: {0}")]
    InvalidState(String),

    /// Attempted to write in a read-only transaction.
    #[error("read only transaction cannot write")]
    ReadOnly,

    /// The requested key was not found.
    #[error("key not found: {0}")]
    KeyNotFound(String),
}

impl std::fmt::Display for TransactionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "txn-{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_store() -> Arc<Store> {
        Arc::new(Store::new(16))
    }

    #[tokio::test]
    async fn test_transaction_manager_creation() {
        let store = create_test_store();
        let config = TransactionManagerConfig::default();
        let tm = TransactionManager::new(store, config).await.unwrap();

        assert_eq!(tm.active_count().await, 0);
    }

    #[tokio::test]
    async fn test_begin_transaction() {
        let store = create_test_store();
        let config = TransactionManagerConfig::default();
        let tm = TransactionManager::new(store, config).await.unwrap();

        let txn = tm.begin(None).await.unwrap();
        assert!(tm.is_active(txn.id()).await);
        assert_eq!(tm.active_count().await, 1);
    }

    #[tokio::test]
    async fn test_commit_transaction() {
        let store = create_test_store();
        let config = TransactionManagerConfig::default();
        let tm = TransactionManager::new(store, config).await.unwrap();

        let txn = tm.begin(None).await.unwrap();
        let txn_id = txn.id();

        tm.commit(txn_id).await.unwrap();
        assert!(!tm.is_active(txn_id).await);
    }

    #[tokio::test]
    async fn test_rollback_transaction() {
        let store = create_test_store();
        let config = TransactionManagerConfig::default();
        let tm = TransactionManager::new(store, config).await.unwrap();

        let txn = tm.begin(None).await.unwrap();
        let txn_id = txn.id();

        tm.rollback(txn_id).await.unwrap();
        assert!(!tm.is_active(txn_id).await);
    }

    #[tokio::test]
    async fn test_transaction_limit() {
        let store = create_test_store();
        let mut config = TransactionManagerConfig::default();
        config.max_concurrent = 2;

        let tm = TransactionManager::new(store, config).await.unwrap();

        let _txn1 = tm.begin(None).await.unwrap();
        let _txn2 = tm.begin(None).await.unwrap();

        let result = tm.begin(None).await;
        assert!(matches!(result, Err(TransactionError::TooManyTransactions)));
    }
}
