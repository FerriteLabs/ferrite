//! Transaction type and operations
//!
//! Provides the core Transaction struct with read/write operations,
//! snapshot isolation, and conflict detection.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::RwLock;

use super::isolation::IsolationLevel;
use super::lock::{LockManager, LockMode};
use super::mvcc::VersionManager;
use super::wal::TransactionLog;
use super::TransactionError;
use crate::storage::{Store, Value as StorageValue};

/// Transaction identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransactionId(pub u64);

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TransactionState {
    /// Transaction is active
    Active = 0,
    /// Transaction is in prepare phase (2PC)
    Preparing = 1,
    /// Transaction is prepared, waiting for commit (2PC)
    Prepared = 2,
    /// Transaction is committing
    Committing = 3,
    /// Transaction has been committed
    Committed = 4,
    /// Transaction is aborting
    Aborting = 5,
    /// Transaction has been aborted
    Aborted = 6,
}

impl From<u8> for TransactionState {
    fn from(v: u8) -> Self {
        match v {
            0 => TransactionState::Active,
            1 => TransactionState::Preparing,
            2 => TransactionState::Prepared,
            3 => TransactionState::Committing,
            4 => TransactionState::Committed,
            5 => TransactionState::Aborting,
            6 => TransactionState::Aborted,
            _ => TransactionState::Active,
        }
    }
}

/// Transaction configuration
#[derive(Clone, Debug)]
pub struct TransactionConfig {
    /// Isolation level
    pub isolation: IsolationLevel,
    /// Read-only transaction
    pub read_only: bool,
    /// Timeout duration
    pub timeout: Duration,
    /// Database index
    pub database: u8,
}

impl Default for TransactionConfig {
    fn default() -> Self {
        Self {
            isolation: IsolationLevel::RepeatableRead,
            read_only: false,
            timeout: Duration::from_secs(30),
            database: 0,
        }
    }
}

/// A transaction with ACID guarantees
pub struct Transaction {
    /// Transaction ID
    id: TransactionId,
    /// Isolation level
    isolation: IsolationLevel,
    /// Snapshot timestamp for MVCC
    snapshot_ts: u64,
    /// Current state
    state: AtomicU8,
    /// Start time for timeout
    start_time: Instant,
    /// Timeout duration
    timeout: Duration,
    /// Database index
    database: RwLock<u8>,
    /// Read set (keys read during transaction)
    read_set: RwLock<HashSet<(u8, Bytes)>>,
    /// Write set (keys and values to be written)
    write_set: RwLock<HashMap<(u8, Bytes), Option<StorageValue>>>,
    /// Storage reference
    store: Arc<Store>,
    /// Version manager
    version_manager: Arc<VersionManager>,
    /// Lock manager
    lock_manager: Arc<LockManager>,
    /// Transaction log
    wal: Arc<TransactionLog>,
    /// Read-only flag
    read_only: bool,
}

impl Transaction {
    /// Create a new transaction
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: TransactionId,
        isolation: IsolationLevel,
        snapshot_ts: u64,
        store: Arc<Store>,
        version_manager: Arc<VersionManager>,
        lock_manager: Arc<LockManager>,
        wal: Arc<TransactionLog>,
        timeout: Duration,
    ) -> Self {
        Self {
            id,
            isolation,
            snapshot_ts,
            state: AtomicU8::new(TransactionState::Active as u8),
            start_time: Instant::now(),
            timeout,
            database: RwLock::new(0),
            read_set: RwLock::new(HashSet::new()),
            write_set: RwLock::new(HashMap::new()),
            store,
            version_manager,
            lock_manager,
            wal,
            read_only: false,
        }
    }

    /// Create a read-only transaction
    #[allow(clippy::too_many_arguments)]
    pub fn new_readonly(
        id: TransactionId,
        isolation: IsolationLevel,
        snapshot_ts: u64,
        store: Arc<Store>,
        version_manager: Arc<VersionManager>,
        lock_manager: Arc<LockManager>,
        wal: Arc<TransactionLog>,
        timeout: Duration,
    ) -> Self {
        Self {
            id,
            isolation,
            snapshot_ts,
            state: AtomicU8::new(TransactionState::Active as u8),
            start_time: Instant::now(),
            timeout,
            database: RwLock::new(0),
            read_set: RwLock::new(HashSet::new()),
            write_set: RwLock::new(HashMap::new()),
            store,
            version_manager,
            lock_manager,
            wal,
            read_only: true,
        }
    }

    /// Get the transaction ID
    pub fn id(&self) -> TransactionId {
        self.id
    }

    /// Get the transaction state
    pub fn state(&self) -> TransactionState {
        self.state.load(Ordering::SeqCst).into()
    }

    /// Get the snapshot timestamp
    pub fn snapshot_ts(&self) -> u64 {
        self.snapshot_ts
    }

    /// Get the isolation level
    pub fn isolation(&self) -> &IsolationLevel {
        &self.isolation
    }

    /// Check if transaction is read-only
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Check if transaction has timed out
    pub fn is_timed_out(&self) -> bool {
        self.start_time.elapsed() > self.timeout
    }

    /// Set the database
    pub async fn set_database(&self, db: u8) {
        *self.database.write().await = db;
    }

    /// Get the database
    pub async fn get_database(&self) -> u8 {
        *self.database.read().await
    }

    /// Check if transaction is still active
    fn check_active(&self) -> Result<(), TransactionError> {
        match self.state() {
            TransactionState::Active => {
                if self.is_timed_out() {
                    Err(TransactionError::Timeout)
                } else {
                    Ok(())
                }
            }
            TransactionState::Committed => Err(TransactionError::AlreadyFinished),
            TransactionState::Aborted => Err(TransactionError::AlreadyFinished),
            _ => Err(TransactionError::InvalidState(format!(
                "{:?}",
                self.state()
            ))),
        }
    }

    /// Read a key within the transaction
    pub async fn get(&self, key: &Bytes) -> Result<Option<StorageValue>, TransactionError> {
        self.check_active()?;

        let db = self.get_database().await;
        let cache_key = (db, key.clone());

        // Check write set first (read your own writes)
        {
            let write_set = self.write_set.read().await;
            if let Some(value) = write_set.get(&cache_key) {
                return Ok(value.clone());
            }
        }

        // Acquire read lock if using serializable isolation
        if self.isolation == IsolationLevel::Serializable {
            self.lock_manager
                .acquire(self.id, db, key.clone(), LockMode::Shared)
                .await?;
        }

        // Read from storage using MVCC
        let value = match self.isolation {
            IsolationLevel::ReadUncommitted => {
                // Read latest version
                self.store.get(db, key)
            }
            IsolationLevel::ReadCommitted => {
                // Read latest committed version
                self.version_manager
                    .read_committed(db, key, &self.store)
                    .await
            }
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                // Read from snapshot
                self.version_manager
                    .read_snapshot(db, key, self.snapshot_ts, &self.store)
                    .await
            }
        };

        // Track in read set
        self.read_set.write().await.insert(cache_key);

        Ok(value)
    }

    /// Write a key within the transaction
    pub async fn set(&self, key: Bytes, value: StorageValue) -> Result<(), TransactionError> {
        self.check_active()?;

        if self.read_only {
            return Err(TransactionError::ReadOnly);
        }

        let db = self.get_database().await;
        let cache_key = (db, key.clone());

        // Acquire write lock
        self.lock_manager
            .acquire(self.id, db, key.clone(), LockMode::Exclusive)
            .await?;

        // Check for write conflicts in serializable mode
        if self.isolation == IsolationLevel::Serializable {
            // Check if key was modified after our snapshot
            if self
                .version_manager
                .has_conflict(db, &key, self.snapshot_ts)
                .await
            {
                return Err(TransactionError::WriteConflict(
                    String::from_utf8_lossy(&key).to_string(),
                ));
            }
        }

        // Log the write
        self.wal
            .log_write(self.id, db, key.clone(), Some(value.clone()))
            .await
            .map_err(|e| TransactionError::WalError(e.to_string()))?;

        // Add to write set
        self.write_set.write().await.insert(cache_key, Some(value));

        Ok(())
    }

    /// Delete a key within the transaction
    pub async fn delete(&self, key: Bytes) -> Result<(), TransactionError> {
        self.check_active()?;

        if self.read_only {
            return Err(TransactionError::ReadOnly);
        }

        let db = self.get_database().await;
        let cache_key = (db, key.clone());

        // Acquire write lock
        self.lock_manager
            .acquire(self.id, db, key.clone(), LockMode::Exclusive)
            .await?;

        // Check for write conflicts in serializable mode
        if self.isolation == IsolationLevel::Serializable
            && self
                .version_manager
                .has_conflict(db, &key, self.snapshot_ts)
                .await
        {
            return Err(TransactionError::WriteConflict(
                String::from_utf8_lossy(&key).to_string(),
            ));
        }

        // Log the delete
        self.wal
            .log_write(self.id, db, key.clone(), None)
            .await
            .map_err(|e| TransactionError::WalError(e.to_string()))?;

        // Add to write set with None value (tombstone)
        self.write_set.write().await.insert(cache_key, None);

        Ok(())
    }

    /// Increment a key atomically
    pub async fn incr(&self, key: Bytes, delta: i64) -> Result<i64, TransactionError> {
        let current = self.get(&key).await?;

        let value = match current {
            Some(StorageValue::String(bytes)) => {
                let s = String::from_utf8_lossy(&bytes);
                s.parse::<i64>().unwrap_or(0)
            }
            None => 0,
            _ => return Err(TransactionError::StorageError("not a string".to_string())),
        };

        let new_value = value + delta;
        self.set(
            key,
            StorageValue::String(Bytes::from(new_value.to_string())),
        )
        .await?;

        Ok(new_value)
    }

    /// Check if a key exists
    pub async fn exists(&self, key: &Bytes) -> Result<bool, TransactionError> {
        Ok(self.get(key).await?.is_some())
    }

    /// Get multiple keys
    pub async fn mget(
        &self,
        keys: &[Bytes],
    ) -> Result<Vec<Option<StorageValue>>, TransactionError> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(key).await?);
        }
        Ok(results)
    }

    /// Set multiple keys
    pub async fn mset(&self, pairs: Vec<(Bytes, StorageValue)>) -> Result<(), TransactionError> {
        for (key, value) in pairs {
            self.set(key, value).await?;
        }
        Ok(())
    }

    /// Apply writes to storage (called during commit)
    pub async fn apply_writes(&self) -> Result<(), TransactionError> {
        self.state
            .store(TransactionState::Committing as u8, Ordering::SeqCst);

        let write_set = self.write_set.read().await;

        for ((db, key), value) in write_set.iter() {
            match value {
                Some(v) => {
                    // Record version before write
                    self.version_manager
                        .record_write(*db, key.clone(), self.id.0)
                        .await;
                    self.store.set(*db, key.clone(), v.clone());
                }
                None => {
                    self.version_manager
                        .record_write(*db, key.clone(), self.id.0)
                        .await;
                    self.store.del(*db, &[key.clone()]);
                }
            }
        }

        self.state
            .store(TransactionState::Committed as u8, Ordering::SeqCst);
        Ok(())
    }

    /// Discard writes (called during rollback)
    pub async fn discard_writes(&self) {
        self.state
            .store(TransactionState::Aborting as u8, Ordering::SeqCst);
        self.write_set.write().await.clear();
        self.read_set.write().await.clear();
        self.state
            .store(TransactionState::Aborted as u8, Ordering::SeqCst);
    }

    /// Get the read set
    pub async fn read_set(&self) -> HashSet<(u8, Bytes)> {
        self.read_set.read().await.clone()
    }

    /// Get the write set
    pub async fn write_set(&self) -> HashMap<(u8, Bytes), Option<StorageValue>> {
        self.write_set.read().await.clone()
    }

    /// Check for conflicts with another transaction
    pub async fn conflicts_with(&self, other: &Transaction) -> bool {
        let my_writes = self.write_set.read().await;
        let other_writes = other.write_set.read().await;

        // Check write-write conflicts
        for key in my_writes.keys() {
            if other_writes.contains_key(key) {
                return true;
            }
        }

        // For serializable, also check write-read conflicts
        if self.isolation == IsolationLevel::Serializable {
            let other_reads = other.read_set.read().await;
            for key in my_writes.keys() {
                if other_reads.contains(key) {
                    return true;
                }
            }
        }

        false
    }

    /// Prepare for 2PC (validate and acquire all locks)
    pub async fn prepare(&self) -> Result<(), TransactionError> {
        self.check_active()?;

        self.state
            .store(TransactionState::Preparing as u8, Ordering::SeqCst);

        // Validate all reads are still valid
        if self.isolation == IsolationLevel::Serializable {
            let read_set = self.read_set.read().await;
            for (db, key) in read_set.iter() {
                if self
                    .version_manager
                    .has_conflict(*db, key, self.snapshot_ts)
                    .await
                {
                    self.state
                        .store(TransactionState::Active as u8, Ordering::SeqCst);
                    return Err(TransactionError::SerializationFailure);
                }
            }
        }

        // Validate all write locks are held
        let write_set = self.write_set.read().await;
        for (db, key) in write_set.keys() {
            if !self.lock_manager.is_held(self.id, *db, key).await {
                self.state
                    .store(TransactionState::Active as u8, Ordering::SeqCst);
                return Err(TransactionError::InvalidState("lock not held".to_string()));
            }
        }

        self.state
            .store(TransactionState::Prepared as u8, Ordering::SeqCst);
        Ok(())
    }
}

impl std::fmt::Debug for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transaction")
            .field("id", &self.id)
            .field("state", &self.state())
            .field("isolation", &self.isolation)
            .field("snapshot_ts", &self.snapshot_ts)
            .field("read_only", &self.read_only)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::WalSyncMode;

    fn create_test_deps() -> (Arc<Store>, Arc<VersionManager>, Arc<LockManager>) {
        let store = Arc::new(Store::new(16));
        let version_manager = Arc::new(VersionManager::new());
        let lock_manager = Arc::new(LockManager::new(Duration::from_secs(5), true));
        (store, version_manager, lock_manager)
    }

    #[tokio::test]
    async fn test_transaction_creation() {
        let (store, vm, lm) = create_test_deps();
        let wal = Arc::new(TransactionLog::new(WalSyncMode::Background).await.unwrap());

        let txn = Transaction::new(
            TransactionId(1),
            IsolationLevel::RepeatableRead,
            0,
            store,
            vm,
            lm,
            wal,
            Duration::from_secs(30),
        );

        assert_eq!(txn.id(), TransactionId(1));
        assert_eq!(txn.state(), TransactionState::Active);
        assert!(!txn.is_read_only());
    }

    #[tokio::test]
    async fn test_transaction_read_write() {
        let (store, vm, lm) = create_test_deps();
        let wal = Arc::new(TransactionLog::new(WalSyncMode::Background).await.unwrap());

        let txn = Transaction::new(
            TransactionId(1),
            IsolationLevel::RepeatableRead,
            0,
            store,
            vm,
            lm,
            wal,
            Duration::from_secs(30),
        );

        // Write a value
        let key = Bytes::from("test_key");
        let value = StorageValue::String(Bytes::from("test_value"));
        txn.set(key.clone(), value).await.unwrap();

        // Read it back (should come from write set)
        let result = txn.get(&key).await.unwrap();
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_transaction_delete() {
        let (store, vm, lm) = create_test_deps();
        let wal = Arc::new(TransactionLog::new(WalSyncMode::Background).await.unwrap());

        // Pre-populate store
        store.set(
            0,
            Bytes::from("key"),
            StorageValue::String(Bytes::from("value")),
        );

        let txn = Transaction::new(
            TransactionId(1),
            IsolationLevel::RepeatableRead,
            0,
            store,
            vm,
            lm,
            wal,
            Duration::from_secs(30),
        );

        // Delete the key
        txn.delete(Bytes::from("key")).await.unwrap();

        // Should read as None
        let result = txn.get(&Bytes::from("key")).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_readonly_transaction() {
        let (store, vm, lm) = create_test_deps();
        let wal = Arc::new(TransactionLog::new(WalSyncMode::Background).await.unwrap());

        let txn = Transaction::new_readonly(
            TransactionId(1),
            IsolationLevel::RepeatableRead,
            0,
            store,
            vm,
            lm,
            wal,
            Duration::from_secs(30),
        );

        assert!(txn.is_read_only());

        // Trying to write should fail
        let result = txn
            .set(
                Bytes::from("key"),
                StorageValue::String(Bytes::from("value")),
            )
            .await;
        assert!(matches!(result, Err(TransactionError::ReadOnly)));
    }

    #[tokio::test]
    async fn test_transaction_incr() {
        let (store, vm, lm) = create_test_deps();
        let wal = Arc::new(TransactionLog::new(WalSyncMode::Background).await.unwrap());

        let txn = Transaction::new(
            TransactionId(1),
            IsolationLevel::RepeatableRead,
            0,
            store,
            vm,
            lm,
            wal,
            Duration::from_secs(30),
        );

        let key = Bytes::from("counter");

        // Increment non-existent key
        let v1 = txn.incr(key.clone(), 1).await.unwrap();
        assert_eq!(v1, 1);

        // Increment again
        let v2 = txn.incr(key.clone(), 5).await.unwrap();
        assert_eq!(v2, 6);
    }
}
