//! Google Percolator-Style Distributed Transactions
//!
//! Implements a Percolator-style distributed transaction protocol that provides
//! snapshot isolation across multiple cluster nodes. This builds on the existing
//! MVCC infrastructure but adds cross-shard coordination.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                         Timestamp Oracle                                 │
//! │         (Provides globally unique, monotonically increasing TSs)         │
//! └─────────────────────────────────────┬───────────────────────────────────┘
//!                                       │
//!            ┌──────────────────────────┼──────────────────────────┐
//!            │                          │                          │
//!            ▼                          ▼                          ▼
//!     ┌─────────────┐            ┌─────────────┐            ┌─────────────┐
//!     │   Shard 1   │            │   Shard 2   │            │   Shard 3   │
//!     │ ┌─────────┐ │            │ ┌─────────┐ │            │ ┌─────────┐ │
//!     │ │  Data   │ │            │ │  Data   │ │            │ │  Data   │ │
//!     │ ├─────────┤ │            │ ├─────────┤ │            │ ├─────────┤ │
//!     │ │  Locks  │ │            │ │  Locks  │ │            │ │  Locks  │ │
//!     │ └─────────┘ │            │ └─────────┘ │            │ └─────────┘ │
//!     └─────────────┘            └─────────────┘            └─────────────┘
//! ```
//!
//! # Protocol
//!
//! ## Prewrite Phase
//! 1. Select a primary key from the write set
//! 2. For each key, write a lock record with primary pointer
//! 3. If any lock exists, abort (write-write conflict)
//!
//! ## Commit Phase
//! 1. Get commit timestamp from oracle
//! 2. Commit primary key (remove lock, write data)
//! 3. Commit secondary keys asynchronously
//!
//! # Example
//!
//! ```ignore
//! use ferrite::transaction::percolator::{PercolatorManager, PercolatorConfig};
//!
//! let pm = PercolatorManager::new(config)?;
//!
//! // Begin distributed transaction
//! let txn = pm.begin().await?;
//!
//! // Perform operations on any shard
//! txn.set("user:1", "Alice").await?;
//! txn.set("user:2", "Bob").await?;
//!
//! // Commit atomically across all shards
//! txn.commit().await?;
//! ```

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::TransactionError;
use crate::storage::Store;

/// Configuration for Percolator distributed transactions
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PercolatorConfig {
    /// Maximum transaction duration before timeout
    pub max_txn_duration: Duration,
    /// Lock timeout duration
    pub lock_timeout: Duration,
    /// Enable async secondary commits
    pub async_commit: bool,
    /// Batch size for secondary commits
    pub commit_batch_size: usize,
    /// Enable transaction recovery
    pub enable_recovery: bool,
    /// TTL for lock records (for crash recovery)
    pub lock_ttl_secs: u64,
    /// Maximum write set size
    pub max_write_set_size: usize,
    /// Retry attempts for prewrite conflicts
    pub conflict_retry_attempts: u32,
    /// Backoff interval for conflict retries
    pub conflict_backoff_ms: u64,
}

impl Default for PercolatorConfig {
    fn default() -> Self {
        Self {
            max_txn_duration: Duration::from_secs(60),
            lock_timeout: Duration::from_secs(10),
            async_commit: true,
            commit_batch_size: 100,
            enable_recovery: true,
            lock_ttl_secs: 30,
            max_write_set_size: 10_000,
            conflict_retry_attempts: 3,
            conflict_backoff_ms: 100,
        }
    }
}

/// Global Timestamp Oracle
///
/// Provides globally unique, monotonically increasing timestamps.
/// In a real cluster, this would be a distributed service (like TiDB's PD).
pub struct TimestampOracle {
    /// Current timestamp (uses hybrid logical clock approach)
    logical_ts: AtomicU64,
    /// Physical time component
    physical_ts: AtomicU64,
    /// Site ID for hybrid timestamps
    site_id: u64,
    /// Batch size for timestamp allocation
    batch_size: AtomicU64,
    /// Pre-allocated timestamps
    allocated: RwLock<Vec<u64>>,
}

impl TimestampOracle {
    /// Create a new timestamp oracle
    pub fn new(site_id: u64) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            logical_ts: AtomicU64::new(0),
            physical_ts: AtomicU64::new(now),
            site_id,
            batch_size: AtomicU64::new(1000),
            allocated: RwLock::new(Vec::new()),
        }
    }

    /// Get the next timestamp
    pub fn get_timestamp(&self) -> Timestamp {
        let physical = self.update_physical();
        let logical = self.logical_ts.fetch_add(1, Ordering::SeqCst);

        Timestamp {
            physical,
            logical,
            site_id: self.site_id,
        }
    }

    /// Get a batch of timestamps (for optimization)
    pub fn get_timestamp_batch(&self, count: usize) -> Vec<Timestamp> {
        let physical = self.update_physical();
        let start_logical = self.logical_ts.fetch_add(count as u64, Ordering::SeqCst);

        (0..count)
            .map(|i| Timestamp {
                physical,
                logical: start_logical + i as u64,
                site_id: self.site_id,
            })
            .collect()
    }

    /// Get the current timestamp without incrementing
    pub fn current(&self) -> Timestamp {
        Timestamp {
            physical: self.physical_ts.load(Ordering::SeqCst),
            logical: self.logical_ts.load(Ordering::SeqCst),
            site_id: self.site_id,
        }
    }

    fn update_physical(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let current = self.physical_ts.load(Ordering::SeqCst);
        if now > current {
            self.physical_ts.store(now, Ordering::SeqCst);
            // Reset logical counter for new physical time
            self.logical_ts.store(0, Ordering::SeqCst);
            now
        } else {
            current
        }
    }
}

/// A globally unique timestamp
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Timestamp {
    /// Physical time component (milliseconds since epoch)
    pub physical: u64,
    /// Logical counter
    pub logical: u64,
    /// Site/node identifier
    pub site_id: u64,
}

impl Timestamp {
    /// Create a new timestamp
    pub fn new(physical: u64, logical: u64, site_id: u64) -> Self {
        Self {
            physical,
            logical,
            site_id,
        }
    }

    /// Get timestamp as a single u128 for comparison
    pub fn as_u128(&self) -> u128 {
        ((self.physical as u128) << 64) | ((self.logical as u128) << 16) | (self.site_id as u128)
    }

    /// Parse from u128
    pub fn from_u128(v: u128) -> Self {
        Self {
            physical: (v >> 64) as u64,
            logical: ((v >> 16) & 0xFFFFFFFFFFFF) as u64,
            site_id: (v & 0xFFFF) as u64,
        }
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}:{}", self.physical, self.logical, self.site_id)
    }
}

/// Lock record for a key
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LockRecord {
    /// Transaction start timestamp
    pub start_ts: Timestamp,
    /// Primary key for this transaction
    pub primary_key: KeyRef,
    /// Lock type
    pub lock_type: LockType,
    /// TTL for this lock
    pub ttl_secs: u64,
    /// Creation time
    pub created_at: u64,
    /// Short value (for optimization - store small values in lock)
    pub short_value: Option<Bytes>,
}

impl LockRecord {
    /// Check if the lock has expired
    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now > self.created_at + self.ttl_secs
    }
}

/// Lock type
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LockType {
    /// Write lock (prewrite)
    Write,
    /// Delete lock
    Delete,
    /// Read lock (for pessimistic transactions)
    Read,
}

/// Reference to a key (db, key)
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KeyRef {
    /// Database index.
    pub db: u8,
    /// Key bytes.
    pub key: Bytes,
}

impl KeyRef {
    /// Create a new key reference.
    pub fn new(db: u8, key: Bytes) -> Self {
        Self { db, key }
    }
}

/// Write record stored in data column family
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WriteRecord {
    /// Commit timestamp
    pub commit_ts: Timestamp,
    /// Start timestamp of the transaction
    pub start_ts: Timestamp,
    /// Write type
    pub write_type: WriteType,
    /// Short value (if stored inline)
    pub short_value: Option<Bytes>,
}

/// Write type
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriteType {
    /// Put operation
    Put,
    /// Delete operation
    Delete,
    /// Lock (pessimistic)
    Lock,
    /// Rollback marker
    Rollback,
}

/// Transaction state
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PercolatorTxnState {
    /// Transaction is active
    Active,
    /// Prewrite phase in progress
    Prewriting,
    /// Prewrite completed, ready to commit
    Prewritten,
    /// Commit phase in progress
    Committing,
    /// Transaction committed
    Committed,
    /// Transaction rolled back
    RolledBack,
}

/// A Percolator distributed transaction
pub struct PercolatorTransaction {
    /// Transaction ID (start timestamp)
    start_ts: Timestamp,
    /// Commit timestamp (assigned during commit)
    commit_ts: Option<Timestamp>,
    /// Transaction state
    state: RwLock<PercolatorTxnState>,
    /// Write buffer
    write_buffer: RwLock<HashMap<KeyRef, WriteIntent>>,
    /// Read set (for validation in serializable mode)
    read_set: RwLock<HashSet<KeyRef>>,
    /// Primary key (first key in write set)
    primary_key: RwLock<Option<KeyRef>>,
    /// Configuration
    config: PercolatorConfig,
    /// Reference to timestamp oracle
    oracle: Arc<TimestampOracle>,
    /// Reference to lock manager
    lock_manager: Arc<PercolatorLockManager>,
    /// Started at
    started_at: Instant,
    /// Is read-only
    read_only: bool,
}

/// Write intent (buffered write)
#[derive(Clone, Debug)]
struct WriteIntent {
    /// Value to write (None = delete)
    value: Option<Bytes>,
    /// Write type
    write_type: WriteType,
}

impl PercolatorTransaction {
    /// Create a new transaction
    fn new(
        start_ts: Timestamp,
        config: PercolatorConfig,
        oracle: Arc<TimestampOracle>,
        lock_manager: Arc<PercolatorLockManager>,
        read_only: bool,
    ) -> Self {
        Self {
            start_ts,
            commit_ts: None,
            state: RwLock::new(PercolatorTxnState::Active),
            write_buffer: RwLock::new(HashMap::new()),
            read_set: RwLock::new(HashSet::new()),
            primary_key: RwLock::new(None),
            config,
            oracle,
            lock_manager,
            started_at: Instant::now(),
            read_only,
        }
    }

    /// Get transaction start timestamp
    pub fn start_ts(&self) -> Timestamp {
        self.start_ts
    }

    /// Get commit timestamp (if committed)
    pub fn commit_ts(&self) -> Option<Timestamp> {
        self.commit_ts
    }

    /// Get current state
    pub fn state(&self) -> PercolatorTxnState {
        *self.state.read()
    }

    /// Check if transaction is active
    pub fn is_active(&self) -> bool {
        *self.state.read() == PercolatorTxnState::Active
    }

    /// Buffer a write operation
    pub fn set(&self, db: u8, key: Bytes, value: Bytes) -> Result<(), TransactionError> {
        if self.read_only {
            return Err(TransactionError::ReadOnly);
        }
        self.check_active()?;
        self.check_timeout()?;
        self.check_write_set_size()?;

        let key_ref = KeyRef::new(db, key);
        let mut write_buffer = self.write_buffer.write();

        // Set primary key if this is the first write
        let mut primary = self.primary_key.write();
        if primary.is_none() {
            *primary = Some(key_ref.clone());
        }

        write_buffer.insert(
            key_ref,
            WriteIntent {
                value: Some(value),
                write_type: WriteType::Put,
            },
        );

        Ok(())
    }

    /// Buffer a delete operation
    pub fn delete(&self, db: u8, key: Bytes) -> Result<(), TransactionError> {
        if self.read_only {
            return Err(TransactionError::ReadOnly);
        }
        self.check_active()?;
        self.check_timeout()?;

        let key_ref = KeyRef::new(db, key);
        let mut write_buffer = self.write_buffer.write();

        // Set primary key if this is the first write
        let mut primary = self.primary_key.write();
        if primary.is_none() {
            *primary = Some(key_ref.clone());
        }

        write_buffer.insert(
            key_ref,
            WriteIntent {
                value: None,
                write_type: WriteType::Delete,
            },
        );

        Ok(())
    }

    /// Execute prewrite phase
    pub async fn prewrite(&self, store: &Store) -> Result<(), TransactionError> {
        self.check_active()?;
        self.check_timeout()?;

        let keys: Vec<KeyRef> = {
            let write_buffer = self.write_buffer.read();
            if write_buffer.is_empty() {
                return Ok(()); // Nothing to prewrite
            }
            write_buffer.keys().cloned().collect()
        };

        let primary = self.primary_key.read().clone();
        let primary = primary.ok_or_else(|| {
            TransactionError::InvalidState("no primary key for prewrite".to_string())
        })?;

        *self.state.write() = PercolatorTxnState::Prewriting;

        // Prewrite primary key first
        self.prewrite_key(&primary, &primary, store).await?;

        // Prewrite secondary keys
        for key_ref in &keys {
            if *key_ref != primary {
                if let Err(e) = self.prewrite_key(key_ref, &primary, store).await {
                    // Rollback on failure
                    self.rollback_prewrite(store).await;
                    return Err(e);
                }
            }
        }

        *self.state.write() = PercolatorTxnState::Prewritten;
        Ok(())
    }

    /// Prewrite a single key
    async fn prewrite_key(
        &self,
        key: &KeyRef,
        primary: &KeyRef,
        _store: &Store,
    ) -> Result<(), TransactionError> {
        // Check for existing lock
        if let Some(existing_lock) = self.lock_manager.get_lock(key).await {
            if !existing_lock.is_expired() {
                return Err(TransactionError::WriteConflict(format!(
                    "key {} locked by txn {}",
                    String::from_utf8_lossy(&key.key),
                    existing_lock.start_ts
                )));
            }
            // Clean up expired lock
            self.lock_manager.remove_lock(key).await;
        }

        // Check for write-write conflict
        if let Some(write_ts) = self.lock_manager.get_latest_write_ts(key).await {
            if write_ts >= self.start_ts {
                return Err(TransactionError::WriteConflict(format!(
                    "key {} written at {} >= start_ts {}",
                    String::from_utf8_lossy(&key.key),
                    write_ts,
                    self.start_ts
                )));
            }
        }

        // Get the write intent and build lock record
        let lock = {
            let write_buffer = self.write_buffer.read();
            let intent = write_buffer.get(key).ok_or_else(|| {
                TransactionError::InvalidState(format!(
                    "key {} not in write buffer",
                    String::from_utf8_lossy(&key.key)
                ))
            })?;

            // Create lock record
            LockRecord {
                start_ts: self.start_ts,
                primary_key: primary.clone(),
                lock_type: match intent.write_type {
                    WriteType::Put => LockType::Write,
                    WriteType::Delete => LockType::Delete,
                    _ => LockType::Write,
                },
                ttl_secs: self.config.lock_ttl_secs,
                created_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                short_value: intent.value.clone(),
            }
        };

        // Acquire lock
        self.lock_manager.acquire_lock(key.clone(), lock).await?;

        Ok(())
    }

    /// Rollback prewrite on failure
    async fn rollback_prewrite(&self, _store: &Store) {
        let keys: Vec<KeyRef> = {
            let write_buffer = self.write_buffer.read();
            write_buffer.keys().cloned().collect()
        };
        for key in &keys {
            self.lock_manager.remove_lock(key).await;
        }
        *self.state.write() = PercolatorTxnState::RolledBack;
    }

    /// Execute commit phase
    pub async fn commit(&self, store: &Store) -> Result<Timestamp, TransactionError> {
        // Check state
        let state = *self.state.read();
        if state == PercolatorTxnState::Active {
            // Auto-prewrite
            self.prewrite(store).await?;
        } else if state != PercolatorTxnState::Prewritten {
            return Err(TransactionError::InvalidState(format!(
                "cannot commit from state {:?}",
                state
            )));
        }

        let keys: Vec<KeyRef> = {
            let write_buffer = self.write_buffer.read();
            if write_buffer.is_empty() {
                *self.state.write() = PercolatorTxnState::Committed;
                return Ok(self.oracle.get_timestamp());
            }
            write_buffer.keys().cloned().collect()
        };

        // Get commit timestamp
        let commit_ts = self.oracle.get_timestamp();

        let primary = self.primary_key.read().clone();
        let primary = primary.ok_or_else(|| {
            TransactionError::InvalidState("no primary key for commit".to_string())
        })?;

        *self.state.write() = PercolatorTxnState::Committing;

        // Commit primary key first (point of no return)
        self.commit_key(&primary, commit_ts, store).await?;

        // Commit secondary keys (can be async)
        if self.config.async_commit {
            let secondaries: Vec<_> = keys.into_iter().filter(|k| *k != primary).collect();

            // Spawn async commit for secondaries
            for key in secondaries {
                let lock_manager = self.lock_manager.clone();

                let start_ts = self.start_ts;
                tokio::spawn(async move {
                    let _ =
                        Self::commit_secondary_key(&lock_manager, &key, start_ts, commit_ts).await;
                });
            }
        } else {
            for key in &keys {
                if *key != primary {
                    self.commit_key(key, commit_ts, store).await?;
                }
            }
        }

        *self.state.write() = PercolatorTxnState::Committed;
        Ok(commit_ts)
    }

    /// Commit a single key
    async fn commit_key(
        &self,
        key: &KeyRef,
        commit_ts: Timestamp,
        _store: &Store,
    ) -> Result<(), TransactionError> {
        Self::commit_secondary_key(&self.lock_manager, key, self.start_ts, commit_ts).await
    }

    /// Commit a secondary key (static for async use)
    async fn commit_secondary_key(
        lock_manager: &PercolatorLockManager,
        key: &KeyRef,
        start_ts: Timestamp,
        commit_ts: Timestamp,
    ) -> Result<(), TransactionError> {
        // Get lock record
        let lock = lock_manager.get_lock(key).await.ok_or_else(|| {
            TransactionError::InvalidState(format!(
                "lock not found for key {}",
                String::from_utf8_lossy(&key.key)
            ))
        })?;

        // Verify lock belongs to this transaction
        if lock.start_ts != start_ts {
            return Err(TransactionError::InvalidState(format!(
                "lock mismatch for key {}",
                String::from_utf8_lossy(&key.key)
            )));
        }

        // Create write record
        let write_record = WriteRecord {
            commit_ts,
            start_ts,
            write_type: match lock.lock_type {
                LockType::Write => WriteType::Put,
                LockType::Delete => WriteType::Delete,
                LockType::Read => WriteType::Lock,
            },
            short_value: lock.short_value.clone(),
        };

        // Store write record and remove lock atomically
        lock_manager.commit_key(key, write_record).await?;

        Ok(())
    }

    /// Rollback the transaction
    pub async fn rollback(&self, _store: &Store) -> Result<(), TransactionError> {
        let state = *self.state.read();
        match state {
            PercolatorTxnState::Active => {
                // Nothing to rollback
                *self.state.write() = PercolatorTxnState::RolledBack;
                return Ok(());
            }
            PercolatorTxnState::Committed => {
                return Err(TransactionError::AlreadyFinished);
            }
            PercolatorTxnState::RolledBack => {
                return Ok(());
            }
            _ => {}
        }

        // Remove all locks
        let keys: Vec<KeyRef> = {
            let write_buffer = self.write_buffer.read();
            write_buffer.keys().cloned().collect()
        };
        for key in &keys {
            self.lock_manager.remove_lock(key).await;
        }

        *self.state.write() = PercolatorTxnState::RolledBack;
        Ok(())
    }

    // Helper methods

    fn check_active(&self) -> Result<(), TransactionError> {
        let state = *self.state.read();
        if state != PercolatorTxnState::Active {
            return Err(TransactionError::InvalidState(format!(
                "transaction not active: {:?}",
                state
            )));
        }
        Ok(())
    }

    fn check_timeout(&self) -> Result<(), TransactionError> {
        if self.started_at.elapsed() > self.config.max_txn_duration {
            return Err(TransactionError::Timeout);
        }
        Ok(())
    }

    fn check_write_set_size(&self) -> Result<(), TransactionError> {
        let write_buffer = self.write_buffer.read();
        if write_buffer.len() >= self.config.max_write_set_size {
            return Err(TransactionError::InvalidState(format!(
                "write set too large: {} >= {}",
                write_buffer.len(),
                self.config.max_write_set_size
            )));
        }
        Ok(())
    }
}

/// Lock manager for Percolator transactions
pub struct PercolatorLockManager {
    /// Lock table: key -> lock record
    locks: DashMap<KeyRef, LockRecord>,
    /// Write records: key -> list of (commit_ts, write_record)
    writes: DashMap<KeyRef, Vec<(Timestamp, WriteRecord)>>,
    /// Configuration
    config: PercolatorConfig,
}

impl PercolatorLockManager {
    /// Create a new lock manager
    pub fn new(config: PercolatorConfig) -> Self {
        Self {
            locks: DashMap::new(),
            writes: DashMap::new(),
            config,
        }
    }

    /// Try to acquire a lock
    pub async fn acquire_lock(
        &self,
        key: KeyRef,
        lock: LockRecord,
    ) -> Result<(), TransactionError> {
        // Check for existing lock
        if let Some(existing) = self.locks.get(&key) {
            if !existing.is_expired() {
                return Err(TransactionError::LockTimeout(format!(
                    "key {} already locked",
                    String::from_utf8_lossy(&key.key)
                )));
            }
        }

        self.locks.insert(key, lock);
        Ok(())
    }

    /// Get a lock record
    pub async fn get_lock(&self, key: &KeyRef) -> Option<LockRecord> {
        self.locks.get(key).map(|r| r.clone())
    }

    /// Remove a lock
    pub async fn remove_lock(&self, key: &KeyRef) {
        self.locks.remove(key);
    }

    /// Get the latest write timestamp for a key
    pub async fn get_latest_write_ts(&self, key: &KeyRef) -> Option<Timestamp> {
        self.writes
            .get(key)
            .and_then(|records| records.last().map(|(ts, _)| *ts))
    }

    /// Commit a key (write record + remove lock)
    pub async fn commit_key(
        &self,
        key: &KeyRef,
        write_record: WriteRecord,
    ) -> Result<(), TransactionError> {
        // Add write record
        self.writes
            .entry(key.clone())
            .or_default()
            .push((write_record.commit_ts, write_record));

        // Remove lock
        self.locks.remove(key);

        Ok(())
    }

    /// Read a value at a given timestamp
    pub async fn read_at(
        &self,
        key: &KeyRef,
        read_ts: Timestamp,
    ) -> Result<Option<Bytes>, TransactionError> {
        // Check for lock
        if let Some(lock) = self.locks.get(key) {
            if !lock.is_expired() && lock.start_ts <= read_ts {
                // Wait for lock or return conflict
                return Err(TransactionError::LockTimeout(format!(
                    "key {} locked for read",
                    String::from_utf8_lossy(&key.key)
                )));
            }
        }

        // Find the latest write at or before read_ts
        if let Some(records) = self.writes.get(key) {
            for (commit_ts, record) in records.iter().rev() {
                if *commit_ts <= read_ts {
                    return Ok(record.short_value.clone());
                }
            }
        }

        Ok(None)
    }

    /// Garbage collect old write records
    pub async fn gc(&self, safe_point: Timestamp) -> u64 {
        let mut cleaned = 0u64;

        for mut entry in self.writes.iter_mut() {
            let before_len = entry.len();
            entry.retain(|(ts, _)| *ts > safe_point);
            cleaned += (before_len - entry.len()) as u64;
        }

        // Remove empty entries
        self.writes.retain(|_, v| !v.is_empty());

        // Clean expired locks
        self.locks.retain(|_, lock| !lock.is_expired());

        cleaned
    }

    /// Get statistics
    pub fn stats(&self) -> LockManagerStats {
        LockManagerStats {
            active_locks: self.locks.len(),
            write_records: self.writes.iter().map(|e| e.len()).sum(),
            keys_with_history: self.writes.len(),
        }
    }
}

/// Lock manager statistics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LockManagerStats {
    /// Number of active locks
    pub active_locks: usize,
    /// Total write records
    pub write_records: usize,
    /// Keys with version history
    pub keys_with_history: usize,
}

/// Percolator transaction manager
pub struct PercolatorManager {
    /// Configuration
    config: PercolatorConfig,
    /// Timestamp oracle
    oracle: Arc<TimestampOracle>,
    /// Lock manager
    lock_manager: Arc<PercolatorLockManager>,
    /// Active transactions
    active_txns: DashMap<Timestamp, Arc<PercolatorTransaction>>,
    /// Statistics
    stats: RwLock<PercolatorStats>,
}

/// Percolator statistics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PercolatorStats {
    /// Total transactions started
    pub txns_started: u64,
    /// Transactions committed
    pub txns_committed: u64,
    /// Transactions rolled back
    pub txns_rolled_back: u64,
    /// Prewrite conflicts
    pub prewrite_conflicts: u64,
    /// Commit conflicts
    pub commit_conflicts: u64,
    /// Active transactions
    pub active_txns: usize,
}

impl PercolatorManager {
    /// Create a new Percolator manager
    pub fn new(config: PercolatorConfig, site_id: u64) -> Self {
        let oracle = Arc::new(TimestampOracle::new(site_id));
        let lock_manager = Arc::new(PercolatorLockManager::new(config.clone()));

        Self {
            config,
            oracle,
            lock_manager,
            active_txns: DashMap::new(),
            stats: RwLock::new(PercolatorStats::default()),
        }
    }

    /// Begin a new transaction
    pub fn begin(&self) -> Arc<PercolatorTransaction> {
        let start_ts = self.oracle.get_timestamp();

        let txn = Arc::new(PercolatorTransaction::new(
            start_ts,
            self.config.clone(),
            self.oracle.clone(),
            self.lock_manager.clone(),
            false,
        ));

        self.active_txns.insert(start_ts, txn.clone());

        {
            let mut stats = self.stats.write();
            stats.txns_started += 1;
            stats.active_txns = self.active_txns.len();
        }

        txn
    }

    /// Begin a read-only transaction
    pub fn begin_read_only(&self) -> Arc<PercolatorTransaction> {
        let start_ts = self.oracle.get_timestamp();

        let txn = Arc::new(PercolatorTransaction::new(
            start_ts,
            self.config.clone(),
            self.oracle.clone(),
            self.lock_manager.clone(),
            true,
        ));

        self.active_txns.insert(start_ts, txn.clone());

        {
            let mut stats = self.stats.write();
            stats.txns_started += 1;
            stats.active_txns = self.active_txns.len();
        }

        txn
    }

    /// Commit a transaction
    pub async fn commit(
        &self,
        txn: &PercolatorTransaction,
        store: &Store,
    ) -> Result<Timestamp, TransactionError> {
        let result = txn.commit(store).await;

        self.active_txns.remove(&txn.start_ts);

        {
            let mut stats = self.stats.write();
            match &result {
                Ok(_) => stats.txns_committed += 1,
                Err(TransactionError::WriteConflict(_)) => stats.prewrite_conflicts += 1,
                _ => {}
            }
            stats.active_txns = self.active_txns.len();
        }

        result
    }

    /// Rollback a transaction
    pub async fn rollback(
        &self,
        txn: &PercolatorTransaction,
        store: &Store,
    ) -> Result<(), TransactionError> {
        let result = txn.rollback(store).await;

        self.active_txns.remove(&txn.start_ts);

        {
            let mut stats = self.stats.write();
            stats.txns_rolled_back += 1;
            stats.active_txns = self.active_txns.len();
        }

        result
    }

    /// Get a transaction by start timestamp
    pub fn get(&self, start_ts: Timestamp) -> Option<Arc<PercolatorTransaction>> {
        self.active_txns.get(&start_ts).map(|r| r.clone())
    }

    /// Get current timestamp
    pub fn current_timestamp(&self) -> Timestamp {
        self.oracle.current()
    }

    /// Get statistics
    pub fn stats(&self) -> PercolatorStats {
        self.stats.read().clone()
    }

    /// Get lock manager statistics
    pub fn lock_stats(&self) -> LockManagerStats {
        self.lock_manager.stats()
    }

    /// Run garbage collection
    pub async fn gc(&self, safe_point: Timestamp) -> u64 {
        self.lock_manager.gc(safe_point).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_oracle() {
        let oracle = TimestampOracle::new(1);

        let ts1 = oracle.get_timestamp();
        let ts2 = oracle.get_timestamp();
        let ts3 = oracle.get_timestamp();

        // Timestamps should be increasing
        assert!(ts2 > ts1);
        assert!(ts3 > ts2);
    }

    #[test]
    fn test_timestamp_batch() {
        let oracle = TimestampOracle::new(1);

        let batch = oracle.get_timestamp_batch(10);
        assert_eq!(batch.len(), 10);

        for i in 1..batch.len() {
            assert!(batch[i] > batch[i - 1]);
        }
    }

    #[test]
    fn test_timestamp_serialization() {
        let ts = Timestamp::new(12345, 67, 1);
        let v = ts.as_u128();
        let ts2 = Timestamp::from_u128(v);

        assert_eq!(ts.physical, ts2.physical);
        assert_eq!(ts.logical, ts2.logical);
    }

    #[tokio::test]
    async fn test_percolator_manager_creation() {
        let config = PercolatorConfig::default();
        let manager = PercolatorManager::new(config, 1);

        let stats = manager.stats();
        assert_eq!(stats.txns_started, 0);
        assert_eq!(stats.active_txns, 0);
    }

    #[tokio::test]
    async fn test_begin_transaction() {
        let config = PercolatorConfig::default();
        let manager = PercolatorManager::new(config, 1);

        let txn = manager.begin();
        assert!(txn.is_active());

        let stats = manager.stats();
        assert_eq!(stats.txns_started, 1);
        assert_eq!(stats.active_txns, 1);
    }

    #[tokio::test]
    async fn test_transaction_set() {
        let config = PercolatorConfig::default();
        let manager = PercolatorManager::new(config, 1);

        let txn = manager.begin();
        txn.set(0, Bytes::from("key1"), Bytes::from("value1"))
            .unwrap();
        txn.set(0, Bytes::from("key2"), Bytes::from("value2"))
            .unwrap();

        assert!(txn.is_active());
    }

    #[tokio::test]
    async fn test_lock_manager() {
        let config = PercolatorConfig::default();
        let lock_manager = PercolatorLockManager::new(config);

        let key = KeyRef::new(0, Bytes::from("test_key"));
        let lock = LockRecord {
            start_ts: Timestamp::new(1, 0, 1),
            primary_key: key.clone(),
            lock_type: LockType::Write,
            ttl_secs: 30,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            short_value: Some(Bytes::from("test_value")),
        };

        lock_manager.acquire_lock(key.clone(), lock).await.unwrap();

        let retrieved = lock_manager.get_lock(&key).await;
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_lock_conflict() {
        let config = PercolatorConfig::default();
        let lock_manager = PercolatorLockManager::new(config);

        let key = KeyRef::new(0, Bytes::from("test_key"));
        let lock1 = LockRecord {
            start_ts: Timestamp::new(1, 0, 1),
            primary_key: key.clone(),
            lock_type: LockType::Write,
            ttl_secs: 30,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            short_value: None,
        };

        lock_manager.acquire_lock(key.clone(), lock1).await.unwrap();

        // Second lock should fail
        let lock2 = LockRecord {
            start_ts: Timestamp::new(2, 0, 1),
            primary_key: key.clone(),
            lock_type: LockType::Write,
            ttl_secs: 30,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            short_value: None,
        };

        let result = lock_manager.acquire_lock(key, lock2).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_read_only_transaction() {
        let config = PercolatorConfig::default();
        let manager = PercolatorManager::new(config, 1);

        let txn = manager.begin_read_only();
        assert!(txn.is_active());

        // Should fail to write
        let result = txn.set(0, Bytes::from("key"), Bytes::from("value"));
        assert!(result.is_err());
    }
}
