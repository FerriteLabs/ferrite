//! Transaction Write-Ahead Log (WAL)
//!
//! Provides durability for transactions through write-ahead logging.
//! All transaction operations are logged before being applied.
//!
//! # Log Entry Format
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────┐
//! │ Header (16 bytes)                                          │
//! ├──────────┬──────────┬──────────┬──────────────────────────┤
//! │ Magic(4) │ CRC32(4) │ TxnID(8) │                          │
//! └──────────┴──────────┴──────────┤                          │
//! ├────────────────────────────────┤                          │
//! │ EntryType (1 byte)             │                          │
//! │ 0x01 = BEGIN                   │ Payload                  │
//! │ 0x02 = WRITE                   │ (variable)               │
//! │ 0x03 = PREPARE                 │                          │
//! │ 0x04 = COMMIT                  │                          │
//! │ 0x05 = ABORT                   │                          │
//! │ 0x06 = CHECKPOINT              │                          │
//! └────────────────────────────────┴──────────────────────────┘
//! ```
//!
//! # Recovery Process
//!
//! 1. Read all entries since last checkpoint
//! 2. Build list of committed transactions
//! 3. Replay writes for committed transactions
//! 4. Abort any prepared but not committed transactions

use std::collections::HashMap;

use bytes::Bytes;
use tokio::sync::{Mutex, RwLock};

use super::txn::TransactionId;
use super::{TransactionError, WalSyncMode};
use crate::storage::Value as StorageValue;

/// Log entry types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogEntryType {
    /// Transaction begin
    Begin,
    /// Write operation (set or delete)
    Write {
        /// Database index.
        db: u8,
        /// Key being written.
        key: Bytes,
        /// Value to write, or `None` for delete.
        value: Option<Bytes>,
    },
    /// 2PC prepare
    Prepare,
    /// Transaction commit
    Commit,
    /// Transaction abort
    Abort,
    /// Checkpoint with active transaction list
    Checkpoint {
        /// List of active transaction IDs at checkpoint time.
        active_txns: Vec<TransactionId>,
    },
}

impl LogEntryType {
    /// Get the type code
    pub fn type_code(&self) -> u8 {
        match self {
            LogEntryType::Begin => 0x01,
            LogEntryType::Write { .. } => 0x02,
            LogEntryType::Prepare => 0x03,
            LogEntryType::Commit => 0x04,
            LogEntryType::Abort => 0x05,
            LogEntryType::Checkpoint { .. } => 0x06,
        }
    }

    /// Parse from type code and payload
    pub fn from_bytes(type_code: u8, payload: &[u8]) -> Option<Self> {
        match type_code {
            0x01 => Some(LogEntryType::Begin),
            0x02 => {
                // Parse Write: db(1) + key_len(4) + key + value_len(4) + value
                if payload.len() < 5 {
                    return None;
                }
                let db = payload[0];
                let key_len = u32::from_le_bytes(payload[1..5].try_into().ok()?) as usize;
                if payload.len() < 5 + key_len + 4 {
                    return None;
                }
                let key = Bytes::copy_from_slice(&payload[5..5 + key_len]);
                let value_len =
                    u32::from_le_bytes(payload[5 + key_len..5 + key_len + 4].try_into().ok()?)
                        as usize;
                let value = if value_len == 0 {
                    None
                } else {
                    Some(Bytes::copy_from_slice(
                        &payload[5 + key_len + 4..5 + key_len + 4 + value_len],
                    ))
                };
                Some(LogEntryType::Write { db, key, value })
            }
            0x03 => Some(LogEntryType::Prepare),
            0x04 => Some(LogEntryType::Commit),
            0x05 => Some(LogEntryType::Abort),
            0x06 => {
                // Parse Checkpoint: count(4) + [txn_id(8)]*
                if payload.len() < 4 {
                    return None;
                }
                let count = u32::from_le_bytes(payload[0..4].try_into().ok()?) as usize;
                let mut active_txns = Vec::with_capacity(count);
                for i in 0..count {
                    let offset = 4 + i * 8;
                    if payload.len() < offset + 8 {
                        return None;
                    }
                    let txn_id = u64::from_le_bytes(payload[offset..offset + 8].try_into().ok()?);
                    active_txns.push(TransactionId(txn_id));
                }
                Some(LogEntryType::Checkpoint { active_txns })
            }
            _ => None,
        }
    }

    /// Serialize the entry type payload to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            LogEntryType::Begin
            | LogEntryType::Prepare
            | LogEntryType::Commit
            | LogEntryType::Abort => {
                // No payload
            }
            LogEntryType::Write { db, key, value } => {
                buf.push(*db);
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
                match value {
                    Some(v) => {
                        buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                        buf.extend_from_slice(v);
                    }
                    None => {
                        buf.extend_from_slice(&0u32.to_le_bytes());
                    }
                }
            }
            LogEntryType::Checkpoint { active_txns } => {
                buf.extend_from_slice(&(active_txns.len() as u32).to_le_bytes());
                for txn_id in active_txns {
                    buf.extend_from_slice(&txn_id.0.to_le_bytes());
                }
            }
        }
        buf
    }
}

/// A single log entry
#[derive(Debug, Clone)]
pub struct LogEntry {
    /// Transaction ID
    pub txn_id: TransactionId,
    /// Entry type and payload
    pub entry_type: LogEntryType,
    /// Log sequence number
    pub lsn: u64,
}

impl LogEntry {
    /// Create a new log entry
    pub fn new(txn_id: TransactionId, entry_type: LogEntryType, lsn: u64) -> Self {
        Self {
            txn_id,
            entry_type,
            lsn,
        }
    }

    /// Serialize to bytes for storage
    pub fn serialize(&self) -> Vec<u8> {
        let payload = self.entry_type.to_bytes();
        let total_len = 8 + 1 + payload.len(); // txn_id + type + payload

        let mut buf = Vec::with_capacity(4 + total_len);

        // Length prefix
        buf.extend_from_slice(&(total_len as u32).to_le_bytes());

        // Transaction ID
        buf.extend_from_slice(&self.txn_id.0.to_le_bytes());

        // Type code
        buf.push(self.entry_type.type_code());

        // Payload
        buf.extend_from_slice(&payload);

        // CRC32 at the end
        let crc = crc32fast::hash(&buf[4..]);
        buf.extend_from_slice(&crc.to_le_bytes());

        buf
    }

    /// Deserialize from bytes
    pub fn deserialize(data: &[u8], lsn: u64) -> Option<Self> {
        if data.len() < 4 + 8 + 1 + 4 {
            return None; // Minimum: length + txn_id + type + crc
        }

        let len = u32::from_le_bytes(data[0..4].try_into().ok()?) as usize;
        if data.len() < 4 + len + 4 {
            return None;
        }

        // Verify CRC
        let stored_crc = u32::from_le_bytes(data[4 + len..4 + len + 4].try_into().ok()?);
        let computed_crc = crc32fast::hash(&data[4..4 + len]);
        if stored_crc != computed_crc {
            return None; // Corrupted entry
        }

        // Parse entry
        let txn_id = TransactionId(u64::from_le_bytes(data[4..12].try_into().ok()?));
        let type_code = data[12];
        let payload = &data[13..4 + len];

        let entry_type = LogEntryType::from_bytes(type_code, payload)?;

        Some(LogEntry {
            txn_id,
            entry_type,
            lsn,
        })
    }
}

/// Transaction Write-Ahead Log
pub struct TransactionLog {
    /// Sync mode
    sync_mode: WalSyncMode,
    /// Current LSN
    next_lsn: RwLock<u64>,
    /// In-memory log buffer (for Background/Batched modes)
    buffer: Mutex<Vec<LogEntry>>,
    /// Persisted entries (for testing/recovery simulation)
    persisted: RwLock<Vec<LogEntry>>,
    /// Write batch count (for Batched sync mode)
    batch_count: Mutex<u32>,
}

impl TransactionLog {
    /// Create a new transaction log
    pub async fn new(sync_mode: WalSyncMode) -> Result<Self, TransactionError> {
        Ok(Self {
            sync_mode,
            next_lsn: RwLock::new(1),
            buffer: Mutex::new(Vec::new()),
            persisted: RwLock::new(Vec::new()),
            batch_count: Mutex::new(0),
        })
    }

    /// Get the next LSN
    async fn allocate_lsn(&self) -> u64 {
        let mut lsn = self.next_lsn.write().await;
        let current = *lsn;
        *lsn += 1;
        current
    }

    /// Append an entry to the log
    async fn append(&self, entry: LogEntry) -> Result<u64, TransactionError> {
        let lsn = entry.lsn;

        match &self.sync_mode {
            WalSyncMode::Immediate => {
                // Immediately "persist" (simulate with persisted vec)
                self.persisted.write().await.push(entry);
            }
            WalSyncMode::Batched(batch_size) => {
                let mut buffer = self.buffer.lock().await;
                buffer.push(entry);

                let mut count = self.batch_count.lock().await;
                *count += 1;

                if *count >= *batch_size {
                    // Flush batch
                    let entries = std::mem::take(&mut *buffer);
                    self.persisted.write().await.extend(entries);
                    *count = 0;
                }
            }
            WalSyncMode::Background => {
                // Just buffer, background task will flush
                self.buffer.lock().await.push(entry);
            }
        }

        Ok(lsn)
    }

    /// Log transaction begin
    pub async fn log_begin(&self, txn_id: TransactionId) -> Result<u64, TransactionError> {
        let lsn = self.allocate_lsn().await;
        let entry = LogEntry::new(txn_id, LogEntryType::Begin, lsn);
        self.append(entry).await
    }

    /// Log a write operation
    pub async fn log_write(
        &self,
        txn_id: TransactionId,
        db: u8,
        key: Bytes,
        value: Option<StorageValue>,
    ) -> Result<u64, TransactionError> {
        let lsn = self.allocate_lsn().await;

        // Convert StorageValue to Bytes for logging
        let value_bytes = value.map(|v| match v {
            StorageValue::String(b) => b,
            _ => {
                // For complex types, serialize to JSON
                Bytes::from(serde_json::to_string(&format!("{:?}", v)).unwrap_or_default())
            }
        });

        let entry = LogEntry::new(
            txn_id,
            LogEntryType::Write {
                db,
                key,
                value: value_bytes,
            },
            lsn,
        );
        self.append(entry).await
    }

    /// Log 2PC prepare
    pub async fn log_prepare(&self, txn_id: TransactionId) -> Result<u64, TransactionError> {
        let lsn = self.allocate_lsn().await;
        let entry = LogEntry::new(txn_id, LogEntryType::Prepare, lsn);
        self.append(entry).await
    }

    /// Log transaction commit
    pub async fn log_commit(&self, txn_id: TransactionId) -> Result<u64, TransactionError> {
        let lsn = self.allocate_lsn().await;
        let entry = LogEntry::new(txn_id, LogEntryType::Commit, lsn);
        self.append(entry).await
    }

    /// Log transaction abort
    pub async fn log_abort(&self, txn_id: TransactionId) -> Result<u64, TransactionError> {
        let lsn = self.allocate_lsn().await;
        let entry = LogEntry::new(txn_id, LogEntryType::Abort, lsn);
        self.append(entry).await
    }

    /// Log a checkpoint
    pub async fn log_checkpoint(
        &self,
        active_txns: Vec<TransactionId>,
    ) -> Result<u64, TransactionError> {
        // Use a special transaction ID for checkpoint entries
        let lsn = self.allocate_lsn().await;
        let entry = LogEntry::new(
            TransactionId(0),
            LogEntryType::Checkpoint { active_txns },
            lsn,
        );
        self.append(entry).await
    }

    /// Flush buffered entries to storage
    pub async fn flush(&self) -> Result<(), TransactionError> {
        let entries = std::mem::take(&mut *self.buffer.lock().await);
        self.persisted.write().await.extend(entries);
        Ok(())
    }

    /// Read all entries (for recovery)
    pub async fn read_all(&self) -> Result<Vec<LogEntry>, TransactionError> {
        // First flush any buffered entries
        self.flush().await?;

        Ok(self.persisted.read().await.clone())
    }

    /// Read entries since a given LSN
    pub async fn read_since(&self, since_lsn: u64) -> Result<Vec<LogEntry>, TransactionError> {
        self.flush().await?;

        Ok(self
            .persisted
            .read()
            .await
            .iter()
            .filter(|e| e.lsn >= since_lsn)
            .cloned()
            .collect())
    }

    /// Get the current LSN
    pub async fn current_lsn(&self) -> u64 {
        *self.next_lsn.read().await - 1
    }

    /// Get the entry count
    pub async fn entry_count(&self) -> usize {
        let buffered = self.buffer.lock().await.len();
        let persisted = self.persisted.read().await.len();
        buffered + persisted
    }

    /// Truncate log up to a given LSN (after checkpoint)
    pub async fn truncate(&self, up_to_lsn: u64) -> Result<u64, TransactionError> {
        let mut persisted = self.persisted.write().await;
        let before_len = persisted.len();
        persisted.retain(|e| e.lsn > up_to_lsn);
        Ok((before_len - persisted.len()) as u64)
    }
}

/// Recovery helper for transaction log
pub struct WalRecovery {
    /// Transaction states during recovery
    txn_states: HashMap<TransactionId, RecoveryTxnState>,
}

struct RecoveryTxnState {
    begun: bool,
    prepared: bool,
    committed: bool,
    aborted: bool,
    writes: Vec<(u8, Bytes, Option<Bytes>)>,
}

impl WalRecovery {
    /// Create a new recovery helper
    pub fn new() -> Self {
        Self {
            txn_states: HashMap::new(),
        }
    }

    /// Process a log entry during recovery
    pub fn process_entry(&mut self, entry: &LogEntry) {
        let state = self
            .txn_states
            .entry(entry.txn_id)
            .or_insert(RecoveryTxnState {
                begun: false,
                prepared: false,
                committed: false,
                aborted: false,
                writes: Vec::new(),
            });

        match &entry.entry_type {
            LogEntryType::Begin => state.begun = true,
            LogEntryType::Write { db, key, value } => {
                state.writes.push((*db, key.clone(), value.clone()));
            }
            LogEntryType::Prepare => state.prepared = true,
            LogEntryType::Commit => state.committed = true,
            LogEntryType::Abort => state.aborted = true,
            LogEntryType::Checkpoint { .. } => {}
        }
    }

    /// Get transactions that need to be replayed (committed but not yet applied)
    #[allow(clippy::type_complexity)]
    pub fn get_replay_transactions(&self) -> Vec<(TransactionId, Vec<(u8, Bytes, Option<Bytes>)>)> {
        self.txn_states
            .iter()
            .filter(|(_, state)| state.committed && !state.aborted)
            .map(|(txn_id, state)| (*txn_id, state.writes.clone()))
            .collect()
    }

    /// Get transactions that need to be rolled back (prepared but not committed)
    pub fn get_rollback_transactions(&self) -> Vec<TransactionId> {
        self.txn_states
            .iter()
            .filter(|(_, state)| state.prepared && !state.committed && !state.aborted)
            .map(|(txn_id, _)| *txn_id)
            .collect()
    }
}

impl Default for WalRecovery {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_wal_creation() {
        let wal = TransactionLog::new(WalSyncMode::Immediate).await.unwrap();
        assert_eq!(wal.entry_count().await, 0);
    }

    #[tokio::test]
    async fn test_log_begin_commit() {
        let wal = TransactionLog::new(WalSyncMode::Immediate).await.unwrap();
        let txn_id = TransactionId(1);

        let lsn1 = wal.log_begin(txn_id).await.unwrap();
        let lsn2 = wal.log_commit(txn_id).await.unwrap();

        assert!(lsn2 > lsn1);
        assert_eq!(wal.entry_count().await, 2);
    }

    #[tokio::test]
    async fn test_log_write() {
        let wal = TransactionLog::new(WalSyncMode::Immediate).await.unwrap();
        let txn_id = TransactionId(1);

        wal.log_begin(txn_id).await.unwrap();
        wal.log_write(
            txn_id,
            0,
            Bytes::from("key"),
            Some(StorageValue::String(Bytes::from("value"))),
        )
        .await
        .unwrap();
        wal.log_commit(txn_id).await.unwrap();

        let entries = wal.read_all().await.unwrap();
        assert_eq!(entries.len(), 3);
        assert!(matches!(entries[1].entry_type, LogEntryType::Write { .. }));
    }

    #[tokio::test]
    async fn test_checkpoint() {
        let wal = TransactionLog::new(WalSyncMode::Immediate).await.unwrap();

        wal.log_checkpoint(vec![TransactionId(1), TransactionId(2)])
            .await
            .unwrap();

        let entries = wal.read_all().await.unwrap();
        assert_eq!(entries.len(), 1);

        if let LogEntryType::Checkpoint { active_txns } = &entries[0].entry_type {
            assert_eq!(active_txns.len(), 2);
        } else {
            panic!("Expected checkpoint entry");
        }
    }

    #[tokio::test]
    async fn test_batched_mode() {
        let wal = TransactionLog::new(WalSyncMode::Batched(3)).await.unwrap();

        // These won't be persisted yet
        wal.log_begin(TransactionId(1)).await.unwrap();
        wal.log_begin(TransactionId(2)).await.unwrap();

        // Check persisted is empty
        assert_eq!(wal.persisted.read().await.len(), 0);

        // Third entry triggers batch flush
        wal.log_begin(TransactionId(3)).await.unwrap();
        assert_eq!(wal.persisted.read().await.len(), 3);
    }

    #[tokio::test]
    async fn test_read_since() {
        let wal = TransactionLog::new(WalSyncMode::Immediate).await.unwrap();

        let lsn1 = wal.log_begin(TransactionId(1)).await.unwrap();
        let lsn2 = wal.log_begin(TransactionId(2)).await.unwrap();
        let _lsn3 = wal.log_begin(TransactionId(3)).await.unwrap();

        let entries = wal.read_since(lsn2).await.unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn test_truncate() {
        let wal = TransactionLog::new(WalSyncMode::Immediate).await.unwrap();

        let lsn1 = wal.log_begin(TransactionId(1)).await.unwrap();
        let lsn2 = wal.log_begin(TransactionId(2)).await.unwrap();
        let _lsn3 = wal.log_begin(TransactionId(3)).await.unwrap();

        let truncated = wal.truncate(lsn2).await.unwrap();
        assert_eq!(truncated, 2);
        assert_eq!(wal.entry_count().await, 1);
    }

    #[test]
    fn test_log_entry_serialization() {
        let entry = LogEntry::new(
            TransactionId(42),
            LogEntryType::Write {
                db: 0,
                key: Bytes::from("test_key"),
                value: Some(Bytes::from("test_value")),
            },
            100,
        );

        let serialized = entry.serialize();
        let deserialized = LogEntry::deserialize(&serialized, 100).unwrap();

        assert_eq!(deserialized.txn_id, TransactionId(42));
        if let LogEntryType::Write { db, key, value } = deserialized.entry_type {
            assert_eq!(db, 0);
            assert_eq!(key, Bytes::from("test_key"));
            assert_eq!(value, Some(Bytes::from("test_value")));
        } else {
            panic!("Expected Write entry");
        }
    }

    #[test]
    fn test_recovery_helper() {
        let mut recovery = WalRecovery::new();

        // Simulate recovery entries
        recovery.process_entry(&LogEntry::new(TransactionId(1), LogEntryType::Begin, 1));
        recovery.process_entry(&LogEntry::new(
            TransactionId(1),
            LogEntryType::Write {
                db: 0,
                key: Bytes::from("k1"),
                value: Some(Bytes::from("v1")),
            },
            2,
        ));
        recovery.process_entry(&LogEntry::new(TransactionId(1), LogEntryType::Commit, 3));

        recovery.process_entry(&LogEntry::new(TransactionId(2), LogEntryType::Begin, 4));
        recovery.process_entry(&LogEntry::new(TransactionId(2), LogEntryType::Prepare, 5));
        // No commit for txn 2

        let replay = recovery.get_replay_transactions();
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].0, TransactionId(1));

        let rollback = recovery.get_rollback_transactions();
        assert_eq!(rollback.len(), 1);
        assert_eq!(rollback[0], TransactionId(2));
    }
}
