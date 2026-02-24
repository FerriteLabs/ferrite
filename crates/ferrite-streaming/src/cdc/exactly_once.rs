//! Exactly-Once Delivery Semantics
//!
//! Provides idempotent producers, transactional writes, consumer offset commits,
//! and an exactly-once coordinator that ties them together.

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;
use thiserror::Error;
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors related to exactly-once processing.
#[derive(Debug, Error)]
pub enum ExactlyOnceError {
    #[error("duplicate sequence number {seq} for producer {producer_id} (expected {expected})")]
    DuplicateSequence {
        producer_id: String,
        seq: u64,
        expected: u64,
    },

    #[error("out-of-order sequence number {seq} for producer {producer_id} (expected {expected})")]
    OutOfOrderSequence {
        producer_id: String,
        seq: u64,
        expected: u64,
    },

    #[error("transaction {tx_id} is not active")]
    TransactionNotActive { tx_id: u64 },

    #[error("transaction {tx_id} already exists")]
    TransactionExists { tx_id: u64 },

    #[error("consumer group '{group}' offset commit failed: {reason}")]
    OffsetCommitFailed { group: String, reason: String },
}

// ---------------------------------------------------------------------------
// IdempotentProducer
// ---------------------------------------------------------------------------

/// Tracks per-producer sequence numbers to detect duplicates.
pub struct IdempotentProducer {
    /// producer_id → next expected sequence number
    sequences: DashMap<String, u64>,
    /// Number of duplicates detected.
    duplicates_detected: AtomicU64,
}

impl IdempotentProducer {
    pub fn new() -> Self {
        Self {
            sequences: DashMap::new(),
            duplicates_detected: AtomicU64::new(0),
        }
    }

    /// Validate and advance the sequence for a producer.
    ///
    /// Returns `Ok(())` if the sequence is valid (the expected next one).
    pub fn check_and_advance(
        &self,
        producer_id: &str,
        sequence: u64,
    ) -> Result<(), ExactlyOnceError> {
        let mut entry = self.sequences.entry(producer_id.to_string()).or_insert(0);
        let expected = *entry;

        if sequence < expected {
            self.duplicates_detected.fetch_add(1, Ordering::Relaxed);
            return Err(ExactlyOnceError::DuplicateSequence {
                producer_id: producer_id.to_string(),
                seq: sequence,
                expected,
            });
        }

        if sequence > expected {
            return Err(ExactlyOnceError::OutOfOrderSequence {
                producer_id: producer_id.to_string(),
                seq: sequence,
                expected,
            });
        }

        *entry = expected + 1;
        Ok(())
    }

    /// Get the next expected sequence for a producer.
    pub fn next_sequence(&self, producer_id: &str) -> u64 {
        self.sequences.get(producer_id).map(|v| *v).unwrap_or(0)
    }

    /// Reset sequence tracking for a producer.
    pub fn reset(&self, producer_id: &str) {
        self.sequences.remove(producer_id);
    }

    /// Total duplicate detections.
    pub fn duplicates_detected(&self) -> u64 {
        self.duplicates_detected.load(Ordering::Relaxed)
    }
}

impl Default for IdempotentProducer {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// TransactionState / TransactionalWriter
// ---------------------------------------------------------------------------

/// State of a transaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    Active,
    Committed,
    Aborted,
}

/// A buffered event inside a transaction.
#[derive(Clone, Debug)]
pub struct TransactionEvent {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: SystemTime,
}

/// An in-flight transaction.
struct Transaction {
    #[allow(dead_code)] // Planned for v0.2 — stored for transaction deduplication
    id: u64,
    #[allow(dead_code)] // Planned for v0.2 — stored for transaction attribution
    producer_id: String,
    state: TransactionState,
    events: Vec<TransactionEvent>,
    #[allow(dead_code)] // Planned for v0.2 — stored for transaction timeout tracking
    created_at: SystemTime,
}

/// Provides atomic batch writes (transactions).
pub struct TransactionalWriter {
    /// Active transactions.
    transactions: RwLock<BTreeMap<u64, Transaction>>,
    /// Next transaction ID.
    next_tx_id: AtomicU64,
    /// Committed transaction count.
    committed: AtomicU64,
    /// Aborted transaction count.
    aborted: AtomicU64,
}

impl TransactionalWriter {
    pub fn new() -> Self {
        Self {
            transactions: RwLock::new(BTreeMap::new()),
            next_tx_id: AtomicU64::new(1),
            committed: AtomicU64::new(0),
            aborted: AtomicU64::new(0),
        }
    }

    /// Begin a new transaction. Returns the transaction ID.
    pub async fn begin(&self, producer_id: &str) -> Result<u64, ExactlyOnceError> {
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        let tx = Transaction {
            id: tx_id,
            producer_id: producer_id.to_string(),
            state: TransactionState::Active,
            events: Vec::new(),
            created_at: SystemTime::now(),
        };

        self.transactions.write().await.insert(tx_id, tx);
        Ok(tx_id)
    }

    /// Append an event to a transaction's buffer.
    pub async fn append(
        &self,
        tx_id: u64,
        event: TransactionEvent,
    ) -> Result<(), ExactlyOnceError> {
        let mut txns = self.transactions.write().await;
        let tx = txns
            .get_mut(&tx_id)
            .ok_or(ExactlyOnceError::TransactionNotActive { tx_id })?;

        if tx.state != TransactionState::Active {
            return Err(ExactlyOnceError::TransactionNotActive { tx_id });
        }

        tx.events.push(event);
        Ok(())
    }

    /// Commit a transaction, returning its events for the caller to persist atomically.
    pub async fn commit(&self, tx_id: u64) -> Result<Vec<TransactionEvent>, ExactlyOnceError> {
        let mut txns = self.transactions.write().await;
        let tx = txns
            .get_mut(&tx_id)
            .ok_or(ExactlyOnceError::TransactionNotActive { tx_id })?;

        if tx.state != TransactionState::Active {
            return Err(ExactlyOnceError::TransactionNotActive { tx_id });
        }

        tx.state = TransactionState::Committed;
        let events = std::mem::take(&mut tx.events);
        self.committed.fetch_add(1, Ordering::Relaxed);

        Ok(events)
    }

    /// Abort a transaction, discarding buffered events.
    pub async fn abort(&self, tx_id: u64) -> Result<(), ExactlyOnceError> {
        let mut txns = self.transactions.write().await;
        let tx = txns
            .get_mut(&tx_id)
            .ok_or(ExactlyOnceError::TransactionNotActive { tx_id })?;

        if tx.state != TransactionState::Active {
            return Err(ExactlyOnceError::TransactionNotActive { tx_id });
        }

        tx.state = TransactionState::Aborted;
        tx.events.clear();
        self.aborted.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Number of committed transactions.
    pub fn committed_count(&self) -> u64 {
        self.committed.load(Ordering::Relaxed)
    }

    /// Number of aborted transactions.
    pub fn aborted_count(&self) -> u64 {
        self.aborted.load(Ordering::Relaxed)
    }
}

impl Default for TransactionalWriter {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// ConsumerOffsetStore
// ---------------------------------------------------------------------------

/// Stores committed consumer offsets per group and partition.
pub struct ConsumerOffsetStore {
    /// (group, partition) → committed offset
    offsets: DashMap<(String, u32), u64>,
}

impl ConsumerOffsetStore {
    pub fn new() -> Self {
        Self {
            offsets: DashMap::new(),
        }
    }

    /// Commit an offset for a group/partition.
    pub fn commit(&self, group: &str, partition: u32, offset: u64) {
        self.offsets.insert((group.to_string(), partition), offset);
    }

    /// Get the committed offset for a group/partition.
    pub fn get(&self, group: &str, partition: u32) -> Option<u64> {
        self.offsets
            .get(&(group.to_string(), partition))
            .map(|v| *v)
    }

    /// Get all committed offsets for a group.
    pub fn get_all(&self, group: &str) -> HashMap<u32, u64> {
        self.offsets
            .iter()
            .filter(|e| e.key().0 == group)
            .map(|e| (e.key().1, *e.value()))
            .collect()
    }

    /// Reset offsets for a group.
    pub fn reset(&self, group: &str) {
        self.offsets.retain(|k, _| k.0 != group);
    }
}

impl Default for ConsumerOffsetStore {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// ExactlyOnceCoordinator
// ---------------------------------------------------------------------------

/// Coordinates idempotent production and consumer offset tracking
/// to achieve exactly-once semantics.
pub struct ExactlyOnceCoordinator {
    pub producer: IdempotentProducer,
    pub writer: TransactionalWriter,
    pub offsets: ConsumerOffsetStore,
}

impl ExactlyOnceCoordinator {
    pub fn new() -> Self {
        Self {
            producer: IdempotentProducer::new(),
            writer: TransactionalWriter::new(),
            offsets: ConsumerOffsetStore::new(),
        }
    }

    /// Produce a message with exactly-once semantics:
    /// validate sequence, buffer in transaction, commit atomically.
    pub async fn produce_exactly_once(
        &self,
        producer_id: &str,
        sequence: u64,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    ) -> Result<u64, ExactlyOnceError> {
        // 1. Check idempotency
        self.producer.check_and_advance(producer_id, sequence)?;

        // 2. Write via transaction
        let tx_id = self.writer.begin(producer_id).await?;
        let event = TransactionEvent {
            key,
            value,
            timestamp: SystemTime::now(),
        };
        self.writer.append(tx_id, event).await?;
        self.writer.commit(tx_id).await?;

        Ok(tx_id)
    }

    /// Consume-and-commit: commit the consumer offset atomically after processing.
    pub fn commit_consumer_offset(&self, group: &str, partition: u32, offset: u64) {
        self.offsets.commit(group, partition, offset);
    }
}

impl Default for ExactlyOnceCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_idempotent_producer_sequence() {
        let producer = IdempotentProducer::new();

        assert!(producer.check_and_advance("p1", 0).is_ok());
        assert!(producer.check_and_advance("p1", 1).is_ok());
        assert!(producer.check_and_advance("p1", 2).is_ok());

        // Duplicate
        assert!(producer.check_and_advance("p1", 1).is_err());
        assert_eq!(producer.duplicates_detected(), 1);

        // Out of order
        assert!(producer.check_and_advance("p1", 5).is_err());
    }

    #[test]
    fn test_idempotent_producer_multiple_producers() {
        let producer = IdempotentProducer::new();

        assert!(producer.check_and_advance("p1", 0).is_ok());
        assert!(producer.check_and_advance("p2", 0).is_ok());
        assert!(producer.check_and_advance("p1", 1).is_ok());
        assert!(producer.check_and_advance("p2", 1).is_ok());

        assert_eq!(producer.next_sequence("p1"), 2);
        assert_eq!(producer.next_sequence("p2"), 2);
    }

    #[tokio::test]
    async fn test_transactional_writer_commit() {
        let writer = TransactionalWriter::new();

        let tx_id = writer.begin("p1").await.unwrap();
        writer
            .append(
                tx_id,
                TransactionEvent {
                    key: b"key1".to_vec(),
                    value: Some(b"val1".to_vec()),
                    timestamp: SystemTime::now(),
                },
            )
            .await
            .unwrap();
        writer
            .append(
                tx_id,
                TransactionEvent {
                    key: b"key2".to_vec(),
                    value: Some(b"val2".to_vec()),
                    timestamp: SystemTime::now(),
                },
            )
            .await
            .unwrap();

        let events = writer.commit(tx_id).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(writer.committed_count(), 1);
    }

    #[tokio::test]
    async fn test_transactional_writer_abort() {
        let writer = TransactionalWriter::new();

        let tx_id = writer.begin("p1").await.unwrap();
        writer
            .append(
                tx_id,
                TransactionEvent {
                    key: b"key".to_vec(),
                    value: None,
                    timestamp: SystemTime::now(),
                },
            )
            .await
            .unwrap();

        writer.abort(tx_id).await.unwrap();
        assert_eq!(writer.aborted_count(), 1);

        // Cannot commit after abort
        assert!(writer.commit(tx_id).await.is_err());
    }

    #[test]
    fn test_consumer_offset_store() {
        let store = ConsumerOffsetStore::new();

        store.commit("group1", 0, 100);
        store.commit("group1", 1, 200);
        store.commit("group2", 0, 50);

        assert_eq!(store.get("group1", 0), Some(100));
        assert_eq!(store.get("group1", 1), Some(200));
        assert_eq!(store.get("group2", 0), Some(50));
        assert_eq!(store.get("group1", 99), None);

        let all = store.get_all("group1");
        assert_eq!(all.len(), 2);

        store.reset("group1");
        assert_eq!(store.get("group1", 0), None);
    }

    #[tokio::test]
    async fn test_exactly_once_coordinator() {
        let coord = ExactlyOnceCoordinator::new();

        // First message succeeds
        let tx = coord
            .produce_exactly_once("p1", 0, b"key".to_vec(), Some(b"val".to_vec()))
            .await;
        assert!(tx.is_ok());

        // Duplicate fails
        let dup = coord
            .produce_exactly_once("p1", 0, b"key".to_vec(), Some(b"val".to_vec()))
            .await;
        assert!(dup.is_err());

        // Next sequence succeeds
        let tx2 = coord
            .produce_exactly_once("p1", 1, b"key2".to_vec(), None)
            .await;
        assert!(tx2.is_ok());

        // Consumer offset commit
        coord.commit_consumer_offset("group1", 0, 42);
        assert_eq!(coord.offsets.get("group1", 0), Some(42));
    }
}
