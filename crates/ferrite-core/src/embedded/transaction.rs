//! Transaction support for embedded database

use super::database::Database;
use super::error::{EmbeddedError, Result};
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, Ordering};

/// Transaction context for atomic operations
pub struct Transaction<'a> {
    db: &'a Database,
    queued_ops: Vec<QueuedOperation>,
    is_active: AtomicBool,
}

/// A queued operation in a transaction
enum QueuedOperation {
    Set {
        key: Bytes,
        value: Bytes,
    },
    Del {
        key: Bytes,
    },
    Incr {
        key: Bytes,
        delta: i64,
    },
    HSet {
        key: Bytes,
        field: Bytes,
        value: Bytes,
    },
    HDel {
        key: Bytes,
        field: Bytes,
    },
    LPush {
        key: Bytes,
        values: Vec<Bytes>,
    },
    RPush {
        key: Bytes,
        values: Vec<Bytes>,
    },
    SAdd {
        key: Bytes,
        members: Vec<Bytes>,
    },
    SRem {
        key: Bytes,
        members: Vec<Bytes>,
    },
}

impl<'a> Transaction<'a> {
    /// Create a new transaction
    pub(crate) fn new(db: &'a Database) -> Self {
        Self {
            db,
            queued_ops: Vec::new(),
            is_active: AtomicBool::new(true),
        }
    }

    /// Check if transaction is still active
    fn check_active(&self) -> Result<()> {
        if !self.is_active.load(Ordering::Acquire) {
            Err(EmbeddedError::Transaction(
                "transaction is no longer active".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    /// Queue a SET operation
    pub fn set<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, value: V) -> Result<()> {
        self.check_active()?;
        self.queued_ops.push(QueuedOperation::Set {
            key: Bytes::copy_from_slice(key.as_ref()),
            value: Bytes::copy_from_slice(value.as_ref()),
        });
        Ok(())
    }

    /// Queue a DEL operation
    pub fn del<K: AsRef<[u8]>>(&mut self, key: K) -> Result<()> {
        self.check_active()?;
        self.queued_ops.push(QueuedOperation::Del {
            key: Bytes::copy_from_slice(key.as_ref()),
        });
        Ok(())
    }

    /// Queue an INCR operation
    pub fn incr<K: AsRef<[u8]>>(&mut self, key: K) -> Result<()> {
        self.incr_by(key, 1)
    }

    /// Queue an INCRBY operation
    pub fn incr_by<K: AsRef<[u8]>>(&mut self, key: K, delta: i64) -> Result<()> {
        self.check_active()?;
        self.queued_ops.push(QueuedOperation::Incr {
            key: Bytes::copy_from_slice(key.as_ref()),
            delta,
        });
        Ok(())
    }

    /// Queue an HSET operation
    pub fn hset<K: AsRef<[u8]>, F: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        key: K,
        field: F,
        value: V,
    ) -> Result<()> {
        self.check_active()?;
        self.queued_ops.push(QueuedOperation::HSet {
            key: Bytes::copy_from_slice(key.as_ref()),
            field: Bytes::copy_from_slice(field.as_ref()),
            value: Bytes::copy_from_slice(value.as_ref()),
        });
        Ok(())
    }

    /// Queue an HDEL operation
    pub fn hdel<K: AsRef<[u8]>, F: AsRef<[u8]>>(&mut self, key: K, field: F) -> Result<()> {
        self.check_active()?;
        self.queued_ops.push(QueuedOperation::HDel {
            key: Bytes::copy_from_slice(key.as_ref()),
            field: Bytes::copy_from_slice(field.as_ref()),
        });
        Ok(())
    }

    /// Queue an LPUSH operation
    pub fn lpush<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, values: &[V]) -> Result<()> {
        self.check_active()?;
        self.queued_ops.push(QueuedOperation::LPush {
            key: Bytes::copy_from_slice(key.as_ref()),
            values: values
                .iter()
                .map(|v| Bytes::copy_from_slice(v.as_ref()))
                .collect(),
        });
        Ok(())
    }

    /// Queue an RPUSH operation
    pub fn rpush<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, values: &[V]) -> Result<()> {
        self.check_active()?;
        self.queued_ops.push(QueuedOperation::RPush {
            key: Bytes::copy_from_slice(key.as_ref()),
            values: values
                .iter()
                .map(|v| Bytes::copy_from_slice(v.as_ref()))
                .collect(),
        });
        Ok(())
    }

    /// Queue a SADD operation
    pub fn sadd<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, members: &[V]) -> Result<()> {
        self.check_active()?;
        self.queued_ops.push(QueuedOperation::SAdd {
            key: Bytes::copy_from_slice(key.as_ref()),
            members: members
                .iter()
                .map(|v| Bytes::copy_from_slice(v.as_ref()))
                .collect(),
        });
        Ok(())
    }

    /// Queue a SREM operation
    pub fn srem<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, members: &[V]) -> Result<()> {
        self.check_active()?;
        self.queued_ops.push(QueuedOperation::SRem {
            key: Bytes::copy_from_slice(key.as_ref()),
            members: members
                .iter()
                .map(|v| Bytes::copy_from_slice(v.as_ref()))
                .collect(),
        });
        Ok(())
    }

    /// Read operation - GET (reads are immediate, not queued)
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>> {
        self.check_active()?;
        self.db.get(key)
    }

    /// Read operation - HGET (reads are immediate, not queued)
    pub fn hget<K: AsRef<[u8]>, F: AsRef<[u8]>>(
        &self,
        key: K,
        field: F,
    ) -> Result<Option<Vec<u8>>> {
        self.check_active()?;
        self.db.hget(key, field)
    }

    /// Read operation - EXISTS (reads are immediate, not queued)
    pub fn exists<K: AsRef<[u8]>>(&self, keys: &[K]) -> Result<usize> {
        self.check_active()?;
        self.db.exists(keys)
    }

    /// Get the number of queued operations
    pub fn queue_len(&self) -> usize {
        self.queued_ops.len()
    }

    /// Discard all queued operations
    pub fn discard(&mut self) {
        self.queued_ops.clear();
        self.is_active.store(false, Ordering::Release);
    }

    /// Execute all queued operations atomically
    pub(crate) fn execute(mut self) -> Result<usize> {
        self.is_active.store(false, Ordering::Release);

        let ops_count = self.queued_ops.len();

        for op in self.queued_ops.drain(..) {
            match op {
                QueuedOperation::Set { key, value } => {
                    self.db.set(&key[..], &value[..])?;
                }
                QueuedOperation::Del { key } => {
                    self.db.del(&[&key[..]])?;
                }
                QueuedOperation::Incr { key, delta } => {
                    self.db.incr_by(&key[..], delta)?;
                }
                QueuedOperation::HSet { key, field, value } => {
                    self.db.hset(&key[..], &field[..], &value[..])?;
                }
                QueuedOperation::HDel { key, field } => {
                    self.db.hdel(&key[..], &[&field[..]])?;
                }
                QueuedOperation::LPush { key, values } => {
                    let vals: Vec<&[u8]> = values.iter().map(|v| &v[..]).collect();
                    self.db.lpush(&key[..], &vals)?;
                }
                QueuedOperation::RPush { key, values } => {
                    let vals: Vec<&[u8]> = values.iter().map(|v| &v[..]).collect();
                    self.db.rpush(&key[..], &vals)?;
                }
                QueuedOperation::SAdd { key, members } => {
                    let mems: Vec<&[u8]> = members.iter().map(|v| &v[..]).collect();
                    self.db.sadd(&key[..], &mems)?;
                }
                QueuedOperation::SRem { key, members } => {
                    let mems: Vec<&[u8]> = members.iter().map(|v| &v[..]).collect();
                    self.db.srem(&key[..], &mems)?;
                }
            }
        }

        Ok(ops_count)
    }
}

impl Database {
    /// Execute operations atomically in a transaction
    pub fn transaction<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut Transaction<'_>) -> Result<T>,
    {
        let mut tx = Transaction::new(self);
        let result = f(&mut tx)?;
        tx.execute()?;
        Ok(result)
    }

    /// Begin a transaction (alternative API)
    pub fn begin_transaction(&self) -> Transaction<'_> {
        Transaction::new(self)
    }

    /// Execute and commit a transaction
    pub fn commit_transaction(&self, tx: Transaction<'_>) -> Result<usize> {
        tx.execute()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_basic() {
        let db = Database::memory().unwrap();

        let result = db.transaction(|tx| {
            tx.set("key1", "value1")?;
            tx.set("key2", "value2")?;
            tx.incr("counter")?;
            Ok(())
        });

        assert!(result.is_ok());
        assert_eq!(db.get("key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(db.get("key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(db.get_str("counter").unwrap(), Some("1".to_string()));
    }

    #[test]
    fn test_transaction_with_reads() {
        let db = Database::memory().unwrap();
        db.set("balance", "100").unwrap();

        let result = db.transaction(|tx| {
            let balance_bytes = tx.get("balance")?.unwrap_or_default();
            let balance: i64 = String::from_utf8_lossy(&balance_bytes).parse().unwrap_or(0);

            if balance >= 50 {
                tx.incr_by("balance", -50)?;
                Ok(true)
            } else {
                Ok(false)
            }
        });

        assert!(result.unwrap());
        assert_eq!(db.get_str("balance").unwrap(), Some("50".to_string()));
    }

    #[test]
    fn test_transaction_discard() {
        let db = Database::memory().unwrap();
        db.set("key", "original").unwrap();

        let mut tx = db.begin_transaction();
        tx.set("key", "modified").unwrap();
        tx.discard();

        // Transaction was discarded, original value should remain
        assert_eq!(db.get("key").unwrap(), Some(b"original".to_vec()));
    }

    #[test]
    fn test_transaction_queue_len() {
        let db = Database::memory().unwrap();
        let mut tx = db.begin_transaction();

        assert_eq!(tx.queue_len(), 0);
        tx.set("key1", "value1").unwrap();
        assert_eq!(tx.queue_len(), 1);
        tx.set("key2", "value2").unwrap();
        assert_eq!(tx.queue_len(), 2);
    }

    #[test]
    fn test_transaction_hash_ops() {
        let db = Database::memory().unwrap();

        db.transaction(|tx| {
            tx.hset("user:1", "name", "Alice")?;
            tx.hset("user:1", "email", "alice@example.com")?;
            Ok(())
        })
        .unwrap();

        assert_eq!(db.hget("user:1", "name").unwrap(), Some(b"Alice".to_vec()));
        assert_eq!(
            db.hget("user:1", "email").unwrap(),
            Some(b"alice@example.com".to_vec())
        );
    }

    #[test]
    fn test_transaction_list_ops() {
        let db = Database::memory().unwrap();

        db.transaction(|tx| {
            tx.rpush("queue", &["item1", "item2"])?;
            tx.lpush("queue", &["item0"])?;
            Ok(())
        })
        .unwrap();

        assert_eq!(db.llen("queue").unwrap(), 3);
    }

    #[test]
    fn test_transaction_set_ops() {
        let db = Database::memory().unwrap();

        db.transaction(|tx| {
            tx.sadd("tags", &["rust", "redis"])?;
            Ok(())
        })
        .unwrap();

        assert_eq!(db.scard("tags").unwrap(), 2);
    }
}
