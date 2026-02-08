//! Append-Only File (AOF) persistence
//!
//! This module implements AOF-based durability for Ferrite.
//! Every write command is logged to the AOF file before being executed.

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::config::SyncPolicy;
#[cfg(feature = "crypto")]
use crate::crypto::{Encryption, SharedEncryption};
use crate::error::{FerriteError, Result};
use crate::storage::{Store, Value};

/// A single entry in the AOF
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AofEntry {
    /// Timestamp in milliseconds since UNIX epoch
    pub timestamp: u64,

    /// Database index (0-15)
    pub database: u8,

    /// The command that was executed
    pub command: AofCommand,
}

/// Commands that modify state and need to be persisted
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(missing_docs)]
pub enum AofCommand {
    /// SET key value
    Set { key: Vec<u8>, value: Vec<u8> },

    /// SET key value with expiration (milliseconds)
    SetEx {
        key: Vec<u8>,
        value: Vec<u8>,
        expire_ms: u64,
    },

    /// DEL key [key ...]
    Del { keys: Vec<Vec<u8>> },

    /// EXPIRE key milliseconds
    Expire { key: Vec<u8>, expire_ms: u64 },

    /// PERSIST key (remove expiration)
    Persist { key: Vec<u8> },

    /// INCR/INCRBY result stored directly
    IncrBy { key: Vec<u8>, value: i64 },

    /// APPEND key value
    Append { key: Vec<u8>, value: Vec<u8> },

    /// MSET key value [key value ...]
    MSet { pairs: Vec<(Vec<u8>, Vec<u8>)> },

    /// FLUSHDB
    FlushDb,

    /// FLUSHALL
    FlushAll,
}

impl AofEntry {
    /// Create a new AOF entry
    pub fn new(database: u8, command: AofCommand) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            timestamp,
            database,
            command,
        }
    }

    /// Serialize the entry to bytes using bincode
    pub fn serialize(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| FerriteError::Aof(e.to_string()))
    }

    /// Deserialize an entry from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        bincode::deserialize(data).map_err(|e| FerriteError::Aof(e.to_string()))
    }
}

/// AOF writer that handles persistence
pub struct AofWriter {
    /// The underlying file writer
    writer: BufWriter<File>,

    /// Sync policy
    sync_policy: SyncPolicy,

    /// Counter for entries since last sync
    entries_since_sync: usize,

    /// Last sync time
    last_sync: std::time::Instant,

    /// Optional encryption context
    #[cfg(feature = "crypto")]
    encryption: Option<SharedEncryption>,
}

impl AofWriter {
    /// Create a new AOF writer
    pub fn new<P: AsRef<Path>>(path: P, sync_policy: SyncPolicy) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        Ok(Self {
            writer: BufWriter::new(file),
            sync_policy,
            entries_since_sync: 0,
            last_sync: std::time::Instant::now(),
            #[cfg(feature = "crypto")]
            encryption: None,
        })
    }

    /// Create a new AOF writer with optional encryption
    #[cfg(feature = "crypto")]
    pub fn with_encryption<P: AsRef<Path>>(
        path: P,
        sync_policy: SyncPolicy,
        encryption: Option<SharedEncryption>,
    ) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        Ok(Self {
            writer: BufWriter::new(file),
            sync_policy,
            entries_since_sync: 0,
            last_sync: std::time::Instant::now(),
            encryption,
        })
    }

    /// Write an entry to the AOF
    pub fn write(&mut self, entry: &AofEntry) -> Result<()> {
        let data = entry.serialize()?;

        // Encrypt if enabled
        #[cfg(feature = "crypto")]
        let data = if let Some(ref encryption) = self.encryption {
            encryption.encrypt(&data)?
        } else {
            data
        };

        // Write length-prefixed entry
        let len = data.len() as u32;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&data)?;

        self.entries_since_sync += 1;

        // Handle sync based on policy
        match self.sync_policy {
            SyncPolicy::Always => {
                self.sync()?;
            }
            SyncPolicy::EverySecond => {
                if self.last_sync.elapsed() >= Duration::from_secs(1) {
                    self.sync()?;
                }
            }
            SyncPolicy::No => {
                // No explicit sync, let OS decide
            }
        }

        Ok(())
    }

    /// Flush and sync the AOF to disk
    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        self.entries_since_sync = 0;
        self.last_sync = std::time::Instant::now();
        Ok(())
    }
}

/// AOF reader for recovery
pub struct AofReader {
    /// The underlying file reader
    reader: BufReader<File>,

    /// Optional encryption context for decryption
    #[cfg(feature = "crypto")]
    encryption: Option<SharedEncryption>,
}

impl AofReader {
    /// Open an AOF file for reading
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            reader: BufReader::new(file),
            #[cfg(feature = "crypto")]
            encryption: None,
        })
    }

    /// Open an AOF file for reading with optional encryption
    #[cfg(feature = "crypto")]
    pub fn with_encryption<P: AsRef<Path>>(
        path: P,
        encryption: Option<SharedEncryption>,
    ) -> Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            reader: BufReader::new(file),
            encryption,
        })
    }

    /// Read all entries from the AOF
    pub fn read_entries(&mut self) -> Vec<AofEntry> {
        let mut entries = Vec::new();
        let mut len_buf = [0u8; 4];

        loop {
            // Read length prefix
            match io::Read::read_exact(&mut self.reader, &mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    warn!("Error reading AOF length: {}", e);
                    break;
                }
            }

            let len = u32::from_le_bytes(len_buf) as usize;

            // Read entry data
            let mut data = vec![0u8; len];
            match io::Read::read_exact(&mut self.reader, &mut data) {
                Ok(()) => {}
                Err(e) => {
                    warn!("Error reading AOF entry: {}", e);
                    break;
                }
            }

            // Decrypt if encryption is enabled and data is encrypted
            #[cfg(feature = "crypto")]
            let data = if let Some(ref encryption) = self.encryption {
                if Encryption::is_encrypted(&data) {
                    match encryption.decrypt(&data) {
                        Ok(decrypted) => decrypted,
                        Err(e) => {
                            warn!("Error decrypting AOF entry: {}", e);
                            break;
                        }
                    }
                } else {
                    // Data is not encrypted, use as-is (legacy unencrypted data)
                    data
                }
            } else if Encryption::is_encrypted(&data) {
                warn!("Encrypted AOF data found but no encryption key provided");
                break;
            } else {
                data
            };

            // Deserialize entry
            match AofEntry::deserialize(&data) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    warn!("Error deserializing AOF entry: {}", e);
                    break;
                }
            }
        }

        entries
    }
}

/// Replay AOF entries to restore state
pub fn replay_aof(entries: Vec<AofEntry>, store: &Store) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    for entry in entries {
        match entry.command {
            AofCommand::Set { key, value } => {
                store.set(
                    entry.database,
                    Bytes::from(key),
                    Value::String(Bytes::from(value)),
                );
            }
            AofCommand::SetEx {
                key,
                value,
                expire_ms,
            } => {
                // Calculate expiration from timestamp
                let expire_at = entry.timestamp + expire_ms;
                if expire_at > now {
                    // Still valid
                    let expires_at =
                        std::time::UNIX_EPOCH + Duration::from_millis(expire_at);
                    store.set_with_expiry(
                        entry.database,
                        Bytes::from(key),
                        Value::String(Bytes::from(value)),
                        expires_at,
                    );
                }
                // If expired, just skip
            }
            AofCommand::Del { keys } => {
                let keys: Vec<Bytes> = keys.into_iter().map(Bytes::from).collect();
                store.del(entry.database, &keys);
            }
            AofCommand::Expire { key, expire_ms } => {
                let expire_at = entry.timestamp + expire_ms;
                if expire_at > now {
                    let expires_at =
                        std::time::UNIX_EPOCH + Duration::from_millis(expire_at);
                    store.expire(entry.database, &Bytes::from(key), expires_at);
                }
            }
            AofCommand::Persist { key } => {
                // Remove expiration - implementation detail handled by executor
                // For now, we just log that we saw this command
                debug!(
                    "Persist replay for key {:?} - handled by storage layer",
                    key
                );
            }
            AofCommand::IncrBy { key, value } => {
                store.set(
                    entry.database,
                    Bytes::from(key),
                    Value::String(Bytes::from(value.to_string())),
                );
            }
            AofCommand::Append { key, value } => {
                let key_bytes = Bytes::from(key);
                let new_value = match store.get(entry.database, &key_bytes) {
                    Some(Value::String(data)) => {
                        let mut vec = data.to_vec();
                        vec.extend_from_slice(&value);
                        Bytes::from(vec)
                    }
                    // If it's a wrong type, just skip the append (or treat as empty)
                    _ => Bytes::from(value),
                };
                store.set(entry.database, key_bytes, Value::String(new_value));
            }
            AofCommand::MSet { pairs } => {
                for (key, value) in pairs {
                    store.set(
                        entry.database,
                        Bytes::from(key),
                        Value::String(Bytes::from(value)),
                    );
                }
            }
            AofCommand::FlushDb => {
                debug!("Replaying FlushDb for database {}", entry.database);
                store.flush_db(entry.database);
            }
            AofCommand::FlushAll => {
                debug!("Replaying FlushAll");
                store.flush_all();
            }
        }
    }
}

/// Async AOF manager that handles background writes
pub struct AofManager {
    /// Channel to send entries to the writer
    sender: mpsc::UnboundedSender<AofEntry>,
}

impl AofManager {
    /// Create a new AOF manager with a background writer task
    pub fn new<P: AsRef<Path> + Send + 'static>(path: P, sync_policy: SyncPolicy) -> Result<Self> {
        let (sender, mut receiver) = mpsc::unbounded_channel::<AofEntry>();

        // Spawn background writer task
        let path = path.as_ref().to_path_buf();
        tokio::spawn(async move {
            let mut writer = match AofWriter::new(&path, sync_policy) {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to create AOF writer: {}", e);
                    return;
                }
            };

            while let Some(entry) = receiver.recv().await {
                if let Err(e) = writer.write(&entry) {
                    error!("Failed to write AOF entry: {}", e);
                }
            }

            // Ensure final sync on shutdown
            if let Err(e) = writer.sync() {
                error!("Failed to sync AOF on shutdown: {}", e);
            }
            info!("AOF writer shutting down");
        });

        Ok(Self { sender })
    }

    /// Create a new AOF manager with optional encryption
    #[cfg(feature = "crypto")]
    pub fn with_encryption<P: AsRef<Path> + Send + 'static>(
        path: P,
        sync_policy: SyncPolicy,
        encryption: Option<SharedEncryption>,
    ) -> Result<Self> {
        let (sender, mut receiver) = mpsc::unbounded_channel::<AofEntry>();

        // Spawn background writer task
        let path = path.as_ref().to_path_buf();
        tokio::spawn(async move {
            let mut writer = match AofWriter::with_encryption(&path, sync_policy, encryption) {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to create AOF writer: {}", e);
                    return;
                }
            };

            while let Some(entry) = receiver.recv().await {
                if let Err(e) = writer.write(&entry) {
                    error!("Failed to write AOF entry: {}", e);
                }
            }

            // Ensure final sync on shutdown
            if let Err(e) = writer.sync() {
                error!("Failed to sync AOF on shutdown: {}", e);
            }
            info!("AOF writer shutting down");
        });

        Ok(Self { sender })
    }

    /// Write an entry asynchronously
    pub fn write(&self, entry: AofEntry) {
        if let Err(e) = self.sender.send(entry) {
            error!("Failed to send AOF entry: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_aof_entry_serialize_deserialize() {
        let entry = AofEntry::new(
            0,
            AofCommand::Set {
                key: b"test_key".to_vec(),
                value: b"test_value".to_vec(),
            },
        );

        let serialized = entry.serialize().unwrap();
        let deserialized = AofEntry::deserialize(&serialized).unwrap();

        assert_eq!(entry.database, deserialized.database);
        match (&entry.command, &deserialized.command) {
            (AofCommand::Set { key: k1, value: v1 }, AofCommand::Set { key: k2, value: v2 }) => {
                assert_eq!(k1, k2);
                assert_eq!(v1, v2);
            }
            _ => panic!("Command mismatch"),
        }
    }

    #[test]
    fn test_aof_write_and_read() {
        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");

        // Write entries
        {
            let mut writer = AofWriter::new(&aof_path, SyncPolicy::Always).unwrap();
            let entry1 = AofEntry::new(
                0,
                AofCommand::Set {
                    key: b"key1".to_vec(),
                    value: b"value1".to_vec(),
                },
            );
            writer.write(&entry1).unwrap();

            let entry2 = AofEntry::new(
                1,
                AofCommand::Del {
                    keys: vec![b"key2".to_vec()],
                },
            );
            writer.write(&entry2).unwrap();
            writer.sync().unwrap();
        }

        // Read entries
        let mut reader = AofReader::open(&aof_path).unwrap();
        let entries = reader.read_entries();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].database, 0);
        assert_eq!(entries[1].database, 1);

        match &entries[0].command {
            AofCommand::Set { key, value } => {
                assert_eq!(key, b"key1");
                assert_eq!(value, b"value1");
            }
            _ => panic!("Expected Set command"),
        }
    }

    #[test]
    fn test_aof_replay() {
        let store = Store::new(16);

        let entries = vec![
            AofEntry::new(
                0,
                AofCommand::Set {
                    key: b"key1".to_vec(),
                    value: b"value1".to_vec(),
                },
            ),
            AofEntry::new(
                0,
                AofCommand::Set {
                    key: b"key2".to_vec(),
                    value: b"value2".to_vec(),
                },
            ),
            AofEntry::new(
                0,
                AofCommand::Del {
                    keys: vec![b"key1".to_vec()],
                },
            ),
        ];

        replay_aof(entries, &store);

        // key1 should be deleted
        assert!(store.get(0, &Bytes::from("key1")).is_none());

        // key2 should exist
        match store.get(0, &Bytes::from("key2")) {
            Some(Value::String(v)) => assert_eq!(v, Bytes::from("value2")),
            _ => panic!("Expected key2 to exist"),
        }
    }

    #[test]
    fn test_aof_entry_all_commands() {
        // Test serialization of all command types
        let commands = vec![
            AofCommand::Set {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            },
            AofCommand::SetEx {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                expire_ms: 1000,
            },
            AofCommand::Del {
                keys: vec![b"k1".to_vec(), b"k2".to_vec()],
            },
            AofCommand::Expire {
                key: b"k".to_vec(),
                expire_ms: 1000,
            },
            AofCommand::Persist { key: b"k".to_vec() },
            AofCommand::IncrBy {
                key: b"k".to_vec(),
                value: 42,
            },
            AofCommand::Append {
                key: b"k".to_vec(),
                value: b"suffix".to_vec(),
            },
            AofCommand::MSet {
                pairs: vec![(b"k1".to_vec(), b"v1".to_vec())],
            },
            AofCommand::FlushDb,
            AofCommand::FlushAll,
        ];

        for cmd in commands {
            let entry = AofEntry::new(0, cmd);
            let serialized = entry.serialize().unwrap();
            let deserialized = AofEntry::deserialize(&serialized).unwrap();
            assert_eq!(entry.database, deserialized.database);
        }
    }

    #[test]
    fn test_aof_replay_flushdb() {
        let store = Store::new(16);

        // Set keys in database 0 and 1
        store.set(0, Bytes::from("key1"), Value::String(Bytes::from("value1")));
        store.set(0, Bytes::from("key2"), Value::String(Bytes::from("value2")));
        store.set(1, Bytes::from("key3"), Value::String(Bytes::from("value3")));

        // Verify keys exist
        assert!(store.get(0, &Bytes::from("key1")).is_some());
        assert!(store.get(0, &Bytes::from("key2")).is_some());
        assert!(store.get(1, &Bytes::from("key3")).is_some());

        // Replay FlushDb for database 0
        let entries = vec![AofEntry::new(0, AofCommand::FlushDb)];
        replay_aof(entries, &store);

        // Database 0 should be empty
        assert!(store.get(0, &Bytes::from("key1")).is_none());
        assert!(store.get(0, &Bytes::from("key2")).is_none());

        // Database 1 should still have its key
        assert!(store.get(1, &Bytes::from("key3")).is_some());
    }

    #[test]
    fn test_aof_replay_flushall() {
        let store = Store::new(16);

        // Set keys in multiple databases
        store.set(0, Bytes::from("key1"), Value::String(Bytes::from("value1")));
        store.set(1, Bytes::from("key2"), Value::String(Bytes::from("value2")));
        store.set(2, Bytes::from("key3"), Value::String(Bytes::from("value3")));

        // Verify keys exist
        assert!(store.get(0, &Bytes::from("key1")).is_some());
        assert!(store.get(1, &Bytes::from("key2")).is_some());
        assert!(store.get(2, &Bytes::from("key3")).is_some());

        // Replay FlushAll
        let entries = vec![AofEntry::new(0, AofCommand::FlushAll)];
        replay_aof(entries, &store);

        // All databases should be empty
        assert!(store.get(0, &Bytes::from("key1")).is_none());
        assert!(store.get(1, &Bytes::from("key2")).is_none());
        assert!(store.get(2, &Bytes::from("key3")).is_none());
    }

    #[test]
    #[cfg(feature = "crypto")]
    fn test_aof_encrypted_write_and_read() {
        use crate::config::EncryptionAlgorithm;
        use crate::crypto::{Encryption, EncryptionKey};
        use std::sync::Arc;

        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("encrypted.aof");

        // Create encryption context
        let key = EncryptionKey::from_bytes([0x42u8; 32]);
        let encryption = Arc::new(Encryption::new(key, EncryptionAlgorithm::ChaCha20Poly1305));

        // Write encrypted entries
        {
            let mut writer =
                AofWriter::with_encryption(&aof_path, SyncPolicy::Always, Some(encryption.clone()))
                    .unwrap();
            let entry1 = AofEntry::new(
                0,
                AofCommand::Set {
                    key: b"secret_key".to_vec(),
                    value: b"secret_value".to_vec(),
                },
            );
            writer.write(&entry1).unwrap();
            writer.sync().unwrap();
        }

        // Read encrypted entries
        let mut reader = AofReader::with_encryption(&aof_path, Some(encryption.clone())).unwrap();
        let entries = reader.read_entries();

        assert_eq!(entries.len(), 1);
        match &entries[0].command {
            AofCommand::Set { key, value } => {
                assert_eq!(key, b"secret_key");
                assert_eq!(value, b"secret_value");
            }
            _ => panic!("Expected Set command"),
        }
    }

    #[test]
    #[cfg(feature = "crypto")]
    fn test_aof_encrypted_wrong_key_fails() {
        use crate::config::EncryptionAlgorithm;
        use crate::crypto::{Encryption, EncryptionKey};
        use std::sync::Arc;

        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("encrypted_wrong_key.aof");

        // Write with one key
        let key1 = EncryptionKey::from_bytes([0x42u8; 32]);
        let encryption1 = Arc::new(Encryption::new(key1, EncryptionAlgorithm::ChaCha20Poly1305));

        {
            let mut writer = AofWriter::with_encryption(
                &aof_path,
                SyncPolicy::Always,
                Some(encryption1.clone()),
            )
            .unwrap();
            let entry = AofEntry::new(
                0,
                AofCommand::Set {
                    key: b"key".to_vec(),
                    value: b"value".to_vec(),
                },
            );
            writer.write(&entry).unwrap();
            writer.sync().unwrap();
        }

        // Try to read with different key - should fail gracefully
        let key2 = EncryptionKey::from_bytes([0x11u8; 32]);
        let encryption2 = Arc::new(Encryption::new(key2, EncryptionAlgorithm::ChaCha20Poly1305));

        let mut reader = AofReader::with_encryption(&aof_path, Some(encryption2)).unwrap();
        let entries = reader.read_entries();

        // Should return empty because decryption failed
        assert!(entries.is_empty());
    }

    #[test]
    #[cfg(feature = "crypto")]
    fn test_aof_unencrypted_read_with_encryption_key() {
        use crate::config::EncryptionAlgorithm;
        use crate::crypto::{Encryption, EncryptionKey};
        use std::sync::Arc;

        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("unencrypted.aof");

        // Write unencrypted
        {
            let mut writer = AofWriter::new(&aof_path, SyncPolicy::Always).unwrap();
            let entry = AofEntry::new(
                0,
                AofCommand::Set {
                    key: b"plain_key".to_vec(),
                    value: b"plain_value".to_vec(),
                },
            );
            writer.write(&entry).unwrap();
            writer.sync().unwrap();
        }

        // Read with encryption key - should still work (legacy compatibility)
        let key = EncryptionKey::from_bytes([0x42u8; 32]);
        let encryption = Arc::new(Encryption::new(key, EncryptionAlgorithm::ChaCha20Poly1305));

        let mut reader = AofReader::with_encryption(&aof_path, Some(encryption)).unwrap();
        let entries = reader.read_entries();

        assert_eq!(entries.len(), 1);
        match &entries[0].command {
            AofCommand::Set { key, value } => {
                assert_eq!(key, b"plain_key");
                assert_eq!(value, b"plain_value");
            }
            _ => panic!("Expected Set command"),
        }
    }
}
