//! Backup codec — compatibility-sensitive serialization and deserialization.
//!
//! Extracted from BackupManager: owns the exact byte format for backup files
//! and incremental backup segments including format constants, tags, and
//! version markers.

use std::collections::HashMap;

use bytes::{BufMut, Bytes, BytesMut};
use chrono::Utc;

use crate::storage::Value;

use super::manager::{BackupEntry, BackupError, BackupInfo, BackupManager, BackupResult};

/// Backup file header magic number
pub(super) const BACKUP_MAGIC: &[u8] = b"FERRITE_BKP";
/// Backup format version
pub(super) const BACKUP_VERSION: u8 = 1;

impl BackupManager {
    pub(super) fn count_aof_entries(&self, data: &[u8]) -> u64 {
        let mut count = 0u64;
        let mut offset = 0;

        while offset + 4 <= data.len() {
            let len = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;

            if offset + 4 + len > data.len() {
                break;
            }

            count += 1;
            offset += 4 + len;
        }

        count
    }

    /// Serialize incremental backup data
    pub(super) fn serialize_incremental(
        &self,
        aof_data: &[u8],
        base_backup: &str,
        start_sequence: u64,
    ) -> BackupResult<Vec<u8>> {
        let mut buffer = BytesMut::new();

        // Write incremental header
        buffer.put_slice(b"FERRITE_INC");
        buffer.put_u8(1); // Version

        // Write base backup reference
        let base_bytes = base_backup.as_bytes();
        buffer.put_u32(base_bytes.len() as u32);
        buffer.put_slice(base_bytes);

        // Write sequence info
        buffer.put_u64(start_sequence);

        // Write AOF data length and data
        buffer.put_u64(aof_data.len() as u64);
        buffer.put_slice(aof_data);

        // Write metadata
        let metadata = serde_json::json!({
            "created_at": Utc::now().to_rfc3339(),
            "entry_count": self.count_aof_entries(aof_data),
        });
        let metadata_json =
            serde_json::to_vec(&metadata).map_err(|e| BackupError::Serialization(e.to_string()))?;
        buffer.put_u32(metadata_json.len() as u32);
        buffer.put_slice(&metadata_json);

        Ok(buffer.to_vec())
    }

    /// Generate incremental backup name
    pub(super) fn deserialize_incremental(
        &self,
        data: &[u8],
    ) -> BackupResult<(Vec<u8>, String, u64)> {
        use std::io::Read;

        let mut cursor = std::io::Cursor::new(data);

        // Verify header
        let mut header = [0u8; 11];
        cursor.read_exact(&mut header)?;
        if &header != b"FERRITE_INC" {
            return Err(BackupError::Invalid(
                "Invalid incremental backup header".to_string(),
            ));
        }

        // Read version
        let mut version = [0u8; 1];
        cursor.read_exact(&mut version)?;
        if version[0] != 1 {
            return Err(BackupError::Invalid(format!(
                "Unsupported incremental backup version: {}",
                version[0]
            )));
        }

        // Read base backup name (big-endian to match BytesMut::put_u32)
        let mut len_buf = [0u8; 4];
        cursor.read_exact(&mut len_buf)?;
        let base_len = u32::from_be_bytes(len_buf) as usize;
        let mut base_bytes = vec![0u8; base_len];
        cursor.read_exact(&mut base_bytes)?;
        let base_backup = String::from_utf8_lossy(&base_bytes).to_string();

        // Read start sequence (big-endian to match BytesMut::put_u64)
        let mut seq_buf = [0u8; 8];
        cursor.read_exact(&mut seq_buf)?;
        let start_sequence = u64::from_be_bytes(seq_buf);

        // Read AOF data length and data (big-endian)
        cursor.read_exact(&mut seq_buf)?;
        let aof_len = u64::from_be_bytes(seq_buf) as usize;
        let mut aof_data = vec![0u8; aof_len];
        cursor.read_exact(&mut aof_data)?;

        Ok((aof_data, base_backup, start_sequence))
    }

    /// Parse raw AOF data into entries
    pub(super) fn serialize_entry(
        &self,
        buffer: &mut BytesMut,
        entry: &BackupEntry,
    ) -> BackupResult<()> {
        // Entry format:
        // - database: u8
        // - key_len: u32
        // - key: [u8]
        // - value_type: u8
        // - value_len: u32
        // - value: [u8]
        // - has_ttl: u8
        // - ttl_ms: i64 (if has_ttl)

        buffer.put_u8(entry.database);

        // Key
        let key_bytes = entry.key.as_bytes();
        buffer.put_u32(key_bytes.len() as u32);
        buffer.put_slice(key_bytes);

        // Value
        match &entry.value {
            Value::String(s) => {
                buffer.put_u8(0); // String type
                buffer.put_u32(s.len() as u32);
                buffer.put_slice(s);
            }
            Value::List(list) => {
                buffer.put_u8(1); // List type
                buffer.put_u32(list.len() as u32);
                for item in list {
                    buffer.put_u32(item.len() as u32);
                    buffer.put_slice(item);
                }
            }
            Value::Hash(hash) => {
                buffer.put_u8(2); // Hash type
                buffer.put_u32(hash.len() as u32);
                for (k, v) in hash {
                    buffer.put_u32(k.len() as u32);
                    buffer.put_slice(k);
                    buffer.put_u32(v.len() as u32);
                    buffer.put_slice(v);
                }
            }
            Value::Set(set) => {
                buffer.put_u8(3); // Set type
                buffer.put_u32(set.len() as u32);
                for item in set {
                    buffer.put_u32(item.len() as u32);
                    buffer.put_slice(item);
                }
            }
            Value::SortedSet { by_member, .. } => {
                buffer.put_u8(4); // SortedSet type
                buffer.put_u32(by_member.len() as u32);
                for (member, score) in by_member {
                    buffer.put_u32(member.len() as u32);
                    buffer.put_slice(member);
                    buffer.put_f64(*score);
                }
            }
            Value::Stream(stream) => {
                buffer.put_u8(5); // Stream type
                buffer.put_u32(stream.entries.len() as u32);
                for (id, fields) in &stream.entries {
                    // Write entry ID
                    buffer.put_u64(id.ms);
                    buffer.put_u64(id.seq);
                    // Write fields
                    buffer.put_u32(fields.len() as u32);
                    for (k, v) in fields {
                        buffer.put_u32(k.len() as u32);
                        buffer.put_slice(k);
                        buffer.put_u32(v.len() as u32);
                        buffer.put_slice(v);
                    }
                }
                // Write last_id
                buffer.put_u64(stream.last_id.ms);
                buffer.put_u64(stream.last_id.seq);
            }
            Value::HyperLogLog(registers) => {
                buffer.put_u8(6); // HyperLogLog type
                buffer.put_u32(registers.len() as u32);
                buffer.put_slice(registers);
            }
        }

        // TTL
        match entry.ttl {
            Some(ttl) => {
                buffer.put_u8(1);
                buffer.put_i64(ttl);
            }
            None => {
                buffer.put_u8(0);
            }
        }

        Ok(())
    }

    /// Deserialize a backup
    pub(super) fn deserialize_backup(
        &self,
        data: &[u8],
    ) -> BackupResult<(Vec<BackupEntry>, BackupInfo)> {
        let mut cursor = std::io::Cursor::new(data);
        let mut buf = [0u8; 11];

        // Read and verify header
        std::io::Read::read_exact(&mut cursor, &mut buf[..BACKUP_MAGIC.len()])
            .map_err(|e| BackupError::Invalid(format!("Failed to read header: {}", e)))?;

        if &buf[..BACKUP_MAGIC.len()] != BACKUP_MAGIC {
            return Err(BackupError::Invalid(
                "Invalid backup magic number".to_string(),
            ));
        }

        let version = {
            let mut v = [0u8; 1];
            std::io::Read::read_exact(&mut cursor, &mut v)
                .map_err(|e| BackupError::Invalid(format!("Failed to read version: {}", e)))?;
            v[0]
        };

        if version != BACKUP_VERSION {
            return Err(BackupError::Invalid(format!(
                "Unsupported backup version: {}",
                version
            )));
        }

        // Read entry count
        let entry_count = {
            let mut buf = [0u8; 8];
            std::io::Read::read_exact(&mut cursor, &mut buf)
                .map_err(|e| BackupError::Invalid(format!("Failed to read entry count: {}", e)))?;
            u64::from_be_bytes(buf)
        };

        // Read entries
        let mut entries = Vec::with_capacity(entry_count as usize);
        for _ in 0..entry_count {
            let entry = self.deserialize_entry(&mut cursor)?;
            entries.push(entry);
        }

        // Read metadata
        let metadata_len = {
            let mut buf = [0u8; 4];
            std::io::Read::read_exact(&mut cursor, &mut buf).map_err(|e| {
                BackupError::Invalid(format!("Failed to read metadata length: {}", e))
            })?;
            u32::from_be_bytes(buf) as usize
        };

        let mut metadata_buf = vec![0u8; metadata_len];
        std::io::Read::read_exact(&mut cursor, &mut metadata_buf)
            .map_err(|e| BackupError::Invalid(format!("Failed to read metadata: {}", e)))?;

        let metadata: BackupInfo = serde_json::from_slice(&metadata_buf)
            .map_err(|e| BackupError::Serialization(e.to_string()))?;

        Ok((entries, metadata))
    }

    /// Deserialize a single entry
    pub(super) fn deserialize_entry(
        &self,
        cursor: &mut std::io::Cursor<&[u8]>,
    ) -> BackupResult<BackupEntry> {
        use std::io::Read;

        // Database
        let database = {
            let mut buf = [0u8; 1];
            cursor.read_exact(&mut buf)?;
            buf[0]
        };

        // Key
        let key_len = {
            let mut buf = [0u8; 4];
            cursor.read_exact(&mut buf)?;
            u32::from_be_bytes(buf) as usize
        };
        let mut key_buf = vec![0u8; key_len];
        cursor.read_exact(&mut key_buf)?;
        let key = String::from_utf8_lossy(&key_buf).to_string();

        // Value type
        let value_type = {
            let mut buf = [0u8; 1];
            cursor.read_exact(&mut buf)?;
            buf[0]
        };

        let value = match value_type {
            0 => {
                // String
                let len = {
                    let mut buf = [0u8; 4];
                    cursor.read_exact(&mut buf)?;
                    u32::from_be_bytes(buf) as usize
                };
                let mut data = vec![0u8; len];
                cursor.read_exact(&mut data)?;
                Value::String(Bytes::from(data))
            }
            1 => {
                // List
                let count = {
                    let mut buf = [0u8; 4];
                    cursor.read_exact(&mut buf)?;
                    u32::from_be_bytes(buf) as usize
                };
                let mut list = std::collections::VecDeque::with_capacity(count);
                for _ in 0..count {
                    let len = {
                        let mut buf = [0u8; 4];
                        cursor.read_exact(&mut buf)?;
                        u32::from_be_bytes(buf) as usize
                    };
                    let mut data = vec![0u8; len];
                    cursor.read_exact(&mut data)?;
                    list.push_back(Bytes::from(data));
                }
                Value::List(list)
            }
            2 => {
                // Hash
                let count = {
                    let mut buf = [0u8; 4];
                    cursor.read_exact(&mut buf)?;
                    u32::from_be_bytes(buf) as usize
                };
                let mut hash = HashMap::with_capacity(count);
                for _ in 0..count {
                    let key_len = {
                        let mut buf = [0u8; 4];
                        cursor.read_exact(&mut buf)?;
                        u32::from_be_bytes(buf) as usize
                    };
                    let mut key_data = vec![0u8; key_len];
                    cursor.read_exact(&mut key_data)?;

                    let val_len = {
                        let mut buf = [0u8; 4];
                        cursor.read_exact(&mut buf)?;
                        u32::from_be_bytes(buf) as usize
                    };
                    let mut val_data = vec![0u8; val_len];
                    cursor.read_exact(&mut val_data)?;

                    hash.insert(Bytes::from(key_data), Bytes::from(val_data));
                }
                Value::Hash(hash)
            }
            3 => {
                // Set
                let count = {
                    let mut buf = [0u8; 4];
                    cursor.read_exact(&mut buf)?;
                    u32::from_be_bytes(buf) as usize
                };
                let mut set = std::collections::HashSet::with_capacity(count);
                for _ in 0..count {
                    let len = {
                        let mut buf = [0u8; 4];
                        cursor.read_exact(&mut buf)?;
                        u32::from_be_bytes(buf) as usize
                    };
                    let mut data = vec![0u8; len];
                    cursor.read_exact(&mut data)?;
                    set.insert(Bytes::from(data));
                }
                Value::Set(set)
            }
            4 => {
                // SortedSet
                let count = {
                    let mut buf = [0u8; 4];
                    cursor.read_exact(&mut buf)?;
                    u32::from_be_bytes(buf) as usize
                };
                let mut by_member = HashMap::with_capacity(count);
                let mut by_score = std::collections::BTreeMap::new();
                for _ in 0..count {
                    let member_len = {
                        let mut buf = [0u8; 4];
                        cursor.read_exact(&mut buf)?;
                        u32::from_be_bytes(buf) as usize
                    };
                    let mut member_data = vec![0u8; member_len];
                    cursor.read_exact(&mut member_data)?;
                    let member = Bytes::from(member_data);

                    let score = {
                        let mut buf = [0u8; 8];
                        cursor.read_exact(&mut buf)?;
                        f64::from_be_bytes(buf)
                    };

                    by_member.insert(member.clone(), score);
                    by_score.insert((ordered_float::OrderedFloat(score), member), ());
                }
                Value::SortedSet {
                    by_score,
                    by_member,
                }
            }
            5 => {
                // Stream
                use crate::storage::{Stream, StreamEntryId};
                use std::collections::BTreeMap;

                let entry_count = {
                    let mut buf = [0u8; 4];
                    cursor.read_exact(&mut buf)?;
                    u32::from_be_bytes(buf) as usize
                };

                let mut entries: BTreeMap<StreamEntryId, Vec<(Bytes, Bytes)>> = BTreeMap::new();
                for _ in 0..entry_count {
                    // Read entry ID
                    let ms = {
                        let mut buf = [0u8; 8];
                        cursor.read_exact(&mut buf)?;
                        u64::from_be_bytes(buf)
                    };
                    let seq = {
                        let mut buf = [0u8; 8];
                        cursor.read_exact(&mut buf)?;
                        u64::from_be_bytes(buf)
                    };
                    let id = StreamEntryId::new(ms, seq);

                    // Read fields
                    let field_count = {
                        let mut buf = [0u8; 4];
                        cursor.read_exact(&mut buf)?;
                        u32::from_be_bytes(buf) as usize
                    };

                    let mut fields = Vec::with_capacity(field_count);
                    for _ in 0..field_count {
                        let key_len = {
                            let mut buf = [0u8; 4];
                            cursor.read_exact(&mut buf)?;
                            u32::from_be_bytes(buf) as usize
                        };
                        let mut key_data = vec![0u8; key_len];
                        cursor.read_exact(&mut key_data)?;

                        let val_len = {
                            let mut buf = [0u8; 4];
                            cursor.read_exact(&mut buf)?;
                            u32::from_be_bytes(buf) as usize
                        };
                        let mut val_data = vec![0u8; val_len];
                        cursor.read_exact(&mut val_data)?;

                        fields.push((Bytes::from(key_data), Bytes::from(val_data)));
                    }

                    entries.insert(id, fields);
                }

                // Read last_id
                let last_ms = {
                    let mut buf = [0u8; 8];
                    cursor.read_exact(&mut buf)?;
                    u64::from_be_bytes(buf)
                };
                let last_seq = {
                    let mut buf = [0u8; 8];
                    cursor.read_exact(&mut buf)?;
                    u64::from_be_bytes(buf)
                };

                let stream = Stream {
                    entries,
                    last_id: StreamEntryId::new(last_ms, last_seq),
                    length: entry_count,
                    consumer_groups: std::collections::HashMap::new(),
                };
                Value::Stream(stream)
            }
            6 => {
                // HyperLogLog
                let len = {
                    let mut buf = [0u8; 4];
                    cursor.read_exact(&mut buf)?;
                    u32::from_be_bytes(buf) as usize
                };
                let mut registers = vec![0u8; len];
                cursor.read_exact(&mut registers)?;
                Value::HyperLogLog(registers)
            }
            _ => {
                return Err(BackupError::Invalid(format!(
                    "Unknown value type: {}",
                    value_type
                )))
            }
        };

        // TTL
        let has_ttl = {
            let mut buf = [0u8; 1];
            cursor.read_exact(&mut buf)?;
            buf[0] == 1
        };

        let ttl = if has_ttl {
            let mut buf = [0u8; 8];
            cursor.read_exact(&mut buf)?;
            Some(i64::from_be_bytes(buf))
        } else {
            None
        };

        Ok(BackupEntry {
            database,
            key,
            value,
            ttl,
        })
    }
}
