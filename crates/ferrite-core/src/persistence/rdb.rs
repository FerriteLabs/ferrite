//! RDB (Redis Database) persistence format
//!
//! This module implements RDB serialization and deserialization for Ferrite.
//! The RDB format is used for snapshots and replication full sync.

use bytes::{BufMut, Bytes, BytesMut};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::io::Read;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ordered_float::OrderedFloat;

#[cfg(feature = "crypto")]
use crate::crypto::{Encryption, SharedEncryption};
use crate::error::{FerriteError, Result};
use crate::storage::expiration::systemtime_to_epoch_ms;
use crate::storage::{Store, Stream, StreamEntryId, Value};

// RDB constants
const RDB_MAGIC: &[u8] = b"REDIS";
const RDB_VERSION: &[u8] = b"0011";

// RDB opcodes
const RDB_OPCODE_AUX: u8 = 0xFA;
const RDB_OPCODE_RESIZEDB: u8 = 0xFB;
const RDB_OPCODE_EXPIRETIME_MS: u8 = 0xFC;
const RDB_OPCODE_EXPIRETIME: u8 = 0xFD;
const RDB_OPCODE_SELECTDB: u8 = 0xFE;
const RDB_OPCODE_EOF: u8 = 0xFF;

// RDB type constants
const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_LIST: u8 = 1;
const RDB_TYPE_SET: u8 = 2;
const RDB_TYPE_ZSET: u8 = 3;
const RDB_TYPE_HASH: u8 = 4;
const RDB_TYPE_ZSET_2: u8 = 5; // ZSET with scores as double
const RDB_TYPE_STREAM_LISTPACKS: u8 = 15;
const RDB_TYPE_HASH_LISTPACK: u8 = 16;
const RDB_TYPE_ZSET_LISTPACK: u8 = 17;
const RDB_TYPE_LIST_QUICKLIST_2: u8 = 18;
const RDB_TYPE_SET_LISTPACK: u8 = 20;
const RDB_TYPE_STREAM_LISTPACKS_2: u8 = 21;

// RDB encoding types
const RDB_ENC_INT8: u8 = 0;
const RDB_ENC_INT16: u8 = 1;
const RDB_ENC_INT32: u8 = 2;
const RDB_ENC_LZF: u8 = 3;

/// RDB writer for creating RDB snapshots
pub struct RdbWriter {
    buffer: BytesMut,
}

impl RdbWriter {
    /// Create a new RDB writer
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(64 * 1024),
        }
    }

    /// Write the RDB header
    pub fn write_header(&mut self) {
        self.buffer.extend_from_slice(RDB_MAGIC);
        self.buffer.extend_from_slice(RDB_VERSION);
    }

    /// Write auxiliary field
    pub fn write_aux(&mut self, key: &str, value: &str) {
        self.buffer.put_u8(RDB_OPCODE_AUX);
        self.write_string(key.as_bytes());
        self.write_string(value.as_bytes());
    }

    /// Write database selector
    pub fn write_select_db(&mut self, db: u8) {
        self.buffer.put_u8(RDB_OPCODE_SELECTDB);
        self.write_length(db as u64);
    }

    /// Write resize db info
    pub fn write_resize_db(&mut self, db_size: u64, expires_size: u64) {
        self.buffer.put_u8(RDB_OPCODE_RESIZEDB);
        self.write_length(db_size);
        self.write_length(expires_size);
    }

    /// Write expiry time in milliseconds
    pub fn write_expiry_ms(&mut self, expire_ms: u64) {
        self.buffer.put_u8(RDB_OPCODE_EXPIRETIME_MS);
        self.buffer.put_u64_le(expire_ms);
    }

    /// Write a key-value pair
    pub fn write_key_value(&mut self, key: &[u8], value: &Value, expire_ms: Option<u64>) {
        // Write expiry if present
        if let Some(expire_ms) = expire_ms {
            self.write_expiry_ms(expire_ms);
        }

        // Write type and value
        match value {
            Value::String(s) => {
                self.buffer.put_u8(RDB_TYPE_STRING);
                self.write_string(key);
                self.write_string(s);
            }
            Value::List(list) => {
                self.buffer.put_u8(RDB_TYPE_LIST);
                self.write_string(key);
                self.write_length(list.len() as u64);
                for item in list {
                    self.write_string(item);
                }
            }
            Value::Set(set) => {
                self.buffer.put_u8(RDB_TYPE_SET);
                self.write_string(key);
                self.write_length(set.len() as u64);
                for member in set {
                    self.write_string(member);
                }
            }
            Value::Hash(hash) => {
                self.buffer.put_u8(RDB_TYPE_HASH);
                self.write_string(key);
                self.write_length(hash.len() as u64);
                for (field, val) in hash {
                    self.write_string(field);
                    self.write_string(val);
                }
            }
            Value::SortedSet { by_member, .. } => {
                self.buffer.put_u8(RDB_TYPE_ZSET_2);
                self.write_string(key);
                self.write_length(by_member.len() as u64);
                for (member, score) in by_member {
                    self.write_string(member);
                    self.write_double(*score);
                }
            }
            Value::Stream(stream) => {
                self.write_stream(key, stream);
            }
            Value::HyperLogLog(hll) => {
                // Store HyperLogLog as a string with special prefix
                self.buffer.put_u8(RDB_TYPE_STRING);
                self.write_string(key);
                let mut hll_data = Vec::with_capacity(hll.len() + 4);
                hll_data.extend_from_slice(b"HYLL");
                hll_data.extend_from_slice(hll);
                self.write_string(&hll_data);
            }
        }
    }

    /// Write stream data
    fn write_stream(&mut self, key: &[u8], stream: &Stream) {
        self.buffer.put_u8(RDB_TYPE_STREAM_LISTPACKS_2);
        self.write_string(key);

        // Write number of entries
        self.write_length(stream.entries.len() as u64);

        // Write entries
        for (id, fields) in &stream.entries {
            // Write entry ID
            self.write_length(id.ms);
            self.write_length(id.seq);
            // Write fields count
            self.write_length(fields.len() as u64);
            for (field, value) in fields {
                self.write_string(field);
                self.write_string(value);
            }
        }

        // Write last entry ID
        self.write_length(stream.last_id.ms);
        self.write_length(stream.last_id.seq);

        // Write stream length
        self.write_length(stream.length as u64);

        // Write consumer groups count
        self.write_length(stream.consumer_groups.len() as u64);

        // Write consumer groups
        for (name, group) in &stream.consumer_groups {
            self.write_string(name);
            // Last delivered ID
            self.write_length(group.last_delivered_id.ms);
            self.write_length(group.last_delivered_id.seq);
            // Entries read
            self.write_length(group.entries_read.unwrap_or(0));
            // Pending entries count
            self.write_length(group.pending.len() as u64);
            // Consumers count
            self.write_length(group.consumers.len() as u64);
            for (consumer_name, consumer) in &group.consumers {
                self.write_string(consumer_name);
                self.write_length(consumer.seen_time);
                self.write_length(consumer.pending.len() as u64);
            }
        }
    }

    /// Write EOF marker and checksum
    pub fn write_eof(&mut self) {
        self.buffer.put_u8(RDB_OPCODE_EOF);
        // CRC64 checksum (8 bytes, simplified - zeros for now)
        let checksum = crc64(&self.buffer);
        self.buffer.put_u64_le(checksum);
    }

    /// Get the serialized RDB data
    pub fn into_bytes(self) -> Bytes {
        self.buffer.freeze()
    }

    /// Write a length-encoded integer
    fn write_length(&mut self, len: u64) {
        if len < 64 {
            // 6-bit length (00xxxxxx)
            self.buffer.put_u8(len as u8);
        } else if len < 16384 {
            // 14-bit length (01xxxxxx xxxxxxxx)
            self.buffer.put_u8(0x40 | ((len >> 8) as u8 & 0x3F));
            self.buffer.put_u8((len & 0xFF) as u8);
        } else if len < (1 << 32) {
            // 32-bit length (10000000 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx)
            self.buffer.put_u8(0x80);
            self.buffer.extend_from_slice(&(len as u32).to_be_bytes());
        } else {
            // 64-bit length (10000001 xxxxxxxx...)
            self.buffer.put_u8(0x81);
            self.buffer.extend_from_slice(&len.to_be_bytes());
        }
    }

    /// Write a string
    fn write_string(&mut self, s: &[u8]) {
        self.write_length(s.len() as u64);
        self.buffer.extend_from_slice(s);
    }

    /// Write a double-precision float
    fn write_double(&mut self, val: f64) {
        self.buffer.put_f64_le(val);
    }
}

impl Default for RdbWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// RDB reader for parsing RDB snapshots
pub struct RdbReader<R: Read> {
    reader: R,
    db_index: u8,
}

impl<R: Read> RdbReader<R> {
    /// Create a new RDB reader
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            db_index: 0,
        }
    }

    /// Read and verify the RDB header
    pub fn read_header(&mut self) -> Result<u32> {
        let mut magic = [0u8; 5];
        self.reader.read_exact(&mut magic)?;
        if magic != RDB_MAGIC {
            return Err(FerriteError::Rdb("Invalid RDB magic".into()));
        }

        let mut version = [0u8; 4];
        self.reader.read_exact(&mut version)?;
        let version_str = std::str::from_utf8(&version)
            .map_err(|_| FerriteError::Rdb("Invalid RDB version".into()))?;
        let version_num: u32 = version_str
            .parse()
            .map_err(|_| FerriteError::Rdb("Invalid RDB version number".into()))?;

        Ok(version_num)
    }

    /// Read the next entry from the RDB file
    pub fn read_entry(&mut self) -> Result<Option<RdbEntry>> {
        let opcode = self.read_u8()?;

        match opcode {
            RDB_OPCODE_EOF => {
                // Read checksum (8 bytes)
                let mut checksum = [0u8; 8];
                self.reader.read_exact(&mut checksum)?;
                Ok(None)
            }
            RDB_OPCODE_SELECTDB => {
                let db = self.read_length()? as u8;
                self.db_index = db;
                Ok(Some(RdbEntry::SelectDb(db)))
            }
            RDB_OPCODE_RESIZEDB => {
                let db_size = self.read_length()?;
                let expires_size = self.read_length()?;
                Ok(Some(RdbEntry::ResizeDb {
                    db_size,
                    expires_size,
                }))
            }
            RDB_OPCODE_AUX => {
                let key = self.read_string()?;
                let value = self.read_string()?;
                Ok(Some(RdbEntry::Aux {
                    key: String::from_utf8_lossy(&key).into_owned(),
                    value: String::from_utf8_lossy(&value).into_owned(),
                }))
            }
            RDB_OPCODE_EXPIRETIME_MS => {
                let expire_ms = self.read_u64_le()?;
                // Next should be the value type
                let value_entry = self.read_value_entry(Some(expire_ms))?;
                Ok(Some(value_entry))
            }
            RDB_OPCODE_EXPIRETIME => {
                let expire_sec = self.read_u32_le()?;
                let expire_ms = (expire_sec as u64) * 1000;
                let value_entry = self.read_value_entry(Some(expire_ms))?;
                Ok(Some(value_entry))
            }
            _ => {
                // This should be a value type
                self.read_value_entry_with_type(opcode, None).map(Some)
            }
        }
    }

    fn read_value_entry(&mut self, expire_ms: Option<u64>) -> Result<RdbEntry> {
        let value_type = self.read_u8()?;
        self.read_value_entry_with_type(value_type, expire_ms)
    }

    fn read_value_entry_with_type(
        &mut self,
        value_type: u8,
        expire_ms: Option<u64>,
    ) -> Result<RdbEntry> {
        let key = self.read_string()?;
        let value = self.read_value(value_type)?;

        let expire = expire_ms.and_then(|ms| {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()?
                .as_millis() as u64;
            if ms > now {
                Some(UNIX_EPOCH + Duration::from_millis(ms))
            } else {
                None // Already expired
            }
        });

        Ok(RdbEntry::KeyValue {
            db: self.db_index,
            key: Bytes::from(key),
            value,
            expire,
        })
    }

    fn read_value(&mut self, value_type: u8) -> Result<Value> {
        match value_type {
            RDB_TYPE_STRING => {
                let data = self.read_string()?;
                // Check for HyperLogLog prefix
                if data.starts_with(b"HYLL") && data.len() > 4 {
                    Ok(Value::HyperLogLog(data[4..].to_vec()))
                } else {
                    Ok(Value::String(Bytes::from(data)))
                }
            }
            RDB_TYPE_LIST | RDB_TYPE_LIST_QUICKLIST_2 => {
                let len = self.read_length()? as usize;
                let mut list = VecDeque::with_capacity(len);
                for _ in 0..len {
                    list.push_back(Bytes::from(self.read_string()?));
                }
                Ok(Value::List(list))
            }
            RDB_TYPE_SET | RDB_TYPE_SET_LISTPACK => {
                let len = self.read_length()? as usize;
                let mut set = HashSet::with_capacity(len);
                for _ in 0..len {
                    set.insert(Bytes::from(self.read_string()?));
                }
                Ok(Value::Set(set))
            }
            RDB_TYPE_HASH | RDB_TYPE_HASH_LISTPACK => {
                let len = self.read_length()? as usize;
                let mut hash = HashMap::with_capacity(len);
                for _ in 0..len {
                    let field = Bytes::from(self.read_string()?);
                    let value = Bytes::from(self.read_string()?);
                    hash.insert(field, value);
                }
                Ok(Value::Hash(hash))
            }
            RDB_TYPE_ZSET | RDB_TYPE_ZSET_2 | RDB_TYPE_ZSET_LISTPACK => {
                let len = self.read_length()? as usize;
                let mut by_score = BTreeMap::new();
                let mut by_member = HashMap::with_capacity(len);
                for _ in 0..len {
                    let member = Bytes::from(self.read_string()?);
                    let score = if value_type == RDB_TYPE_ZSET_2 {
                        self.read_f64_le()?
                    } else {
                        // Old format: score as string
                        let score_str = self.read_string()?;
                        String::from_utf8_lossy(&score_str)
                            .parse::<f64>()
                            .unwrap_or(0.0)
                    };
                    by_score.insert((OrderedFloat(score), member.clone()), ());
                    by_member.insert(member, score);
                }
                Ok(Value::SortedSet {
                    by_score,
                    by_member,
                })
            }
            RDB_TYPE_STREAM_LISTPACKS | RDB_TYPE_STREAM_LISTPACKS_2 => self.read_stream(),
            _ => Err(FerriteError::Rdb(format!(
                "Unknown RDB value type: {}",
                value_type
            ))),
        }
    }

    fn read_stream(&mut self) -> Result<Value> {
        let entries_count = self.read_length()? as usize;
        let mut entries = BTreeMap::new();

        for _ in 0..entries_count {
            let ms = self.read_length()?;
            let seq = self.read_length()?;
            let id = StreamEntryId::new(ms, seq);

            let fields_count = self.read_length()? as usize;
            let mut fields = Vec::with_capacity(fields_count);
            for _ in 0..fields_count {
                let field = Bytes::from(self.read_string()?);
                let value = Bytes::from(self.read_string()?);
                fields.push((field, value));
            }
            entries.insert(id, fields);
        }

        let last_ms = self.read_length()?;
        let last_seq = self.read_length()?;
        let last_id = StreamEntryId::new(last_ms, last_seq);

        let length = self.read_length()? as usize;

        // Read consumer groups count (but skip detailed parsing for now)
        let groups_count = self.read_length()? as usize;
        for _ in 0..groups_count {
            let _name = self.read_string()?;
            let _last_ms = self.read_length()?;
            let _last_seq = self.read_length()?;
            let _entries_read = self.read_length()?;
            let _pending_count = self.read_length()?;
            let consumers_count = self.read_length()? as usize;
            for _ in 0..consumers_count {
                let _consumer_name = self.read_string()?;
                let _seen_time = self.read_length()?;
                let _consumer_pending = self.read_length()?;
            }
        }

        Ok(Value::Stream(Stream {
            entries,
            last_id,
            length,
            consumer_groups: HashMap::new(), // Simplified - groups not fully restored
        }))
    }

    fn read_u8(&mut self) -> Result<u8> {
        let mut buf = [0u8; 1];
        self.reader.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    fn read_u32_le(&mut self) -> Result<u32> {
        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }

    fn read_u64_le(&mut self) -> Result<u64> {
        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }

    fn read_f64_le(&mut self) -> Result<f64> {
        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf)?;
        Ok(f64::from_le_bytes(buf))
    }

    fn read_length(&mut self) -> Result<u64> {
        let first = self.read_u8()?;
        let encoding = (first & 0xC0) >> 6;

        match encoding {
            0 => {
                // 6-bit length
                Ok((first & 0x3F) as u64)
            }
            1 => {
                // 14-bit length
                let second = self.read_u8()?;
                Ok((((first & 0x3F) as u64) << 8) | (second as u64))
            }
            2 => {
                // 32 or 64 bit length
                if first == 0x80 {
                    let mut buf = [0u8; 4];
                    self.reader.read_exact(&mut buf)?;
                    Ok(u32::from_be_bytes(buf) as u64)
                } else if first == 0x81 {
                    let mut buf = [0u8; 8];
                    self.reader.read_exact(&mut buf)?;
                    Ok(u64::from_be_bytes(buf))
                } else {
                    Err(FerriteError::Rdb("Invalid length encoding".into()))
                }
            }
            3 => {
                // Special encoding (integer)
                let enc_type = first & 0x3F;
                match enc_type {
                    RDB_ENC_INT8 => Ok(self.read_u8()? as u64),
                    RDB_ENC_INT16 => {
                        let mut buf = [0u8; 2];
                        self.reader.read_exact(&mut buf)?;
                        Ok(u16::from_le_bytes(buf) as u64)
                    }
                    RDB_ENC_INT32 => Ok(self.read_u32_le()? as u64),
                    _ => Err(FerriteError::Rdb(format!(
                        "Unsupported encoding type: {}",
                        enc_type
                    ))),
                }
            }
            _ => unreachable!(),
        }
    }

    fn read_string(&mut self) -> Result<Vec<u8>> {
        let first = self.read_u8()?;
        let encoding = (first & 0xC0) >> 6;

        if encoding == 3 {
            // Special encoding
            let enc_type = first & 0x3F;
            match enc_type {
                RDB_ENC_INT8 => {
                    let val = self.read_u8()? as i8;
                    Ok(val.to_string().into_bytes())
                }
                RDB_ENC_INT16 => {
                    let mut buf = [0u8; 2];
                    self.reader.read_exact(&mut buf)?;
                    let val = i16::from_le_bytes(buf);
                    Ok(val.to_string().into_bytes())
                }
                RDB_ENC_INT32 => {
                    let mut buf = [0u8; 4];
                    self.reader.read_exact(&mut buf)?;
                    let val = i32::from_le_bytes(buf);
                    Ok(val.to_string().into_bytes())
                }
                RDB_ENC_LZF => {
                    // LZF compressed string
                    let compressed_len = self.read_length()? as usize;
                    let _uncompressed_len = self.read_length()? as usize;
                    let mut compressed = vec![0u8; compressed_len];
                    self.reader.read_exact(&mut compressed)?;
                    // For now, return compressed data (proper LZF decompression would be needed)
                    Ok(compressed)
                }
                _ => Err(FerriteError::Rdb(format!(
                    "Unsupported string encoding: {}",
                    enc_type
                ))),
            }
        } else {
            // Regular length-prefixed string
            let len = match encoding {
                0 => (first & 0x3F) as usize,
                1 => {
                    let second = self.read_u8()?;
                    (((first & 0x3F) as usize) << 8) | (second as usize)
                }
                2 => {
                    if first == 0x80 {
                        let mut buf = [0u8; 4];
                        self.reader.read_exact(&mut buf)?;
                        u32::from_be_bytes(buf) as usize
                    } else if first == 0x81 {
                        let mut buf = [0u8; 8];
                        self.reader.read_exact(&mut buf)?;
                        u64::from_be_bytes(buf) as usize
                    } else {
                        return Err(FerriteError::Rdb("Invalid string length".into()));
                    }
                }
                _ => unreachable!(),
            };

            let mut buf = vec![0u8; len];
            self.reader.read_exact(&mut buf)?;
            Ok(buf)
        }
    }
}

/// Entry types read from RDB
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub enum RdbEntry {
    /// Auxiliary field (metadata)
    Aux { key: String, value: String },
    /// Select database
    SelectDb(u8),
    /// Resize DB hint
    ResizeDb { db_size: u64, expires_size: u64 },
    /// Key-value pair
    KeyValue {
        db: u8,
        key: Bytes,
        value: Value,
        expire: Option<SystemTime>,
    },
}

/// Generate a full RDB snapshot from the store
pub fn generate_rdb(store: &Store) -> Bytes {
    let mut writer = RdbWriter::new();

    // Write header
    writer.write_header();

    // Write auxiliary fields
    writer.write_aux("redis-ver", "7.0.0");
    writer.write_aux("redis-bits", "64");
    writer.write_aux(
        "ctime",
        &format!(
            "{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        ),
    );
    writer.write_aux("used-mem", "0");

    // Write each database
    for db_idx in 0..store.num_databases() {
        // Get all keys
        let keys = store.keys(db_idx as u8);
        if keys.is_empty() {
            continue;
        }

        // Write database selector
        writer.write_select_db(db_idx as u8);

        // Count entries with expiry
        let mut expires_count = 0;
        for key in &keys {
            if let Some((_, expire)) = store.get_entry(db_idx as u8, key) {
                if expire.is_some() {
                    expires_count += 1;
                }
            }
        }

        // Write resize hint
        writer.write_resize_db(keys.len() as u64, expires_count);

        // Write all key-value pairs
        for key in keys {
            if let Some((value, expire)) = store.get_entry(db_idx as u8, &key) {
                let expire_ms = expire.and_then(systemtime_to_epoch_ms);
                writer.write_key_value(&key, &value, expire_ms);
            }
        }
    }

    // Write EOF
    writer.write_eof();

    writer.into_bytes()
}

/// Load RDB data into a store
pub fn load_rdb(data: &[u8], store: &Store) -> Result<usize> {
    load_rdb_inner(data, store)
}

/// Load RDB data into a store with optional decryption
#[cfg(feature = "crypto")]
#[allow(dead_code)] // Planned for v0.2 — used when crypto feature is enabled
pub fn load_rdb_with_encryption(
    data: &[u8],
    store: &Store,
    encryption: Option<SharedEncryption>,
) -> Result<usize> {
    // Check if data is encrypted
    let data = if Encryption::is_encrypted(data) {
        if let Some(ref enc) = encryption {
            enc.decrypt(data)?
        } else {
            return Err(FerriteError::Rdb(
                "Encrypted RDB data but no encryption key provided".to_string(),
            ));
        }
    } else {
        data.to_vec()
    };

    load_rdb_inner(&data, store)
}

/// Internal RDB loading logic shared by encrypted and unencrypted paths
fn load_rdb_inner(data: &[u8], store: &Store) -> Result<usize> {
    let mut reader = RdbReader::new(std::io::Cursor::new(data));

    // Read header
    let version = reader.read_header()?;
    if version > 11 {
        return Err(FerriteError::Rdb(format!(
            "Unsupported RDB version: {}",
            version
        )));
    }

    let mut count = 0;

    // Read entries
    while let Some(entry) = reader.read_entry()? {
        match entry {
            RdbEntry::KeyValue {
                db,
                key,
                value,
                expire,
            } => {
                if let Some(exp) = expire {
                    store.set_with_expiry(db, key, value, exp);
                } else {
                    store.set(db, key, value);
                }
                count += 1;
            }
            RdbEntry::SelectDb(_) | RdbEntry::ResizeDb { .. } | RdbEntry::Aux { .. } => {
                // Skip metadata entries
            }
        }
    }

    Ok(count)
}

/// Generate an encrypted RDB snapshot from the store
#[cfg(feature = "crypto")]
#[allow(dead_code)] // Planned for v0.2 — used when crypto feature is enabled
pub fn generate_rdb_encrypted(store: &Store, encryption: &SharedEncryption) -> Result<Bytes> {
    let rdb_data = generate_rdb(store);
    let encrypted = encryption.encrypt(&rdb_data)?;
    Ok(Bytes::from(encrypted))
}

/// Simple CRC64 checksum (ECMA-182)
fn crc64(data: &[u8]) -> u64 {
    const POLY: u64 = 0xC96C5795D7870F42;
    let mut crc: u64 = 0;

    for &byte in data {
        crc ^= byte as u64;
        for _ in 0..8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ POLY;
            } else {
                crc >>= 1;
            }
        }
    }

    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rdb_write_read_header() {
        let mut writer = RdbWriter::new();
        writer.write_header();
        let data = writer.into_bytes();

        assert!(data.starts_with(RDB_MAGIC));
        assert!(data[5..9] == *RDB_VERSION);
    }

    #[test]
    fn test_rdb_roundtrip_string() {
        let store = Store::new(16);
        store.set(0, Bytes::from("key1"), Value::String(Bytes::from("value1")));
        store.set(0, Bytes::from("key2"), Value::String(Bytes::from("value2")));

        let rdb = generate_rdb(&store);

        // Load into new store
        let store2 = Store::new(16);
        let count = load_rdb(&rdb, &store2).unwrap();

        assert_eq!(count, 2);
        assert_eq!(
            store2.get(0, &Bytes::from("key1")),
            Some(Value::String(Bytes::from("value1")))
        );
        assert_eq!(
            store2.get(0, &Bytes::from("key2")),
            Some(Value::String(Bytes::from("value2")))
        );
    }

    #[test]
    fn test_rdb_roundtrip_list() {
        let store = Store::new(16);
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("a"));
        list.push_back(Bytes::from("b"));
        list.push_back(Bytes::from("c"));
        store.set(0, Bytes::from("mylist"), Value::List(list));

        let rdb = generate_rdb(&store);
        let store2 = Store::new(16);
        load_rdb(&rdb, &store2).unwrap();

        match store2.get(0, &Bytes::from("mylist")) {
            Some(Value::List(l)) => {
                assert_eq!(l.len(), 3);
                assert_eq!(l[0], Bytes::from("a"));
            }
            _ => panic!("Expected list"),
        }
    }

    #[test]
    fn test_rdb_roundtrip_hash() {
        let store = Store::new(16);
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("field1"), Bytes::from("value1"));
        hash.insert(Bytes::from("field2"), Bytes::from("value2"));
        store.set(0, Bytes::from("myhash"), Value::Hash(hash));

        let rdb = generate_rdb(&store);
        let store2 = Store::new(16);
        load_rdb(&rdb, &store2).unwrap();

        match store2.get(0, &Bytes::from("myhash")) {
            Some(Value::Hash(h)) => {
                assert_eq!(h.len(), 2);
                assert_eq!(h.get(&Bytes::from("field1")), Some(&Bytes::from("value1")));
            }
            _ => panic!("Expected hash"),
        }
    }

    #[test]
    fn test_rdb_roundtrip_set() {
        let store = Store::new(16);
        let mut set = HashSet::new();
        set.insert(Bytes::from("member1"));
        set.insert(Bytes::from("member2"));
        store.set(0, Bytes::from("myset"), Value::Set(set));

        let rdb = generate_rdb(&store);
        let store2 = Store::new(16);
        load_rdb(&rdb, &store2).unwrap();

        match store2.get(0, &Bytes::from("myset")) {
            Some(Value::Set(s)) => {
                assert_eq!(s.len(), 2);
                assert!(s.contains(&Bytes::from("member1")));
            }
            _ => panic!("Expected set"),
        }
    }

    #[test]
    fn test_rdb_roundtrip_sorted_set() {
        let store = Store::new(16);
        let mut by_score = BTreeMap::new();
        let mut by_member = HashMap::new();
        by_score.insert((OrderedFloat(1.0), Bytes::from("one")), ());
        by_score.insert((OrderedFloat(2.0), Bytes::from("two")), ());
        by_member.insert(Bytes::from("one"), 1.0);
        by_member.insert(Bytes::from("two"), 2.0);
        store.set(
            0,
            Bytes::from("myzset"),
            Value::SortedSet {
                by_score,
                by_member,
            },
        );

        let rdb = generate_rdb(&store);
        let store2 = Store::new(16);
        load_rdb(&rdb, &store2).unwrap();

        match store2.get(0, &Bytes::from("myzset")) {
            Some(Value::SortedSet { by_member, .. }) => {
                assert_eq!(by_member.len(), 2);
                assert_eq!(by_member.get(&Bytes::from("one")), Some(&1.0));
            }
            _ => panic!("Expected sorted set"),
        }
    }

    #[test]
    fn test_rdb_multiple_databases() {
        let store = Store::new(16);
        store.set(0, Bytes::from("key0"), Value::String(Bytes::from("val0")));
        store.set(1, Bytes::from("key1"), Value::String(Bytes::from("val1")));
        store.set(5, Bytes::from("key5"), Value::String(Bytes::from("val5")));

        let rdb = generate_rdb(&store);
        let store2 = Store::new(16);
        let count = load_rdb(&rdb, &store2).unwrap();

        assert_eq!(count, 3);
        assert_eq!(
            store2.get(0, &Bytes::from("key0")),
            Some(Value::String(Bytes::from("val0")))
        );
        assert_eq!(
            store2.get(1, &Bytes::from("key1")),
            Some(Value::String(Bytes::from("val1")))
        );
        assert_eq!(
            store2.get(5, &Bytes::from("key5")),
            Some(Value::String(Bytes::from("val5")))
        );
    }

    #[test]
    #[cfg(feature = "crypto")]
    fn test_rdb_encrypted_roundtrip() {
        use crate::config::EncryptionAlgorithm;
        use crate::crypto::EncryptionKey;
        use std::sync::Arc;

        let store = Store::new(16);
        store.set(0, Bytes::from("key1"), Value::String(Bytes::from("value1")));
        store.set(0, Bytes::from("key2"), Value::String(Bytes::from("value2")));

        // Create encryption
        let key = EncryptionKey::generate();
        let encryption = Arc::new(Encryption::new(key, EncryptionAlgorithm::ChaCha20Poly1305));

        // Generate encrypted RDB
        let encrypted_rdb = generate_rdb_encrypted(&store, &encryption).unwrap();

        // Verify it's encrypted (starts with magic header)
        assert!(Encryption::is_encrypted(&encrypted_rdb));

        // Load into new store with same key
        let store2 = Store::new(16);
        let count = load_rdb_with_encryption(&encrypted_rdb, &store2, Some(encryption)).unwrap();

        assert_eq!(count, 2);
        assert_eq!(
            store2.get(0, &Bytes::from("key1")),
            Some(Value::String(Bytes::from("value1")))
        );
        assert_eq!(
            store2.get(0, &Bytes::from("key2")),
            Some(Value::String(Bytes::from("value2")))
        );
    }

    #[test]
    #[cfg(feature = "crypto")]
    fn test_rdb_encrypted_wrong_key_fails() {
        use crate::config::EncryptionAlgorithm;
        use crate::crypto::EncryptionKey;
        use std::sync::Arc;

        let store = Store::new(16);
        store.set(0, Bytes::from("key1"), Value::String(Bytes::from("value1")));

        // Create encryption with first key
        let key1 = EncryptionKey::generate();
        let encryption1 = Arc::new(Encryption::new(key1, EncryptionAlgorithm::ChaCha20Poly1305));

        // Generate encrypted RDB
        let encrypted_rdb = generate_rdb_encrypted(&store, &encryption1).unwrap();

        // Try to load with different key
        let key2 = EncryptionKey::generate();
        let encryption2 = Arc::new(Encryption::new(key2, EncryptionAlgorithm::ChaCha20Poly1305));

        let store2 = Store::new(16);
        let result = load_rdb_with_encryption(&encrypted_rdb, &store2, Some(encryption2));

        // Should fail with decryption error
        assert!(result.is_err());
    }

    #[test]
    #[cfg(feature = "crypto")]
    fn test_rdb_encrypted_requires_key() {
        use crate::config::EncryptionAlgorithm;
        use crate::crypto::EncryptionKey;
        use std::sync::Arc;

        let store = Store::new(16);
        store.set(0, Bytes::from("key1"), Value::String(Bytes::from("value1")));

        // Create encryption
        let key = EncryptionKey::generate();
        let encryption = Arc::new(Encryption::new(key, EncryptionAlgorithm::ChaCha20Poly1305));

        // Generate encrypted RDB
        let encrypted_rdb = generate_rdb_encrypted(&store, &encryption).unwrap();

        // Try to load without encryption key
        let store2 = Store::new(16);
        let result = load_rdb_with_encryption(&encrypted_rdb, &store2, None);

        // Should fail because encrypted data requires key
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("no encryption key"));
    }

    #[test]
    #[cfg(feature = "crypto")]
    fn test_rdb_unencrypted_read_with_encryption_key() {
        use crate::config::EncryptionAlgorithm;
        use crate::crypto::EncryptionKey;
        use std::sync::Arc;

        let store = Store::new(16);
        store.set(0, Bytes::from("key1"), Value::String(Bytes::from("value1")));

        // Generate unencrypted RDB
        let rdb = generate_rdb(&store);

        // Should be able to load with encryption key configured
        // (backward compatibility - can read unencrypted data)
        let key = EncryptionKey::generate();
        let encryption = Arc::new(Encryption::new(key, EncryptionAlgorithm::ChaCha20Poly1305));

        let store2 = Store::new(16);
        let count = load_rdb_with_encryption(&rdb, &store2, Some(encryption)).unwrap();

        assert_eq!(count, 1);
        assert_eq!(
            store2.get(0, &Bytes::from("key1")),
            Some(Value::String(Bytes::from("value1")))
        );
    }
}
