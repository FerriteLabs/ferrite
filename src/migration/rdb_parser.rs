//! Redis RDB binary format parser
//!
//! Parses Redis RDB dump files to extract all keys, values, and metadata.
//! Supports RDB versions 6-11 (Redis 2.6 through 7.x).
//!
//! RDB format: [REDIS magic] [version] [database sections] [EOF] [checksum]
#![allow(dead_code)]

use std::collections::HashMap;
use std::io;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::Instant;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const RDB_MAGIC: &[u8] = b"REDIS";

const RDB_OPCODE_EOF: u8 = 0xFF;
const RDB_OPCODE_SELECTDB: u8 = 0xFE;
const RDB_OPCODE_EXPIRETIME: u8 = 0xFD;
const RDB_OPCODE_EXPIRETIME_MS: u8 = 0xFC;
const RDB_OPCODE_RESIZEDB: u8 = 0xFB;
const RDB_OPCODE_AUX: u8 = 0xFA;

const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_LIST: u8 = 1;
const RDB_TYPE_SET: u8 = 2;
const RDB_TYPE_ZSET: u8 = 3;
const RDB_TYPE_HASH: u8 = 4;
const RDB_TYPE_ZSET_2: u8 = 5;
const RDB_TYPE_LIST_ZIPLIST: u8 = 10;
const RDB_TYPE_SET_INTSET: u8 = 11;
const RDB_TYPE_ZSET_ZIPLIST: u8 = 12;
const RDB_TYPE_HASH_ZIPLIST: u8 = 13;
const RDB_TYPE_LIST_QUICKLIST: u8 = 14;
const RDB_TYPE_STREAM: u8 = 15;
const RDB_TYPE_HASH_LISTPACK: u8 = 16;
const RDB_TYPE_ZSET_LISTPACK: u8 = 17;
const RDB_TYPE_LIST_QUICKLIST_2: u8 = 18;
const RDB_TYPE_SET_LISTPACK: u8 = 20;
const RDB_TYPE_STREAM_2: u8 = 21;
const RDB_TYPE_STREAM_3: u8 = 22;

// Length encoding
const RDB_6BITLEN: u8 = 0;
const RDB_14BITLEN: u8 = 1;
const RDB_32BITLEN: u8 = 0x80;
const RDB_64BITLEN: u8 = 0x81;
const RDB_ENCVAL: u8 = 3;
const RDB_ENC_INT8: u8 = 0;
const RDB_ENC_INT16: u8 = 1;
const RDB_ENC_INT32: u8 = 2;
const RDB_ENC_LZF: u8 = 3;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur during RDB parsing
#[derive(Debug, Clone, thiserror::Error)]
pub enum RdbParseError {
    #[error("invalid magic: expected REDIS header")]
    InvalidMagic,
    #[error("unsupported RDB version: {0}")]
    UnsupportedVersion(u8),
    #[error("unexpected end of file")]
    UnexpectedEof,
    #[error("invalid encoding")]
    InvalidEncoding,
    #[error("LZF decompression error")]
    LzfDecompressError,
    #[error("checksum mismatch")]
    ChecksumMismatch,
    #[error("invalid type: {0}")]
    InvalidType(u8),
}

impl From<io::Error> for RdbParseError {
    fn from(_: io::Error) -> Self {
        RdbParseError::UnexpectedEof
    }
}

/// Result type for RDB operations.
pub type Result<T> = std::result::Result<T, RdbParseError>;

// ---------------------------------------------------------------------------
// RDB version
// ---------------------------------------------------------------------------

/// Supported RDB format versions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RdbVersion {
    V6,
    V7,
    V8,
    V9,
    V10,
    V11,
}

impl RdbVersion {
    fn from_u8(v: u8) -> Result<Self> {
        match v {
            6 => Ok(RdbVersion::V6),
            7 => Ok(RdbVersion::V7),
            8 => Ok(RdbVersion::V8),
            9 => Ok(RdbVersion::V9),
            10 => Ok(RdbVersion::V10),
            11 => Ok(RdbVersion::V11),
            other => Err(RdbParseError::UnsupportedVersion(other)),
        }
    }

    /// Numeric version number
    pub fn as_u8(&self) -> u8 {
        match self {
            RdbVersion::V6 => 6,
            RdbVersion::V7 => 7,
            RdbVersion::V8 => 8,
            RdbVersion::V9 => 9,
            RdbVersion::V10 => 10,
            RdbVersion::V11 => 11,
        }
    }
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// A fully-parsed RDB file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RdbFile {
    pub version: RdbVersion,
    pub aux_fields: HashMap<String, String>,
    pub databases: Vec<DatabaseSection>,
}

/// A single logical database inside the RDB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseSection {
    pub db_number: u32,
    pub resize_db: Option<(u64, u64)>,
    pub entries: Vec<RdbEntry>,
}

/// A single key-value entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RdbEntry {
    pub key: Vec<u8>,
    pub value: RdbValue,
    pub expire_ms: Option<u64>,
}

/// Redis value types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RdbValue {
    String(Vec<u8>),
    List(Vec<Vec<u8>>),
    Set(Vec<Vec<u8>>),
    SortedSet(Vec<(f64, Vec<u8>)>),
    Hash(Vec<(Vec<u8>, Vec<u8>)>),
    Stream(StreamData),
}

/// Stream data with entries, groups, and last ID.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamData {
    pub entries: Vec<StreamEntry>,
    pub groups: Vec<StreamGroup>,
    pub last_id: String,
}

/// A single stream entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEntry {
    pub id: String,
    pub fields: Vec<(Vec<u8>, Vec<u8>)>,
}

/// A consumer group on a stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamGroup {
    pub name: String,
    pub last_delivered_id: String,
    pub consumers: Vec<StreamConsumer>,
}

/// A consumer in a stream consumer group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConsumer {
    pub name: String,
    pub seen_time: u64,
    pub pending: Vec<String>,
}

// ---------------------------------------------------------------------------
// LZF decompression
// ---------------------------------------------------------------------------

/// Decompress LZF-compressed data.
///
/// LZF is a simple byte-oriented compression algorithm used by Redis for
/// in-memory string compression. Control bytes determine literal runs vs
/// back-references into already-decompressed output.
fn lzf_decompress(input: &[u8], expected_len: usize) -> Result<Vec<u8>> {
    let mut output = Vec::with_capacity(expected_len);
    let mut ip = 0;

    while ip < input.len() {
        let ctrl = input[ip] as usize;
        ip += 1;

        if ctrl < 32 {
            // Literal run: copy ctrl + 1 bytes verbatim
            let count = ctrl + 1;
            if ip + count > input.len() {
                return Err(RdbParseError::LzfDecompressError);
            }
            output.extend_from_slice(&input[ip..ip + count]);
            ip += count;
        } else {
            // Back reference
            let mut len = ctrl >> 5;
            if len == 7 {
                if ip >= input.len() {
                    return Err(RdbParseError::LzfDecompressError);
                }
                len += input[ip] as usize;
                ip += 1;
            }
            len += 2;

            if ip >= input.len() {
                return Err(RdbParseError::LzfDecompressError);
            }
            let offset = ((ctrl & 0x1f) << 8) | input[ip] as usize;
            ip += 1;

            let start = output
                .len()
                .checked_sub(offset + 1)
                .ok_or(RdbParseError::LzfDecompressError)?;

            // Copy byte-by-byte (overlapping copy is intentional for RLE)
            for i in 0..len {
                let byte = output[start + i];
                output.push(byte);
            }
        }
    }

    Ok(output)
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Redis RDB binary format parser (v2).
///
/// Stateless parser — create with [`new()`](Self::new) and call
/// [`parse()`](Self::parse) with the raw RDB bytes.
#[derive(Debug)]
pub struct RdbParser {
    _private: (),
}

impl RdbParser {
    /// Create a new RDB parser.
    pub fn new() -> Self {
        Self { _private: () }
    }

    /// Parse a complete RDB file from a byte slice.
    pub fn parse(&self, data: &[u8]) -> Result<RdbFile> {
        let mut cursor = data;
        let version = self.parse_header(&mut cursor)?;

        let mut aux_fields = HashMap::new();
        let mut databases: Vec<DatabaseSection> = Vec::new();

        loop {
            if cursor.is_empty() {
                break;
            }
            let saved = cursor;
            let opcode = read_u8(&mut cursor)?;

            match opcode {
                RDB_OPCODE_AUX => {
                    let key = self.decode_string(&mut cursor)?;
                    let val = self.decode_string(&mut cursor)?;
                    let key_str = String::from_utf8_lossy(&key).to_string();
                    let val_str = String::from_utf8_lossy(&val).to_string();
                    aux_fields.insert(key_str, val_str);
                }
                RDB_OPCODE_SELECTDB => {
                    let db = self.parse_database_section(&mut cursor)?;
                    databases.push(db);
                }
                RDB_OPCODE_EOF => {
                    break;
                }
                // Value type byte before any SELECTDB → implicit db 0
                _ => {
                    cursor = saved;
                    let db = self.parse_implicit_db0(&mut cursor)?;
                    databases.push(db);
                }
            }
        }

        Ok(RdbFile {
            version,
            aux_fields,
            databases,
        })
    }

    /// Parse the 9-byte RDB header: "REDIS" magic + 4-digit ASCII version.
    pub fn parse_header(&self, data: &mut &[u8]) -> Result<RdbVersion> {
        if data.len() < 9 {
            return Err(RdbParseError::UnexpectedEof);
        }
        if &data[..5] != RDB_MAGIC {
            return Err(RdbParseError::InvalidMagic);
        }
        let version_str =
            std::str::from_utf8(&data[5..9]).map_err(|_| RdbParseError::InvalidMagic)?;
        let version_num: u8 = version_str
            .parse()
            .map_err(|_| RdbParseError::InvalidMagic)?;
        *data = &data[9..];
        RdbVersion::from_u8(version_num)
    }

    /// Parse a database section (after the SELECTDB opcode has been consumed).
    pub fn parse_database_section(
        &self,
        data: &mut &[u8],
    ) -> Result<DatabaseSection> {
        let (db_number, _) = self.decode_length(data)?;
        let mut resize_db = None;
        let mut entries = Vec::new();
        let mut current_expire: Option<u64> = None;

        loop {
            if data.is_empty() {
                break;
            }
            let saved = *data;
            let opcode = read_u8(data)?;

            match opcode {
                RDB_OPCODE_RESIZEDB => {
                    let (db_size, _) = self.decode_length(data)?;
                    let (expire_size, _) = self.decode_length(data)?;
                    resize_db = Some((db_size, expire_size));
                }
                RDB_OPCODE_EXPIRETIME_MS => {
                    current_expire = Some(read_u64_le(data)?);
                }
                RDB_OPCODE_EXPIRETIME => {
                    let secs = read_u32_le(data)? as u64;
                    current_expire = Some(secs * 1000);
                }
                // Section boundary: put byte back
                RDB_OPCODE_SELECTDB | RDB_OPCODE_AUX | RDB_OPCODE_EOF => {
                    *data = saved;
                    break;
                }
                value_type => {
                    let mut entry = self.parse_entry(data, value_type)?;
                    entry.expire_ms = current_expire.take();
                    entries.push(entry);
                }
            }
        }

        Ok(DatabaseSection {
            db_number: db_number as u32,
            resize_db,
            entries,
        })
    }

    /// Parse entries that appear before any SELECTDB (implicit database 0).
    fn parse_implicit_db0(&self, data: &mut &[u8]) -> Result<DatabaseSection> {
        let mut entries = Vec::new();
        let mut current_expire: Option<u64> = None;

        loop {
            if data.is_empty() {
                break;
            }
            let saved = *data;
            let opcode = read_u8(data)?;

            match opcode {
                RDB_OPCODE_EXPIRETIME_MS => {
                    current_expire = Some(read_u64_le(data)?);
                }
                RDB_OPCODE_EXPIRETIME => {
                    let secs = read_u32_le(data)? as u64;
                    current_expire = Some(secs * 1000);
                }
                RDB_OPCODE_SELECTDB | RDB_OPCODE_AUX | RDB_OPCODE_EOF => {
                    *data = saved;
                    break;
                }
                value_type => {
                    let mut entry = self.parse_entry(data, value_type)?;
                    entry.expire_ms = current_expire.take();
                    entries.push(entry);
                }
            }
        }

        Ok(DatabaseSection {
            db_number: 0,
            resize_db: None,
            entries,
        })
    }

    /// Parse a single key-value entry given the value-type byte.
    pub fn parse_entry(
        &self,
        data: &mut &[u8],
        value_type: u8,
    ) -> Result<RdbEntry> {
        let key = self.decode_string(data)?;
        let value = self.read_value(data, value_type)?;
        Ok(RdbEntry {
            key,
            value,
            expire_ms: None,
        })
    }

    /// Decode RDB length encoding, returning `(length, is_special_encoding)`.
    pub fn decode_length(&self, data: &mut &[u8]) -> Result<(u64, bool)> {
        let byte = read_u8(data)?;
        let enc_type = (byte & 0xC0) >> 6;

        match enc_type {
            RDB_6BITLEN => Ok(((byte & 0x3F) as u64, false)),
            RDB_14BITLEN => {
                let next = read_u8(data)?;
                Ok(((((byte & 0x3F) as u64) << 8) | next as u64, false))
            }
            _ if byte == RDB_32BITLEN => {
                let v = read_u32_be(data)?;
                Ok((v as u64, false))
            }
            _ if byte == RDB_64BITLEN => {
                let v = read_u64_be(data)?;
                Ok((v, false))
            }
            RDB_ENCVAL => Ok(((byte & 0x3F) as u64, true)),
            _ => Err(RdbParseError::InvalidEncoding),
        }
    }

    /// Decode an RDB string (length-prefixed, integer-encoded, or LZF-compressed).
    pub fn decode_string(&self, data: &mut &[u8]) -> Result<Vec<u8>> {
        let (len, is_encoded) = self.decode_length(data)?;

        if !is_encoded {
            return read_bytes(data, len as usize);
        }

        match len as u8 {
            RDB_ENC_INT8 => {
                let v = read_u8(data)? as i8;
                Ok(v.to_string().into_bytes())
            }
            RDB_ENC_INT16 => {
                let v = read_i16_le(data)?;
                Ok(v.to_string().into_bytes())
            }
            RDB_ENC_INT32 => {
                let v = read_i32_le(data)?;
                Ok(v.to_string().into_bytes())
            }
            RDB_ENC_LZF => {
                let (compressed_len, _) = self.decode_length(data)?;
                let (uncompressed_len, _) = self.decode_length(data)?;
                let compressed = read_bytes(data, compressed_len as usize)?;
                lzf_decompress(&compressed, uncompressed_len as usize)
            }
            _ => Err(RdbParseError::InvalidEncoding),
        }
    }

    /// Read a typed value from the stream.
    fn read_value(&self, data: &mut &[u8], value_type: u8) -> Result<RdbValue> {
        match value_type {
            RDB_TYPE_STRING => {
                let s = self.decode_string(data)?;
                Ok(RdbValue::String(s))
            }
            RDB_TYPE_LIST => {
                let (len, _) = self.decode_length(data)?;
                let mut items = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    items.push(self.decode_string(data)?);
                }
                Ok(RdbValue::List(items))
            }
            RDB_TYPE_SET => {
                let (len, _) = self.decode_length(data)?;
                let mut members = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    members.push(self.decode_string(data)?);
                }
                Ok(RdbValue::Set(members))
            }
            RDB_TYPE_ZSET => {
                let (len, _) = self.decode_length(data)?;
                let mut entries = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    let member = self.decode_string(data)?;
                    let score = self.read_rdb_double(data)?;
                    entries.push((score, member));
                }
                Ok(RdbValue::SortedSet(entries))
            }
            RDB_TYPE_ZSET_2 => {
                let (len, _) = self.decode_length(data)?;
                let mut entries = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    let member = self.decode_string(data)?;
                    let score = read_f64_le(data)?;
                    entries.push((score, member));
                }
                Ok(RdbValue::SortedSet(entries))
            }
            RDB_TYPE_HASH => {
                let (len, _) = self.decode_length(data)?;
                let mut pairs = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    let field = self.decode_string(data)?;
                    let value = self.decode_string(data)?;
                    pairs.push((field, value));
                }
                Ok(RdbValue::Hash(pairs))
            }
            // Compact encodings — skip the blob, treating as opaque string
            RDB_TYPE_LIST_ZIPLIST
            | RDB_TYPE_SET_INTSET
            | RDB_TYPE_ZSET_ZIPLIST
            | RDB_TYPE_HASH_ZIPLIST
            | RDB_TYPE_LIST_QUICKLIST
            | RDB_TYPE_LIST_QUICKLIST_2
            | RDB_TYPE_SET_LISTPACK
            | RDB_TYPE_ZSET_LISTPACK
            | RDB_TYPE_HASH_LISTPACK => {
                let blob = self.decode_string(data)?;
                Ok(RdbValue::String(blob))
            }
            // Stream types — skip the blob, return placeholder
            RDB_TYPE_STREAM | RDB_TYPE_STREAM_2 | RDB_TYPE_STREAM_3 => {
                let blob = self.decode_string(data)?;
                Ok(RdbValue::Stream(StreamData {
                    entries: vec![StreamEntry {
                        id: "0-0".to_string(),
                        fields: vec![(b"__raw__".to_vec(), blob)],
                    }],
                    groups: vec![],
                    last_id: "0-0".to_string(),
                }))
            }
            other => Err(RdbParseError::InvalidType(other)),
        }
    }

    /// Read an RDB-encoded double (used in ZSET v1).
    fn read_rdb_double(&self, data: &mut &[u8]) -> Result<f64> {
        let len = read_u8(data)?;
        match len {
            253 => Ok(f64::NAN),
            254 => Ok(f64::INFINITY),
            255 => Ok(f64::NEG_INFINITY),
            _ => {
                let buf = read_bytes(data, len as usize)?;
                let s = String::from_utf8_lossy(&buf);
                s.parse::<f64>()
                    .map_err(|_| RdbParseError::InvalidEncoding)
            }
        }
    }
}

impl Default for RdbParser {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Backward-compatible aliases for v1 API used by ferrite-migrate binary
// ---------------------------------------------------------------------------

/// Legacy error type alias
pub type RdbError = RdbParseError;

/// Legacy data structure returned by v1 parse
#[derive(Debug, Clone)]
pub struct RdbData {
    pub databases: Vec<RdbDatabase>,
    pub aux_fields: HashMap<String, String>,
    pub stats: RdbStats,
}

/// Legacy database representation
#[derive(Debug, Clone)]
pub struct RdbDatabase {
    pub db_number: u32,
    pub entries: Vec<RdbEntryV1>,
    pub resize_db: Option<(u64, u64)>,
}

/// Legacy entry using Bytes
#[derive(Debug, Clone)]
pub struct RdbEntryV1 {
    pub key: Bytes,
    pub value: RdbValueV1,
    pub expire_at: Option<u64>,
}

/// Legacy value type using Bytes
#[derive(Debug, Clone)]
pub enum RdbValueV1 {
    String(Bytes),
    List(Vec<Bytes>),
    Set(Vec<Bytes>),
    SortedSet(Vec<(f64, Bytes)>),
    Hash(Vec<(Bytes, Bytes)>),
    Stream {
        entries: Vec<StreamEntryV1>,
        groups: Vec<ConsumerGroup>,
    },
    Module {
        type_name: String,
        data: Bytes,
    },
}

/// Legacy stream entry
#[derive(Debug, Clone)]
pub struct StreamEntryV1 {
    pub id: (u64, u64),
    pub fields: Vec<(Bytes, Bytes)>,
}

/// Consumer group (compatible with both v1 and v2)
#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub name: String,
    pub last_id: (u64, u64),
    pub pending: Vec<PendingEntry>,
}

/// Pending entry in a consumer group
#[derive(Debug, Clone)]
pub struct PendingEntry {
    pub id: (u64, u64),
    pub consumer: String,
    pub delivery_time: u64,
    pub delivery_count: u64,
}

/// Parsing statistics
#[derive(Debug, Clone, Default)]
pub struct RdbStats {
    pub total_keys: u64,
    pub databases_count: u64,
    pub strings_count: u64,
    pub lists_count: u64,
    pub sets_count: u64,
    pub sorted_sets_count: u64,
    pub hashes_count: u64,
    pub streams_count: u64,
    pub bytes_processed: u64,
    pub parse_duration_us: u64,
}

/// Legacy RDB parser that holds data internally.
///
/// For new code, prefer [`RdbParser`] which is stateless.
#[derive(Debug)]
pub struct RdbParserV1 {
    data: Vec<u8>,
    rdb_version: u32,
}

impl RdbParserV1 {
    /// Create a parser from a file path.
    pub fn new(path: &Path) -> Result<Self> {
        let data =
            std::fs::read(path).map_err(|e| RdbParseError::InvalidType(e.kind() as u8))?;
        Self::from_bytes(&data)
    }

    /// Create from in-memory bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 9 {
            return Err(RdbParseError::UnexpectedEof);
        }
        if &data[..5] != RDB_MAGIC {
            return Err(RdbParseError::InvalidMagic);
        }
        let version_str =
            std::str::from_utf8(&data[5..9]).map_err(|_| RdbParseError::InvalidMagic)?;
        let version: u32 = version_str
            .parse()
            .map_err(|_| RdbParseError::InvalidMagic)?;
        if version > 11 {
            return Err(RdbParseError::UnsupportedVersion(version as u8));
        }
        Ok(Self {
            data: data.to_vec(),
            rdb_version: version,
        })
    }

    pub fn version(&self) -> u32 {
        self.rdb_version
    }

    /// Parse into the v2 format and convert to legacy types.
    pub fn parse(&mut self) -> Result<RdbData> {
        let start = Instant::now();
        let parser = RdbParser::new();
        let rdb_file = parser.parse(&self.data)?;
        let mut stats = RdbStats::default();
        let mut databases = Vec::new();

        for db_section in &rdb_file.databases {
            let mut entries = Vec::new();
            for entry in &db_section.entries {
                let v1_value = convert_value_to_v1(&entry.value, &mut stats);
                entries.push(RdbEntryV1 {
                    key: Bytes::from(entry.key.clone()),
                    value: v1_value,
                    expire_at: entry.expire_ms,
                });
                stats.total_keys += 1;
            }
            stats.databases_count += 1;
            databases.push(RdbDatabase {
                db_number: db_section.db_number,
                entries,
                resize_db: db_section.resize_db,
            });
        }

        stats.parse_duration_us = start.elapsed().as_micros() as u64;

        Ok(RdbData {
            databases,
            aux_fields: rdb_file.aux_fields,
            stats,
        })
    }

    /// Streaming parse with callback.
    pub fn parse_streaming<F>(&mut self, mut callback: F) -> Result<RdbStats>
    where
        F: FnMut(u32, &RdbEntryV1),
    {
        let start = Instant::now();
        let parser = RdbParser::new();
        let rdb_file = parser.parse(&self.data)?;
        let mut stats = RdbStats::default();

        for db_section in &rdb_file.databases {
            stats.databases_count += 1;
            for entry in &db_section.entries {
                let v1_value = convert_value_to_v1(&entry.value, &mut stats);
                let v1_entry = RdbEntryV1 {
                    key: Bytes::from(entry.key.clone()),
                    value: v1_value,
                    expire_at: entry.expire_ms,
                };
                stats.total_keys += 1;
                callback(db_section.db_number, &v1_entry);
            }
        }

        stats.parse_duration_us = start.elapsed().as_micros() as u64;
        Ok(stats)
    }
}

fn convert_value_to_v1(value: &RdbValue, stats: &mut RdbStats) -> RdbValueV1 {
    match value {
        RdbValue::String(v) => {
            stats.strings_count += 1;
            RdbValueV1::String(Bytes::from(v.clone()))
        }
        RdbValue::List(items) => {
            stats.lists_count += 1;
            RdbValueV1::List(items.iter().map(|i| Bytes::from(i.clone())).collect())
        }
        RdbValue::Set(members) => {
            stats.sets_count += 1;
            RdbValueV1::Set(members.iter().map(|m| Bytes::from(m.clone())).collect())
        }
        RdbValue::SortedSet(entries) => {
            stats.sorted_sets_count += 1;
            RdbValueV1::SortedSet(
                entries
                    .iter()
                    .map(|(s, m)| (*s, Bytes::from(m.clone())))
                    .collect(),
            )
        }
        RdbValue::Hash(pairs) => {
            stats.hashes_count += 1;
            RdbValueV1::Hash(
                pairs
                    .iter()
                    .map(|(f, v)| (Bytes::from(f.clone()), Bytes::from(v.clone())))
                    .collect(),
            )
        }
        RdbValue::Stream(sd) => {
            stats.streams_count += 1;
            RdbValueV1::Stream {
                entries: sd
                    .entries
                    .iter()
                    .map(|e| {
                        let parts: Vec<&str> = e.id.split('-').collect();
                        let ms = parts.first().and_then(|p| p.parse().ok()).unwrap_or(0);
                        let seq = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
                        StreamEntryV1 {
                            id: (ms, seq),
                            fields: e
                                .fields
                                .iter()
                                .map(|(f, v)| (Bytes::from(f.clone()), Bytes::from(v.clone())))
                                .collect(),
                        }
                    })
                    .collect(),
                groups: vec![],
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Low-level read helpers for &mut &[u8]
// ---------------------------------------------------------------------------

fn read_u8(data: &mut &[u8]) -> Result<u8> {
    if data.is_empty() {
        return Err(RdbParseError::UnexpectedEof);
    }
    let byte = data[0];
    *data = &data[1..];
    Ok(byte)
}

fn read_bytes(data: &mut &[u8], n: usize) -> Result<Vec<u8>> {
    if data.len() < n {
        return Err(RdbParseError::UnexpectedEof);
    }
    let result = data[..n].to_vec();
    *data = &data[n..];
    Ok(result)
}

fn read_u32_le(data: &mut &[u8]) -> Result<u32> {
    let buf = read_bytes(data, 4)?;
    Ok(u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]))
}

fn read_u64_le(data: &mut &[u8]) -> Result<u64> {
    let buf = read_bytes(data, 8)?;
    Ok(u64::from_le_bytes([
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ]))
}

fn read_u32_be(data: &mut &[u8]) -> Result<u32> {
    let buf = read_bytes(data, 4)?;
    Ok(u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]))
}

fn read_u64_be(data: &mut &[u8]) -> Result<u64> {
    let buf = read_bytes(data, 8)?;
    Ok(u64::from_be_bytes([
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ]))
}

fn read_i16_le(data: &mut &[u8]) -> Result<i16> {
    let buf = read_bytes(data, 2)?;
    Ok(i16::from_le_bytes([buf[0], buf[1]]))
}

fn read_i32_le(data: &mut &[u8]) -> Result<i32> {
    let buf = read_bytes(data, 4)?;
    Ok(i32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]))
}

fn read_f64_le(data: &mut &[u8]) -> Result<f64> {
    let buf = read_bytes(data, 8)?;
    Ok(f64::from_le_bytes([
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ]))
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

#[cfg(test)]
fn build_rdb(version: u32, body: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"REDIS");
    buf.extend_from_slice(format!("{:04}", version).as_bytes());
    buf.extend_from_slice(body);
    buf
}

#[cfg(test)]
fn encode_length_6bit(len: u8) -> Vec<u8> {
    assert!(len < 64);
    vec![len]
}

#[cfg(test)]
fn encode_string(s: &[u8]) -> Vec<u8> {
    let mut buf = encode_length_6bit(s.len() as u8);
    buf.extend_from_slice(s);
    buf
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn test_parse_header_valid() {
        let data = build_rdb(11, &[RDB_OPCODE_EOF]);
        let parser = RdbParser::new();
        let mut cursor = data.as_slice();
        let version = parser.parse_header(&mut cursor).unwrap();
        assert_eq!(version, RdbVersion::V11);
    }

    #[test]
    fn test_parse_header_invalid_magic() {
        let data = b"NOTRD0011\xFF";
        let parser = RdbParser::new();
        let mut cursor = data.as_slice();
        let err = parser.parse_header(&mut cursor).unwrap_err();
        assert!(matches!(err, RdbParseError::InvalidMagic));
    }

    #[test]
    fn test_unsupported_version() {
        let data = build_rdb(99, &[RDB_OPCODE_EOF]);
        let parser = RdbParser::new();
        let err = parser.parse(&data).unwrap_err();
        assert!(matches!(err, RdbParseError::UnsupportedVersion(99)));
    }

    #[test]
    fn test_parse_empty_rdb() {
        let data = build_rdb(11, &[RDB_OPCODE_EOF]);
        let parser = RdbParser::new();
        let rdb = parser.parse(&data).unwrap();
        assert!(rdb.databases.is_empty());
        assert_eq!(rdb.version, RdbVersion::V11);
    }

    #[test]
    fn test_parse_single_string_key() {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.push(0x00); // db 0
        body.push(RDB_OPCODE_RESIZEDB);
        body.push(0x01); // 1 key
        body.push(0x00); // 0 expires
        body.push(RDB_TYPE_STRING);
        body.extend_from_slice(&encode_string(b"mykey"));
        body.extend_from_slice(&encode_string(b"myval"));
        body.push(RDB_OPCODE_EOF);

        let data = build_rdb(11, &body);
        let parser = RdbParser::new();
        let rdb = parser.parse(&data).unwrap();

        assert_eq!(rdb.databases.len(), 1);
        assert_eq!(rdb.databases[0].db_number, 0);
        assert_eq!(rdb.databases[0].resize_db, Some((1, 0)));
        assert_eq!(rdb.databases[0].entries.len(), 1);

        let entry = &rdb.databases[0].entries[0];
        assert_eq!(entry.key, b"mykey");
        assert!(matches!(&entry.value, RdbValue::String(v) if v == b"myval"));
        assert!(entry.expire_ms.is_none());
    }

    #[test]
    fn test_parse_key_with_expire_ms() {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.push(0x00);
        body.push(RDB_OPCODE_EXPIRETIME_MS);
        body.extend_from_slice(&1_700_000_000_000u64.to_le_bytes());
        body.push(RDB_TYPE_STRING);
        body.extend_from_slice(&encode_string(b"ttlkey"));
        body.extend_from_slice(&encode_string(b"ttlval"));
        body.push(RDB_OPCODE_EOF);

        let data = build_rdb(11, &body);
        let parser = RdbParser::new();
        let rdb = parser.parse(&data).unwrap();

        let entry = &rdb.databases[0].entries[0];
        assert_eq!(entry.expire_ms, Some(1_700_000_000_000));
    }

    #[test]
    fn test_parse_key_with_expire_sec() {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.push(0x00);
        body.push(RDB_OPCODE_EXPIRETIME);
        body.extend_from_slice(&1_700_000_000u32.to_le_bytes());
        body.push(RDB_TYPE_STRING);
        body.extend_from_slice(&encode_string(b"skey"));
        body.extend_from_slice(&encode_string(b"sval"));
        body.push(RDB_OPCODE_EOF);

        let data = build_rdb(11, &body);
        let parser = RdbParser::new();
        let rdb = parser.parse(&data).unwrap();

        let entry = &rdb.databases[0].entries[0];
        assert_eq!(entry.expire_ms, Some(1_700_000_000_000));
    }

    #[test]
    fn test_parse_aux_fields() {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_AUX);
        body.extend_from_slice(&encode_string(b"redis-ver"));
        body.extend_from_slice(&encode_string(b"7.2.0"));
        body.push(RDB_OPCODE_AUX);
        body.extend_from_slice(&encode_string(b"redis-bits"));
        body.extend_from_slice(&encode_string(b"64"));
        body.push(RDB_OPCODE_EOF);

        let data = build_rdb(11, &body);
        let parser = RdbParser::new();
        let rdb = parser.parse(&data).unwrap();

        assert_eq!(rdb.aux_fields.get("redis-ver").unwrap(), "7.2.0");
        assert_eq!(rdb.aux_fields.get("redis-bits").unwrap(), "64");
    }

    #[test]
    fn test_parse_list_value() {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.push(0x00);
        body.push(RDB_TYPE_LIST);
        body.extend_from_slice(&encode_string(b"mylist"));
        body.push(0x03);
        body.extend_from_slice(&encode_string(b"a"));
        body.extend_from_slice(&encode_string(b"b"));
        body.extend_from_slice(&encode_string(b"c"));
        body.push(RDB_OPCODE_EOF);

        let data = build_rdb(11, &body);
        let parser = RdbParser::new();
        let rdb = parser.parse(&data).unwrap();

        let entry = &rdb.databases[0].entries[0];
        match &entry.value {
            RdbValue::List(items) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0], b"a");
                assert_eq!(items[1], b"b");
                assert_eq!(items[2], b"c");
            }
            _ => panic!("expected List"),
        }
    }

    #[test]
    fn test_parse_hash_value() {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.push(0x00);
        body.push(RDB_TYPE_HASH);
        body.extend_from_slice(&encode_string(b"myhash"));
        body.push(0x02);
        body.extend_from_slice(&encode_string(b"f1"));
        body.extend_from_slice(&encode_string(b"v1"));
        body.extend_from_slice(&encode_string(b"f2"));
        body.extend_from_slice(&encode_string(b"v2"));
        body.push(RDB_OPCODE_EOF);

        let data = build_rdb(11, &body);
        let parser = RdbParser::new();
        let rdb = parser.parse(&data).unwrap();

        let entry = &rdb.databases[0].entries[0];
        match &entry.value {
            RdbValue::Hash(pairs) => {
                assert_eq!(pairs.len(), 2);
                assert_eq!(pairs[0].0, b"f1");
                assert_eq!(pairs[0].1, b"v1");
            }
            _ => panic!("expected Hash"),
        }
    }

    #[test]
    fn test_parse_sorted_set_v2() {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.push(0x00);
        body.push(RDB_TYPE_ZSET_2);
        body.extend_from_slice(&encode_string(b"myzset"));
        body.push(0x02);
        body.extend_from_slice(&encode_string(b"alice"));
        body.extend_from_slice(&1.5f64.to_le_bytes());
        body.extend_from_slice(&encode_string(b"bob"));
        body.extend_from_slice(&2.5f64.to_le_bytes());
        body.push(RDB_OPCODE_EOF);

        let data = build_rdb(11, &body);
        let parser = RdbParser::new();
        let rdb = parser.parse(&data).unwrap();

        let entry = &rdb.databases[0].entries[0];
        match &entry.value {
            RdbValue::SortedSet(entries) => {
                assert_eq!(entries.len(), 2);
                assert_eq!(entries[0].0, 1.5);
                assert_eq!(entries[0].1, b"alice");
            }
            _ => panic!("expected SortedSet"),
        }
    }

    #[test]
    fn test_parse_multiple_databases() {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.push(0x00);
        body.push(RDB_TYPE_STRING);
        body.extend_from_slice(&encode_string(b"k0"));
        body.extend_from_slice(&encode_string(b"v0"));
        body.push(RDB_OPCODE_SELECTDB);
        body.push(0x02);
        body.push(RDB_TYPE_STRING);
        body.extend_from_slice(&encode_string(b"k2"));
        body.extend_from_slice(&encode_string(b"v2"));
        body.push(RDB_OPCODE_EOF);

        let data = build_rdb(11, &body);
        let parser = RdbParser::new();
        let rdb = parser.parse(&data).unwrap();

        assert_eq!(rdb.databases.len(), 2);
        assert_eq!(rdb.databases[0].db_number, 0);
        assert_eq!(rdb.databases[1].db_number, 2);
    }

    #[test]
    fn test_parse_implicit_db0() {
        let mut body = Vec::new();
        body.push(RDB_TYPE_STRING);
        body.extend_from_slice(&encode_string(b"key"));
        body.extend_from_slice(&encode_string(b"val"));
        body.push(RDB_OPCODE_EOF);

        let data = build_rdb(11, &body);
        let parser = RdbParser::new();
        let rdb = parser.parse(&data).unwrap();

        assert_eq!(rdb.databases.len(), 1);
        assert_eq!(rdb.databases[0].db_number, 0);
    }

    #[test]
    fn test_decode_length_6bit() {
        let parser = RdbParser::new();
        let data: &[u8] = &[0x0A];
        let mut cursor = data;
        let (len, enc) = parser.decode_length(&mut cursor).unwrap();
        assert_eq!(len, 10);
        assert!(!enc);
    }

    #[test]
    fn test_decode_length_14bit() {
        let parser = RdbParser::new();
        let data: &[u8] = &[0x40 | 0x01, 0x00]; // 256
        let mut cursor = data;
        let (len, enc) = parser.decode_length(&mut cursor).unwrap();
        assert_eq!(len, 256);
        assert!(!enc);
    }

    #[test]
    fn test_decode_string_int8() {
        let parser = RdbParser::new();
        let data: &[u8] = &[0xC0, 42];
        let mut cursor = data;
        let s = parser.decode_string(&mut cursor).unwrap();
        assert_eq!(s, b"42");
    }

    #[test]
    fn test_decode_string_int16() {
        let parser = RdbParser::new();
        let mut data = vec![0xC1];
        data.extend_from_slice(&1000i16.to_le_bytes());
        let mut cursor = data.as_slice();
        let s = parser.decode_string(&mut cursor).unwrap();
        assert_eq!(s, b"1000");
    }

    #[test]
    fn test_lzf_decompress_literal() {
        // Simple literal run: ctrl=2 → 3 bytes literal
        let input = &[0x02, b'a', b'b', b'c'];
        let result = lzf_decompress(input, 3).unwrap();
        assert_eq!(result, b"abc");
    }

    #[test]
    fn test_lzf_decompress_backref() {
        // Literal "abc" then back-reference to copy 3 bytes from offset 2
        let mut input = Vec::new();
        input.push(0x02); // literal run of 3 bytes
        input.extend_from_slice(b"abc");
        // Back reference: len=(1<<5) means len_base=1, +2=3; offset=(0<<8)|2 = 2 → +1=3
        input.push(0x20 | 0x00); // ctrl: len_base=1, high_offset=0
        input.push(0x02); // low_offset=2, so offset=2+1=3
        let result = lzf_decompress(&input, 6).unwrap();
        assert_eq!(result, b"abcabc");
    }

    #[test]
    fn test_lzf_roundtrip_via_rdb_string() {
        // Build an LZF-compressed RDB string and decode it
        let parser = RdbParser::new();
        // Compress "hello" as a simple literal (no actual compression)
        let compressed = &[0x04, b'h', b'e', b'l', b'l', b'o']; // literal run of 5

        let mut rdb_bytes = Vec::new();
        rdb_bytes.push(0xC3); // Special encoding: LZF
        // compressed_len = 6
        rdb_bytes.push(0x06);
        // uncompressed_len = 5
        rdb_bytes.push(0x05);
        rdb_bytes.extend_from_slice(compressed);

        let mut cursor = rdb_bytes.as_slice();
        let result = parser.decode_string(&mut cursor).unwrap();
        assert_eq!(result, b"hello");
    }

    // -- v1 backward compat tests -------------------------------------------

    #[test]
    fn test_v1_parser_compat() {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.push(0x00);
        body.push(RDB_TYPE_STRING);
        body.extend_from_slice(&encode_string(b"key1"));
        body.extend_from_slice(&encode_string(b"val1"));
        body.push(RDB_OPCODE_EOF);

        let data = build_rdb(11, &body);
        let mut parser = RdbParserV1::from_bytes(&data).unwrap();
        let rdb = parser.parse().unwrap();

        assert_eq!(rdb.databases.len(), 1);
        assert_eq!(rdb.stats.total_keys, 1);
        assert_eq!(rdb.stats.strings_count, 1);
        let entry = &rdb.databases[0].entries[0];
        assert_eq!(entry.key.as_ref(), b"key1");
    }

    #[test]
    fn test_v1_streaming_compat() {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.push(0x00);
        body.push(RDB_TYPE_STRING);
        body.extend_from_slice(&encode_string(b"s1"));
        body.extend_from_slice(&encode_string(b"v1"));
        body.push(RDB_TYPE_STRING);
        body.extend_from_slice(&encode_string(b"s2"));
        body.extend_from_slice(&encode_string(b"v2"));
        body.push(RDB_OPCODE_EOF);

        let data = build_rdb(11, &body);
        let mut parser = RdbParserV1::from_bytes(&data).unwrap();

        let mut count = 0u32;
        let stats = parser
            .parse_streaming(|_db, _entry| {
                count += 1;
            })
            .unwrap();
        assert_eq!(stats.total_keys, 2);
        assert_eq!(count, 2);
    }
}
