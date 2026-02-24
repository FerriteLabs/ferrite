//! Redis RDB File Parser
//!
//! Parses Redis RDB snapshot files for importing data into Ferrite during
//! migration. Supports RDB format versions 1–11 with streaming and full-parse
//! modes.
//!
//! # Features
//!
//! - Full RDB file parsing into an in-memory [`RdbData`] structure
//! - Streaming mode with per-entry callback for large files
//! - All core Redis value types (strings, lists, sets, sorted sets, hashes)
//! - Stream and consumer group support (struct-level)
//! - Length-encoded integer and string decoding
//! - LZF-compressed string stubs
//!
//! # Example
//!
//! ```ignore
//! use std::path::Path;
//! use ferrite::migration::rdb_parser::RdbParser;
//!
//! let mut parser = RdbParser::new(Path::new("dump.rdb"))?;
//! let data = parser.parse()?;
//! println!("Imported {} databases", data.databases.len());
//! ```

use bytes::Bytes;
use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::path::Path;
use std::time::Instant;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Constants – RDB magic & opcodes
// ---------------------------------------------------------------------------

/// RDB file magic prefix (`REDIS`)
const RDB_MAGIC: &[u8; 5] = b"REDIS";

/// Maximum supported RDB format version
const RDB_MAX_VERSION: u32 = 11;

// -- Opcodes ----------------------------------------------------------------

/// Auxiliary field (redis-ver, redis-bits, etc.)
const RDB_OPCODE_AUX: u8 = 0xFA;
/// Hash-table resize hint
const RDB_OPCODE_RESIZEDB: u8 = 0xFB;
/// Expire time in milliseconds (8-byte LE)
const RDB_OPCODE_EXPIRETIME_MS: u8 = 0xFC;
/// Expire time in seconds (4-byte LE)
const RDB_OPCODE_EXPIRETIME: u8 = 0xFD;
/// Database selector
const RDB_OPCODE_SELECTDB: u8 = 0xFE;
/// End of file
const RDB_OPCODE_EOF: u8 = 0xFF;

// -- Value-type encodings ---------------------------------------------------

const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_LIST: u8 = 1;
const RDB_TYPE_SET: u8 = 2;
const RDB_TYPE_ZSET: u8 = 3;
const RDB_TYPE_HASH: u8 = 4;
const RDB_TYPE_ZSET_2: u8 = 5;
const RDB_TYPE_MODULE: u8 = 6;
const RDB_TYPE_MODULE_2: u8 = 7;

// -- Encoded value types ----------------------------------------------------

const RDB_TYPE_HASH_ZIPMAP: u8 = 9;
const RDB_TYPE_LIST_ZIPLIST: u8 = 10;
const RDB_TYPE_SET_INTSET: u8 = 11;
const RDB_TYPE_ZSET_ZIPLIST: u8 = 12;
const RDB_TYPE_HASH_ZIPLIST: u8 = 13;
const RDB_TYPE_LIST_QUICKLIST: u8 = 14;
const RDB_TYPE_STREAM_LISTPACKS: u8 = 15;
const RDB_TYPE_SET_LISTPACK: u8 = 16;
const RDB_TYPE_ZSET_LISTPACK: u8 = 17;
const RDB_TYPE_HASH_LISTPACK: u8 = 18;
const RDB_TYPE_LIST_QUICKLIST_2: u8 = 19;
const RDB_TYPE_STREAM_LISTPACKS_2: u8 = 20;
const RDB_TYPE_STREAM_LISTPACKS_3: u8 = 21;

// -- Length-encoding special bits -------------------------------------------

/// 6-bit length
const RDB_6BITLEN: u8 = 0;
/// 14-bit length
const RDB_14BITLEN: u8 = 1;
/// 32-bit length (big-endian)
const RDB_32BITLEN: u8 = 0x80;
/// 64-bit length (big-endian)
const RDB_64BITLEN: u8 = 0x81;
/// Special encoding follows
const RDB_ENCVAL: u8 = 3;

// -- String special encodings -----------------------------------------------

/// 8-bit integer
const RDB_ENC_INT8: u8 = 0;
/// 16-bit integer (LE)
const RDB_ENC_INT16: u8 = 1;
/// 32-bit integer (LE)
const RDB_ENC_INT32: u8 = 2;
/// LZF-compressed string
const RDB_ENC_LZF: u8 = 3;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur during RDB parsing
#[derive(Debug, Clone, thiserror::Error)]
pub enum RdbError {
    /// File does not start with the `REDIS` magic
    #[error("invalid magic: expected REDIS header")]
    InvalidMagic,

    /// RDB version is higher than what this parser supports
    #[error("unsupported RDB version: {0} (max supported: {RDB_MAX_VERSION})")]
    UnsupportedVersion(u32),

    /// A value-type encoding is not yet implemented
    #[error("unsupported encoding: {0}")]
    UnsupportedEncoding(String),

    /// An encoding byte is invalid
    #[error("invalid encoding at offset {offset}: {detail}")]
    InvalidEncoding {
        /// Byte offset in the stream
        offset: u64,
        /// Human-readable detail
        detail: String,
    },

    /// Unexpected end of data
    #[error("truncated data: {0}")]
    TruncatedData(String),

    /// Wrapper for I/O errors
    #[error("I/O error: {0}")]
    IoError(String),

    /// Checksum mismatch
    #[error("checksum mismatch")]
    ChecksumMismatch,
}

impl From<std::io::Error> for RdbError {
    fn from(e: std::io::Error) -> Self {
        RdbError::IoError(e.to_string())
    }
}

/// Result type for RDB operations
pub type Result<T> = std::result::Result<T, RdbError>;

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// A fully-parsed RDB snapshot.
#[derive(Debug, Clone)]
pub struct RdbData {
    /// Databases contained in the RDB file
    pub databases: Vec<RdbDatabase>,
    /// Auxiliary fields (redis-ver, redis-bits, ctime, used-mem, etc.)
    pub aux_fields: HashMap<String, String>,
    /// Parsing statistics
    pub stats: RdbStats,
}

/// A single logical database inside the RDB.
#[derive(Debug, Clone)]
pub struct RdbDatabase {
    /// Database index (0–15 typically)
    pub db_number: u32,
    /// Key-value entries
    pub entries: Vec<RdbEntry>,
    /// Optional hash-table resize hint `(db_size, expire_size)`
    pub resize_db: Option<(u64, u64)>,
}

/// A single key-value entry.
#[derive(Debug, Clone)]
pub struct RdbEntry {
    /// Key bytes
    pub key: Bytes,
    /// Value
    pub value: RdbValue,
    /// Optional expiry as a Unix timestamp in milliseconds
    pub expire_at: Option<u64>,
}

/// Redis value types.
#[derive(Debug, Clone)]
pub enum RdbValue {
    /// Simple string / raw bytes
    String(Bytes),
    /// List of elements
    List(Vec<Bytes>),
    /// Unordered set of elements
    Set(Vec<Bytes>),
    /// Sorted set with scores
    SortedSet(Vec<(f64, Bytes)>),
    /// Hash map of field-value pairs
    Hash(Vec<(Bytes, Bytes)>),
    /// Redis Stream
    Stream {
        /// Stream entries
        entries: Vec<StreamEntry>,
        /// Consumer groups
        groups: Vec<ConsumerGroup>,
    },
    /// Redis Module data (opaque)
    Module {
        /// Module type name
        type_name: String,
        /// Raw module payload
        data: Bytes,
    },
}

/// A single stream entry.
#[derive(Debug, Clone)]
pub struct StreamEntry {
    /// Entry ID `(ms, seq)`
    pub id: (u64, u64),
    /// Field-value pairs
    pub fields: Vec<(Bytes, Bytes)>,
}

/// A consumer group on a stream.
#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    /// Group name
    pub name: String,
    /// Last delivered ID
    pub last_id: (u64, u64),
    /// Pending entries
    pub pending: Vec<PendingEntry>,
}

/// A pending entry in a consumer group.
#[derive(Debug, Clone)]
pub struct PendingEntry {
    /// Entry ID
    pub id: (u64, u64),
    /// Consumer name
    pub consumer: String,
    /// Delivery timestamp (ms)
    pub delivery_time: u64,
    /// Number of deliveries
    pub delivery_count: u64,
}

/// Parsing statistics.
#[derive(Debug, Clone, Default)]
pub struct RdbStats {
    /// Total keys parsed
    pub total_keys: u64,
    /// Number of databases
    pub databases_count: u64,
    /// Count of string values
    pub strings_count: u64,
    /// Count of list values
    pub lists_count: u64,
    /// Count of set values
    pub sets_count: u64,
    /// Count of sorted-set values
    pub sorted_sets_count: u64,
    /// Count of hash values
    pub hashes_count: u64,
    /// Count of stream values
    pub streams_count: u64,
    /// Total bytes consumed from the input
    pub bytes_processed: u64,
    /// Parse duration in microseconds
    pub parse_duration_us: u64,
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Redis RDB file parser.
///
/// Supports both full in-memory parsing via [`parse`](Self::parse) and
/// streaming mode via [`parse_streaming`](Self::parse_streaming) for
/// memory-efficient processing of large snapshots.
#[derive(Debug)]
pub struct RdbParser {
    /// Raw RDB bytes
    data: Vec<u8>,
    /// RDB format version extracted from the header
    rdb_version: u32,
}

impl RdbParser {
    /// Create a parser from a file on disk.
    pub fn new(path: &Path) -> Result<Self> {
        let data = std::fs::read(path).map_err(|e| RdbError::IoError(e.to_string()))?;
        Self::from_bytes(&data)
    }

    /// Create a parser from an in-memory byte slice.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 9 {
            return Err(RdbError::TruncatedData(
                "file too short for RDB header".into(),
            ));
        }

        // Validate magic
        if &data[..5] != RDB_MAGIC {
            return Err(RdbError::InvalidMagic);
        }

        // Parse version from ASCII digits (e.g. "0011" → 11)
        let version_str = std::str::from_utf8(&data[5..9]).map_err(|_| RdbError::InvalidMagic)?;
        let version: u32 = version_str.parse().map_err(|_| RdbError::InvalidMagic)?;

        if version > RDB_MAX_VERSION {
            return Err(RdbError::UnsupportedVersion(version));
        }

        debug!(version, bytes = data.len(), "parsed RDB header");

        Ok(Self {
            data: data.to_vec(),
            rdb_version: version,
        })
    }

    /// Return the RDB format version.
    pub fn version(&self) -> u32 {
        self.rdb_version
    }

    /// Parse the entire RDB file into an [`RdbData`] structure.
    pub fn parse(&mut self) -> Result<RdbData> {
        let start = Instant::now();
        let mut cursor = Cursor::new(&self.data);
        // Skip the 9-byte header we already validated
        cursor.set_position(9);

        let mut databases: Vec<RdbDatabase> = Vec::new();
        let mut aux_fields: HashMap<String, String> = HashMap::new();
        let mut stats = RdbStats::default();

        // Current DB context
        let mut current_db: Option<RdbDatabase> = None;
        let mut current_expire: Option<u64> = None;

        loop {
            let opcode = read_byte(&mut cursor)?;

            match opcode {
                RDB_OPCODE_AUX => {
                    let key = read_string(&mut cursor)?;
                    let value = read_string(&mut cursor)?;
                    let key_str = String::from_utf8_lossy(&key).to_string();
                    let val_str = String::from_utf8_lossy(&value).to_string();
                    debug!(key = %key_str, value = %val_str, "aux field");
                    aux_fields.insert(key_str, val_str);
                }

                RDB_OPCODE_SELECTDB => {
                    // Flush previous database
                    if let Some(db) = current_db.take() {
                        stats.databases_count += 1;
                        databases.push(db);
                    }
                    let db_number = read_length(&mut cursor)? as u32;
                    debug!(db_number, "select database");
                    current_db = Some(RdbDatabase {
                        db_number,
                        entries: Vec::new(),
                        resize_db: None,
                    });
                }

                RDB_OPCODE_RESIZEDB => {
                    let db_size = read_length(&mut cursor)?;
                    let expire_size = read_length(&mut cursor)?;
                    if let Some(ref mut db) = current_db {
                        db.resize_db = Some((db_size, expire_size));
                    }
                }

                RDB_OPCODE_EXPIRETIME_MS => {
                    current_expire = Some(read_u64_le(&mut cursor)?);
                }

                RDB_OPCODE_EXPIRETIME => {
                    let secs = read_u32_le(&mut cursor)? as u64;
                    current_expire = Some(secs * 1000);
                }

                RDB_OPCODE_EOF => {
                    // Flush last database
                    if let Some(db) = current_db.take() {
                        stats.databases_count += 1;
                        databases.push(db);
                    }
                    // Remaining bytes are an optional 8-byte CRC64 checksum
                    debug!("reached EOF opcode");
                    break;
                }

                // Otherwise it's a value-type byte
                value_type => {
                    let key = read_string(&mut cursor)?;
                    let value = read_value(value_type, &mut cursor)?;
                    update_stats(&value, &mut stats);
                    stats.total_keys += 1;

                    let entry = RdbEntry {
                        key: Bytes::from(key),
                        value,
                        expire_at: current_expire.take(),
                    };

                    // Ensure we have a database context (db 0 is implicit)
                    if current_db.is_none() {
                        current_db = Some(RdbDatabase {
                            db_number: 0,
                            entries: Vec::new(),
                            resize_db: None,
                        });
                    }
                    current_db.as_mut().unwrap().entries.push(entry);
                }
            }
        }

        stats.bytes_processed = cursor.position();
        stats.parse_duration_us = start.elapsed().as_micros() as u64;

        info!(
            keys = stats.total_keys,
            databases = stats.databases_count,
            bytes = stats.bytes_processed,
            duration_us = stats.parse_duration_us,
            "RDB parse complete"
        );

        Ok(RdbData {
            databases,
            aux_fields,
            stats,
        })
    }

    /// Parse the RDB file in streaming mode, invoking `callback` for every
    /// key-value entry. This avoids holding the full dataset in memory.
    pub fn parse_streaming<F>(&mut self, mut callback: F) -> Result<RdbStats>
    where
        F: FnMut(u32, &RdbEntry),
    {
        let start = Instant::now();
        let mut cursor = Cursor::new(&self.data);
        cursor.set_position(9);

        let mut stats = RdbStats::default();
        let mut current_db_number: u32 = 0;
        let mut current_expire: Option<u64> = None;

        loop {
            let opcode = read_byte(&mut cursor)?;

            match opcode {
                RDB_OPCODE_AUX => {
                    let _key = read_string(&mut cursor)?;
                    let _value = read_string(&mut cursor)?;
                }

                RDB_OPCODE_SELECTDB => {
                    current_db_number = read_length(&mut cursor)? as u32;
                    stats.databases_count += 1;
                }

                RDB_OPCODE_RESIZEDB => {
                    let _db_size = read_length(&mut cursor)?;
                    let _expire_size = read_length(&mut cursor)?;
                }

                RDB_OPCODE_EXPIRETIME_MS => {
                    current_expire = Some(read_u64_le(&mut cursor)?);
                }

                RDB_OPCODE_EXPIRETIME => {
                    let secs = read_u32_le(&mut cursor)? as u64;
                    current_expire = Some(secs * 1000);
                }

                RDB_OPCODE_EOF => {
                    break;
                }

                value_type => {
                    let key = read_string(&mut cursor)?;
                    let value = read_value(value_type, &mut cursor)?;
                    update_stats(&value, &mut stats);
                    stats.total_keys += 1;

                    let entry = RdbEntry {
                        key: Bytes::from(key),
                        value,
                        expire_at: current_expire.take(),
                    };

                    callback(current_db_number, &entry);
                }
            }
        }

        stats.bytes_processed = cursor.position();
        stats.parse_duration_us = start.elapsed().as_micros() as u64;

        info!(
            keys = stats.total_keys,
            duration_us = stats.parse_duration_us,
            "RDB streaming parse complete"
        );

        Ok(stats)
    }
}

// ---------------------------------------------------------------------------
// Low-level read helpers
// ---------------------------------------------------------------------------

/// Read a single byte from the cursor.
fn read_byte(cursor: &mut Cursor<&Vec<u8>>) -> Result<u8> {
    let mut buf = [0u8; 1];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| RdbError::TruncatedData("unexpected end of data".into()))?;
    Ok(buf[0])
}

/// Read exactly `n` bytes.
fn read_exact(cursor: &mut Cursor<&Vec<u8>>, n: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; n];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| RdbError::TruncatedData(format!("expected {} bytes", n)))?;
    Ok(buf)
}

/// Read a 4-byte little-endian u32.
fn read_u32_le(cursor: &mut Cursor<&Vec<u8>>) -> Result<u32> {
    let buf = read_exact(cursor, 4)?;
    Ok(u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]))
}

/// Read an 8-byte little-endian u64.
fn read_u64_le(cursor: &mut Cursor<&Vec<u8>>) -> Result<u64> {
    let buf = read_exact(cursor, 8)?;
    Ok(u64::from_le_bytes([
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ]))
}

/// Read an 8-byte little-endian f64 (binary double).
fn read_f64_le(cursor: &mut Cursor<&Vec<u8>>) -> Result<f64> {
    let buf = read_exact(cursor, 8)?;
    Ok(f64::from_le_bytes([
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ]))
}

/// Read a length-encoded integer.
///
/// Redis uses a variable-length encoding:
/// - `00` prefix → 6-bit length in the remaining bits
/// - `01` prefix → 14-bit length across two bytes
/// - `10 000000` (0x80) → next 4 bytes are a 32-bit BE length
/// - `10 000001` (0x81) → next 8 bytes are a 64-bit BE length
/// - `11` prefix → special encoding (integer / LZF), returned via error
///
/// Returns `Ok(length)` for normal lengths. For special encodings, the
/// caller should use [`read_string`] instead, which handles both cases.
fn read_length(cursor: &mut Cursor<&Vec<u8>>) -> Result<u64> {
    let byte = read_byte(cursor)?;
    let enc_type = (byte & 0xC0) >> 6;

    match enc_type {
        RDB_6BITLEN => Ok((byte & 0x3F) as u64),
        RDB_14BITLEN => {
            let next = read_byte(cursor)?;
            Ok((((byte & 0x3F) as u64) << 8) | next as u64)
        }
        // enc_type == 2 but we match on the full byte for 0x80 / 0x81
        _ if byte == RDB_32BITLEN => {
            let buf = read_exact(cursor, 4)?;
            Ok(u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as u64)
        }
        _ if byte == RDB_64BITLEN => {
            let buf = read_exact(cursor, 8)?;
            Ok(u64::from_be_bytes([
                buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
            ]))
        }
        RDB_ENCVAL => {
            // Special encoding – should not be reached when we expect a plain
            // length. Return the sub-type so callers can handle it.
            Err(RdbError::InvalidEncoding {
                offset: cursor.position(),
                detail: format!("unexpected special encoding {}", byte & 0x3F),
            })
        }
        _ => Err(RdbError::InvalidEncoding {
            offset: cursor.position(),
            detail: format!("unknown length encoding byte 0x{:02X}", byte),
        }),
    }
}

/// Read a length-encoded integer, returning `(length, is_encoded)`.
///
/// When `is_encoded` is true the returned value is the encoding sub-type
/// (INT8, INT16, INT32, LZF) rather than an actual length.
fn read_length_with_encoding(cursor: &mut Cursor<&Vec<u8>>) -> Result<(u64, bool)> {
    let byte = read_byte(cursor)?;
    let enc_type = (byte & 0xC0) >> 6;

    match enc_type {
        RDB_6BITLEN => Ok(((byte & 0x3F) as u64, false)),
        RDB_14BITLEN => {
            let next = read_byte(cursor)?;
            Ok(((((byte & 0x3F) as u64) << 8) | next as u64, false))
        }
        _ if byte == RDB_32BITLEN => {
            let buf = read_exact(cursor, 4)?;
            Ok((
                u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as u64,
                false,
            ))
        }
        _ if byte == RDB_64BITLEN => {
            let buf = read_exact(cursor, 8)?;
            Ok((
                u64::from_be_bytes([
                    buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
                ]),
                false,
            ))
        }
        RDB_ENCVAL => Ok(((byte & 0x3F) as u64, true)),
        _ => Err(RdbError::InvalidEncoding {
            offset: cursor.position(),
            detail: format!("unknown length encoding byte 0x{:02X}", byte),
        }),
    }
}

/// Read a length-prefixed or specially-encoded string.
fn read_string(cursor: &mut Cursor<&Vec<u8>>) -> Result<Vec<u8>> {
    let (len, is_encoded) = read_length_with_encoding(cursor)?;

    if !is_encoded {
        return read_exact(cursor, len as usize);
    }

    // Special encoding
    match len as u8 {
        RDB_ENC_INT8 => {
            let v = read_byte(cursor)? as i8;
            Ok(v.to_string().into_bytes())
        }
        RDB_ENC_INT16 => {
            let buf = read_exact(cursor, 2)?;
            let v = i16::from_le_bytes([buf[0], buf[1]]);
            Ok(v.to_string().into_bytes())
        }
        RDB_ENC_INT32 => {
            let buf = read_exact(cursor, 4)?;
            let v = i32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
            Ok(v.to_string().into_bytes())
        }
        RDB_ENC_LZF => {
            let compressed_len = read_length(cursor)? as usize;
            let _uncompressed_len = read_length(cursor)? as usize;
            // Skip the compressed payload – LZF decompression is not yet implemented
            let _compressed = read_exact(cursor, compressed_len)?;
            warn!("LZF-compressed string encountered; returning raw compressed bytes");
            Err(RdbError::UnsupportedEncoding(
                "LZF string decompression not yet implemented".into(),
            ))
        }
        other => Err(RdbError::InvalidEncoding {
            offset: cursor.position(),
            detail: format!("unknown string encoding sub-type {}", other),
        }),
    }
}

/// Read an RDB-encoded double (used in ZSET v1).
fn read_rdb_double(cursor: &mut Cursor<&Vec<u8>>) -> Result<f64> {
    let len = read_byte(cursor)?;
    match len {
        253 => Ok(f64::NAN),
        254 => Ok(f64::INFINITY),
        255 => Ok(f64::NEG_INFINITY),
        _ => {
            let buf = read_exact(cursor, len as usize)?;
            let s = String::from_utf8_lossy(&buf);
            s.parse::<f64>().map_err(|_| RdbError::InvalidEncoding {
                offset: cursor.position(),
                detail: format!("invalid double string: {}", s),
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Value reading
// ---------------------------------------------------------------------------

/// Read a value of the given type.
fn read_value(value_type: u8, cursor: &mut Cursor<&Vec<u8>>) -> Result<RdbValue> {
    match value_type {
        RDB_TYPE_STRING => {
            let data = read_string(cursor)?;
            Ok(RdbValue::String(Bytes::from(data)))
        }

        RDB_TYPE_LIST => {
            let len = read_length(cursor)?;
            let mut items = Vec::with_capacity(len as usize);
            for _ in 0..len {
                items.push(Bytes::from(read_string(cursor)?));
            }
            Ok(RdbValue::List(items))
        }

        RDB_TYPE_SET => {
            let len = read_length(cursor)?;
            let mut members = Vec::with_capacity(len as usize);
            for _ in 0..len {
                members.push(Bytes::from(read_string(cursor)?));
            }
            Ok(RdbValue::Set(members))
        }

        RDB_TYPE_ZSET => {
            let len = read_length(cursor)?;
            let mut entries = Vec::with_capacity(len as usize);
            for _ in 0..len {
                let member = Bytes::from(read_string(cursor)?);
                let score = read_rdb_double(cursor)?;
                entries.push((score, member));
            }
            Ok(RdbValue::SortedSet(entries))
        }

        RDB_TYPE_ZSET_2 => {
            let len = read_length(cursor)?;
            let mut entries = Vec::with_capacity(len as usize);
            for _ in 0..len {
                let member = Bytes::from(read_string(cursor)?);
                let score = read_f64_le(cursor)?;
                entries.push((score, member));
            }
            Ok(RdbValue::SortedSet(entries))
        }

        RDB_TYPE_HASH => {
            let len = read_length(cursor)?;
            let mut pairs = Vec::with_capacity(len as usize);
            for _ in 0..len {
                let field = Bytes::from(read_string(cursor)?);
                let value = Bytes::from(read_string(cursor)?);
                pairs.push((field, value));
            }
            Ok(RdbValue::Hash(pairs))
        }

        // -- Compact / compressed encodings (stubs) -------------------------
        RDB_TYPE_HASH_ZIPMAP => Err(RdbError::UnsupportedEncoding(
            "HASH_ZIPMAP encoding not yet implemented".into(),
        )),

        RDB_TYPE_LIST_ZIPLIST => Err(RdbError::UnsupportedEncoding(
            "LIST_ZIPLIST encoding not yet implemented".into(),
        )),

        RDB_TYPE_SET_INTSET => Err(RdbError::UnsupportedEncoding(
            "SET_INTSET encoding not yet implemented".into(),
        )),

        RDB_TYPE_ZSET_ZIPLIST => Err(RdbError::UnsupportedEncoding(
            "ZSET_ZIPLIST encoding not yet implemented".into(),
        )),

        RDB_TYPE_HASH_ZIPLIST => Err(RdbError::UnsupportedEncoding(
            "HASH_ZIPLIST encoding not yet implemented".into(),
        )),

        RDB_TYPE_LIST_QUICKLIST => Err(RdbError::UnsupportedEncoding(
            "LIST_QUICKLIST encoding not yet implemented".into(),
        )),

        RDB_TYPE_LIST_QUICKLIST_2 => Err(RdbError::UnsupportedEncoding(
            "LIST_QUICKLIST_2 encoding not yet implemented".into(),
        )),

        RDB_TYPE_SET_LISTPACK => Err(RdbError::UnsupportedEncoding(
            "SET_LISTPACK encoding not yet implemented".into(),
        )),

        RDB_TYPE_ZSET_LISTPACK => Err(RdbError::UnsupportedEncoding(
            "ZSET_LISTPACK encoding not yet implemented".into(),
        )),

        RDB_TYPE_HASH_LISTPACK => Err(RdbError::UnsupportedEncoding(
            "HASH_LISTPACK encoding not yet implemented".into(),
        )),

        RDB_TYPE_STREAM_LISTPACKS | RDB_TYPE_STREAM_LISTPACKS_2 | RDB_TYPE_STREAM_LISTPACKS_3 => {
            Err(RdbError::UnsupportedEncoding(
                "STREAM_LISTPACKS encoding not yet implemented".into(),
            ))
        }

        RDB_TYPE_MODULE | RDB_TYPE_MODULE_2 => Err(RdbError::UnsupportedEncoding(
            "MODULE encoding not yet implemented".into(),
        )),

        other => Err(RdbError::InvalidEncoding {
            offset: cursor.position(),
            detail: format!("unknown value type 0x{:02X}", other),
        }),
    }
}

/// Update running statistics for a parsed value.
fn update_stats(value: &RdbValue, stats: &mut RdbStats) {
    match value {
        RdbValue::String(_) => stats.strings_count += 1,
        RdbValue::List(_) => stats.lists_count += 1,
        RdbValue::Set(_) => stats.sets_count += 1,
        RdbValue::SortedSet(_) => stats.sorted_sets_count += 1,
        RdbValue::Hash(_) => stats.hashes_count += 1,
        RdbValue::Stream { .. } => stats.streams_count += 1,
        RdbValue::Module { .. } => {}
    }
}

// ---------------------------------------------------------------------------
// Test helpers – build minimal RDB byte sequences
// ---------------------------------------------------------------------------

/// Helper to build a minimal valid RDB file for testing.
#[cfg(test)]
fn build_rdb(version: u32, body: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"REDIS");
    buf.extend_from_slice(format!("{:04}", version).as_bytes());
    buf.extend_from_slice(body);
    buf
}

/// Encode a length as a 6-bit length-prefix byte (for lengths 0..63).
#[cfg(test)]
fn encode_length_6bit(len: u8) -> Vec<u8> {
    assert!(len < 64);
    vec![len]
}

/// Encode a raw string: length-prefix + bytes.
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
    use std::io::Cursor;

    // -- Header / magic tests -----------------------------------------------

    #[test]
    fn test_valid_magic_v11() {
        let data = build_rdb(11, &[RDB_OPCODE_EOF]);
        let parser = RdbParser::from_bytes(&data).unwrap();
        assert_eq!(parser.version(), 11);
    }

    #[test]
    fn test_valid_magic_v9() {
        let data = build_rdb(9, &[RDB_OPCODE_EOF]);
        let parser = RdbParser::from_bytes(&data).unwrap();
        assert_eq!(parser.version(), 9);
    }

    #[test]
    fn test_invalid_magic() {
        let data = b"NOTRD0011\xFF";
        let err = RdbParser::from_bytes(data).unwrap_err();
        assert!(matches!(err, RdbError::InvalidMagic));
    }

    #[test]
    fn test_unsupported_version() {
        let data = build_rdb(99, &[RDB_OPCODE_EOF]);
        let err = RdbParser::from_bytes(&data).unwrap_err();
        assert!(matches!(err, RdbError::UnsupportedVersion(99)));
    }

    #[test]
    fn test_truncated_header() {
        let data = b"REDIS";
        let err = RdbParser::from_bytes(data).unwrap_err();
        assert!(matches!(err, RdbError::TruncatedData(_)));
    }

    // -- Length-encoded integer tests ----------------------------------------

    #[test]
    fn test_length_6bit() {
        let data = vec![0x0A]; // 10
        let mut cursor = Cursor::new(&data);
        let (len, enc) = read_length_with_encoding(&mut cursor).unwrap();
        assert_eq!(len, 10);
        assert!(!enc);
    }

    #[test]
    fn test_length_14bit() {
        // 01_HHHHHH LLLLLLLL → 0x40 | high_6, low_8
        let data = vec![0x40 | 0x01, 0x00]; // (1 << 8) | 0 = 256
        let mut cursor = Cursor::new(&data);
        let (len, enc) = read_length_with_encoding(&mut cursor).unwrap();
        assert_eq!(len, 256);
        assert!(!enc);
    }

    #[test]
    fn test_length_32bit() {
        let mut data = vec![RDB_32BITLEN];
        data.extend_from_slice(&1000u32.to_be_bytes());
        let mut cursor = Cursor::new(&data);
        let (len, enc) = read_length_with_encoding(&mut cursor).unwrap();
        assert_eq!(len, 1000);
        assert!(!enc);
    }

    #[test]
    fn test_length_64bit() {
        let mut data = vec![RDB_64BITLEN];
        data.extend_from_slice(&(u32::MAX as u64 + 1).to_be_bytes());
        let mut cursor = Cursor::new(&data);
        let (len, enc) = read_length_with_encoding(&mut cursor).unwrap();
        assert_eq!(len, u32::MAX as u64 + 1);
        assert!(!enc);
    }

    #[test]
    fn test_length_special_encoding() {
        // 11_000001 → special encoding, sub-type 1
        let data = vec![0xC1];
        let mut cursor = Cursor::new(&data);
        let (val, enc) = read_length_with_encoding(&mut cursor).unwrap();
        assert_eq!(val, 1); // sub-type
        assert!(enc);
    }

    // -- String encoding tests ----------------------------------------------

    #[test]
    fn test_read_raw_string() {
        let data = encode_string(b"hello");
        let mut cursor = Cursor::new(&data);
        let s = read_string(&mut cursor).unwrap();
        assert_eq!(s, b"hello");
    }

    #[test]
    fn test_read_int8_string() {
        // 0xC0 = special enc, sub-type 0 (INT8), then one byte value
        let data = vec![0xC0, 42];
        let mut cursor = Cursor::new(&data);
        let s = read_string(&mut cursor).unwrap();
        assert_eq!(s, b"42");
    }

    #[test]
    fn test_read_int16_string() {
        // 0xC1 = special enc, sub-type 1 (INT16), then two LE bytes
        let mut data = vec![0xC1];
        data.extend_from_slice(&1000i16.to_le_bytes());
        let mut cursor = Cursor::new(&data);
        let s = read_string(&mut cursor).unwrap();
        assert_eq!(s, b"1000");
    }

    #[test]
    fn test_read_int32_string() {
        // 0xC2 = special enc, sub-type 2 (INT32), then four LE bytes
        let mut data = vec![0xC2];
        data.extend_from_slice(&100_000i32.to_le_bytes());
        let mut cursor = Cursor::new(&data);
        let s = read_string(&mut cursor).unwrap();
        assert_eq!(s, b"100000");
    }

    #[test]
    fn test_read_empty_string() {
        let data = encode_string(b"");
        let mut cursor = Cursor::new(&data);
        let s = read_string(&mut cursor).unwrap();
        assert!(s.is_empty());
    }

    // -- Full RDB parse tests -----------------------------------------------

    #[test]
    fn test_parse_empty_rdb() {
        let data = build_rdb(11, &[RDB_OPCODE_EOF]);
        let mut parser = RdbParser::from_bytes(&data).unwrap();
        let rdb = parser.parse().unwrap();
        assert!(rdb.databases.is_empty());
        assert_eq!(rdb.stats.total_keys, 0);
    }

    #[test]
    fn test_parse_single_string_key() {
        let mut body = Vec::new();
        // SELECT DB 0
        body.push(RDB_OPCODE_SELECTDB);
        body.push(0x00); // db 0
                         // RESIZEDB hint
        body.push(RDB_OPCODE_RESIZEDB);
        body.push(0x01); // 1 key
        body.push(0x00); // 0 expires
                         // String entry: type=0, key="mykey", value="myval"
        body.push(RDB_TYPE_STRING);
        body.extend_from_slice(&encode_string(b"mykey"));
        body.extend_from_slice(&encode_string(b"myval"));
        // EOF
        body.push(RDB_OPCODE_EOF);

        let data = build_rdb(11, &body);
        let mut parser = RdbParser::from_bytes(&data).unwrap();
        let rdb = parser.parse().unwrap();

        assert_eq!(rdb.databases.len(), 1);
        assert_eq!(rdb.databases[0].db_number, 0);
        assert_eq!(rdb.databases[0].resize_db, Some((1, 0)));
        assert_eq!(rdb.databases[0].entries.len(), 1);

        let entry = &rdb.databases[0].entries[0];
        assert_eq!(entry.key.as_ref(), b"mykey");
        assert!(matches!(&entry.value, RdbValue::String(v) if v.as_ref() == b"myval"));
        assert!(entry.expire_at.is_none());
        assert_eq!(rdb.stats.total_keys, 1);
        assert_eq!(rdb.stats.strings_count, 1);
    }

    #[test]
    fn test_parse_key_with_expire_ms() {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.push(0x00);
        // Expire in ms
        body.push(RDB_OPCODE_EXPIRETIME_MS);
        body.extend_from_slice(&1_700_000_000_000u64.to_le_bytes());
        // String entry
        body.push(RDB_TYPE_STRING);
        body.extend_from_slice(&encode_string(b"ttlkey"));
        body.extend_from_slice(&encode_string(b"ttlval"));
        body.push(RDB_OPCODE_EOF);

        let data = build_rdb(11, &body);
        let mut parser = RdbParser::from_bytes(&data).unwrap();
        let rdb = parser.parse().unwrap();

        let entry = &rdb.databases[0].entries[0];
        assert_eq!(entry.expire_at, Some(1_700_000_000_000));
    }

    #[test]
    fn test_parse_key_with_expire_sec() {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.push(0x00);
        // Expire in seconds
        body.push(RDB_OPCODE_EXPIRETIME);
        body.extend_from_slice(&1_700_000_000u32.to_le_bytes());
        // String entry
        body.push(RDB_TYPE_STRING);
        body.extend_from_slice(&encode_string(b"skey"));
        body.extend_from_slice(&encode_string(b"sval"));
        body.push(RDB_OPCODE_EOF);

        let data = build_rdb(11, &body);
        let mut parser = RdbParser::from_bytes(&data).unwrap();
        let rdb = parser.parse().unwrap();

        let entry = &rdb.databases[0].entries[0];
        assert_eq!(entry.expire_at, Some(1_700_000_000_000));
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
        let mut parser = RdbParser::from_bytes(&data).unwrap();
        let rdb = parser.parse().unwrap();

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
        body.push(0x03); // 3 elements
        body.extend_from_slice(&encode_string(b"a"));
        body.extend_from_slice(&encode_string(b"b"));
        body.extend_from_slice(&encode_string(b"c"));
        body.push(RDB_OPCODE_EOF);

        let data = build_rdb(11, &body);
        let mut parser = RdbParser::from_bytes(&data).unwrap();
        let rdb = parser.parse().unwrap();

        let entry = &rdb.databases[0].entries[0];
        match &entry.value {
            RdbValue::List(items) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0].as_ref(), b"a");
                assert_eq!(items[1].as_ref(), b"b");
                assert_eq!(items[2].as_ref(), b"c");
            }
            _ => panic!("expected List"),
        }
        assert_eq!(rdb.stats.lists_count, 1);
    }

    #[test]
    fn test_parse_set_value() {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.push(0x00);
        body.push(RDB_TYPE_SET);
        body.extend_from_slice(&encode_string(b"myset"));
        body.push(0x02); // 2 members
        body.extend_from_slice(&encode_string(b"x"));
        body.extend_from_slice(&encode_string(b"y"));
        body.push(RDB_OPCODE_EOF);

        let data = build_rdb(11, &body);
        let mut parser = RdbParser::from_bytes(&data).unwrap();
        let rdb = parser.parse().unwrap();

        let entry = &rdb.databases[0].entries[0];
        match &entry.value {
            RdbValue::Set(members) => {
                assert_eq!(members.len(), 2);
            }
            _ => panic!("expected Set"),
        }
        assert_eq!(rdb.stats.sets_count, 1);
    }

    #[test]
    fn test_parse_hash_value() {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.push(0x00);
        body.push(RDB_TYPE_HASH);
        body.extend_from_slice(&encode_string(b"myhash"));
        body.push(0x02); // 2 field-value pairs
        body.extend_from_slice(&encode_string(b"f1"));
        body.extend_from_slice(&encode_string(b"v1"));
        body.extend_from_slice(&encode_string(b"f2"));
        body.extend_from_slice(&encode_string(b"v2"));
        body.push(RDB_OPCODE_EOF);

        let data = build_rdb(11, &body);
        let mut parser = RdbParser::from_bytes(&data).unwrap();
        let rdb = parser.parse().unwrap();

        let entry = &rdb.databases[0].entries[0];
        match &entry.value {
            RdbValue::Hash(pairs) => {
                assert_eq!(pairs.len(), 2);
                assert_eq!(pairs[0].0.as_ref(), b"f1");
                assert_eq!(pairs[0].1.as_ref(), b"v1");
            }
            _ => panic!("expected Hash"),
        }
        assert_eq!(rdb.stats.hashes_count, 1);
    }

    #[test]
    fn test_parse_sorted_set_v2() {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.push(0x00);
        body.push(RDB_TYPE_ZSET_2);
        body.extend_from_slice(&encode_string(b"myzset"));
        body.push(0x02); // 2 entries
        body.extend_from_slice(&encode_string(b"alice"));
        body.extend_from_slice(&1.5f64.to_le_bytes());
        body.extend_from_slice(&encode_string(b"bob"));
        body.extend_from_slice(&2.5f64.to_le_bytes());
        body.push(RDB_OPCODE_EOF);

        let data = build_rdb(11, &body);
        let mut parser = RdbParser::from_bytes(&data).unwrap();
        let rdb = parser.parse().unwrap();

        let entry = &rdb.databases[0].entries[0];
        match &entry.value {
            RdbValue::SortedSet(entries) => {
                assert_eq!(entries.len(), 2);
                assert_eq!(entries[0].0, 1.5);
                assert_eq!(entries[0].1.as_ref(), b"alice");
                assert_eq!(entries[1].0, 2.5);
            }
            _ => panic!("expected SortedSet"),
        }
        assert_eq!(rdb.stats.sorted_sets_count, 1);
    }

    #[test]
    fn test_parse_multiple_databases() {
        let mut body = Vec::new();
        // DB 0
        body.push(RDB_OPCODE_SELECTDB);
        body.push(0x00);
        body.push(RDB_TYPE_STRING);
        body.extend_from_slice(&encode_string(b"k0"));
        body.extend_from_slice(&encode_string(b"v0"));
        // DB 2
        body.push(RDB_OPCODE_SELECTDB);
        body.push(0x02);
        body.push(RDB_TYPE_STRING);
        body.extend_from_slice(&encode_string(b"k2"));
        body.extend_from_slice(&encode_string(b"v2"));
        body.push(RDB_OPCODE_EOF);

        let data = build_rdb(11, &body);
        let mut parser = RdbParser::from_bytes(&data).unwrap();
        let rdb = parser.parse().unwrap();

        assert_eq!(rdb.databases.len(), 2);
        assert_eq!(rdb.databases[0].db_number, 0);
        assert_eq!(rdb.databases[1].db_number, 2);
        assert_eq!(rdb.stats.databases_count, 2);
        assert_eq!(rdb.stats.total_keys, 2);
    }

    #[test]
    fn test_parse_implicit_db0() {
        // Entry without a preceding SELECTDB → should default to db 0
        let mut body = Vec::new();
        body.push(RDB_TYPE_STRING);
        body.extend_from_slice(&encode_string(b"key"));
        body.extend_from_slice(&encode_string(b"val"));
        body.push(RDB_OPCODE_EOF);

        let data = build_rdb(11, &body);
        let mut parser = RdbParser::from_bytes(&data).unwrap();
        let rdb = parser.parse().unwrap();

        assert_eq!(rdb.databases.len(), 1);
        assert_eq!(rdb.databases[0].db_number, 0);
    }

    // -- Streaming parse test -----------------------------------------------

    #[test]
    fn test_streaming_parse() {
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
        let mut parser = RdbParser::from_bytes(&data).unwrap();

        let mut collected: Vec<(u32, Bytes)> = Vec::new();
        let stats = parser
            .parse_streaming(|db, entry| {
                collected.push((db, entry.key.clone()));
            })
            .unwrap();

        assert_eq!(stats.total_keys, 2);
        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0], (0, Bytes::from_static(b"s1")));
        assert_eq!(collected[1], (0, Bytes::from_static(b"s2")));
    }

    // -- Error handling tests -----------------------------------------------

    #[test]
    fn test_truncated_string() {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.push(0x00);
        body.push(RDB_TYPE_STRING);
        body.extend_from_slice(&[0x05]); // key length 5 but no bytes follow
                                         // No key data, no value, no EOF

        let data = build_rdb(11, &body);
        let mut parser = RdbParser::from_bytes(&data).unwrap();
        let err = parser.parse().unwrap_err();
        assert!(matches!(err, RdbError::TruncatedData(_)));
    }

    #[test]
    fn test_unknown_value_type() {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.push(0x00);
        body.push(0xEE); // bogus value type
        body.extend_from_slice(&encode_string(b"key"));
        body.push(RDB_OPCODE_EOF);

        let data = build_rdb(11, &body);
        let mut parser = RdbParser::from_bytes(&data).unwrap();
        // 0xEE will be treated as a value type; reading key succeeds but
        // the value read for type 0xEE should fail.
        let err = parser.parse().unwrap_err();
        assert!(matches!(
            err,
            RdbError::InvalidEncoding { .. } | RdbError::TruncatedData(_)
        ));
    }

    #[test]
    fn test_unsupported_ziplist_encoding() {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.push(0x00);
        body.push(RDB_TYPE_LIST_ZIPLIST);
        body.extend_from_slice(&encode_string(b"zkey"));
        body.push(RDB_OPCODE_EOF);

        let data = build_rdb(11, &body);
        let mut parser = RdbParser::from_bytes(&data).unwrap();
        let err = parser.parse().unwrap_err();
        assert!(matches!(err, RdbError::UnsupportedEncoding(_)));
    }

    #[test]
    fn test_stats_accuracy() {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.push(0x00);
        // 2 strings
        for i in 0..2u8 {
            body.push(RDB_TYPE_STRING);
            let k = format!("str{}", i);
            body.extend_from_slice(&encode_string(k.as_bytes()));
            body.extend_from_slice(&encode_string(b"v"));
        }
        // 1 list
        body.push(RDB_TYPE_LIST);
        body.extend_from_slice(&encode_string(b"list0"));
        body.push(0x01);
        body.extend_from_slice(&encode_string(b"item"));
        // 1 hash
        body.push(RDB_TYPE_HASH);
        body.extend_from_slice(&encode_string(b"hash0"));
        body.push(0x01);
        body.extend_from_slice(&encode_string(b"f"));
        body.extend_from_slice(&encode_string(b"v"));
        body.push(RDB_OPCODE_EOF);

        let data = build_rdb(11, &body);
        let mut parser = RdbParser::from_bytes(&data).unwrap();
        let rdb = parser.parse().unwrap();

        assert_eq!(rdb.stats.total_keys, 4);
        assert_eq!(rdb.stats.strings_count, 2);
        assert_eq!(rdb.stats.lists_count, 1);
        assert_eq!(rdb.stats.hashes_count, 1);
        assert_eq!(rdb.stats.sets_count, 0);
        assert_eq!(rdb.stats.sorted_sets_count, 0);
        assert_eq!(rdb.stats.streams_count, 0);
    }
}
