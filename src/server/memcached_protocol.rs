//! Memcached protocol support
//!
//! Implements both text and binary Memcached protocols, mapping
//! commands to Ferrite's internal Store operations.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::{Bytes, BytesMut};

use crate::storage::{Store, Value};

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// Parsed Memcached command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemcachedCommand {
    /// `get <key>*`
    Get { keys: Vec<String> },
    /// `gets <key>*` (with CAS token)
    Gets { keys: Vec<String> },
    /// `set <key> <flags> <exptime> <bytes>\r\n<data>\r\n`
    Set {
        key: String,
        flags: u32,
        exptime: u32,
        bytes: usize,
        value: Vec<u8>,
    },
    /// `add <key> <flags> <exptime> <value>`
    Add {
        key: String,
        flags: u32,
        exptime: u32,
        value: Vec<u8>,
    },
    /// `replace <key> <flags> <exptime> <value>`
    Replace {
        key: String,
        flags: u32,
        exptime: u32,
        value: Vec<u8>,
    },
    /// `delete <key>`
    Delete { key: String },
    /// `incr <key> <delta>`
    Incr { key: String, delta: u64 },
    /// `decr <key> <delta>`
    Decr { key: String, delta: u64 },
    /// `stats`
    Stats,
    /// `version`
    Version,
    /// `quit`
    Quit,
    /// `flush_all [delay]`
    FlushAll { delay: Option<u32> },
}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

/// Memcached response types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemcachedResponse {
    /// VALUE response for get/gets
    Value {
        key: String,
        flags: u32,
        bytes: usize,
        cas_unique: Option<u64>,
        data: Vec<u8>,
    },
    /// STORED
    Stored,
    /// NOT_STORED
    NotStored,
    /// EXISTS (CAS conflict)
    Exists,
    /// NOT_FOUND
    NotFound,
    /// DELETED
    Deleted,
    /// ERROR
    Error(String),
    /// END (terminates value responses)
    End,
    /// VERSION
    Version(String),
    /// STAT name value
    Stat { name: String, value: String },
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Memcached protocol errors.
#[derive(Debug, thiserror::Error)]
pub enum MemcachedError {
    /// Unknown or malformed command.
    #[error("invalid command: {0}")]
    InvalidCommand(String),

    /// Wrong number or type of arguments.
    #[error("invalid arguments: {0}")]
    InvalidArguments(String),

    /// Value exceeds maximum allowed size.
    #[error("value too large: {size} bytes (max {max})")]
    ValueTooLarge { size: usize, max: usize },

    /// Client disconnected.
    #[error("connection closed")]
    ConnectionClosed,
}

// ---------------------------------------------------------------------------
// Protocol stats
// ---------------------------------------------------------------------------

/// Runtime statistics for the Memcached protocol adapter.
#[derive(Debug, Clone, Default)]
pub struct MemcachedStats {
    /// Total GET commands processed.
    pub get_commands: u64,
    /// Total SET commands processed.
    pub set_commands: u64,
    /// Total DELETE commands processed.
    pub delete_commands: u64,
    /// Cache hits.
    pub hits: u64,
    /// Cache misses.
    pub misses: u64,
    /// Current number of stored items (approximation).
    pub current_items: u64,
    /// Total bytes stored.
    pub bytes_stored: u64,
}

// ---------------------------------------------------------------------------
// Text protocol parser
// ---------------------------------------------------------------------------

/// Parser for the Memcached text protocol.
pub struct MemcachedTextParser;

impl MemcachedTextParser {
    /// Parse a Memcached text-protocol command from raw bytes.
    pub fn parse_command(input: &[u8]) -> Result<MemcachedCommand, MemcachedError> {
        let text = std::str::from_utf8(input)
            .map_err(|_| MemcachedError::InvalidCommand("non-UTF8 input".to_string()))?;

        let trimmed = text.trim_end_matches("\r\n").trim_end_matches('\n');
        let parts: Vec<&str> = trimmed.split_whitespace().collect();

        if parts.is_empty() {
            return Err(MemcachedError::InvalidCommand("empty command".to_string()));
        }

        match parts[0].to_lowercase().as_str() {
            "get" => {
                if parts.len() < 2 {
                    return Err(MemcachedError::InvalidArguments(
                        "get requires at least one key".to_string(),
                    ));
                }
                Ok(MemcachedCommand::Get {
                    keys: parts[1..].iter().map(|s| s.to_string()).collect(),
                })
            }
            "gets" => {
                if parts.len() < 2 {
                    return Err(MemcachedError::InvalidArguments(
                        "gets requires at least one key".to_string(),
                    ));
                }
                Ok(MemcachedCommand::Gets {
                    keys: parts[1..].iter().map(|s| s.to_string()).collect(),
                })
            }
            "set" => Self::parse_storage_command(&parts, "set"),
            "add" => {
                let cmd = Self::parse_storage_command(&parts, "add")?;
                if let MemcachedCommand::Set {
                    key,
                    flags,
                    exptime,
                    value,
                    ..
                } = cmd
                {
                    Ok(MemcachedCommand::Add {
                        key,
                        flags,
                        exptime,
                        value,
                    })
                } else {
                    Err(MemcachedError::InvalidCommand(
                        "unexpected parse result".to_string(),
                    ))
                }
            }
            "replace" => {
                let cmd = Self::parse_storage_command(&parts, "replace")?;
                if let MemcachedCommand::Set {
                    key,
                    flags,
                    exptime,
                    value,
                    ..
                } = cmd
                {
                    Ok(MemcachedCommand::Replace {
                        key,
                        flags,
                        exptime,
                        value,
                    })
                } else {
                    Err(MemcachedError::InvalidCommand(
                        "unexpected parse result".to_string(),
                    ))
                }
            }
            "delete" => {
                if parts.len() < 2 {
                    return Err(MemcachedError::InvalidArguments(
                        "delete requires a key".to_string(),
                    ));
                }
                Ok(MemcachedCommand::Delete {
                    key: parts[1].to_string(),
                })
            }
            "incr" => {
                if parts.len() < 3 {
                    return Err(MemcachedError::InvalidArguments(
                        "incr requires key and delta".to_string(),
                    ));
                }
                let delta = parts[2]
                    .parse::<u64>()
                    .map_err(|_| MemcachedError::InvalidArguments("invalid delta".to_string()))?;
                Ok(MemcachedCommand::Incr {
                    key: parts[1].to_string(),
                    delta,
                })
            }
            "decr" => {
                if parts.len() < 3 {
                    return Err(MemcachedError::InvalidArguments(
                        "decr requires key and delta".to_string(),
                    ));
                }
                let delta = parts[2]
                    .parse::<u64>()
                    .map_err(|_| MemcachedError::InvalidArguments("invalid delta".to_string()))?;
                Ok(MemcachedCommand::Decr {
                    key: parts[1].to_string(),
                    delta,
                })
            }
            "stats" => Ok(MemcachedCommand::Stats),
            "version" => Ok(MemcachedCommand::Version),
            "quit" => Ok(MemcachedCommand::Quit),
            "flush_all" => {
                let delay = if parts.len() > 1 {
                    parts[1].parse::<u32>().ok()
                } else {
                    None
                };
                Ok(MemcachedCommand::FlushAll { delay })
            }
            other => Err(MemcachedError::InvalidCommand(format!(
                "unknown command: {}",
                other
            ))),
        }
    }

    /// Parse a storage command (set/add/replace).
    fn parse_storage_command(
        parts: &[&str],
        cmd_name: &str,
    ) -> Result<MemcachedCommand, MemcachedError> {
        // <cmd> <key> <flags> <exptime> <bytes>
        if parts.len() < 5 {
            return Err(MemcachedError::InvalidArguments(format!(
                "{} requires: key flags exptime bytes",
                cmd_name
            )));
        }

        let key = parts[1].to_string();
        let flags = parts[2]
            .parse::<u32>()
            .map_err(|_| MemcachedError::InvalidArguments("invalid flags value".to_string()))?;
        let exptime = parts[3]
            .parse::<u32>()
            .map_err(|_| MemcachedError::InvalidArguments("invalid exptime value".to_string()))?;
        let bytes = parts[4]
            .parse::<usize>()
            .map_err(|_| MemcachedError::InvalidArguments("invalid bytes value".to_string()))?;

        Ok(MemcachedCommand::Set {
            key,
            flags,
            exptime,
            bytes,
            value: Vec::new(), // Populated by parse_command_with_data
        })
    }

    /// Parse a memcached text command that may include a data payload.
    ///
    /// In the memcached text protocol, storage commands (SET/ADD/REPLACE)
    /// have the value on the line following the header. This method accepts
    /// the full input including the data block.
    pub fn parse_command_with_data(input: &[u8]) -> Result<MemcachedCommand, MemcachedError> {
        // Find the first \r\n to split header from data
        let header_end = input
            .windows(2)
            .position(|w| w == b"\r\n")
            .unwrap_or(input.len());
        let header = &input[..header_end];

        // Parse header line
        let mut cmd = Self::parse_command(header)?;

        // For storage commands, read value data after the header
        let data_start = (header_end + 2).min(input.len());
        match &mut cmd {
            MemcachedCommand::Set {
                bytes: expected,
                value,
                ..
            } => {
                let take = (*expected).min(input.len().saturating_sub(data_start));
                if take > 0 {
                    *value = input[data_start..data_start + take].to_vec();
                }
            }
            MemcachedCommand::Add { value, .. } | MemcachedCommand::Replace { value, .. } => {
                let available = input.len().saturating_sub(data_start);
                if available > 0 {
                    // Trim trailing \r\n from value
                    let end = if input.ends_with(b"\r\n") {
                        input.len() - 2
                    } else {
                        input.len()
                    };
                    let take = end.saturating_sub(data_start);
                    if take > 0 {
                        *value = input[data_start..data_start + take].to_vec();
                    }
                }
            }
            _ => {}
        }

        Ok(cmd)
    }

    /// Encode a Memcached response to bytes for the text protocol.
    pub fn encode_response(response: &MemcachedResponse) -> Vec<u8> {
        let mut buf = BytesMut::new();
        match response {
            MemcachedResponse::Value {
                key,
                flags,
                bytes,
                cas_unique,
                data,
            } => {
                if let Some(cas) = cas_unique {
                    buf.extend_from_slice(
                        format!("VALUE {} {} {} {}\r\n", key, flags, bytes, cas).as_bytes(),
                    );
                } else {
                    buf.extend_from_slice(
                        format!("VALUE {} {} {}\r\n", key, flags, bytes).as_bytes(),
                    );
                }
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            MemcachedResponse::Stored => buf.extend_from_slice(b"STORED\r\n"),
            MemcachedResponse::NotStored => buf.extend_from_slice(b"NOT_STORED\r\n"),
            MemcachedResponse::Exists => buf.extend_from_slice(b"EXISTS\r\n"),
            MemcachedResponse::NotFound => buf.extend_from_slice(b"NOT_FOUND\r\n"),
            MemcachedResponse::Deleted => buf.extend_from_slice(b"DELETED\r\n"),
            MemcachedResponse::Error(msg) => {
                buf.extend_from_slice(format!("SERVER_ERROR {}\r\n", msg).as_bytes());
            }
            MemcachedResponse::End => buf.extend_from_slice(b"END\r\n"),
            MemcachedResponse::Version(ver) => {
                buf.extend_from_slice(format!("VERSION {}\r\n", ver).as_bytes());
            }
            MemcachedResponse::Stat { name, value } => {
                buf.extend_from_slice(format!("STAT {} {}\r\n", name, value).as_bytes());
            }
        }
        buf.to_vec()
    }
}

/// Convert Memcached protocol stats to a summary map.
pub fn stats_to_map(stats: &MemcachedStats) -> HashMap<String, String> {
    let mut map = HashMap::new();
    map.insert("get_commands".to_string(), stats.get_commands.to_string());
    map.insert("set_commands".to_string(), stats.set_commands.to_string());
    map.insert(
        "delete_commands".to_string(),
        stats.delete_commands.to_string(),
    );
    map.insert("hits".to_string(), stats.hits.to_string());
    map.insert("misses".to_string(), stats.misses.to_string());
    map.insert("current_items".to_string(), stats.current_items.to_string());
    map.insert("bytes_stored".to_string(), stats.bytes_stored.to_string());
    map
}

// ---------------------------------------------------------------------------
// Command handler — executes memcached commands against the Store
// ---------------------------------------------------------------------------

/// Handles memcached commands by executing them against a Ferrite Store.
pub struct MemcachedHandler {
    store: Arc<Store>,
}

impl MemcachedHandler {
    /// Create a new handler backed by the given store.
    pub fn new(store: Arc<Store>) -> Self {
        Self { store }
    }

    /// Execute a parsed memcached command and return the response(s).
    pub fn execute(&self, cmd: MemcachedCommand) -> Vec<MemcachedResponse> {
        match cmd {
            MemcachedCommand::Get { keys } | MemcachedCommand::Gets { keys } => {
                let mut responses = Vec::with_capacity(keys.len() + 1);
                for key in &keys {
                    let key_bytes = Bytes::from(key.clone());
                    if let Some(Value::String(data)) = self.store.get(0, &key_bytes) {
                        responses.push(MemcachedResponse::Value {
                            key: key.clone(),
                            flags: 0,
                            bytes: data.len(),
                            cas_unique: None,
                            data: data.to_vec(),
                        });
                    }
                }
                responses.push(MemcachedResponse::End);
                responses
            }
            MemcachedCommand::Set {
                key,
                exptime,
                value,
                ..
            } => {
                let key_bytes = Bytes::from(key);
                let val = Value::String(Bytes::from(value));
                if exptime > 0 {
                    let expires_at = SystemTime::now() + Duration::from_secs(u64::from(exptime));
                    self.store.set_with_expiry(0, key_bytes, val, expires_at);
                } else {
                    self.store.set(0, key_bytes, val);
                }
                vec![MemcachedResponse::Stored]
            }
            MemcachedCommand::Add {
                key,
                exptime,
                value,
                ..
            } => {
                let key_bytes = Bytes::from(key);
                if self.store.get(0, &key_bytes).is_some() {
                    return vec![MemcachedResponse::NotStored];
                }
                let val = Value::String(Bytes::from(value));
                if exptime > 0 {
                    let expires_at = SystemTime::now() + Duration::from_secs(u64::from(exptime));
                    self.store.set_with_expiry(0, key_bytes, val, expires_at);
                } else {
                    self.store.set(0, key_bytes, val);
                }
                vec![MemcachedResponse::Stored]
            }
            MemcachedCommand::Replace {
                key,
                exptime,
                value,
                ..
            } => {
                let key_bytes = Bytes::from(key);
                if self.store.get(0, &key_bytes).is_none() {
                    return vec![MemcachedResponse::NotStored];
                }
                let val = Value::String(Bytes::from(value));
                if exptime > 0 {
                    let expires_at = SystemTime::now() + Duration::from_secs(u64::from(exptime));
                    self.store.set_with_expiry(0, key_bytes, val, expires_at);
                } else {
                    self.store.set(0, key_bytes, val);
                }
                vec![MemcachedResponse::Stored]
            }
            MemcachedCommand::Delete { key } => {
                let key_bytes = Bytes::from(key);
                let deleted = self.store.del(0, &[key_bytes]);
                if deleted > 0 {
                    vec![MemcachedResponse::Deleted]
                } else {
                    vec![MemcachedResponse::NotFound]
                }
            }
            MemcachedCommand::Incr { key, delta } => {
                let key_bytes = Bytes::from(key);
                match self.store.get(0, &key_bytes) {
                    Some(Value::String(data)) => {
                        let current: u64 = std::str::from_utf8(&data)
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);
                        let new_val = current.saturating_add(delta);
                        self.store.set(
                            0,
                            key_bytes,
                            Value::String(Bytes::from(new_val.to_string())),
                        );
                        vec![MemcachedResponse::Value {
                            key: String::new(),
                            flags: 0,
                            bytes: 0,
                            cas_unique: None,
                            data: new_val.to_string().into_bytes(),
                        }]
                    }
                    _ => vec![MemcachedResponse::NotFound],
                }
            }
            MemcachedCommand::Decr { key, delta } => {
                let key_bytes = Bytes::from(key);
                match self.store.get(0, &key_bytes) {
                    Some(Value::String(data)) => {
                        let current: u64 = std::str::from_utf8(&data)
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);
                        let new_val = current.saturating_sub(delta);
                        self.store.set(
                            0,
                            key_bytes,
                            Value::String(Bytes::from(new_val.to_string())),
                        );
                        vec![MemcachedResponse::Value {
                            key: String::new(),
                            flags: 0,
                            bytes: 0,
                            cas_unique: None,
                            data: new_val.to_string().into_bytes(),
                        }]
                    }
                    _ => vec![MemcachedResponse::NotFound],
                }
            }
            MemcachedCommand::FlushAll { .. } => {
                self.store.flush_db(0);
                vec![MemcachedResponse::Stored]
            }
            MemcachedCommand::Version => {
                vec![MemcachedResponse::Version("ferrite 0.3.0".to_string())]
            }
            MemcachedCommand::Stats => {
                let key_count = self.store.keys(0).len();
                vec![
                    MemcachedResponse::Stat {
                        name: "curr_items".to_string(),
                        value: key_count.to_string(),
                    },
                    MemcachedResponse::Stat {
                        name: "version".to_string(),
                        value: "ferrite 0.3.0".to_string(),
                    },
                    MemcachedResponse::End,
                ]
            }
            MemcachedCommand::Quit => vec![],
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_get() {
        let cmd = MemcachedTextParser::parse_command(b"get key1 key2\r\n").expect("should parse");
        assert_eq!(
            cmd,
            MemcachedCommand::Get {
                keys: vec!["key1".to_string(), "key2".to_string()]
            }
        );
    }

    #[test]
    fn test_parse_set() {
        let cmd =
            MemcachedTextParser::parse_command(b"set mykey 0 3600 5\r\n").expect("should parse");
        match cmd {
            MemcachedCommand::Set {
                key,
                flags,
                exptime,
                bytes,
                ..
            } => {
                assert_eq!(key, "mykey");
                assert_eq!(flags, 0);
                assert_eq!(exptime, 3600);
                assert_eq!(bytes, 5);
            }
            _ => panic!("expected Set command"),
        }
    }

    #[test]
    fn test_parse_set_with_data() {
        let input = b"set mykey 0 3600 5\r\nhello\r\n";
        let cmd = MemcachedTextParser::parse_command_with_data(input).expect("should parse");
        match cmd {
            MemcachedCommand::Set {
                key, value, bytes, ..
            } => {
                assert_eq!(key, "mykey");
                assert_eq!(bytes, 5);
                assert_eq!(value, b"hello");
            }
            _ => panic!("expected Set command"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let cmd = MemcachedTextParser::parse_command(b"delete foo\r\n").expect("should parse");
        assert_eq!(
            cmd,
            MemcachedCommand::Delete {
                key: "foo".to_string()
            }
        );
    }

    #[test]
    fn test_parse_incr() {
        let cmd = MemcachedTextParser::parse_command(b"incr counter 10\r\n").expect("should parse");
        assert_eq!(
            cmd,
            MemcachedCommand::Incr {
                key: "counter".to_string(),
                delta: 10
            }
        );
    }

    #[test]
    fn test_parse_stats() {
        let cmd = MemcachedTextParser::parse_command(b"stats\r\n").expect("should parse");
        assert_eq!(cmd, MemcachedCommand::Stats);
    }

    #[test]
    fn test_parse_version() {
        let cmd = MemcachedTextParser::parse_command(b"version\r\n").expect("should parse");
        assert_eq!(cmd, MemcachedCommand::Version);
    }

    #[test]
    fn test_parse_quit() {
        let cmd = MemcachedTextParser::parse_command(b"quit\r\n").expect("should parse");
        assert_eq!(cmd, MemcachedCommand::Quit);
    }

    #[test]
    fn test_parse_flush_all() {
        let cmd = MemcachedTextParser::parse_command(b"flush_all 30\r\n").expect("should parse");
        assert_eq!(cmd, MemcachedCommand::FlushAll { delay: Some(30) });
    }

    #[test]
    fn test_parse_invalid_command() {
        let result = MemcachedTextParser::parse_command(b"foobar\r\n");
        assert!(result.is_err());
    }

    #[test]
    fn test_encode_stored() {
        let encoded = MemcachedTextParser::encode_response(&MemcachedResponse::Stored);
        assert_eq!(encoded, b"STORED\r\n");
    }

    #[test]
    fn test_encode_value() {
        let resp = MemcachedResponse::Value {
            key: "k".to_string(),
            flags: 0,
            bytes: 5,
            cas_unique: None,
            data: b"hello".to_vec(),
        };
        let encoded = MemcachedTextParser::encode_response(&resp);
        assert!(encoded.starts_with(b"VALUE k 0 5\r\n"));
        assert!(encoded.ends_with(b"\r\n"));
    }

    #[test]
    fn test_encode_version() {
        let encoded =
            MemcachedTextParser::encode_response(&MemcachedResponse::Version("1.0".to_string()));
        assert_eq!(encoded, b"VERSION 1.0\r\n");
    }

    #[test]
    fn test_encode_stat() {
        let resp = MemcachedResponse::Stat {
            name: "hits".to_string(),
            value: "42".to_string(),
        };
        let encoded = MemcachedTextParser::encode_response(&resp);
        assert_eq!(encoded, b"STAT hits 42\r\n");
    }

    #[test]
    fn test_encode_deleted() {
        let encoded = MemcachedTextParser::encode_response(&MemcachedResponse::Deleted);
        assert_eq!(encoded, b"DELETED\r\n");
    }

    #[test]
    fn test_encode_end() {
        let encoded = MemcachedTextParser::encode_response(&MemcachedResponse::End);
        assert_eq!(encoded, b"END\r\n");
    }

    #[test]
    fn test_stats_to_map() {
        let stats = MemcachedStats {
            get_commands: 100,
            set_commands: 50,
            ..Default::default()
        };
        let map = stats_to_map(&stats);
        assert_eq!(map.get("get_commands"), Some(&"100".to_string()));
    }

    // --- Handler tests ---

    #[test]
    fn test_handler_set_and_get() {
        let store = Arc::new(Store::new(16));
        let handler = MemcachedHandler::new(store);

        // SET
        let resp = handler.execute(MemcachedCommand::Set {
            key: "hello".to_string(),
            flags: 0,
            exptime: 0,
            bytes: 5,
            value: b"world".to_vec(),
        });
        assert_eq!(resp, vec![MemcachedResponse::Stored]);

        // GET
        let resp = handler.execute(MemcachedCommand::Get {
            keys: vec!["hello".to_string()],
        });
        assert_eq!(resp.len(), 2); // VALUE + END
        match &resp[0] {
            MemcachedResponse::Value { key, data, .. } => {
                assert_eq!(key, "hello");
                assert_eq!(data, b"world");
            }
            _ => panic!("expected Value response"),
        }
        assert_eq!(resp[1], MemcachedResponse::End);
    }

    #[test]
    fn test_handler_add_existing_key() {
        let store = Arc::new(Store::new(16));
        store.set(
            0,
            Bytes::from("existing"),
            Value::String(Bytes::from("val")),
        );
        let handler = MemcachedHandler::new(store);

        let resp = handler.execute(MemcachedCommand::Add {
            key: "existing".to_string(),
            flags: 0,
            exptime: 0,
            value: b"new".to_vec(),
        });
        assert_eq!(resp, vec![MemcachedResponse::NotStored]);
    }

    #[test]
    fn test_handler_replace_missing_key() {
        let store = Arc::new(Store::new(16));
        let handler = MemcachedHandler::new(store);

        let resp = handler.execute(MemcachedCommand::Replace {
            key: "missing".to_string(),
            flags: 0,
            exptime: 0,
            value: b"val".to_vec(),
        });
        assert_eq!(resp, vec![MemcachedResponse::NotStored]);
    }

    #[test]
    fn test_handler_delete() {
        let store = Arc::new(Store::new(16));
        store.set(0, Bytes::from("k"), Value::String(Bytes::from("v")));
        let handler = MemcachedHandler::new(store);

        let resp = handler.execute(MemcachedCommand::Delete {
            key: "k".to_string(),
        });
        assert_eq!(resp, vec![MemcachedResponse::Deleted]);

        let resp = handler.execute(MemcachedCommand::Delete {
            key: "k".to_string(),
        });
        assert_eq!(resp, vec![MemcachedResponse::NotFound]);
    }

    #[test]
    fn test_handler_incr_decr() {
        let store = Arc::new(Store::new(16));
        store.set(0, Bytes::from("counter"), Value::String(Bytes::from("10")));
        let handler = MemcachedHandler::new(store);

        let resp = handler.execute(MemcachedCommand::Incr {
            key: "counter".to_string(),
            delta: 5,
        });
        match &resp[0] {
            MemcachedResponse::Value { data, .. } => {
                assert_eq!(data, b"15");
            }
            _ => panic!("expected Value response"),
        }

        let resp = handler.execute(MemcachedCommand::Decr {
            key: "counter".to_string(),
            delta: 3,
        });
        match &resp[0] {
            MemcachedResponse::Value { data, .. } => {
                assert_eq!(data, b"12");
            }
            _ => panic!("expected Value response"),
        }
    }
}
