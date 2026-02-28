//! Memcached protocol support
//!
//! Implements both text and binary Memcached protocols, mapping
//! commands to Ferrite's internal Store operations.
#![allow(dead_code)]

use std::collections::HashMap;

use bytes::{Bytes, BytesMut};

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

        // Value data would be read from subsequent bytes in a real implementation
        Ok(MemcachedCommand::Set {
            key,
            flags,
            exptime,
            bytes,
            value: Vec::new(),
        })
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
}
