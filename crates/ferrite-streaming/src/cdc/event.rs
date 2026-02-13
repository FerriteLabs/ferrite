//! CDC event types
//!
//! Defines the core event types for change data capture.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// A single change event
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangeEvent {
    /// Unique event ID (monotonically increasing)
    pub id: u64,
    /// Event timestamp
    #[serde(with = "system_time_serde")]
    pub timestamp: SystemTime,
    /// Database number
    pub db: u8,
    /// Operation type
    pub operation: Operation,
    /// Affected key
    #[serde(with = "bytes_serde")]
    pub key: Bytes,
    /// New value (for SET-like operations)
    #[serde(with = "option_bytes_serde")]
    pub value: Option<Bytes>,
    /// Old value (if capture_old_value enabled)
    #[serde(with = "option_bytes_serde")]
    pub old_value: Option<Bytes>,
    /// Additional metadata
    pub metadata: ChangeMetadata,
}

impl ChangeEvent {
    /// Create a new change event
    pub fn new(operation: Operation, key: Bytes, db: u8) -> Self {
        Self {
            id: 0,
            timestamp: SystemTime::now(),
            db,
            operation,
            key,
            value: None,
            old_value: None,
            metadata: ChangeMetadata::default(),
        }
    }

    /// Set the value
    pub fn with_value(mut self, value: Bytes) -> Self {
        self.value = Some(value);
        self
    }

    /// Set the old value
    pub fn with_old_value(mut self, old_value: Bytes) -> Self {
        self.old_value = Some(old_value);
        self
    }

    /// Set metadata
    pub fn with_metadata(mut self, metadata: ChangeMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Get the timestamp as milliseconds since epoch
    pub fn timestamp_ms(&self) -> u64 {
        self.timestamp
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Serialize to JSON
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    /// Serialize to JSON with pretty formatting
    pub fn to_json_pretty(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_default()
    }
}

impl Default for ChangeEvent {
    fn default() -> Self {
        Self {
            id: 0,
            timestamp: SystemTime::now(),
            db: 0,
            operation: Operation::Set,
            key: Bytes::new(),
            value: None,
            old_value: None,
            metadata: ChangeMetadata::default(),
        }
    }
}

/// Operation types that can be captured
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Operation {
    // String operations
    /// SET command
    Set,
    /// SETEX/PSETEX command
    SetEx {
        /// TTL in milliseconds
        ttl_ms: u64,
    },
    /// APPEND command
    Append,
    /// INCR/INCRBY command
    Incr {
        /// Increment delta
        delta: i64,
    },
    /// INCRBYFLOAT command
    IncrByFloat {
        /// Increment delta
        #[serde(with = "ordered_float_serde")]
        delta: f64,
    },

    // Key operations
    /// DEL command
    Del,
    /// EXPIRE/PEXPIRE command
    Expire {
        /// TTL in milliseconds
        ttl_ms: u64,
    },
    /// RENAME command
    Rename {
        /// New key name
        #[serde(with = "bytes_serde")]
        new_key: Bytes,
    },
    /// PERSIST command
    Persist,

    // Hash operations
    /// HSET command
    HSet {
        /// Field name
        #[serde(with = "bytes_serde")]
        field: Bytes,
    },
    /// HDEL command
    HDel {
        /// Field name
        #[serde(with = "bytes_serde")]
        field: Bytes,
    },
    /// HINCRBY command
    HIncrBy {
        /// Field name
        #[serde(with = "bytes_serde")]
        field: Bytes,
        /// Increment delta
        delta: i64,
    },

    // List operations
    /// LPUSH command
    LPush {
        /// Values pushed
        count: usize,
    },
    /// RPUSH command
    RPush {
        /// Values pushed
        count: usize,
    },
    /// LPOP command
    LPop,
    /// RPOP command
    RPop,
    /// LSET command
    LSet {
        /// Index
        index: i64,
    },
    /// LTRIM command
    LTrim {
        /// Start index
        start: i64,
        /// Stop index
        stop: i64,
    },

    // Set operations
    /// SADD command
    SAdd {
        /// Number of members added
        count: usize,
    },
    /// SREM command
    SRem {
        /// Number of members removed
        count: usize,
    },

    // Sorted set operations
    /// ZADD command
    ZAdd {
        /// Number of members added/updated
        count: usize,
    },
    /// ZREM command
    ZRem {
        /// Number of members removed
        count: usize,
    },
    /// ZINCRBY command
    ZIncrBy {
        /// Member name
        #[serde(with = "bytes_serde")]
        member: Bytes,
        /// Increment delta
        #[serde(with = "ordered_float_serde")]
        delta: f64,
    },

    // Stream operations
    /// XADD command
    XAdd {
        /// Stream entry ID
        #[serde(with = "bytes_serde")]
        id: Bytes,
    },
    /// XDEL command
    XDel {
        /// Number of entries deleted
        count: usize,
    },
    /// XTRIM command
    XTrim {
        /// Trim strategy
        strategy: String,
        /// Threshold
        threshold: i64,
    },

    // Other
    /// FLUSHDB command
    FlushDb,
    /// FLUSHALL command
    FlushAll,
    /// Key expiration (internal)
    Expired,
    /// Key eviction (internal)
    Evicted,
}

impl Operation {
    /// Get operation name as string
    pub fn name(&self) -> &'static str {
        match self {
            Self::Set => "SET",
            Self::SetEx { .. } => "SETEX",
            Self::Append => "APPEND",
            Self::Incr { .. } => "INCR",
            Self::IncrByFloat { .. } => "INCRBYFLOAT",
            Self::Del => "DEL",
            Self::Expire { .. } => "EXPIRE",
            Self::Rename { .. } => "RENAME",
            Self::Persist => "PERSIST",
            Self::HSet { .. } => "HSET",
            Self::HDel { .. } => "HDEL",
            Self::HIncrBy { .. } => "HINCRBY",
            Self::LPush { .. } => "LPUSH",
            Self::RPush { .. } => "RPUSH",
            Self::LPop => "LPOP",
            Self::RPop => "RPOP",
            Self::LSet { .. } => "LSET",
            Self::LTrim { .. } => "LTRIM",
            Self::SAdd { .. } => "SADD",
            Self::SRem { .. } => "SREM",
            Self::ZAdd { .. } => "ZADD",
            Self::ZRem { .. } => "ZREM",
            Self::ZIncrBy { .. } => "ZINCRBY",
            Self::XAdd { .. } => "XADD",
            Self::XDel { .. } => "XDEL",
            Self::XTrim { .. } => "XTRIM",
            Self::FlushDb => "FLUSHDB",
            Self::FlushAll => "FLUSHALL",
            Self::Expired => "EXPIRED",
            Self::Evicted => "EVICTED",
        }
    }

    /// Check if this is a string operation
    pub fn is_string_op(&self) -> bool {
        matches!(
            self,
            Self::Set
                | Self::SetEx { .. }
                | Self::Append
                | Self::Incr { .. }
                | Self::IncrByFloat { .. }
        )
    }

    /// Check if this is a key operation
    pub fn is_key_op(&self) -> bool {
        matches!(
            self,
            Self::Del | Self::Expire { .. } | Self::Rename { .. } | Self::Persist
        )
    }

    /// Check if this is a hash operation
    pub fn is_hash_op(&self) -> bool {
        matches!(
            self,
            Self::HSet { .. } | Self::HDel { .. } | Self::HIncrBy { .. }
        )
    }

    /// Check if this is a list operation
    pub fn is_list_op(&self) -> bool {
        matches!(
            self,
            Self::LPush { .. }
                | Self::RPush { .. }
                | Self::LPop
                | Self::RPop
                | Self::LSet { .. }
                | Self::LTrim { .. }
        )
    }

    /// Check if this is a set operation
    pub fn is_set_op(&self) -> bool {
        matches!(self, Self::SAdd { .. } | Self::SRem { .. })
    }

    /// Check if this is a sorted set operation
    pub fn is_zset_op(&self) -> bool {
        matches!(
            self,
            Self::ZAdd { .. } | Self::ZRem { .. } | Self::ZIncrBy { .. }
        )
    }

    /// Check if this is a stream operation
    pub fn is_stream_op(&self) -> bool {
        matches!(
            self,
            Self::XAdd { .. } | Self::XDel { .. } | Self::XTrim { .. }
        )
    }

    /// Check if this is an internal operation
    pub fn is_internal(&self) -> bool {
        matches!(self, Self::Expired | Self::Evicted)
    }

    /// Parse operation from string
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "SET" => Some(Self::Set),
            "SETEX" | "PSETEX" => Some(Self::SetEx { ttl_ms: 0 }),
            "APPEND" => Some(Self::Append),
            "INCR" | "INCRBY" | "DECR" | "DECRBY" => Some(Self::Incr { delta: 0 }),
            "INCRBYFLOAT" => Some(Self::IncrByFloat { delta: 0.0 }),
            "DEL" => Some(Self::Del),
            "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" => Some(Self::Expire { ttl_ms: 0 }),
            "RENAME" => Some(Self::Rename {
                new_key: Bytes::new(),
            }),
            "PERSIST" => Some(Self::Persist),
            "HSET" | "HSETNX" | "HMSET" => Some(Self::HSet {
                field: Bytes::new(),
            }),
            "HDEL" => Some(Self::HDel {
                field: Bytes::new(),
            }),
            "HINCRBY" => Some(Self::HIncrBy {
                field: Bytes::new(),
                delta: 0,
            }),
            "LPUSH" | "LPUSHX" => Some(Self::LPush { count: 0 }),
            "RPUSH" | "RPUSHX" => Some(Self::RPush { count: 0 }),
            "LPOP" => Some(Self::LPop),
            "RPOP" => Some(Self::RPop),
            "LSET" => Some(Self::LSet { index: 0 }),
            "LTRIM" => Some(Self::LTrim { start: 0, stop: 0 }),
            "SADD" => Some(Self::SAdd { count: 0 }),
            "SREM" => Some(Self::SRem { count: 0 }),
            "ZADD" => Some(Self::ZAdd { count: 0 }),
            "ZREM" => Some(Self::ZRem { count: 0 }),
            "ZINCRBY" => Some(Self::ZIncrBy {
                member: Bytes::new(),
                delta: 0.0,
            }),
            "XADD" => Some(Self::XAdd { id: Bytes::new() }),
            "XDEL" => Some(Self::XDel { count: 0 }),
            "XTRIM" => Some(Self::XTrim {
                strategy: String::new(),
                threshold: 0,
            }),
            "FLUSHDB" => Some(Self::FlushDb),
            "FLUSHALL" => Some(Self::FlushAll),
            "EXPIRED" => Some(Self::Expired),
            "EVICTED" => Some(Self::Evicted),
            _ => None,
        }
    }
}

/// Additional metadata for change events
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ChangeMetadata {
    /// Client that made the change (if available)
    pub client_id: Option<String>,
    /// Client IP address
    pub client_addr: Option<String>,
    /// Original command
    pub command: String,
    /// Transaction ID (if in MULTI/EXEC)
    pub transaction_id: Option<u64>,
    /// Replication source
    pub source: ChangeSource,
}

impl ChangeMetadata {
    /// Create metadata for a local operation
    pub fn local(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            source: ChangeSource::Local,
            ..Default::default()
        }
    }

    /// Create metadata with client info
    pub fn with_client(mut self, client_id: String, client_addr: String) -> Self {
        self.client_id = Some(client_id);
        self.client_addr = Some(client_addr);
        self
    }

    /// Create metadata with transaction ID
    pub fn with_transaction(mut self, tx_id: u64) -> Self {
        self.transaction_id = Some(tx_id);
        self
    }
}

/// Source of a change event
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum ChangeSource {
    /// Local client operation
    #[default]
    Local,
    /// Replicated from primary
    Replication {
        /// Source server ID
        source_id: String,
    },
    /// Restored from backup
    Restore,
    /// Internal operation (expiry, eviction)
    Internal,
}

impl ChangeSource {
    /// Check if this is a local operation
    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local)
    }

    /// Check if this is a replicated operation
    pub fn is_replication(&self) -> bool {
        matches!(self, Self::Replication { .. })
    }

    /// Get source name
    pub fn name(&self) -> &str {
        match self {
            Self::Local => "local",
            Self::Replication { .. } => "replication",
            Self::Restore => "restore",
            Self::Internal => "internal",
        }
    }
}

// Serde helpers for SystemTime
mod system_time_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    pub fn serialize<S>(time: &SystemTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let millis = time
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;
        millis.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<SystemTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = u64::deserialize(deserializer)?;
        Ok(UNIX_EPOCH + Duration::from_millis(millis))
    }
}

// Serde helpers for Bytes
mod bytes_serde {
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(bytes: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Try to serialize as UTF-8 string, fallback to base64
        if let Ok(s) = std::str::from_utf8(bytes) {
            s.serialize(serializer)
        } else {
            use base64::Engine;
            let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
            format!("base64:{}", encoded).serialize(serializer)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if let Some(encoded) = s.strip_prefix("base64:") {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map(Bytes::from)
                .map_err(serde::de::Error::custom)
        } else {
            Ok(Bytes::from(s))
        }
    }
}

// Serde helpers for Option<Bytes>
mod option_bytes_serde {
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(bytes: &Option<Bytes>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match bytes {
            Some(b) => {
                if let Ok(s) = std::str::from_utf8(b) {
                    Some(s).serialize(serializer)
                } else {
                    use base64::Engine;
                    let encoded = base64::engine::general_purpose::STANDARD.encode(b);
                    Some(format!("base64:{}", encoded)).serialize(serializer)
                }
            }
            None => None::<String>.serialize(serializer),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Bytes>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt = Option::<String>::deserialize(deserializer)?;
        match opt {
            Some(s) => {
                if let Some(encoded) = s.strip_prefix("base64:") {
                    use base64::Engine;
                    base64::engine::general_purpose::STANDARD
                        .decode(encoded)
                        .map(|v| Some(Bytes::from(v)))
                        .map_err(serde::de::Error::custom)
                } else {
                    Ok(Some(Bytes::from(s)))
                }
            }
            None => Ok(None),
        }
    }
}

// Serde helpers for f64 to ensure consistent ordering
mod ordered_float_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<f64, D::Error>
    where
        D: Deserializer<'de>,
    {
        f64::deserialize(deserializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_name() {
        assert_eq!(Operation::Set.name(), "SET");
        assert_eq!(Operation::Del.name(), "DEL");
        assert_eq!(Operation::SetEx { ttl_ms: 1000 }.name(), "SETEX");
    }

    #[test]
    fn test_operation_from_str() {
        assert!(matches!(Operation::from_str("SET"), Some(Operation::Set)));
        assert!(matches!(Operation::from_str("del"), Some(Operation::Del)));
        assert!(matches!(
            Operation::from_str("ZADD"),
            Some(Operation::ZAdd { .. })
        ));
        assert!(Operation::from_str("INVALID").is_none());
    }

    #[test]
    fn test_operation_categories() {
        assert!(Operation::Set.is_string_op());
        assert!(Operation::Del.is_key_op());
        assert!(Operation::HSet {
            field: Bytes::new()
        }
        .is_hash_op());
        assert!(Operation::LPush { count: 1 }.is_list_op());
        assert!(Operation::SAdd { count: 1 }.is_set_op());
        assert!(Operation::ZAdd { count: 1 }.is_zset_op());
        assert!(Operation::XAdd { id: Bytes::new() }.is_stream_op());
        assert!(Operation::Expired.is_internal());
    }

    #[test]
    fn test_change_event_builder() {
        let event = ChangeEvent::new(Operation::Set, Bytes::from_static(b"key"), 0)
            .with_value(Bytes::from_static(b"value"))
            .with_old_value(Bytes::from_static(b"old"))
            .with_metadata(ChangeMetadata::local("SET key value"));

        assert_eq!(event.key, Bytes::from_static(b"key"));
        assert_eq!(event.value, Some(Bytes::from_static(b"value")));
        assert_eq!(event.old_value, Some(Bytes::from_static(b"old")));
        assert_eq!(event.metadata.command, "SET key value");
    }

    #[test]
    fn test_change_source() {
        assert!(ChangeSource::Local.is_local());
        assert!(ChangeSource::Replication {
            source_id: "node1".to_string()
        }
        .is_replication());
        assert_eq!(ChangeSource::Local.name(), "local");
        assert_eq!(ChangeSource::Internal.name(), "internal");
    }

    #[test]
    fn test_json_serialization() {
        let event = ChangeEvent::new(Operation::Set, Bytes::from_static(b"test:key"), 0)
            .with_value(Bytes::from_static(b"hello"));

        let json = event.to_json();
        assert!(json.contains("\"key\":\"test:key\""));
        assert!(json.contains("\"operation\":\"Set\""));
    }
}
