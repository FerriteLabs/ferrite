//! Storage backend abstraction
//!
//! This module provides a trait abstraction for different storage backends,
//! allowing the Store to use either in-memory DashMap or HybridLog storage.

use std::io;
use std::path::PathBuf;
use std::time::SystemTime;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::expiration::systemtime_to_epoch_ms;
use super::memory::Value;

/// Serializable entry for storage in HybridLog backend
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableEntry {
    /// The value type tag
    pub value_type: ValueType,
    /// Serialized value data
    pub data: Vec<u8>,
    /// Expiration time (milliseconds since UNIX epoch)
    pub expires_at_ms: Option<u64>,
}

/// Value type tags for serialization
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[repr(u8)]
pub enum ValueType {
    /// String value type
    String = 0,
    /// List value type
    List = 1,
    /// Hash value type
    Hash = 2,
    /// Set value type
    Set = 3,
    /// Sorted set value type
    SortedSet = 4,
    /// Stream value type
    Stream = 5,
    /// HyperLogLog value type
    HyperLogLog = 6,
}

impl SerializableEntry {
    /// Create a new serializable entry from a Value
    pub fn from_value(value: &Value, expires_at: Option<SystemTime>) -> io::Result<Self> {
        let (value_type, data) = serialize_value(value)?;
        let expires_at_ms = expires_at.and_then(systemtime_to_epoch_ms);

        Ok(Self {
            value_type,
            data,
            expires_at_ms,
        })
    }

    /// Convert back to Value and optional expiration
    pub fn to_value(&self) -> io::Result<(Value, Option<SystemTime>)> {
        let value = deserialize_value(self.value_type, &self.data)?;
        let expires_at = self
            .expires_at_ms
            .map(|exp_ms| std::time::UNIX_EPOCH + std::time::Duration::from_millis(exp_ms));

        Ok((value, expires_at))
    }

    /// Check if the entry has expired
    pub fn is_expired(&self) -> bool {
        if let Some(exp_ms) = self.expires_at_ms {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            now_ms >= exp_ms
        } else {
            false
        }
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> io::Result<Bytes> {
        bincode::serialize(self)
            .map(Bytes::from)
            .map_err(io::Error::other)
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> io::Result<Self> {
        bincode::deserialize(data).map_err(io::Error::other)
    }
}

/// Serialize a Value to bytes
fn serialize_value(value: &Value) -> io::Result<(ValueType, Vec<u8>)> {
    match value {
        Value::String(s) => Ok((ValueType::String, s.to_vec())),
        Value::List(list) => {
            let items: Vec<Vec<u8>> = list.iter().map(|b| b.to_vec()).collect();
            let data = bincode::serialize(&items).map_err(io::Error::other)?;
            Ok((ValueType::List, data))
        }
        Value::Hash(hash) => {
            let items: Vec<(Vec<u8>, Vec<u8>)> =
                hash.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
            let data = bincode::serialize(&items).map_err(io::Error::other)?;
            Ok((ValueType::Hash, data))
        }
        Value::Set(set) => {
            let items: Vec<Vec<u8>> = set.iter().map(|b| b.to_vec()).collect();
            let data = bincode::serialize(&items).map_err(io::Error::other)?;
            Ok((ValueType::Set, data))
        }
        Value::SortedSet { by_member, .. } => {
            let items: Vec<(Vec<u8>, f64)> =
                by_member.iter().map(|(k, v)| (k.to_vec(), *v)).collect();
            let data = bincode::serialize(&items).map_err(io::Error::other)?;
            Ok((ValueType::SortedSet, data))
        }
        Value::Stream(stream) => {
            // Serialize stream as JSON for simplicity (complex nested structure)
            let data = serde_json::to_vec(stream).map_err(io::Error::other)?;
            Ok((ValueType::Stream, data))
        }
        Value::HyperLogLog(hll) => Ok((ValueType::HyperLogLog, hll.clone())),
    }
}

/// Deserialize bytes to a Value
fn deserialize_value(value_type: ValueType, data: &[u8]) -> io::Result<Value> {
    match value_type {
        ValueType::String => Ok(Value::String(Bytes::copy_from_slice(data))),
        ValueType::List => {
            let items: Vec<Vec<u8>> = bincode::deserialize(data).map_err(io::Error::other)?;
            let list = items.into_iter().map(Bytes::from).collect();
            Ok(Value::List(list))
        }
        ValueType::Hash => {
            let items: Vec<(Vec<u8>, Vec<u8>)> =
                bincode::deserialize(data).map_err(io::Error::other)?;
            let hash = items
                .into_iter()
                .map(|(k, v)| (Bytes::from(k), Bytes::from(v)))
                .collect();
            Ok(Value::Hash(hash))
        }
        ValueType::Set => {
            let items: Vec<Vec<u8>> = bincode::deserialize(data).map_err(io::Error::other)?;
            let set = items.into_iter().map(Bytes::from).collect();
            Ok(Value::Set(set))
        }
        ValueType::SortedSet => {
            let items: Vec<(Vec<u8>, f64)> =
                bincode::deserialize(data).map_err(io::Error::other)?;
            use ordered_float::OrderedFloat;
            use std::collections::{BTreeMap, HashMap};
            let mut by_score = BTreeMap::new();
            let mut by_member = HashMap::new();
            for (k, score) in items {
                let key = Bytes::from(k);
                by_score.insert((OrderedFloat(score), key.clone()), ());
                by_member.insert(key, score);
            }
            Ok(Value::SortedSet {
                by_score,
                by_member,
            })
        }
        ValueType::Stream => {
            let stream = serde_json::from_slice(data).map_err(io::Error::other)?;
            Ok(Value::Stream(stream))
        }
        ValueType::HyperLogLog => Ok(Value::HyperLogLog(data.to_vec())),
    }
}

/// Configuration for HybridLog backend
#[derive(Debug, Clone)]
pub struct HybridLogBackendConfig {
    /// Size of the mutable region in bytes
    pub mutable_size: usize,
    /// Size of the read-only region in bytes
    pub readonly_size: usize,
    /// Directory for persistent storage
    pub data_dir: PathBuf,
    /// Whether to enable automatic tiering
    pub auto_tiering: bool,
    /// Migration threshold (0.0-1.0)
    pub migration_threshold: f64,
}

impl Default for HybridLogBackendConfig {
    fn default() -> Self {
        Self {
            mutable_size: 64 * 1024 * 1024,   // 64MB
            readonly_size: 256 * 1024 * 1024, // 256MB
            data_dir: PathBuf::from("./data"),
            auto_tiering: true,
            migration_threshold: 0.8,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet, VecDeque};

    #[test]
    fn test_serialize_string() {
        let value = Value::String(Bytes::from("hello"));
        let entry = SerializableEntry::from_value(&value, None).unwrap();
        let (decoded, _) = entry.to_value().unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_serialize_list() {
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("a"));
        list.push_back(Bytes::from("b"));
        let value = Value::List(list);
        let entry = SerializableEntry::from_value(&value, None).unwrap();
        let (decoded, _) = entry.to_value().unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_serialize_hash() {
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("key1"), Bytes::from("value1"));
        hash.insert(Bytes::from("key2"), Bytes::from("value2"));
        let value = Value::Hash(hash);
        let entry = SerializableEntry::from_value(&value, None).unwrap();
        let (decoded, _) = entry.to_value().unwrap();
        if let (Value::Hash(orig), Value::Hash(dec)) = (&value, &decoded) {
            assert_eq!(orig.len(), dec.len());
            for (k, v) in orig {
                assert_eq!(dec.get(k), Some(v));
            }
        } else {
            panic!("Expected Hash values");
        }
    }

    #[test]
    fn test_serialize_set() {
        let mut set = HashSet::new();
        set.insert(Bytes::from("a"));
        set.insert(Bytes::from("b"));
        let value = Value::Set(set);
        let entry = SerializableEntry::from_value(&value, None).unwrap();
        let (decoded, _) = entry.to_value().unwrap();
        if let (Value::Set(orig), Value::Set(dec)) = (&value, &decoded) {
            assert_eq!(orig.len(), dec.len());
            for item in orig {
                assert!(dec.contains(item));
            }
        } else {
            panic!("Expected Set values");
        }
    }

    #[test]
    fn test_serialize_sorted_set() {
        use ordered_float::OrderedFloat;
        use std::collections::BTreeMap;

        let mut by_score = BTreeMap::new();
        let mut by_member = HashMap::new();
        by_score.insert((OrderedFloat(1.0), Bytes::from("a")), ());
        by_score.insert((OrderedFloat(2.0), Bytes::from("b")), ());
        by_member.insert(Bytes::from("a"), 1.0);
        by_member.insert(Bytes::from("b"), 2.0);
        let value = Value::SortedSet {
            by_score,
            by_member,
        };

        let entry = SerializableEntry::from_value(&value, None).unwrap();
        let (decoded, _) = entry.to_value().unwrap();

        if let (
            Value::SortedSet {
                by_member: orig, ..
            },
            Value::SortedSet { by_member: dec, .. },
        ) = (&value, &decoded)
        {
            assert_eq!(orig.len(), dec.len());
            for (k, v) in orig {
                assert_eq!(dec.get(k), Some(v));
            }
        } else {
            panic!("Expected SortedSet values");
        }
    }

    #[test]
    fn test_serialize_hyperloglog() {
        let value = Value::HyperLogLog(vec![1, 2, 3, 4, 5]);
        let entry = SerializableEntry::from_value(&value, None).unwrap();
        let (decoded, _) = entry.to_value().unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_entry_expiration() {
        let value = Value::String(Bytes::from("test"));
        let expires_at = SystemTime::now() - std::time::Duration::from_secs(1);
        let entry = SerializableEntry::from_value(&value, Some(expires_at)).unwrap();
        assert!(entry.is_expired());

        let future = SystemTime::now() + std::time::Duration::from_secs(60);
        let entry2 = SerializableEntry::from_value(&value, Some(future)).unwrap();
        assert!(!entry2.is_expired());
    }

    #[test]
    fn test_entry_to_bytes_roundtrip() {
        let value = Value::String(Bytes::from("hello"));
        let entry = SerializableEntry::from_value(&value, None).unwrap();
        let bytes = entry.to_bytes().unwrap();
        let decoded = SerializableEntry::from_bytes(&bytes).unwrap();
        let (decoded_value, _) = decoded.to_value().unwrap();
        assert_eq!(decoded_value, value);
    }
}
