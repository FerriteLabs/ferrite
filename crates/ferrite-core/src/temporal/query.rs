//! Temporal Query Engine
//!
//! Resolves point-in-time and history queries using the temporal index.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;

use super::index::{OperationType, TemporalIndex};
use super::TimestampSpec;
use crate::storage::Store;
use crate::storage::Value;

/// Query engine for temporal queries
pub struct QueryEngine {
    /// Reference to the temporal index
    index: Arc<TemporalIndex>,
}

impl QueryEngine {
    /// Create a new query engine
    pub fn new(index: Arc<TemporalIndex>) -> Self {
        Self { index }
    }

    /// Get value at a specific point in time
    pub async fn get_at(
        &self,
        key: &Bytes,
        timestamp: SystemTime,
        store: &Store,
        db: u8,
    ) -> Result<Option<Value>, QueryError> {
        let version = self.index.version_at(key, timestamp);

        match version {
            Some(v) if v.op.is_delete() => Ok(None),
            Some(v) => {
                // For inline values, return directly
                if let super::index::LogOffset::Inline(data) = &v.log_offset {
                    return Ok(Some(Value::String(data.clone())));
                }

                // Otherwise read from current store (simplified - full impl would read from log)
                // In a full implementation, we'd read from the HybridLog at the stored offset
                let current = store.get(db, key);
                Ok(current)
            }
            None => Ok(None),
        }
    }

    /// Get full history of a key
    pub async fn history(
        &self,
        key: &Bytes,
        query: HistoryQuery,
        store: &Store,
    ) -> Result<Vec<HistoryEntry>, QueryError> {
        let from = query.from.and_then(|s| s.to_system_time());
        let to = query.to.and_then(|s| s.to_system_time());

        let versions = self.index.versions_in_range(
            key,
            from,
            to,
            query.limit.unwrap_or(100),
            query.ascending,
        );

        let mut entries = Vec::with_capacity(versions.len());

        for v in versions {
            let value = if query.with_values {
                // In full implementation, read from log at v.log_offset
                if let super::index::LogOffset::Inline(data) = &v.log_offset {
                    Some(Value::String(data.clone()))
                } else {
                    store.get(v.db, key)
                }
            } else {
                None
            };

            entries.push(HistoryEntry {
                timestamp: v.timestamp,
                timestamp_ms: v.timestamp_ms(),
                operation: v.op.clone(),
                value,
                ttl_ms: v.ttl.map(|d| d.as_millis() as u64),
            });
        }

        Ok(entries)
    }

    /// Get version count for a key
    pub fn history_count(
        &self,
        key: &Bytes,
        from: Option<TimestampSpec>,
        to: Option<TimestampSpec>,
    ) -> u64 {
        let from_time = from.and_then(|s| s.to_system_time());
        let to_time = to.and_then(|s| s.to_system_time());

        self.index
            .versions_in_range(key, from_time, to_time, usize::MAX, false)
            .len() as u64
    }

    /// Get first (oldest) version
    pub fn history_first(&self, key: &Bytes) -> Option<HistoryEntry> {
        self.index.first_version(key).map(|v| HistoryEntry {
            timestamp: v.timestamp,
            timestamp_ms: v.timestamp_ms(),
            operation: v.op.clone(),
            value: None,
            ttl_ms: v.ttl.map(|d| d.as_millis() as u64),
        })
    }

    /// Get last (newest) version
    pub fn history_last(&self, key: &Bytes) -> Option<HistoryEntry> {
        self.index.last_version(key).map(|v| HistoryEntry {
            timestamp: v.timestamp,
            timestamp_ms: v.timestamp_ms(),
            operation: v.op.clone(),
            value: None,
            ttl_ms: v.ttl.map(|d| d.as_millis() as u64),
        })
    }

    /// Compare values at two points in time
    pub async fn diff(
        &self,
        key: &Bytes,
        ts1: TimestampSpec,
        ts2: TimestampSpec,
        store: &Store,
        db: u8,
    ) -> Result<DiffResult, QueryError> {
        let time1 = ts1.to_system_time().ok_or(QueryError::InvalidTimestamp)?;
        let time2 = ts2.to_system_time().ok_or(QueryError::InvalidTimestamp)?;

        let val1 = self.get_at(key, time1, store, db).await?;
        let val2 = self.get_at(key, time2, store, db).await?;

        let changes = compute_diff(&val1, &val2);

        Ok(DiffResult {
            key: key.clone(),
            timestamp1: time1,
            timestamp2: time2,
            value1: val1,
            value2: val2,
            changes,
        })
    }

    /// Restore a key to a previous state
    pub async fn restore_from(
        &self,
        key: &Bytes,
        timestamp: TimestampSpec,
        store: &Store,
        db: u8,
        target_key: Option<&Bytes>,
    ) -> Result<bool, QueryError> {
        let time = timestamp
            .to_system_time()
            .ok_or(QueryError::InvalidTimestamp)?;

        let value = self.get_at(key, time, store, db).await?;

        let dest_key = target_key.unwrap_or(key);

        match value {
            Some(v) => {
                store.set(db, dest_key.clone(), v);
                Ok(true)
            }
            None => {
                // Value didn't exist at that time
                if target_key.is_none() {
                    store.del(db, &[key.clone()]);
                }
                Ok(false)
            }
        }
    }
}

/// Parameters for a history query
#[derive(Clone, Debug, Default)]
pub struct HistoryQuery {
    /// Start timestamp (inclusive)
    pub from: Option<TimestampSpec>,
    /// End timestamp (inclusive)
    pub to: Option<TimestampSpec>,
    /// Maximum entries to return
    pub limit: Option<usize>,
    /// Sort order (true = oldest first)
    pub ascending: bool,
    /// Include values in results
    pub with_values: bool,
}

impl HistoryQuery {
    /// Create a new history query
    pub fn new() -> Self {
        Self::default()
    }

    /// Set from timestamp
    pub fn from(mut self, ts: TimestampSpec) -> Self {
        self.from = Some(ts);
        self
    }

    /// Set to timestamp
    pub fn to(mut self, ts: TimestampSpec) -> Self {
        self.to = Some(ts);
        self
    }

    /// Set limit
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set ascending order
    pub fn ascending(mut self) -> Self {
        self.ascending = true;
        self
    }

    /// Include values
    pub fn with_values(mut self) -> Self {
        self.with_values = true;
        self
    }
}

/// A temporal query specification
#[derive(Clone, Debug)]
pub enum TemporalQuery {
    /// Point-in-time query
    PointInTime {
        /// Key to query
        key: Bytes,
        /// Timestamp
        timestamp: TimestampSpec,
    },
    /// History query
    History {
        /// Key to query
        key: Bytes,
        /// Query parameters
        params: HistoryQuery,
    },
    /// Diff query
    Diff {
        /// Key to compare
        key: Bytes,
        /// First timestamp
        timestamp1: TimestampSpec,
        /// Second timestamp
        timestamp2: TimestampSpec,
    },
    /// Restore query
    Restore {
        /// Source key
        key: Bytes,
        /// Timestamp to restore from
        timestamp: TimestampSpec,
        /// Target key (None = restore in place)
        target: Option<Bytes>,
    },
}

/// Entry in history results
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoryEntry {
    /// Timestamp of this version
    #[serde(skip, default = "default_system_time")]
    pub timestamp: SystemTime,
    /// Timestamp as milliseconds since epoch
    pub timestamp_ms: u64,
    /// Operation type
    pub operation: OperationType,
    /// Value at this version (if requested)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    /// TTL at this version (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
}

fn default_system_time() -> SystemTime {
    SystemTime::UNIX_EPOCH
}

/// Result of a diff operation
#[derive(Clone, Debug)]
pub struct DiffResult {
    /// Key that was compared
    pub key: Bytes,
    /// First timestamp
    pub timestamp1: SystemTime,
    /// Second timestamp
    pub timestamp2: SystemTime,
    /// Value at first timestamp
    pub value1: Option<Value>,
    /// Value at second timestamp
    pub value2: Option<Value>,
    /// List of changes
    pub changes: Vec<DiffEntry>,
}

impl DiffResult {
    /// Check if there are any changes
    pub fn has_changes(&self) -> bool {
        !self.changes.is_empty()
    }

    /// Check if key was created
    pub fn was_created(&self) -> bool {
        self.value1.is_none() && self.value2.is_some()
    }

    /// Check if key was deleted
    pub fn was_deleted(&self) -> bool {
        self.value1.is_some() && self.value2.is_none()
    }
}

/// A single change in a diff
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DiffEntry {
    /// Value was created
    Created {
        /// The new value
        value: String,
    },
    /// Value was deleted
    Deleted {
        /// The old value
        value: String,
    },
    /// Value was modified
    Modified {
        /// Old value
        old: String,
        /// New value
        new: String,
    },
    /// Hash field added
    HashFieldAdded {
        /// Field name
        field: String,
        /// Value
        value: String,
    },
    /// Hash field removed
    HashFieldRemoved {
        /// Field name
        field: String,
        /// Old value
        value: String,
    },
    /// Hash field modified
    HashFieldModified {
        /// Field name
        field: String,
        /// Old value
        old: String,
        /// New value
        new: String,
    },
    /// Set member added
    SetMemberAdded {
        /// Member
        member: String,
    },
    /// Set member removed
    SetMemberRemoved {
        /// Member
        member: String,
    },
    /// List changed
    ListChanged {
        /// Description of change
        description: String,
    },
}

/// Compute diff between two values
fn compute_diff(val1: &Option<Value>, val2: &Option<Value>) -> Vec<DiffEntry> {
    match (val1, val2) {
        (None, None) => vec![],
        (None, Some(v)) => vec![DiffEntry::Created {
            value: format_value(v),
        }],
        (Some(v), None) => vec![DiffEntry::Deleted {
            value: format_value(v),
        }],
        (Some(v1), Some(v2)) => compute_value_diff(v1, v2),
    }
}

/// Compute diff between two values
fn compute_value_diff(val1: &Value, val2: &Value) -> Vec<DiffEntry> {
    match (val1, val2) {
        (Value::String(s1), Value::String(s2)) => {
            if s1 == s2 {
                vec![]
            } else {
                vec![DiffEntry::Modified {
                    old: String::from_utf8_lossy(s1).to_string(),
                    new: String::from_utf8_lossy(s2).to_string(),
                }]
            }
        }
        (Value::Hash(h1), Value::Hash(h2)) => compute_hash_diff(h1, h2),
        (Value::Set(s1), Value::Set(s2)) => compute_set_diff(s1, s2),
        _ => {
            // Different types or complex comparison
            vec![DiffEntry::Modified {
                old: format_value(val1),
                new: format_value(val2),
            }]
        }
    }
}

/// Compute diff between two hashes
fn compute_hash_diff(
    h1: &std::collections::HashMap<Bytes, Bytes>,
    h2: &std::collections::HashMap<Bytes, Bytes>,
) -> Vec<DiffEntry> {
    let mut changes = Vec::new();

    // Check for removed or modified fields
    for (field, value1) in h1 {
        let field_str = String::from_utf8_lossy(field).to_string();
        match h2.get(field) {
            None => {
                changes.push(DiffEntry::HashFieldRemoved {
                    field: field_str,
                    value: String::from_utf8_lossy(value1).to_string(),
                });
            }
            Some(value2) if value1 != value2 => {
                changes.push(DiffEntry::HashFieldModified {
                    field: field_str,
                    old: String::from_utf8_lossy(value1).to_string(),
                    new: String::from_utf8_lossy(value2).to_string(),
                });
            }
            _ => {}
        }
    }

    // Check for added fields
    for (field, value) in h2 {
        if !h1.contains_key(field) {
            changes.push(DiffEntry::HashFieldAdded {
                field: String::from_utf8_lossy(field).to_string(),
                value: String::from_utf8_lossy(value).to_string(),
            });
        }
    }

    changes
}

/// Compute diff between two sets
fn compute_set_diff(
    s1: &std::collections::HashSet<Bytes>,
    s2: &std::collections::HashSet<Bytes>,
) -> Vec<DiffEntry> {
    let mut changes = Vec::new();

    // Check for removed members
    for member in s1 {
        if !s2.contains(member) {
            changes.push(DiffEntry::SetMemberRemoved {
                member: String::from_utf8_lossy(member).to_string(),
            });
        }
    }

    // Check for added members
    for member in s2 {
        if !s1.contains(member) {
            changes.push(DiffEntry::SetMemberAdded {
                member: String::from_utf8_lossy(member).to_string(),
            });
        }
    }

    changes
}

/// Format a value for display
fn format_value(value: &Value) -> String {
    match value {
        Value::String(s) => String::from_utf8_lossy(s).to_string(),
        Value::List(l) => format!("[list with {} items]", l.len()),
        Value::Hash(h) => format!("{{hash with {} fields}}", h.len()),
        Value::Set(s) => format!("{{set with {} members}}", s.len()),
        Value::SortedSet { by_member, .. } => format!("{{zset with {} members}}", by_member.len()),
        Value::Stream(_) => "[stream]".to_string(),
        Value::HyperLogLog(_) => "[hyperloglog]".to_string(),
    }
}

/// Error types for temporal queries
#[derive(Clone, Debug, thiserror::Error)]
pub enum QueryError {
    /// Key not found
    #[error("key not found")]
    KeyNotFound,

    /// Invalid timestamp
    #[error("invalid timestamp")]
    InvalidTimestamp,

    /// No history available
    #[error("no history available for key")]
    NoHistory,

    /// IO error
    #[error("IO error: {0}")]
    Io(String),

    /// Internal error
    #[error("internal error: {0}")]
    Internal(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn test_history_query_builder() {
        let query = HistoryQuery::new()
            .from(TimestampSpec::Relative(std::time::Duration::from_secs(
                3600,
            )))
            .limit(50)
            .ascending()
            .with_values();

        assert!(query.from.is_some());
        assert_eq!(query.limit, Some(50));
        assert!(query.ascending);
        assert!(query.with_values);
    }

    #[test]
    fn test_diff_entry_types() {
        let created = DiffEntry::Created {
            value: "test".to_string(),
        };
        let deleted = DiffEntry::Deleted {
            value: "test".to_string(),
        };
        let modified = DiffEntry::Modified {
            old: "old".to_string(),
            new: "new".to_string(),
        };

        // Just verify these compile and can be matched
        match created {
            DiffEntry::Created { value } => assert_eq!(value, "test"),
            _ => panic!("wrong variant"),
        }

        match deleted {
            DiffEntry::Deleted { value } => assert_eq!(value, "test"),
            _ => panic!("wrong variant"),
        }

        match modified {
            DiffEntry::Modified { old, new } => {
                assert_eq!(old, "old");
                assert_eq!(new, "new");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_compute_diff_none_to_some() {
        let val = Value::String(Bytes::from("test"));
        let changes = compute_diff(&None, &Some(val));

        assert_eq!(changes.len(), 1);
        match &changes[0] {
            DiffEntry::Created { value } => assert_eq!(value, "test"),
            _ => panic!("wrong diff type"),
        }
    }

    #[test]
    fn test_compute_diff_some_to_none() {
        let val = Value::String(Bytes::from("test"));
        let changes = compute_diff(&Some(val), &None);

        assert_eq!(changes.len(), 1);
        match &changes[0] {
            DiffEntry::Deleted { value } => assert_eq!(value, "test"),
            _ => panic!("wrong diff type"),
        }
    }

    #[test]
    fn test_compute_diff_string_modified() {
        let val1 = Value::String(Bytes::from("old"));
        let val2 = Value::String(Bytes::from("new"));
        let changes = compute_diff(&Some(val1), &Some(val2));

        assert_eq!(changes.len(), 1);
        match &changes[0] {
            DiffEntry::Modified { old, new } => {
                assert_eq!(old, "old");
                assert_eq!(new, "new");
            }
            _ => panic!("wrong diff type"),
        }
    }

    #[test]
    fn test_compute_hash_diff() {
        let mut h1 = HashMap::new();
        h1.insert(Bytes::from("a"), Bytes::from("1"));
        h1.insert(Bytes::from("b"), Bytes::from("2"));

        let mut h2 = HashMap::new();
        h2.insert(Bytes::from("a"), Bytes::from("1")); // unchanged
        h2.insert(Bytes::from("b"), Bytes::from("3")); // modified
        h2.insert(Bytes::from("c"), Bytes::from("4")); // added

        let changes = compute_hash_diff(&h1, &h2);

        // Should have: b modified, c added
        assert_eq!(changes.len(), 2);
    }

    #[test]
    fn test_compute_set_diff() {
        let mut s1 = HashSet::new();
        s1.insert(Bytes::from("a"));
        s1.insert(Bytes::from("b"));

        let mut s2 = HashSet::new();
        s2.insert(Bytes::from("b"));
        s2.insert(Bytes::from("c"));

        let changes = compute_set_diff(&s1, &s2);

        // Should have: a removed, c added
        assert_eq!(changes.len(), 2);
    }

    #[test]
    fn test_diff_result_helpers() {
        let result = DiffResult {
            key: Bytes::from("test"),
            timestamp1: SystemTime::now(),
            timestamp2: SystemTime::now(),
            value1: None,
            value2: Some(Value::String(Bytes::from("new"))),
            changes: vec![DiffEntry::Created {
                value: "new".to_string(),
            }],
        };

        assert!(result.was_created());
        assert!(!result.was_deleted());
        assert!(result.has_changes());
    }

    #[test]
    fn test_query_error_display() {
        let err = QueryError::KeyNotFound;
        assert_eq!(err.to_string(), "key not found");

        let err = QueryError::InvalidTimestamp;
        assert_eq!(err.to_string(), "invalid timestamp");
    }
}
