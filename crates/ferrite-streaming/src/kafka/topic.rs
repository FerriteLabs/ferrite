#![forbid(unsafe_code)]
//! Kafka-compatible topic and partition types.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Cleanup policy for topic log segments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CleanupPolicy {
    Delete,
    Compact,
    DeleteAndCompact,
}

impl Default for CleanupPolicy {
    fn default() -> Self {
        Self::Delete
    }
}

/// Compression codec for records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Gzip,
    Lz4,
    Snappy,
}

impl Default for CompressionType {
    fn default() -> Self {
        Self::None
    }
}

/// Retention policy for a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RetentionPolicy {
    /// Retain records for a fixed duration.
    Time(Duration),
    /// Retain up to a total byte size.
    Size(u64),
    /// Retain indefinitely.
    Infinite,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self::Time(Duration::from_millis(7 * 24 * 60 * 60 * 1000)) // 7 days
    }
}

/// Per-topic configuration knobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    /// Maximum size of a single message in bytes (default 1 MB).
    pub max_message_bytes: usize,
    /// Retention time in milliseconds (-1 for infinite).
    pub retention_ms: i64,
    /// Retention size in bytes (-1 for unlimited).
    pub retention_bytes: i64,
    /// Log cleanup policy.
    pub cleanup_policy: CleanupPolicy,
    /// Record compression codec.
    pub compression: CompressionType,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            max_message_bytes: 1_048_576, // 1 MB
            retention_ms: 7 * 24 * 60 * 60 * 1000, // 7 days
            retention_bytes: -1,          // unlimited
            cleanup_policy: CleanupPolicy::default(),
            compression: CompressionType::default(),
        }
    }
}

/// A Kafka-compatible topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topic {
    /// Topic name.
    pub name: String,
    /// Number of partitions.
    pub num_partitions: u32,
    /// Replication factor.
    pub replication_factor: u16,
    /// Retention policy.
    pub retention: RetentionPolicy,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Topic-level configuration.
    pub config: TopicConfig,
}

impl Topic {
    /// Create a new topic with the given parameters.
    pub fn new(name: String, num_partitions: u32, replication_factor: u16) -> Self {
        Self {
            name,
            num_partitions,
            replication_factor,
            retention: RetentionPolicy::default(),
            created_at: Utc::now(),
            config: TopicConfig::default(),
        }
    }
}

/// A single partition of a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partition {
    /// Partition id within the topic.
    pub id: u32,
    /// Owning topic name.
    pub topic: String,
    /// Leader node id.
    pub leader: u32,
    /// Latest committed offset.
    pub high_watermark: i64,
    /// Earliest available offset.
    pub log_start_offset: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_defaults() {
        let topic = Topic::new("test-topic".into(), 4, 1);
        assert_eq!(topic.name, "test-topic");
        assert_eq!(topic.num_partitions, 4);
        assert_eq!(topic.replication_factor, 1);
        assert_eq!(topic.config.max_message_bytes, 1_048_576);
        assert_eq!(topic.config.retention_ms, 604_800_000);
        assert_eq!(topic.config.retention_bytes, -1);
    }

    #[test]
    fn test_cleanup_policy_default() {
        assert_eq!(CleanupPolicy::default(), CleanupPolicy::Delete);
    }

    #[test]
    fn test_compression_default() {
        assert_eq!(CompressionType::default(), CompressionType::None);
    }
}
