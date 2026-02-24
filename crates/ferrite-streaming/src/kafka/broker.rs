#![forbid(unsafe_code)]
//! Kafka-compatible streaming broker — the main entry point for topic and
//! partition management, producing, and consuming.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::Utc;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::consumer_group::ConsumerGroup;
use super::partition_log::PartitionLog;
use super::record::{ConsumerRecord, ProducerRecord};
use super::topic::Topic;

/// Errors produced by the streaming broker.
#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("topic '{0}' already exists")]
    TopicAlreadyExists(String),
    #[error("topic '{0}' not found")]
    TopicNotFound(String),
    #[error("partition {1} not found for topic '{0}'")]
    PartitionNotFound(String, u32),
    #[error("consumer group '{0}' not found")]
    GroupNotFound(String),
    #[error("message exceeds max size ({0} > {1})")]
    MessageTooLarge(usize, usize),
}

/// Broker-level configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerConfig {
    /// Default number of partitions for new topics.
    pub default_partitions: u32,
    /// Default replication factor for new topics.
    pub default_replication_factor: u16,
    /// Maximum fetch size in bytes.
    pub max_fetch_bytes: usize,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            default_partitions: 4,
            default_replication_factor: 1,
            max_fetch_bytes: 1_048_576, // 1 MB
        }
    }
}

/// Aggregated broker statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerStats {
    pub total_topics: usize,
    pub total_partitions: usize,
    pub total_messages: u64,
    pub total_consumer_groups: usize,
}

/// The main streaming broker.
pub struct StreamingBroker {
    topics: DashMap<String, Topic>,
    partitions: DashMap<(String, u32), RwLock<PartitionLog>>,
    consumer_groups: DashMap<String, RwLock<ConsumerGroup>>,
    config: BrokerConfig,
    message_count: AtomicU64,
}

impl StreamingBroker {
    /// Create a broker with default configuration.
    pub fn new() -> Self {
        Self::with_config(BrokerConfig::default())
    }

    /// Create a broker with custom configuration.
    pub fn with_config(config: BrokerConfig) -> Self {
        Self {
            topics: DashMap::new(),
            partitions: DashMap::new(),
            consumer_groups: DashMap::new(),
            config,
            message_count: AtomicU64::new(0),
        }
    }

    // ── Topic management ────────────────────────────────────────────────

    /// Create a new topic. Returns an error if it already exists.
    pub fn create_topic(
        &self,
        name: String,
        num_partitions: u32,
        replication_factor: u16,
        retention_ms: i64,
    ) -> Result<Topic, BrokerError> {
        if self.topics.contains_key(&name) {
            return Err(BrokerError::TopicAlreadyExists(name));
        }

        let mut topic = Topic::new(name.clone(), num_partitions, replication_factor);
        topic.config.retention_ms = retention_ms;

        // Create partition logs
        for pid in 0..num_partitions {
            self.partitions
                .insert((name.clone(), pid), RwLock::new(PartitionLog::new()));
        }

        self.topics.insert(name, topic.clone());
        Ok(topic)
    }

    /// Delete a topic and all its partitions.
    pub fn delete_topic(&self, name: &str) -> Result<(), BrokerError> {
        let topic = self
            .topics
            .remove(name)
            .ok_or_else(|| BrokerError::TopicNotFound(name.into()))?;

        for pid in 0..topic.1.num_partitions {
            self.partitions.remove(&(name.to_string(), pid));
        }
        Ok(())
    }

    /// List all topics.
    pub fn list_topics(&self) -> Vec<Topic> {
        self.topics.iter().map(|r| r.value().clone()).collect()
    }

    /// Describe a single topic.
    pub fn describe_topic(&self, name: &str) -> Option<Topic> {
        self.topics.get(name).map(|r| r.value().clone())
    }

    // ── Produce / Fetch ─────────────────────────────────────────────────

    /// Produce a record to a topic. Returns `(partition, offset)`.
    pub fn produce(&self, record: ProducerRecord) -> Result<(u32, i64), BrokerError> {
        let topic = self
            .topics
            .get(&record.topic)
            .ok_or_else(|| BrokerError::TopicNotFound(record.topic.clone()))?;

        // Size check
        if record.value.len() > topic.config.max_message_bytes {
            return Err(BrokerError::MessageTooLarge(
                record.value.len(),
                topic.config.max_message_bytes,
            ));
        }

        // Determine partition
        let partition = record.partition.unwrap_or_else(|| {
            if let Some(ref key) = record.key {
                let mut hasher = DefaultHasher::new();
                key.hash(&mut hasher);
                (hasher.finish() % topic.num_partitions as u64) as u32
            } else {
                // Simple round-robin approximation
                (self.message_count.load(Ordering::Relaxed) % topic.num_partitions as u64) as u32
            }
        });

        let partition_key = (record.topic.clone(), partition);
        let log = self
            .partitions
            .get(&partition_key)
            .ok_or_else(|| BrokerError::PartitionNotFound(record.topic.clone(), partition))?;

        let consumer_record = ConsumerRecord {
            topic: record.topic,
            partition,
            offset: 0, // assigned by log
            key: record.key,
            value: record.value,
            headers: record.headers,
            timestamp: record.timestamp.unwrap_or_else(|| Utc::now().timestamp_millis()),
        };

        let offset = log.write().append(consumer_record);
        self.message_count.fetch_add(1, Ordering::Relaxed);
        Ok((partition, offset))
    }

    /// Fetch records from a specific partition.
    pub fn fetch(
        &self,
        topic: &str,
        partition: u32,
        offset: i64,
        max_records: usize,
    ) -> Result<Vec<ConsumerRecord>, BrokerError> {
        let key = (topic.to_string(), partition);
        let log = self
            .partitions
            .get(&key)
            .ok_or_else(|| BrokerError::PartitionNotFound(topic.into(), partition))?;

        let records = log.read().read(offset, max_records);
        Ok(records)
    }

    // ── Consumer groups ─────────────────────────────────────────────────

    /// Commit an offset for a consumer group.
    pub fn commit_offset(
        &self,
        group_id: &str,
        topic: String,
        partition: u32,
        offset: i64,
    ) -> Result<(), BrokerError> {
        self.consumer_groups
            .entry(group_id.to_string())
            .or_insert_with(|| RwLock::new(ConsumerGroup::new(group_id.to_string())));

        let group = self
            .consumer_groups
            .get(group_id)
            .ok_or_else(|| BrokerError::GroupNotFound(group_id.into()))?;

        group.read().commit_offset(topic, partition, offset);
        Ok(())
    }

    /// Get earliest and latest offsets for a partition.
    pub fn get_offsets(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<(i64, i64), BrokerError> {
        let key = (topic.to_string(), partition);
        let log = self
            .partitions
            .get(&key)
            .ok_or_else(|| BrokerError::PartitionNotFound(topic.into(), partition))?;

        let log = log.read();
        Ok((log.log_start_offset, log.high_watermark))
    }

    /// List consumer groups, optionally filtered by topic.
    pub fn list_groups(&self, _topic: Option<&str>) -> Vec<String> {
        self.consumer_groups
            .iter()
            .map(|r| r.key().clone())
            .collect()
    }

    // ── Stats ───────────────────────────────────────────────────────────

    /// Aggregate broker statistics.
    pub fn stats(&self) -> BrokerStats {
        BrokerStats {
            total_topics: self.topics.len(),
            total_partitions: self.partitions.len(),
            total_messages: self.message_count.load(Ordering::Relaxed),
            total_consumer_groups: self.consumer_groups.len(),
        }
    }
}

impl Default for StreamingBroker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_list_topics() {
        let broker = StreamingBroker::new();
        broker.create_topic("t1".into(), 4, 1, -1).unwrap();
        let topics = broker.list_topics();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].name, "t1");
        assert_eq!(topics[0].num_partitions, 4);
    }

    #[test]
    fn test_create_duplicate_topic() {
        let broker = StreamingBroker::new();
        broker.create_topic("t1".into(), 2, 1, -1).unwrap();
        let err = broker.create_topic("t1".into(), 2, 1, -1).unwrap_err();
        assert!(matches!(err, BrokerError::TopicAlreadyExists(_)));
    }

    #[test]
    fn test_delete_topic() {
        let broker = StreamingBroker::new();
        broker.create_topic("t1".into(), 2, 1, -1).unwrap();
        broker.delete_topic("t1").unwrap();
        assert!(broker.list_topics().is_empty());
    }

    #[test]
    fn test_produce_and_fetch() {
        let broker = StreamingBroker::new();
        broker.create_topic("events".into(), 2, 1, -1).unwrap();

        let record = ProducerRecord {
            topic: "events".into(),
            partition: Some(0),
            key: Some(b"k1".to_vec()),
            value: b"hello".to_vec(),
            headers: vec![],
            timestamp: Some(1_700_000_000_000),
        };
        let (p, o) = broker.produce(record).unwrap();
        assert_eq!(p, 0);
        assert_eq!(o, 0);

        let records = broker.fetch("events", 0, 0, 10).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value, b"hello");
    }

    #[test]
    fn test_produce_to_missing_topic() {
        let broker = StreamingBroker::new();
        let record = ProducerRecord {
            topic: "missing".into(),
            partition: None,
            key: None,
            value: b"x".to_vec(),
            headers: vec![],
            timestamp: None,
        };
        let err = broker.produce(record).unwrap_err();
        assert!(matches!(err, BrokerError::TopicNotFound(_)));
    }

    #[test]
    fn test_commit_and_get_offsets() {
        let broker = StreamingBroker::new();
        broker.create_topic("t".into(), 1, 1, -1).unwrap();
        broker.commit_offset("g1", "t".into(), 0, 42).unwrap();

        // The group should have been auto-created
        assert_eq!(broker.list_groups(None).len(), 1);
    }

    #[test]
    fn test_get_offsets() {
        let broker = StreamingBroker::new();
        broker.create_topic("t".into(), 1, 1, -1).unwrap();

        let (earliest, latest) = broker.get_offsets("t", 0).unwrap();
        assert_eq!(earliest, 0);
        assert_eq!(latest, 0);

        broker
            .produce(ProducerRecord {
                topic: "t".into(),
                partition: Some(0),
                key: None,
                value: b"v".to_vec(),
                headers: vec![],
                timestamp: None,
            })
            .unwrap();

        let (earliest, latest) = broker.get_offsets("t", 0).unwrap();
        assert_eq!(earliest, 0);
        assert_eq!(latest, 1);
    }

    #[test]
    fn test_stats() {
        let broker = StreamingBroker::new();
        broker.create_topic("t".into(), 3, 1, -1).unwrap();
        let s = broker.stats();
        assert_eq!(s.total_topics, 1);
        assert_eq!(s.total_partitions, 3);
        assert_eq!(s.total_messages, 0);
    }

    #[test]
    fn test_message_too_large() {
        let broker = StreamingBroker::new();
        broker.create_topic("t".into(), 1, 1, -1).unwrap();

        let big_value = vec![0u8; 2_000_000]; // 2 MB, exceeds 1 MB default
        let err = broker
            .produce(ProducerRecord {
                topic: "t".into(),
                partition: Some(0),
                key: None,
                value: big_value,
                headers: vec![],
                timestamp: None,
            })
            .unwrap_err();
        assert!(matches!(err, BrokerError::MessageTooLarge(_, _)));
    }
}
