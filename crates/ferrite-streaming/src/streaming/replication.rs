//! Streaming Replication Protocol
//!
//! Kafka-compatible wire protocol for producer/consumer patterns with CDC bridge.
//! Supports topic management, producer/consumer groups, offset tracking, and
//! integration with the native CDC module for event bridging.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────┐     ┌──────────────────────────────┐     ┌──────────────┐
//! │   Producer   │────▶│  Streaming Replication Broker │────▶│   Consumer   │
//! │  (Kafka API) │     │  ┌────────┐  ┌────────────┐  │     │  (Kafka API) │
//! └──────────────┘     │  │ Topics │  │  Offsets    │  │     └──────────────┘
//!                      │  └────────┘  └────────────┘  │
//!                      │  ┌──────────────────────┐    │
//!                      │  │     CDC Bridge       │    │
//!                      │  └──────────────────────┘    │
//!                      └──────────────────────────────┘
//! ```

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A single record in the replication stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationRecord {
    /// Optional key for partitioning
    pub key: Option<Vec<u8>>,
    /// Payload bytes
    pub value: Vec<u8>,
    /// Record headers
    pub headers: HashMap<String, Vec<u8>>,
    /// Timestamp (ms since epoch)
    pub timestamp: u64,
    /// Assigned offset (populated on commit)
    pub offset: u64,
    /// Partition index
    pub partition: u32,
}

impl ReplicationRecord {
    pub fn new(key: Option<Vec<u8>>, value: Vec<u8>) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self {
            key,
            value,
            headers: HashMap::new(),
            timestamp,
            offset: 0,
            partition: 0,
        }
    }

    pub fn with_header(mut self, key: &str, value: Vec<u8>) -> Self {
        self.headers.insert(key.to_string(), value);
        self
    }
}

/// Acknowledgement policy for producers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Acks {
    /// No acknowledgement required
    None,
    /// Leader acknowledgement
    Leader,
    /// All in-sync replicas
    All,
}

impl Default for Acks {
    fn default() -> Self {
        Self::Leader
    }
}

/// Offset reset strategy when no committed offset exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OffsetReset {
    Earliest,
    Latest,
}

impl Default for OffsetReset {
    fn default() -> Self {
        Self::Latest
    }
}

// ---------------------------------------------------------------------------
// Partition
// ---------------------------------------------------------------------------

/// A single partition storing an ordered log of records.
#[derive(Debug)]
struct Partition {
    id: u32,
    records: VecDeque<ReplicationRecord>,
    next_offset: AtomicU64,
    retention: Duration,
}

impl Partition {
    fn new(id: u32, retention: Duration) -> Self {
        Self {
            id,
            records: VecDeque::new(),
            next_offset: AtomicU64::new(0),
            retention,
        }
    }

    fn append(&mut self, mut record: ReplicationRecord) -> u64 {
        let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);
        record.offset = offset;
        record.partition = self.id;
        self.records.push_back(record);
        self.enforce_retention();
        offset
    }

    fn fetch(&self, start_offset: u64, max_records: usize) -> Vec<ReplicationRecord> {
        self.records
            .iter()
            .filter(|r| r.offset >= start_offset)
            .take(max_records)
            .cloned()
            .collect()
    }

    fn earliest_offset(&self) -> u64 {
        self.records.front().map(|r| r.offset).unwrap_or(0)
    }

    fn latest_offset(&self) -> u64 {
        self.next_offset.load(Ordering::SeqCst)
    }

    fn enforce_retention(&mut self) {
        let cutoff = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
            - self.retention.as_millis() as u64;

        while let Some(front) = self.records.front() {
            if front.timestamp < cutoff {
                self.records.pop_front();
            } else {
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Topic
// ---------------------------------------------------------------------------

/// Topic configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    pub name: String,
    pub num_partitions: u32,
    pub replication_factor: u16,
    pub retention: Duration,
    pub max_message_bytes: usize,
    pub cleanup_policy: CleanupPolicy,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            num_partitions: 1,
            replication_factor: 1,
            retention: Duration::from_secs(7 * 24 * 3600),
            max_message_bytes: 1_048_576, // 1 MB
            cleanup_policy: CleanupPolicy::Delete,
        }
    }
}

/// Cleanup strategy for old records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CleanupPolicy {
    Delete,
    Compact,
    DeleteAndCompact,
}

/// Represents a topic with its partitions.
struct Topic {
    config: TopicConfig,
    partitions: Vec<RwLock<Partition>>,
}

impl Topic {
    fn new(config: TopicConfig) -> Self {
        let partitions = (0..config.num_partitions)
            .map(|i| RwLock::new(Partition::new(i, config.retention)))
            .collect();
        Self { config, partitions }
    }

    fn partition_for_key(&self, key: &[u8]) -> u32 {
        let hash = crc32fast::hash(key);
        hash % self.config.num_partitions
    }
}

// ---------------------------------------------------------------------------
// Consumer Group
// ---------------------------------------------------------------------------

/// Tracks committed offsets for a consumer group.
#[derive(Debug, Default)]
struct ConsumerGroupState {
    /// topic -> partition -> committed offset
    offsets: HashMap<String, BTreeMap<u32, u64>>,
    members: HashMap<String, ConsumerMember>,
    generation: u64,
}

#[derive(Debug, Clone)]
struct ConsumerMember {
    id: String,
    assigned_partitions: Vec<(String, u32)>,
    last_heartbeat: u64,
}

/// Consumer group coordinator.
struct ConsumerGroup {
    group_id: String,
    state: RwLock<ConsumerGroupState>,
}

impl ConsumerGroup {
    fn new(group_id: String) -> Self {
        Self {
            group_id,
            state: RwLock::new(ConsumerGroupState::default()),
        }
    }

    async fn commit_offset(&self, topic: &str, partition: u32, offset: u64) {
        let mut state = self.state.write().await;
        state
            .offsets
            .entry(topic.to_string())
            .or_default()
            .insert(partition, offset);
    }

    async fn get_offset(&self, topic: &str, partition: u32) -> Option<u64> {
        let state = self.state.read().await;
        state
            .offsets
            .get(topic)
            .and_then(|p| p.get(&partition).copied())
    }

    async fn join(&self, member_id: String) -> u64 {
        let mut state = self.state.write().await;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        state.members.insert(
            member_id.clone(),
            ConsumerMember {
                id: member_id,
                assigned_partitions: Vec::new(),
                last_heartbeat: now,
            },
        );
        state.generation += 1;
        state.generation
    }

    async fn leave(&self, member_id: &str) {
        let mut state = self.state.write().await;
        state.members.remove(member_id);
        state.generation += 1;
    }
}

// ---------------------------------------------------------------------------
// Broker (main entry point)
// ---------------------------------------------------------------------------

/// Configuration for the streaming replication broker.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub default_partitions: u32,
    pub default_retention: Duration,
    pub max_message_bytes: usize,
    pub fetch_max_wait: Duration,
    pub fetch_max_bytes: usize,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            default_partitions: 4,
            default_retention: Duration::from_secs(7 * 24 * 3600),
            max_message_bytes: 1_048_576,
            fetch_max_wait: Duration::from_millis(500),
            fetch_max_bytes: 52_428_800,
        }
    }
}

/// Central streaming replication broker managing topics, partitions,
/// consumer groups, and the CDC bridge.
pub struct ReplicationBroker {
    config: BrokerConfig,
    topics: Arc<RwLock<HashMap<String, Arc<Topic>>>>,
    consumer_groups: Arc<RwLock<HashMap<String, Arc<ConsumerGroup>>>>,
    metrics: Arc<BrokerMetrics>,
}

/// Broker-level metrics.
#[derive(Debug, Default)]
pub struct BrokerMetrics {
    pub records_produced: AtomicU64,
    pub records_consumed: AtomicU64,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
    pub active_topics: AtomicU64,
    pub active_consumer_groups: AtomicU64,
}

/// Produce result returned to the caller.
#[derive(Debug, Clone)]
pub struct ProduceResult {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub timestamp: u64,
}

/// Fetch result containing records and metadata.
#[derive(Debug, Clone)]
pub struct FetchResult {
    pub topic: String,
    pub partition: u32,
    pub records: Vec<ReplicationRecord>,
    pub high_watermark: u64,
}

/// Topic metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMetadata {
    pub name: String,
    pub num_partitions: u32,
    pub replication_factor: u16,
    pub retention: Duration,
}

/// Errors from the streaming replication module.
#[derive(Debug, thiserror::Error)]
pub enum ReplicationError {
    #[error("Topic already exists: {0}")]
    TopicExists(String),

    #[error("Topic not found: {0}")]
    TopicNotFound(String),

    #[error("Partition not found: {topic} partition {partition}")]
    PartitionNotFound { topic: String, partition: u32 },

    #[error("Consumer group not found: {0}")]
    GroupNotFound(String),

    #[error("Message too large: {size} bytes (max {max})")]
    MessageTooLarge { size: usize, max: usize },

    #[error("Invalid offset: {0}")]
    InvalidOffset(u64),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl ReplicationBroker {
    /// Create a new broker with the given configuration.
    pub fn new(config: BrokerConfig) -> Self {
        Self {
            config,
            topics: Arc::new(RwLock::new(HashMap::new())),
            consumer_groups: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(BrokerMetrics::default()),
        }
    }

    // -----------------------------------------------------------------------
    // Topic management
    // -----------------------------------------------------------------------

    /// Create a new topic.
    pub async fn create_topic(
        &self,
        config: TopicConfig,
    ) -> Result<TopicMetadata, ReplicationError> {
        let mut topics = self.topics.write().await;

        if topics.contains_key(&config.name) {
            return Err(ReplicationError::TopicExists(config.name.clone()));
        }

        let meta = TopicMetadata {
            name: config.name.clone(),
            num_partitions: config.num_partitions,
            replication_factor: config.replication_factor,
            retention: config.retention,
        };

        topics.insert(config.name.clone(), Arc::new(Topic::new(config)));
        self.metrics.active_topics.fetch_add(1, Ordering::Relaxed);
        Ok(meta)
    }

    /// Delete a topic.
    pub async fn delete_topic(&self, name: &str) -> Result<(), ReplicationError> {
        let mut topics = self.topics.write().await;
        if topics.remove(name).is_none() {
            return Err(ReplicationError::TopicNotFound(name.to_string()));
        }
        self.metrics.active_topics.fetch_sub(1, Ordering::Relaxed);
        Ok(())
    }

    /// List all topics.
    pub async fn list_topics(&self) -> Vec<TopicMetadata> {
        let topics = self.topics.read().await;
        topics
            .values()
            .map(|t| TopicMetadata {
                name: t.config.name.clone(),
                num_partitions: t.config.num_partitions,
                replication_factor: t.config.replication_factor,
                retention: t.config.retention,
            })
            .collect()
    }

    /// Get metadata for a single topic.
    pub async fn topic_metadata(&self, name: &str) -> Result<TopicMetadata, ReplicationError> {
        let topics = self.topics.read().await;
        let topic = topics
            .get(name)
            .ok_or_else(|| ReplicationError::TopicNotFound(name.to_string()))?;
        Ok(TopicMetadata {
            name: topic.config.name.clone(),
            num_partitions: topic.config.num_partitions,
            replication_factor: topic.config.replication_factor,
            retention: topic.config.retention,
        })
    }

    // -----------------------------------------------------------------------
    // Produce
    // -----------------------------------------------------------------------

    /// Produce a record to a topic. If a key is provided it determines the partition;
    /// otherwise a round-robin or random partition is chosen.
    pub async fn produce(
        &self,
        topic_name: &str,
        record: ReplicationRecord,
    ) -> Result<ProduceResult, ReplicationError> {
        if record.value.len() > self.config.max_message_bytes {
            return Err(ReplicationError::MessageTooLarge {
                size: record.value.len(),
                max: self.config.max_message_bytes,
            });
        }

        let topics = self.topics.read().await;
        let topic = topics
            .get(topic_name)
            .ok_or_else(|| ReplicationError::TopicNotFound(topic_name.to_string()))?;

        let partition_id = match &record.key {
            Some(key) => topic.partition_for_key(key),
            None => {
                let counter = self.metrics.records_produced.load(Ordering::Relaxed);
                (counter as u32) % topic.config.num_partitions
            }
        };

        let partition = topic.partitions.get(partition_id as usize).ok_or_else(|| {
            ReplicationError::PartitionNotFound {
                topic: topic_name.to_string(),
                partition: partition_id,
            }
        })?;

        let timestamp = record.timestamp;
        let mut part = partition.write().await;
        let offset = part.append(record);

        self.metrics
            .records_produced
            .fetch_add(1, Ordering::Relaxed);
        self.metrics.bytes_in.fetch_add(
            part.records
                .back()
                .map(|r| r.value.len() as u64)
                .unwrap_or(0),
            Ordering::Relaxed,
        );

        Ok(ProduceResult {
            topic: topic_name.to_string(),
            partition: partition_id,
            offset,
            timestamp,
        })
    }

    /// Produce a batch of records.
    pub async fn produce_batch(
        &self,
        topic_name: &str,
        records: Vec<ReplicationRecord>,
    ) -> Result<Vec<ProduceResult>, ReplicationError> {
        let mut results = Vec::with_capacity(records.len());
        for record in records {
            results.push(self.produce(topic_name, record).await?);
        }
        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Fetch / consume
    // -----------------------------------------------------------------------

    /// Fetch records from a specific topic-partition starting at the given offset.
    pub async fn fetch(
        &self,
        topic_name: &str,
        partition_id: u32,
        start_offset: u64,
        max_records: usize,
    ) -> Result<FetchResult, ReplicationError> {
        let topics = self.topics.read().await;
        let topic = topics
            .get(topic_name)
            .ok_or_else(|| ReplicationError::TopicNotFound(topic_name.to_string()))?;

        let partition = topic.partitions.get(partition_id as usize).ok_or_else(|| {
            ReplicationError::PartitionNotFound {
                topic: topic_name.to_string(),
                partition: partition_id,
            }
        })?;

        let part = partition.read().await;
        let records = part.fetch(start_offset, max_records);
        let high_watermark = part.latest_offset();

        let bytes_out: u64 = records.iter().map(|r| r.value.len() as u64).sum();
        self.metrics
            .records_consumed
            .fetch_add(records.len() as u64, Ordering::Relaxed);
        self.metrics
            .bytes_out
            .fetch_add(bytes_out, Ordering::Relaxed);

        Ok(FetchResult {
            topic: topic_name.to_string(),
            partition: partition_id,
            records,
            high_watermark,
        })
    }

    // -----------------------------------------------------------------------
    // Consumer groups
    // -----------------------------------------------------------------------

    /// Get or create a consumer group.
    pub async fn get_or_create_group(&self, group_id: &str) -> Arc<ConsumerGroup> {
        let mut groups = self.consumer_groups.write().await;
        groups
            .entry(group_id.to_string())
            .or_insert_with(|| {
                self.metrics
                    .active_consumer_groups
                    .fetch_add(1, Ordering::Relaxed);
                Arc::new(ConsumerGroup::new(group_id.to_string()))
            })
            .clone()
    }

    /// Commit an offset for a consumer group.
    pub async fn commit_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<(), ReplicationError> {
        let group = {
            let groups = self.consumer_groups.read().await;
            groups
                .get(group_id)
                .cloned()
                .ok_or_else(|| ReplicationError::GroupNotFound(group_id.to_string()))?
        };
        group.commit_offset(topic, partition, offset).await;
        Ok(())
    }

    /// Get the committed offset for a consumer group.
    pub async fn get_committed_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
    ) -> Result<Option<u64>, ReplicationError> {
        let group = {
            let groups = self.consumer_groups.read().await;
            groups
                .get(group_id)
                .cloned()
                .ok_or_else(|| ReplicationError::GroupNotFound(group_id.to_string()))?
        };
        Ok(group.get_offset(topic, partition).await)
    }

    /// Join a consumer group.
    pub async fn join_group(
        &self,
        group_id: &str,
        member_id: String,
    ) -> Result<u64, ReplicationError> {
        let group = self.get_or_create_group(group_id).await;
        Ok(group.join(member_id).await)
    }

    /// Leave a consumer group.
    pub async fn leave_group(
        &self,
        group_id: &str,
        member_id: &str,
    ) -> Result<(), ReplicationError> {
        let group = {
            let groups = self.consumer_groups.read().await;
            groups
                .get(group_id)
                .cloned()
                .ok_or_else(|| ReplicationError::GroupNotFound(group_id.to_string()))?
        };
        group.leave(member_id).await;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // CDC bridge
    // -----------------------------------------------------------------------

    /// Bridge a CDC change event into a topic.
    /// Converts CDC events (from `ferrite::cdc`) into `ReplicationRecord`s and
    /// produces them to the designated topic.
    pub async fn bridge_cdc_event(
        &self,
        topic_name: &str,
        key: Vec<u8>,
        value: Vec<u8>,
        operation: &str,
    ) -> Result<ProduceResult, ReplicationError> {
        let record = ReplicationRecord::new(Some(key), value)
            .with_header("cdc.operation", operation.as_bytes().to_vec());
        self.produce(topic_name, record).await
    }

    // -----------------------------------------------------------------------
    // Metrics
    // -----------------------------------------------------------------------

    /// Get a snapshot of broker metrics.
    pub fn metrics(&self) -> &BrokerMetrics {
        &self.metrics
    }

    /// Get partition offsets for a topic.
    pub async fn get_offsets(
        &self,
        topic_name: &str,
    ) -> Result<Vec<(u32, u64, u64)>, ReplicationError> {
        let topics = self.topics.read().await;
        let topic = topics
            .get(topic_name)
            .ok_or_else(|| ReplicationError::TopicNotFound(topic_name.to_string()))?;

        let mut offsets = Vec::with_capacity(topic.partitions.len());
        for partition in &topic.partitions {
            let part = partition.read().await;
            offsets.push((part.id, part.earliest_offset(), part.latest_offset()));
        }
        Ok(offsets)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_and_list_topics() {
        let broker = ReplicationBroker::new(BrokerConfig::default());
        let config = TopicConfig {
            name: "test-topic".to_string(),
            num_partitions: 3,
            ..Default::default()
        };
        let meta = broker.create_topic(config).await.unwrap();
        assert_eq!(meta.name, "test-topic");
        assert_eq!(meta.num_partitions, 3);

        let topics = broker.list_topics().await;
        assert_eq!(topics.len(), 1);
    }

    #[tokio::test]
    async fn test_produce_and_fetch() {
        let broker = ReplicationBroker::new(BrokerConfig::default());
        broker
            .create_topic(TopicConfig {
                name: "events".to_string(),
                num_partitions: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let record = ReplicationRecord::new(Some(b"key1".to_vec()), b"value1".to_vec());
        let result = broker.produce("events", record).await.unwrap();
        assert_eq!(result.offset, 0);
        assert_eq!(result.partition, 0);

        let fetch = broker.fetch("events", 0, 0, 10).await.unwrap();
        assert_eq!(fetch.records.len(), 1);
        assert_eq!(fetch.records[0].value, b"value1");
        assert_eq!(fetch.high_watermark, 1);
    }

    #[tokio::test]
    async fn test_consumer_group_offsets() {
        let broker = ReplicationBroker::new(BrokerConfig::default());
        broker
            .create_topic(TopicConfig {
                name: "orders".to_string(),
                num_partitions: 2,
                ..Default::default()
            })
            .await
            .unwrap();

        let gen = broker
            .join_group("my-group", "member-1".to_string())
            .await
            .unwrap();
        assert_eq!(gen, 1);

        broker
            .commit_offset("my-group", "orders", 0, 42)
            .await
            .unwrap();

        let offset = broker
            .get_committed_offset("my-group", "orders", 0)
            .await
            .unwrap();
        assert_eq!(offset, Some(42));

        let no_offset = broker
            .get_committed_offset("my-group", "orders", 1)
            .await
            .unwrap();
        assert_eq!(no_offset, None);
    }

    #[tokio::test]
    async fn test_produce_message_too_large() {
        let broker = ReplicationBroker::new(BrokerConfig {
            max_message_bytes: 10,
            ..Default::default()
        });
        broker
            .create_topic(TopicConfig {
                name: "small".to_string(),
                num_partitions: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let record = ReplicationRecord::new(None, vec![0u8; 100]);
        let result = broker.produce("small", record).await;
        assert!(matches!(
            result,
            Err(ReplicationError::MessageTooLarge { .. })
        ));
    }

    #[tokio::test]
    async fn test_duplicate_topic_error() {
        let broker = ReplicationBroker::new(BrokerConfig::default());
        let config = TopicConfig {
            name: "dup".to_string(),
            ..Default::default()
        };
        broker.create_topic(config.clone()).await.unwrap();
        let result = broker.create_topic(config).await;
        assert!(matches!(result, Err(ReplicationError::TopicExists(_))));
    }

    #[tokio::test]
    async fn test_cdc_bridge() {
        let broker = ReplicationBroker::new(BrokerConfig::default());
        broker
            .create_topic(TopicConfig {
                name: "cdc-events".to_string(),
                num_partitions: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let result = broker
            .bridge_cdc_event("cdc-events", b"user:1".to_vec(), b"data".to_vec(), "SET")
            .await
            .unwrap();

        let fetch = broker.fetch("cdc-events", 0, 0, 10).await.unwrap();
        assert_eq!(fetch.records.len(), 1);
        assert_eq!(
            fetch.records[0].headers.get("cdc.operation"),
            Some(&b"SET".to_vec())
        );
    }

    #[tokio::test]
    async fn test_batch_produce() {
        let broker = ReplicationBroker::new(BrokerConfig::default());
        broker
            .create_topic(TopicConfig {
                name: "batch".to_string(),
                num_partitions: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let records: Vec<_> = (0..5)
            .map(|i| ReplicationRecord::new(Some(b"k".to_vec()), format!("v{}", i).into_bytes()))
            .collect();

        let results = broker.produce_batch("batch", records).await.unwrap();
        assert_eq!(results.len(), 5);

        let fetch = broker.fetch("batch", 0, 0, 100).await.unwrap();
        assert_eq!(fetch.records.len(), 5);
        assert_eq!(fetch.high_watermark, 5);
    }
}
