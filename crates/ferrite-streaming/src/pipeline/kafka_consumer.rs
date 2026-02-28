//! Kafka consumer adapter for the vector ingestion pipeline
//!
//! Bridges ferrite-streaming's Kafka-compatible broker to the
//! vector ingestion pipeline, consuming records and converting
//! them to IngestDocument format.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::kafka::broker::StreamingBroker;
use crate::kafka::record::ConsumerRecord as KafkaConsumerRecord;

use super::vector_ingest::IngestDocument;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur in the Kafka consumer.
#[derive(Debug, thiserror::Error)]
pub enum KafkaConsumerError {
    /// Failed to connect to the broker.
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// Topic was not found on the broker.
    #[error("topic not found: {0}")]
    TopicNotFound(String),

    /// Failed to deserialize a record.
    #[error("deserialization error: {0}")]
    DeserializationError(String),

    /// Failed to commit offsets.
    #[error("commit failed: {0}")]
    CommitFailed(String),

    /// Operation timed out.
    #[error("timeout after {0:?}")]
    Timeout(Duration),

    /// Consumer has been closed.
    #[error("consumer is closed")]
    ConsumerClosed,
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Offset reset policy when no committed offset exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AutoOffsetReset {
    /// Start from the earliest available offset.
    Earliest,
    /// Start from the latest (tail) offset.
    Latest,
}

impl Default for AutoOffsetReset {
    fn default() -> Self {
        Self::Earliest
    }
}

/// Configuration for the Kafka consumer adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConsumerConfig {
    /// Broker addresses (used for identification; the adapter uses a local broker).
    pub brokers: Vec<String>,
    /// Topic to consume from.
    pub topic: String,
    /// Consumer group identifier.
    pub group_id: String,
    /// Offset reset policy when no committed offset exists.
    #[serde(default)]
    pub auto_offset_reset: AutoOffsetReset,
    /// Maximum number of records returned per poll.
    #[serde(default = "default_max_poll_records")]
    pub max_poll_records: usize,
    /// Timeout for a single poll operation.
    #[serde(default = "default_poll_timeout")]
    pub poll_timeout: Duration,
    /// Session timeout before the consumer is considered dead.
    #[serde(default = "default_session_timeout")]
    pub session_timeout: Duration,
    /// Whether to automatically commit offsets after polling.
    #[serde(default = "default_enable_auto_commit")]
    pub enable_auto_commit: bool,
    /// Interval between automatic offset commits.
    #[serde(default = "default_auto_commit_interval")]
    pub auto_commit_interval: Duration,
}

fn default_max_poll_records() -> usize {
    500
}
fn default_poll_timeout() -> Duration {
    Duration::from_millis(100)
}
fn default_session_timeout() -> Duration {
    Duration::from_secs(30)
}
fn default_enable_auto_commit() -> bool {
    true
}
fn default_auto_commit_interval() -> Duration {
    Duration::from_secs(5)
}

impl Default for KafkaConsumerConfig {
    fn default() -> Self {
        Self {
            brokers: vec!["localhost:9092".to_string()],
            topic: String::new(),
            group_id: "ferrite-consumer".to_string(),
            auto_offset_reset: AutoOffsetReset::default(),
            max_poll_records: default_max_poll_records(),
            poll_timeout: default_poll_timeout(),
            session_timeout: default_session_timeout(),
            enable_auto_commit: default_enable_auto_commit(),
            auto_commit_interval: default_auto_commit_interval(),
        }
    }
}

// ---------------------------------------------------------------------------
// Consumer record (pipeline-facing)
// ---------------------------------------------------------------------------

/// A record consumed from a Kafka topic, ready for pipeline processing.
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    /// Source topic name.
    pub topic: String,
    /// Partition the record belongs to.
    pub partition: u32,
    /// Offset within the partition.
    pub offset: u64,
    /// Optional record key.
    pub key: Option<Vec<u8>>,
    /// Record payload.
    pub value: Vec<u8>,
    /// Broker-assigned timestamp (epoch millis).
    pub timestamp: u64,
    /// Application-level headers.
    pub headers: HashMap<String, Vec<u8>>,
}

impl From<KafkaConsumerRecord> for ConsumerRecord {
    fn from(r: KafkaConsumerRecord) -> Self {
        let headers = r
            .headers
            .into_iter()
            .collect::<HashMap<String, Vec<u8>>>();
        Self {
            topic: r.topic,
            partition: r.partition,
            offset: r.offset as u64,
            key: r.key,
            value: r.value,
            timestamp: r.timestamp as u64,
            headers,
        }
    }
}

// ---------------------------------------------------------------------------
// Consumer state
// ---------------------------------------------------------------------------

/// Internal mutable state of the consumer.
struct ConsumerInner {
    /// The topic currently subscribed to.
    subscribed_topic: Option<String>,
    /// Tracked offsets per partition: next offset to fetch.
    offsets: HashMap<u32, u64>,
    /// Whether the consumer is paused.
    paused: bool,
    /// Whether the consumer has been closed.
    closed: bool,
}

// ---------------------------------------------------------------------------
// KafkaConsumer
// ---------------------------------------------------------------------------

/// Kafka consumer adapter backed by the internal [`StreamingBroker`].
///
/// Wraps the broker's fetch/commit API and presents a high-level consumer
/// interface suitable for the vector ingestion pipeline.
pub struct KafkaConsumer {
    config: KafkaConsumerConfig,
    broker: Arc<StreamingBroker>,
    inner: RwLock<ConsumerInner>,
}

impl KafkaConsumer {
    /// Create a new consumer and connect to the internal broker.
    ///
    /// The broker is created lazily; no real network connections are made.
    pub fn new(config: KafkaConsumerConfig) -> Result<Self, KafkaConsumerError> {
        if config.brokers.is_empty() {
            return Err(KafkaConsumerError::ConnectionFailed(
                "no brokers configured".to_string(),
            ));
        }

        let broker = Arc::new(StreamingBroker::new());

        Ok(Self {
            config,
            broker,
            inner: RwLock::new(ConsumerInner {
                subscribed_topic: None,
                offsets: HashMap::new(),
                paused: false,
                closed: false,
            }),
        })
    }

    /// Create a consumer with a pre-existing broker instance (for testing
    /// and integration with other pipeline components).
    pub fn with_broker(
        config: KafkaConsumerConfig,
        broker: Arc<StreamingBroker>,
    ) -> Result<Self, KafkaConsumerError> {
        if config.brokers.is_empty() {
            return Err(KafkaConsumerError::ConnectionFailed(
                "no brokers configured".to_string(),
            ));
        }

        Ok(Self {
            config,
            broker,
            inner: RwLock::new(ConsumerInner {
                subscribed_topic: None,
                offsets: HashMap::new(),
                paused: false,
                closed: false,
            }),
        })
    }

    /// Subscribe to a topic. Initialises per-partition offsets based on the
    /// configured [`AutoOffsetReset`] policy.
    pub fn subscribe(&mut self, topic: &str) -> Result<(), KafkaConsumerError> {
        let inner = self.inner.get_mut();
        if inner.closed {
            return Err(KafkaConsumerError::ConsumerClosed);
        }

        let topic_meta = self
            .broker
            .describe_topic(topic)
            .ok_or_else(|| KafkaConsumerError::TopicNotFound(topic.to_string()))?;

        // Initialise offsets for each partition.
        for pid in 0..topic_meta.num_partitions {
            let offset = match self.config.auto_offset_reset {
                AutoOffsetReset::Earliest => {
                    let (earliest, _) = self
                        .broker
                        .get_offsets(topic, pid)
                        .map_err(|e| KafkaConsumerError::ConnectionFailed(e.to_string()))?;
                    earliest as u64
                }
                AutoOffsetReset::Latest => {
                    let (_, latest) = self
                        .broker
                        .get_offsets(topic, pid)
                        .map_err(|e| KafkaConsumerError::ConnectionFailed(e.to_string()))?;
                    latest as u64
                }
            };
            inner.offsets.insert(pid, offset);
        }

        inner.subscribed_topic = Some(topic.to_string());
        Ok(())
    }

    /// Poll the broker for new records across all assigned partitions.
    ///
    /// Returns up to `max_poll_records` records. If the consumer is paused
    /// or closed, returns an error.
    pub fn poll(&self, timeout: Duration) -> Result<Vec<ConsumerRecord>, KafkaConsumerError> {
        let mut inner = self.inner.write();
        if inner.closed {
            return Err(KafkaConsumerError::ConsumerClosed);
        }
        if inner.paused {
            return Ok(Vec::new());
        }

        let topic = inner
            .subscribed_topic
            .clone()
            .ok_or_else(|| KafkaConsumerError::TopicNotFound("no subscription".to_string()))?;

        let topic_meta = self
            .broker
            .describe_topic(&topic)
            .ok_or_else(|| KafkaConsumerError::TopicNotFound(topic.clone()))?;

        let max = self.config.max_poll_records;
        let mut records = Vec::new();
        let per_partition = (max / topic_meta.num_partitions as usize).max(1);

        for pid in 0..topic_meta.num_partitions {
            let current_offset = inner.offsets.get(&pid).copied().unwrap_or(0);
            let fetched = self
                .broker
                .fetch(&topic, pid, current_offset as i64, per_partition)
                .map_err(|e| {
                    KafkaConsumerError::ConnectionFailed(format!(
                        "fetch partition {pid} failed: {e}"
                    ))
                })?;

            if let Some(last) = fetched.last() {
                inner.offsets.insert(pid, (last.offset + 1) as u64);
            }

            records.extend(fetched.into_iter().map(ConsumerRecord::from));

            if records.len() >= max {
                break;
            }
        }

        // Simulate timeout semantics: if no records were found and the
        // caller specified a non-zero timeout, we'd normally block.  In
        // this in-memory implementation we simply return the (possibly
        // empty) result immediately — the `timeout` is recorded for API
        // compatibility.
        let _ = timeout;

        // Auto-commit if enabled.
        if self.config.enable_auto_commit && !records.is_empty() {
            for (&pid, &off) in &inner.offsets {
                let _ = self.broker.commit_offset(
                    &self.config.group_id,
                    topic.clone(),
                    pid,
                    off as i64,
                );
            }
        }

        Ok(records)
    }

    /// Manually commit all current offsets to the broker.
    pub fn commit(&self) -> Result<(), KafkaConsumerError> {
        let inner = self.inner.read();
        if inner.closed {
            return Err(KafkaConsumerError::ConsumerClosed);
        }

        let topic = inner
            .subscribed_topic
            .as_ref()
            .ok_or_else(|| KafkaConsumerError::CommitFailed("no subscription".to_string()))?;

        for (&pid, &off) in &inner.offsets {
            self.broker
                .commit_offset(&self.config.group_id, topic.clone(), pid, off as i64)
                .map_err(|e| KafkaConsumerError::CommitFailed(e.to_string()))?;
        }

        Ok(())
    }

    /// Commit a specific offset for a single partition.
    pub fn commit_offset(
        &self,
        partition: u32,
        offset: u64,
    ) -> Result<(), KafkaConsumerError> {
        let inner = self.inner.read();
        if inner.closed {
            return Err(KafkaConsumerError::ConsumerClosed);
        }

        let topic = inner
            .subscribed_topic
            .as_ref()
            .ok_or_else(|| KafkaConsumerError::CommitFailed("no subscription".to_string()))?;

        self.broker
            .commit_offset(&self.config.group_id, topic.clone(), partition, offset as i64)
            .map_err(|e| KafkaConsumerError::CommitFailed(e.to_string()))?;

        Ok(())
    }

    /// Get the current tracked offset for a partition.
    pub fn get_offset(&self, partition: u32) -> Option<u64> {
        self.inner.read().offsets.get(&partition).copied()
    }

    /// Pause consumption. Subsequent polls return empty results.
    pub fn pause(&self) -> Result<(), KafkaConsumerError> {
        let mut inner = self.inner.write();
        if inner.closed {
            return Err(KafkaConsumerError::ConsumerClosed);
        }
        inner.paused = true;
        Ok(())
    }

    /// Resume consumption after a pause.
    pub fn resume(&self) -> Result<(), KafkaConsumerError> {
        let mut inner = self.inner.write();
        if inner.closed {
            return Err(KafkaConsumerError::ConsumerClosed);
        }
        inner.paused = false;
        Ok(())
    }

    /// Close the consumer. All subsequent operations will return
    /// [`KafkaConsumerError::ConsumerClosed`].
    pub fn close(&self) -> Result<(), KafkaConsumerError> {
        let mut inner = self.inner.write();
        if inner.closed {
            return Err(KafkaConsumerError::ConsumerClosed);
        }
        // Commit outstanding offsets before closing.
        if let Some(topic) = &inner.subscribed_topic {
            for (&pid, &off) in &inner.offsets {
                let _ = self.broker.commit_offset(
                    &self.config.group_id,
                    topic.clone(),
                    pid,
                    off as i64,
                );
            }
        }
        inner.closed = true;
        Ok(())
    }

    /// Returns a reference to the underlying broker (for testing).
    pub fn broker(&self) -> &Arc<StreamingBroker> {
        &self.broker
    }
}

// ---------------------------------------------------------------------------
// Record deserializer trait
// ---------------------------------------------------------------------------

/// Deserializes a [`ConsumerRecord`] into an [`IngestDocument`].
pub trait RecordDeserializer: Send + Sync {
    /// Attempt to convert a raw consumer record into an ingest document.
    fn deserialize(&self, record: &ConsumerRecord) -> Result<IngestDocument, KafkaConsumerError>;
}

/// JSON-based deserializer that extracts `id` and `text` fields from the
/// record value and treats remaining top-level keys as metadata.
pub struct JsonDeserializer;

impl RecordDeserializer for JsonDeserializer {
    fn deserialize(&self, record: &ConsumerRecord) -> Result<IngestDocument, KafkaConsumerError> {
        let parsed: serde_json::Value =
            serde_json::from_slice(&record.value).map_err(|e| {
                KafkaConsumerError::DeserializationError(format!("invalid JSON: {e}"))
            })?;

        let obj = parsed.as_object().ok_or_else(|| {
            KafkaConsumerError::DeserializationError("expected JSON object".to_string())
        })?;

        let id = obj
            .get("id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("{}-{}-{}", record.topic, record.partition, record.offset));

        let text = obj
            .get("text")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                KafkaConsumerError::DeserializationError(
                    "missing required field 'text'".to_string(),
                )
            })?
            .to_string();

        let metadata: HashMap<String, serde_json::Value> = obj
            .iter()
            .filter(|(k, _)| k.as_str() != "id" && k.as_str() != "text")
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        Ok(IngestDocument { id, text, metadata })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::record::ProducerRecord;

    fn test_config() -> KafkaConsumerConfig {
        KafkaConsumerConfig {
            brokers: vec!["localhost:9092".to_string()],
            topic: "test-topic".to_string(),
            group_id: "test-group".to_string(),
            ..Default::default()
        }
    }

    /// Helper: create a broker with a topic and produce some records.
    fn broker_with_records(topic: &str, count: usize) -> Arc<StreamingBroker> {
        let broker = Arc::new(StreamingBroker::new());
        broker
            .create_topic(topic.to_string(), 2, 1, -1)
            .unwrap();

        for i in 0..count {
            let value = format!(r#"{{"id":"doc-{i}","text":"hello {i}","tag":"v{i}"}}"#);
            broker
                .produce(ProducerRecord {
                    topic: topic.to_string(),
                    partition: Some((i % 2) as u32),
                    key: Some(format!("key-{i}").into_bytes()),
                    value: value.into_bytes(),
                    headers: vec![],
                    timestamp: Some(1_700_000_000_000 + i as i64),
                })
                .unwrap();
        }
        broker
    }

    #[test]
    fn test_consumer_creation() {
        let consumer = KafkaConsumer::new(test_config());
        assert!(consumer.is_ok());
    }

    #[test]
    fn test_consumer_no_brokers() {
        let config = KafkaConsumerConfig {
            brokers: vec![],
            ..test_config()
        };
        let result = KafkaConsumer::new(config);
        assert!(matches!(
            result,
            Err(KafkaConsumerError::ConnectionFailed(_))
        ));
    }

    #[test]
    fn test_subscribe_and_poll() {
        let broker = broker_with_records("test-topic", 4);
        let config = test_config();
        let mut consumer = KafkaConsumer::with_broker(config, broker).unwrap();

        consumer.subscribe("test-topic").unwrap();
        let records = consumer
            .poll(Duration::from_millis(100))
            .unwrap();

        assert_eq!(records.len(), 4);
    }

    #[test]
    fn test_subscribe_missing_topic() {
        let broker = Arc::new(StreamingBroker::new());
        let mut consumer = KafkaConsumer::with_broker(test_config(), broker).unwrap();
        let result = consumer.subscribe("no-such-topic");
        assert!(matches!(
            result,
            Err(KafkaConsumerError::TopicNotFound(_))
        ));
    }

    #[test]
    fn test_poll_advances_offset() {
        let broker = broker_with_records("test-topic", 6);
        let config = KafkaConsumerConfig {
            max_poll_records: 4,
            ..test_config()
        };
        let mut consumer = KafkaConsumer::with_broker(config, broker).unwrap();
        consumer.subscribe("test-topic").unwrap();

        let first = consumer.poll(Duration::from_millis(100)).unwrap();
        let second = consumer.poll(Duration::from_millis(100)).unwrap();

        assert_eq!(first.len() + second.len(), 6);
    }

    #[test]
    fn test_pause_resume() {
        let broker = broker_with_records("test-topic", 2);
        let mut consumer = KafkaConsumer::with_broker(test_config(), broker).unwrap();
        consumer.subscribe("test-topic").unwrap();

        consumer.pause().unwrap();
        let records = consumer.poll(Duration::from_millis(100)).unwrap();
        assert!(records.is_empty());

        consumer.resume().unwrap();
        let records = consumer.poll(Duration::from_millis(100)).unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn test_commit_and_get_offset() {
        let broker = broker_with_records("test-topic", 2);
        let mut consumer = KafkaConsumer::with_broker(test_config(), broker).unwrap();
        consumer.subscribe("test-topic").unwrap();

        consumer.poll(Duration::from_millis(100)).unwrap();
        consumer.commit().unwrap();

        // Offsets should reflect the next-to-read position.
        assert!(consumer.get_offset(0).is_some());
    }

    #[test]
    fn test_commit_offset_single_partition() {
        let broker = broker_with_records("test-topic", 2);
        let mut consumer = KafkaConsumer::with_broker(test_config(), broker).unwrap();
        consumer.subscribe("test-topic").unwrap();

        consumer.commit_offset(0, 42).unwrap();
    }

    #[test]
    fn test_close() {
        let broker = broker_with_records("test-topic", 2);
        let mut consumer = KafkaConsumer::with_broker(test_config(), broker).unwrap();
        consumer.subscribe("test-topic").unwrap();

        consumer.close().unwrap();

        assert!(matches!(
            consumer.poll(Duration::from_millis(100)),
            Err(KafkaConsumerError::ConsumerClosed)
        ));
        assert!(matches!(
            consumer.commit(),
            Err(KafkaConsumerError::ConsumerClosed)
        ));
        assert!(matches!(
            consumer.close(),
            Err(KafkaConsumerError::ConsumerClosed)
        ));
    }

    #[test]
    fn test_json_deserializer() {
        let deser = JsonDeserializer;
        let record = ConsumerRecord {
            topic: "t".to_string(),
            partition: 0,
            offset: 5,
            key: None,
            value: br#"{"id":"doc-1","text":"hello world","category":"test"}"#.to_vec(),
            timestamp: 1_700_000_000_000,
            headers: HashMap::new(),
        };

        let doc = deser.deserialize(&record).unwrap();
        assert_eq!(doc.id, "doc-1");
        assert_eq!(doc.text, "hello world");
        assert_eq!(
            doc.metadata.get("category"),
            Some(&serde_json::Value::String("test".to_string()))
        );
        // id and text should not appear in metadata
        assert!(!doc.metadata.contains_key("id"));
        assert!(!doc.metadata.contains_key("text"));
    }

    #[test]
    fn test_json_deserializer_missing_text() {
        let deser = JsonDeserializer;
        let record = ConsumerRecord {
            topic: "t".to_string(),
            partition: 0,
            offset: 0,
            key: None,
            value: br#"{"id":"doc-1"}"#.to_vec(),
            timestamp: 0,
            headers: HashMap::new(),
        };

        let result = deser.deserialize(&record);
        assert!(matches!(
            result,
            Err(KafkaConsumerError::DeserializationError(_))
        ));
    }

    #[test]
    fn test_json_deserializer_generates_id() {
        let deser = JsonDeserializer;
        let record = ConsumerRecord {
            topic: "events".to_string(),
            partition: 3,
            offset: 42,
            key: None,
            value: br#"{"text":"no id here"}"#.to_vec(),
            timestamp: 0,
            headers: HashMap::new(),
        };

        let doc = deser.deserialize(&record).unwrap();
        assert_eq!(doc.id, "events-3-42");
    }

    #[test]
    fn test_consumer_record_from_kafka_record() {
        let kafka_record = KafkaConsumerRecord {
            topic: "t".to_string(),
            partition: 1,
            offset: 10,
            key: Some(b"k".to_vec()),
            value: b"v".to_vec(),
            headers: vec![("h1".to_string(), b"hv".to_vec())],
            timestamp: 1_700_000_000_000,
        };

        let record: ConsumerRecord = kafka_record.into();
        assert_eq!(record.partition, 1);
        assert_eq!(record.offset, 10);
        assert_eq!(record.headers.get("h1"), Some(&b"hv".to_vec()));
    }
}
