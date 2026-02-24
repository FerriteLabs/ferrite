#![forbid(unsafe_code)]
//! Kafka-compatible record types.

use serde::{Deserialize, Serialize};

/// A record sent by a producer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerRecord {
    /// Target topic name.
    pub topic: String,
    /// Optional target partition (broker assigns one when `None`).
    pub partition: Option<u32>,
    /// Optional record key (used for partitioning).
    pub key: Option<Vec<u8>>,
    /// Record payload.
    pub value: Vec<u8>,
    /// Application-level headers.
    pub headers: Vec<(String, Vec<u8>)>,
    /// Optional producer-supplied timestamp (epoch millis).
    pub timestamp: Option<i64>,
}

/// A record returned to a consumer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerRecord {
    /// Source topic name.
    pub topic: String,
    /// Partition the record belongs to.
    pub partition: u32,
    /// Offset within the partition.
    pub offset: i64,
    /// Optional record key.
    pub key: Option<Vec<u8>>,
    /// Record payload.
    pub value: Vec<u8>,
    /// Application-level headers.
    pub headers: Vec<(String, Vec<u8>)>,
    /// Broker-assigned timestamp (epoch millis).
    pub timestamp: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_producer_record_defaults() {
        let record = ProducerRecord {
            topic: "events".into(),
            partition: None,
            key: Some(b"user-1".to_vec()),
            value: b"hello".to_vec(),
            headers: vec![("trace-id".into(), b"abc".to_vec())],
            timestamp: None,
        };
        assert_eq!(record.topic, "events");
        assert!(record.partition.is_none());
        assert_eq!(record.headers.len(), 1);
    }

    #[test]
    fn test_consumer_record() {
        let record = ConsumerRecord {
            topic: "events".into(),
            partition: 0,
            offset: 42,
            key: None,
            value: b"world".to_vec(),
            headers: vec![],
            timestamp: 1_700_000_000_000,
        };
        assert_eq!(record.offset, 42);
        assert_eq!(record.partition, 0);
    }
}
