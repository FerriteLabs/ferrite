#![forbid(unsafe_code)]
//! Kafka-compatible streaming engine.
//!
//! Provides topic management, partitioned append-only logs, producer/consumer
//! record types, consumer group offset tracking, and a unified [`StreamingBroker`]
//! entry point.

pub mod broker;
pub mod consumer_group;
pub mod partition_log;
pub mod record;
pub mod topic;

pub use broker::{BrokerConfig, BrokerError, BrokerStats, StreamingBroker};
pub use consumer_group::{ConsumerGroup, GroupMember, GroupState};
pub use partition_log::PartitionLog;
pub use record::{ConsumerRecord, ProducerRecord};
pub use topic::{CleanupPolicy, CompressionType, Partition, RetentionPolicy, Topic, TopicConfig};
