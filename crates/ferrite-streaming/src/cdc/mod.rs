//! # Native Change Data Capture (CDC)
//!
//! First-class event streaming for data changes, enabling real-time integrations,
//! event-driven architectures, and data synchronization without custom application logic.
//!
//! ## Why CDC?
//!
//! Traditional change tracking requires polling or application-level code. Ferrite's
//! native CDC provides:
//!
//! - **Zero application changes**: Capture all writes automatically
//! - **Event replay**: Rebuild state from any point in time
//! - **Real-time streaming**: Push changes to downstream systems instantly
//! - **Multiple sinks**: Kafka, Kinesis, PubSub, HTTP webhooks, S3
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
//! │   Client    │────▶│   Ferrite   │────▶│  Change Log │
//! │  SET/DEL    │     │   Engine    │     │  (Durable)  │
//! └─────────────┘     └─────────────┘     └──────┬──────┘
//!                                                │
//!                     ┌──────────────────────────┼──────────────────────────┐
//!                     │                          │                          │
//!               ┌─────▼─────┐              ┌─────▼─────┐              ┌─────▼─────┐
//!               │   Kafka   │              │  Webhook  │              │    S3     │
//!               │   Sink    │              │   Sink    │              │   Sink    │
//!               └───────────┘              └───────────┘              └───────────┘
//! ```
//!
//! ## Quick Start
//!
//! ### Subscribe to Changes
//!
//! ```no_run
//! use ferrite::cdc::{CdcEngine, SubscribeOptions, OutputFormat, DeliveryMode};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let engine = CdcEngine::default();
//!
//!     // Subscribe to user changes with pattern matching
//!     let sub_id = engine.subscribe(
//!         "user_changes",
//!         vec!["user:*".to_string()],
//!         SubscribeOptions {
//!             format: OutputFormat::Json,
//!             mode: DeliveryMode::Push,
//!             include_old_value: true,
//!             ..Default::default()
//!         },
//!     ).await?;
//!
//!     println!("Subscription created: {}", sub_id);
//!     Ok(())
//! }
//! ```
//!
//! ### Configure Kafka Sink
//!
//! ```no_run
//! use ferrite::cdc::{SinkManager, SinkConfig, SinkType, KafkaSinkConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let sink_manager = SinkManager::new();
//!
//! // Stream order changes to Kafka
//! sink_manager.create_sink(SinkConfig {
//!     name: "order_events".to_string(),
//!     sink_type: SinkType::Kafka(KafkaSinkConfig {
//!         brokers: vec!["kafka:9092".to_string()],
//!         topic: "ferrite.orders".to_string(),
//!         batch_size: 100,
//!         linger_ms: 10,
//!         ..Default::default()
//!     }),
//!     patterns: vec!["order:*".to_string()],
//!     operations: vec!["SET".to_string(), "DEL".to_string()],
//!     ..Default::default()
//! }).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Configure HTTP Webhook
//!
//! ```no_run
//! use ferrite::cdc::{SinkManager, SinkConfig, SinkType, HttpSinkConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let sink_manager = SinkManager::new();
//!
//! // Send payment events to webhook
//! sink_manager.create_sink(SinkConfig {
//!     name: "payment_webhook".to_string(),
//!     sink_type: SinkType::Http(HttpSinkConfig {
//!         url: "https://api.example.com/webhooks/payments".to_string(),
//!         headers: vec![
//!             ("Authorization".to_string(), "Bearer secret123".to_string()),
//!             ("Content-Type".to_string(), "application/json".to_string()),
//!         ],
//!         timeout_ms: 5000,
//!         retry_count: 3,
//!         ..Default::default()
//!     }),
//!     patterns: vec!["payment:*".to_string()],
//!     ..Default::default()
//! }).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Redis CLI Commands
//!
//! ```text
//! # Subscribe to user changes
//! CDC.SUBSCRIBE user_changes PATTERN "user:*" OPERATIONS SET DEL
//!
//! # Read changes (pull mode)
//! CDC.READ user_changes COUNT 100
//!
//! # Start push streaming
//! CDC.PUSH user_changes
//!
//! # List subscriptions
//! CDC.LIST
//!
//! # Get subscription info
//! CDC.INFO user_changes
//!
//! # Unsubscribe
//! CDC.UNSUBSCRIBE user_changes
//! ```
//!
//! ## Event Structure
//!
//! Each change event contains:
//!
//! ```json
//! {
//!   "id": 12345,
//!   "timestamp": "2024-01-15T10:30:00Z",
//!   "db": 0,
//!   "operation": "SET",
//!   "key": "user:123",
//!   "value": "{\"name\": \"Alice\"}",
//!   "old_value": null,
//!   "metadata": {
//!     "source": "client",
//!     "client_id": "conn:456",
//!     "ttl": 3600
//!   }
//! }
//! ```
//!
//! ## Output Formats
//!
//! | Format | Use Case | Size | Notes |
//! |--------|----------|------|-------|
//! | JSON | Debugging, webhooks | Large | Human readable |
//! | Avro | Kafka, analytics | Small | Schema registry support |
//! | Protobuf | High throughput | Smallest | Requires schema |
//! | RESP3 | Redis clients | Medium | Native format |
//!
//! ## Performance
//!
//! | Metric | Value | Notes |
//! |--------|-------|-------|
//! | Capture overhead | <5μs | Per write operation |
//! | Throughput | 500K events/s | Single core |
//! | Latency (push) | <1ms | To subscriber |
//! | Log retention | Configurable | Default 7 days |
//!
//! ## Best Practices
//!
//! 1. **Use pattern filters**: Narrow subscriptions to reduce event volume
//! 2. **Enable batching**: For high-throughput sinks (Kafka, S3)
//! 3. **Set retention policies**: Prevent unbounded log growth
//! 4. **Monitor lag**: Track subscriber position vs log head
//! 5. **Use Avro/Protobuf**: For bandwidth-sensitive deployments

pub mod backpressure;
pub mod capture;
pub mod consumer_group;
mod engine;
pub mod exactly_once;
mod event;
pub mod kafka_bridge;
mod log;
mod sink;
pub mod streaming_metrics;
mod subscription;

pub use backpressure::{
    BackpressureConfig, BackpressureController, BackpressureError, BackpressureStats,
    OverflowPolicy, RateLimiter,
};
pub use capture::{CdcCaptureManager, CdcCheckpoint, CdcCheckpointStore, CdcFilter};
pub use consumer_group::{
    ConsumerGroup, ConsumerGroupConfig, ConsumerGroupId, ConsumerGroupInfo,
    ConsumerGroupJoinResult, ConsumerGroupManager, ConsumerId, ConsumerMember,
    PartitionAssignmentStrategy, PendingEntry, PendingEntryList, StartingOffset,
};
pub use engine::{CdcEngine, CdcInfo, CdcStats, SubscribeOptions};
pub use event::{ChangeEvent, ChangeMetadata, ChangeSource, Operation};
pub use exactly_once::{
    ConsumerOffsetStore, ExactlyOnceCoordinator, ExactlyOnceError, IdempotentProducer,
    TransactionEvent, TransactionState, TransactionalWriter,
};
pub use kafka_bridge::{
    BridgeError, BridgeStats, KafkaBridge, KafkaBridgeConfig, SchemaFormat, TopicMapping,
};
pub use log::{ChangeLog, ChangeLogConfig, Segment, SegmentId};
pub use sink::{
    HttpSinkConfig, KafkaSinkConfig, KinesisSinkConfig, PubSubSinkConfig, S3SinkConfig, Sink,
    SinkConfig, SinkManager, SinkStatus, SinkType,
};
pub use streaming_metrics::{MetricsSnapshot, StreamingMetrics};
pub use subscription::{
    DeliveryMode, OutputFormat, Subscription, SubscriptionId, SubscriptionManager,
    SubscriptionState,
};

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_operation_display() {
        assert_eq!(Operation::Set.name(), "SET");
        assert_eq!(Operation::Del.name(), "DEL");
        assert_eq!(
            Operation::HSet {
                field: Bytes::from_static(b"name")
            }
            .name(),
            "HSET"
        );
    }

    #[test]
    fn test_change_event_serialization() {
        let event = ChangeEvent {
            id: 1,
            timestamp: std::time::SystemTime::now(),
            db: 0,
            operation: Operation::Set,
            key: Bytes::from_static(b"test:key"),
            value: Some(Bytes::from_static(b"value")),
            old_value: None,
            metadata: ChangeMetadata::default(),
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"key\""));
        // Operation::Set serializes as "Set" (default serde naming)
        assert!(json.contains("Set"));
    }

    #[test]
    fn test_subscription_pattern_matching() {
        let sub = Subscription::new("test", vec!["user:*".to_string()]);
        assert!(sub.matches_key(b"user:123"));
        assert!(!sub.matches_key(b"order:456"));
    }

    #[tokio::test]
    async fn test_cdc_engine_basic() {
        let engine = CdcEngine::default();

        // Create a subscription
        let sub_id = engine
            .subscribe("test", vec!["*".to_string()], Default::default())
            .await;
        assert!(sub_id.is_ok());

        // Capture a change
        let event = ChangeEvent {
            id: 0,
            timestamp: std::time::SystemTime::now(),
            db: 0,
            operation: Operation::Set,
            key: Bytes::from_static(b"key1"),
            value: Some(Bytes::from_static(b"value1")),
            old_value: None,
            metadata: ChangeMetadata::default(),
        };

        engine.capture(event).await;

        // Check info
        let info = engine.info().await;
        assert!(info.enabled);
        assert_eq!(info.subscriptions, 1);
    }
}
