//! Stream Processing Engine - Kafka Streams-compatible streaming data processing
//!
//! This module provides a full-featured stream processing engine supporting:
//! - Real-time event processing
//! - Windowing (tumbling, sliding, session, global)
//! - Stream-table joins
//! - Exactly-once processing semantics
//! - State management with changelog
//! - Complex event processing (CEP)
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                   Stream Processing Engine                   │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Sources          Operators           Sinks                  │
//! │  ┌────────┐      ┌──────────┐       ┌────────┐              │
//! │  │ Kafka  │─────►│ Filter   │──────►│ Kafka  │              │
//! │  │ Redis  │      │ Map      │       │ Redis  │              │
//! │  │ File   │      │ FlatMap  │       │ File   │              │
//! │  │ HTTP   │      │ Aggregate│       │ HTTP   │              │
//! │  └────────┘      │ Join     │       └────────┘              │
//! │                  │ Window   │                                │
//! │                  └──────────┘                                │
//! ├─────────────────────────────────────────────────────────────┤
//! │                    State Store                               │
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
//! │  │  Key-Value  │  │   Window    │  │   Session   │         │
//! │  │    Store    │  │    Store    │  │    Store    │         │
//! │  └─────────────┘  └─────────────┘  └─────────────┘         │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use ferrite::streaming::{StreamBuilder, WindowType};
//!
//! let pipeline = StreamBuilder::new("click-analytics")
//!     .source(RedisSource::new("clicks"))
//!     .filter(|event| event.get("type") == "click")
//!     .map(|event| {
//!         let page = event.get("page").expect("page field required");
//!         (page, 1)
//!     })
//!     .window(WindowType::Tumbling(Duration::from_secs(60)))
//!     .aggregate(|a, b| a + b)
//!     .sink(RedisSink::new("click-counts"))
//!     .build();
//!
//! pipeline.run().await?;
//! ```

#![allow(dead_code)]
pub mod cep;
pub mod event_bus;
pub mod operator;
pub mod pipeline;
pub mod replication;
pub mod sink;
pub mod source;
pub mod state;
pub mod watermark;
pub mod window;

pub use event_bus::{DataEvent, EventBus, EventBusConfig, EventFilter, FunctionChain};
pub use operator::{Operator, OperatorType};
pub use pipeline::{Pipeline, PipelineBuilder};
pub use sink::{Sink, SinkType};
pub use source::{Source, SourceType};
pub use state::{StateBackend, StateStore};
pub use watermark::{Watermark, WatermarkGenerator};
pub use window::{Window, WindowAssigner, WindowType};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

/// Stream event
#[derive(Debug, Clone)]
pub struct StreamEvent {
    /// Event key
    pub key: Option<String>,
    /// Event value
    pub value: serde_json::Value,
    /// Event timestamp
    pub timestamp: u64,
    /// Event headers
    pub headers: HashMap<String, String>,
    /// Partition (for distributed processing)
    pub partition: Option<u32>,
    /// Offset (for exactly-once semantics)
    pub offset: Option<u64>,
}

impl StreamEvent {
    /// Create a new event
    pub fn new(key: Option<String>, value: serde_json::Value) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            key,
            value,
            timestamp,
            headers: HashMap::new(),
            partition: None,
            offset: None,
        }
    }

    /// Create an event with timestamp
    pub fn with_timestamp(key: Option<String>, value: serde_json::Value, timestamp: u64) -> Self {
        Self {
            key,
            value,
            timestamp,
            headers: HashMap::new(),
            partition: None,
            offset: None,
        }
    }

    /// Add a header
    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(key.to_string(), value.to_string());
        self
    }

    /// Set partition
    pub fn with_partition(mut self, partition: u32) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Set offset
    pub fn with_offset(mut self, offset: u64) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Get a field from the value
    pub fn get(&self, field: &str) -> Option<&serde_json::Value> {
        self.value.get(field)
    }

    /// Get a string field
    pub fn get_str(&self, field: &str) -> Option<&str> {
        self.value.get(field).and_then(|v| v.as_str())
    }

    /// Get a number field
    pub fn get_i64(&self, field: &str) -> Option<i64> {
        self.value.get(field).and_then(|v| v.as_i64())
    }
}

/// Stream processing configuration
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// Application ID
    pub application_id: String,
    /// Number of processing threads
    pub num_threads: usize,
    /// Commit interval for offsets
    pub commit_interval: Duration,
    /// State directory
    pub state_dir: String,
    /// Processing guarantee
    pub processing_guarantee: ProcessingGuarantee,
    /// Default timestamp extractor
    pub timestamp_extractor: TimestampExtractor,
    /// Buffering configuration
    pub buffering: BufferingConfig,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            application_id: "ferrite-stream".to_string(),
            num_threads: num_cpus::get(),
            commit_interval: Duration::from_secs(1),
            state_dir: "/tmp/ferrite-stream".to_string(),
            processing_guarantee: ProcessingGuarantee::AtLeastOnce,
            timestamp_extractor: TimestampExtractor::WallClock,
            buffering: BufferingConfig::default(),
        }
    }
}

/// Processing guarantee levels
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessingGuarantee {
    /// At most once (may lose data)
    AtMostOnce,
    /// At least once (may duplicate data)
    AtLeastOnce,
    /// Exactly once (no data loss or duplicates)
    ExactlyOnce,
}

/// Timestamp extraction strategy
#[derive(Clone)]
pub enum TimestampExtractor {
    /// Use wall clock time
    WallClock,
    /// Extract from event field
    EventField(String),
    /// Custom extractor function
    Custom(Arc<dyn Fn(&StreamEvent) -> u64 + Send + Sync>),
}

impl std::fmt::Debug for TimestampExtractor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimestampExtractor::WallClock => write!(f, "WallClock"),
            TimestampExtractor::EventField(field) => write!(f, "EventField({})", field),
            TimestampExtractor::Custom(_) => write!(f, "Custom(<fn>)"),
        }
    }
}

/// Buffering configuration
#[derive(Debug, Clone)]
pub struct BufferingConfig {
    /// Maximum buffer size per partition
    pub max_buffer_size: usize,
    /// Buffer timeout
    pub buffer_timeout: Duration,
    /// Enable batching
    pub enable_batching: bool,
    /// Batch size
    pub batch_size: usize,
}

impl Default for BufferingConfig {
    fn default() -> Self {
        Self {
            max_buffer_size: 10000,
            buffer_timeout: Duration::from_millis(100),
            enable_batching: true,
            batch_size: 100,
        }
    }
}

/// Stream processing metrics
#[derive(Debug, Default)]
pub struct StreamMetrics {
    /// Events received
    pub events_received: u64,
    /// Events processed
    pub events_processed: u64,
    /// Events emitted
    pub events_emitted: u64,
    /// Events failed
    pub events_failed: u64,
    /// Processing latency (ms)
    pub processing_latency_ms: f64,
    /// Bytes received
    pub bytes_received: u64,
    /// Bytes emitted
    pub bytes_emitted: u64,
    /// State store size
    pub state_store_size: u64,
    /// Current watermark
    pub current_watermark: u64,
    /// Lag (events behind)
    pub lag: u64,
}

/// Main stream processing engine
pub struct StreamEngine {
    /// Configuration
    config: StreamConfig,
    /// Pipelines
    pipelines: Arc<RwLock<HashMap<String, Pipeline>>>,
    /// Metrics
    metrics: Arc<RwLock<StreamMetrics>>,
    /// Shutdown signal
    shutdown: Arc<RwLock<bool>>,
}

impl StreamEngine {
    /// Create a new stream engine
    pub fn new(config: StreamConfig) -> Self {
        Self {
            config,
            pipelines: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(RwLock::new(StreamMetrics::default())),
            shutdown: Arc::new(RwLock::new(false)),
        }
    }

    /// Register a pipeline
    pub async fn register_pipeline(&self, pipeline: Pipeline) -> Result<(), StreamError> {
        let mut pipelines = self.pipelines.write().await;
        let name = pipeline.name().to_string();

        if pipelines.contains_key(&name) {
            return Err(StreamError::PipelineExists(name));
        }

        pipelines.insert(name, pipeline);
        Ok(())
    }

    /// Remove a pipeline
    pub async fn remove_pipeline(&self, name: &str) -> Result<(), StreamError> {
        let mut pipelines = self.pipelines.write().await;

        if pipelines.remove(name).is_none() {
            return Err(StreamError::PipelineNotFound(name.to_string()));
        }

        Ok(())
    }

    /// Start all pipelines
    pub async fn start(&self) -> Result<(), StreamError> {
        let pipelines = self.pipelines.read().await;

        for (name, pipeline) in pipelines.iter() {
            tracing::info!("Starting pipeline: {}", name);
            pipeline.start().await?;
        }

        Ok(())
    }

    /// Stop all pipelines
    pub async fn stop(&self) -> Result<(), StreamError> {
        *self.shutdown.write().await = true;

        let pipelines = self.pipelines.read().await;

        for (name, pipeline) in pipelines.iter() {
            tracing::info!("Stopping pipeline: {}", name);
            pipeline.stop().await?;
        }

        Ok(())
    }

    /// Get metrics
    pub async fn metrics(&self) -> StreamMetrics {
        let metrics = self.metrics.read().await;
        StreamMetrics {
            events_received: metrics.events_received,
            events_processed: metrics.events_processed,
            events_emitted: metrics.events_emitted,
            events_failed: metrics.events_failed,
            processing_latency_ms: metrics.processing_latency_ms,
            bytes_received: metrics.bytes_received,
            bytes_emitted: metrics.bytes_emitted,
            state_store_size: metrics.state_store_size,
            current_watermark: metrics.current_watermark,
            lag: metrics.lag,
        }
    }

    /// Create a new pipeline builder
    pub fn builder(&self, name: &str) -> PipelineBuilder {
        PipelineBuilder::new(name.to_string(), self.config.clone())
    }
}

/// Stream builder for fluent API
pub struct StreamBuilder {
    name: String,
    source: Option<Box<dyn Source>>,
    operators: Vec<Box<dyn Operator<String, serde_json::Value>>>,
    sink: Option<Box<dyn Sink>>,
    config: StreamConfig,
}

impl StreamBuilder {
    /// Create a new stream builder
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            source: None,
            operators: Vec::new(),
            sink: None,
            config: StreamConfig::default(),
        }
    }

    /// Set configuration
    pub fn config(mut self, config: StreamConfig) -> Self {
        self.config = config;
        self
    }

    /// Add a filter operator
    pub fn filter<F>(mut self, predicate: F) -> Self
    where
        F: Fn(&StreamEvent) -> bool + Send + Sync + 'static,
    {
        let op = operator::FilterOperator::new(predicate);
        self.operators.push(Box::new(op));
        self
    }

    /// Add a map operator
    pub fn map<F>(mut self, mapper: F) -> Self
    where
        F: Fn(StreamEvent) -> StreamEvent + Send + Sync + 'static,
    {
        let op = operator::MapOperator::new(mapper);
        self.operators.push(Box::new(op));
        self
    }

    /// Add a flat map operator
    pub fn flat_map<F>(mut self, mapper: F) -> Self
    where
        F: Fn(StreamEvent) -> Vec<StreamEvent> + Send + Sync + 'static,
    {
        let op = operator::FlatMapOperator::new(mapper);
        self.operators.push(Box::new(op));
        self
    }
}

/// Stream processing errors
#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    /// Pipeline already exists
    #[error("Pipeline already exists: {0}")]
    PipelineExists(String),

    /// Pipeline not found
    #[error("Pipeline not found: {0}")]
    PipelineNotFound(String),

    /// Source error
    #[error("Source error: {0}")]
    SourceError(String),

    /// Sink error
    #[error("Sink error: {0}")]
    SinkError(String),

    /// Operator error
    #[error("Operator error: {0}")]
    OperatorError(String),

    /// State error
    #[error("State error: {0}")]
    StateError(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Timeout error
    #[error("Timeout error: {0}")]
    TimeoutError(String),

    /// Channel closed
    #[error("Channel closed")]
    ChannelClosed,

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),
}

/// Helper function to get number of CPUs
mod num_cpus {
    pub fn get() -> usize {
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_stream_event_creation() {
        let event = StreamEvent::new(
            Some("user-123".to_string()),
            json!({ "action": "click", "page": "/home" }),
        );

        assert_eq!(event.key, Some("user-123".to_string()));
        assert_eq!(event.get_str("action"), Some("click"));
        assert!(event.timestamp > 0);
    }

    #[test]
    fn test_stream_event_with_headers() {
        let event = StreamEvent::new(None, json!({ "data": "test" }))
            .with_header("content-type", "application/json")
            .with_partition(0)
            .with_offset(100);

        assert_eq!(
            event.headers.get("content-type"),
            Some(&"application/json".to_string())
        );
        assert_eq!(event.partition, Some(0));
        assert_eq!(event.offset, Some(100));
    }

    #[tokio::test]
    async fn test_stream_engine() {
        let config = StreamConfig::default();
        let engine = StreamEngine::new(config);

        let metrics = engine.metrics().await;
        assert_eq!(metrics.events_processed, 0);
    }
}
