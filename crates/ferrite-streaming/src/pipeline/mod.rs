//! Native Streaming Pipelines
//!
//! Built-in stream processing engine with map, filter, window, and join
//! operators for processing data changes in-place without external infrastructure.
//! Supports exactly-once semantics and stateful processing.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │                    Pipeline Engine                            │
//! ├──────────────────────────────────────────────────────────────┤
//! │  Source ──► Operator Chain ──► Sink                           │
//! │                                                              │
//! │  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐  ┌───────┐ │
//! │  │  CDC   │─►│ Filter │─►│  Map   │─►│ Window │─►│ Sink  │ │
//! │  │ Source │  │        │  │        │  │  Agg   │  │       │ │
//! │  └────────┘  └────────┘  └────────┘  └────────┘  └───────┘ │
//! │                                                              │
//! │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │
//! │  │ State Store  │  │ Checkpoint   │  │ Metrics          │   │
//! │  │              │  │ Manager      │  │                  │   │
//! │  └──────────────┘  └──────────────┘  └──────────────────┘   │
//! └──────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use ferrite::pipeline::{PipelineBuilder, WindowType, Operator};
//! use std::time::Duration;
//!
//! let pipeline = PipelineBuilder::new("click-counts")
//!     .source("clicks:*")
//!     .filter(|event| event.contains("click"))
//!     .window(WindowType::Tumbling(Duration::from_secs(60)))
//!     .count()
//!     .sink("stats:clicks")
//!     .build()?;
//!
//! pipeline.start().await?;
//! ```

pub mod checkpoint;
pub mod operator;
pub mod window;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for a streaming pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    /// Pipeline name (unique identifier).
    pub name: String,
    /// Processing guarantee level.
    pub guarantee: ProcessingGuarantee,
    /// Checkpoint interval.
    pub checkpoint_interval: Duration,
    /// Maximum batch size for processing.
    pub batch_size: usize,
    /// Maximum idle time before flushing.
    pub flush_interval: Duration,
    /// Maximum in-flight events.
    pub max_in_flight: usize,
    /// Enable dead-letter queue for failed events.
    pub dead_letter_enabled: bool,
    /// Maximum retries for failed events.
    pub max_retries: u32,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            guarantee: ProcessingGuarantee::AtLeastOnce,
            checkpoint_interval: Duration::from_secs(30),
            batch_size: 100,
            flush_interval: Duration::from_secs(1),
            max_in_flight: 10_000,
            dead_letter_enabled: true,
            max_retries: 3,
        }
    }
}

/// Processing guarantee level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProcessingGuarantee {
    /// Events may be lost (fastest).
    AtMostOnce,
    /// Events may be duplicated but never lost.
    AtLeastOnce,
    /// Events processed exactly once (slowest, requires 2PC).
    ExactlyOnce,
}

impl Default for ProcessingGuarantee {
    fn default() -> Self {
        Self::AtLeastOnce
    }
}

// ---------------------------------------------------------------------------
// Pipeline types
// ---------------------------------------------------------------------------

/// An event flowing through the pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineEvent {
    /// Event ID for deduplication.
    pub id: String,
    /// Source key pattern.
    pub source: String,
    /// The key that triggered this event.
    pub key: String,
    /// Event payload.
    pub payload: serde_json::Value,
    /// Event timestamp.
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Processing metadata.
    pub metadata: HashMap<String, String>,
}

/// Window type for time-based aggregations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowType {
    /// Fixed-size non-overlapping windows.
    Tumbling(Duration),
    /// Fixed-size overlapping windows.
    Sliding { size: Duration, slide: Duration },
    /// Windows that close after inactivity.
    Session { gap: Duration },
    /// Single global window (all events).
    Global,
}

/// An operator in the pipeline processing chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperatorType {
    /// Filter events based on a predicate.
    Filter { predicate: String },
    /// Transform events.
    Map { expression: String },
    /// Expand events into multiple events.
    FlatMap { expression: String },
    /// Aggregate events within a window.
    Aggregate {
        function: AggregateFunction,
        field: String,
    },
    /// Count events within a window.
    Count,
    /// Join two streams.
    Join {
        other_source: String,
        join_key: String,
    },
    /// Deduplicate events by a key.
    Deduplicate { key_field: String, window: Duration },
}

/// Supported aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateFunction {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    First,
    Last,
}

/// Source type for pipeline input.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SourceType {
    /// Ferrite CDC events matching a key pattern.
    Cdc { pattern: String },
    /// Ferrite Pub/Sub channel.
    PubSub { channel: String },
    /// HTTP webhook endpoint.
    Http { endpoint: String },
    /// Kafka consumer.
    Kafka { topic: String, group_id: String },
}

/// Sink type for pipeline output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SinkType {
    /// Write results to Ferrite keys.
    Ferrite { key_pattern: String },
    /// Publish to Pub/Sub channel.
    PubSub { channel: String },
    /// POST to HTTP endpoint.
    Http { url: String },
    /// Produce to Kafka topic.
    Kafka { topic: String },
    /// Write to log/stdout (for debugging).
    Log,
}

// ---------------------------------------------------------------------------
// Pipeline definition and builder
// ---------------------------------------------------------------------------

/// A complete pipeline definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineDefinition {
    pub config: PipelineConfig,
    pub source: SourceType,
    pub operators: Vec<OperatorType>,
    pub window: Option<WindowType>,
    pub sink: SinkType,
}

/// Builder for constructing pipelines fluently.
pub struct PipelineBuilder {
    config: PipelineConfig,
    source: Option<SourceType>,
    operators: Vec<OperatorType>,
    window: Option<WindowType>,
    sink: Option<SinkType>,
}

impl PipelineBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            config: PipelineConfig {
                name: name.to_string(),
                ..Default::default()
            },
            source: None,
            operators: Vec::new(),
            window: None,
            sink: None,
        }
    }

    /// Sets the CDC source pattern.
    pub fn source(mut self, pattern: &str) -> Self {
        self.source = Some(SourceType::Cdc {
            pattern: pattern.to_string(),
        });
        self
    }

    /// Sets a Kafka source.
    pub fn kafka_source(mut self, topic: &str, group_id: &str) -> Self {
        self.source = Some(SourceType::Kafka {
            topic: topic.to_string(),
            group_id: group_id.to_string(),
        });
        self
    }

    /// Adds a filter operator.
    pub fn filter(mut self, predicate: &str) -> Self {
        self.operators.push(OperatorType::Filter {
            predicate: predicate.to_string(),
        });
        self
    }

    /// Adds a map operator.
    pub fn map(mut self, expression: &str) -> Self {
        self.operators.push(OperatorType::Map {
            expression: expression.to_string(),
        });
        self
    }

    /// Sets the window type.
    pub fn window(mut self, window_type: WindowType) -> Self {
        self.window = Some(window_type);
        self
    }

    /// Adds a count aggregation.
    pub fn count(mut self) -> Self {
        self.operators.push(OperatorType::Count);
        self
    }

    /// Adds an aggregate operator.
    pub fn aggregate(mut self, function: AggregateFunction, field: &str) -> Self {
        self.operators.push(OperatorType::Aggregate {
            function,
            field: field.to_string(),
        });
        self
    }

    /// Adds a join operator.
    pub fn join(mut self, other_source: &str, join_key: &str) -> Self {
        self.operators.push(OperatorType::Join {
            other_source: other_source.to_string(),
            join_key: join_key.to_string(),
        });
        self
    }

    /// Sets the sink as a Ferrite key pattern.
    pub fn sink(mut self, key_pattern: &str) -> Self {
        self.sink = Some(SinkType::Ferrite {
            key_pattern: key_pattern.to_string(),
        });
        self
    }

    /// Sets the sink as a Kafka topic.
    pub fn kafka_sink(mut self, topic: &str) -> Self {
        self.sink = Some(SinkType::Kafka {
            topic: topic.to_string(),
        });
        self
    }

    /// Sets the processing guarantee.
    pub fn guarantee(mut self, guarantee: ProcessingGuarantee) -> Self {
        self.config.guarantee = guarantee;
        self
    }

    /// Builds the pipeline definition.
    pub fn build(self) -> Result<PipelineDefinition, PipelineError> {
        let source = self.source.ok_or(PipelineError::NoSource)?;
        let sink = self.sink.ok_or(PipelineError::NoSink)?;

        Ok(PipelineDefinition {
            config: self.config,
            source,
            operators: self.operators,
            window: self.window,
            sink,
        })
    }
}

// ---------------------------------------------------------------------------
// Pipeline manager
// ---------------------------------------------------------------------------

/// Manages multiple streaming pipelines.
pub struct PipelineManager {
    pipelines: RwLock<HashMap<String, PipelineState>>,
    stats: PipelineManagerStats,
}

struct PipelineState {
    definition: PipelineDefinition,
    status: PipelineStatus,
    events_processed: u64,
    events_failed: u64,
    last_checkpoint: Option<chrono::DateTime<chrono::Utc>>,
}

/// Status of a pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PipelineStatus {
    Created,
    Running,
    Paused,
    Failed,
    Stopped,
}

#[derive(Debug, Default)]
struct PipelineManagerStats {
    total_events_processed: AtomicU64,
    total_events_failed: AtomicU64,
    active_pipelines: AtomicU64,
}

impl PipelineManager {
    pub fn new() -> Self {
        Self {
            pipelines: RwLock::new(HashMap::new()),
            stats: PipelineManagerStats::default(),
        }
    }

    /// Registers a new pipeline.
    pub fn register(&self, definition: PipelineDefinition) -> Result<(), PipelineError> {
        let name = definition.config.name.clone();
        let mut pipelines = self.pipelines.write();
        if pipelines.contains_key(&name) {
            return Err(PipelineError::AlreadyExists(name));
        }
        pipelines.insert(
            name,
            PipelineState {
                definition,
                status: PipelineStatus::Created,
                events_processed: 0,
                events_failed: 0,
                last_checkpoint: None,
            },
        );
        Ok(())
    }

    /// Starts a pipeline.
    pub fn start(&self, name: &str) -> Result<(), PipelineError> {
        let mut pipelines = self.pipelines.write();
        let state = pipelines
            .get_mut(name)
            .ok_or_else(|| PipelineError::NotFound(name.to_string()))?;
        state.status = PipelineStatus::Running;
        self.stats.active_pipelines.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Pauses a pipeline.
    pub fn pause(&self, name: &str) -> Result<(), PipelineError> {
        let mut pipelines = self.pipelines.write();
        let state = pipelines
            .get_mut(name)
            .ok_or_else(|| PipelineError::NotFound(name.to_string()))?;
        state.status = PipelineStatus::Paused;
        self.stats.active_pipelines.fetch_sub(1, Ordering::Relaxed);
        Ok(())
    }

    /// Stops and removes a pipeline.
    pub fn stop(&self, name: &str) -> Result<(), PipelineError> {
        let mut pipelines = self.pipelines.write();
        if pipelines.remove(name).is_none() {
            return Err(PipelineError::NotFound(name.to_string()));
        }
        Ok(())
    }

    /// Returns the status of a pipeline.
    pub fn status(&self, name: &str) -> Option<PipelineStatus> {
        self.pipelines.read().get(name).map(|s| s.status)
    }

    /// Lists all pipelines with their status.
    pub fn list(&self) -> Vec<PipelineInfo> {
        self.pipelines
            .read()
            .iter()
            .map(|(name, state)| PipelineInfo {
                name: name.clone(),
                status: state.status,
                events_processed: state.events_processed,
                events_failed: state.events_failed,
                guarantee: state.definition.config.guarantee,
                last_checkpoint: state.last_checkpoint,
            })
            .collect()
    }

    /// Returns the count of registered pipelines.
    pub fn pipeline_count(&self) -> usize {
        self.pipelines.read().len()
    }
}

impl Default for PipelineManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary information about a pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineInfo {
    pub name: String,
    pub status: PipelineStatus,
    pub events_processed: u64,
    pub events_failed: u64,
    pub guarantee: ProcessingGuarantee,
    pub last_checkpoint: Option<chrono::DateTime<chrono::Utc>>,
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, thiserror::Error)]
pub enum PipelineError {
    #[error("pipeline not found: {0}")]
    NotFound(String),

    #[error("pipeline already exists: {0}")]
    AlreadyExists(String),

    #[error("no source configured")]
    NoSource,

    #[error("no sink configured")]
    NoSink,

    #[error("invalid operator configuration: {0}")]
    InvalidOperator(String),

    #[error("checkpoint failed: {0}")]
    CheckpointFailed(String),

    #[error("processing error: {0}")]
    ProcessingError(String),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_builder() {
        let pipeline = PipelineBuilder::new("test-pipeline")
            .source("events:*")
            .filter("type == 'click'")
            .window(WindowType::Tumbling(Duration::from_secs(60)))
            .count()
            .sink("stats:clicks")
            .build()
            .unwrap();

        assert_eq!(pipeline.config.name, "test-pipeline");
        assert_eq!(pipeline.operators.len(), 2); // filter + count
        assert!(pipeline.window.is_some());
    }

    #[test]
    fn test_builder_no_source_error() {
        let result = PipelineBuilder::new("test").sink("output").build();
        assert!(matches!(result, Err(PipelineError::NoSource)));
    }

    #[test]
    fn test_builder_no_sink_error() {
        let result = PipelineBuilder::new("test").source("input:*").build();
        assert!(matches!(result, Err(PipelineError::NoSink)));
    }

    #[test]
    fn test_pipeline_manager_lifecycle() {
        let manager = PipelineManager::new();

        let pipeline = PipelineBuilder::new("p1")
            .source("events:*")
            .sink("output:*")
            .build()
            .unwrap();

        manager.register(pipeline).unwrap();
        assert_eq!(manager.pipeline_count(), 1);
        assert_eq!(manager.status("p1"), Some(PipelineStatus::Created));

        manager.start("p1").unwrap();
        assert_eq!(manager.status("p1"), Some(PipelineStatus::Running));

        manager.pause("p1").unwrap();
        assert_eq!(manager.status("p1"), Some(PipelineStatus::Paused));

        manager.stop("p1").unwrap();
        assert_eq!(manager.pipeline_count(), 0);
    }

    #[test]
    fn test_duplicate_pipeline_error() {
        let manager = PipelineManager::new();
        let p1 = PipelineBuilder::new("p1")
            .source("a:*")
            .sink("b:*")
            .build()
            .unwrap();
        let p2 = PipelineBuilder::new("p1")
            .source("c:*")
            .sink("d:*")
            .build()
            .unwrap();

        manager.register(p1).unwrap();
        let result = manager.register(p2);
        assert!(matches!(result, Err(PipelineError::AlreadyExists(_))));
    }

    #[test]
    fn test_list_pipelines() {
        let manager = PipelineManager::new();

        let p1 = PipelineBuilder::new("p1")
            .source("a:*")
            .sink("b:*")
            .guarantee(ProcessingGuarantee::ExactlyOnce)
            .build()
            .unwrap();

        manager.register(p1).unwrap();
        manager.start("p1").unwrap();

        let list = manager.list();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].status, PipelineStatus::Running);
        assert_eq!(list[0].guarantee, ProcessingGuarantee::ExactlyOnce);
    }

    #[test]
    fn test_kafka_source_and_sink() {
        let pipeline = PipelineBuilder::new("kafka-pipeline")
            .kafka_source("input-topic", "my-group")
            .map("$.value * 2")
            .kafka_sink("output-topic")
            .build()
            .unwrap();

        assert!(matches!(pipeline.source, SourceType::Kafka { .. }));
        assert!(matches!(pipeline.sink, SinkType::Kafka { .. }));
    }

    #[test]
    fn test_window_types() {
        let tumbling = WindowType::Tumbling(Duration::from_secs(60));
        let json = serde_json::to_string(&tumbling).unwrap();
        assert!(json.contains("Tumbling"));

        let sliding = WindowType::Sliding {
            size: Duration::from_secs(60),
            slide: Duration::from_secs(10),
        };
        let json = serde_json::to_string(&sliding).unwrap();
        assert!(json.contains("Sliding"));
    }

    #[test]
    fn test_processing_guarantees() {
        let g1 = ProcessingGuarantee::AtMostOnce;
        let g2 = ProcessingGuarantee::AtLeastOnce;
        let g3 = ProcessingGuarantee::ExactlyOnce;

        assert_ne!(g1, g2);
        assert_ne!(g2, g3);
    }
}
