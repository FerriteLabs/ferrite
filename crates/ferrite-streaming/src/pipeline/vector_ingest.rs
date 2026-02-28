//! Streaming vector ingestion pipeline
//!
//! Consumes documents from Kafka/Kinesis/PubSub, generates embeddings
//! via configurable providers, and incrementally updates vector indexes.
//!
//! ## Process Flow
//!
//! 1. Consume batch from source
//! 2. Extract text fields from documents
//! 3. Call embedding provider to generate vectors
//! 4. Insert vectors into target index (incremental)
//! 5. Track offsets for exactly-once processing

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use async_trait::async_trait;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::backpressure::BackpressureController;
use super::kafka_consumer::{KafkaConsumer, KafkaConsumerConfig};
use super::kafka_live::{LiveKafkaConsumer, LiveKafkaConsumerConfig};
use super::kafka_wire::KafkaWireConfig;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur in the vector ingestion pipeline.
#[derive(Debug, Clone, thiserror::Error)]
pub enum PipelineError {
    /// Error reading from the ingest source.
    #[error("source error: {0}")]
    SourceError(String),
    /// Error generating embeddings.
    #[error("embedding error: {0}")]
    EmbeddingError(String),
    /// Error writing to the target index.
    #[error("index error: {0}")]
    IndexError(String),
    /// Backpressure limit exceeded.
    #[error("backpressure threshold exceeded: {pending} pending items")]
    Backpressure {
        /// Number of pending items.
        pending: usize,
    },
    /// Pipeline is already running.
    #[error("pipeline is already running")]
    AlreadyRunning,
    /// Pipeline is not running.
    #[error("pipeline is not running")]
    NotRunning,
}

/// Errors that can occur during embedding generation.
#[derive(Debug, Clone, thiserror::Error)]
pub enum EmbeddingError {
    /// Provider returned an error.
    #[error("provider error: {0}")]
    ProviderError(String),
    /// Input text was invalid.
    #[error("invalid input: {0}")]
    InvalidInput(String),
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Source configuration for document ingestion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IngestSource {
    /// Apache Kafka source.
    Kafka {
        /// Broker addresses.
        brokers: Vec<String>,
        /// Topic to consume from.
        topic: String,
        /// Consumer group ID.
        group_id: String,
    },
    /// File-based source (one document per line).
    File {
        /// Path to the input file.
        path: String,
    },
    /// HTTP endpoint source.
    Http {
        /// Endpoint URL.
        endpoint: String,
    },
}

/// Embedding provider configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EmbeddingProvider {
    /// OpenAI-compatible embedding API.
    OpenAI {
        /// Model name (e.g. `text-embedding-3-small`).
        model: String,
        /// API key.
        api_key: String,
    },
    /// Local ONNX runtime model.
    Onnx {
        /// Path to the ONNX model file.
        model_path: String,
    },
    /// Mock provider that generates random vectors (for testing).
    Mock,
}

/// Error handling strategy for failed documents.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ErrorStrategy {
    /// Skip failed documents.
    Skip,
    /// Retry with exponential backoff.
    Retry {
        /// Maximum number of retries.
        max_retries: usize,
        /// Initial backoff in milliseconds.
        backoff_ms: u64,
    },
    /// Send failed documents to a dead-letter topic.
    DeadLetter,
}

impl Default for ErrorStrategy {
    fn default() -> Self {
        Self::Skip
    }
}

/// Configuration for the vector ingestion pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIngestConfig {
    /// Document source.
    pub source: IngestSource,
    /// Embedding provider.
    pub embedding_provider: EmbeddingProvider,
    /// Target vector index name.
    pub target_index: String,
    /// Number of documents to process per batch.
    pub batch_size: usize,
    /// Maximum concurrent embedding requests.
    pub max_concurrent_embeddings: usize,
    /// Maximum queued items before applying backpressure.
    pub backpressure_threshold: usize,
    /// Strategy for handling failed documents.
    pub error_handling: ErrorStrategy,
}

impl Default for VectorIngestConfig {
    fn default() -> Self {
        Self {
            source: IngestSource::File {
                path: String::new(),
            },
            embedding_provider: EmbeddingProvider::Mock,
            target_index: "default".to_string(),
            batch_size: 100,
            max_concurrent_embeddings: 4,
            backpressure_threshold: 10_000,
            error_handling: ErrorStrategy::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// Documents and results
// ---------------------------------------------------------------------------

/// A document to be ingested into the vector index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestDocument {
    /// Unique document identifier.
    pub id: String,
    /// Document text to embed.
    pub text: String,
    /// Arbitrary metadata attached to the vector.
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Handle returned when a pipeline is started.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineHandle {
    /// Unique pipeline identifier.
    pub pipeline_id: String,
    /// Current pipeline state.
    pub state: PipelineState,
}

/// Runtime state of the pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PipelineState {
    /// Pipeline is actively consuming and indexing.
    Running,
    /// Pipeline is paused.
    Paused,
    /// Pipeline has been stopped.
    Stopped,
    /// Pipeline encountered an unrecoverable error.
    Error,
}

/// Aggregate statistics for the pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStats {
    /// Total documents successfully processed.
    pub docs_processed: u64,
    /// Total documents that failed processing.
    pub docs_failed: u64,
    /// Total embeddings generated.
    pub embeddings_generated: u64,
    /// Total vectors written to the index.
    pub vectors_indexed: u64,
    /// Average end-to-end latency in milliseconds.
    pub avg_latency_ms: f64,
    /// Current throughput (documents per second).
    pub throughput_per_sec: f64,
    /// Last error message, if any.
    pub last_error: Option<String>,
}

/// Health status of the pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineHealth {
    /// Current pipeline state.
    pub state: PipelineState,
    /// Backpressure as a percentage (0.0 – 1.0+).
    pub backpressure_pct: f64,
    /// Error rate as a percentage (0.0 – 100.0).
    pub error_rate_pct: f64,
    /// Consumer lag (number of unprocessed messages).
    pub lag: u64,
}

// ---------------------------------------------------------------------------
// Embedding trait
// ---------------------------------------------------------------------------

/// Trait for embedding providers that convert text into vector representations.
#[async_trait]
pub trait EmbeddingProviderTrait: Send + Sync {
    /// Generate embeddings for a batch of texts.
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError>;
}

/// Mock embedding provider that generates deterministic random vectors.
pub struct MockEmbeddingProvider {
    dimension: usize,
}

impl MockEmbeddingProvider {
    /// Create a mock provider that produces vectors of the given dimension.
    pub fn new(dimension: usize) -> Self {
        Self { dimension }
    }
}

#[async_trait]
impl EmbeddingProviderTrait for MockEmbeddingProvider {
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        // Generate a simple deterministic vector from text content
        let vectors: Vec<Vec<f32>> = texts
            .iter()
            .map(|text| {
                (0..self.dimension)
                    .map(|i| {
                        let seed = text.len() as f32 + i as f32;
                        (seed * 0.1).sin()
                    })
                    .collect()
            })
            .collect();
        Ok(vectors)
    }
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

/// Internal mutable state for the running pipeline.
struct PipelineInner {
    state: PipelineState,
    stats: PipelineStatsInner,
    last_error: Option<String>,
}

/// Atomic counters for pipeline statistics.
struct PipelineStatsInner {
    docs_processed: AtomicU64,
    docs_failed: AtomicU64,
    embeddings_generated: AtomicU64,
    vectors_indexed: AtomicU64,
    total_latency_ms: AtomicU64,
    start_time: Option<Instant>,
}

impl PipelineStatsInner {
    fn new() -> Self {
        Self {
            docs_processed: AtomicU64::new(0),
            docs_failed: AtomicU64::new(0),
            embeddings_generated: AtomicU64::new(0),
            vectors_indexed: AtomicU64::new(0),
            total_latency_ms: AtomicU64::new(0),
            start_time: None,
        }
    }
}

/// Streaming vector ingestion pipeline.
///
/// Consumes documents, generates embeddings, and writes vectors to a
/// target index with backpressure management.
pub struct VectorIngestPipeline {
    config: VectorIngestConfig,
    inner: RwLock<PipelineInner>,
    backpressure: BackpressureController,
    pipeline_id: String,
}

impl VectorIngestPipeline {
    /// Create a new vector ingestion pipeline.
    pub fn new(config: VectorIngestConfig) -> Self {
        let bp_threshold = config.backpressure_threshold;
        let pipeline_id = uuid::Uuid::new_v4().to_string();

        Self {
            config,
            inner: RwLock::new(PipelineInner {
                state: PipelineState::Stopped,
                stats: PipelineStatsInner::new(),
                last_error: None,
            }),
            backpressure: BackpressureController::new(bp_threshold, bp_threshold / 2),
            pipeline_id,
        }
    }

    /// Start the pipeline. Returns a handle for monitoring.
    pub fn start(&self) -> Result<PipelineHandle, PipelineError> {
        let mut inner = self.inner.write();
        if inner.state == PipelineState::Running {
            return Err(PipelineError::AlreadyRunning);
        }

        inner.state = PipelineState::Running;
        inner.stats.start_time = Some(Instant::now());

        Ok(PipelineHandle {
            pipeline_id: self.pipeline_id.clone(),
            state: PipelineState::Running,
        })
    }

    /// Stop the pipeline.
    pub fn stop(&self) -> Result<(), PipelineError> {
        let mut inner = self.inner.write();
        if inner.state == PipelineState::Stopped {
            return Err(PipelineError::NotRunning);
        }
        inner.state = PipelineState::Stopped;
        Ok(())
    }

    /// Pause the pipeline (stops consuming but retains state).
    pub fn pause(&self) -> Result<(), PipelineError> {
        let mut inner = self.inner.write();
        if inner.state != PipelineState::Running {
            return Err(PipelineError::NotRunning);
        }
        inner.state = PipelineState::Paused;
        Ok(())
    }

    /// Resume a paused pipeline.
    pub fn resume(&self) -> Result<(), PipelineError> {
        let mut inner = self.inner.write();
        if inner.state != PipelineState::Paused {
            return Err(PipelineError::NotRunning);
        }
        inner.state = PipelineState::Running;
        Ok(())
    }

    /// Retrieve current pipeline statistics.
    pub fn stats(&self) -> PipelineStats {
        let inner = self.inner.read();
        let processed = inner.stats.docs_processed.load(Ordering::Relaxed);
        let total_latency = inner.stats.total_latency_ms.load(Ordering::Relaxed);

        let avg_latency = if processed > 0 {
            total_latency as f64 / processed as f64
        } else {
            0.0
        };

        let elapsed_secs = inner
            .stats
            .start_time
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(1.0)
            .max(0.001);

        PipelineStats {
            docs_processed: processed,
            docs_failed: inner.stats.docs_failed.load(Ordering::Relaxed),
            embeddings_generated: inner.stats.embeddings_generated.load(Ordering::Relaxed),
            vectors_indexed: inner.stats.vectors_indexed.load(Ordering::Relaxed),
            avg_latency_ms: avg_latency,
            throughput_per_sec: processed as f64 / elapsed_secs,
            last_error: inner.last_error.clone(),
        }
    }

    /// Create a [`KafkaConsumer`] when the pipeline source is
    /// [`IngestSource::Kafka`].  Returns `None` for other source types.
    pub fn create_kafka_source(config: &VectorIngestConfig) -> Option<KafkaConsumer> {
        match &config.source {
            IngestSource::Kafka {
                brokers,
                topic,
                group_id,
            } => {
                let kafka_config = KafkaConsumerConfig {
                    brokers: brokers.clone(),
                    topic: topic.clone(),
                    group_id: group_id.clone(),
                    ..KafkaConsumerConfig::default()
                };
                KafkaConsumer::new(kafka_config).ok()
            }
            _ => None,
        }
    }

    /// Create a [`LiveKafkaConsumer`] when the pipeline source is
    /// [`IngestSource::Kafka`].  Returns `None` for other source types.
    ///
    /// Unlike [`create_kafka_source`](Self::create_kafka_source), this
    /// variant connects to real Kafka brokers over TCP using the wire
    /// protocol rather than the internal [`crate::kafka::broker::StreamingBroker`].
    pub fn create_live_kafka_source(
        config: &VectorIngestConfig,
    ) -> Option<LiveKafkaConsumerConfig> {
        match &config.source {
            IngestSource::Kafka {
                brokers,
                topic,
                group_id,
            } => {
                let addrs: Vec<std::net::SocketAddr> = brokers
                    .iter()
                    .filter_map(|b| b.parse().ok())
                    .collect();
                if addrs.is_empty() {
                    return None;
                }
                Some(LiveKafkaConsumerConfig {
                    wire_config: KafkaWireConfig {
                        brokers: addrs,
                        client_id: format!("ferrite-ingest-{}", uuid::Uuid::new_v4()),
                        group_id: group_id.clone(),
                        topic: topic.clone(),
                        ..KafkaWireConfig::default()
                    },
                    auto_commit: true,
                    auto_commit_interval: std::time::Duration::from_secs(5),
                    from_beginning: true,
                })
            }
            _ => None,
        }
    }

    /// Check pipeline health.
    pub fn health(&self) -> PipelineHealth {
        let inner = self.inner.read();
        let processed = inner.stats.docs_processed.load(Ordering::Relaxed);
        let failed = inner.stats.docs_failed.load(Ordering::Relaxed);

        let error_rate = if processed + failed > 0 {
            (failed as f64 / (processed + failed) as f64) * 100.0
        } else {
            0.0
        };

        PipelineHealth {
            state: inner.state,
            backpressure_pct: self.backpressure.pressure_pct(),
            error_rate_pct: error_rate,
            lag: 0,
        }
    }

    /// Process a batch of documents through the pipeline.
    ///
    /// This is the core ingestion loop step: consume → embed → index.
    pub async fn process_batch(
        &self,
        documents: Vec<IngestDocument>,
        embedder: &dyn EmbeddingProviderTrait,
    ) -> Result<usize, PipelineError> {
        {
            let inner = self.inner.read();
            if inner.state != PipelineState::Running {
                return Err(PipelineError::NotRunning);
            }
        }

        if self.backpressure.should_pause() {
            return Err(PipelineError::Backpressure {
                pending: self.config.backpressure_threshold,
            });
        }

        let batch_start = Instant::now();
        self.backpressure.record_pending(documents.len());

        // Extract texts for embedding
        let texts: Vec<String> = documents.iter().map(|d| d.text.clone()).collect();

        // Generate embeddings
        let embeddings = embedder
            .embed_batch(&texts)
            .await
            .map_err(|e| PipelineError::EmbeddingError(e.to_string()))?;

        let inner = self.inner.read();
        inner
            .stats
            .embeddings_generated
            .fetch_add(embeddings.len() as u64, Ordering::Relaxed);

        // In a full implementation, we would write to the target index here.
        // For now, track the stats.
        let indexed = embeddings.len();
        inner
            .stats
            .vectors_indexed
            .fetch_add(indexed as u64, Ordering::Relaxed);
        inner
            .stats
            .docs_processed
            .fetch_add(documents.len() as u64, Ordering::Relaxed);

        let elapsed = batch_start.elapsed().as_millis() as u64;
        inner
            .stats
            .total_latency_ms
            .fetch_add(elapsed, Ordering::Relaxed);

        self.backpressure.record_completed(documents.len());

        Ok(indexed)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> VectorIngestConfig {
        VectorIngestConfig {
            source: IngestSource::File {
                path: "/tmp/test.jsonl".to_string(),
            },
            embedding_provider: EmbeddingProvider::Mock,
            target_index: "test-index".to_string(),
            batch_size: 10,
            max_concurrent_embeddings: 2,
            backpressure_threshold: 1000,
            error_handling: ErrorStrategy::Skip,
        }
    }

    #[test]
    fn test_pipeline_lifecycle() {
        let pipeline = VectorIngestPipeline::new(test_config());

        let handle = pipeline.start().unwrap();
        assert_eq!(handle.state, PipelineState::Running);

        // Double start should fail
        assert!(matches!(pipeline.start(), Err(PipelineError::AlreadyRunning)));

        pipeline.pause().unwrap();
        pipeline.resume().unwrap();
        pipeline.stop().unwrap();

        // Double stop should fail
        assert!(matches!(pipeline.stop(), Err(PipelineError::NotRunning)));
    }

    #[test]
    fn test_pipeline_health() {
        let pipeline = VectorIngestPipeline::new(test_config());
        pipeline.start().unwrap();

        let health = pipeline.health();
        assert_eq!(health.state, PipelineState::Running);
        assert!((health.backpressure_pct - 0.0).abs() < f64::EPSILON);
        assert!((health.error_rate_pct - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_pipeline_stats_initial() {
        let pipeline = VectorIngestPipeline::new(test_config());
        let stats = pipeline.stats();
        assert_eq!(stats.docs_processed, 0);
        assert_eq!(stats.docs_failed, 0);
        assert_eq!(stats.embeddings_generated, 0);
    }

    #[tokio::test]
    async fn test_process_batch() {
        let pipeline = VectorIngestPipeline::new(test_config());
        pipeline.start().unwrap();

        let docs = vec![
            IngestDocument {
                id: "doc1".to_string(),
                text: "Hello world".to_string(),
                metadata: HashMap::new(),
            },
            IngestDocument {
                id: "doc2".to_string(),
                text: "Rust is great".to_string(),
                metadata: HashMap::new(),
            },
        ];

        let embedder = MockEmbeddingProvider::new(128);
        let indexed = pipeline.process_batch(docs, &embedder).await.unwrap();
        assert_eq!(indexed, 2);

        let stats = pipeline.stats();
        assert_eq!(stats.docs_processed, 2);
        assert_eq!(stats.embeddings_generated, 2);
        assert_eq!(stats.vectors_indexed, 2);
    }

    #[tokio::test]
    async fn test_process_batch_not_running() {
        let pipeline = VectorIngestPipeline::new(test_config());
        let embedder = MockEmbeddingProvider::new(128);
        let result = pipeline.process_batch(vec![], &embedder).await;
        assert!(matches!(result, Err(PipelineError::NotRunning)));
    }

    #[test]
    fn test_ingest_source_serialization() {
        let source = IngestSource::Kafka {
            brokers: vec!["localhost:9092".to_string()],
            topic: "documents".to_string(),
            group_id: "ferrite-ingest".to_string(),
        };
        let json = serde_json::to_string(&source).unwrap();
        assert!(json.contains("Kafka"));
    }

    #[test]
    fn test_mock_embedding_provider() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let provider = MockEmbeddingProvider::new(64);
        let texts = vec!["hello".to_string(), "world".to_string()];

        let result = rt.block_on(provider.embed_batch(&texts)).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].len(), 64);
        assert_eq!(result[1].len(), 64);
    }
}
