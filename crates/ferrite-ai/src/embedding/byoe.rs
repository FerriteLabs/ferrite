//! BYOE - Bring Your Own Embedding Model
//!
//! This module provides a flexible plugin architecture for custom embedding models:
//! - Register custom embedding models at runtime
//! - Hot-swap models without downtime
//! - Model versioning and A/B testing
//! - Model validation and benchmarking
//! - Unified interface for local and remote models

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{error, info};

/// Model identifier
pub type ModelId = String;

/// Model version
pub type ModelVersion = String;

/// Configuration for BYOE
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ByoeConfig {
    /// Enable BYOE
    pub enabled: bool,
    /// Default model ID
    pub default_model: ModelId,
    /// Model validation enabled
    pub validation_enabled: bool,
    /// Validation sample texts
    pub validation_samples: Vec<String>,
    /// Expected dimension for validation
    pub expected_dimension: Option<usize>,
    /// Maximum latency threshold (ms)
    pub max_latency_ms: u64,
    /// Enable model metrics collection
    pub metrics_enabled: bool,
    /// Model cache size
    pub cache_size: usize,
    /// Allow model hot-swap
    pub hot_swap_enabled: bool,
}

impl Default for ByoeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_model: "default".to_string(),
            validation_enabled: true,
            validation_samples: vec![
                "The quick brown fox".to_string(),
                "Machine learning is powerful".to_string(),
            ],
            expected_dimension: None,
            max_latency_ms: 1000,
            metrics_enabled: true,
            cache_size: 10000,
            hot_swap_enabled: true,
        }
    }
}

/// Custom embedding model trait that users implement
#[async_trait]
pub trait CustomEmbeddingModel: Send + Sync {
    /// Get model name
    fn name(&self) -> &str;

    /// Get model version
    fn version(&self) -> &str;

    /// Get embedding dimension
    fn dimension(&self) -> usize;

    /// Get model metadata
    fn metadata(&self) -> &ModelMetadata;

    /// Generate embedding for a single text
    async fn embed(&self, text: &str) -> Result<Vec<f32>, EmbedError>;

    /// Generate embeddings for multiple texts (batch)
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        let mut results = Vec::with_capacity(texts.len());
        for text in texts {
            results.push(self.embed(text).await?);
        }
        Ok(results)
    }

    /// Warm up the model (e.g., load weights)
    async fn warm_up(&self) -> Result<(), EmbedError> {
        Ok(())
    }

    /// Shutdown the model (cleanup resources)
    async fn shutdown(&self) -> Result<(), EmbedError> {
        Ok(())
    }

    /// Check if model is healthy
    fn is_healthy(&self) -> bool {
        true
    }

    /// Get model statistics
    fn stats(&self) -> ModelStats {
        ModelStats::default()
    }
}

/// Model metadata
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ModelMetadata {
    /// Model description
    pub description: String,
    /// Model author
    pub author: Option<String>,
    /// Model license
    pub license: Option<String>,
    /// Model source URL
    pub source_url: Option<String>,
    /// Training data info
    pub training_data: Option<String>,
    /// Supported languages
    pub languages: Vec<String>,
    /// Model size (parameters)
    pub parameters: Option<u64>,
    /// Model type (e.g., "transformer", "word2vec")
    pub model_type: Option<String>,
    /// Custom tags
    pub tags: Vec<String>,
    /// Custom properties
    pub properties: HashMap<String, String>,
}

/// Model statistics
#[derive(Clone, Debug, Default)]
pub struct ModelStats {
    /// Total embeddings generated
    pub embeddings_generated: u64,
    /// Total batches processed
    pub batches_processed: u64,
    /// Total errors
    pub errors: u64,
    /// Average latency (ms)
    pub avg_latency_ms: f64,
    /// P95 latency (ms)
    pub p95_latency_ms: f64,
    /// P99 latency (ms)
    pub p99_latency_ms: f64,
    /// Cache hits
    pub cache_hits: u64,
    /// Model load time (ms)
    pub load_time_ms: u64,
    /// Memory usage (bytes)
    pub memory_bytes: u64,
}

/// Embedding error
#[derive(Debug, Clone, thiserror::Error)]
pub enum EmbedError {
    #[error("Model not found: {0}")]
    ModelNotFound(String),

    #[error("Model initialization failed: {0}")]
    InitializationFailed(String),

    #[error("Embedding generation failed: {0}")]
    EmbeddingFailed(String),

    #[error("Model validation failed: {0}")]
    ValidationFailed(String),

    #[error("Dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },

    #[error("Rate limited: retry after {0} seconds")]
    RateLimited(u64),

    #[error("Timeout")]
    Timeout,

    #[error("Model is unhealthy")]
    Unhealthy,

    #[error("Configuration error: {0}")]
    ConfigError(String),
}

/// Registered model info
struct RegisteredModel {
    /// The model instance
    model: Arc<dyn CustomEmbeddingModel>,
    /// Registration timestamp
    registered_at: Instant,
    /// Is currently active
    is_active: AtomicBool,
    /// Embeddings generated
    embeddings_count: AtomicU64,
    /// Errors count
    errors_count: AtomicU64,
    /// Total latency (for averaging)
    total_latency_ns: AtomicU64,
}

/// Model loader trait for loading models from various sources
#[async_trait]
pub trait ModelLoader: Send + Sync {
    /// Loader name
    fn name(&self) -> &str;

    /// Check if this loader can handle the given source
    fn can_load(&self, source: &ModelSource) -> bool;

    /// Load a model from the source
    async fn load(&self, source: &ModelSource)
        -> Result<Arc<dyn CustomEmbeddingModel>, EmbedError>;
}

/// Model source specification
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelSource {
    /// Source type
    pub source_type: ModelSourceType,
    /// Source path or URL
    pub location: String,
    /// Model name override
    pub name: Option<String>,
    /// Model version override
    pub version: Option<String>,
    /// Model dimension
    pub dimension: usize,
    /// Additional configuration
    pub config: HashMap<String, String>,
}

/// Model source types
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelSourceType {
    /// Local file path (ONNX, etc.)
    LocalFile,
    /// Remote URL
    RemoteUrl,
    /// Hugging Face model hub
    HuggingFace,
    /// OpenAI API
    OpenAI,
    /// Cohere API
    Cohere,
    /// Custom HTTP endpoint
    CustomHttp,
    /// gRPC service
    Grpc,
    /// WebSocket service
    WebSocket,
}

/// Validation result
#[derive(Clone, Debug)]
pub struct ValidationResult {
    /// Passed validation
    pub passed: bool,
    /// Validation checks
    pub checks: Vec<ValidationCheck>,
    /// Total time taken
    pub duration: Duration,
}

/// Individual validation check
#[derive(Clone, Debug)]
pub struct ValidationCheck {
    /// Check name
    pub name: String,
    /// Passed
    pub passed: bool,
    /// Message
    pub message: String,
}

/// BYOE Model Registry
///
/// Central registry for managing custom embedding models
pub struct ModelRegistry {
    /// Configuration
    config: ByoeConfig,
    /// Registered models
    models: RwLock<HashMap<ModelId, RegisteredModel>>,
    /// Model loaders
    loaders: RwLock<Vec<Arc<dyn ModelLoader>>>,
    /// Active model ID
    active_model: RwLock<ModelId>,
    /// A/B test configuration
    ab_test: RwLock<Option<AbTestConfig>>,
    /// Global metrics
    metrics: Arc<RegistryMetrics>,
    /// Running flag
    running: AtomicBool,
}

/// Registry metrics
#[derive(Default)]
pub struct RegistryMetrics {
    /// Total models registered
    pub models_registered: AtomicU64,
    /// Total models unregistered
    pub models_unregistered: AtomicU64,
    /// Hot swaps performed
    pub hot_swaps: AtomicU64,
    /// Total embeddings via registry
    pub total_embeddings: AtomicU64,
    /// Total errors via registry
    pub total_errors: AtomicU64,
    /// Model validations run
    pub validations_run: AtomicU64,
    /// Validation failures
    pub validation_failures: AtomicU64,
}

/// A/B test configuration
#[derive(Clone, Debug)]
pub struct AbTestConfig {
    /// Model A (control)
    pub model_a: ModelId,
    /// Model B (treatment)
    pub model_b: ModelId,
    /// Traffic split for model B (0.0 to 1.0)
    pub traffic_split: f64,
    /// Test name
    pub test_name: String,
    /// Metrics to track
    pub metrics: Vec<String>,
}

impl ModelRegistry {
    /// Create a new model registry
    pub fn new(config: ByoeConfig) -> Self {
        let default_model = config.default_model.clone();

        Self {
            config,
            models: RwLock::new(HashMap::new()),
            loaders: RwLock::new(Vec::new()),
            active_model: RwLock::new(default_model),
            ab_test: RwLock::new(None),
            metrics: Arc::new(RegistryMetrics::default()),
            running: AtomicBool::new(true),
        }
    }

    /// Register a model loader
    pub fn register_loader(&self, loader: Arc<dyn ModelLoader>) {
        self.loaders.write().push(loader);
        info!("Registered model loader: {}", self.loaders.read().len());
    }

    /// Register a custom embedding model
    pub async fn register(
        &self,
        model_id: ModelId,
        model: Arc<dyn CustomEmbeddingModel>,
    ) -> Result<(), EmbedError> {
        // Validate the model if enabled
        if self.config.validation_enabled {
            let validation = self.validate_model(&*model).await?;
            if !validation.passed {
                return Err(EmbedError::ValidationFailed(format!(
                    "Model failed validation: {:?}",
                    validation.checks
                )));
            }
        }

        // Warm up the model
        model.warm_up().await?;

        let registered = RegisteredModel {
            model,
            registered_at: Instant::now(),
            is_active: AtomicBool::new(true),
            embeddings_count: AtomicU64::new(0),
            errors_count: AtomicU64::new(0),
            total_latency_ns: AtomicU64::new(0),
        };

        self.models.write().insert(model_id.clone(), registered);
        self.metrics
            .models_registered
            .fetch_add(1, Ordering::Relaxed);

        info!("Registered embedding model: {}", model_id);
        Ok(())
    }

    /// Load and register a model from a source
    pub async fn load_model(
        &self,
        model_id: ModelId,
        source: ModelSource,
    ) -> Result<(), EmbedError> {
        let loader = {
            let loaders = self.loaders.read();
            loaders
                .iter()
                .find(|l| l.can_load(&source))
                .cloned()
        };

        if let Some(loader) = loader {
            let model = loader.load(&source).await?;
            return self.register(model_id, model).await;
        }

        Err(EmbedError::ConfigError(format!(
            "No loader found for source type: {:?}",
            source.source_type
        )))
    }

    /// Unregister a model
    pub async fn unregister(&self, model_id: &ModelId) -> Result<(), EmbedError> {
        let registered = {
            let mut models = self.models.write();
            models.remove(model_id)
        };

        if let Some(registered) = registered {
            registered.is_active.store(false, Ordering::SeqCst);
            registered.model.shutdown().await?;
            self.metrics
                .models_unregistered
                .fetch_add(1, Ordering::Relaxed);
            info!("Unregistered embedding model: {}", model_id);
            Ok(())
        } else {
            Err(EmbedError::ModelNotFound(model_id.clone()))
        }
    }

    /// Set the active model
    pub fn set_active(&self, model_id: ModelId) -> Result<(), EmbedError> {
        if !self.models.read().contains_key(&model_id) {
            return Err(EmbedError::ModelNotFound(model_id));
        }

        let old_model = self.active_model.read().clone();
        *self.active_model.write() = model_id.clone();

        if self.config.hot_swap_enabled {
            self.metrics.hot_swaps.fetch_add(1, Ordering::Relaxed);
        }

        info!("Active model changed: {} -> {}", old_model, model_id);
        Ok(())
    }

    /// Get the active model
    pub fn get_active(&self) -> Result<Arc<dyn CustomEmbeddingModel>, EmbedError> {
        // Check A/B test first
        if let Some(ab_config) = self.ab_test.read().as_ref() {
            let model_id = if rand::random::<f64>() < ab_config.traffic_split {
                &ab_config.model_b
            } else {
                &ab_config.model_a
            };

            if let Some(registered) = self.models.read().get(model_id) {
                return Ok(Arc::clone(&registered.model));
            }
        }

        let model_id = self.active_model.read().clone();
        self.get(&model_id)
    }

    /// Get a specific model by ID
    pub fn get(&self, model_id: &ModelId) -> Result<Arc<dyn CustomEmbeddingModel>, EmbedError> {
        self.models
            .read()
            .get(model_id)
            .map(|r| Arc::clone(&r.model))
            .ok_or_else(|| EmbedError::ModelNotFound(model_id.clone()))
    }

    /// List all registered models
    pub fn list_models(&self) -> Vec<ModelInfo> {
        self.models
            .read()
            .iter()
            .map(|(id, registered)| ModelInfo {
                id: id.clone(),
                name: registered.model.name().to_string(),
                version: registered.model.version().to_string(),
                dimension: registered.model.dimension(),
                is_active: *self.active_model.read() == *id,
                is_healthy: registered.model.is_healthy(),
                embeddings_count: registered.embeddings_count.load(Ordering::Relaxed),
                errors_count: registered.errors_count.load(Ordering::Relaxed),
                registered_at_ms: registered.registered_at.elapsed().as_millis() as u64,
                metadata: registered.model.metadata().clone(),
            })
            .collect()
    }

    /// Generate embedding using the active model
    pub async fn embed(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        let (model, model_id) = self.get_model_for_embedding()?;

        let start = Instant::now();
        let result = model.embed(text).await;
        let latency_ns = start.elapsed().as_nanos() as u64;

        // Update metrics
        if let Some(registered) = self.models.read().get(&model_id) {
            match &result {
                Ok(_) => {
                    registered.embeddings_count.fetch_add(1, Ordering::Relaxed);
                    registered
                        .total_latency_ns
                        .fetch_add(latency_ns, Ordering::Relaxed);
                }
                Err(_) => {
                    registered.errors_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        self.metrics
            .total_embeddings
            .fetch_add(1, Ordering::Relaxed);
        if result.is_err() {
            self.metrics.total_errors.fetch_add(1, Ordering::Relaxed);
        }

        result
    }

    /// Generate embeddings in batch using the active model
    pub async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        let (model, model_id) = self.get_model_for_embedding()?;

        let start = Instant::now();
        let result = model.embed_batch(texts).await;
        let latency_ns = start.elapsed().as_nanos() as u64;

        // Update metrics
        if let Some(registered) = self.models.read().get(&model_id) {
            match &result {
                Ok(embeddings) => {
                    registered
                        .embeddings_count
                        .fetch_add(embeddings.len() as u64, Ordering::Relaxed);
                    registered
                        .total_latency_ns
                        .fetch_add(latency_ns, Ordering::Relaxed);
                }
                Err(_) => {
                    registered.errors_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        self.metrics
            .total_embeddings
            .fetch_add(texts.len() as u64, Ordering::Relaxed);
        if result.is_err() {
            self.metrics.total_errors.fetch_add(1, Ordering::Relaxed);
        }

        result
    }

    fn get_model_for_embedding(
        &self,
    ) -> Result<(Arc<dyn CustomEmbeddingModel>, ModelId), EmbedError> {
        // Check A/B test first
        if let Some(ab_config) = self.ab_test.read().as_ref() {
            let model_id = if rand::random::<f64>() < ab_config.traffic_split {
                ab_config.model_b.clone()
            } else {
                ab_config.model_a.clone()
            };

            if let Some(registered) = self.models.read().get(&model_id) {
                if !registered.model.is_healthy() {
                    return Err(EmbedError::Unhealthy);
                }
                return Ok((Arc::clone(&registered.model), model_id));
            }
        }

        let model_id = self.active_model.read().clone();
        let models = self.models.read();

        let registered = models
            .get(&model_id)
            .ok_or_else(|| EmbedError::ModelNotFound(model_id.clone()))?;

        if !registered.model.is_healthy() {
            return Err(EmbedError::Unhealthy);
        }

        Ok((Arc::clone(&registered.model), model_id))
    }

    /// Validate a model
    pub async fn validate_model(
        &self,
        model: &dyn CustomEmbeddingModel,
    ) -> Result<ValidationResult, EmbedError> {
        let start = Instant::now();
        let mut checks = Vec::new();

        // Check 1: Model health
        let health_check = ValidationCheck {
            name: "health".to_string(),
            passed: model.is_healthy(),
            message: if model.is_healthy() {
                "Model is healthy".to_string()
            } else {
                "Model reports unhealthy".to_string()
            },
        };
        checks.push(health_check);

        // Check 2: Dimension consistency
        if let Some(expected) = self.config.expected_dimension {
            let actual = model.dimension();
            let passed = expected == actual;
            checks.push(ValidationCheck {
                name: "dimension".to_string(),
                passed,
                message: if passed {
                    format!("Dimension matches: {}", actual)
                } else {
                    format!("Dimension mismatch: expected {}, got {}", expected, actual)
                },
            });
        }

        // Check 3: Sample embeddings
        for (i, sample) in self.config.validation_samples.iter().enumerate() {
            let embed_start = Instant::now();
            let result = model.embed(sample).await;
            let latency_ms = embed_start.elapsed().as_millis() as u64;

            match result {
                Ok(embedding) => {
                    // Check dimension matches declared dimension
                    let dim_match = embedding.len() == model.dimension();

                    // Check latency is acceptable
                    let latency_ok = latency_ms <= self.config.max_latency_ms;

                    // Check embedding is valid (no NaN/Inf)
                    let valid_values = embedding.iter().all(|v| v.is_finite());

                    let passed = dim_match && latency_ok && valid_values;

                    checks.push(ValidationCheck {
                        name: format!("sample_{}", i),
                        passed,
                        message: format!(
                            "dim_match={}, latency={}ms (max={}), valid_values={}",
                            dim_match, latency_ms, self.config.max_latency_ms, valid_values
                        ),
                    });
                }
                Err(e) => {
                    checks.push(ValidationCheck {
                        name: format!("sample_{}", i),
                        passed: false,
                        message: format!("Embedding failed: {}", e),
                    });
                }
            }
        }

        self.metrics.validations_run.fetch_add(1, Ordering::Relaxed);

        let passed = checks.iter().all(|c| c.passed);
        if !passed {
            self.metrics
                .validation_failures
                .fetch_add(1, Ordering::Relaxed);
        }

        Ok(ValidationResult {
            passed,
            checks,
            duration: start.elapsed(),
        })
    }

    /// Configure A/B testing
    pub fn configure_ab_test(&self, config: AbTestConfig) -> Result<(), EmbedError> {
        // Verify both models exist
        let models = self.models.read();
        if !models.contains_key(&config.model_a) {
            return Err(EmbedError::ModelNotFound(config.model_a.clone()));
        }
        if !models.contains_key(&config.model_b) {
            return Err(EmbedError::ModelNotFound(config.model_b.clone()));
        }
        drop(models);

        *self.ab_test.write() = Some(config.clone());
        info!(
            "A/B test configured: {} vs {} ({}% to B)",
            config.model_a,
            config.model_b,
            config.traffic_split * 100.0
        );

        Ok(())
    }

    /// Stop A/B testing
    pub fn stop_ab_test(&self) {
        *self.ab_test.write() = None;
        info!("A/B test stopped");
    }

    /// Get registry metrics
    pub fn get_metrics(&self) -> RegistryMetricsSnapshot {
        RegistryMetricsSnapshot {
            models_registered: self.metrics.models_registered.load(Ordering::Relaxed),
            models_unregistered: self.metrics.models_unregistered.load(Ordering::Relaxed),
            hot_swaps: self.metrics.hot_swaps.load(Ordering::Relaxed),
            total_embeddings: self.metrics.total_embeddings.load(Ordering::Relaxed),
            total_errors: self.metrics.total_errors.load(Ordering::Relaxed),
            validations_run: self.metrics.validations_run.load(Ordering::Relaxed),
            validation_failures: self.metrics.validation_failures.load(Ordering::Relaxed),
            active_models: self.models.read().len(),
        }
    }

    /// Shutdown the registry
    pub async fn shutdown(&self) -> Result<(), EmbedError> {
        self.running.store(false, Ordering::SeqCst);

        let models: Vec<_> = self.models.read().keys().cloned().collect();
        for model_id in models {
            if let Err(e) = self.unregister(&model_id).await {
                error!("Error unregistering model {}: {}", model_id, e);
            }
        }

        info!("Model registry shutdown complete");
        Ok(())
    }
}

/// Model info for listing
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelInfo {
    /// Model ID
    pub id: ModelId,
    /// Model name
    pub name: String,
    /// Model version
    pub version: String,
    /// Embedding dimension
    pub dimension: usize,
    /// Is this the active model
    pub is_active: bool,
    /// Is model healthy
    pub is_healthy: bool,
    /// Total embeddings generated
    pub embeddings_count: u64,
    /// Total errors
    pub errors_count: u64,
    /// Time since registration (ms)
    pub registered_at_ms: u64,
    /// Model metadata
    pub metadata: ModelMetadata,
}

/// Registry metrics snapshot
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegistryMetricsSnapshot {
    pub models_registered: u64,
    pub models_unregistered: u64,
    pub hot_swaps: u64,
    pub total_embeddings: u64,
    pub total_errors: u64,
    pub validations_run: u64,
    pub validation_failures: u64,
    pub active_models: usize,
}

/// Simple HTTP model implementation
pub struct HttpEmbeddingModel {
    name: String,
    version: String,
    dimension: usize,
    endpoint: String,
    api_key: Option<String>,
    metadata: ModelMetadata,
    client: reqwest::Client,
    healthy: AtomicBool,
}

impl HttpEmbeddingModel {
    /// Create a new HTTP-based embedding model
    pub fn new(
        name: impl Into<String>,
        version: impl Into<String>,
        dimension: usize,
        endpoint: impl Into<String>,
        api_key: Option<String>,
    ) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            dimension,
            endpoint: endpoint.into(),
            api_key,
            metadata: ModelMetadata::default(),
            client: reqwest::Client::new(),
            healthy: AtomicBool::new(true),
        }
    }

    /// Set metadata
    pub fn with_metadata(mut self, metadata: ModelMetadata) -> Self {
        self.metadata = metadata;
        self
    }
}

#[derive(Serialize)]
struct EmbedRequest {
    inputs: Vec<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum EmbedResponse {
    Direct(Vec<Vec<f32>>),
    WithEmbeddings { embeddings: Vec<Vec<f32>> },
    WithData { data: Vec<EmbedData> },
}

#[derive(Deserialize)]
struct EmbedData {
    embedding: Vec<f32>,
}

#[async_trait]
impl CustomEmbeddingModel for HttpEmbeddingModel {
    fn name(&self) -> &str {
        &self.name
    }

    fn version(&self) -> &str {
        &self.version
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    fn metadata(&self) -> &ModelMetadata {
        &self.metadata
    }

    async fn embed(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        let request = EmbedRequest {
            inputs: vec![text.to_string()],
        };

        let mut req_builder = self
            .client
            .post(&self.endpoint)
            .header("Content-Type", "application/json")
            .json(&request);

        if let Some(key) = &self.api_key {
            req_builder = req_builder.header("Authorization", format!("Bearer {}", key));
        }

        let response = req_builder
            .send()
            .await
            .map_err(|e| EmbedError::EmbeddingFailed(format!("Request failed: {}", e)))?;

        let status = response.status();
        if !status.is_success() {
            self.healthy.store(false, Ordering::SeqCst);
            let body = response.text().await.unwrap_or_default();
            return Err(EmbedError::EmbeddingFailed(format!(
                "HTTP {}: {}",
                status, body
            )));
        }

        let embed_response: EmbedResponse = response
            .json()
            .await
            .map_err(|e| EmbedError::EmbeddingFailed(format!("Parse failed: {}", e)))?;

        let embeddings = match embed_response {
            EmbedResponse::Direct(embs) => embs,
            EmbedResponse::WithEmbeddings { embeddings } => embeddings,
            EmbedResponse::WithData { data } => data.into_iter().map(|d| d.embedding).collect(),
        };

        embeddings
            .into_iter()
            .next()
            .ok_or_else(|| EmbedError::EmbeddingFailed("Empty response".to_string()))
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        let request = EmbedRequest {
            inputs: texts.to_vec(),
        };

        let mut req_builder = self
            .client
            .post(&self.endpoint)
            .header("Content-Type", "application/json")
            .json(&request);

        if let Some(key) = &self.api_key {
            req_builder = req_builder.header("Authorization", format!("Bearer {}", key));
        }

        let response = req_builder
            .send()
            .await
            .map_err(|e| EmbedError::EmbeddingFailed(format!("Request failed: {}", e)))?;

        let status = response.status();
        if !status.is_success() {
            self.healthy.store(false, Ordering::SeqCst);
            let body = response.text().await.unwrap_or_default();
            return Err(EmbedError::EmbeddingFailed(format!(
                "HTTP {}: {}",
                status, body
            )));
        }

        let embed_response: EmbedResponse = response
            .json()
            .await
            .map_err(|e| EmbedError::EmbeddingFailed(format!("Parse failed: {}", e)))?;

        match embed_response {
            EmbedResponse::Direct(embs) => Ok(embs),
            EmbedResponse::WithEmbeddings { embeddings } => Ok(embeddings),
            EmbedResponse::WithData { data } => Ok(data.into_iter().map(|d| d.embedding).collect()),
        }
    }

    fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::SeqCst)
    }
}

/// Mock embedding model for testing
pub struct MockEmbeddingModel {
    name: String,
    version: String,
    dimension: usize,
    metadata: ModelMetadata,
}

impl MockEmbeddingModel {
    /// Create a new mock model
    pub fn new(name: impl Into<String>, dimension: usize) -> Self {
        Self {
            name: name.into(),
            version: "1.0.0".to_string(),
            dimension,
            metadata: ModelMetadata {
                description: "Mock embedding model for testing".to_string(),
                ..Default::default()
            },
        }
    }

    fn generate_embedding(&self, text: &str) -> Vec<f32> {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        text.hash(&mut hasher);
        let seed = hasher.finish();

        let mut state = seed;
        (0..self.dimension)
            .map(|_| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                ((state >> 33) as f32 / u32::MAX as f32) * 2.0 - 1.0
            })
            .collect()
    }
}

#[async_trait]
impl CustomEmbeddingModel for MockEmbeddingModel {
    fn name(&self) -> &str {
        &self.name
    }

    fn version(&self) -> &str {
        &self.version
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    fn metadata(&self) -> &ModelMetadata {
        &self.metadata
    }

    async fn embed(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        Ok(self.generate_embedding(text))
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        Ok(texts.iter().map(|t| self.generate_embedding(t)).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_model() {
        let model = MockEmbeddingModel::new("test-model", 384);

        assert_eq!(model.name(), "test-model");
        assert_eq!(model.dimension(), 384);

        let embedding = model.embed("test text").await.unwrap();
        assert_eq!(embedding.len(), 384);
    }

    #[tokio::test]
    async fn test_model_registry() {
        let config = ByoeConfig {
            validation_enabled: false,
            ..Default::default()
        };
        let registry = ModelRegistry::new(config);

        let model = Arc::new(MockEmbeddingModel::new("mock", 384));
        registry.register("mock".to_string(), model).await.unwrap();

        let models = registry.list_models();
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].name, "mock");
    }

    #[tokio::test]
    async fn test_model_embedding_via_registry() {
        let config = ByoeConfig {
            validation_enabled: false,
            default_model: "mock".to_string(),
            ..Default::default()
        };
        let registry = ModelRegistry::new(config);

        let model = Arc::new(MockEmbeddingModel::new("mock", 384));
        registry.register("mock".to_string(), model).await.unwrap();
        registry.set_active("mock".to_string()).unwrap();

        let embedding = registry.embed("test").await.unwrap();
        assert_eq!(embedding.len(), 384);

        let metrics = registry.get_metrics();
        assert_eq!(metrics.total_embeddings, 1);
    }

    #[tokio::test]
    async fn test_model_validation() {
        let config = ByoeConfig {
            validation_enabled: true,
            expected_dimension: Some(384),
            max_latency_ms: 10000,
            validation_samples: vec!["test".to_string()],
            ..Default::default()
        };
        let registry = ModelRegistry::new(config);

        let model = MockEmbeddingModel::new("test", 384);
        let result = registry.validate_model(&model).await.unwrap();

        assert!(result.passed);
    }

    #[tokio::test]
    async fn test_dimension_mismatch_validation() {
        let config = ByoeConfig {
            validation_enabled: true,
            expected_dimension: Some(512),
            max_latency_ms: 10000,
            validation_samples: vec!["test".to_string()],
            ..Default::default()
        };
        let registry = ModelRegistry::new(config);

        let model = MockEmbeddingModel::new("test", 384); // Wrong dimension
        let result = registry.validate_model(&model).await.unwrap();

        assert!(!result.passed);
    }

    #[tokio::test]
    async fn test_ab_testing() {
        let config = ByoeConfig {
            validation_enabled: false,
            ..Default::default()
        };
        let registry = ModelRegistry::new(config);

        let model_a = Arc::new(MockEmbeddingModel::new("model-a", 384));
        let model_b = Arc::new(MockEmbeddingModel::new("model-b", 384));

        registry
            .register("model-a".to_string(), model_a)
            .await
            .unwrap();
        registry
            .register("model-b".to_string(), model_b)
            .await
            .unwrap();

        let ab_config = AbTestConfig {
            model_a: "model-a".to_string(),
            model_b: "model-b".to_string(),
            traffic_split: 0.5,
            test_name: "test".to_string(),
            metrics: vec!["latency".to_string()],
        };

        registry.configure_ab_test(ab_config).unwrap();

        // Run multiple embeddings to test A/B split
        for _ in 0..10 {
            let _ = registry.embed("test").await;
        }

        registry.stop_ab_test();
    }

    #[tokio::test]
    async fn test_hot_swap() {
        let config = ByoeConfig {
            validation_enabled: false,
            hot_swap_enabled: true,
            ..Default::default()
        };
        let registry = ModelRegistry::new(config);

        let model1 = Arc::new(MockEmbeddingModel::new("model-1", 384));
        let model2 = Arc::new(MockEmbeddingModel::new("model-2", 512));

        registry
            .register("model-1".to_string(), model1)
            .await
            .unwrap();
        registry
            .register("model-2".to_string(), model2)
            .await
            .unwrap();

        registry.set_active("model-1".to_string()).unwrap();
        let emb1 = registry.embed("test").await.unwrap();
        assert_eq!(emb1.len(), 384);

        registry.set_active("model-2".to_string()).unwrap();
        let emb2 = registry.embed("test").await.unwrap();
        assert_eq!(emb2.len(), 512);

        let metrics = registry.get_metrics();
        assert_eq!(metrics.hot_swaps, 2); // Two set_active calls
    }

    #[tokio::test]
    async fn test_batch_embedding() {
        let config = ByoeConfig {
            validation_enabled: false,
            default_model: "mock".to_string(),
            ..Default::default()
        };
        let registry = ModelRegistry::new(config);

        let model = Arc::new(MockEmbeddingModel::new("mock", 384));
        registry.register("mock".to_string(), model).await.unwrap();
        registry.set_active("mock".to_string()).unwrap();

        let texts = vec![
            "text 1".to_string(),
            "text 2".to_string(),
            "text 3".to_string(),
        ];

        let embeddings = registry.embed_batch(&texts).await.unwrap();
        assert_eq!(embeddings.len(), 3);
        for emb in embeddings {
            assert_eq!(emb.len(), 384);
        }
    }
}
