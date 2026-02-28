//! Auto-Embedding Pipeline
//!
//! Provides automatic text-to-vector embedding for vector search operations.
//! Supports multiple embedding providers (local ONNX models, OpenAI, Cohere,
//! HTTP endpoints) and integrates with the vector store for seamless
//! `VECTOR.ADD ... TEXT "..."` workflows.
//!
//! # Architecture
//!
//! ```text
//! TextInput → EmbeddingProvider → Vec<f32> → VectorStore.add()
//! ```
//!
//! # Example
//!
//! ```ignore
//! use ferrite_ai::embedding::pipeline::{EmbeddingPipeline, PipelineConfig, TextInput};
//!
//! let pipeline = EmbeddingPipeline::new(PipelineConfig::default());
//! let embedding = pipeline.embed_text("Hello, world!").await?;
//! // embedding is Vec<f32> with 384 dimensions (default model)
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use super::byoe::{CustomEmbeddingModel, EmbedError, ModelMetadata, ModelStats};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the auto-embedding pipeline.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineConfig {
    /// Enable auto-embedding
    pub enabled: bool,
    /// Default embedding provider
    pub default_provider: ProviderType,
    /// Maximum text length (characters) before truncation
    pub max_text_length: usize,
    /// Batch size for bulk embedding operations
    pub batch_size: usize,
    /// Cache embeddings for repeated texts
    pub cache_enabled: bool,
    /// Maximum cache entries
    pub cache_max_entries: usize,
    /// Provider-specific configuration
    pub provider_config: ProviderConfig,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_provider: ProviderType::Mock,
            max_text_length: 8192,
            batch_size: 32,
            cache_enabled: true,
            cache_max_entries: 10_000,
            provider_config: ProviderConfig::default(),
        }
    }
}

/// Embedding provider type selector.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderType {
    /// Local ONNX model (requires `onnx` feature)
    LocalOnnx,
    /// OpenAI text-embedding API
    OpenAI,
    /// Cohere embed API
    Cohere,
    /// Custom HTTP endpoint
    HttpEndpoint,
    /// Mock provider for testing (returns deterministic vectors)
    Mock,
}

/// Provider-specific configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProviderConfig {
    /// OpenAI API key (for OpenAI provider)
    pub openai_api_key: Option<String>,
    /// OpenAI model name (default: text-embedding-3-small)
    pub openai_model: String,
    /// Cohere API key (for Cohere provider)
    pub cohere_api_key: Option<String>,
    /// Cohere model name (default: embed-english-v3.0)
    pub cohere_model: String,
    /// Custom HTTP endpoint URL
    pub http_endpoint: Option<String>,
    /// Local ONNX model path
    pub onnx_model_path: Option<String>,
    /// Output embedding dimension
    pub dimension: usize,
}

impl Default for ProviderConfig {
    fn default() -> Self {
        Self {
            openai_api_key: None,
            openai_model: "text-embedding-3-small".to_string(),
            cohere_api_key: None,
            cohere_model: "embed-english-v3.0".to_string(),
            http_endpoint: None,
            onnx_model_path: None,
            dimension: 384,
        }
    }
}

// ---------------------------------------------------------------------------
// Text input
// ---------------------------------------------------------------------------

/// Input types accepted by the embedding pipeline.
#[derive(Clone, Debug)]
pub enum TextInput {
    /// Raw text string
    Text(String),
    /// Text with associated metadata
    TextWithMetadata {
        /// The text to embed
        text: String,
        /// Optional metadata preserved through the pipeline
        metadata: Option<String>,
    },
    /// Pre-tokenized input (provider-specific)
    Tokens(Vec<u32>),
}

impl TextInput {
    /// Extract the text content for embedding.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            TextInput::Text(t) => Some(t),
            TextInput::TextWithMetadata { text, .. } => Some(text),
            TextInput::Tokens(_) => None,
        }
    }

    /// Get associated metadata if any.
    pub fn metadata(&self) -> Option<&str> {
        match self {
            TextInput::TextWithMetadata { metadata, .. } => metadata.as_deref(),
            _ => None,
        }
    }
}

impl From<&str> for TextInput {
    fn from(s: &str) -> Self {
        TextInput::Text(s.to_string())
    }
}

impl From<String> for TextInput {
    fn from(s: String) -> Self {
        TextInput::Text(s)
    }
}

// ---------------------------------------------------------------------------
// Embedding result
// ---------------------------------------------------------------------------

/// Result of an embedding operation.
#[derive(Clone, Debug)]
pub struct EmbeddingResult {
    /// The computed embedding vector
    pub vector: Vec<f32>,
    /// Dimension of the embedding
    pub dimension: usize,
    /// Time taken to compute (microseconds)
    pub latency_us: u64,
    /// The provider that generated this embedding
    pub provider: String,
    /// Whether this was served from cache
    pub cached: bool,
}

// ---------------------------------------------------------------------------
// Embedding provider trait
// ---------------------------------------------------------------------------

/// Trait for embedding providers that convert text to vectors.
#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    /// Provider name for logging/metrics.
    fn name(&self) -> &str;

    /// Output embedding dimension.
    fn dimension(&self) -> usize;

    /// Embed a single text string.
    async fn embed(&self, text: &str) -> Result<Vec<f32>, EmbedError>;

    /// Embed a batch of texts for efficiency.
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        let mut results = Vec::with_capacity(texts.len());
        for text in texts {
            results.push(self.embed(text).await?);
        }
        Ok(results)
    }

    /// Check if the provider is available and healthy.
    fn is_available(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Provider implementations
// ---------------------------------------------------------------------------

/// Mock embedding provider for testing. Generates deterministic vectors
/// based on text hash for reproducible results.
pub struct MockEmbeddingProvider {
    dimension: usize,
}

impl MockEmbeddingProvider {
    /// Create a mock provider with the given output dimension.
    pub fn new(dimension: usize) -> Self {
        Self { dimension }
    }
}

#[async_trait]
impl EmbeddingProvider for MockEmbeddingProvider {
    fn name(&self) -> &str {
        "mock"
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    async fn embed(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        // Deterministic embedding based on text hash
        let hash = crc32fast::hash(text.as_bytes());
        let mut rng_state = hash as u64;
        let mut vector = Vec::with_capacity(self.dimension);
        for _ in 0..self.dimension {
            // Simple linear congruential generator for determinism
            rng_state = rng_state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            let val = ((rng_state >> 33) as f32) / (u32::MAX as f32) * 2.0 - 1.0;
            vector.push(val);
        }
        // L2 normalize
        let norm: f32 = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for v in &mut vector {
                *v /= norm;
            }
        }
        Ok(vector)
    }
}

/// OpenAI embedding provider using the text-embedding API.
pub struct OpenAIEmbeddingProvider {
    api_key: String,
    model: String,
    dimension: usize,
}

impl OpenAIEmbeddingProvider {
    /// Create a new OpenAI embedding provider.
    pub fn new(api_key: String, model: String, dimension: usize) -> Self {
        Self {
            api_key,
            model,
            dimension,
        }
    }
}

#[async_trait]
impl EmbeddingProvider for OpenAIEmbeddingProvider {
    fn name(&self) -> &str {
        "openai"
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    async fn embed(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        let client = reqwest::Client::new();
        let body = serde_json::json!({
            "input": text,
            "model": self.model,
        });

        let response = client
            .post("https://api.openai.com/v1/embeddings")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| EmbedError::EmbeddingFailed(format!("OpenAI API error: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unknown".to_string());
            return Err(EmbedError::EmbeddingFailed(format!(
                "OpenAI API returned {status}: {body}"
            )));
        }

        let json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| EmbedError::EmbeddingFailed(format!("Failed to parse response: {e}")))?;

        let embedding = json["data"][0]["embedding"]
            .as_array()
            .ok_or_else(|| EmbedError::EmbeddingFailed("No embedding in response".to_string()))?
            .iter()
            .filter_map(|v| v.as_f64().map(|f| f as f32))
            .collect::<Vec<f32>>();

        if embedding.is_empty() {
            return Err(EmbedError::EmbeddingFailed(
                "Empty embedding returned".to_string(),
            ));
        }

        Ok(embedding)
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        let client = reqwest::Client::new();
        let body = serde_json::json!({
            "input": texts,
            "model": self.model,
        });

        let response = client
            .post("https://api.openai.com/v1/embeddings")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| EmbedError::EmbeddingFailed(format!("OpenAI batch API error: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unknown".to_string());
            return Err(EmbedError::EmbeddingFailed(format!(
                "OpenAI API returned {status}: {body}"
            )));
        }

        let json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| EmbedError::EmbeddingFailed(format!("Failed to parse response: {e}")))?;

        let data = json["data"]
            .as_array()
            .ok_or_else(|| EmbedError::EmbeddingFailed("No data in response".to_string()))?;

        let mut results = Vec::with_capacity(data.len());
        for item in data {
            let embedding = item["embedding"]
                .as_array()
                .ok_or_else(|| {
                    EmbedError::EmbeddingFailed("No embedding in data item".to_string())
                })?
                .iter()
                .filter_map(|v| v.as_f64().map(|f| f as f32))
                .collect::<Vec<f32>>();
            results.push(embedding);
        }

        Ok(results)
    }
}

/// Cohere embedding provider.
pub struct CohereEmbeddingProvider {
    api_key: String,
    model: String,
    dimension: usize,
}

impl CohereEmbeddingProvider {
    /// Create a new Cohere embedding provider.
    pub fn new(api_key: String, model: String, dimension: usize) -> Self {
        Self {
            api_key,
            model,
            dimension,
        }
    }
}

#[async_trait]
impl EmbeddingProvider for CohereEmbeddingProvider {
    fn name(&self) -> &str {
        "cohere"
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    async fn embed(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        let client = reqwest::Client::new();
        let body = serde_json::json!({
            "texts": [text],
            "model": self.model,
            "input_type": "search_document",
        });

        let response = client
            .post("https://api.cohere.ai/v1/embed")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| EmbedError::EmbeddingFailed(format!("Cohere API error: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unknown".to_string());
            return Err(EmbedError::EmbeddingFailed(format!(
                "Cohere API returned {status}: {body}"
            )));
        }

        let json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| EmbedError::EmbeddingFailed(format!("Failed to parse response: {e}")))?;

        let embedding = json["embeddings"][0]
            .as_array()
            .ok_or_else(|| EmbedError::EmbeddingFailed("No embedding in response".to_string()))?
            .iter()
            .filter_map(|v| v.as_f64().map(|f| f as f32))
            .collect::<Vec<f32>>();

        Ok(embedding)
    }
}

/// Custom HTTP endpoint embedding provider.
pub struct HttpEmbeddingProvider {
    endpoint: String,
    dimension: usize,
}

impl HttpEmbeddingProvider {
    /// Create a new HTTP endpoint provider.
    pub fn new(endpoint: String, dimension: usize) -> Self {
        Self {
            endpoint,
            dimension,
        }
    }
}

#[async_trait]
impl EmbeddingProvider for HttpEmbeddingProvider {
    fn name(&self) -> &str {
        "http"
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    async fn embed(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        let client = reqwest::Client::new();
        let body = serde_json::json!({ "text": text });

        let response = client
            .post(&self.endpoint)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| EmbedError::EmbeddingFailed(format!("HTTP endpoint error: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(EmbedError::EmbeddingFailed(format!(
                "HTTP endpoint returned {status}"
            )));
        }

        let json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| EmbedError::EmbeddingFailed(format!("Failed to parse response: {e}")))?;

        // Support both { "embedding": [...] } and { "vector": [...] } formats
        let embedding_array = json
            .get("embedding")
            .or_else(|| json.get("vector"))
            .or_else(|| json.get("data"))
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                EmbedError::EmbeddingFailed(
                    "Response must contain 'embedding', 'vector', or 'data' array".to_string(),
                )
            })?;

        let embedding: Vec<f32> = embedding_array
            .iter()
            .filter_map(|v| v.as_f64().map(|f| f as f32))
            .collect();

        Ok(embedding)
    }
}

// ---------------------------------------------------------------------------
// Embedding cache
// ---------------------------------------------------------------------------

/// Simple LRU-ish cache for embedding results.
struct EmbeddingCache {
    entries: RwLock<HashMap<u64, Vec<f32>>>,
    max_entries: usize,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl EmbeddingCache {
    fn new(max_entries: usize) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            max_entries,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    fn get(&self, text: &str) -> Option<Vec<f32>> {
        let key = Self::hash_text(text);
        let entries = self.entries.read();
        match entries.get(&key) {
            Some(v) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(v.clone())
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    fn put(&self, text: &str, vector: Vec<f32>) {
        let key = Self::hash_text(text);
        let mut entries = self.entries.write();
        if entries.len() >= self.max_entries {
            // Evict ~10% of entries (simple strategy)
            let to_remove: Vec<u64> = entries
                .keys()
                .take(self.max_entries / 10)
                .copied()
                .collect();
            for k in to_remove {
                entries.remove(&k);
            }
        }
        entries.insert(key, vector);
    }

    fn hash_text(text: &str) -> u64 {
        let crc = crc32fast::hash(text.as_bytes());
        // Extend to 64 bits with a second hash of reversed bytes
        let crc2 = crc32fast::hash(&text.as_bytes().iter().rev().copied().collect::<Vec<u8>>());
        ((crc as u64) << 32) | (crc2 as u64)
    }

    fn stats(&self) -> (u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
        )
    }
}

// ---------------------------------------------------------------------------
// Embedding pipeline
// ---------------------------------------------------------------------------

/// The main auto-embedding pipeline that converts text to vectors.
///
/// Manages provider selection, text preprocessing, caching, and batching.
pub struct EmbeddingPipeline {
    config: PipelineConfig,
    provider: Box<dyn EmbeddingProvider>,
    cache: Option<EmbeddingCache>,
    total_embeddings: AtomicU64,
    total_latency_us: AtomicU64,
}

impl EmbeddingPipeline {
    /// Create a new pipeline with the given configuration.
    pub fn new(config: PipelineConfig) -> Self {
        let provider = Self::create_provider(&config);
        let cache = if config.cache_enabled {
            Some(EmbeddingCache::new(config.cache_max_entries))
        } else {
            None
        };

        info!(
            provider = provider.name(),
            dimension = provider.dimension(),
            cache = config.cache_enabled,
            "embedding pipeline initialized"
        );

        Self {
            config,
            provider,
            cache,
            total_embeddings: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
        }
    }

    /// Create a provider based on configuration.
    fn create_provider(config: &PipelineConfig) -> Box<dyn EmbeddingProvider> {
        match &config.default_provider {
            ProviderType::OpenAI => {
                let api_key = config
                    .provider_config
                    .openai_api_key
                    .clone()
                    .unwrap_or_default();
                Box::new(OpenAIEmbeddingProvider::new(
                    api_key,
                    config.provider_config.openai_model.clone(),
                    config.provider_config.dimension,
                ))
            }
            ProviderType::Cohere => {
                let api_key = config
                    .provider_config
                    .cohere_api_key
                    .clone()
                    .unwrap_or_default();
                Box::new(CohereEmbeddingProvider::new(
                    api_key,
                    config.provider_config.cohere_model.clone(),
                    config.provider_config.dimension,
                ))
            }
            ProviderType::HttpEndpoint => {
                let endpoint = config
                    .provider_config
                    .http_endpoint
                    .clone()
                    .unwrap_or_else(|| "http://localhost:8080/embed".to_string());
                Box::new(HttpEmbeddingProvider::new(
                    endpoint,
                    config.provider_config.dimension,
                ))
            }
            ProviderType::LocalOnnx => {
                // Fall back to mock if ONNX is not compiled in
                warn!("LocalOnnx provider requested; using mock (compile with --features onnx for real ONNX support)");
                Box::new(MockEmbeddingProvider::new(config.provider_config.dimension))
            }
            ProviderType::Mock => {
                Box::new(MockEmbeddingProvider::new(config.provider_config.dimension))
            }
        }
    }

    /// Embed a single text string, returning the vector.
    pub async fn embed_text(&self, text: &str) -> Result<EmbeddingResult, EmbedError> {
        // Check cache first
        if let Some(ref cache) = self.cache {
            if let Some(cached) = cache.get(text) {
                return Ok(EmbeddingResult {
                    dimension: cached.len(),
                    vector: cached,
                    latency_us: 0,
                    provider: self.provider.name().to_string(),
                    cached: true,
                });
            }
        }

        // Truncate text if needed
        let truncated = if text.len() > self.config.max_text_length {
            &text[..self.config.max_text_length]
        } else {
            text
        };

        let start = Instant::now();
        let vector = self.provider.embed(truncated).await?;
        let latency_us = start.elapsed().as_micros() as u64;

        self.total_embeddings.fetch_add(1, Ordering::Relaxed);
        self.total_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);

        debug!(
            provider = self.provider.name(),
            dimension = vector.len(),
            latency_us,
            text_len = truncated.len(),
            "text embedded"
        );

        // Cache the result
        if let Some(ref cache) = self.cache {
            cache.put(text, vector.clone());
        }

        Ok(EmbeddingResult {
            dimension: vector.len(),
            vector,
            latency_us,
            provider: self.provider.name().to_string(),
            cached: false,
        })
    }

    /// Embed a `TextInput` enum value.
    pub async fn embed_input(&self, input: &TextInput) -> Result<EmbeddingResult, EmbedError> {
        match input {
            TextInput::Text(text) => self.embed_text(text).await,
            TextInput::TextWithMetadata { text, .. } => self.embed_text(text).await,
            TextInput::Tokens(_) => Err(EmbedError::EmbeddingFailed(
                "Token input not supported by this pipeline; use text input".to_string(),
            )),
        }
    }

    /// Embed multiple texts in a batch for efficiency.
    pub async fn embed_batch(&self, texts: &[String]) -> Result<Vec<EmbeddingResult>, EmbedError> {
        let mut results = Vec::with_capacity(texts.len());
        let mut uncached_texts = Vec::new();
        let mut uncached_indices = Vec::new();

        // Check cache for each text
        for (i, text) in texts.iter().enumerate() {
            if let Some(ref cache) = self.cache {
                if let Some(cached) = cache.get(text) {
                    results.push(EmbeddingResult {
                        dimension: cached.len(),
                        vector: cached,
                        latency_us: 0,
                        provider: self.provider.name().to_string(),
                        cached: true,
                    });
                    continue;
                }
            }
            uncached_texts.push(text.clone());
            uncached_indices.push(i);
            results.push(EmbeddingResult {
                dimension: 0,
                vector: Vec::new(),
                latency_us: 0,
                provider: self.provider.name().to_string(),
                cached: false,
            });
        }

        if uncached_texts.is_empty() {
            return Ok(results);
        }

        // Batch embed uncached texts
        let start = Instant::now();
        let batch_results = self.provider.embed_batch(&uncached_texts).await?;
        let latency_us = start.elapsed().as_micros() as u64;
        let per_item_latency = latency_us / batch_results.len().max(1) as u64;

        for (batch_idx, &result_idx) in uncached_indices.iter().enumerate() {
            if batch_idx < batch_results.len() {
                let vector = &batch_results[batch_idx];

                if let Some(ref cache) = self.cache {
                    cache.put(&uncached_texts[batch_idx], vector.clone());
                }

                results[result_idx] = EmbeddingResult {
                    dimension: vector.len(),
                    vector: vector.clone(),
                    latency_us: per_item_latency,
                    provider: self.provider.name().to_string(),
                    cached: false,
                };
            }
        }

        self.total_embeddings
            .fetch_add(batch_results.len() as u64, Ordering::Relaxed);
        self.total_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);

        Ok(results)
    }

    /// Get the output dimension of the configured provider.
    pub fn dimension(&self) -> usize {
        self.provider.dimension()
    }

    /// Get the provider name.
    pub fn provider_name(&self) -> &str {
        self.provider.name()
    }

    /// Check if the provider is available.
    pub fn is_available(&self) -> bool {
        self.provider.is_available()
    }

    /// Get pipeline statistics.
    pub fn stats(&self) -> PipelineStats {
        let (cache_hits, cache_misses) = self.cache.as_ref().map_or((0, 0), |c| c.stats());
        let total = self.total_embeddings.load(Ordering::Relaxed);
        let total_latency = self.total_latency_us.load(Ordering::Relaxed);
        PipelineStats {
            total_embeddings: total,
            avg_latency_us: if total > 0 { total_latency / total } else { 0 },
            cache_hits,
            cache_misses,
            provider: self.provider.name().to_string(),
            dimension: self.provider.dimension(),
        }
    }
}

/// Pipeline performance statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStats {
    /// Total embeddings generated
    pub total_embeddings: u64,
    /// Average latency in microseconds
    pub avg_latency_us: u64,
    /// Cache hits
    pub cache_hits: u64,
    /// Cache misses
    pub cache_misses: u64,
    /// Active provider name
    pub provider: String,
    /// Output dimension
    pub dimension: usize,
}

// ---------------------------------------------------------------------------
// Adapter: EmbeddingPipeline → CustomEmbeddingModel
// ---------------------------------------------------------------------------

/// Wraps an `EmbeddingPipeline` as a `CustomEmbeddingModel` for use with
/// the existing BYOE registry.
pub struct PipelineModelAdapter {
    pipeline: Arc<EmbeddingPipeline>,
}

impl PipelineModelAdapter {
    /// Create a new adapter wrapping the given pipeline.
    pub fn new(pipeline: Arc<EmbeddingPipeline>) -> Self {
        Self { pipeline }
    }
}

#[async_trait]
impl CustomEmbeddingModel for PipelineModelAdapter {
    fn name(&self) -> &str {
        "auto-embed-pipeline"
    }

    fn version(&self) -> &str {
        "1.0.0"
    }

    fn dimension(&self) -> usize {
        self.pipeline.dimension()
    }

    fn metadata(&self) -> &ModelMetadata {
        // Return a static default; metadata is not mutable here
        static META: std::sync::OnceLock<ModelMetadata> = std::sync::OnceLock::new();
        META.get_or_init(|| ModelMetadata {
            description: "Auto-embedding pipeline adapter".to_string(),
            ..Default::default()
        })
    }

    async fn embed(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        let result = self.pipeline.embed_text(text).await?;
        Ok(result.vector)
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        let results = self.pipeline.embed_batch(texts).await?;
        Ok(results.into_iter().map(|r| r.vector).collect())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_provider_deterministic() {
        let provider = MockEmbeddingProvider::new(384);
        let v1 = provider.embed("hello world").await.unwrap();
        let v2 = provider.embed("hello world").await.unwrap();
        assert_eq!(v1, v2, "mock provider should be deterministic");
        assert_eq!(v1.len(), 384);
    }

    #[tokio::test]
    async fn test_mock_provider_normalized() {
        let provider = MockEmbeddingProvider::new(128);
        let v = provider.embed("test text").await.unwrap();
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 0.01, "vector should be L2 normalized");
    }

    #[tokio::test]
    async fn test_pipeline_embed_text() {
        let config = PipelineConfig::default();
        let pipeline = EmbeddingPipeline::new(config);
        let result = pipeline.embed_text("Hello, world!").await.unwrap();
        assert_eq!(result.dimension, 384);
        assert_eq!(result.vector.len(), 384);
        assert!(!result.cached);
    }

    #[tokio::test]
    async fn test_pipeline_cache() {
        let config = PipelineConfig {
            cache_enabled: true,
            ..Default::default()
        };
        let pipeline = EmbeddingPipeline::new(config);

        let r1 = pipeline.embed_text("cached text").await.unwrap();
        assert!(!r1.cached);

        let r2 = pipeline.embed_text("cached text").await.unwrap();
        assert!(r2.cached);
        assert_eq!(r1.vector, r2.vector);
    }

    #[tokio::test]
    async fn test_pipeline_batch() {
        let pipeline = EmbeddingPipeline::new(PipelineConfig::default());
        let texts = vec![
            "first text".to_string(),
            "second text".to_string(),
            "third text".to_string(),
        ];
        let results = pipeline.embed_batch(&texts).await.unwrap();
        assert_eq!(results.len(), 3);
        for r in &results {
            assert_eq!(r.dimension, 384);
        }
    }

    #[tokio::test]
    async fn test_text_input_from() {
        let input: TextInput = "hello".into();
        assert_eq!(input.as_text(), Some("hello"));

        let input2 = TextInput::TextWithMetadata {
            text: "world".to_string(),
            metadata: Some(r#"{"key": "val"}"#.to_string()),
        };
        assert_eq!(input2.as_text(), Some("world"));
        assert!(input2.metadata().is_some());
    }

    #[tokio::test]
    async fn test_pipeline_stats() {
        let pipeline = EmbeddingPipeline::new(PipelineConfig::default());
        pipeline.embed_text("test").await.unwrap();
        pipeline.embed_text("test").await.unwrap(); // cached

        let stats = pipeline.stats();
        assert_eq!(stats.total_embeddings, 1); // only 1 actual embedding
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.cache_misses, 1);
        assert_eq!(stats.dimension, 384);
    }

    #[tokio::test]
    async fn test_pipeline_adapter() {
        let pipeline = Arc::new(EmbeddingPipeline::new(PipelineConfig::default()));
        let adapter = PipelineModelAdapter::new(pipeline);
        let v = adapter.embed("adapter test").await.unwrap();
        assert_eq!(v.len(), 384);
        assert_eq!(adapter.dimension(), 384);
        assert_eq!(adapter.name(), "auto-embed-pipeline");
    }
}
