//! Embedding Generation
//!
//! Provides integration with embedding providers for converting
//! text to vector representations.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::chunk::Chunk;

/// Embedding result
#[derive(Debug, Clone)]
pub struct EmbeddingResult {
    /// Input text
    pub text: String,
    /// Generated embedding
    pub embedding: Vec<f32>,
    /// Dimension of the embedding
    pub dimension: usize,
    /// Generation time in milliseconds
    pub latency_ms: f64,
    /// Token count (if available)
    pub tokens: Option<usize>,
}

/// Embedding provider configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingConfig {
    /// Provider type
    pub provider: EmbeddingProviderType,
    /// Model name
    pub model: String,
    /// API key (for remote providers)
    pub api_key: Option<String>,
    /// API endpoint (for custom providers)
    pub endpoint: Option<String>,
    /// Embedding dimension
    pub dimension: usize,
    /// Batch size for bulk embeddings
    pub batch_size: usize,
    /// Request timeout
    pub timeout_secs: u64,
    /// Enable caching
    pub cache_enabled: bool,
    /// Maximum cache entries
    pub cache_size: usize,
}

impl Default for EmbeddingConfig {
    fn default() -> Self {
        Self {
            provider: EmbeddingProviderType::Mock,
            model: "mock".to_string(),
            api_key: None,
            endpoint: None,
            dimension: 384,
            batch_size: 32,
            timeout_secs: 30,
            cache_enabled: true,
            cache_size: 10000,
        }
    }
}

impl EmbeddingConfig {
    /// Create OpenAI configuration
    pub fn openai(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            provider: EmbeddingProviderType::OpenAI,
            model: model.into(),
            api_key: Some(api_key.into()),
            dimension: 1536, // text-embedding-3-small default
            ..Default::default()
        }
    }

    /// Create local provider configuration
    pub fn local(model_path: impl Into<String>, dimension: usize) -> Self {
        Self {
            provider: EmbeddingProviderType::Local,
            model: model_path.into(),
            dimension,
            ..Default::default()
        }
    }
}

/// Embedding provider types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EmbeddingProviderType {
    /// Mock provider for testing
    Mock,
    /// OpenAI embeddings API
    OpenAI,
    /// Local model (ONNX, etc.)
    Local,
    /// Custom HTTP endpoint
    Custom,
    /// Cohere embeddings
    Cohere,
    /// Hugging Face Inference API
    HuggingFace,
}

/// Trait for embedding providers
#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    /// Get provider name
    #[allow(dead_code)] // Planned for v0.2 — used for provider identification in logs
    fn name(&self) -> &str;

    /// Get embedding dimension
    #[allow(dead_code)] // Planned for v0.2 — used for vector dimension validation
    fn dimension(&self) -> usize;

    /// Generate embedding for a single text
    async fn embed(&self, text: &str) -> Result<EmbeddingResult, EmbeddingError>;

    /// Generate embeddings for multiple texts
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<EmbeddingResult>, EmbeddingError>;

    /// Get provider statistics
    #[allow(dead_code)] // Planned for v0.2 — used for provider metrics reporting
    fn stats(&self) -> &EmbedderStats;
}

/// Embedding errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum EmbeddingError {
    /// API error
    #[error("API error: {0}")]
    Api(String),

    /// Rate limited
    #[error("rate limited, retry after {0} seconds")]
    RateLimited(u64),

    /// Invalid input
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// Timeout
    #[error("request timeout")]
    Timeout,

    /// Provider not configured
    #[error("provider not configured: {0}")]
    NotConfigured(String),

    /// Model error
    #[error("model error: {0}")]
    ModelError(String),
}

/// Embedding statistics
#[derive(Debug, Default)]
pub struct EmbedderStats {
    /// Total embeddings generated
    pub embeddings_generated: AtomicU64,
    /// Total tokens processed
    pub tokens_processed: AtomicU64,
    /// Total batches processed
    pub batches_processed: AtomicU64,
    /// Cache hits
    pub cache_hits: AtomicU64,
    /// Cache misses
    pub cache_misses: AtomicU64,
    /// Total latency in milliseconds
    pub total_latency_ms: AtomicU64,
    /// Error count
    pub errors: AtomicU64,
}

impl EmbedderStats {
    /// Get average latency
    pub fn avg_latency_ms(&self) -> f64 {
        let total = self.embeddings_generated.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        self.total_latency_ms.load(Ordering::Relaxed) as f64 / total as f64
    }

    /// Get cache hit rate
    pub fn cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            return 0.0;
        }
        hits as f64 / total as f64
    }
}

/// Main embedder with caching and batching
pub struct Embedder {
    /// Configuration
    config: EmbeddingConfig,
    /// Provider implementation
    provider: Box<dyn EmbeddingProvider>,
    /// Embedding cache
    cache: Option<Arc<RwLock<EmbeddingCache>>>,
    /// Statistics
    stats: Arc<EmbedderStats>,
}

/// Simple LRU-like embedding cache
struct EmbeddingCache {
    /// Cache entries (text hash -> embedding)
    entries: HashMap<u64, CacheEntry>,
    /// Maximum entries
    max_size: usize,
}

struct CacheEntry {
    embedding: Vec<f32>,
    last_used: Instant,
}

impl EmbeddingCache {
    fn new(max_size: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_size,
        }
    }

    fn get(&mut self, key: u64) -> Option<Vec<f32>> {
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.last_used = Instant::now();
            Some(entry.embedding.clone())
        } else {
            None
        }
    }

    fn insert(&mut self, key: u64, embedding: Vec<f32>) {
        // Evict oldest if full
        if self.entries.len() >= self.max_size {
            if let Some(oldest_key) = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| *k)
            {
                self.entries.remove(&oldest_key);
            }
        }

        self.entries.insert(
            key,
            CacheEntry {
                embedding,
                last_used: Instant::now(),
            },
        );
    }

    fn hash_text(text: &str) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        text.hash(&mut hasher);
        hasher.finish()
    }
}

impl Embedder {
    /// Create a new embedder
    pub fn new(config: EmbeddingConfig) -> Self {
        let provider: Box<dyn EmbeddingProvider> = match config.provider {
            EmbeddingProviderType::Mock => Box::new(MockEmbeddingProvider::new(config.dimension)),
            EmbeddingProviderType::OpenAI => Box::new(OpenAIEmbeddingProvider::new(&config)),
            EmbeddingProviderType::Local => Box::new(LocalEmbeddingProvider::new(&config)),
            EmbeddingProviderType::Custom => Box::new(CustomEmbeddingProvider::new(&config)),
            EmbeddingProviderType::Cohere => Box::new(CohereEmbeddingProvider::new(&config)),
            EmbeddingProviderType::HuggingFace => {
                Box::new(HuggingFaceEmbeddingProvider::new(&config))
            }
        };

        let cache = if config.cache_enabled {
            Some(Arc::new(RwLock::new(EmbeddingCache::new(
                config.cache_size,
            ))))
        } else {
            None
        };

        Self {
            config,
            provider,
            cache,
            stats: Arc::new(EmbedderStats::default()),
        }
    }

    /// Embed a single text
    pub async fn embed(&self, text: &str) -> Result<EmbeddingResult, EmbeddingError> {
        // Check cache
        if let Some(cache) = &self.cache {
            let key = EmbeddingCache::hash_text(text);
            if let Some(embedding) = cache.write().get(key) {
                self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(EmbeddingResult {
                    text: text.to_string(),
                    embedding,
                    dimension: self.config.dimension,
                    latency_ms: 0.0,
                    tokens: None,
                });
            }
            self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
        }

        // Generate embedding
        let result = self.provider.embed(text).await?;

        // Cache result
        if let Some(cache) = &self.cache {
            let key = EmbeddingCache::hash_text(text);
            cache.write().insert(key, result.embedding.clone());
        }

        self.stats
            .embeddings_generated
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_latency_ms
            .fetch_add(result.latency_ms as u64, Ordering::Relaxed);

        Ok(result)
    }

    /// Embed multiple texts
    pub async fn embed_batch(
        &self,
        texts: &[String],
    ) -> Result<Vec<EmbeddingResult>, EmbeddingError> {
        let results = self.provider.embed_batch(texts).await?;

        // Cache results
        if let Some(cache) = &self.cache {
            let mut cache = cache.write();
            for result in &results {
                let key = EmbeddingCache::hash_text(&result.text);
                cache.insert(key, result.embedding.clone());
            }
        }

        self.stats
            .embeddings_generated
            .fetch_add(results.len() as u64, Ordering::Relaxed);
        self.stats.batches_processed.fetch_add(1, Ordering::Relaxed);

        Ok(results)
    }

    /// Embed chunks and set their embeddings
    pub async fn embed_chunks(&self, chunks: &mut [Chunk]) -> Result<(), EmbeddingError> {
        let texts: Vec<String> = chunks.iter().map(|c| c.content.clone()).collect();
        let results = self.embed_batch(&texts).await?;

        for (chunk, result) in chunks.iter_mut().zip(results.into_iter()) {
            chunk.set_embedding(result.embedding);
        }

        Ok(())
    }

    /// Get embedding dimension
    pub fn dimension(&self) -> usize {
        self.config.dimension
    }

    /// Get statistics
    pub fn stats(&self) -> &Arc<EmbedderStats> {
        &self.stats
    }

    /// Get configuration
    pub fn config(&self) -> &EmbeddingConfig {
        &self.config
    }
}

/// Mock embedding provider for testing
pub struct MockEmbeddingProvider {
    dimension: usize,
    stats: EmbedderStats,
}

impl MockEmbeddingProvider {
    pub fn new(dimension: usize) -> Self {
        Self {
            dimension,
            stats: EmbedderStats::default(),
        }
    }

    fn generate_mock_embedding(&self, text: &str) -> Vec<f32> {
        // Generate deterministic embedding based on text hash
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        text.hash(&mut hasher);
        let seed = hasher.finish();

        let mut rng_state = seed;
        (0..self.dimension)
            .map(|_| {
                rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
                ((rng_state >> 33) as f32 / u32::MAX as f32) * 2.0 - 1.0
            })
            .collect()
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

    async fn embed(&self, text: &str) -> Result<EmbeddingResult, EmbeddingError> {
        let start = Instant::now();
        let embedding = self.generate_mock_embedding(text);
        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

        self.stats
            .embeddings_generated
            .fetch_add(1, Ordering::Relaxed);

        Ok(EmbeddingResult {
            text: text.to_string(),
            embedding,
            dimension: self.dimension,
            latency_ms,
            tokens: Some(text.split_whitespace().count()),
        })
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<EmbeddingResult>, EmbeddingError> {
        let mut results = Vec::with_capacity(texts.len());
        for text in texts {
            results.push(self.embed(text).await?);
        }
        Ok(results)
    }

    fn stats(&self) -> &EmbedderStats {
        &self.stats
    }
}

/// OpenAI embedding provider
pub struct OpenAIEmbeddingProvider {
    config: EmbeddingConfig,
    client: reqwest::Client,
    stats: EmbedderStats,
}

/// OpenAI API request format
#[derive(Debug, Serialize)]
struct OpenAIEmbeddingRequest {
    model: String,
    input: Vec<String>,
    encoding_format: String,
}

/// OpenAI API response format
#[derive(Debug, Deserialize)]
struct OpenAIEmbeddingResponse {
    data: Vec<OpenAIEmbeddingData>,
    usage: Option<OpenAIUsage>,
}

#[derive(Debug, Deserialize)]
struct OpenAIEmbeddingData {
    embedding: Vec<f32>,
    index: usize,
}

#[derive(Debug, Deserialize)]
struct OpenAIUsage {
    #[allow(dead_code)] // Planned for v0.2 — stored for API usage tracking
    prompt_tokens: usize,
    total_tokens: usize,
}

/// OpenAI API error response
#[derive(Debug, Deserialize)]
struct OpenAIErrorResponse {
    error: OpenAIError,
}

#[derive(Debug, Deserialize)]
struct OpenAIError {
    message: String,
    #[serde(rename = "type")]
    #[allow(dead_code)] // Planned for v0.2 — stored for error classification
    error_type: Option<String>,
}

impl OpenAIEmbeddingProvider {
    /// Default OpenAI API endpoint
    const DEFAULT_ENDPOINT: &'static str = "https://api.openai.com/v1/embeddings";
    /// Maximum inputs per batch request
    const MAX_BATCH_SIZE: usize = 2048;

    pub fn new(config: &EmbeddingConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()
            .unwrap_or_default();

        Self {
            config: config.clone(),
            client,
            stats: EmbedderStats::default(),
        }
    }

    /// Make API request to OpenAI
    async fn call_api(
        &self,
        texts: Vec<String>,
    ) -> Result<OpenAIEmbeddingResponse, EmbeddingError> {
        let api_key =
            self.config.api_key.as_ref().ok_or_else(|| {
                EmbeddingError::NotConfigured("OpenAI API key required".to_string())
            })?;

        let endpoint = self
            .config
            .endpoint
            .as_deref()
            .unwrap_or(Self::DEFAULT_ENDPOINT);

        let request = OpenAIEmbeddingRequest {
            model: self.config.model.clone(),
            input: texts,
            encoding_format: "float".to_string(),
        };

        let response = self
            .client
            .post(endpoint)
            .header("Authorization", format!("Bearer {}", api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    EmbeddingError::Timeout
                } else {
                    EmbeddingError::Api(format!("Request failed: {}", e))
                }
            })?;

        let status = response.status();

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            // Extract retry-after header if available
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse().ok())
                .unwrap_or(60);
            return Err(EmbeddingError::RateLimited(retry_after));
        }

        if !status.is_success() {
            let error_body = response.text().await.unwrap_or_default();
            if let Ok(error_response) = serde_json::from_str::<OpenAIErrorResponse>(&error_body) {
                return Err(EmbeddingError::Api(error_response.error.message));
            }
            return Err(EmbeddingError::Api(format!(
                "HTTP {}: {}",
                status, error_body
            )));
        }

        response
            .json::<OpenAIEmbeddingResponse>()
            .await
            .map_err(|e| EmbeddingError::Api(format!("Failed to parse response: {}", e)))
    }
}

#[async_trait]
impl EmbeddingProvider for OpenAIEmbeddingProvider {
    fn name(&self) -> &str {
        "openai"
    }

    fn dimension(&self) -> usize {
        self.config.dimension
    }

    async fn embed(&self, text: &str) -> Result<EmbeddingResult, EmbeddingError> {
        let start = Instant::now();

        let response = self.call_api(vec![text.to_string()]).await?;

        let embedding = response
            .data
            .into_iter()
            .next()
            .ok_or_else(|| EmbeddingError::Api("Empty response from API".to_string()))?
            .embedding;

        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
        let tokens = response.usage.map(|u| u.total_tokens);

        self.stats
            .embeddings_generated
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_latency_ms
            .fetch_add(latency_ms as u64, Ordering::Relaxed);
        if let Some(t) = tokens {
            self.stats
                .tokens_processed
                .fetch_add(t as u64, Ordering::Relaxed);
        }

        Ok(EmbeddingResult {
            text: text.to_string(),
            embedding,
            dimension: self.config.dimension,
            latency_ms,
            tokens,
        })
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<EmbeddingResult>, EmbeddingError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let start = Instant::now();
        let mut all_results = Vec::with_capacity(texts.len());

        // Process in chunks of MAX_BATCH_SIZE
        for chunk in texts.chunks(Self::MAX_BATCH_SIZE) {
            let response = self.call_api(chunk.to_vec()).await?;

            // Sort by index to maintain order
            let mut data = response.data;
            data.sort_by_key(|d| d.index);

            let tokens = response.usage.map(|u| u.total_tokens);
            if let Some(t) = tokens {
                self.stats
                    .tokens_processed
                    .fetch_add(t as u64, Ordering::Relaxed);
            }

            for (i, item) in data.into_iter().enumerate() {
                all_results.push(EmbeddingResult {
                    text: chunk[i].clone(),
                    embedding: item.embedding,
                    dimension: self.config.dimension,
                    latency_ms: 0.0, // Will set total at end
                    tokens: None,
                });
            }
        }

        let total_latency_ms = start.elapsed().as_secs_f64() * 1000.0;
        let per_item_latency = total_latency_ms / all_results.len() as f64;

        // Update latency for all results
        for result in &mut all_results {
            result.latency_ms = per_item_latency;
        }

        self.stats
            .embeddings_generated
            .fetch_add(all_results.len() as u64, Ordering::Relaxed);
        self.stats
            .total_latency_ms
            .fetch_add(total_latency_ms as u64, Ordering::Relaxed);
        self.stats.batches_processed.fetch_add(1, Ordering::Relaxed);

        Ok(all_results)
    }

    fn stats(&self) -> &EmbedderStats {
        &self.stats
    }
}

/// Local embedding provider using ONNX Runtime
///
/// When the `onnx` feature is enabled, this provider loads an ONNX model
/// and runs inference locally. Without the feature, it falls back to mock embeddings.
pub struct LocalEmbeddingProvider {
    config: EmbeddingConfig,
    stats: EmbedderStats,
    #[cfg(feature = "onnx")]
    session: Option<parking_lot::Mutex<ort::session::Session>>,
}

impl LocalEmbeddingProvider {
    /// Create a new local embedding provider
    ///
    /// If the `onnx` feature is enabled and a valid model path is provided,
    /// the ONNX model will be loaded. Otherwise, falls back to mock embeddings.
    pub fn new(config: &EmbeddingConfig) -> Self {
        #[cfg(feature = "onnx")]
        let session = Self::load_onnx_model(&config.model);

        Self {
            config: config.clone(),
            stats: EmbedderStats::default(),
            #[cfg(feature = "onnx")]
            session,
        }
    }

    #[cfg(feature = "onnx")]
    fn load_onnx_model(model_path: &str) -> Option<parking_lot::Mutex<ort::session::Session>> {
        use ort::session::Session;
        use std::path::Path;

        if model_path.is_empty() || model_path == "mock" {
            return None;
        }

        let path = Path::new(model_path);
        if !path.exists() {
            tracing::warn!(
                "ONNX model not found at {}, falling back to mock",
                model_path
            );
            return None;
        }

        match Session::builder() {
            Ok(builder) => match builder.commit_from_file(model_path) {
                Ok(session) => {
                    tracing::info!("Loaded ONNX model from {}", model_path);
                    Some(parking_lot::Mutex::new(session))
                }
                Err(e) => {
                    tracing::error!("Failed to load ONNX model: {}", e);
                    None
                }
            },
            Err(e) => {
                tracing::error!("Failed to create ONNX session builder: {}", e);
                None
            }
        }
    }

    #[cfg(feature = "onnx")]
    fn run_onnx_inference(&self, _text: &str) -> Result<Vec<f32>, EmbeddingError> {
        // Verify model is loaded
        let _session_guard = self
            .session
            .as_ref()
            .ok_or_else(|| EmbeddingError::NotConfigured("ONNX model not loaded".to_string()))?;

        // TODO: Implement full ONNX inference when ort 2.0 API stabilizes
        // The ort 2.0.0-rc.x API has complex type requirements for inputs.
        // For now, log that the model is loaded and fall back to mock.
        //
        // Future implementation should:
        // 1. Use proper tokenizer (e.g., tokenizers crate) for text -> token IDs
        // 2. Create properly typed input tensors
        // 3. Run inference with session.run(ort::inputs![...])
        // 4. Extract embeddings from output
        //
        // See https://ort.pyke.io for latest API documentation

        tracing::debug!("ONNX model loaded but inference not yet implemented, using fallback");
        Err(EmbeddingError::ModelError(
            "ONNX inference pending ort 2.0 API stabilization".to_string(),
        ))
    }

    #[cfg(feature = "onnx")]
    // Used when onnx feature is enabled
    #[allow(dead_code)]
    fn extract_embedding_from_outputs(
        &self,
        outputs: ort::session::SessionOutputs<'_>,
        seq_len: usize,
    ) -> Result<Vec<f32>, EmbeddingError> {
        // Get the first output - most models have a single output
        let (_, output) = outputs
            .iter()
            .next()
            .ok_or_else(|| EmbeddingError::ModelError("No output found".to_string()))?;

        // Try to extract as f32 array
        let tensor = output
            .try_extract_array::<f32>()
            .map_err(|e| EmbeddingError::ModelError(format!("Failed to extract tensor: {}", e)))?;

        let shape = tensor.shape();

        // Handle different output shapes
        let embedding: Vec<f32> = if shape.len() == 2 {
            // Shape [1, dim] - sentence embedding
            tensor.iter().copied().collect()
        } else if shape.len() == 3 {
            // Shape [1, seq_len, hidden_dim] - need mean pooling
            let hidden_dim = shape[2];
            let mut pooled = vec![0.0f32; hidden_dim];
            for i in 0..seq_len.min(shape[1]) {
                for j in 0..hidden_dim {
                    pooled[j] += tensor[[0, i, j]];
                }
            }
            for val in &mut pooled {
                *val /= seq_len as f32;
            }
            pooled
        } else {
            return Err(EmbeddingError::ModelError(format!(
                "Unexpected output shape: {:?}",
                shape
            )));
        };

        // Normalize to configured dimension if needed
        if embedding.len() != self.config.dimension {
            // Truncate or pad
            let mut result = vec![0.0f32; self.config.dimension];
            for (i, val) in embedding.iter().take(self.config.dimension).enumerate() {
                result[i] = *val;
            }
            Ok(result)
        } else {
            Ok(embedding)
        }
    }
}

#[async_trait]
impl EmbeddingProvider for LocalEmbeddingProvider {
    fn name(&self) -> &str {
        "local"
    }

    fn dimension(&self) -> usize {
        self.config.dimension
    }

    async fn embed(&self, text: &str) -> Result<EmbeddingResult, EmbeddingError> {
        #[cfg(feature = "onnx")]
        let start = Instant::now();

        #[cfg(feature = "onnx")]
        {
            if self.session.is_some() {
                match self.run_onnx_inference(text) {
                    Ok(embedding) => {
                        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
                        self.stats
                            .embeddings_generated
                            .fetch_add(1, Ordering::Relaxed);
                        self.stats
                            .total_latency_ms
                            .fetch_add(latency_ms as u64, Ordering::Relaxed);

                        return Ok(EmbeddingResult {
                            text: text.to_string(),
                            embedding,
                            dimension: self.config.dimension,
                            latency_ms,
                            tokens: Some(text.split_whitespace().count()),
                        });
                    }
                    Err(e) => {
                        tracing::warn!("ONNX inference failed, falling back to mock: {}", e);
                        self.stats.errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }

        // Fallback to mock when ONNX not available or fails
        let mock = MockEmbeddingProvider::new(self.config.dimension);
        mock.embed(text).await
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<EmbeddingResult>, EmbeddingError> {
        let start = Instant::now();
        let mut results = Vec::with_capacity(texts.len());

        for text in texts {
            results.push(self.embed(text).await?);
        }

        let total_latency_ms = start.elapsed().as_secs_f64() * 1000.0;
        self.stats.batches_processed.fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_latency_ms
            .fetch_add(total_latency_ms as u64, Ordering::Relaxed);

        Ok(results)
    }

    fn stats(&self) -> &EmbedderStats {
        &self.stats
    }
}

/// Custom HTTP embedding provider
/// Supports any embedding API with configurable endpoint
pub struct CustomEmbeddingProvider {
    config: EmbeddingConfig,
    client: reqwest::Client,
    stats: EmbedderStats,
}

/// Generic request format for custom endpoints
#[derive(Debug, Serialize)]
struct CustomEmbeddingRequest {
    inputs: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
}

/// Generic response format - supports both array and object formats
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum CustomEmbeddingResponse {
    /// Direct array of embeddings
    DirectArray(Vec<Vec<f32>>),
    /// Object with embeddings field
    WithEmbeddings { embeddings: Vec<Vec<f32>> },
    /// Object with data field (OpenAI-like)
    WithData { data: Vec<CustomEmbeddingData> },
}

#[derive(Debug, Deserialize)]
struct CustomEmbeddingData {
    embedding: Vec<f32>,
}

impl CustomEmbeddingProvider {
    pub fn new(config: &EmbeddingConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()
            .unwrap_or_default();

        Self {
            config: config.clone(),
            client,
            stats: EmbedderStats::default(),
        }
    }

    async fn call_api(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        let endpoint = self.config.endpoint.as_ref().ok_or_else(|| {
            EmbeddingError::NotConfigured("Custom endpoint URL required".to_string())
        })?;

        let request = CustomEmbeddingRequest {
            inputs: texts,
            model: if self.config.model.is_empty() {
                None
            } else {
                Some(self.config.model.clone())
            },
        };

        let mut req_builder = self
            .client
            .post(endpoint)
            .header("Content-Type", "application/json")
            .json(&request);

        // Add API key if provided
        if let Some(api_key) = &self.config.api_key {
            req_builder = req_builder.header("Authorization", format!("Bearer {}", api_key));
        }

        let response = req_builder.send().await.map_err(|e| {
            if e.is_timeout() {
                EmbeddingError::Timeout
            } else {
                EmbeddingError::Api(format!("Request failed: {}", e))
            }
        })?;

        let status = response.status();
        if !status.is_success() {
            let error_body = response.text().await.unwrap_or_default();
            return Err(EmbeddingError::Api(format!(
                "HTTP {}: {}",
                status, error_body
            )));
        }

        let api_response: CustomEmbeddingResponse = response
            .json()
            .await
            .map_err(|e| EmbeddingError::Api(format!("Failed to parse response: {}", e)))?;

        match api_response {
            CustomEmbeddingResponse::DirectArray(embeddings) => Ok(embeddings),
            CustomEmbeddingResponse::WithEmbeddings { embeddings } => Ok(embeddings),
            CustomEmbeddingResponse::WithData { data } => {
                Ok(data.into_iter().map(|d| d.embedding).collect())
            }
        }
    }
}

#[async_trait]
impl EmbeddingProvider for CustomEmbeddingProvider {
    fn name(&self) -> &str {
        "custom"
    }

    fn dimension(&self) -> usize {
        self.config.dimension
    }

    async fn embed(&self, text: &str) -> Result<EmbeddingResult, EmbeddingError> {
        let start = Instant::now();

        let embeddings = self.call_api(vec![text.to_string()]).await?;
        let embedding = embeddings
            .into_iter()
            .next()
            .ok_or_else(|| EmbeddingError::Api("Empty response from API".to_string()))?;

        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

        self.stats
            .embeddings_generated
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_latency_ms
            .fetch_add(latency_ms as u64, Ordering::Relaxed);

        Ok(EmbeddingResult {
            text: text.to_string(),
            embedding,
            dimension: self.config.dimension,
            latency_ms,
            tokens: None,
        })
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<EmbeddingResult>, EmbeddingError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let start = Instant::now();
        let embeddings = self.call_api(texts.to_vec()).await?;
        let total_latency_ms = start.elapsed().as_secs_f64() * 1000.0;
        let per_item_latency = total_latency_ms / embeddings.len() as f64;

        let results: Vec<EmbeddingResult> = texts
            .iter()
            .zip(embeddings)
            .map(|(text, embedding)| EmbeddingResult {
                text: text.clone(),
                embedding,
                dimension: self.config.dimension,
                latency_ms: per_item_latency,
                tokens: None,
            })
            .collect();

        self.stats
            .embeddings_generated
            .fetch_add(results.len() as u64, Ordering::Relaxed);
        self.stats
            .total_latency_ms
            .fetch_add(total_latency_ms as u64, Ordering::Relaxed);
        self.stats.batches_processed.fetch_add(1, Ordering::Relaxed);

        Ok(results)
    }

    fn stats(&self) -> &EmbedderStats {
        &self.stats
    }
}

/// Cohere embedding provider
pub struct CohereEmbeddingProvider {
    config: EmbeddingConfig,
    client: reqwest::Client,
    stats: EmbedderStats,
}

/// Cohere API request format
#[derive(Debug, Serialize)]
struct CohereEmbeddingRequest {
    model: String,
    texts: Vec<String>,
    input_type: String,
    truncate: String,
}

/// Cohere API response format
#[derive(Debug, Deserialize)]
struct CohereEmbeddingResponse {
    embeddings: Vec<Vec<f32>>,
}

/// Cohere API error response
#[derive(Debug, Deserialize)]
struct CohereErrorResponse {
    message: String,
}

impl CohereEmbeddingProvider {
    const DEFAULT_ENDPOINT: &'static str = "https://api.cohere.ai/v1/embed";
    const DEFAULT_MODEL: &'static str = "embed-english-v3.0";
    const MAX_BATCH_SIZE: usize = 96; // Cohere's limit

    pub fn new(config: &EmbeddingConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()
            .unwrap_or_default();

        Self {
            config: config.clone(),
            client,
            stats: EmbedderStats::default(),
        }
    }

    async fn call_api(
        &self,
        texts: Vec<String>,
        input_type: &str,
    ) -> Result<CohereEmbeddingResponse, EmbeddingError> {
        let api_key =
            self.config.api_key.as_ref().ok_or_else(|| {
                EmbeddingError::NotConfigured("Cohere API key required".to_string())
            })?;

        let endpoint = self
            .config
            .endpoint
            .as_deref()
            .unwrap_or(Self::DEFAULT_ENDPOINT);

        let model = if self.config.model.is_empty() {
            Self::DEFAULT_MODEL.to_string()
        } else {
            self.config.model.clone()
        };

        let request = CohereEmbeddingRequest {
            model,
            texts,
            input_type: input_type.to_string(),
            truncate: "END".to_string(),
        };

        let response = self
            .client
            .post(endpoint)
            .header("Authorization", format!("Bearer {}", api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    EmbeddingError::Timeout
                } else {
                    EmbeddingError::Api(format!("Request failed: {}", e))
                }
            })?;

        let status = response.status();

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse().ok())
                .unwrap_or(60);
            return Err(EmbeddingError::RateLimited(retry_after));
        }

        if !status.is_success() {
            let error_body = response.text().await.unwrap_or_default();
            if let Ok(error_response) = serde_json::from_str::<CohereErrorResponse>(&error_body) {
                return Err(EmbeddingError::Api(error_response.message));
            }
            return Err(EmbeddingError::Api(format!(
                "HTTP {}: {}",
                status, error_body
            )));
        }

        response
            .json::<CohereEmbeddingResponse>()
            .await
            .map_err(|e| EmbeddingError::Api(format!("Failed to parse response: {}", e)))
    }
}

#[async_trait]
impl EmbeddingProvider for CohereEmbeddingProvider {
    fn name(&self) -> &str {
        "cohere"
    }

    fn dimension(&self) -> usize {
        self.config.dimension
    }

    async fn embed(&self, text: &str) -> Result<EmbeddingResult, EmbeddingError> {
        let start = Instant::now();

        let response = self
            .call_api(vec![text.to_string()], "search_document")
            .await?;
        let embedding = response
            .embeddings
            .into_iter()
            .next()
            .ok_or_else(|| EmbeddingError::Api("Empty response from API".to_string()))?;

        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

        self.stats
            .embeddings_generated
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_latency_ms
            .fetch_add(latency_ms as u64, Ordering::Relaxed);

        Ok(EmbeddingResult {
            text: text.to_string(),
            embedding,
            dimension: self.config.dimension,
            latency_ms,
            tokens: None,
        })
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<EmbeddingResult>, EmbeddingError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let start = Instant::now();
        let mut all_embeddings = Vec::with_capacity(texts.len());

        // Process in chunks of MAX_BATCH_SIZE
        for chunk in texts.chunks(Self::MAX_BATCH_SIZE) {
            let response = self.call_api(chunk.to_vec(), "search_document").await?;
            all_embeddings.extend(response.embeddings);
        }

        let total_latency_ms = start.elapsed().as_secs_f64() * 1000.0;
        let per_item_latency = total_latency_ms / all_embeddings.len() as f64;

        let results: Vec<EmbeddingResult> = texts
            .iter()
            .zip(all_embeddings)
            .map(|(text, embedding)| EmbeddingResult {
                text: text.clone(),
                embedding,
                dimension: self.config.dimension,
                latency_ms: per_item_latency,
                tokens: None,
            })
            .collect();

        self.stats
            .embeddings_generated
            .fetch_add(results.len() as u64, Ordering::Relaxed);
        self.stats
            .total_latency_ms
            .fetch_add(total_latency_ms as u64, Ordering::Relaxed);
        self.stats.batches_processed.fetch_add(1, Ordering::Relaxed);

        Ok(results)
    }

    fn stats(&self) -> &EmbedderStats {
        &self.stats
    }
}

/// HuggingFace Inference API embedding provider
pub struct HuggingFaceEmbeddingProvider {
    config: EmbeddingConfig,
    client: reqwest::Client,
    stats: EmbedderStats,
}

/// HuggingFace API request format
#[derive(Debug, Serialize)]
struct HuggingFaceRequest {
    inputs: Vec<String>,
}

impl HuggingFaceEmbeddingProvider {
    const DEFAULT_MODEL: &'static str = "sentence-transformers/all-MiniLM-L6-v2";

    pub fn new(config: &EmbeddingConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()
            .unwrap_or_default();

        Self {
            config: config.clone(),
            client,
            stats: EmbedderStats::default(),
        }
    }

    fn get_endpoint(&self) -> String {
        if let Some(endpoint) = &self.config.endpoint {
            endpoint.clone()
        } else {
            let model = if self.config.model.is_empty() {
                Self::DEFAULT_MODEL
            } else {
                &self.config.model
            };
            format!(
                "https://api-inference.huggingface.co/pipeline/feature-extraction/{}",
                model
            )
        }
    }

    async fn call_api(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        let api_key = self.config.api_key.as_ref().ok_or_else(|| {
            EmbeddingError::NotConfigured("HuggingFace API token required".to_string())
        })?;

        let endpoint = self.get_endpoint();
        let request = HuggingFaceRequest { inputs: texts };

        let response = self
            .client
            .post(&endpoint)
            .header("Authorization", format!("Bearer {}", api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    EmbeddingError::Timeout
                } else {
                    EmbeddingError::Api(format!("Request failed: {}", e))
                }
            })?;

        let status = response.status();

        // Handle model loading (503)
        if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Err(EmbeddingError::Api(
                "Model is loading. Please retry in a few seconds.".to_string(),
            ));
        }

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(EmbeddingError::RateLimited(60));
        }

        if !status.is_success() {
            let error_body = response.text().await.unwrap_or_default();
            return Err(EmbeddingError::Api(format!(
                "HTTP {}: {}",
                status, error_body
            )));
        }

        // HuggingFace returns a nested array directly
        response
            .json::<Vec<Vec<f32>>>()
            .await
            .map_err(|e| EmbeddingError::Api(format!("Failed to parse response: {}", e)))
    }
}

#[async_trait]
impl EmbeddingProvider for HuggingFaceEmbeddingProvider {
    fn name(&self) -> &str {
        "huggingface"
    }

    fn dimension(&self) -> usize {
        self.config.dimension
    }

    async fn embed(&self, text: &str) -> Result<EmbeddingResult, EmbeddingError> {
        let start = Instant::now();

        let embeddings = self.call_api(vec![text.to_string()]).await?;
        let embedding = embeddings
            .into_iter()
            .next()
            .ok_or_else(|| EmbeddingError::Api("Empty response from API".to_string()))?;

        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

        self.stats
            .embeddings_generated
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_latency_ms
            .fetch_add(latency_ms as u64, Ordering::Relaxed);

        Ok(EmbeddingResult {
            text: text.to_string(),
            embedding,
            dimension: self.config.dimension,
            latency_ms,
            tokens: None,
        })
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<EmbeddingResult>, EmbeddingError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let start = Instant::now();
        let embeddings = self.call_api(texts.to_vec()).await?;
        let total_latency_ms = start.elapsed().as_secs_f64() * 1000.0;
        let per_item_latency = total_latency_ms / embeddings.len() as f64;

        let results: Vec<EmbeddingResult> = texts
            .iter()
            .zip(embeddings)
            .map(|(text, embedding)| EmbeddingResult {
                text: text.clone(),
                embedding,
                dimension: self.config.dimension,
                latency_ms: per_item_latency,
                tokens: None,
            })
            .collect();

        self.stats
            .embeddings_generated
            .fetch_add(results.len() as u64, Ordering::Relaxed);
        self.stats
            .total_latency_ms
            .fetch_add(total_latency_ms as u64, Ordering::Relaxed);
        self.stats.batches_processed.fetch_add(1, Ordering::Relaxed);

        Ok(results)
    }

    fn stats(&self) -> &EmbedderStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_embedding_config_default() {
        let config = EmbeddingConfig::default();
        assert_eq!(config.provider, EmbeddingProviderType::Mock);
        assert_eq!(config.dimension, 384);
    }

    #[test]
    fn test_openai_config() {
        let config = EmbeddingConfig::openai("sk-test", "text-embedding-3-small");
        assert_eq!(config.provider, EmbeddingProviderType::OpenAI);
        assert_eq!(config.dimension, 1536);
    }

    #[tokio::test]
    async fn test_mock_embedder() {
        let config = EmbeddingConfig::default();
        let embedder = Embedder::new(config);

        let result = embedder.embed("Hello, world!").await.unwrap();
        assert_eq!(result.dimension, 384);
        assert_eq!(result.embedding.len(), 384);
    }

    #[tokio::test]
    async fn test_batch_embedding() {
        let config = EmbeddingConfig::default();
        let embedder = Embedder::new(config);

        let texts = vec![
            "First text".to_string(),
            "Second text".to_string(),
            "Third text".to_string(),
        ];

        let results = embedder.embed_batch(&texts).await.unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn test_embedding_caching() {
        let mut config = EmbeddingConfig::default();
        config.cache_enabled = true;
        config.cache_size = 100;

        let embedder = Embedder::new(config);

        // First call - cache miss
        let _ = embedder.embed("Cached text").await.unwrap();
        assert_eq!(embedder.stats().cache_misses.load(Ordering::Relaxed), 1);

        // Second call - cache hit
        let _ = embedder.embed("Cached text").await.unwrap();
        assert_eq!(embedder.stats().cache_hits.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_embed_chunks() {
        let config = EmbeddingConfig::default();
        let embedder = Embedder::new(config);

        let doc_id = super::super::document::DocumentId::from_string("doc:test");
        let mut chunks = vec![
            super::Chunk::new(doc_id.clone(), "Chunk 1".to_string(), 0, 7, 0, 2),
            super::Chunk::new(doc_id, "Chunk 2".to_string(), 8, 15, 1, 2),
        ];

        embedder.embed_chunks(&mut chunks).await.unwrap();

        assert!(chunks[0].has_embedding());
        assert!(chunks[1].has_embedding());
    }

    #[tokio::test]
    async fn test_deterministic_mock_embeddings() {
        let provider = MockEmbeddingProvider::new(384);

        let result1 = provider.embed("Same text").await.unwrap();
        let result2 = provider.embed("Same text").await.unwrap();

        // Same text should produce same embedding
        assert_eq!(result1.embedding, result2.embedding);
    }

    #[test]
    fn test_embedder_stats() {
        let stats = EmbedderStats::default();

        stats.embeddings_generated.store(100, Ordering::Release);
        stats.total_latency_ms.store(1000, Ordering::Release);
        stats.cache_hits.store(80, Ordering::Release);
        stats.cache_misses.store(20, Ordering::Release);

        assert_eq!(stats.avg_latency_ms(), 10.0);
        assert_eq!(stats.cache_hit_rate(), 0.8);
    }
}
