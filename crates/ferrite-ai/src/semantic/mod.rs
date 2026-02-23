//! # Semantic Caching Engine
//!
//! Cache by meaning, not just exact key matches. Uses vector similarity
//! to find cached responses for semantically similar queries.
//!
//! ## Why Semantic Caching?
//!
//! Traditional caching is key-based - only exact matches hit the cache.
//! But users ask the same questions in different ways:
//!
//! - "What is the capital of France?"
//! - "France's capital city?"
//! - "What city is France's capital?"
//!
//! Semantic caching uses embeddings to cache by **meaning**, enabling:
//!
//! - **LLM Cost Reduction**: 40-60% reduction in API calls
//! - **Faster Responses**: Cache hits return in microseconds vs seconds for LLM calls
//! - **Better UX**: Similar queries get consistent answers
//!
//! ## Quick Start
//!
//! ### Basic Usage (Pre-computed Embeddings)
//!
//! ```no_run
//! use ferrite::semantic::{SemanticCache, SemanticConfig};
//!
//! // Create cache with default settings
//! let cache = SemanticCache::new(SemanticConfig::default())?;
//!
//! // Store with pre-computed embedding
//! let question = "What is the capital of France?";
//! let answer = "Paris is the capital of France.";
//! let embedding = compute_embedding(question);  // Your embedding function
//! cache.set(question, answer, &embedding, None)?;
//!
//! // Query with embedding
//! let query = "France's capital city?";
//! let query_embedding = compute_embedding(query);
//! let result = cache.get(&query_embedding, 0.85)?;  // Threshold: 85% similarity
//!
//! if let Some(hit) = result {
//!     println!("Cache hit! Answer: {}", hit.value);
//!     println!("Similarity: {:.2}%", hit.score * 100.0);
//! }
//! # Ok::<(), ferrite::semantic::SemanticError>(())
//! ```
//!
//! ### With Automatic Embedding (ONNX Local)
//!
//! ```no_run
//! use ferrite::semantic::{SemanticCache, SemanticConfig, EmbeddingModelConfig, EmbeddingModelType};
//!
//! // Configure with local ONNX model (no API calls!)
//! let config = SemanticConfig {
//!     auto_embed: true,
//!     embedding_model: EmbeddingModelConfig {
//!         model_type: EmbeddingModelType::Onnx,
//!         model_path: Some("./models/all-MiniLM-L6-v2.onnx".to_string()),
//!         ..Default::default()
//!     },
//!     ..Default::default()
//! };
//!
//! let cache = SemanticCache::new(config)?;
//!
//! // Store - embedding generated automatically
//! cache.set_auto("What is Rust?", "Rust is a systems programming language...", None)?;
//!
//! // Query - embedding generated automatically
//! let result = cache.get_auto("Tell me about Rust programming", 0.85)?;
//! # Ok::<(), ferrite::semantic::SemanticError>(())
//! ```
//!
//! ### With OpenAI Embeddings
//!
//! ```no_run
//! use ferrite::semantic::{SemanticCache, SemanticConfig, EmbeddingModelConfig, EmbeddingModelType};
//!
//! let config = SemanticConfig {
//!     auto_embed: true,
//!     embedding_dim: 1536,  // text-embedding-3-small
//!     embedding_model: EmbeddingModelConfig {
//!         model_type: EmbeddingModelType::OpenAI,
//!         api_key: Some(std::env::var("OPENAI_API_KEY").unwrap_or_default()),
//!         ..Default::default()
//!     },
//!     ..Default::default()
//! };
//!
//! let cache = SemanticCache::new(config)?;
//! # Ok::<(), ferrite::semantic::SemanticError>(())
//! ```
//!
//! ## LLM Response Caching
//!
//! Specialized cache for LLM applications with cost tracking:
//!
//! ```no_run
//! use ferrite::semantic::llm_cache::{LlmCache, LlmCacheConfig, CostTrackingConfig};
//!
//! let cache = LlmCache::new(LlmCacheConfig {
//!     similarity_threshold: 0.88,
//!     max_entries: 100_000,
//!     cost_tracking: CostTrackingConfig {
//!         enabled: true,
//!         cost_per_1k_tokens: 0.002,  // GPT-4 pricing
//!         avg_tokens_per_query: 500,
//!     },
//!     ..Default::default()
//! })?;
//!
//! // Check cache before calling LLM
//! let query = "Explain quantum computing";
//! match cache.get(query).await? {
//!     Some(cached) => {
//!         println!("Cache hit! Saved ${:.4}", cached.estimated_savings);
//!         return Ok(cached.response);
//!     }
//!     None => {
//!         let response = call_llm(query).await?;
//!         cache.set(query, &response).await?;
//!         return Ok(response);
//!     }
//! }
//!
//! // Check cost savings
//! let stats = cache.stats();
//! println!("Total saved: ${:.2}", stats.total_cost_saved);
//! println!("Hit rate: {:.1}%", stats.hit_rate * 100.0);
//! # Ok::<(), ferrite::semantic::SemanticError>(())
//! ```
//!
//! ## Resilient Embedding with Circuit Breaker
//!
//! Handle embedding API failures gracefully:
//!
//! ```no_run
//! use ferrite::semantic::resilience::{ResilientEmbedder, CircuitBreaker, RetryPolicy, FallbackStrategy};
//!
//! let embedder = ResilientEmbedder::builder()
//!     .primary(openai_embedder)
//!     .fallback(FallbackStrategy::Secondary(onnx_embedder))
//!     .circuit_breaker(CircuitBreaker::new(
//!         5,       // failures before open
//!         60,      // seconds before half-open
//!     ))
//!     .retry_policy(RetryPolicy::exponential(3, 100))  // 3 retries, 100ms base
//!     .build();
//!
//! // Automatically retries, falls back, and handles circuit breaker
//! let embedding = embedder.embed("Hello world").await?;
//! # Ok::<(), ferrite::semantic::SemanticError>(())
//! ```
//!
//! ## Threshold Tuning
//!
//! The similarity threshold controls cache hit sensitivity:
//!
//! | Threshold | Hit Rate | Quality | Use Case |
//! |-----------|----------|---------|----------|
//! | 0.95+ | Low | Very High | Critical applications |
//! | 0.90 | Medium | High | Production default |
//! | 0.85 | High | Good | Most LLM caching |
//! | 0.80 | Very High | Moderate | Cost-sensitive apps |
//!
//! **Recommendation**: Start with 0.85, monitor quality, adjust as needed.
//!
//! ## Performance Characteristics
//!
//! | Operation | Latency | Throughput |
//! |-----------|---------|------------|
//! | Cache hit (pre-embedded) | 20-50μs | 40K/s |
//! | Cache hit (ONNX embed) | 0.5-2ms | 1K/s |
//! | Cache hit (OpenAI embed) | 5-20ms | 100/s |
//! | Cache set | 30-80μs | 25K/s |
//!
//! ## Best Practices
//!
//! 1. **Use local embeddings** (ONNX) for latency-sensitive applications
//! 2. **Set appropriate TTLs** for time-sensitive content
//! 3. **Monitor hit rates** - if too low, lower threshold; if quality drops, raise it
//! 4. **Warm the cache** with common queries at startup
//! 5. **Use separate caches** for different domains (code, general knowledge, etc.)

#![allow(dead_code)]
pub mod async_embeddings;
mod cache;
mod embeddings;
pub mod function_cache;
pub mod ga_cache;
pub mod hnsw;
pub mod llm_cache;
pub mod metrics;
pub mod resilience;
pub mod semantic_cache;
pub mod streaming;

pub use async_embeddings::{
    AsyncEmbedder, AsyncEmbeddingConfig, EmbeddingBatchProcessor, EmbeddingPool,
};
pub use cache::{CacheEntry, CacheResult, CacheStatistics, SemanticCache, SemanticCacheBuilder};
pub use embeddings::{EmbeddingModel, EmbeddingProvider};
pub use function_cache::{
    FunctionCache, FunctionCacheBuilder, FunctionCacheConfig, FunctionCacheResult,
    FunctionCacheStats, FunctionCall, FunctionCallMetadata, FunctionCostConfig, InvalidationConfig,
    PerFunctionStats,
};
pub use ga_cache::{
    DistanceMetricType, GaCacheConfig, GaCacheEntry, GaCacheHit, GaCacheSnapshot, GaSemanticCache,
    PersistenceError,
};
pub use hnsw::{HnswConfig, HnswIndex};
pub use llm_cache::{
    CacheConfig, CostStats, CostTrackingConfig, EmbeddingConfig, EmbeddingProviderType,
    FallbackConfig, LlmCache, LlmCacheBuilder, LlmCacheConfig, LlmCacheResult, LlmCacheStats,
};
pub use metrics::{MetricsSummary, SemanticMetrics, Timer};
pub use resilience::{
    Bulkhead, BulkheadPermit, CircuitBreaker, CircuitBreakerStatistics, CircuitState,
    FallbackStrategy, ResilientEmbedder, RetryPolicy,
};
pub use semantic_cache::{CacheHit, CacheStats, EnhancedSemanticCache, SemanticCacheConfig};
pub use streaming::{EmbeddingStream, StreamStats, StreamingConfig, StreamingEmbedder};

use serde::{Deserialize, Serialize};

/// Configuration for semantic caching
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SemanticConfig {
    /// Enable semantic caching
    pub enabled: bool,
    /// Default similarity threshold (0.0 to 1.0)
    pub default_threshold: f32,
    /// Maximum number of cached entries
    pub max_entries: usize,
    /// Embedding dimension
    pub embedding_dim: usize,
    /// Default TTL in seconds (0 = no expiry)
    pub default_ttl_secs: u64,
    /// Index type for similarity search
    pub index_type: IndexType,
    /// Distance metric for similarity
    pub distance_metric: DistanceMetric,
    /// Enable automatic embedding generation
    pub auto_embed: bool,
    /// Embedding model configuration
    pub embedding_model: EmbeddingModelConfig,
}

impl Default for SemanticConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_threshold: 0.85,
            max_entries: 100_000,
            embedding_dim: 384, // Common for sentence transformers
            default_ttl_secs: 3600,
            index_type: IndexType::Hnsw,
            distance_metric: DistanceMetric::Cosine,
            auto_embed: false,
            embedding_model: EmbeddingModelConfig::default(),
        }
    }
}

/// Index type for similarity search
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexType {
    /// HNSW index - best for high recall
    Hnsw,
    /// Flat index - exact search for small datasets
    Flat,
}

/// Distance metric for similarity calculation
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DistanceMetric {
    /// Cosine similarity (recommended for text embeddings)
    Cosine,
    /// Euclidean distance
    Euclidean,
    /// Dot product
    DotProduct,
}

/// Configuration for embedding model
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EmbeddingModelConfig {
    /// Model type
    pub model_type: EmbeddingModelType,
    /// Model path (for local models)
    pub model_path: Option<String>,
    /// API endpoint (for remote models)
    pub api_endpoint: Option<String>,
    /// API key (for remote models)
    pub api_key: Option<String>,
    /// Batch size for embedding generation
    pub batch_size: usize,
    /// Cache embeddings in memory
    pub cache_embeddings: bool,
}

impl Default for EmbeddingModelConfig {
    fn default() -> Self {
        Self {
            model_type: EmbeddingModelType::None,
            model_path: None,
            api_endpoint: None,
            api_key: None,
            batch_size: 32,
            cache_embeddings: true,
        }
    }
}

/// Embedding model types
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EmbeddingModelType {
    /// No automatic embedding - user provides vectors
    None,
    /// Local ONNX model
    Onnx,
    /// OpenAI API
    OpenAI,
    /// Cohere API
    Cohere,
    /// Custom HTTP endpoint
    Custom,
}

/// Errors that can occur in semantic caching
#[derive(Debug, Clone, thiserror::Error)]
pub enum SemanticError {
    /// Dimension mismatch
    #[error("dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch {
        /// Expected dimension
        expected: usize,
        /// Got dimension
        got: usize,
    },

    /// Entry not found
    #[error("entry not found: {0}")]
    NotFound(String),

    /// Cache is full
    #[error("cache is full (max entries: {0})")]
    CacheFull(usize),

    /// Embedding generation failed
    #[error("embedding generation failed: {0}")]
    EmbeddingFailed(String),

    /// Invalid threshold
    #[error("invalid threshold: {0} (must be 0.0 to 1.0)")]
    InvalidThreshold(f32),

    /// Configuration error
    #[error("configuration error: {0}")]
    ConfigError(String),

    /// Internal error
    #[error("internal error: {0}")]
    Internal(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_semantic_config_default() {
        let config = SemanticConfig::default();
        assert!(config.enabled);
        assert_eq!(config.default_threshold, 0.85);
        assert_eq!(config.embedding_dim, 384);
        assert_eq!(config.index_type, IndexType::Hnsw);
        assert_eq!(config.distance_metric, DistanceMetric::Cosine);
    }

    #[test]
    fn test_embedding_model_config_default() {
        let config = EmbeddingModelConfig::default();
        assert_eq!(config.model_type, EmbeddingModelType::None);
        assert!(config.model_path.is_none());
        assert_eq!(config.batch_size, 32);
    }
}
