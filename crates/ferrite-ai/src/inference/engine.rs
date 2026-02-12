//! Inference Engine
//!
//! Main entry point for ML inference operations.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use super::batch::{BatchConfig, BatchManager};
use super::cache::{InferenceCache, InferenceCacheConfig};
use super::model::{ModelConfig, ModelRegistry};
use super::{InferenceInput, InferenceOutput, Result};

/// Inference engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceConfig {
    /// Batch configuration
    #[serde(default)]
    pub batch: BatchConfig,
    /// Cache configuration
    #[serde(default)]
    pub cache: InferenceCacheConfig,
    /// Maximum concurrent inferences
    pub max_concurrent: usize,
    /// Default timeout in milliseconds
    pub default_timeout_ms: u64,
    /// Enable metrics collection
    pub enable_metrics: bool,
}

impl Default for InferenceConfig {
    fn default() -> Self {
        Self {
            batch: BatchConfig::default(),
            cache: InferenceCacheConfig::default(),
            max_concurrent: 100,
            default_timeout_ms: 5000,
            enable_metrics: true,
        }
    }
}

impl InferenceConfig {
    /// Create high-throughput config
    pub fn high_throughput() -> Self {
        Self {
            batch: BatchConfig::high_throughput(),
            ..Default::default()
        }
    }

    /// Create low-latency config
    pub fn low_latency() -> Self {
        Self {
            batch: BatchConfig::low_latency(),
            cache: InferenceCacheConfig::default().with_ttl(60),
            ..Default::default()
        }
    }
}

/// Inference result with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceResult {
    /// Model output
    pub output: InferenceOutput,
    /// Model name
    pub model: String,
    /// Inference latency in milliseconds
    pub latency_ms: u64,
    /// Whether result was from cache
    pub from_cache: bool,
    /// Batch size (if batched)
    pub batch_size: Option<usize>,
}

/// Inference statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InferenceStats {
    /// Total predictions
    pub total_predictions: u64,
    /// Cache hit rate
    pub cache_hit_rate: f64,
    /// Average latency
    pub avg_latency_ms: f64,
    /// Average batch size
    pub avg_batch_size: f64,
    /// Models loaded
    pub models_loaded: usize,
    /// Total batches processed
    pub total_batches: u64,
}

/// The main inference engine
pub struct InferenceEngine {
    config: InferenceConfig,
    registry: Arc<ModelRegistry>,
    cache: Arc<InferenceCache>,
    batch_manager: BatchManager,
    // Stats
    total_predictions: AtomicU64,
    cache_hits: AtomicU64,
    total_latency_ms: AtomicU64,
}

impl InferenceEngine {
    /// Create a new inference engine
    pub fn new(config: InferenceConfig) -> Self {
        let cache = Arc::new(InferenceCache::new(config.cache.clone()));
        let batch_manager = BatchManager::new(config.batch.clone());

        Self {
            config,
            registry: Arc::new(ModelRegistry::new()),
            cache,
            batch_manager,
            total_predictions: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            total_latency_ms: AtomicU64::new(0),
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(InferenceConfig::default())
    }

    /// Load a model
    pub async fn load_model(&self, name: &str, config: ModelConfig) -> Result<()> {
        let model_config = config.with_name(name);
        self.registry.load(model_config).await
    }

    /// Unload a model
    pub fn unload_model(&self, name: &str) -> bool {
        self.registry.unload(name)
    }

    /// Run inference
    pub async fn predict(&self, model: &str, input: InferenceInput) -> Result<InferenceResult> {
        let start = Instant::now();

        // Check cache first
        if let Some(output) = self.cache.get(model, &input) {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            self.total_predictions.fetch_add(1, Ordering::Relaxed);

            return Ok(InferenceResult {
                output,
                model: model.to_string(),
                latency_ms: start.elapsed().as_millis() as u64,
                from_cache: true,
                batch_size: None,
            });
        }

        // Run inference
        let output = self.registry.predict(model, &input).await?;

        // Cache result
        self.cache.put(model, &input, output.clone());

        let latency = start.elapsed().as_millis() as u64;
        self.total_predictions.fetch_add(1, Ordering::Relaxed);
        self.total_latency_ms.fetch_add(latency, Ordering::Relaxed);

        Ok(InferenceResult {
            output,
            model: model.to_string(),
            latency_ms: latency,
            from_cache: false,
            batch_size: None,
        })
    }

    /// Run batched inference
    pub async fn predict_batch(
        &self,
        model: &str,
        inputs: Vec<InferenceInput>,
    ) -> Result<Vec<InferenceResult>> {
        let start = Instant::now();
        let batch_size = inputs.len();

        // Check cache for each input
        let mut results = Vec::with_capacity(batch_size);
        let mut uncached_inputs = Vec::new();
        let mut uncached_indices = Vec::new();

        for (i, input) in inputs.iter().enumerate() {
            if let Some(output) = self.cache.get(model, input) {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                results.push(Some(InferenceResult {
                    output,
                    model: model.to_string(),
                    latency_ms: 0,
                    from_cache: true,
                    batch_size: Some(batch_size),
                }));
            } else {
                results.push(None);
                uncached_inputs.push(input.clone());
                uncached_indices.push(i);
            }
        }

        // Run inference on uncached inputs
        if !uncached_inputs.is_empty() {
            let outputs = self.registry.predict_batch(model, &uncached_inputs).await?;

            for (idx, (input, output)) in uncached_indices
                .iter()
                .zip(uncached_inputs.iter().zip(outputs.iter()))
            {
                self.cache.put(model, input, output.clone());
                results[*idx] = Some(InferenceResult {
                    output: output.clone(),
                    model: model.to_string(),
                    latency_ms: 0,
                    from_cache: false,
                    batch_size: Some(batch_size),
                });
            }
        }

        let latency = start.elapsed().as_millis() as u64;
        self.total_predictions
            .fetch_add(batch_size as u64, Ordering::Relaxed);
        self.total_latency_ms.fetch_add(latency, Ordering::Relaxed);

        // Update batch stats
        self.batch_manager.record_batch(batch_size, latency, 0);

        // Unwrap all results (they should all be Some now)
        Ok(results.into_iter().flatten().collect())
    }

    /// Get model info
    pub fn model_info(&self, name: &str) -> Option<super::model::ModelInfo> {
        self.registry.get_info(name)
    }

    /// List models
    pub fn list_models(&self) -> Vec<String> {
        self.registry.list()
    }

    /// Check if model exists
    pub fn has_model(&self, name: &str) -> bool {
        self.registry.has_model(name)
    }

    /// Get engine statistics
    pub fn stats(&self) -> InferenceStats {
        let total = self.total_predictions.load(Ordering::Relaxed);
        let cache_hits = self.cache_hits.load(Ordering::Relaxed);
        let total_latency = self.total_latency_ms.load(Ordering::Relaxed);
        let batch_stats = self.batch_manager.stats();

        InferenceStats {
            total_predictions: total,
            cache_hit_rate: if total > 0 {
                cache_hits as f64 / total as f64
            } else {
                0.0
            },
            avg_latency_ms: if total > 0 {
                total_latency as f64 / total as f64
            } else {
                0.0
            },
            avg_batch_size: batch_stats.avg_batch_size,
            models_loaded: self.registry.count(),
            total_batches: batch_stats.total_batches,
        }
    }

    /// Clear cache
    pub fn clear_cache(&self) {
        self.cache.clear();
    }

    /// Get configuration
    pub fn config(&self) -> &InferenceConfig {
        &self.config
    }
}

impl Default for InferenceEngine {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_engine_creation() {
        let engine = InferenceEngine::with_defaults();
        assert_eq!(engine.list_models().len(), 0);
    }

    #[tokio::test]
    async fn test_load_and_predict() {
        let engine = InferenceEngine::with_defaults();

        engine
            .load_model("test", ModelConfig::onnx("test.onnx"))
            .await
            .unwrap();

        assert!(engine.has_model("test"));

        let input = InferenceInput::Vector(vec![1.0, 2.0, 3.0]);
        let result = engine.predict("test", input).await.unwrap();

        assert_eq!(result.model, "test");
        assert!(!result.from_cache);
    }

    #[tokio::test]
    async fn test_cache_hit() {
        let engine = InferenceEngine::with_defaults();

        engine
            .load_model("test", ModelConfig::onnx("test.onnx"))
            .await
            .unwrap();

        let input = InferenceInput::Text("hello".to_string());

        // First call - miss
        let result1 = engine.predict("test", input.clone()).await.unwrap();
        assert!(!result1.from_cache);

        // Second call - hit
        let result2 = engine.predict("test", input).await.unwrap();
        assert!(result2.from_cache);
    }

    #[tokio::test]
    async fn test_batch_predict() {
        let engine = InferenceEngine::with_defaults();

        engine
            .load_model("test", ModelConfig::onnx("test.onnx"))
            .await
            .unwrap();

        let inputs = vec![
            InferenceInput::Scalar(1.0),
            InferenceInput::Scalar(2.0),
            InferenceInput::Scalar(3.0),
        ];

        let results = engine.predict_batch("test", inputs).await.unwrap();
        assert_eq!(results.len(), 3);

        for result in &results {
            assert_eq!(result.batch_size, Some(3));
        }
    }

    #[tokio::test]
    async fn test_stats() {
        let engine = InferenceEngine::with_defaults();

        engine
            .load_model("test", ModelConfig::onnx("test.onnx"))
            .await
            .unwrap();

        engine
            .predict("test", InferenceInput::Scalar(1.0))
            .await
            .unwrap();

        let stats = engine.stats();
        assert_eq!(stats.total_predictions, 1);
        assert_eq!(stats.models_loaded, 1);
    }

    #[test]
    fn test_config_presets() {
        let high_throughput = InferenceConfig::high_throughput();
        assert_eq!(high_throughput.batch.max_batch_size, 64);

        let low_latency = InferenceConfig::low_latency();
        assert_eq!(low_latency.batch.max_batch_size, 8);
    }
}
