//! Async embedding generation with batching and concurrency control
//!
//! Provides non-blocking embedding generation for high-throughput scenarios.

use super::{EmbeddingModel, SemanticError};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Mutex, Semaphore};
use tokio::time::timeout;

/// Async embedding request
struct EmbeddingRequest {
    texts: Vec<String>,
    response_tx: oneshot::Sender<Result<Vec<Vec<f32>>, SemanticError>>,
}

/// Configuration for async embeddings
#[derive(Clone, Debug)]
pub struct AsyncEmbeddingConfig {
    /// Batch size for embedding requests
    pub batch_size: usize,
    /// Maximum queue size
    pub max_queue_size: usize,
    /// Batch timeout in milliseconds
    pub batch_timeout_ms: u64,
    /// Maximum concurrent batches
    pub max_concurrent_batches: usize,
    /// Request timeout in seconds
    pub request_timeout_secs: u64,
}

impl Default for AsyncEmbeddingConfig {
    fn default() -> Self {
        Self {
            batch_size: 32,
            max_queue_size: 1000,
            batch_timeout_ms: 50,
            max_concurrent_batches: 4,
            request_timeout_secs: 30,
        }
    }
}

/// Async embedding provider with batching
pub struct AsyncEmbedder {
    model: Arc<EmbeddingModel>,
    config: AsyncEmbeddingConfig,
    semaphore: Arc<Semaphore>,
    queue: Arc<Mutex<VecDeque<EmbeddingRequest>>>,
}

impl AsyncEmbedder {
    /// Create a new async embedder
    pub fn new(model: EmbeddingModel, config: AsyncEmbeddingConfig) -> Self {
        Self {
            model: Arc::new(model),
            semaphore: Arc::new(Semaphore::new(config.max_concurrent_batches)),
            queue: Arc::new(Mutex::new(VecDeque::new())),
            config,
        }
    }

    /// Create with default configuration
    pub fn with_defaults(model: EmbeddingModel) -> Self {
        Self::new(model, AsyncEmbeddingConfig::default())
    }

    /// Create a placeholder async embedder
    pub fn placeholder(dimension: usize) -> Self {
        Self::with_defaults(EmbeddingModel::placeholder(dimension))
    }

    /// Get embedding dimension
    pub fn dimension(&self) -> usize {
        self.model.dimension()
    }

    /// Generate embedding for a single text
    pub async fn embed(&self, text: &str) -> Result<Vec<f32>, SemanticError> {
        let results = self.embed_batch(&[text]).await?;
        results
            .into_iter()
            .next()
            .ok_or_else(|| SemanticError::EmbeddingFailed("Empty batch result".to_string()))
    }

    /// Generate embeddings for multiple texts
    pub async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, SemanticError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        // Acquire permit for concurrency control
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|e| SemanticError::Internal(format!("Semaphore error: {}", e)))?;

        // Use tokio::task::spawn_blocking for the synchronous embedding call
        let model = Arc::clone(&self.model);
        let texts_owned: Vec<String> = texts.iter().map(|s| s.to_string()).collect();
        let timeout_duration = Duration::from_secs(self.config.request_timeout_secs);

        let result = timeout(timeout_duration, async move {
            tokio::task::spawn_blocking(move || {
                let text_refs: Vec<&str> = texts_owned.iter().map(|s| s.as_str()).collect();
                model.embed_batch(&text_refs)
            })
            .await
            .map_err(|e| SemanticError::Internal(format!("Task join error: {}", e)))?
        })
        .await;

        match result {
            Ok(inner) => inner,
            Err(_) => Err(SemanticError::EmbeddingFailed(
                "Request timeout".to_string(),
            )),
        }
    }

    /// Generate embeddings with automatic batching
    pub async fn embed_with_batching(
        &self,
        texts: &[&str],
    ) -> Result<Vec<Vec<f32>>, SemanticError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let batch_size = self.config.batch_size;
        let mut all_embeddings = Vec::with_capacity(texts.len());

        // Process in batches
        for chunk in texts.chunks(batch_size) {
            let embeddings = self.embed_batch(chunk).await?;
            all_embeddings.extend(embeddings);
        }

        Ok(all_embeddings)
    }
}

/// Batch processor for embedding requests
pub struct EmbeddingBatchProcessor {
    embedder: Arc<AsyncEmbedder>,
    request_tx: mpsc::Sender<EmbeddingRequest>,
    config: AsyncEmbeddingConfig,
}

impl EmbeddingBatchProcessor {
    /// Create a new batch processor
    pub fn new(embedder: AsyncEmbedder) -> Self {
        let config = embedder.config.clone();
        let embedder = Arc::new(embedder);
        let (request_tx, request_rx) = mpsc::channel(config.max_queue_size);

        // Spawn batch processing task
        let processor_embedder = Arc::clone(&embedder);
        let processor_config = config.clone();
        tokio::spawn(async move {
            Self::process_batches(processor_embedder, request_rx, processor_config).await;
        });

        Self {
            embedder,
            request_tx,
            config,
        }
    }

    /// Submit a batch of texts for embedding
    pub async fn submit(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, SemanticError> {
        let (response_tx, response_rx) = oneshot::channel();

        let request = EmbeddingRequest { texts, response_tx };

        self.request_tx
            .send(request)
            .await
            .map_err(|_| SemanticError::Internal("Batch processor channel closed".to_string()))?;

        response_rx
            .await
            .map_err(|_| SemanticError::Internal("Response channel closed".to_string()))?
    }

    /// Submit a single text for embedding
    pub async fn submit_one(&self, text: String) -> Result<Vec<f32>, SemanticError> {
        let results = self.submit(vec![text]).await?;
        results
            .into_iter()
            .next()
            .ok_or_else(|| SemanticError::EmbeddingFailed("Empty result".to_string()))
    }

    /// Process batches from the queue
    async fn process_batches(
        embedder: Arc<AsyncEmbedder>,
        mut request_rx: mpsc::Receiver<EmbeddingRequest>,
        config: AsyncEmbeddingConfig,
    ) {
        let mut pending_requests: Vec<EmbeddingRequest> = Vec::new();
        let batch_timeout = Duration::from_millis(config.batch_timeout_ms);

        loop {
            // Wait for requests or timeout
            let request = if pending_requests.is_empty() {
                // No pending requests, wait indefinitely
                request_rx.recv().await
            } else {
                // Have pending requests, wait with timeout
                tokio::time::timeout(batch_timeout, request_rx.recv()).await
                    .unwrap_or_default() // Timeout, process current batch
            };

            match request {
                Some(req) => {
                    pending_requests.push(req);

                    // Check if we should process the batch
                    let total_texts: usize = pending_requests.iter().map(|r| r.texts.len()).sum();
                    if total_texts >= config.batch_size {
                        Self::process_batch(&embedder, &mut pending_requests).await;
                    }
                }
                None => {
                    if pending_requests.is_empty() {
                        // Channel closed and no pending requests
                        break;
                    } else {
                        // Timeout or channel closed, process remaining
                        Self::process_batch(&embedder, &mut pending_requests).await;
                    }
                }
            }
        }
    }

    /// Process a batch of requests
    async fn process_batch(embedder: &Arc<AsyncEmbedder>, requests: &mut Vec<EmbeddingRequest>) {
        if requests.is_empty() {
            return;
        }

        // Collect all texts
        let mut all_texts: Vec<String> = Vec::new();
        let mut request_sizes: Vec<usize> = Vec::new();

        for req in requests.iter() {
            request_sizes.push(req.texts.len());
            all_texts.extend(req.texts.iter().cloned());
        }

        // Generate embeddings
        let text_refs: Vec<&str> = all_texts.iter().map(|s| s.as_str()).collect();
        let result = embedder.embed_batch(&text_refs).await;

        // Distribute results back to requests
        let requests = std::mem::take(requests);
        let mut offset = 0;

        match result {
            Ok(embeddings) => {
                for (req, size) in requests.into_iter().zip(request_sizes) {
                    let req_embeddings = embeddings[offset..offset + size].to_vec();
                    offset += size;
                    let _ = req.response_tx.send(Ok(req_embeddings));
                }
            }
            Err(e) => {
                for req in requests {
                    let _ = req.response_tx.send(Err(e.clone()));
                }
            }
        }
    }
}

/// Embedding pool for managing multiple embedding providers
pub struct EmbeddingPool {
    embedders: Vec<Arc<AsyncEmbedder>>,
    current: std::sync::atomic::AtomicUsize,
}

impl EmbeddingPool {
    /// Create a new embedding pool with multiple embedders
    pub fn new(embedders: Vec<AsyncEmbedder>) -> Self {
        Self {
            embedders: embedders.into_iter().map(Arc::new).collect(),
            current: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Create a pool with a single embedder
    pub fn single(embedder: AsyncEmbedder) -> Self {
        Self::new(vec![embedder])
    }

    /// Get the next embedder (round-robin)
    pub fn get(&self) -> Arc<AsyncEmbedder> {
        let idx = self
            .current
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Arc::clone(&self.embedders[idx % self.embedders.len()])
    }

    /// Generate embedding using the pool
    pub async fn embed(&self, text: &str) -> Result<Vec<f32>, SemanticError> {
        self.get().embed(text).await
    }

    /// Generate embeddings for a batch using the pool
    pub async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, SemanticError> {
        self.get().embed_batch(texts).await
    }

    /// Get pool size
    pub fn size(&self) -> usize {
        self.embedders.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_async_embedder() {
        let embedder = AsyncEmbedder::placeholder(384);

        let embedding = embedder.embed("test text").await.unwrap();
        assert_eq!(embedding.len(), 384);
    }

    #[tokio::test]
    async fn test_async_batch_embedding() {
        let embedder = AsyncEmbedder::placeholder(384);

        let texts = vec!["text1", "text2", "text3"];
        let embeddings = embedder.embed_batch(&texts).await.unwrap();

        assert_eq!(embeddings.len(), 3);
        for emb in &embeddings {
            assert_eq!(emb.len(), 384);
        }
    }

    #[tokio::test]
    async fn test_embedding_with_batching() {
        let config = AsyncEmbeddingConfig {
            batch_size: 2,
            ..Default::default()
        };
        let embedder = AsyncEmbedder::new(EmbeddingModel::placeholder(384), config);

        let texts: Vec<&str> = (0..5).map(|i| "text").collect();
        let embeddings = embedder.embed_with_batching(&texts).await.unwrap();

        assert_eq!(embeddings.len(), 5);
    }

    #[tokio::test]
    async fn test_embedding_pool() {
        let pool = EmbeddingPool::new(vec![
            AsyncEmbedder::placeholder(384),
            AsyncEmbedder::placeholder(384),
        ]);

        assert_eq!(pool.size(), 2);

        let embedding = pool.embed("test").await.unwrap();
        assert_eq!(embedding.len(), 384);
    }
}
