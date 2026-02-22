//! Streaming Embeddings for Real-Time Processing
//!
//! Provides streaming embedding generation for real-time semantic cache
//! population and query processing with backpressure support.
//!
//! # Features
//!
//! - Async streaming with backpressure
//! - Automatic batching for efficiency
//! - Priority queue for urgent requests
//! - Rate limiting to prevent API throttling
//! - Circuit breaker for fault tolerance
//!
//! # Example
//!
//! ```ignore
//! use ferrite::semantic::streaming::{StreamingEmbedder, StreamingConfig};
//!
//! let config = StreamingConfig::default();
//! let embedder = StreamingEmbedder::new(model, config);
//!
//! // Stream embeddings
//! let mut stream = embedder.stream(texts);
//! while let Some(result) = stream.next().await {
//!     let embedding = result?;
//!     // Process embedding
//! }
//! ```

use super::{EmbeddingModel, SemanticError};
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::time::sleep;

/// Configuration for streaming embeddings
#[derive(Clone, Debug)]
pub struct StreamingConfig {
    /// Batch size for embedding requests
    pub batch_size: usize,
    /// Maximum queue size (backpressure threshold)
    pub max_queue_size: usize,
    /// Batch timeout in milliseconds
    pub batch_timeout_ms: u64,
    /// Maximum concurrent batches
    pub max_concurrent: usize,
    /// Rate limit (requests per second, 0 = unlimited)
    pub rate_limit_rps: u32,
    /// Circuit breaker failure threshold
    pub circuit_breaker_threshold: u32,
    /// Circuit breaker recovery time in seconds
    pub circuit_breaker_recovery_secs: u64,
    /// Enable priority queue
    pub enable_priority: bool,
    /// Request timeout in seconds
    pub request_timeout_secs: u64,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            batch_size: 32,
            max_queue_size: 1000,
            batch_timeout_ms: 50,
            max_concurrent: 4,
            rate_limit_rps: 0, // Unlimited
            circuit_breaker_threshold: 5,
            circuit_breaker_recovery_secs: 30,
            enable_priority: false,
            request_timeout_secs: 30,
        }
    }
}

impl StreamingConfig {
    /// Configuration for high throughput
    pub fn high_throughput() -> Self {
        Self {
            batch_size: 64,
            max_queue_size: 5000,
            batch_timeout_ms: 100,
            max_concurrent: 8,
            rate_limit_rps: 0,
            circuit_breaker_threshold: 10,
            circuit_breaker_recovery_secs: 60,
            enable_priority: false,
            request_timeout_secs: 60,
        }
    }

    /// Configuration for low latency
    pub fn low_latency() -> Self {
        Self {
            batch_size: 8,
            max_queue_size: 100,
            batch_timeout_ms: 10,
            max_concurrent: 2,
            rate_limit_rps: 100,
            circuit_breaker_threshold: 3,
            circuit_breaker_recovery_secs: 10,
            enable_priority: true,
            request_timeout_secs: 10,
        }
    }

    /// Configuration for API rate limiting
    pub fn rate_limited(rps: u32) -> Self {
        Self {
            rate_limit_rps: rps,
            batch_size: 16,
            max_queue_size: 500,
            ..Default::default()
        }
    }
}

/// Priority level for embedding requests
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    /// Low priority (background processing)
    Low = 0,
    /// Normal priority
    #[default]
    Normal = 1,
    /// High priority (user-facing requests)
    High = 2,
    /// Critical priority (bypass batching)
    Critical = 3,
}



/// Streaming embedding request
struct StreamRequest {
    text: String,
    priority: Priority,
    response_tx: oneshot::Sender<Result<Vec<f32>, SemanticError>>,
    submitted_at: Instant,
}

/// Circuit breaker state
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

/// Circuit breaker for fault tolerance
struct CircuitBreaker {
    state: RwLock<CircuitState>,
    failure_count: AtomicU32,
    last_failure: RwLock<Option<Instant>>,
    threshold: u32,
    recovery_duration: Duration,
}

impl CircuitBreaker {
    fn new(threshold: u32, recovery_secs: u64) -> Self {
        Self {
            state: RwLock::new(CircuitState::Closed),
            failure_count: AtomicU32::new(0),
            last_failure: RwLock::new(None),
            threshold,
            recovery_duration: Duration::from_secs(recovery_secs),
        }
    }

    fn is_open(&self) -> bool {
        let state = *self.state.read();
        match state {
            CircuitState::Open => {
                // Check if we should transition to half-open
                if let Some(last) = *self.last_failure.read() {
                    if last.elapsed() >= self.recovery_duration {
                        *self.state.write() = CircuitState::HalfOpen;
                        return false;
                    }
                }
                true
            }
            _ => false,
        }
    }

    fn record_success(&self) {
        let state = *self.state.read();
        if state == CircuitState::HalfOpen {
            *self.state.write() = CircuitState::Closed;
            self.failure_count.store(0, Ordering::SeqCst);
        }
    }

    fn record_failure(&self) {
        let count = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;
        *self.last_failure.write() = Some(Instant::now());

        if count >= self.threshold {
            *self.state.write() = CircuitState::Open;
        }
    }

    fn state(&self) -> CircuitState {
        *self.state.read()
    }
}

use std::sync::atomic::AtomicU32;

/// Rate limiter using token bucket algorithm
struct RateLimiter {
    tokens: AtomicU64,
    max_tokens: u64,
    refill_rate: u64, // tokens per second
    last_refill: RwLock<Instant>,
}

impl RateLimiter {
    fn new(rps: u32) -> Self {
        let max_tokens = rps as u64;
        Self {
            tokens: AtomicU64::new(max_tokens),
            max_tokens,
            refill_rate: rps as u64,
            last_refill: RwLock::new(Instant::now()),
        }
    }

    fn try_acquire(&self) -> bool {
        self.refill();

        loop {
            let current = self.tokens.load(Ordering::Acquire);
            if current == 0 {
                return false;
            }
            if self
                .tokens
                .compare_exchange(current, current - 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    async fn acquire(&self) {
        while !self.try_acquire() {
            sleep(Duration::from_millis(10)).await;
        }
    }

    fn refill(&self) {
        let mut last = self.last_refill.write();
        let elapsed = last.elapsed();
        let new_tokens = (elapsed.as_secs_f64() * self.refill_rate as f64) as u64;

        if new_tokens > 0 {
            let current = self.tokens.load(Ordering::Acquire);
            let new_value = (current + new_tokens).min(self.max_tokens);
            self.tokens.store(new_value, Ordering::Release);
            *last = Instant::now();
        }
    }
}

/// Statistics for streaming embeddings
#[derive(Clone, Debug, Default)]
pub struct StreamStats {
    /// Total requests processed
    pub total_requests: u64,
    /// Successful requests
    pub successful: u64,
    /// Failed requests
    pub failed: u64,
    /// Total batches processed
    pub batches: u64,
    /// Average batch size
    pub avg_batch_size: f64,
    /// Average latency in ms
    pub avg_latency_ms: f64,
    /// Current queue depth
    pub queue_depth: usize,
    /// Circuit breaker state
    pub circuit_state: String,
    /// Rate limiter tokens available
    pub rate_tokens: u64,
}

/// Streaming embedder with batching and backpressure
pub struct StreamingEmbedder {
    model: Arc<EmbeddingModel>,
    config: StreamingConfig,
    request_tx: mpsc::Sender<StreamRequest>,
    stats: Arc<RwLock<StreamStats>>,
    circuit_breaker: Arc<CircuitBreaker>,
    rate_limiter: Option<Arc<RateLimiter>>,
    semaphore: Arc<Semaphore>,
    shutdown: Arc<AtomicBool>,
}

impl StreamingEmbedder {
    /// Create a new streaming embedder
    pub fn new(model: EmbeddingModel, config: StreamingConfig) -> Self {
        let model = Arc::new(model);
        let (request_tx, request_rx) = mpsc::channel(config.max_queue_size);
        let stats = Arc::new(RwLock::new(StreamStats::default()));
        let circuit_breaker = Arc::new(CircuitBreaker::new(
            config.circuit_breaker_threshold,
            config.circuit_breaker_recovery_secs,
        ));
        let rate_limiter = if config.rate_limit_rps > 0 {
            Some(Arc::new(RateLimiter::new(config.rate_limit_rps)))
        } else {
            None
        };
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent));
        let shutdown = Arc::new(AtomicBool::new(false));

        // Spawn batch processor
        let processor = BatchProcessor {
            model: Arc::clone(&model),
            config: config.clone(),
            stats: Arc::clone(&stats),
            circuit_breaker: Arc::clone(&circuit_breaker),
            rate_limiter: rate_limiter.clone(),
            semaphore: Arc::clone(&semaphore),
            shutdown: Arc::clone(&shutdown),
        };

        tokio::spawn(processor.run(request_rx));

        Self {
            model,
            config,
            request_tx,
            stats,
            circuit_breaker,
            rate_limiter,
            semaphore,
            shutdown,
        }
    }

    /// Create with default configuration
    pub fn with_defaults(model: EmbeddingModel) -> Self {
        Self::new(model, StreamingConfig::default())
    }

    /// Create placeholder for testing
    pub fn placeholder(dimension: usize) -> Self {
        Self::with_defaults(EmbeddingModel::placeholder(dimension))
    }

    /// Get embedding dimension
    pub fn dimension(&self) -> usize {
        self.model.dimension()
    }

    /// Embed a single text
    pub async fn embed(&self, text: &str) -> Result<Vec<f32>, SemanticError> {
        self.embed_with_priority(text, Priority::Normal).await
    }

    /// Embed with priority
    pub async fn embed_with_priority(
        &self,
        text: &str,
        priority: Priority,
    ) -> Result<Vec<f32>, SemanticError> {
        if self.circuit_breaker.is_open() {
            return Err(SemanticError::Internal("Circuit breaker open".to_string()));
        }

        let (response_tx, response_rx) = oneshot::channel();

        let request = StreamRequest {
            text: text.to_string(),
            priority,
            response_tx,
            submitted_at: Instant::now(),
        };

        self.request_tx
            .send(request)
            .await
            .map_err(|_| SemanticError::Internal("Channel closed".to_string()))?;

        let timeout = Duration::from_secs(self.config.request_timeout_secs);
        match tokio::time::timeout(timeout, response_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(SemanticError::Internal(
                "Response channel closed".to_string(),
            )),
            Err(_) => Err(SemanticError::EmbeddingFailed(
                "Request timeout".to_string(),
            )),
        }
    }

    /// Create an embedding stream for multiple texts
    pub fn stream(&self, texts: Vec<String>) -> EmbeddingStream<'_> {
        EmbeddingStream::new(self, texts)
    }

    /// Embed multiple texts with streaming results
    pub async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, SemanticError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::with_capacity(texts.len());
        let mut receivers = Vec::with_capacity(texts.len());

        // Submit all requests
        for text in texts {
            let (response_tx, response_rx) = oneshot::channel();
            let request = StreamRequest {
                text: text.to_string(),
                priority: Priority::Normal,
                response_tx,
                submitted_at: Instant::now(),
            };
            self.request_tx
                .send(request)
                .await
                .map_err(|_| SemanticError::Internal("Channel closed".to_string()))?;
            receivers.push(response_rx);
        }

        // Collect results
        for rx in receivers {
            let result = rx
                .await
                .map_err(|_| SemanticError::Internal("Response channel closed".to_string()))??;
            results.push(result);
        }

        Ok(results)
    }

    /// Get current statistics
    pub fn stats(&self) -> StreamStats {
        let mut stats = self.stats.read().clone();
        stats.circuit_state = format!("{:?}", self.circuit_breaker.state());
        stats.rate_tokens = self
            .rate_limiter
            .as_ref()
            .map(|r| r.tokens.load(Ordering::Relaxed))
            .unwrap_or(u64::MAX);
        stats
    }

    /// Get configuration
    pub fn config(&self) -> &StreamingConfig {
        &self.config
    }

    /// Shutdown the embedder
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }
}

impl Drop for StreamingEmbedder {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Batch processor task
struct BatchProcessor {
    model: Arc<EmbeddingModel>,
    config: StreamingConfig,
    stats: Arc<RwLock<StreamStats>>,
    circuit_breaker: Arc<CircuitBreaker>,
    rate_limiter: Option<Arc<RateLimiter>>,
    semaphore: Arc<Semaphore>,
    shutdown: Arc<AtomicBool>,
}

impl BatchProcessor {
    async fn run(self, mut request_rx: mpsc::Receiver<StreamRequest>) {
        let mut pending: VecDeque<StreamRequest> = VecDeque::new();
        let batch_timeout = Duration::from_millis(self.config.batch_timeout_ms);

        loop {
            if self.shutdown.load(Ordering::SeqCst) && pending.is_empty() {
                break;
            }

            // Receive requests with timeout
            let request = if pending.is_empty() {
                request_rx.recv().await
            } else {
                tokio::time::timeout(batch_timeout, request_rx.recv()).await
                    .unwrap_or_default() // Timeout, process batch
            };

            match request {
                Some(req) => {
                    // Handle critical priority immediately
                    if req.priority == Priority::Critical {
                        self.process_single(req).await;
                        continue;
                    }

                    pending.push_back(req);

                    // Sort by priority if enabled
                    if self.config.enable_priority {
                        #[allow(clippy::iter_with_drain)]
                        let mut vec: Vec<_> = pending.drain(..).collect();
                        vec.sort_by(|a, b| b.priority.cmp(&a.priority));
                        pending.extend(vec);
                    }

                    // Check if we should process
                    if pending.len() >= self.config.batch_size {
                        self.process_batch(&mut pending).await;
                    }
                }
                None => {
                    if !pending.is_empty() {
                        self.process_batch(&mut pending).await;
                    }
                    if self.shutdown.load(Ordering::SeqCst) {
                        break;
                    }
                }
            }
        }
    }

    async fn process_single(&self, request: StreamRequest) {
        // Rate limiting
        if let Some(limiter) = &self.rate_limiter {
            limiter.acquire().await;
        }

        let _permit = match self.semaphore.acquire().await {
            Ok(p) => p,
            Err(_) => {
                let _ = request
                    .response_tx
                    .send(Err(SemanticError::Internal("Semaphore closed".to_string())));
                return;
            }
        };

        let latency = request.submitted_at.elapsed();
        let result = self.model.embed(&request.text);

        // Update stats and circuit breaker
        {
            let mut stats = self.stats.write();
            stats.total_requests += 1;
            match &result {
                Ok(_) => {
                    stats.successful += 1;
                    self.circuit_breaker.record_success();
                }
                Err(_) => {
                    stats.failed += 1;
                    self.circuit_breaker.record_failure();
                }
            }
            let total = stats.total_requests as f64;
            stats.avg_latency_ms =
                (stats.avg_latency_ms * (total - 1.0) + latency.as_secs_f64() * 1000.0) / total;
        }

        let _ = request.response_tx.send(result);
    }

    async fn process_batch(&self, pending: &mut VecDeque<StreamRequest>) {
        let batch_size = pending.len().min(self.config.batch_size);
        let batch: Vec<_> = pending.drain(..batch_size).collect();

        if batch.is_empty() {
            return;
        }

        // Rate limiting
        if let Some(limiter) = &self.rate_limiter {
            limiter.acquire().await;
        }

        let _permit = match self.semaphore.acquire().await {
            Ok(p) => p,
            Err(_) => {
                for req in batch {
                    let _ = req
                        .response_tx
                        .send(Err(SemanticError::Internal("Semaphore closed".to_string())));
                }
                return;
            }
        };

        let texts: Vec<&str> = batch.iter().map(|r| r.text.as_str()).collect();
        let start = Instant::now();
        let result = self.model.embed_batch(&texts);
        let latency = start.elapsed();

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.batches += 1;
            stats.total_requests += batch.len() as u64;
            let total_batches = stats.batches as f64;
            stats.avg_batch_size =
                (stats.avg_batch_size * (total_batches - 1.0) + batch.len() as f64) / total_batches;
            stats.avg_latency_ms = (stats.avg_latency_ms * (total_batches - 1.0)
                + latency.as_secs_f64() * 1000.0)
                / total_batches;
        }

        match result {
            Ok(embeddings) => {
                self.circuit_breaker.record_success();
                {
                    let mut stats = self.stats.write();
                    stats.successful += batch.len() as u64;
                }
                for (req, emb) in batch.into_iter().zip(embeddings) {
                    let _ = req.response_tx.send(Ok(emb));
                }
            }
            Err(e) => {
                self.circuit_breaker.record_failure();
                {
                    let mut stats = self.stats.write();
                    stats.failed += batch.len() as u64;
                }
                for req in batch {
                    let _ = req.response_tx.send(Err(e.clone()));
                }
            }
        }
    }
}

/// Stream of embeddings for async iteration
pub struct EmbeddingStream<'a> {
    embedder: &'a StreamingEmbedder,
    texts: VecDeque<String>,
    pending: Option<oneshot::Receiver<Result<Vec<f32>, SemanticError>>>,
}

impl<'a> EmbeddingStream<'a> {
    fn new(embedder: &'a StreamingEmbedder, texts: Vec<String>) -> Self {
        Self {
            embedder,
            texts: texts.into(),
            pending: None,
        }
    }

    /// Get next embedding
    pub async fn next(&mut self) -> Option<Result<Vec<f32>, SemanticError>> {
        // If we have a pending request, await it
        if let Some(rx) = self.pending.take() {
            match rx.await {
                Ok(result) => return Some(result),
                Err(_) => return Some(Err(SemanticError::Internal("Channel closed".to_string()))),
            }
        }

        // Get next text
        let text = self.texts.pop_front()?;

        // Submit request
        let (response_tx, response_rx) = oneshot::channel();
        let request = StreamRequest {
            text,
            priority: Priority::Normal,
            response_tx,
            submitted_at: Instant::now(),
        };

        match self.embedder.request_tx.send(request).await {
            Ok(()) => match response_rx.await {
                Ok(result) => Some(result),
                Err(_) => Some(Err(SemanticError::Internal("Channel closed".to_string()))),
            },
            Err(_) => Some(Err(SemanticError::Internal("Channel closed".to_string()))),
        }
    }

    /// Check if stream has more items
    pub fn has_next(&self) -> bool {
        !self.texts.is_empty() || self.pending.is_some()
    }

    /// Get remaining count
    pub fn remaining(&self) -> usize {
        self.texts.len() + if self.pending.is_some() { 1 } else { 0 }
    }
}

// Thread safety
unsafe impl Send for StreamingEmbedder {}
unsafe impl Sync for StreamingEmbedder {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_streaming_embedder() {
        let embedder = StreamingEmbedder::placeholder(384);

        let embedding = embedder.embed("test text").await.unwrap();
        assert_eq!(embedding.len(), 384);
    }

    #[tokio::test]
    async fn test_streaming_batch() {
        let embedder = StreamingEmbedder::placeholder(384);

        let texts = vec!["text1", "text2", "text3"];
        let embeddings = embedder.embed_batch(&texts).await.unwrap();

        assert_eq!(embeddings.len(), 3);
        for emb in &embeddings {
            assert_eq!(emb.len(), 384);
        }
    }

    #[tokio::test]
    async fn test_embedding_stream() {
        let embedder = StreamingEmbedder::placeholder(384);

        let texts = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let mut stream = embedder.stream(texts);

        let mut count = 0;
        while let Some(result) = stream.next().await {
            assert!(result.is_ok());
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_streaming_stats() {
        let embedder = StreamingEmbedder::placeholder(384);

        embedder.embed("test1").await.unwrap();
        embedder.embed("test2").await.unwrap();

        let stats = embedder.stats();
        assert!(stats.total_requests >= 2);
        assert!(stats.successful >= 2);
    }

    #[test]
    fn test_streaming_configs() {
        let high_throughput = StreamingConfig::high_throughput();
        assert_eq!(high_throughput.batch_size, 64);
        assert_eq!(high_throughput.max_concurrent, 8);

        let low_latency = StreamingConfig::low_latency();
        assert_eq!(low_latency.batch_size, 8);
        assert!(low_latency.enable_priority);

        let rate_limited = StreamingConfig::rate_limited(100);
        assert_eq!(rate_limited.rate_limit_rps, 100);
    }

    #[tokio::test]
    async fn test_priority_embedding() {
        let config = StreamingConfig {
            enable_priority: true,
            ..Default::default()
        };
        let embedder = StreamingEmbedder::new(EmbeddingModel::placeholder(384), config);

        let result = embedder.embed_with_priority("test", Priority::High).await;
        assert!(result.is_ok());
    }
}
