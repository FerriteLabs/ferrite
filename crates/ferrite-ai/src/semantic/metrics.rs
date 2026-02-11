//! Semantic Cache Metrics
//!
//! Prometheus-compatible metrics for semantic caching operations.
//! Provides observability for cache performance, embedding generation,
//! and cost tracking.
//!
//! # Metrics Exposed
//!
//! - `semantic_cache_hits_total` - Total cache hits
//! - `semantic_cache_misses_total` - Total cache misses
//! - `semantic_cache_entries` - Current cache entries
//! - `embedding_requests_total` - Total embedding requests
//! - `embedding_latency_seconds` - Embedding generation latency
//! - `llm_cost_saved_dollars` - Estimated cost saved
//!
//! # Example
//!
//! ```ignore
//! use ferrite::semantic::metrics::SemanticMetrics;
//!
//! let metrics = SemanticMetrics::new();
//! metrics.record_cache_hit();
//! metrics.record_embedding_latency(Duration::from_millis(50));
//! ```

use metrics::{counter, gauge, histogram};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Semantic cache metrics collector
#[derive(Clone)]
pub struct SemanticMetrics {
    inner: Arc<SemanticMetricsInner>,
}

struct SemanticMetricsInner {
    // Cache metrics
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    exact_hits: AtomicU64,
    semantic_hits: AtomicU64,
    cache_sets: AtomicU64,
    cache_evictions: AtomicU64,

    // Embedding metrics
    embedding_requests: AtomicU64,
    embedding_successes: AtomicU64,
    embedding_failures: AtomicU64,
    embedding_cache_hits: AtomicU64,
    embedding_cache_misses: AtomicU64,

    // Cost tracking
    llm_calls_saved: AtomicU64,
    embedding_tokens: AtomicU64,

    // Circuit breaker
    circuit_opens: AtomicU64,
    circuit_closes: AtomicU64,

    // Prefix for metric names
    prefix: String,
}

impl SemanticMetrics {
    /// Create new metrics with default prefix
    pub fn new() -> Self {
        Self::with_prefix("semantic")
    }

    /// Create metrics with custom prefix
    pub fn with_prefix(prefix: &str) -> Self {
        Self {
            inner: Arc::new(SemanticMetricsInner {
                cache_hits: AtomicU64::new(0),
                cache_misses: AtomicU64::new(0),
                exact_hits: AtomicU64::new(0),
                semantic_hits: AtomicU64::new(0),
                cache_sets: AtomicU64::new(0),
                cache_evictions: AtomicU64::new(0),
                embedding_requests: AtomicU64::new(0),
                embedding_successes: AtomicU64::new(0),
                embedding_failures: AtomicU64::new(0),
                embedding_cache_hits: AtomicU64::new(0),
                embedding_cache_misses: AtomicU64::new(0),
                llm_calls_saved: AtomicU64::new(0),
                embedding_tokens: AtomicU64::new(0),
                circuit_opens: AtomicU64::new(0),
                circuit_closes: AtomicU64::new(0),
                prefix: prefix.to_string(),
            }),
        }
    }

    // === Cache Metrics ===

    /// Record a cache hit
    pub fn record_cache_hit(&self) {
        self.inner.cache_hits.fetch_add(1, Ordering::Relaxed);
        counter!(format!("{}_cache_hits_total", self.inner.prefix)).increment(1);
    }

    /// Record an exact match hit
    pub fn record_exact_hit(&self) {
        self.inner.exact_hits.fetch_add(1, Ordering::Relaxed);
        counter!(format!("{}_cache_exact_hits_total", self.inner.prefix)).increment(1);
    }

    /// Record a semantic match hit
    pub fn record_semantic_hit(&self, similarity: f32) {
        self.inner.semantic_hits.fetch_add(1, Ordering::Relaxed);
        counter!(format!("{}_cache_semantic_hits_total", self.inner.prefix)).increment(1);
        histogram!(format!("{}_cache_similarity", self.inner.prefix)).record(similarity as f64);
    }

    /// Record a cache miss
    pub fn record_cache_miss(&self) {
        self.inner.cache_misses.fetch_add(1, Ordering::Relaxed);
        counter!(format!("{}_cache_misses_total", self.inner.prefix)).increment(1);
    }

    /// Record a cache set operation
    pub fn record_cache_set(&self) {
        self.inner.cache_sets.fetch_add(1, Ordering::Relaxed);
        counter!(format!("{}_cache_sets_total", self.inner.prefix)).increment(1);
    }

    /// Record a cache eviction
    pub fn record_cache_eviction(&self) {
        self.inner.cache_evictions.fetch_add(1, Ordering::Relaxed);
        counter!(format!("{}_cache_evictions_total", self.inner.prefix)).increment(1);
    }

    /// Update current cache size
    pub fn set_cache_size(&self, size: usize) {
        gauge!(format!("{}_cache_entries", self.inner.prefix)).set(size as f64);
    }

    // === Embedding Metrics ===

    /// Record an embedding request
    pub fn record_embedding_request(&self) {
        self.inner
            .embedding_requests
            .fetch_add(1, Ordering::Relaxed);
        counter!(format!("{}_embedding_requests_total", self.inner.prefix)).increment(1);
    }

    /// Record embedding success
    pub fn record_embedding_success(&self) {
        self.inner
            .embedding_successes
            .fetch_add(1, Ordering::Relaxed);
        counter!(format!("{}_embedding_successes_total", self.inner.prefix)).increment(1);
    }

    /// Record embedding failure
    pub fn record_embedding_failure(&self, error_type: &str) {
        self.inner
            .embedding_failures
            .fetch_add(1, Ordering::Relaxed);
        counter!(format!("{}_embedding_failures_total", self.inner.prefix), "error_type" => error_type.to_string()).increment(1);
    }

    /// Record embedding latency
    pub fn record_embedding_latency(&self, duration: Duration) {
        histogram!(format!("{}_embedding_latency_seconds", self.inner.prefix))
            .record(duration.as_secs_f64());
    }

    /// Record embedding cache hit
    pub fn record_embedding_cache_hit(&self) {
        self.inner
            .embedding_cache_hits
            .fetch_add(1, Ordering::Relaxed);
        counter!(format!("{}_embedding_cache_hits_total", self.inner.prefix)).increment(1);
    }

    /// Record embedding cache miss
    pub fn record_embedding_cache_miss(&self) {
        self.inner
            .embedding_cache_misses
            .fetch_add(1, Ordering::Relaxed);
        counter!(format!(
            "{}_embedding_cache_misses_total",
            self.inner.prefix
        ))
        .increment(1);
    }

    /// Record batch processing
    pub fn record_batch_processed(&self, batch_size: usize) {
        counter!(format!("{}_batches_total", self.inner.prefix)).increment(1);
        histogram!(format!("{}_batch_size", self.inner.prefix)).record(batch_size as f64);
    }

    // === Cost Tracking ===

    /// Record an LLM call saved
    pub fn record_llm_call_saved(&self, estimated_cost: f64) {
        self.inner.llm_calls_saved.fetch_add(1, Ordering::Relaxed);
        counter!(format!("{}_llm_calls_saved_total", self.inner.prefix)).increment(1);
        // Convert dollars to micro-cents for integer tracking (1 dollar = 100,000,000 micro-cents)
        let cost_micro_cents = (estimated_cost * 100_000_000.0) as u64;
        counter!(format!("{}_cost_saved_micro_cents", self.inner.prefix))
            .increment(cost_micro_cents);
    }

    /// Record embedding tokens used
    pub fn record_embedding_tokens(&self, tokens: u64) {
        self.inner
            .embedding_tokens
            .fetch_add(tokens, Ordering::Relaxed);
        counter!(format!("{}_embedding_tokens_total", self.inner.prefix)).increment(tokens);
    }

    // === Circuit Breaker ===

    /// Record circuit breaker open
    pub fn record_circuit_open(&self) {
        self.inner.circuit_opens.fetch_add(1, Ordering::Relaxed);
        counter!(format!("{}_circuit_opens_total", self.inner.prefix)).increment(1);
        gauge!(format!("{}_circuit_state", self.inner.prefix)).set(1.0);
    }

    /// Record circuit breaker close
    pub fn record_circuit_close(&self) {
        self.inner.circuit_closes.fetch_add(1, Ordering::Relaxed);
        counter!(format!("{}_circuit_closes_total", self.inner.prefix)).increment(1);
        gauge!(format!("{}_circuit_state", self.inner.prefix)).set(0.0);
    }

    // === Query Latency ===

    /// Start a timed operation
    pub fn start_timer(&self) -> Timer {
        Timer {
            start: Instant::now(),
        }
    }

    /// Record query latency
    pub fn record_query_latency(&self, duration: Duration) {
        histogram!(format!("{}_query_latency_seconds", self.inner.prefix))
            .record(duration.as_secs_f64());
    }

    // === Aggregated Stats ===

    /// Get cache hit rate
    pub fn hit_rate(&self) -> f64 {
        let hits = self.inner.cache_hits.load(Ordering::Relaxed);
        let misses = self.inner.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Get embedding success rate
    pub fn embedding_success_rate(&self) -> f64 {
        let successes = self.inner.embedding_successes.load(Ordering::Relaxed);
        let requests = self.inner.embedding_requests.load(Ordering::Relaxed);
        if requests == 0 {
            1.0
        } else {
            successes as f64 / requests as f64
        }
    }

    /// Get summary statistics
    pub fn summary(&self) -> MetricsSummary {
        MetricsSummary {
            cache_hits: self.inner.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.inner.cache_misses.load(Ordering::Relaxed),
            exact_hits: self.inner.exact_hits.load(Ordering::Relaxed),
            semantic_hits: self.inner.semantic_hits.load(Ordering::Relaxed),
            cache_sets: self.inner.cache_sets.load(Ordering::Relaxed),
            cache_evictions: self.inner.cache_evictions.load(Ordering::Relaxed),
            embedding_requests: self.inner.embedding_requests.load(Ordering::Relaxed),
            embedding_successes: self.inner.embedding_successes.load(Ordering::Relaxed),
            embedding_failures: self.inner.embedding_failures.load(Ordering::Relaxed),
            embedding_cache_hits: self.inner.embedding_cache_hits.load(Ordering::Relaxed),
            embedding_cache_misses: self.inner.embedding_cache_misses.load(Ordering::Relaxed),
            llm_calls_saved: self.inner.llm_calls_saved.load(Ordering::Relaxed),
            embedding_tokens: self.inner.embedding_tokens.load(Ordering::Relaxed),
            hit_rate: self.hit_rate(),
            embedding_success_rate: self.embedding_success_rate(),
        }
    }

    /// Reset all counters
    pub fn reset(&self) {
        self.inner.cache_hits.store(0, Ordering::Relaxed);
        self.inner.cache_misses.store(0, Ordering::Relaxed);
        self.inner.exact_hits.store(0, Ordering::Relaxed);
        self.inner.semantic_hits.store(0, Ordering::Relaxed);
        self.inner.cache_sets.store(0, Ordering::Relaxed);
        self.inner.cache_evictions.store(0, Ordering::Relaxed);
        self.inner.embedding_requests.store(0, Ordering::Relaxed);
        self.inner.embedding_successes.store(0, Ordering::Relaxed);
        self.inner.embedding_failures.store(0, Ordering::Relaxed);
        self.inner.embedding_cache_hits.store(0, Ordering::Relaxed);
        self.inner
            .embedding_cache_misses
            .store(0, Ordering::Relaxed);
        self.inner.llm_calls_saved.store(0, Ordering::Relaxed);
        self.inner.embedding_tokens.store(0, Ordering::Relaxed);
    }
}

impl Default for SemanticMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Timer for measuring operation duration
pub struct Timer {
    start: Instant,
}

impl Timer {
    /// Get elapsed duration
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    /// Stop and return duration
    pub fn stop(self) -> Duration {
        self.elapsed()
    }
}

/// Summary of all metrics
#[derive(Clone, Debug)]
pub struct MetricsSummary {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub exact_hits: u64,
    pub semantic_hits: u64,
    pub cache_sets: u64,
    pub cache_evictions: u64,
    pub embedding_requests: u64,
    pub embedding_successes: u64,
    pub embedding_failures: u64,
    pub embedding_cache_hits: u64,
    pub embedding_cache_misses: u64,
    pub llm_calls_saved: u64,
    pub embedding_tokens: u64,
    pub hit_rate: f64,
    pub embedding_success_rate: f64,
}

/// Metrics-aware wrapper for cache operations
pub struct InstrumentedCache<C> {
    cache: C,
    metrics: SemanticMetrics,
}

impl<C> InstrumentedCache<C> {
    /// Create a new instrumented cache
    pub fn new(cache: C, metrics: SemanticMetrics) -> Self {
        Self { cache, metrics }
    }

    /// Get the underlying cache
    pub fn inner(&self) -> &C {
        &self.cache
    }

    /// Get the metrics
    pub fn metrics(&self) -> &SemanticMetrics {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = SemanticMetrics::new();
        assert_eq!(metrics.hit_rate(), 0.0);
    }

    #[test]
    fn test_cache_hit_rate() {
        let metrics = SemanticMetrics::new();

        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_miss();

        let hit_rate = metrics.hit_rate();
        assert!((hit_rate - 0.6666).abs() < 0.01);
    }

    #[test]
    fn test_embedding_success_rate() {
        let metrics = SemanticMetrics::new();

        metrics.record_embedding_request();
        metrics.record_embedding_success();
        metrics.record_embedding_request();
        metrics.record_embedding_failure("timeout");

        let success_rate = metrics.embedding_success_rate();
        assert_eq!(success_rate, 0.5);
    }

    #[test]
    fn test_summary() {
        let metrics = SemanticMetrics::new();

        metrics.record_cache_hit();
        metrics.record_cache_set();
        metrics.record_llm_call_saved(0.002);

        let summary = metrics.summary();
        assert_eq!(summary.cache_hits, 1);
        assert_eq!(summary.cache_sets, 1);
        assert_eq!(summary.llm_calls_saved, 1);
    }

    #[test]
    fn test_timer() {
        let metrics = SemanticMetrics::new();
        let timer = metrics.start_timer();

        std::thread::sleep(Duration::from_millis(10));

        let duration = timer.stop();
        assert!(duration >= Duration::from_millis(10));
    }

    #[test]
    fn test_reset() {
        let metrics = SemanticMetrics::new();

        metrics.record_cache_hit();
        metrics.record_cache_miss();

        assert!(metrics.hit_rate() > 0.0);

        metrics.reset();

        assert_eq!(metrics.hit_rate(), 0.0);
    }

    #[test]
    fn test_custom_prefix() {
        let metrics = SemanticMetrics::with_prefix("llm_cache");
        metrics.record_cache_hit();
        // Just verify it doesn't panic
    }
}
