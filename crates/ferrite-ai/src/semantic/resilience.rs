//! Resilience Patterns for Semantic Cache
//!
//! Production-ready error recovery, retry logic, and graceful degradation
//! for semantic caching operations.
//!
//! # Patterns Implemented
//!
//! - **Retry with Exponential Backoff**: Automatic retry with increasing delays
//! - **Circuit Breaker**: Fail fast when service is unavailable
//! - **Bulkhead**: Limit concurrent requests to prevent overload
//! - **Fallback**: Graceful degradation when primary fails
//! - **Timeout**: Prevent hanging on slow operations
//!
//! # Example
//!
//! ```ignore
//! use ferrite::semantic::resilience::{RetryPolicy, CircuitBreaker, ResilientEmbedder};
//!
//! let retry = RetryPolicy::exponential()
//!     .max_retries(3)
//!     .initial_delay_ms(100);
//!
//! let circuit = CircuitBreaker::new()
//!     .failure_threshold(5)
//!     .recovery_timeout_secs(30);
//!
//! let embedder = ResilientEmbedder::new(base_embedder)
//!     .with_retry(retry)
//!     .with_circuit_breaker(circuit);
//! ```

use super::{EmbeddingModel, SemanticError};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Retry policy configuration
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Initial delay before first retry (milliseconds)
    pub initial_delay_ms: u64,
    /// Maximum delay between retries (milliseconds)
    pub max_delay_ms: u64,
    /// Multiplier for exponential backoff
    pub backoff_multiplier: f64,
    /// Add random jitter to delays
    pub add_jitter: bool,
    /// Retryable error checker
    pub retryable_errors: RetryableErrors,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay_ms: 100,
            max_delay_ms: 10_000,
            backoff_multiplier: 2.0,
            add_jitter: true,
            retryable_errors: RetryableErrors::default(),
        }
    }
}

impl RetryPolicy {
    /// Create a new retry policy with exponential backoff
    pub fn exponential() -> Self {
        Self::default()
    }

    /// Create a fixed delay retry policy
    pub fn fixed(delay_ms: u64) -> Self {
        Self {
            backoff_multiplier: 1.0,
            initial_delay_ms: delay_ms,
            max_delay_ms: delay_ms,
            ..Default::default()
        }
    }

    /// Create a retry policy with no retries
    pub fn none() -> Self {
        Self {
            max_retries: 0,
            ..Default::default()
        }
    }

    /// Set maximum retries
    pub fn max_retries(mut self, max: u32) -> Self {
        self.max_retries = max;
        self
    }

    /// Set initial delay
    pub fn initial_delay_ms(mut self, ms: u64) -> Self {
        self.initial_delay_ms = ms;
        self
    }

    /// Set maximum delay
    pub fn max_delay_ms(mut self, ms: u64) -> Self {
        self.max_delay_ms = ms;
        self
    }

    /// Set backoff multiplier
    pub fn backoff_multiplier(mut self, mult: f64) -> Self {
        self.backoff_multiplier = mult;
        self
    }

    /// Enable/disable jitter
    pub fn jitter(mut self, enabled: bool) -> Self {
        self.add_jitter = enabled;
        self
    }

    /// Calculate delay for a given attempt
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let base_delay =
            (self.initial_delay_ms as f64) * self.backoff_multiplier.powi(attempt as i32);
        let capped_delay = base_delay.min(self.max_delay_ms as f64);

        let final_delay = if self.add_jitter {
            // Add up to 25% jitter
            let jitter = rand::random::<f64>() * 0.25 * capped_delay;
            capped_delay + jitter
        } else {
            capped_delay
        };

        Duration::from_millis(final_delay as u64)
    }

    /// Check if an error is retryable
    pub fn is_retryable(&self, error: &SemanticError) -> bool {
        self.retryable_errors.is_retryable(error)
    }
}

/// Configuration for which errors are retryable
#[derive(Clone, Debug, Default)]
pub struct RetryableErrors {
    /// Retry on embedding failures
    pub embedding_failures: bool,
    /// Retry on internal errors
    pub internal_errors: bool,
    /// Retry on timeout errors
    pub timeouts: bool,
}

impl RetryableErrors {
    /// All errors are retryable
    pub fn all() -> Self {
        Self {
            embedding_failures: true,
            internal_errors: true,
            timeouts: true,
        }
    }

    /// Only transient errors are retryable
    pub fn transient_only() -> Self {
        Self {
            embedding_failures: true,
            internal_errors: false,
            timeouts: true,
        }
    }

    fn is_retryable(&self, error: &SemanticError) -> bool {
        match error {
            SemanticError::EmbeddingFailed(msg) => {
                if msg.contains("timeout") || msg.contains("Timeout") {
                    self.timeouts
                } else {
                    self.embedding_failures
                }
            }
            SemanticError::Internal(_) => self.internal_errors,
            _ => false, // Don't retry validation errors
        }
    }
}

/// Circuit breaker state
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CircuitState {
    /// Circuit is closed (normal operation)
    Closed,
    /// Circuit is open (failing fast)
    Open,
    /// Circuit is half-open (testing recovery)
    HalfOpen,
}

/// Circuit breaker for fault tolerance
pub struct CircuitBreaker {
    /// Current state
    state: RwLock<CircuitState>,
    /// Consecutive failure count
    failure_count: AtomicU32,
    /// Success count in half-open state
    half_open_successes: AtomicU32,
    /// Last state transition time
    last_transition: RwLock<Instant>,
    /// Configuration
    config: CircuitBreakerConfig,
    /// Statistics
    stats: CircuitBreakerStats,
}

/// Circuit breaker configuration
#[derive(Clone, Debug)]
pub struct CircuitBreakerConfig {
    /// Number of failures before opening
    pub failure_threshold: u32,
    /// Duration before attempting recovery (seconds)
    pub recovery_timeout_secs: u64,
    /// Successes needed in half-open to close
    pub success_threshold: u32,
    /// Enable automatic recovery
    pub auto_recovery: bool,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            recovery_timeout_secs: 30,
            success_threshold: 3,
            auto_recovery: true,
        }
    }
}

/// Circuit breaker statistics
#[derive(Default)]
struct CircuitBreakerStats {
    total_calls: AtomicU64,
    rejected_calls: AtomicU64,
    successful_calls: AtomicU64,
    failed_calls: AtomicU64,
    state_transitions: AtomicU64,
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    pub fn new() -> Self {
        Self::with_config(CircuitBreakerConfig::default())
    }

    /// Create with custom configuration
    pub fn with_config(config: CircuitBreakerConfig) -> Self {
        Self {
            state: RwLock::new(CircuitState::Closed),
            failure_count: AtomicU32::new(0),
            half_open_successes: AtomicU32::new(0),
            last_transition: RwLock::new(Instant::now()),
            config,
            stats: CircuitBreakerStats::default(),
        }
    }

    /// Set failure threshold
    pub fn failure_threshold(mut self, threshold: u32) -> Self {
        self.config.failure_threshold = threshold;
        self
    }

    /// Set recovery timeout
    pub fn recovery_timeout_secs(mut self, secs: u64) -> Self {
        self.config.recovery_timeout_secs = secs;
        self
    }

    /// Set success threshold for half-open recovery
    pub fn success_threshold(mut self, threshold: u32) -> Self {
        self.config.success_threshold = threshold;
        self
    }

    /// Check if request is allowed
    pub fn allow_request(&self) -> bool {
        self.stats.total_calls.fetch_add(1, Ordering::Relaxed);

        let state = *self.state.read();
        match state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                // Check if we should transition to half-open
                if self.config.auto_recovery {
                    let elapsed = self.last_transition.read().elapsed();
                    if elapsed >= Duration::from_secs(self.config.recovery_timeout_secs) {
                        self.transition_to(CircuitState::HalfOpen);
                        return true;
                    }
                }
                self.stats.rejected_calls.fetch_add(1, Ordering::Relaxed);
                false
            }
            CircuitState::HalfOpen => true, // Allow limited requests
        }
    }

    /// Record a successful operation
    pub fn record_success(&self) {
        self.stats.successful_calls.fetch_add(1, Ordering::Relaxed);

        let state = *self.state.read();
        match state {
            CircuitState::HalfOpen => {
                let successes = self.half_open_successes.fetch_add(1, Ordering::SeqCst) + 1;
                if successes >= self.config.success_threshold {
                    self.transition_to(CircuitState::Closed);
                }
            }
            CircuitState::Closed => {
                // Reset failure count on success
                self.failure_count.store(0, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Record a failed operation
    pub fn record_failure(&self) {
        self.stats.failed_calls.fetch_add(1, Ordering::Relaxed);

        let state = *self.state.read();
        match state {
            CircuitState::Closed => {
                let failures = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;
                if failures >= self.config.failure_threshold {
                    self.transition_to(CircuitState::Open);
                }
            }
            CircuitState::HalfOpen => {
                // Single failure in half-open returns to open
                self.transition_to(CircuitState::Open);
            }
            _ => {}
        }
    }

    /// Get current state
    pub fn state(&self) -> CircuitState {
        *self.state.read()
    }

    /// Get statistics
    pub fn stats(&self) -> CircuitBreakerStatistics {
        CircuitBreakerStatistics {
            total_calls: self.stats.total_calls.load(Ordering::Relaxed),
            rejected_calls: self.stats.rejected_calls.load(Ordering::Relaxed),
            successful_calls: self.stats.successful_calls.load(Ordering::Relaxed),
            failed_calls: self.stats.failed_calls.load(Ordering::Relaxed),
            state_transitions: self.stats.state_transitions.load(Ordering::Relaxed),
            current_state: self.state(),
        }
    }

    /// Force open the circuit
    pub fn force_open(&self) {
        self.transition_to(CircuitState::Open);
    }

    /// Force close the circuit
    pub fn force_close(&self) {
        self.transition_to(CircuitState::Closed);
    }

    /// Reset the circuit breaker
    pub fn reset(&self) {
        self.transition_to(CircuitState::Closed);
        self.failure_count.store(0, Ordering::Relaxed);
        self.half_open_successes.store(0, Ordering::Relaxed);
    }

    fn transition_to(&self, new_state: CircuitState) {
        let mut state = self.state.write();
        if *state != new_state {
            *state = new_state;
            *self.last_transition.write() = Instant::now();
            self.stats.state_transitions.fetch_add(1, Ordering::Relaxed);

            // Reset counters on state change
            match new_state {
                CircuitState::Closed => {
                    self.failure_count.store(0, Ordering::Relaxed);
                }
                CircuitState::HalfOpen => {
                    self.half_open_successes.store(0, Ordering::Relaxed);
                }
                CircuitState::Open => {}
            }
        }
    }
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new()
    }
}

/// Circuit breaker statistics
#[derive(Clone, Debug)]
pub struct CircuitBreakerStatistics {
    pub total_calls: u64,
    pub rejected_calls: u64,
    pub successful_calls: u64,
    pub failed_calls: u64,
    pub state_transitions: u64,
    pub current_state: CircuitState,
}

/// Bulkhead for limiting concurrency
pub struct Bulkhead {
    /// Available permits
    permits: AtomicU32,
    /// Maximum permits
    max_permits: u32,
    /// Waiting queue size
    queue_size: AtomicU32,
    /// Maximum queue size
    max_queue: u32,
    /// Statistics
    stats: BulkheadStats,
}

#[derive(Default)]
struct BulkheadStats {
    acquired: AtomicU64,
    rejected: AtomicU64,
    queued: AtomicU64,
}

impl Bulkhead {
    /// Create a new bulkhead
    pub fn new(max_concurrent: u32, max_queue: u32) -> Self {
        Self {
            permits: AtomicU32::new(max_concurrent),
            max_permits: max_concurrent,
            queue_size: AtomicU32::new(0),
            max_queue,
            stats: BulkheadStats::default(),
        }
    }

    /// Try to acquire a permit (non-blocking)
    pub fn try_acquire(&self) -> Option<BulkheadPermit<'_>> {
        loop {
            let current = self.permits.load(Ordering::Acquire);
            if current == 0 {
                self.stats.rejected.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            if self
                .permits
                .compare_exchange(current, current - 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                self.stats.acquired.fetch_add(1, Ordering::Relaxed);
                return Some(BulkheadPermit { bulkhead: self });
            }
        }
    }

    /// Acquire a permit (blocking until available)
    pub async fn acquire(&self) -> Option<BulkheadPermit<'_>> {
        // First try non-blocking
        if let Some(permit) = self.try_acquire() {
            return Some(permit);
        }

        // Check queue capacity
        let queue = self.queue_size.fetch_add(1, Ordering::SeqCst);
        if queue >= self.max_queue {
            self.queue_size.fetch_sub(1, Ordering::SeqCst);
            self.stats.rejected.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        self.stats.queued.fetch_add(1, Ordering::Relaxed);

        // Wait for permit
        loop {
            sleep(Duration::from_millis(10)).await;
            if let Some(permit) = self.try_acquire() {
                self.queue_size.fetch_sub(1, Ordering::SeqCst);
                return Some(permit);
            }
        }
    }

    /// Release a permit
    fn release(&self) {
        self.permits.fetch_add(1, Ordering::Release);
    }

    /// Get available permits
    pub fn available(&self) -> u32 {
        self.permits.load(Ordering::Relaxed)
    }

    /// Get queue size
    pub fn queue_depth(&self) -> u32 {
        self.queue_size.load(Ordering::Relaxed)
    }
}

/// RAII permit for bulkhead
pub struct BulkheadPermit<'a> {
    bulkhead: &'a Bulkhead,
}

impl<'a> Drop for BulkheadPermit<'a> {
    fn drop(&mut self) {
        self.bulkhead.release();
    }
}

/// Timeout wrapper for operations
pub struct Timeout {
    duration: Duration,
}

impl Timeout {
    /// Create a new timeout
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }

    /// Create from milliseconds
    pub fn from_millis(ms: u64) -> Self {
        Self::new(Duration::from_millis(ms))
    }

    /// Create from seconds
    pub fn from_secs(secs: u64) -> Self {
        Self::new(Duration::from_secs(secs))
    }

    /// Execute an async operation with timeout
    pub async fn execute<F, T, E>(&self, fut: F) -> Result<T, E>
    where
        F: std::future::Future<Output = Result<T, E>>,
        E: From<TimeoutError>,
    {
        match tokio::time::timeout(self.duration, fut).await {
            Ok(result) => result,
            Err(_) => Err(TimeoutError.into()),
        }
    }
}

/// Timeout error
#[derive(Debug, Clone)]
pub struct TimeoutError;

impl std::fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "operation timed out")
    }
}

impl std::error::Error for TimeoutError {}

impl From<TimeoutError> for SemanticError {
    fn from(_: TimeoutError) -> Self {
        SemanticError::EmbeddingFailed("Timeout".to_string())
    }
}

/// Fallback configuration
#[derive(Clone)]
pub struct FallbackStrategy<T> {
    /// Fallback value generator
    #[allow(clippy::type_complexity)]
    fallback_fn: Arc<dyn Fn(&SemanticError) -> Option<T> + Send + Sync>,
    /// Errors to apply fallback for
    applicable_errors: Vec<FallbackErrorType>,
}

/// Types of errors that trigger fallback
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FallbackErrorType {
    /// All errors
    All,
    /// Only timeout errors
    Timeout,
    /// Only circuit breaker rejections
    CircuitOpen,
    /// Only embedding failures
    EmbeddingFailure,
}

impl<T> FallbackStrategy<T> {
    /// Create a fallback with static value
    pub fn static_value(value: T) -> Self
    where
        T: Clone + Send + Sync + 'static,
    {
        Self {
            fallback_fn: Arc::new(move |_| Some(value.clone())),
            applicable_errors: vec![FallbackErrorType::All],
        }
    }

    /// Create a fallback with dynamic generator
    pub fn dynamic<F>(f: F) -> Self
    where
        F: Fn(&SemanticError) -> Option<T> + Send + Sync + 'static,
    {
        Self {
            fallback_fn: Arc::new(f),
            applicable_errors: vec![FallbackErrorType::All],
        }
    }

    /// Limit fallback to specific error types
    pub fn for_errors(mut self, errors: Vec<FallbackErrorType>) -> Self {
        self.applicable_errors = errors;
        self
    }

    /// Check if fallback applies to this error
    pub fn applies_to(&self, error: &SemanticError) -> bool {
        self.applicable_errors.iter().any(|e| match e {
            FallbackErrorType::All => true,
            FallbackErrorType::Timeout => {
                matches!(error, SemanticError::EmbeddingFailed(msg) if msg.contains("timeout") || msg.contains("Timeout"))
            }
            FallbackErrorType::CircuitOpen => {
                matches!(error, SemanticError::Internal(msg) if msg.contains("Circuit"))
            }
            FallbackErrorType::EmbeddingFailure => {
                matches!(error, SemanticError::EmbeddingFailed(_))
            }
        })
    }

    /// Get fallback value
    pub fn get_fallback(&self, error: &SemanticError) -> Option<T> {
        if self.applies_to(error) {
            (self.fallback_fn)(error)
        } else {
            None
        }
    }
}

/// Resilient embedder with all resilience patterns
pub struct ResilientEmbedder {
    /// Base embedding model
    model: Arc<EmbeddingModel>,
    /// Retry policy
    retry_policy: Option<RetryPolicy>,
    /// Circuit breaker
    circuit_breaker: Option<Arc<CircuitBreaker>>,
    /// Bulkhead
    bulkhead: Option<Arc<Bulkhead>>,
    /// Timeout
    timeout: Option<Duration>,
    /// Fallback for embeddings
    fallback: Option<FallbackStrategy<Vec<f32>>>,
}

impl ResilientEmbedder {
    /// Create a new resilient embedder
    pub fn new(model: EmbeddingModel) -> Self {
        Self {
            model: Arc::new(model),
            retry_policy: None,
            circuit_breaker: None,
            bulkhead: None,
            timeout: None,
            fallback: None,
        }
    }

    /// Add retry policy
    pub fn with_retry(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    /// Add circuit breaker
    pub fn with_circuit_breaker(mut self, cb: CircuitBreaker) -> Self {
        self.circuit_breaker = Some(Arc::new(cb));
        self
    }

    /// Add bulkhead
    pub fn with_bulkhead(mut self, max_concurrent: u32, max_queue: u32) -> Self {
        self.bulkhead = Some(Arc::new(Bulkhead::new(max_concurrent, max_queue)));
        self
    }

    /// Add timeout
    pub fn with_timeout(mut self, duration: Duration) -> Self {
        self.timeout = Some(duration);
        self
    }

    /// Add fallback
    pub fn with_fallback(mut self, fallback: FallbackStrategy<Vec<f32>>) -> Self {
        self.fallback = Some(fallback);
        self
    }

    /// Generate embedding with resilience patterns
    pub async fn embed(&self, text: &str) -> Result<Vec<f32>, SemanticError> {
        // Check circuit breaker
        if let Some(cb) = &self.circuit_breaker {
            if !cb.allow_request() {
                let error = SemanticError::Internal("Circuit breaker open".to_string());
                if let Some(fallback) = &self.fallback {
                    if let Some(value) = fallback.get_fallback(&error) {
                        return Ok(value);
                    }
                }
                return Err(error);
            }
        }

        // Acquire bulkhead permit
        let _permit = if let Some(bulkhead) = &self.bulkhead {
            match bulkhead.acquire().await {
                Some(p) => Some(p),
                None => {
                    let error = SemanticError::Internal("Bulkhead full".to_string());
                    if let Some(fallback) = &self.fallback {
                        if let Some(value) = fallback.get_fallback(&error) {
                            return Ok(value);
                        }
                    }
                    return Err(error);
                }
            }
        } else {
            None
        };

        // Execute with retry
        let result = self.execute_with_retry(text).await;

        // Update circuit breaker
        if let Some(cb) = &self.circuit_breaker {
            match &result {
                Ok(_) => cb.record_success(),
                Err(_) => cb.record_failure(),
            }
        }

        // Apply fallback if needed
        match result {
            Ok(v) => Ok(v),
            Err(e) => {
                if let Some(fallback) = &self.fallback {
                    if let Some(value) = fallback.get_fallback(&e) {
                        return Ok(value);
                    }
                }
                Err(e)
            }
        }
    }

    async fn execute_with_retry(&self, text: &str) -> Result<Vec<f32>, SemanticError> {
        let policy = self.retry_policy.clone().unwrap_or(RetryPolicy::none());
        let mut last_error = None;

        for attempt in 0..=policy.max_retries {
            if attempt > 0 {
                let delay = policy.delay_for_attempt(attempt - 1);
                sleep(delay).await;
            }

            let result = self.execute_single(text).await;

            match result {
                Ok(embedding) => return Ok(embedding),
                Err(e) => {
                    if !policy.is_retryable(&e) || attempt == policy.max_retries {
                        return Err(e);
                    }
                    last_error = Some(e);
                }
            }
        }

        Err(last_error
            .unwrap_or_else(|| SemanticError::EmbeddingFailed("Unknown error".to_string())))
    }

    async fn execute_single(&self, text: &str) -> Result<Vec<f32>, SemanticError> {
        let model = Arc::clone(&self.model);
        let text_owned = text.to_string();

        let fut = tokio::task::spawn_blocking(move || model.embed(&text_owned));

        if let Some(timeout) = self.timeout {
            match tokio::time::timeout(timeout, fut).await {
                Ok(Ok(result)) => result,
                Ok(Err(e)) => Err(SemanticError::Internal(format!("Task error: {}", e))),
                Err(_) => Err(SemanticError::EmbeddingFailed("Timeout".to_string())),
            }
        } else {
            fut.await
                .map_err(|e| SemanticError::Internal(format!("Task error: {}", e)))?
        }
    }

    /// Get dimension
    pub fn dimension(&self) -> usize {
        self.model.dimension()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_policy_delay() {
        let policy = RetryPolicy::exponential()
            .initial_delay_ms(100)
            .backoff_multiplier(2.0)
            .jitter(false);

        assert_eq!(policy.delay_for_attempt(0), Duration::from_millis(100));
        assert_eq!(policy.delay_for_attempt(1), Duration::from_millis(200));
        assert_eq!(policy.delay_for_attempt(2), Duration::from_millis(400));
    }

    #[test]
    fn test_retry_policy_max_delay() {
        let policy = RetryPolicy::exponential()
            .initial_delay_ms(1000)
            .max_delay_ms(2000)
            .jitter(false);

        assert_eq!(policy.delay_for_attempt(5), Duration::from_millis(2000));
    }

    #[test]
    fn test_circuit_breaker_state_transitions() {
        let cb = CircuitBreaker::new().failure_threshold(2);

        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow_request());

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        assert!(!cb.allow_request());
    }

    #[test]
    fn test_circuit_breaker_reset_on_success() {
        let cb = CircuitBreaker::new().failure_threshold(3);

        cb.record_failure();
        cb.record_failure();
        cb.record_success(); // Should reset

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_bulkhead_basic() {
        let bulkhead = Bulkhead::new(2, 10);

        let p1 = bulkhead.try_acquire();
        assert!(p1.is_some());
        assert_eq!(bulkhead.available(), 1);

        let p2 = bulkhead.try_acquire();
        assert!(p2.is_some());
        assert_eq!(bulkhead.available(), 0);

        let p3 = bulkhead.try_acquire();
        assert!(p3.is_none());

        drop(p1);
        assert_eq!(bulkhead.available(), 1);
    }

    #[test]
    fn test_fallback_strategy() {
        let fallback = FallbackStrategy::static_value(vec![0.0; 384]);

        let error = SemanticError::EmbeddingFailed("test".to_string());
        let result = fallback.get_fallback(&error);

        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 384);
    }

    #[test]
    fn test_retryable_errors() {
        let errors = RetryableErrors::transient_only();

        assert!(errors.is_retryable(&SemanticError::EmbeddingFailed("timeout".to_string())));
        assert!(errors.is_retryable(&SemanticError::EmbeddingFailed("api error".to_string())));
        assert!(!errors.is_retryable(&SemanticError::DimensionMismatch {
            expected: 384,
            got: 256,
        }));
    }

    #[tokio::test]
    async fn test_resilient_embedder() {
        let model = EmbeddingModel::placeholder(384);
        let embedder = ResilientEmbedder::new(model)
            .with_retry(RetryPolicy::fixed(10).max_retries(2))
            .with_timeout(Duration::from_secs(5));

        let result = embedder.embed("test").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 384);
    }
}
