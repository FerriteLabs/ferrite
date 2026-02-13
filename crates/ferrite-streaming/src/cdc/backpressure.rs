//! Backpressure and Flow Control
//!
//! Provides configurable buffer sizes, overflow behaviours, producer flow control,
//! and per-stream rate limiting.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{Notify, RwLock};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum BackpressureError {
    #[error("buffer full (capacity {capacity}, overflow policy: {policy})")]
    BufferFull { capacity: usize, policy: String },

    #[error("rate limit exceeded ({events_per_sec} events/s)")]
    RateLimitExceeded { events_per_sec: f64 },

    #[error("producer blocked for longer than {timeout:?}")]
    ProducerBlocked { timeout: Duration },
}

// ---------------------------------------------------------------------------
// OverflowPolicy
// ---------------------------------------------------------------------------

/// What happens when a buffer is full.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Block the producer until space is available.
    #[default]
    Block,
    /// Drop the oldest event in the buffer.
    DropOldest,
    /// Drop the incoming (newest) event.
    DropNewest,
    /// Return an error immediately.
    Error,
}

impl std::fmt::Display for OverflowPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Block => write!(f, "block"),
            Self::DropOldest => write!(f, "drop_oldest"),
            Self::DropNewest => write!(f, "drop_newest"),
            Self::Error => write!(f, "error"),
        }
    }
}

// ---------------------------------------------------------------------------
// BackpressureConfig
// ---------------------------------------------------------------------------

/// Configuration for backpressure on a single stream.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BackpressureConfig {
    /// Maximum number of events in the buffer.
    pub buffer_capacity: usize,
    /// What to do when the buffer is full.
    pub overflow_policy: OverflowPolicy,
    /// Maximum time a producer blocks when using `OverflowPolicy::Block`.
    pub block_timeout: Duration,
    /// Optional rate limit: maximum events per second (0 = unlimited).
    pub rate_limit_per_sec: f64,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            buffer_capacity: 10_000,
            overflow_policy: OverflowPolicy::Block,
            block_timeout: Duration::from_secs(5),
            rate_limit_per_sec: 0.0, // unlimited
        }
    }
}

// ---------------------------------------------------------------------------
// RateLimiter (token-bucket)
// ---------------------------------------------------------------------------

/// Simple token-bucket rate limiter.
pub struct RateLimiter {
    /// Tokens per second.
    rate: f64,
    /// Current token count (scaled by 1000 for precision).
    tokens: RwLock<f64>,
    /// Maximum burst (== rate for 1s worth).
    max_tokens: f64,
    /// Last refill timestamp.
    last_refill: RwLock<Instant>,
}

impl RateLimiter {
    /// Create a new rate limiter. `rate` is events/sec; 0 means unlimited.
    pub fn new(rate: f64) -> Self {
        let effective = if rate <= 0.0 { f64::MAX } else { rate };
        Self {
            rate: effective,
            tokens: RwLock::new(effective),
            max_tokens: effective,
            last_refill: RwLock::new(Instant::now()),
        }
    }

    /// Try to acquire one token. Returns `true` if allowed.
    pub async fn try_acquire(&self) -> bool {
        self.refill().await;
        let mut tokens = self.tokens.write().await;
        if *tokens >= 1.0 {
            *tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Wait until a token is available.
    pub async fn acquire(&self) {
        loop {
            if self.try_acquire().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Refill tokens based on elapsed time.
    async fn refill(&self) {
        let mut last = self.last_refill.write().await;
        let now = Instant::now();
        let elapsed = now.duration_since(*last).as_secs_f64();
        if elapsed > 0.0 {
            let mut tokens = self.tokens.write().await;
            *tokens = (*tokens + self.rate * elapsed).min(self.max_tokens);
            *last = now;
        }
    }

    /// Current available tokens (approximate).
    pub async fn available(&self) -> f64 {
        *self.tokens.read().await
    }
}

// ---------------------------------------------------------------------------
// BackpressureController
// ---------------------------------------------------------------------------

/// Manages a bounded buffer with configurable overflow and optional rate limiting.
pub struct BackpressureController<T> {
    config: BackpressureConfig,
    buffer: RwLock<VecDeque<T>>,
    rate_limiter: Option<RateLimiter>,
    notify: Notify,
    /// Events accepted.
    accepted: AtomicU64,
    /// Events dropped due to overflow.
    dropped: AtomicU64,
    /// Events rejected with error.
    rejected: AtomicU64,
    /// Times producer was blocked.
    blocked: AtomicU64,
}

impl<T: Clone + Send + Sync + 'static> BackpressureController<T> {
    pub fn new(config: BackpressureConfig) -> Self {
        let rate_limiter = if config.rate_limit_per_sec > 0.0 {
            Some(RateLimiter::new(config.rate_limit_per_sec))
        } else {
            None
        };
        Self {
            config,
            buffer: RwLock::new(VecDeque::new()),
            rate_limiter,
            notify: Notify::new(),
            accepted: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            blocked: AtomicU64::new(0),
        }
    }

    /// Push an event into the buffer, applying backpressure as configured.
    pub async fn push(&self, item: T) -> Result<(), BackpressureError> {
        // Rate limit check
        if let Some(ref rl) = self.rate_limiter {
            if !rl.try_acquire().await {
                return Err(BackpressureError::RateLimitExceeded {
                    events_per_sec: self.config.rate_limit_per_sec,
                });
            }
        }

        let mut buf = self.buffer.write().await;

        if buf.len() < self.config.buffer_capacity {
            buf.push_back(item);
            self.accepted.fetch_add(1, Ordering::Relaxed);
            self.notify.notify_one();
            return Ok(());
        }

        // Buffer is full – apply overflow policy.
        match self.config.overflow_policy {
            OverflowPolicy::Block => {
                drop(buf);
                self.blocked.fetch_add(1, Ordering::Relaxed);

                let deadline = Instant::now() + self.config.block_timeout;
                loop {
                    let buf = self.buffer.read().await;
                    if buf.len() < self.config.buffer_capacity {
                        drop(buf);
                        break;
                    }
                    drop(buf);

                    if Instant::now() >= deadline {
                        return Err(BackpressureError::ProducerBlocked {
                            timeout: self.config.block_timeout,
                        });
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }

                let mut buf = self.buffer.write().await;
                buf.push_back(item);
                self.accepted.fetch_add(1, Ordering::Relaxed);
                self.notify.notify_one();
                Ok(())
            }
            OverflowPolicy::DropOldest => {
                buf.pop_front();
                buf.push_back(item);
                self.accepted.fetch_add(1, Ordering::Relaxed);
                self.dropped.fetch_add(1, Ordering::Relaxed);
                self.notify.notify_one();
                Ok(())
            }
            OverflowPolicy::DropNewest => {
                // Drop the incoming item.
                self.dropped.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            OverflowPolicy::Error => {
                self.rejected.fetch_add(1, Ordering::Relaxed);
                Err(BackpressureError::BufferFull {
                    capacity: self.config.buffer_capacity,
                    policy: self.config.overflow_policy.to_string(),
                })
            }
        }
    }

    /// Pop an event from the buffer.
    pub async fn pop(&self) -> Option<T> {
        self.buffer.write().await.pop_front()
    }

    /// Pop up to `max` events from the buffer.
    pub async fn pop_batch(&self, max: usize) -> Vec<T> {
        let mut buf = self.buffer.write().await;
        let n = max.min(buf.len());
        buf.drain(..n).collect()
    }

    /// Current number of buffered events.
    pub async fn len(&self) -> usize {
        self.buffer.read().await.len()
    }

    /// Buffer utilization as a fraction in [0.0, 1.0].
    pub async fn utilization(&self) -> f64 {
        let len = self.buffer.read().await.len();
        len as f64 / self.config.buffer_capacity as f64
    }

    /// Whether the buffer is empty.
    pub async fn is_empty(&self) -> bool {
        self.buffer.read().await.is_empty()
    }

    /// Runtime statistics.
    pub fn stats(&self) -> BackpressureStats {
        BackpressureStats {
            accepted: self.accepted.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            blocked: self.blocked.load(Ordering::Relaxed),
            buffer_capacity: self.config.buffer_capacity,
        }
    }
}

/// Runtime statistics for backpressure.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BackpressureStats {
    pub accepted: u64,
    pub dropped: u64,
    pub rejected: u64,
    pub blocked: u64,
    pub buffer_capacity: usize,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_push_pop_basic() {
        let ctrl = BackpressureController::new(BackpressureConfig {
            buffer_capacity: 10,
            overflow_policy: OverflowPolicy::Error,
            ..Default::default()
        });

        ctrl.push(1u64).await.unwrap();
        ctrl.push(2u64).await.unwrap();
        assert_eq!(ctrl.len().await, 2);

        assert_eq!(ctrl.pop().await, Some(1));
        assert_eq!(ctrl.pop().await, Some(2));
        assert!(ctrl.is_empty().await);
    }

    #[tokio::test]
    async fn test_overflow_error() {
        let ctrl = BackpressureController::new(BackpressureConfig {
            buffer_capacity: 2,
            overflow_policy: OverflowPolicy::Error,
            ..Default::default()
        });

        ctrl.push(1u64).await.unwrap();
        ctrl.push(2u64).await.unwrap();

        let result = ctrl.push(3u64).await;
        assert!(result.is_err());
        assert_eq!(ctrl.stats().rejected, 1);
    }

    #[tokio::test]
    async fn test_overflow_drop_oldest() {
        let ctrl = BackpressureController::new(BackpressureConfig {
            buffer_capacity: 2,
            overflow_policy: OverflowPolicy::DropOldest,
            ..Default::default()
        });

        ctrl.push(1u64).await.unwrap();
        ctrl.push(2u64).await.unwrap();
        ctrl.push(3u64).await.unwrap(); // drops 1

        assert_eq!(ctrl.len().await, 2);
        assert_eq!(ctrl.pop().await, Some(2));
        assert_eq!(ctrl.pop().await, Some(3));
        assert_eq!(ctrl.stats().dropped, 1);
    }

    #[tokio::test]
    async fn test_overflow_drop_newest() {
        let ctrl = BackpressureController::new(BackpressureConfig {
            buffer_capacity: 2,
            overflow_policy: OverflowPolicy::DropNewest,
            ..Default::default()
        });

        ctrl.push(1u64).await.unwrap();
        ctrl.push(2u64).await.unwrap();
        ctrl.push(3u64).await.unwrap(); // dropped silently

        assert_eq!(ctrl.len().await, 2);
        assert_eq!(ctrl.pop().await, Some(1));
        assert_eq!(ctrl.pop().await, Some(2));
        assert_eq!(ctrl.stats().dropped, 1);
    }

    #[tokio::test]
    async fn test_overflow_block_timeout() {
        let ctrl = BackpressureController::new(BackpressureConfig {
            buffer_capacity: 1,
            overflow_policy: OverflowPolicy::Block,
            block_timeout: Duration::from_millis(50),
            ..Default::default()
        });

        ctrl.push(1u64).await.unwrap();

        let result = ctrl.push(2u64).await;
        assert!(result.is_err());
        assert_eq!(ctrl.stats().blocked, 1);
    }

    #[tokio::test]
    async fn test_pop_batch() {
        let ctrl = BackpressureController::new(BackpressureConfig {
            buffer_capacity: 100,
            ..Default::default()
        });

        for i in 0..10 {
            ctrl.push(i as u64).await.unwrap();
        }

        let batch = ctrl.pop_batch(5).await;
        assert_eq!(batch.len(), 5);
        assert_eq!(batch, vec![0, 1, 2, 3, 4]);
        assert_eq!(ctrl.len().await, 5);
    }

    #[tokio::test]
    async fn test_utilization() {
        let ctrl = BackpressureController::new(BackpressureConfig {
            buffer_capacity: 4,
            overflow_policy: OverflowPolicy::Error,
            ..Default::default()
        });

        assert_eq!(ctrl.utilization().await, 0.0);
        ctrl.push(1u64).await.unwrap();
        assert!((ctrl.utilization().await - 0.25).abs() < 0.01);
        ctrl.push(2u64).await.unwrap();
        assert!((ctrl.utilization().await - 0.5).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_rate_limiter_basic() {
        let rl = RateLimiter::new(1000.0);
        // Should be able to acquire tokens immediately
        assert!(rl.try_acquire().await);
    }

    #[tokio::test]
    async fn test_rate_limiter_unlimited() {
        let rl = RateLimiter::new(0.0);
        for _ in 0..100 {
            assert!(rl.try_acquire().await);
        }
    }
}
