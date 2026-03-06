//! Server-level rate limiting using a token bucket algorithm.
//!
//! Provides per-connection command rate limiting to prevent any single client
//! from monopolising server resources. Configurable via `server.rate_limit_per_sec`
//! and `server.rate_limit_burst` in `ferrite.toml`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::Mutex;

/// Token bucket rate limiter for a single connection.
///
/// Tokens refill at `rate_per_sec` tokens per second, up to `burst` capacity.
/// Each command consumes one token. When no tokens are available the command
/// is rejected with an error.
pub struct RateLimiter {
    /// Maximum tokens the bucket can hold (burst size).
    burst: u64,
    /// Tokens added per second.
    rate_per_sec: f64,
    /// Current token count (scaled by 1000 for sub-token precision).
    tokens_milli: Mutex<u64>,
    /// Last refill timestamp.
    last_refill: Mutex<Instant>,
    /// Total commands allowed (for metrics).
    allowed: AtomicU64,
    /// Total commands rejected (for metrics).
    rejected: AtomicU64,
}

impl RateLimiter {
    /// Create a new rate limiter.
    ///
    /// - `rate_per_sec`: sustained commands per second (0 = unlimited)
    /// - `burst`: maximum burst size above the sustained rate
    pub fn new(rate_per_sec: u64, burst: u64) -> Self {
        Self {
            burst,
            rate_per_sec: rate_per_sec as f64,
            tokens_milli: Mutex::new(burst * 1000),
            last_refill: Mutex::new(Instant::now()),
            allowed: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
        }
    }

    /// Try to consume one token. Returns `true` if allowed, `false` if rate-limited.
    pub fn try_acquire(&self) -> bool {
        if self.rate_per_sec == 0.0 {
            return true; // unlimited
        }

        let mut tokens = self.tokens_milli.lock();
        let mut last = self.last_refill.lock();

        // Refill tokens based on elapsed time
        let now = Instant::now();
        let elapsed = now.duration_since(*last).as_secs_f64();
        let refill = (elapsed * self.rate_per_sec * 1000.0) as u64;

        if refill > 0 {
            *tokens = (*tokens + refill).min(self.burst * 1000);
            *last = now;
        }

        // Consume one token (1000 milli-tokens)
        if *tokens >= 1000 {
            *tokens -= 1000;
            self.allowed.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            false
        }
    }

    /// Number of commands allowed since creation.
    pub fn allowed_count(&self) -> u64 {
        self.allowed.load(Ordering::Relaxed)
    }

    /// Number of commands rejected since creation.
    pub fn rejected_count(&self) -> u64 {
        self.rejected.load(Ordering::Relaxed)
    }

    /// Returns `true` when rate limiting is disabled (rate = 0).
    pub fn is_unlimited(&self) -> bool {
        self.rate_per_sec == 0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn unlimited_always_allows() {
        let rl = RateLimiter::new(0, 0);
        for _ in 0..10_000 {
            assert!(rl.try_acquire());
        }
        assert!(rl.is_unlimited());
    }

    #[test]
    fn burst_allows_up_to_capacity() {
        let rl = RateLimiter::new(100, 10);
        // Should allow exactly 10 (burst) immediately
        for _ in 0..10 {
            assert!(rl.try_acquire(), "should allow within burst");
        }
        // 11th should be rejected
        assert!(!rl.try_acquire(), "should reject after burst exhausted");
        assert_eq!(rl.allowed_count(), 10);
        assert_eq!(rl.rejected_count(), 1);
    }

    #[test]
    fn tokens_refill_over_time() {
        let rl = RateLimiter::new(1000, 5);
        // Exhaust burst
        for _ in 0..5 {
            assert!(rl.try_acquire());
        }
        assert!(!rl.try_acquire());

        // Wait for refill (at 1000/sec, 10ms ≈ 10 tokens)
        thread::sleep(Duration::from_millis(20));
        assert!(rl.try_acquire(), "should allow after refill");
    }

    #[test]
    fn metrics_track_correctly() {
        let rl = RateLimiter::new(100, 3);
        for _ in 0..3 {
            let _ = rl.try_acquire();
        }
        let _ = rl.try_acquire(); // rejected
        assert_eq!(rl.allowed_count(), 3);
        assert_eq!(rl.rejected_count(), 1);
    }
}
