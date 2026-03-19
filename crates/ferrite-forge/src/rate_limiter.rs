//! Per-tenant FN.CALL rate limiter (token bucket).

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

pub struct CallBudget {
    inner: RwLock<BudgetInner>,
}

struct BudgetInner {
    buckets: HashMap<String, TokenBucket>,
    rate: f64,
    capacity: u64,
}

struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(capacity: u64) -> Self {
        Self {
            tokens: capacity as f64,
            last_refill: Instant::now(),
        }
    }

    fn refill(&mut self, rate: f64) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens += elapsed * rate;
        self.last_refill = now;
    }
}

impl CallBudget {
    pub fn new(rate_per_second: f64, burst_capacity: u64) -> Self {
        Self {
            inner: RwLock::new(BudgetInner {
                buckets: HashMap::new(),
                rate: rate_per_second,
                capacity: burst_capacity,
            }),
        }
    }

    /// Try to consume one token for the given tenant.
    /// Returns `Ok(())` if allowed, `Err(wait_duration)` if rate-limited.
    pub fn try_acquire(&self, tenant: &str) -> Result<(), Duration> {
        let mut inner = self.inner.write().expect("rate limiter lock poisoned");
        let capacity = inner.capacity;
        let rate = inner.rate;
        let bucket = inner
            .buckets
            .entry(tenant.to_owned())
            .or_insert_with(|| TokenBucket::new(capacity));

        bucket.refill(rate);
        if bucket.tokens > capacity as f64 {
            bucket.tokens = capacity as f64;
        }

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Ok(())
        } else {
            let deficit = 1.0 - bucket.tokens;
            let wait = Duration::from_secs_f64(deficit / rate);
            Err(wait)
        }
    }

    /// Update rate and burst capacity at runtime.
    pub fn reconfigure(&self, rate_per_second: f64, burst_capacity: u64) {
        let mut inner = self.inner.write().expect("rate limiter lock poisoned");
        inner.rate = rate_per_second;
        inner.capacity = burst_capacity;
        inner.buckets.clear();
    }

    /// Current rate (calls per second).
    pub fn rate(&self) -> f64 {
        self.inner.read().expect("rate limiter lock poisoned").rate
    }

    /// Current burst capacity.
    pub fn capacity(&self) -> u64 {
        self.inner
            .read()
            .expect("rate limiter lock poisoned")
            .capacity
    }

    /// Reset all buckets (for testing).
    pub fn reset(&self) {
        let mut inner = self.inner.write().expect("rate limiter lock poisoned");
        inner.buckets.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_burst_up_to_capacity() {
        let budget = CallBudget::new(10.0, 5);
        for _ in 0..5 {
            assert!(budget.try_acquire("t1").is_ok());
        }
        assert!(budget.try_acquire("t1").is_err());
    }

    #[test]
    fn per_tenant_isolation() {
        let budget = CallBudget::new(10.0, 1);
        assert!(budget.try_acquire("alice").is_ok());
        assert!(budget.try_acquire("alice").is_err());
        assert!(budget.try_acquire("bob").is_ok());
        assert!(budget.try_acquire("bob").is_err());
    }
}
