//! Resource Limits and Rate Limiting
//!
//! Defines tenant resource limits and rate limiting implementation.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

/// Resource limits for tenants
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourceLimits {
    /// Maximum memory in bytes
    pub memory_bytes: u64,
    /// Maximum number of keys
    pub max_keys: u64,
    /// Maximum operations per second
    pub ops_per_second: u64,
    /// Maximum concurrent connections
    pub max_connections: u64,
    /// Maximum databases
    pub max_databases: u32,
    /// Maximum key size in bytes
    pub max_key_size: usize,
    /// Maximum value size in bytes
    pub max_value_size: usize,
    /// Maximum command arguments
    pub max_command_args: usize,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            memory_bytes: 256 * 1024 * 1024, // 256MB
            max_keys: 1_000_000,
            ops_per_second: 10_000,
            max_connections: 100,
            max_databases: 16,
            max_key_size: 512,
            max_value_size: 512 * 1024 * 1024, // 512MB
            max_command_args: 1_000_000,
        }
    }
}

impl ResourceLimits {
    /// Create unlimited limits (for admin/internal use)
    pub fn unlimited() -> Self {
        Self {
            memory_bytes: u64::MAX,
            max_keys: u64::MAX,
            ops_per_second: u64::MAX,
            max_connections: u64::MAX,
            max_databases: 256,
            max_key_size: usize::MAX,
            max_value_size: usize::MAX,
            max_command_args: usize::MAX,
        }
    }

    /// Create limits for free tier
    pub fn free_tier() -> Self {
        Self {
            memory_bytes: 64 * 1024 * 1024, // 64MB
            max_keys: 10_000,
            ops_per_second: 100,
            max_connections: 10,
            max_databases: 1,
            max_key_size: 256,
            max_value_size: 1024 * 1024, // 1MB
            max_command_args: 10_000,
        }
    }

    /// Create limits for basic tier
    pub fn basic_tier() -> Self {
        Self {
            memory_bytes: 512 * 1024 * 1024, // 512MB
            max_keys: 100_000,
            ops_per_second: 1_000,
            max_connections: 50,
            max_databases: 8,
            max_key_size: 512,
            max_value_size: 16 * 1024 * 1024, // 16MB
            max_command_args: 100_000,
        }
    }

    /// Create limits for pro tier
    pub fn pro_tier() -> Self {
        Self {
            memory_bytes: 4 * 1024 * 1024 * 1024, // 4GB
            max_keys: 10_000_000,
            ops_per_second: 50_000,
            max_connections: 500,
            max_databases: 32,
            max_key_size: 1024,
            max_value_size: 128 * 1024 * 1024, // 128MB
            max_command_args: 1_000_000,
        }
    }

    /// Create limits for enterprise tier
    pub fn enterprise_tier() -> Self {
        Self {
            memory_bytes: 64 * 1024 * 1024 * 1024, // 64GB
            max_keys: 1_000_000_000,
            ops_per_second: 1_000_000,
            max_connections: 10_000,
            max_databases: 256,
            max_key_size: 4096,
            max_value_size: 512 * 1024 * 1024, // 512MB
            max_command_args: 10_000_000,
        }
    }
}

/// Tenant-specific limits (can override defaults)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TenantLimits {
    /// Maximum memory in bytes
    pub memory_bytes: u64,
    /// Maximum number of keys
    pub max_keys: u64,
    /// Maximum operations per second
    pub ops_per_second: u64,
    /// Maximum concurrent connections
    pub max_connections: u64,
    /// Maximum databases
    pub max_databases: u32,
    /// Maximum key size in bytes
    pub max_key_size: usize,
    /// Maximum value size in bytes
    pub max_value_size: usize,
}

impl Default for TenantLimits {
    fn default() -> Self {
        Self {
            memory_bytes: 256 * 1024 * 1024, // 256MB
            max_keys: 1_000_000,
            ops_per_second: 10_000,
            max_connections: 100,
            max_databases: 16,
            max_key_size: 512,
            max_value_size: 512 * 1024 * 1024, // 512MB
        }
    }
}

impl TenantLimits {
    /// Create from resource limits
    pub fn from_resource_limits(limits: &ResourceLimits) -> Self {
        Self {
            memory_bytes: limits.memory_bytes,
            max_keys: limits.max_keys,
            ops_per_second: limits.ops_per_second,
            max_connections: limits.max_connections,
            max_databases: limits.max_databases,
            max_key_size: limits.max_key_size,
            max_value_size: limits.max_value_size,
        }
    }
}

/// Token bucket rate limiter
pub struct RateLimiter {
    /// Maximum tokens (ops per second)
    capacity: u64,
    /// Current tokens
    tokens: AtomicU64,
    /// Last refill time
    last_refill: Mutex<Instant>,
    /// Refill rate (tokens per millisecond)
    refill_rate: f64,
}

impl std::fmt::Debug for RateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimiter")
            .field("capacity", &self.capacity)
            .field("tokens", &self.tokens.load(Ordering::Relaxed))
            .field("refill_rate", &self.refill_rate)
            .finish()
    }
}

impl RateLimiter {
    /// Create a new rate limiter
    pub fn new(ops_per_second: u64) -> Self {
        Self {
            capacity: ops_per_second,
            tokens: AtomicU64::new(ops_per_second),
            last_refill: Mutex::new(Instant::now()),
            refill_rate: ops_per_second as f64 / 1000.0,
        }
    }

    /// Check if an operation is allowed (and consume a token if so)
    pub fn check(&self) -> bool {
        self.refill();
        self.try_acquire(1)
    }

    /// Check if multiple operations are allowed
    pub fn check_n(&self, n: u64) -> bool {
        self.refill();
        self.try_acquire(n)
    }

    /// Get current available tokens
    pub fn available(&self) -> u64 {
        self.refill();
        self.tokens.load(Ordering::Relaxed)
    }

    /// Refill tokens based on elapsed time
    fn refill(&self) {
        let mut last_refill = self.last_refill.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let elapsed = now.duration_since(*last_refill);
        let elapsed_ms = elapsed.as_millis() as f64;

        if elapsed_ms < 1.0 {
            return;
        }

        let new_tokens = (elapsed_ms * self.refill_rate) as u64;
        if new_tokens > 0 {
            let current = self.tokens.load(Ordering::Relaxed);
            let new_value = (current + new_tokens).min(self.capacity);
            self.tokens.store(new_value, Ordering::Relaxed);
            *last_refill = now;
        }
    }

    /// Try to acquire tokens
    fn try_acquire(&self, n: u64) -> bool {
        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            if current < n {
                return false;
            }

            let new_value = current - n;
            match self.tokens.compare_exchange_weak(
                current,
                new_value,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(_) => continue,
            }
        }
    }

    /// Get the capacity (ops per second)
    pub fn capacity(&self) -> u64 {
        self.capacity
    }
}

/// Sliding window rate limiter (for more accurate rate limiting)
pub struct SlidingWindowLimiter {
    /// Window size in milliseconds
    window_ms: u64,
    /// Maximum requests per window
    max_requests: u64,
    /// Request timestamps (circular buffer)
    timestamps: Mutex<Vec<u64>>,
    /// Current index
    current_index: AtomicU64,
}

impl SlidingWindowLimiter {
    /// Create a new sliding window limiter
    pub fn new(max_requests_per_second: u64) -> Self {
        Self {
            window_ms: 1000,
            max_requests: max_requests_per_second,
            timestamps: Mutex::new(vec![0; max_requests_per_second as usize]),
            current_index: AtomicU64::new(0),
        }
    }

    /// Check if a request is allowed
    pub fn check(&self) -> bool {
        let now = current_timestamp_ms();
        let window_start = now.saturating_sub(self.window_ms);

        let mut timestamps = self.timestamps.lock().unwrap_or_else(|e| e.into_inner());

        // Count requests in window
        let count = timestamps.iter().filter(|&&ts| ts >= window_start).count() as u64;

        if count >= self.max_requests {
            return false;
        }

        // Record this request
        let idx = self.current_index.fetch_add(1, Ordering::Relaxed) as usize % timestamps.len();
        timestamps[idx] = now;

        true
    }
}

/// Get current timestamp in milliseconds
fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_resource_limits_default() {
        let limits = ResourceLimits::default();
        assert_eq!(limits.memory_bytes, 256 * 1024 * 1024);
        assert_eq!(limits.ops_per_second, 10_000);
    }

    #[test]
    fn test_resource_limits_tiers() {
        let free = ResourceLimits::free_tier();
        let basic = ResourceLimits::basic_tier();
        let pro = ResourceLimits::pro_tier();
        let enterprise = ResourceLimits::enterprise_tier();

        assert!(free.memory_bytes < basic.memory_bytes);
        assert!(basic.memory_bytes < pro.memory_bytes);
        assert!(pro.memory_bytes < enterprise.memory_bytes);
    }

    #[test]
    fn test_rate_limiter_basic() {
        let limiter = RateLimiter::new(10);

        // Should allow up to 10 requests
        for _ in 0..10 {
            assert!(limiter.check());
        }

        // 11th request should fail
        assert!(!limiter.check());
    }

    #[test]
    fn test_rate_limiter_refill() {
        let limiter = RateLimiter::new(1000);

        // Exhaust some tokens
        for _ in 0..500 {
            limiter.check();
        }

        // Wait a bit for refill
        std::thread::sleep(Duration::from_millis(100));

        // Should have some tokens available
        assert!(limiter.available() > 0);
    }

    #[test]
    fn test_tenant_limits_from_resource() {
        let resource = ResourceLimits::pro_tier();
        let tenant = TenantLimits::from_resource_limits(&resource);

        assert_eq!(tenant.memory_bytes, resource.memory_bytes);
        assert_eq!(tenant.ops_per_second, resource.ops_per_second);
    }
}
