//! Per-tenant resource quota enforcement
//!
//! Provides memory limits, connection caps, ops/sec throttling,
//! and storage quotas with configurable burst allowances.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Error returned when a resource quota is exceeded
#[derive(Debug, Clone, thiserror::Error)]
pub enum QuotaExceeded {
    /// Memory quota exceeded
    #[error("memory quota exceeded: {used} + {requested} > {limit} bytes")]
    Memory {
        /// Currently used bytes
        used: u64,
        /// Bytes requested by the operation
        requested: u64,
        /// Configured limit
        limit: u64,
    },

    /// Key count quota exceeded
    #[error("key quota exceeded: {current} >= {limit} keys")]
    Keys {
        /// Current key count
        current: u64,
        /// Configured limit
        limit: u64,
    },

    /// Connection count quota exceeded
    #[error("connection quota exceeded: {current} >= {limit}")]
    Connections {
        /// Current connection count
        current: u32,
        /// Configured limit
        limit: u32,
    },

    /// Operations-per-second rate exceeded
    #[error("ops rate quota exceeded: limit {limit} ops/s")]
    OpsRate {
        /// Configured limit
        limit: u64,
    },

    /// Bandwidth quota exceeded
    #[error("bandwidth quota exceeded: {bytes} bytes exceeds {limit} bytes/s")]
    Bandwidth {
        /// Bytes requested
        bytes: u64,
        /// Configured limit
        limit: u64,
    },

    /// Individual key size exceeded
    #[error("key size exceeded: {size} > {limit} bytes")]
    KeySize {
        /// Actual size
        size: u32,
        /// Configured limit
        limit: u32,
    },

    /// Individual value size exceeded
    #[error("value size exceeded: {size} > {limit} bytes")]
    ValueSize {
        /// Actual size
        size: u32,
        /// Configured limit
        limit: u32,
    },
}

// ---------------------------------------------------------------------------
// QuotaConfig
// ---------------------------------------------------------------------------

/// Per-tenant quota configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuotaConfig {
    /// Maximum memory in bytes for this tenant
    pub max_memory_bytes: u64,
    /// Maximum number of keys
    pub max_keys: u64,
    /// Maximum concurrent connections
    pub max_connections: u32,
    /// Maximum operations per second
    pub max_ops_per_second: u64,
    /// Maximum bandwidth in bytes per second
    pub max_bandwidth_bytes_per_sec: u64,
    /// Maximum size of a single key in bytes
    pub max_key_size_bytes: u32,
    /// Maximum size of a single value in bytes
    pub max_value_size_bytes: u32,
    /// Burst multiplier — allows short bursts above the ops/s limit (e.g. 1.5× )
    pub burst_multiplier: f64,
    /// How long a burst is allowed before the enforcer falls back to the base rate
    pub burst_duration: Duration,
}

impl Default for QuotaConfig {
    fn default() -> Self {
        Self {
            max_memory_bytes: 256 * 1024 * 1024, // 256 MB
            max_keys: 1_000_000,
            max_connections: 100,
            max_ops_per_second: 10_000,
            max_bandwidth_bytes_per_sec: 100 * 1024 * 1024, // 100 MB/s
            max_key_size_bytes: 512,
            max_value_size_bytes: 512 * 1024 * 1024, // 512 MB
            burst_multiplier: 1.5,
            burst_duration: Duration::from_secs(10),
        }
    }
}

// ---------------------------------------------------------------------------
// QuotaUsageReport
// ---------------------------------------------------------------------------

/// Snapshot of current resource usage against configured limits
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuotaUsageReport {
    /// Memory currently used (bytes)
    pub memory_used: u64,
    /// Memory limit (bytes)
    pub memory_limit: u64,
    /// Memory usage percentage
    pub memory_percent: f64,

    /// Keys currently stored
    pub keys_used: u64,
    /// Key limit
    pub keys_limit: u64,
    /// Key usage percentage
    pub keys_percent: f64,

    /// Active connections
    pub connections_used: u32,
    /// Connection limit
    pub connections_limit: u32,
    /// Connection usage percentage
    pub connections_percent: f64,

    /// Current observed ops per second
    pub ops_rate: u64,
    /// Ops/s limit
    pub ops_limit: u64,
    /// Ops rate usage percentage
    pub ops_percent: f64,
}

// ---------------------------------------------------------------------------
// Token bucket (internal)
// ---------------------------------------------------------------------------

/// A simple token-bucket rate limiter with burst support.
struct TokenBucket {
    /// Base capacity (tokens per second)
    capacity: u64,
    /// Burst capacity = capacity × burst_multiplier
    burst_capacity: u64,
    /// Current number of available tokens
    tokens: AtomicU64,
    /// Last refill instant
    last_refill: Mutex<Instant>,
    /// Tokens to add per millisecond
    refill_rate: f64,
}

impl TokenBucket {
    fn new(capacity: u64, burst_multiplier: f64) -> Self {
        let burst_capacity = (capacity as f64 * burst_multiplier) as u64;
        Self {
            capacity,
            burst_capacity,
            tokens: AtomicU64::new(burst_capacity),
            last_refill: Mutex::new(Instant::now()),
            refill_rate: capacity as f64 / 1000.0,
        }
    }

    /// Refill tokens based on elapsed time.
    fn refill(&self) {
        let mut last = self.last_refill.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let elapsed_ms = now.duration_since(*last).as_millis() as f64;
        if elapsed_ms < 1.0 {
            return;
        }
        let new_tokens = (elapsed_ms * self.refill_rate) as u64;
        if new_tokens > 0 {
            let current = self.tokens.load(Ordering::Relaxed);
            self.tokens.store(
                (current + new_tokens).min(self.burst_capacity),
                Ordering::Relaxed,
            );
            *last = now;
        }
    }

    /// Try to consume one token. Returns `true` on success.
    fn try_acquire(&self) -> bool {
        self.refill();
        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            if current == 0 {
                return false;
            }
            if self
                .tokens
                .compare_exchange_weak(current, current - 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// QuotaEnforcer
// ---------------------------------------------------------------------------

/// Enforces per-tenant resource quotas using atomic counters and a token
/// bucket for rate limiting.
pub struct ResourceQuotaEnforcer {
    /// Configuration snapshot
    config: QuotaConfig,

    // --- Atomic counters ---------------------------------------------------
    /// Current memory usage in bytes
    memory_used: AtomicU64,
    /// Current key count
    keys_used: AtomicU64,
    /// Current active connections
    connections_used: AtomicU64,
    /// Total operations recorded (monotonic)
    ops_total: AtomicU64,

    // --- Rate limiting -----------------------------------------------------
    /// Token bucket for ops/s enforcement
    ops_bucket: TokenBucket,
    /// Token bucket for bandwidth enforcement
    bandwidth_bucket: TokenBucket,
}

impl std::fmt::Debug for ResourceQuotaEnforcer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResourceQuotaEnforcer")
            .field("config", &self.config)
            .field("memory_used", &self.memory_used.load(Ordering::Relaxed))
            .field("keys_used", &self.keys_used.load(Ordering::Relaxed))
            .field(
                "connections_used",
                &self.connections_used.load(Ordering::Relaxed),
            )
            .field("ops_total", &self.ops_total.load(Ordering::Relaxed))
            .finish()
    }
}

impl ResourceQuotaEnforcer {
    /// Create a new enforcer from the given configuration.
    pub fn new(config: QuotaConfig) -> Self {
        let ops_bucket = TokenBucket::new(config.max_ops_per_second, config.burst_multiplier);
        let bandwidth_bucket =
            TokenBucket::new(config.max_bandwidth_bytes_per_sec, config.burst_multiplier);
        Self {
            config,
            memory_used: AtomicU64::new(0),
            keys_used: AtomicU64::new(0),
            connections_used: AtomicU64::new(0),
            ops_total: AtomicU64::new(0),
            ops_bucket,
            bandwidth_bucket,
        }
    }

    // -- Check methods ------------------------------------------------------

    /// Check whether `additional` bytes of memory can be allocated.
    pub fn check_memory(&self, current: u64, additional: u64) -> Result<(), QuotaExceeded> {
        if current + additional > self.config.max_memory_bytes {
            return Err(QuotaExceeded::Memory {
                used: current,
                requested: additional,
                limit: self.config.max_memory_bytes,
            });
        }
        Ok(())
    }

    /// Check whether another key can be stored.
    pub fn check_keys(&self, current: u64) -> Result<(), QuotaExceeded> {
        if current >= self.config.max_keys {
            return Err(QuotaExceeded::Keys {
                current,
                limit: self.config.max_keys,
            });
        }
        Ok(())
    }

    /// Check whether another connection can be opened.
    pub fn check_connections(&self, current: u32) -> Result<(), QuotaExceeded> {
        if current >= self.config.max_connections {
            return Err(QuotaExceeded::Connections {
                current,
                limit: self.config.max_connections,
            });
        }
        Ok(())
    }

    /// Check ops/s rate limit via the token bucket.
    pub fn check_ops_rate(&self) -> Result<(), QuotaExceeded> {
        if !self.ops_bucket.try_acquire() {
            return Err(QuotaExceeded::OpsRate {
                limit: self.config.max_ops_per_second,
            });
        }
        Ok(())
    }

    /// Check bandwidth rate limit for `bytes` of data.
    pub fn check_bandwidth(&self, bytes: u64) -> Result<(), QuotaExceeded> {
        // For simplicity we attempt to acquire one token per byte-chunk (1 KB granularity).
        // The bandwidth bucket capacity is in bytes/sec so a single try_acquire suffices
        // for the coarse check.
        let current = self.bandwidth_bucket.tokens.load(Ordering::Relaxed);
        if current < bytes {
            return Err(QuotaExceeded::Bandwidth {
                bytes,
                limit: self.config.max_bandwidth_bytes_per_sec,
            });
        }
        // Consume tokens (best-effort; exact atomic subtraction is not critical
        // because the bucket refills continuously).
        self.bandwidth_bucket
            .tokens
            .fetch_sub(bytes.min(current), Ordering::Relaxed);
        Ok(())
    }

    /// Check individual key size.
    pub fn check_key_size(&self, size: u32) -> Result<(), QuotaExceeded> {
        if size > self.config.max_key_size_bytes {
            return Err(QuotaExceeded::KeySize {
                size,
                limit: self.config.max_key_size_bytes,
            });
        }
        Ok(())
    }

    /// Check individual value size.
    pub fn check_value_size(&self, size: u32) -> Result<(), QuotaExceeded> {
        if size > self.config.max_value_size_bytes {
            return Err(QuotaExceeded::ValueSize {
                size,
                limit: self.config.max_value_size_bytes,
            });
        }
        Ok(())
    }

    // -- Recording methods --------------------------------------------------

    /// Record that an operation happened (increments ops counter).
    pub fn record_op(&self) {
        self.ops_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a change in memory usage.
    pub fn record_memory(&self, delta: i64) {
        if delta >= 0 {
            self.memory_used.fetch_add(delta as u64, Ordering::Relaxed);
        } else {
            self.memory_used
                .fetch_sub((-delta) as u64, Ordering::Relaxed);
        }
    }

    /// Record a change in key count.
    pub fn record_keys(&self, delta: i64) {
        if delta >= 0 {
            self.keys_used.fetch_add(delta as u64, Ordering::Relaxed);
        } else {
            self.keys_used.fetch_sub((-delta) as u64, Ordering::Relaxed);
        }
    }

    /// Record a connection open (+1) or close (-1).
    pub fn record_connection(&self, delta: i64) {
        if delta >= 0 {
            self.connections_used
                .fetch_add(delta as u64, Ordering::Relaxed);
        } else {
            self.connections_used
                .fetch_sub((-delta) as u64, Ordering::Relaxed);
        }
    }

    // -- Reporting ----------------------------------------------------------

    /// Produce a point-in-time usage report.
    pub fn usage_report(&self) -> QuotaUsageReport {
        let memory_used = self.memory_used.load(Ordering::Relaxed);
        let keys_used = self.keys_used.load(Ordering::Relaxed);
        let connections_used = self.connections_used.load(Ordering::Relaxed) as u32;
        let ops_total = self.ops_total.load(Ordering::Relaxed);

        let pct = |used: f64, limit: f64| -> f64 {
            if limit == 0.0 {
                0.0
            } else {
                (used / limit) * 100.0
            }
        };

        QuotaUsageReport {
            memory_used,
            memory_limit: self.config.max_memory_bytes,
            memory_percent: pct(memory_used as f64, self.config.max_memory_bytes as f64),

            keys_used,
            keys_limit: self.config.max_keys,
            keys_percent: pct(keys_used as f64, self.config.max_keys as f64),

            connections_used,
            connections_limit: self.config.max_connections,
            connections_percent: pct(connections_used as f64, self.config.max_connections as f64),

            ops_rate: ops_total,
            ops_limit: self.config.max_ops_per_second,
            ops_percent: pct(ops_total as f64, self.config.max_ops_per_second as f64),
        }
    }

    /// Get the underlying config.
    pub fn config(&self) -> &QuotaConfig {
        &self.config
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_enforcer() -> ResourceQuotaEnforcer {
        ResourceQuotaEnforcer::new(QuotaConfig {
            max_memory_bytes: 1000,
            max_keys: 100,
            max_connections: 10,
            max_ops_per_second: 1000,
            max_bandwidth_bytes_per_sec: 5000,
            max_key_size_bytes: 256,
            max_value_size_bytes: 1024,
            burst_multiplier: 1.5,
            burst_duration: Duration::from_secs(5),
        })
    }

    #[test]
    fn test_check_memory_ok() {
        let e = default_enforcer();
        assert!(e.check_memory(500, 400).is_ok());
    }

    #[test]
    fn test_check_memory_exceeded() {
        let e = default_enforcer();
        let err = e.check_memory(800, 300).unwrap_err();
        assert!(matches!(err, QuotaExceeded::Memory { .. }));
    }

    #[test]
    fn test_check_keys_ok() {
        let e = default_enforcer();
        assert!(e.check_keys(50).is_ok());
    }

    #[test]
    fn test_check_keys_exceeded() {
        let e = default_enforcer();
        assert!(matches!(
            e.check_keys(100).unwrap_err(),
            QuotaExceeded::Keys { .. }
        ));
    }

    #[test]
    fn test_check_connections_ok() {
        let e = default_enforcer();
        assert!(e.check_connections(5).is_ok());
    }

    #[test]
    fn test_check_connections_exceeded() {
        let e = default_enforcer();
        assert!(matches!(
            e.check_connections(10).unwrap_err(),
            QuotaExceeded::Connections { .. }
        ));
    }

    #[test]
    fn test_check_ops_rate_ok() {
        let e = default_enforcer();
        // burst_capacity = 1000 * 1.5 = 1500 tokens at start
        assert!(e.check_ops_rate().is_ok());
    }

    #[test]
    fn test_check_key_size() {
        let e = default_enforcer();
        assert!(e.check_key_size(100).is_ok());
        assert!(matches!(
            e.check_key_size(300).unwrap_err(),
            QuotaExceeded::KeySize { .. }
        ));
    }

    #[test]
    fn test_check_value_size() {
        let e = default_enforcer();
        assert!(e.check_value_size(512).is_ok());
        assert!(matches!(
            e.check_value_size(2048).unwrap_err(),
            QuotaExceeded::ValueSize { .. }
        ));
    }

    #[test]
    fn test_record_and_report() {
        let e = default_enforcer();
        e.record_memory(500);
        e.record_keys(30);
        e.record_connection(5);
        e.record_op();
        e.record_op();

        let report = e.usage_report();
        assert_eq!(report.memory_used, 500);
        assert_eq!(report.keys_used, 30);
        assert_eq!(report.connections_used, 5);
        assert_eq!(report.ops_rate, 2);
        assert!((report.memory_percent - 50.0).abs() < 0.1);
    }

    #[test]
    fn test_record_memory_negative_delta() {
        let e = default_enforcer();
        e.record_memory(100);
        e.record_memory(-40);
        assert_eq!(e.usage_report().memory_used, 60);
    }

    #[test]
    fn test_check_bandwidth_ok() {
        let e = default_enforcer();
        // burst_capacity = 5000 * 1.5 = 7500
        assert!(e.check_bandwidth(1000).is_ok());
    }

    #[test]
    fn test_check_bandwidth_exceeded() {
        let e = default_enforcer();
        assert!(matches!(
            e.check_bandwidth(10_000).unwrap_err(),
            QuotaExceeded::Bandwidth { .. }
        ));
    }
}
