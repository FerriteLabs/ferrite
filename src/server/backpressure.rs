//! Graceful degradation and backpressure for the Ferrite server.
//!
//! Provides memory-aware backpressure that rejects write commands when
//! memory usage exceeds configured thresholds, and connection limiting
//! that refuses new connections above `max_connections`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Shared backpressure controller.
pub type SharedBackpressure = Arc<BackpressureController>;

/// Memory backpressure controller.
///
/// Monitors memory usage and rejects write operations when the server
/// approaches its memory limit, preventing OOM kills and data loss.
pub struct BackpressureController {
    /// Maximum memory in bytes before rejecting writes (0 = unlimited).
    max_memory: u64,
    /// Current estimated memory usage in bytes.
    current_memory: AtomicU64,
    /// Threshold ratio (0.0-1.0) at which to start rejecting writes.
    reject_threshold: f64,
    /// Total writes rejected due to backpressure.
    rejected_writes: AtomicU64,
}

impl BackpressureController {
    /// Create a new backpressure controller.
    ///
    /// - `max_memory`: memory limit in bytes (0 = unlimited)
    /// - `reject_threshold`: fraction of max_memory at which to reject writes (e.g., 0.9)
    pub fn new(max_memory: u64, reject_threshold: f64) -> Self {
        Self {
            max_memory,
            current_memory: AtomicU64::new(0),
            reject_threshold: reject_threshold.clamp(0.0, 1.0),
            rejected_writes: AtomicU64::new(0),
        }
    }

    /// Update the current memory estimate.
    pub fn update_memory(&self, bytes: u64) {
        self.current_memory.store(bytes, Ordering::Relaxed);
    }

    /// Check if writes should be rejected due to memory pressure.
    ///
    /// Returns `true` if the server is under memory pressure and writes
    /// should be rejected.
    pub fn should_reject_writes(&self) -> bool {
        if self.max_memory == 0 {
            return false; // unlimited
        }
        let current = self.current_memory.load(Ordering::Relaxed);
        let threshold = (self.max_memory as f64 * self.reject_threshold) as u64;
        if current >= threshold {
            self.rejected_writes.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        false
    }

    /// Returns the current memory usage ratio (0.0-1.0), or 0.0 if unlimited.
    pub fn memory_ratio(&self) -> f64 {
        if self.max_memory == 0 {
            return 0.0;
        }
        self.current_memory.load(Ordering::Relaxed) as f64 / self.max_memory as f64
    }

    /// Total writes rejected since creation.
    pub fn rejected_count(&self) -> u64 {
        self.rejected_writes.load(Ordering::Relaxed)
    }

    /// Returns `true` when backpressure is disabled (max_memory = 0).
    pub fn is_unlimited(&self) -> bool {
        self.max_memory == 0
    }

    /// Returns the configured maximum memory in bytes.
    pub fn max_memory_bytes(&self) -> u64 {
        self.max_memory
    }
}

/// Check if a connection should be accepted based on the current connection count.
///
/// Returns `true` if the connection should be accepted.
pub fn should_accept_connection(current_count: usize, max_connections: usize) -> bool {
    if max_connections == 0 {
        return true; // unlimited
    }
    current_count < max_connections
}

/// Returns `true` if the given command name is read-only (does not modify state).
///
/// Used to allow read commands through during memory backpressure while
/// rejecting writes.
pub fn is_read_only_command(cmd_name: &str) -> bool {
    matches!(
        cmd_name,
        "GET" | "MGET" | "EXISTS" | "STRLEN" | "GETRANGE"
            | "SUBSTR" | "TYPE" | "TTL" | "PTTL"
            | "KEYS" | "SCAN" | "RANDOMKEY" | "DBSIZE"
            | "OBJECT" | "DEBUG" | "WAIT"
            // List reads
            | "LRANGE" | "LLEN" | "LINDEX" | "LPOS"
            // Set reads
            | "SISMEMBER" | "SMISMEMBER" | "SMEMBERS"
            | "SCARD" | "SRANDMEMBER" | "SINTER"
            | "SUNION" | "SDIFF"
            // Hash reads
            | "HGET" | "HMGET" | "HGETALL" | "HLEN"
            | "HEXISTS" | "HKEYS" | "HVALS" | "HSCAN"
            | "HRANDFIELD"
            // Sorted set reads
            | "ZSCORE" | "ZMSCORE" | "ZRANK" | "ZREVRANK"
            | "ZRANGE" | "ZRANGEBYLEX" | "ZRANGEBYSCORE"
            | "ZREVRANGE" | "ZREVRANGEBYSCORE" | "ZREVRANGEBYLEX"
            | "ZCARD" | "ZCOUNT" | "ZLEXCOUNT"
            | "ZRANDMEMBER" | "ZSCAN"
            // HyperLogLog read
            | "PFCOUNT"
            // Stream reads
            | "XLEN" | "XRANGE" | "XREVRANGE" | "XINFO"
            | "XPENDING"
            // Geo reads
            | "GEOPOS" | "GEODIST" | "GEOSEARCH" | "GEOHASH"
            // Bitmap reads
            | "GETBIT" | "BITCOUNT" | "BITPOS" | "BITFIELD_RO"
            // Server/connection (no state mutation)
            | "PING" | "ECHO" | "INFO" | "TIME"
            | "COMMAND" | "CLIENT" | "CONFIG"
            | "SLOWLOG" | "LATENCY" | "MEMORY"
            | "AUTH" | "HELLO" | "RESET" | "QUIT"
            | "SELECT" | "SUBSCRIBE" | "UNSUBSCRIBE"
            | "PSUBSCRIBE" | "PUNSUBSCRIBE"
            | "ACL" | "CLUSTER"
            // Pub/Sub reads
            | "PUBSUB"
            // LCS (read-only string comparison)
            | "LCS"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unlimited_never_rejects() {
        let bp = BackpressureController::new(0, 0.9);
        bp.update_memory(u64::MAX);
        assert!(!bp.should_reject_writes());
        assert!(bp.is_unlimited());
    }

    #[test]
    fn rejects_above_threshold() {
        let bp = BackpressureController::new(1000, 0.9);
        bp.update_memory(899);
        assert!(!bp.should_reject_writes(), "below threshold");

        bp.update_memory(900);
        assert!(bp.should_reject_writes(), "at threshold");

        bp.update_memory(1000);
        assert!(bp.should_reject_writes(), "at max");
        assert_eq!(bp.rejected_count(), 2);
    }

    #[test]
    fn memory_ratio_correct() {
        let bp = BackpressureController::new(1000, 0.9);
        bp.update_memory(500);
        let ratio = bp.memory_ratio();
        assert!((ratio - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn connection_limit_accepts_below() {
        assert!(should_accept_connection(5, 10));
        assert!(should_accept_connection(9, 10));
    }

    #[test]
    fn connection_limit_rejects_at_max() {
        assert!(!should_accept_connection(10, 10));
        assert!(!should_accept_connection(11, 10));
    }

    #[test]
    fn connection_limit_unlimited() {
        assert!(should_accept_connection(999_999, 0));
    }

    #[test]
    fn read_commands_classified_correctly() {
        assert!(is_read_only_command("GET"));
        assert!(is_read_only_command("MGET"));
        assert!(is_read_only_command("HGETALL"));
        assert!(is_read_only_command("LRANGE"));
        assert!(is_read_only_command("ZSCORE"));
        assert!(is_read_only_command("PING"));
        assert!(is_read_only_command("INFO"));
        assert!(is_read_only_command("SCAN"));
        assert!(is_read_only_command("XLEN"));
    }

    #[test]
    fn write_commands_classified_correctly() {
        assert!(!is_read_only_command("SET"));
        assert!(!is_read_only_command("DEL"));
        assert!(!is_read_only_command("LPUSH"));
        assert!(!is_read_only_command("HSET"));
        assert!(!is_read_only_command("ZADD"));
        assert!(!is_read_only_command("INCR"));
        assert!(!is_read_only_command("FLUSHDB"));
        assert!(!is_read_only_command("XADD"));
        assert!(!is_read_only_command("SADD"));
    }
}
