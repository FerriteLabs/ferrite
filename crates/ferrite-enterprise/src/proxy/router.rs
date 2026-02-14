//! Command routing engine for the Redis proxy.
//!
//! Provides routing policies and key-level migration tracking for
//! deterministic traffic splitting between Redis and Ferrite backends.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Routing policy controlling how commands are distributed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutingPolicy {
    /// Route based on key hash for deterministic placement.
    HashBased,
    /// Route based on key prefix mapping.
    PrefixBased,
    /// Route based on whether a key has been migrated.
    MigrationAware,
}

impl Default for RoutingPolicy {
    fn default() -> Self {
        Self::HashBased
    }
}

/// Tracks which keys have been successfully migrated to Ferrite.
pub struct MigrationTracker {
    migrated_prefixes: RwLock<HashSet<String>>,
    migrated_count: AtomicU64,
    pending_count: AtomicU64,
}

impl MigrationTracker {
    pub fn new() -> Self {
        Self {
            migrated_prefixes: RwLock::new(HashSet::new()),
            migrated_count: AtomicU64::new(0),
            pending_count: AtomicU64::new(0),
        }
    }

    /// Marks a key prefix as fully migrated to Ferrite.
    pub fn mark_migrated(&self, prefix: &str) {
        self.migrated_prefixes.write().insert(prefix.to_string());
        self.migrated_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Checks if a key belongs to a migrated prefix.
    pub fn is_migrated(&self, key: &str) -> bool {
        let prefixes = self.migrated_prefixes.read();
        prefixes.iter().any(|p| key.starts_with(p))
    }

    /// Returns the number of migrated prefixes.
    pub fn migrated_count(&self) -> u64 {
        self.migrated_count.load(Ordering::Relaxed)
    }

    /// Sets the pending migration count.
    pub fn set_pending_count(&self, count: u64) {
        self.pending_count.store(count, Ordering::Relaxed);
    }

    /// Returns the pending migration count.
    pub fn pending_count(&self) -> u64 {
        self.pending_count.load(Ordering::Relaxed)
    }

    /// Returns the migration progress as a percentage (0.0 - 100.0).
    pub fn progress_percent(&self) -> f64 {
        let migrated = self.migrated_count.load(Ordering::Relaxed) as f64;
        let pending = self.pending_count.load(Ordering::Relaxed) as f64;
        let total = migrated + pending;
        if total == 0.0 {
            return 100.0;
        }
        (migrated / total) * 100.0
    }

    /// Clears all migration tracking state.
    pub fn reset(&self) {
        self.migrated_prefixes.write().clear();
        self.migrated_count.store(0, Ordering::Relaxed);
        self.pending_count.store(0, Ordering::Relaxed);
    }
}

impl Default for MigrationTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Classifies Redis commands by their routing requirements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandClass {
    /// Read-only commands that can be served from either backend.
    Read,
    /// Write commands that must be routed carefully during migration.
    Write,
    /// Administrative commands that should always go to the proxy.
    Admin,
    /// Multi-key commands requiring special handling.
    MultiKey,
    /// Pub/Sub commands with session affinity requirements.
    PubSub,
    /// Transaction commands (MULTI/EXEC) requiring atomic routing.
    Transaction,
}

/// Classifies a command name into its routing class.
pub fn classify_command(command: &str) -> CommandClass {
    match command.to_uppercase().as_str() {
        // Read commands
        "GET" | "MGET" | "STRLEN" | "GETRANGE" | "EXISTS" | "TYPE" | "TTL" | "PTTL" | "HGET"
        | "HMGET" | "HGETALL" | "HLEN" | "HEXISTS" | "HKEYS" | "HVALS" | "LRANGE" | "LLEN"
        | "LINDEX" | "SMEMBERS" | "SISMEMBER" | "SCARD" | "ZRANGE" | "ZSCORE" | "ZCARD"
        | "ZRANK" | "ZCOUNT" | "SCAN" | "HSCAN" | "SSCAN" | "ZSCAN" | "DBSIZE" | "RANDOMKEY" => {
            CommandClass::Read
        }

        // Write commands
        "SET" | "MSET" | "SETNX" | "SETEX" | "PSETEX" | "APPEND" | "INCR" | "DECR" | "INCRBY"
        | "DECRBY" | "INCRBYFLOAT" | "SETRANGE" | "HSET" | "HMSET" | "HSETNX" | "HDEL"
        | "HINCRBY" | "HINCRBYFLOAT" | "LPUSH" | "RPUSH" | "LPOP" | "RPOP" | "LSET" | "LREM"
        | "LINSERT" | "LTRIM" | "SADD" | "SREM" | "SPOP" | "SMOVE" | "ZADD" | "ZREM"
        | "ZINCRBY" | "ZREMRANGEBYSCORE" | "ZREMRANGEBYRANK" | "DEL" | "UNLINK" | "EXPIRE"
        | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" | "PERSIST" | "RENAME" | "RENAMENX" => {
            CommandClass::Write
        }

        // Admin commands
        "PING" | "ECHO" | "INFO" | "CONFIG" | "SELECT" | "AUTH" | "QUIT" | "COMMAND" | "CLIENT"
        | "SLOWLOG" | "DEBUG" | "MEMORY" | "ACL" | "CLUSTER" => CommandClass::Admin,

        // Multi-key commands
        "SUNION" | "SINTER" | "SDIFF" | "SUNIONSTORE" | "SINTERSTORE" | "SDIFFSTORE"
        | "RPOPLPUSH" | "LMOVE" | "SMOVE" | "SORT" | "OBJECT" => CommandClass::MultiKey,

        // Pub/Sub
        "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE" | "PUBLISH" => {
            CommandClass::PubSub
        }

        // Transactions
        "MULTI" | "EXEC" | "DISCARD" | "WATCH" | "UNWATCH" => CommandClass::Transaction,

        _ => CommandClass::Write, // Default to write for safety
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_tracker() {
        let tracker = MigrationTracker::new();

        assert!(!tracker.is_migrated("users:123"));

        tracker.mark_migrated("users:");
        assert!(tracker.is_migrated("users:123"));
        assert!(tracker.is_migrated("users:456"));
        assert!(!tracker.is_migrated("orders:789"));
    }

    #[test]
    fn test_migration_progress() {
        let tracker = MigrationTracker::new();
        tracker.set_pending_count(100);

        assert!((tracker.progress_percent() - 0.0).abs() < f64::EPSILON);

        tracker.mark_migrated("prefix1:");
        tracker.set_pending_count(99);
        // 1 migrated / (1 + 99) = 1%
        assert!((tracker.progress_percent() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_migration_reset() {
        let tracker = MigrationTracker::new();
        tracker.mark_migrated("prefix:");
        tracker.set_pending_count(50);

        tracker.reset();
        assert_eq!(tracker.migrated_count(), 0);
        assert_eq!(tracker.pending_count(), 0);
        assert!(!tracker.is_migrated("prefix:key"));
    }

    #[test]
    fn test_classify_read_commands() {
        assert_eq!(classify_command("GET"), CommandClass::Read);
        assert_eq!(classify_command("MGET"), CommandClass::Read);
        assert_eq!(classify_command("HGETALL"), CommandClass::Read);
        assert_eq!(classify_command("SMEMBERS"), CommandClass::Read);
        assert_eq!(classify_command("ZRANGE"), CommandClass::Read);
        // Case insensitive
        assert_eq!(classify_command("get"), CommandClass::Read);
    }

    #[test]
    fn test_classify_write_commands() {
        assert_eq!(classify_command("SET"), CommandClass::Write);
        assert_eq!(classify_command("DEL"), CommandClass::Write);
        assert_eq!(classify_command("LPUSH"), CommandClass::Write);
        assert_eq!(classify_command("ZADD"), CommandClass::Write);
    }

    #[test]
    fn test_classify_admin_commands() {
        assert_eq!(classify_command("PING"), CommandClass::Admin);
        assert_eq!(classify_command("INFO"), CommandClass::Admin);
        assert_eq!(classify_command("CONFIG"), CommandClass::Admin);
    }

    #[test]
    fn test_classify_pubsub_commands() {
        assert_eq!(classify_command("SUBSCRIBE"), CommandClass::PubSub);
        assert_eq!(classify_command("PUBLISH"), CommandClass::PubSub);
    }

    #[test]
    fn test_classify_transaction_commands() {
        assert_eq!(classify_command("MULTI"), CommandClass::Transaction);
        assert_eq!(classify_command("EXEC"), CommandClass::Transaction);
    }

    #[test]
    fn test_classify_unknown_defaults_to_write() {
        assert_eq!(classify_command("UNKNOWN_CMD"), CommandClass::Write);
    }
}
