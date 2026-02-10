//! Compatibility Tracker
//!
//! Tracks per-command and per-category Redis compatibility status with
//! test results, producing a structured matrix for certification.

use std::collections::HashMap;
use std::time::Instant;

use serde::{Deserialize, Serialize};

/// Redis command categories
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CommandCategory {
    /// String commands (GET, SET, etc.)
    String,
    /// List commands (LPUSH, RPUSH, etc.)
    List,
    /// Hash commands (HSET, HGET, etc.)
    Hash,
    /// Set commands (SADD, SREM, etc.)
    Set,
    /// Sorted set commands (ZADD, ZREM, etc.)
    SortedSet,
    /// Key management commands (DEL, EXISTS, etc.)
    Key,
    /// Server administration commands (PING, INFO, etc.)
    Server,
    /// Pub/Sub commands (SUBSCRIBE, PUBLISH, etc.)
    PubSub,
    /// Transaction commands (MULTI, EXEC, etc.)
    Transaction,
    /// Scripting commands (EVAL, EVALSHA, etc.)
    Scripting,
    /// HyperLogLog commands (PFADD, PFCOUNT, etc.)
    HyperLogLog,
    /// Geospatial commands (GEOADD, GEODIST, etc.)
    Geo,
    /// Stream commands (XADD, XREAD, etc.)
    Stream,
    /// Cluster management commands.
    Cluster,
    /// Connection management commands (AUTH, QUIT, etc.)
    Connection,
    /// Generic/uncategorized commands.
    Generic,
}

impl CommandCategory {
    /// All categories
    pub fn all() -> &'static [CommandCategory] {
        &[
            Self::String,
            Self::List,
            Self::Hash,
            Self::Set,
            Self::SortedSet,
            Self::Key,
            Self::Server,
            Self::PubSub,
            Self::Transaction,
            Self::Scripting,
            Self::HyperLogLog,
            Self::Geo,
            Self::Stream,
            Self::Cluster,
            Self::Connection,
            Self::Generic,
        ]
    }
}

impl std::fmt::Display for CommandCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Status of a single command's compatibility
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandStatus {
    /// Fully compatible with Redis behavior
    Full,
    /// Partially compatible (some options/flags missing)
    Partial,
    /// Not yet implemented
    NotImplemented,
    /// Implemented but known to differ from Redis
    Divergent,
    /// Tested and failed
    Failed,
}

impl std::fmt::Display for CommandStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => write!(f, "✅ Full"),
            Self::Partial => write!(f, "⚠️  Partial"),
            Self::NotImplemented => write!(f, "❌ Not Implemented"),
            Self::Divergent => write!(f, "🔀 Divergent"),
            Self::Failed => write!(f, "💥 Failed"),
        }
    }
}

/// Result of running a single compatibility test
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestResult {
    /// Test name
    pub name: String,
    /// Whether the test passed
    pub passed: bool,
    /// Failure message if not passed
    pub message: Option<String>,
    /// Test duration
    pub duration_us: u64,
}

/// Metadata for a Redis command being tracked
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandEntry {
    /// Command name (e.g., "GET", "SET")
    pub name: String,
    /// Category
    pub category: CommandCategory,
    /// Current compatibility status
    pub status: CommandStatus,
    /// Redis documentation URL
    pub redis_doc_url: String,
    /// Test results
    pub tests: Vec<TestResult>,
    /// Notes about differences or limitations
    pub notes: Vec<String>,
    /// Whether this is a critical command (top-20 by usage)
    pub critical: bool,
}

/// Tracks compatibility status for all Redis commands
pub struct CompatibilityTracker {
    commands: HashMap<String, CommandEntry>,
}

impl CompatibilityTracker {
    /// Create a new tracker pre-populated with the Redis command set
    pub fn new() -> Self {
        let mut tracker = Self {
            commands: HashMap::new(),
        };
        tracker.register_redis_commands();
        tracker
    }

    /// Register a command
    pub fn register(
        &mut self,
        name: &str,
        category: CommandCategory,
        status: CommandStatus,
        critical: bool,
    ) {
        let entry = CommandEntry {
            name: name.to_uppercase(),
            category,
            status,
            redis_doc_url: format!("https://redis.io/commands/{}", name.to_lowercase()),
            tests: Vec::new(),
            notes: Vec::new(),
            critical,
        };
        self.commands.insert(name.to_uppercase(), entry);
    }

    /// Record a test result for a command
    pub fn record_test(&mut self, command: &str, result: TestResult) {
        if let Some(entry) = self.commands.get_mut(&command.to_uppercase()) {
            if result.passed {
                if entry.status == CommandStatus::NotImplemented
                    || entry.status == CommandStatus::Failed
                {
                    entry.status = CommandStatus::Full;
                }
            } else {
                entry.status = CommandStatus::Failed;
            }
            entry.tests.push(result);
        }
    }

    /// Update command status
    pub fn set_status(&mut self, command: &str, status: CommandStatus) {
        if let Some(entry) = self.commands.get_mut(&command.to_uppercase()) {
            entry.status = status;
        }
    }

    /// Add a note to a command
    pub fn add_note(&mut self, command: &str, note: &str) {
        if let Some(entry) = self.commands.get_mut(&command.to_uppercase()) {
            entry.notes.push(note.to_string());
        }
    }

    /// Get a command entry
    pub fn get(&self, command: &str) -> Option<&CommandEntry> {
        self.commands.get(&command.to_uppercase())
    }

    /// Get all commands in a category
    pub fn commands_in_category(&self, category: CommandCategory) -> Vec<&CommandEntry> {
        self.commands
            .values()
            .filter(|e| e.category == category)
            .collect()
    }

    /// Calculate overall compatibility score (0.0 - 1.0)
    pub fn compatibility_score(&self) -> f64 {
        if self.commands.is_empty() {
            return 0.0;
        }

        let total = self.commands.len() as f64;
        let compatible: f64 = self
            .commands
            .values()
            .map(|e| match e.status {
                CommandStatus::Full => 1.0,
                CommandStatus::Partial => 0.5,
                _ => 0.0,
            })
            .sum();

        compatible / total
    }

    /// Calculate critical-command compatibility score
    pub fn critical_score(&self) -> f64 {
        let critical: Vec<&CommandEntry> = self.commands.values().filter(|e| e.critical).collect();
        if critical.is_empty() {
            return 0.0;
        }

        let total = critical.len() as f64;
        let compatible: f64 = critical
            .iter()
            .map(|e| match e.status {
                CommandStatus::Full => 1.0,
                CommandStatus::Partial => 0.5,
                _ => 0.0,
            })
            .sum();

        compatible / total
    }

    /// Get total command count
    pub fn total_commands(&self) -> usize {
        self.commands.len()
    }

    /// Get count by status
    pub fn count_by_status(&self, status: CommandStatus) -> usize {
        self.commands
            .values()
            .filter(|e| e.status == status)
            .count()
    }

    /// Get all command entries
    pub fn all_commands(&self) -> &HashMap<String, CommandEntry> {
        &self.commands
    }

    /// Run a test closure and record the result
    pub fn run_test<F>(&mut self, command: &str, test_name: &str, test_fn: F)
    where
        F: FnOnce() -> Result<(), String>,
    {
        let start = Instant::now();
        let result = test_fn();
        let duration = start.elapsed();

        let test_result = TestResult {
            name: test_name.to_string(),
            passed: result.is_ok(),
            message: result.err(),
            duration_us: duration.as_micros() as u64,
        };

        self.record_test(command, test_result);
    }

    fn register_redis_commands(&mut self) {
        // String commands
        for (cmd, critical) in [
            ("GET", true),
            ("SET", true),
            ("MGET", true),
            ("MSET", true),
            ("INCR", true),
            ("DECR", true),
            ("INCRBY", true),
            ("DECRBY", false),
            ("INCRBYFLOAT", false),
            ("APPEND", false),
            ("STRLEN", false),
            ("GETRANGE", false),
            ("SETRANGE", false),
            ("GETSET", false),
            ("SETNX", true),
            ("SETEX", true),
            ("PSETEX", false),
            ("MSETNX", false),
            ("GETDEL", false),
            ("GETEX", false),
        ] {
            self.register(cmd, CommandCategory::String, CommandStatus::Full, critical);
        }

        // Key commands
        for (cmd, critical) in [
            ("DEL", true),
            ("EXISTS", true),
            ("EXPIRE", true),
            ("EXPIREAT", false),
            ("PEXPIRE", false),
            ("PEXPIREAT", false),
            ("TTL", true),
            ("PTTL", true),
            ("PERSIST", false),
            ("TYPE", true),
            ("RENAME", false),
            ("RENAMENX", false),
            ("KEYS", true),
            ("SCAN", true),
            ("RANDOMKEY", false),
            ("UNLINK", false),
            ("OBJECT", false),
            ("DUMP", false),
            ("RESTORE", false),
            ("SORT", false),
            ("TOUCH", false),
            ("WAIT", false),
        ] {
            self.register(cmd, CommandCategory::Key, CommandStatus::Full, critical);
        }

        // List commands
        for (cmd, critical) in [
            ("LPUSH", true),
            ("RPUSH", true),
            ("LPOP", true),
            ("RPOP", true),
            ("LRANGE", true),
            ("LLEN", true),
            ("LINDEX", false),
            ("LSET", false),
            ("LREM", false),
            ("LINSERT", false),
            ("RPOPLPUSH", false),
            ("LMOVE", false),
            ("LPOS", false),
            ("LTRIM", false),
        ] {
            self.register(cmd, CommandCategory::List, CommandStatus::Full, critical);
        }

        // Hash commands
        for (cmd, critical) in [
            ("HSET", true),
            ("HGET", true),
            ("HMSET", true),
            ("HMGET", true),
            ("HDEL", true),
            ("HEXISTS", true),
            ("HGETALL", true),
            ("HKEYS", false),
            ("HVALS", false),
            ("HLEN", false),
            ("HINCRBY", false),
            ("HINCRBYFLOAT", false),
            ("HSETNX", false),
            ("HSCAN", false),
            ("HRANDFIELD", false),
        ] {
            self.register(cmd, CommandCategory::Hash, CommandStatus::Full, critical);
        }

        // Set commands
        for (cmd, critical) in [
            ("SADD", true),
            ("SREM", true),
            ("SMEMBERS", true),
            ("SISMEMBER", true),
            ("SCARD", true),
            ("SUNION", false),
            ("SINTER", false),
            ("SDIFF", false),
            ("SUNIONSTORE", false),
            ("SINTERSTORE", false),
            ("SDIFFSTORE", false),
            ("SRANDMEMBER", false),
            ("SPOP", false),
            ("SMOVE", false),
            ("SMISMEMBER", false),
            ("SSCAN", false),
        ] {
            self.register(cmd, CommandCategory::Set, CommandStatus::Full, critical);
        }

        // Sorted Set commands
        for (cmd, critical) in [
            ("ZADD", true),
            ("ZREM", true),
            ("ZSCORE", true),
            ("ZCARD", true),
            ("ZCOUNT", false),
            ("ZRANK", false),
            ("ZREVRANK", false),
            ("ZRANGE", true),
            ("ZREVRANGE", false),
            ("ZRANGEBYSCORE", false),
            ("ZREVRANGEBYSCORE", false),
            ("ZINCRBY", false),
            ("ZLEXCOUNT", false),
            ("ZRANGEBYLEX", false),
            ("ZPOPMIN", false),
            ("ZPOPMAX", false),
            ("ZUNIONSTORE", false),
            ("ZINTERSTORE", false),
            ("ZRANDMEMBER", false),
            ("ZMSCORE", false),
            ("ZSCAN", false),
        ] {
            self.register(
                cmd,
                CommandCategory::SortedSet,
                CommandStatus::Full,
                critical,
            );
        }

        // Server commands
        for (cmd, critical) in [
            ("PING", true),
            ("ECHO", false),
            ("INFO", true),
            ("SELECT", true),
            ("DBSIZE", false),
            ("FLUSHDB", false),
            ("FLUSHALL", false),
            ("CONFIG", false),
            ("COMMAND", false),
            ("CLIENT", false),
            ("SLOWLOG", false),
            ("DEBUG", false),
            ("TIME", false),
            ("BGSAVE", false),
            ("BGREWRITEAOF", false),
            ("LASTSAVE", false),
            ("SAVE", false),
        ] {
            self.register(cmd, CommandCategory::Server, CommandStatus::Full, critical);
        }

        // Pub/Sub commands
        for cmd in [
            "SUBSCRIBE",
            "UNSUBSCRIBE",
            "PSUBSCRIBE",
            "PUNSUBSCRIBE",
            "PUBLISH",
        ] {
            self.register(cmd, CommandCategory::PubSub, CommandStatus::Full, false);
        }

        // Transaction commands
        for cmd in ["MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH"] {
            self.register(
                cmd,
                CommandCategory::Transaction,
                CommandStatus::Full,
                false,
            );
        }

        // Scripting
        for cmd in ["EVAL", "EVALSHA", "SCRIPT"] {
            self.register(cmd, CommandCategory::Scripting, CommandStatus::Full, false);
        }

        // HyperLogLog
        for cmd in ["PFADD", "PFCOUNT", "PFMERGE"] {
            self.register(
                cmd,
                CommandCategory::HyperLogLog,
                CommandStatus::Full,
                false,
            );
        }

        // Geo
        for cmd in [
            "GEOADD",
            "GEODIST",
            "GEOHASH",
            "GEOPOS",
            "GEORADIUS",
            "GEOSEARCH",
            "GEOSEARCHSTORE",
        ] {
            self.register(cmd, CommandCategory::Geo, CommandStatus::Full, false);
        }

        // Stream
        for cmd in [
            "XADD",
            "XLEN",
            "XRANGE",
            "XREVRANGE",
            "XREAD",
            "XINFO",
            "XTRIM",
            "XDEL",
            "XGROUP",
            "XREADGROUP",
            "XACK",
            "XCLAIM",
            "XPENDING",
        ] {
            self.register(cmd, CommandCategory::Stream, CommandStatus::Full, false);
        }

        // Connection
        for cmd in ["AUTH", "QUIT", "HELLO", "RESET"] {
            self.register(cmd, CommandCategory::Connection, CommandStatus::Full, false);
        }
    }
}

impl Default for CompatibilityTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tracker_initialization() {
        let tracker = CompatibilityTracker::new();
        assert!(tracker.total_commands() > 100);
        assert!(tracker.get("GET").is_some());
        assert!(tracker.get("SET").is_some());
        assert!(tracker.get("NONEXISTENT").is_none());
    }

    #[test]
    fn test_compatibility_score() {
        let tracker = CompatibilityTracker::new();
        // All commands initialized as Full
        assert!(tracker.compatibility_score() > 0.9);
    }

    #[test]
    fn test_critical_score() {
        let tracker = CompatibilityTracker::new();
        assert!(tracker.critical_score() > 0.9);
    }

    #[test]
    fn test_record_test_pass() {
        let mut tracker = CompatibilityTracker::new();
        tracker.record_test(
            "GET",
            TestResult {
                name: "basic get".to_string(),
                passed: true,
                message: None,
                duration_us: 100,
            },
        );
        let entry = tracker.get("GET").unwrap();
        assert_eq!(entry.tests.len(), 1);
        assert!(entry.tests[0].passed);
    }

    #[test]
    fn test_record_test_fail() {
        let mut tracker = CompatibilityTracker::new();
        tracker.record_test(
            "GET",
            TestResult {
                name: "edge case".to_string(),
                passed: false,
                message: Some("mismatch".to_string()),
                duration_us: 50,
            },
        );
        let entry = tracker.get("GET").unwrap();
        assert_eq!(entry.status, CommandStatus::Failed);
    }

    #[test]
    fn test_run_test_closure() {
        let mut tracker = CompatibilityTracker::new();
        tracker.run_test("SET", "basic set", || Ok(()));
        let entry = tracker.get("SET").unwrap();
        assert_eq!(entry.tests.len(), 1);
        assert!(entry.tests[0].passed);
    }

    #[test]
    fn test_commands_in_category() {
        let tracker = CompatibilityTracker::new();
        let string_cmds = tracker.commands_in_category(CommandCategory::String);
        assert!(string_cmds.len() >= 10);
    }

    #[test]
    fn test_count_by_status() {
        let tracker = CompatibilityTracker::new();
        let full = tracker.count_by_status(CommandStatus::Full);
        assert!(full > 100);
        assert_eq!(tracker.count_by_status(CommandStatus::Failed), 0);
    }

    #[test]
    fn test_set_status() {
        let mut tracker = CompatibilityTracker::new();
        tracker.set_status("GET", CommandStatus::Partial);
        assert_eq!(tracker.get("GET").unwrap().status, CommandStatus::Partial);
    }

    #[test]
    fn test_add_note() {
        let mut tracker = CompatibilityTracker::new();
        tracker.add_note("GET", "Supports EX/PX options");
        assert_eq!(tracker.get("GET").unwrap().notes.len(), 1);
    }
}
