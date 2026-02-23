//! Redis TCL Test Suite Compatibility Tracker
//!
//! Provides a concurrent, thread-safe tracker for Redis TCL test suite results.
//! Records per-command pass/fail status from the official Redis TCL test suite
//! and generates a public compatibility matrix in Markdown format.
//!
//! # Overview
//!
//! The Redis project ships a comprehensive TCL-based test suite that exercises
//! every command and edge case. This module tracks Ferrite's compatibility
//! against that suite by:
//!
//! 1. Recording individual test results via [`TclCompatTracker::record_test_result`].
//! 2. Aggregating per-command statistics into [`CommandTestSummary`] entries.
//! 3. Producing a full [`CompatibilityReport`] with category breakdowns.
//! 4. Generating a Markdown compatibility matrix via [`TclCompatTracker::generate_matrix`].
//!
//! # Thread Safety
//!
//! All mutable state is behind [`DashMap`] so the tracker can be shared across
//! threads without external synchronization.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur during TCL compatibility tracking.
#[derive(Debug, Error)]
pub enum TclCompatError {
    /// The provided test result is invalid or malformed.
    #[error("invalid test result: {0}")]
    InvalidTestResult(String),

    /// The requested command was not found in the registry.
    #[error("command not found: {0}")]
    CommandNotFound(String),

    /// Failed to generate a compatibility report.
    #[error("report generation failed: {0}")]
    ReportGenerationFailed(String),

    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// Category of a Redis command.
///
/// Maps to the logical groupings used by the Redis documentation and TCL test
/// suite directory structure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CommandCategory {
    /// String commands (GET, SET, APPEND, …)
    String,
    /// List commands (LPUSH, RPUSH, LRANGE, …)
    List,
    /// Hash commands (HSET, HGET, HGETALL, …)
    Hash,
    /// Set commands (SADD, SREM, SMEMBERS, …)
    Set,
    /// Sorted-set commands (ZADD, ZRANGE, ZSCORE, …)
    SortedSet,
    /// Stream commands (XADD, XREAD, XRANGE, …)
    Stream,
    /// Bitmap commands (SETBIT, GETBIT, BITCOUNT, …)
    Bitmap,
    /// HyperLogLog commands (PFADD, PFCOUNT, PFMERGE)
    HyperLogLog,
    /// Geospatial commands (GEOADD, GEODIST, …)
    Geo,
    /// Pub/Sub commands (SUBSCRIBE, PUBLISH, …)
    PubSub,
    /// Transaction commands (MULTI, EXEC, WATCH, …)
    Transaction,
    /// Scripting commands (EVAL, EVALSHA, SCRIPT, …)
    Scripting,
    /// Server management commands (PING, INFO, CONFIG, …)
    Server,
    /// Key management commands (DEL, EXISTS, EXPIRE, …)
    Key,
    /// Cluster commands (CLUSTER, READONLY, …)
    Cluster,
    /// Connection commands (AUTH, SELECT, QUIT, …)
    Connection,
    /// Generic / uncategorized commands.
    Generic,
}

impl std::fmt::Display for CommandCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String => write!(f, "String"),
            Self::List => write!(f, "List"),
            Self::Hash => write!(f, "Hash"),
            Self::Set => write!(f, "Set"),
            Self::SortedSet => write!(f, "SortedSet"),
            Self::Stream => write!(f, "Stream"),
            Self::Bitmap => write!(f, "Bitmap"),
            Self::HyperLogLog => write!(f, "HyperLogLog"),
            Self::Geo => write!(f, "Geo"),
            Self::PubSub => write!(f, "PubSub"),
            Self::Transaction => write!(f, "Transaction"),
            Self::Scripting => write!(f, "Scripting"),
            Self::Server => write!(f, "Server"),
            Self::Key => write!(f, "Key"),
            Self::Cluster => write!(f, "Cluster"),
            Self::Connection => write!(f, "Connection"),
            Self::Generic => write!(f, "Generic"),
        }
    }
}

impl CommandCategory {
    /// Returns a slice of all category variants.
    pub fn all() -> &'static [CommandCategory] {
        &[
            Self::String,
            Self::List,
            Self::Hash,
            Self::Set,
            Self::SortedSet,
            Self::Stream,
            Self::Bitmap,
            Self::HyperLogLog,
            Self::Geo,
            Self::PubSub,
            Self::Transaction,
            Self::Scripting,
            Self::Server,
            Self::Key,
            Self::Cluster,
            Self::Connection,
            Self::Generic,
        ]
    }
}

/// Compatibility status for a single Redis command.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CommandStatus {
    /// All TCL tests pass for this command.
    FullyCompatible,
    /// Some tests pass; includes the pass rate and known issues.
    PartiallyCompatible {
        /// Fraction of tests that pass (0.0–1.0).
        pass_rate: f64,
        /// Descriptions of known compatibility gaps.
        known_issues: Vec<String>,
    },
    /// The command is not yet implemented in Ferrite.
    NotImplemented,
    /// The command is implemented but intentionally diverges from Redis.
    Incompatible {
        /// Human-readable explanation of the divergence.
        reason: String,
    },
    /// No TCL tests have been run for this command yet.
    NotTested,
}

impl std::fmt::Display for CommandStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FullyCompatible => write!(f, "✅ Fully Compatible"),
            Self::PartiallyCompatible { pass_rate, .. } => {
                write!(f, "⚠️  Partial ({:.1}%)", pass_rate * 100.0)
            }
            Self::NotImplemented => write!(f, "❌ Not Implemented"),
            Self::Incompatible { reason } => write!(f, "🔀 Incompatible: {reason}"),
            Self::NotTested => write!(f, "⬜ Not Tested"),
        }
    }
}

// ---------------------------------------------------------------------------
// Data structs
// ---------------------------------------------------------------------------

/// Result from a single Redis TCL test execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TclTestResult {
    /// Name of the individual test (e.g. `"SET and GET basic"`)
    pub test_name: String,
    /// TCL source file that contains the test (e.g. `"unit/basic.tcl"`)
    pub test_file: String,
    /// The primary Redis command exercised by this test.
    pub command: String,
    /// Whether the test passed.
    pub passed: bool,
    /// Wall-clock duration of the test in milliseconds.
    pub duration_ms: u64,
    /// Error message if the test failed.
    pub error_message: Option<String>,
    /// Redis version the TCL suite was written for (e.g. `"7.2"`)
    pub redis_version: String,
    /// Optional tags associated with the test (e.g. `["slow", "needs:repl"]`)
    pub tags: Vec<String>,
}

/// Aggregated test statistics for a single Redis command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandTestSummary {
    /// Command name (upper-case, e.g. `"GET"`).
    pub command: String,
    /// Total number of TCL tests that exercise this command.
    pub total_tests: u64,
    /// Number of passing tests.
    pub passed: u64,
    /// Number of failing tests.
    pub failed: u64,
    /// Number of skipped tests.
    pub skipped: u64,
    /// Pass rate as a fraction (0.0–1.0).
    pub pass_rate: f64,
    /// Derived compatibility status.
    pub status: CommandStatus,
    /// Descriptions of known deviations from Redis behaviour.
    pub known_deviations: Vec<String>,
    /// Timestamp of the most recent test run for this command.
    pub last_tested: DateTime<Utc>,
}

/// Full compatibility report covering all tracked commands.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatibilityReport {
    /// When the report was generated.
    pub generated_at: DateTime<Utc>,
    /// Ferrite version that was tested.
    pub ferrite_version: String,
    /// Redis version the TCL suite targets.
    pub redis_version: String,
    /// Total number of distinct commands tracked.
    pub total_commands: u64,
    /// Commands that are fully compatible.
    pub fully_compatible: u64,
    /// Commands that are partially compatible.
    pub partially_compatible: u64,
    /// Commands that are not yet implemented.
    pub not_implemented: u64,
    /// Commands that are intentionally incompatible.
    pub incompatible: u64,
    /// Overall pass rate across all tests (0.0–1.0).
    pub overall_pass_rate: f64,
    /// Per-command summaries, sorted alphabetically.
    pub command_summaries: Vec<CommandTestSummary>,
    /// Per-category roll-ups.
    pub categories: Vec<CategorySummary>,
}

/// Per-category compatibility summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategorySummary {
    /// The command category.
    pub category: CommandCategory,
    /// Number of distinct commands in this category.
    pub commands: u64,
    /// Aggregate pass rate across all tests in the category (0.0–1.0).
    pub pass_rate: f64,
}

/// High-level statistics from the tracker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatStats {
    /// Total individual test executions recorded.
    pub total_tests_run: u64,
    /// Total passing tests.
    pub total_passed: u64,
    /// Total failing tests.
    pub total_failed: u64,
    /// Number of distinct commands that have at least one test result.
    pub total_commands_tested: u64,
    /// Timestamp of the most recent test execution, if any.
    pub last_run: Option<DateTime<Utc>>,
}

// ---------------------------------------------------------------------------
// Internal per-command accumulator
// ---------------------------------------------------------------------------

/// Internal mutable accumulator stored per command inside the tracker.
#[derive(Debug, Clone)]
struct CommandAccumulator {
    passed: u64,
    failed: u64,
    error_messages: Vec<String>,
    last_tested: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// RedisCommandRegistry
// ---------------------------------------------------------------------------

/// Registry of known Redis commands and their categories.
///
/// Pre-populated with the ~220 most commonly used Redis commands so that the
/// tracker can categorise results even before any tests are run.
pub struct RedisCommandRegistry {
    commands: HashMap<String, CommandCategory>,
}

impl RedisCommandRegistry {
    /// Create a new registry pre-populated with known Redis commands.
    pub fn new() -> Self {
        let mut commands = HashMap::new();

        // -- String commands --
        for cmd in [
            "GET",
            "SET",
            "MGET",
            "MSET",
            "INCR",
            "DECR",
            "INCRBY",
            "DECRBY",
            "INCRBYFLOAT",
            "APPEND",
            "STRLEN",
            "GETRANGE",
            "SETRANGE",
            "GETSET",
            "SETNX",
            "SETEX",
            "PSETEX",
            "MSETNX",
            "GETDEL",
            "GETEX",
            "LCS",
            "SUBSTR",
        ] {
            commands.insert(cmd.to_string(), CommandCategory::String);
        }

        // -- Key commands --
        for cmd in [
            "DEL",
            "EXISTS",
            "EXPIRE",
            "EXPIREAT",
            "PEXPIRE",
            "PEXPIREAT",
            "EXPIRETIME",
            "PEXPIRETIME",
            "TTL",
            "PTTL",
            "PERSIST",
            "TYPE",
            "RENAME",
            "RENAMENX",
            "KEYS",
            "SCAN",
            "RANDOMKEY",
            "UNLINK",
            "OBJECT",
            "DUMP",
            "RESTORE",
            "SORT",
            "SORT_RO",
            "TOUCH",
            "WAIT",
            "WAITAOF",
            "COPY",
        ] {
            commands.insert(cmd.to_string(), CommandCategory::Key);
        }

        // -- List commands --
        for cmd in [
            "LPUSH",
            "RPUSH",
            "LPOP",
            "RPOP",
            "LRANGE",
            "LLEN",
            "LINDEX",
            "LSET",
            "LREM",
            "LINSERT",
            "RPOPLPUSH",
            "LMOVE",
            "LPOS",
            "LTRIM",
            "LMPOP",
            "BLPOP",
            "BRPOP",
            "BLMOVE",
            "BRPOPLPUSH",
            "LMPOP",
            "BLMPOP",
            "LPUSHX",
            "RPUSHX",
        ] {
            commands.insert(cmd.to_string(), CommandCategory::List);
        }

        // -- Hash commands --
        for cmd in [
            "HSET",
            "HGET",
            "HMSET",
            "HMGET",
            "HDEL",
            "HEXISTS",
            "HGETALL",
            "HKEYS",
            "HVALS",
            "HLEN",
            "HINCRBY",
            "HINCRBYFLOAT",
            "HSETNX",
            "HSCAN",
            "HRANDFIELD",
        ] {
            commands.insert(cmd.to_string(), CommandCategory::Hash);
        }

        // -- Set commands --
        for cmd in [
            "SADD",
            "SREM",
            "SMEMBERS",
            "SISMEMBER",
            "SCARD",
            "SUNION",
            "SINTER",
            "SDIFF",
            "SUNIONSTORE",
            "SINTERSTORE",
            "SDIFFSTORE",
            "SRANDMEMBER",
            "SPOP",
            "SMOVE",
            "SMISMEMBER",
            "SSCAN",
            "SINTERCARD",
        ] {
            commands.insert(cmd.to_string(), CommandCategory::Set);
        }

        // -- Sorted-set commands --
        for cmd in [
            "ZADD",
            "ZREM",
            "ZSCORE",
            "ZCARD",
            "ZCOUNT",
            "ZRANK",
            "ZREVRANK",
            "ZRANGE",
            "ZREVRANGE",
            "ZRANGEBYSCORE",
            "ZREVRANGEBYSCORE",
            "ZINCRBY",
            "ZLEXCOUNT",
            "ZRANGEBYLEX",
            "ZREVRANGEBYLEX",
            "ZPOPMIN",
            "ZPOPMAX",
            "ZUNIONSTORE",
            "ZINTERSTORE",
            "ZRANDMEMBER",
            "ZMSCORE",
            "ZSCAN",
            "ZRANGESTORE",
            "ZINTER",
            "ZUNION",
            "ZDIFF",
            "ZDIFFSTORE",
            "ZINTERCARD",
            "ZMPOP",
            "BZPOPMIN",
            "BZPOPMAX",
            "BZMPOP",
        ] {
            commands.insert(cmd.to_string(), CommandCategory::SortedSet);
        }

        // -- Stream commands --
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
            "XAUTOCLAIM",
            "XSETID",
        ] {
            commands.insert(cmd.to_string(), CommandCategory::Stream);
        }

        // -- Bitmap commands --
        for cmd in [
            "SETBIT",
            "GETBIT",
            "BITCOUNT",
            "BITOP",
            "BITPOS",
            "BITFIELD",
            "BITFIELD_RO",
        ] {
            commands.insert(cmd.to_string(), CommandCategory::Bitmap);
        }

        // -- HyperLogLog commands --
        for cmd in ["PFADD", "PFCOUNT", "PFMERGE", "PFDEBUG", "PFSELFTEST"] {
            commands.insert(cmd.to_string(), CommandCategory::HyperLogLog);
        }

        // -- Geo commands --
        for cmd in [
            "GEOADD",
            "GEODIST",
            "GEOHASH",
            "GEOPOS",
            "GEORADIUS",
            "GEORADIUSBYMEMBER",
            "GEOSEARCH",
            "GEOSEARCHSTORE",
        ] {
            commands.insert(cmd.to_string(), CommandCategory::Geo);
        }

        // -- Pub/Sub commands --
        for cmd in [
            "SUBSCRIBE",
            "UNSUBSCRIBE",
            "PSUBSCRIBE",
            "PUNSUBSCRIBE",
            "PUBLISH",
            "PUBSUB",
            "SSUBSCRIBE",
            "SUNSUBSCRIBE",
        ] {
            commands.insert(cmd.to_string(), CommandCategory::PubSub);
        }

        // -- Transaction commands --
        for cmd in ["MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH"] {
            commands.insert(cmd.to_string(), CommandCategory::Transaction);
        }

        // -- Scripting commands --
        for cmd in [
            "EVAL",
            "EVALSHA",
            "EVALSHA_RO",
            "EVAL_RO",
            "SCRIPT",
            "FUNCTION",
            "FCALL",
            "FCALL_RO",
        ] {
            commands.insert(cmd.to_string(), CommandCategory::Scripting);
        }

        // -- Server commands --
        for cmd in [
            "PING",
            "ECHO",
            "INFO",
            "DBSIZE",
            "FLUSHDB",
            "FLUSHALL",
            "CONFIG",
            "COMMAND",
            "CLIENT",
            "SLOWLOG",
            "DEBUG",
            "TIME",
            "BGSAVE",
            "BGREWRITEAOF",
            "LASTSAVE",
            "SAVE",
            "SHUTDOWN",
            "REPLICAOF",
            "SLAVEOF",
            "SWAPDB",
            "MEMORY",
            "MODULE",
            "ACL",
            "LATENCY",
            "LOLWUT",
            "RESET",
            "FAILOVER",
            "OBJECT",
        ] {
            commands.insert(cmd.to_string(), CommandCategory::Server);
        }

        // -- Connection commands --
        for cmd in ["AUTH", "SELECT", "QUIT", "HELLO", "CLIENT", "WAIT"] {
            commands.insert(cmd.to_string(), CommandCategory::Connection);
        }

        // -- Cluster commands --
        for cmd in ["CLUSTER", "READONLY", "READWRITE", "ASKING"] {
            commands.insert(cmd.to_string(), CommandCategory::Cluster);
        }

        Self { commands }
    }

    /// Return the category for a command, defaulting to [`CommandCategory::Generic`].
    pub fn get_category(&self, command: &str) -> CommandCategory {
        self.commands
            .get(&command.to_uppercase())
            .copied()
            .unwrap_or(CommandCategory::Generic)
    }

    /// Return all registered commands with their categories.
    pub fn get_all_commands(&self) -> Vec<(String, CommandCategory)> {
        let mut cmds: Vec<_> = self.commands.iter().map(|(k, v)| (k.clone(), *v)).collect();
        cmds.sort_by(|a, b| a.0.cmp(&b.0));
        cmds
    }

    /// Check whether a command name is present in the registry.
    pub fn is_known_command(&self, command: &str) -> bool {
        self.commands.contains_key(&command.to_uppercase())
    }
}

impl Default for RedisCommandRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// TclCompatTracker
// ---------------------------------------------------------------------------

/// Thread-safe tracker for Redis TCL test suite results.
///
/// Uses [`DashMap`] internally so multiple threads can record results
/// concurrently without external locking.
///
/// # Example
///
/// ```rust
/// use ferrite_core::compatibility::tcl_compat::{TclCompatTracker, TclTestResult};
///
/// let tracker = TclCompatTracker::new();
/// tracker.record_test_result(TclTestResult {
///     test_name: "SET basic".into(),
///     test_file: "unit/set.tcl".into(),
///     command: "SET".into(),
///     passed: true,
///     duration_ms: 12,
///     error_message: None,
///     redis_version: "7.2".into(),
///     tags: vec![],
/// });
///
/// let pct = tracker.get_compatibility_percentage();
/// assert!(pct > 0.0);
/// ```
pub struct TclCompatTracker {
    /// Per-command accumulators keyed by upper-case command name.
    results: DashMap<String, CommandAccumulator>,
    /// Command registry for category lookups.
    registry: RedisCommandRegistry,
}

impl TclCompatTracker {
    /// Create a new, empty tracker with the default Redis command registry.
    pub fn new() -> Self {
        Self {
            results: DashMap::new(),
            registry: RedisCommandRegistry::new(),
        }
    }

    /// Record the outcome of a single TCL test execution.
    ///
    /// The command name is normalised to upper-case before storage.
    pub fn record_test_result(&self, result: TclTestResult) {
        let key = result.command.to_uppercase();
        let now = Utc::now();

        self.results
            .entry(key)
            .and_modify(|acc| {
                if result.passed {
                    acc.passed += 1;
                } else {
                    acc.failed += 1;
                    if let Some(ref msg) = result.error_message {
                        acc.error_messages.push(msg.clone());
                    }
                }
                acc.last_tested = now;
            })
            .or_insert_with(|| {
                let mut acc = CommandAccumulator {
                    passed: 0,
                    failed: 0,
                    error_messages: Vec::new(),
                    last_tested: now,
                };
                if result.passed {
                    acc.passed += 1;
                } else {
                    acc.failed += 1;
                    if let Some(ref msg) = result.error_message {
                        acc.error_messages.push(msg.clone());
                    }
                }
                acc
            });
    }

    /// Return the compatibility status for a single command.
    ///
    /// Returns [`CommandStatus::NotTested`] if no results have been recorded.
    pub fn get_command_status(&self, command: &str) -> CommandStatus {
        let key = command.to_uppercase();
        match self.results.get(&key) {
            Some(acc) => Self::derive_status(&acc),
            None => CommandStatus::NotTested,
        }
    }

    /// Build and return a full compatibility report.
    pub fn get_compatibility_report(&self) -> CompatibilityReport {
        let summaries = self.build_summaries();

        let mut fully_compatible: u64 = 0;
        let mut partially_compatible: u64 = 0;
        let not_implemented: u64 = 0;
        let mut incompatible: u64 = 0;

        for s in &summaries {
            match &s.status {
                CommandStatus::FullyCompatible => fully_compatible += 1,
                CommandStatus::PartiallyCompatible { .. } => partially_compatible += 1,
                CommandStatus::Incompatible { .. } => incompatible += 1,
                _ => {}
            }
        }

        let total_commands = summaries.len() as u64;
        let (total_passed, total_tests) = summaries
            .iter()
            .fold((0u64, 0u64), |(p, t), s| (p + s.passed, t + s.total_tests));
        let overall_pass_rate = if total_tests > 0 {
            total_passed as f64 / total_tests as f64
        } else {
            0.0
        };

        // Category summaries
        let categories = self.build_category_summaries(&summaries);

        CompatibilityReport {
            generated_at: Utc::now(),
            ferrite_version: env!("CARGO_PKG_VERSION").to_string(),
            redis_version: "7.2".to_string(),
            total_commands,
            fully_compatible,
            partially_compatible,
            not_implemented,
            incompatible,
            overall_pass_rate,
            command_summaries: summaries,
            categories,
        }
    }

    /// Return the overall compatibility percentage (0.0–100.0).
    ///
    /// This is the proportion of *tests* that passed, expressed as a percentage.
    pub fn get_compatibility_percentage(&self) -> f64 {
        let (passed, total) = self.results.iter().fold((0u64, 0u64), |(p, t), entry| {
            let acc = entry.value();
            (p + acc.passed, t + acc.passed + acc.failed)
        });
        if total == 0 {
            return 0.0;
        }
        (passed as f64 / total as f64) * 100.0
    }

    /// Return summaries for all commands that have at least one failing test.
    pub fn get_failing_commands(&self) -> Vec<CommandTestSummary> {
        self.build_summaries()
            .into_iter()
            .filter(|s| s.failed > 0)
            .collect()
    }

    /// Return summaries for all commands where every test passes.
    pub fn get_passing_commands(&self) -> Vec<CommandTestSummary> {
        self.build_summaries()
            .into_iter()
            .filter(|s| s.passed > 0 && s.failed == 0)
            .collect()
    }

    /// Generate a Markdown compatibility matrix table.
    ///
    /// Columns: Command | Category | Tests | Passed | Failed | Pass Rate | Status
    pub fn generate_matrix(&self) -> String {
        let summaries = self.build_summaries();
        let mut md = String::new();

        md.push_str("# Ferrite – Redis TCL Test Compatibility Matrix\n\n");
        md.push_str(&format!(
            "_Generated at {}_\n\n",
            Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
        ));
        md.push_str(&format!(
            "**Overall pass rate:** {:.1}%\n\n",
            self.get_compatibility_percentage()
        ));
        md.push_str("| Command | Category | Tests | Passed | Failed | Pass Rate | Status |\n");
        md.push_str("|---------|----------|------:|-------:|-------:|----------:|--------|\n");

        for s in &summaries {
            let category = self.registry.get_category(&s.command);
            let status_icon = match &s.status {
                CommandStatus::FullyCompatible => "✅",
                CommandStatus::PartiallyCompatible { .. } => "⚠️",
                CommandStatus::NotImplemented => "❌",
                CommandStatus::Incompatible { .. } => "🔀",
                CommandStatus::NotTested => "⬜",
            };
            md.push_str(&format!(
                "| {} | {} | {} | {} | {} | {:.1}% | {} |\n",
                s.command,
                category,
                s.total_tests,
                s.passed,
                s.failed,
                s.pass_rate * 100.0,
                status_icon,
            ));
        }

        md
    }

    /// Return high-level statistics from the tracker.
    pub fn get_stats(&self) -> CompatStats {
        let mut total_passed = 0u64;
        let mut total_failed = 0u64;
        let mut last_run: Option<DateTime<Utc>> = None;

        for entry in self.results.iter() {
            let acc = entry.value();
            total_passed += acc.passed;
            total_failed += acc.failed;
            match last_run {
                Some(prev) if acc.last_tested > prev => last_run = Some(acc.last_tested),
                None => last_run = Some(acc.last_tested),
                _ => {}
            }
        }

        CompatStats {
            total_tests_run: total_passed + total_failed,
            total_passed,
            total_failed,
            total_commands_tested: self.results.len() as u64,
            last_run,
        }
    }

    // -- private helpers -----------------------------------------------------

    /// Derive a [`CommandStatus`] from an accumulator.
    fn derive_status(acc: &CommandAccumulator) -> CommandStatus {
        let total = acc.passed + acc.failed;
        if total == 0 {
            return CommandStatus::NotTested;
        }
        if acc.failed == 0 {
            return CommandStatus::FullyCompatible;
        }
        if acc.passed == 0 {
            return CommandStatus::Incompatible {
                reason: "all tests fail".to_string(),
            };
        }
        let pass_rate = acc.passed as f64 / total as f64;
        CommandStatus::PartiallyCompatible {
            pass_rate,
            known_issues: acc.error_messages.clone(),
        }
    }

    /// Build sorted [`CommandTestSummary`] list from current state.
    fn build_summaries(&self) -> Vec<CommandTestSummary> {
        let mut summaries: Vec<CommandTestSummary> = self
            .results
            .iter()
            .map(|entry| {
                let command = entry.key().clone();
                let acc = entry.value();
                let total = acc.passed + acc.failed;
                let pass_rate = if total > 0 {
                    acc.passed as f64 / total as f64
                } else {
                    0.0
                };
                CommandTestSummary {
                    command,
                    total_tests: total,
                    passed: acc.passed,
                    failed: acc.failed,
                    skipped: 0,
                    pass_rate,
                    status: Self::derive_status(acc),
                    known_deviations: acc.error_messages.clone(),
                    last_tested: acc.last_tested,
                }
            })
            .collect();
        summaries.sort_by(|a, b| a.command.cmp(&b.command));
        summaries
    }

    /// Build per-category summaries from command summaries.
    fn build_category_summaries(&self, summaries: &[CommandTestSummary]) -> Vec<CategorySummary> {
        let mut by_cat: HashMap<CommandCategory, (u64, u64, u64)> = HashMap::new();

        for s in summaries {
            let cat = self.registry.get_category(&s.command);
            let entry = by_cat.entry(cat).or_insert((0, 0, 0));
            entry.0 += 1; // commands
            entry.1 += s.passed; // passed
            entry.2 += s.total_tests; // total
        }

        let mut cats: Vec<CategorySummary> = by_cat
            .into_iter()
            .map(|(category, (commands, passed, total))| {
                let pass_rate = if total > 0 {
                    passed as f64 / total as f64
                } else {
                    0.0
                };
                CategorySummary {
                    category,
                    commands,
                    pass_rate,
                }
            })
            .collect();
        cats.sort_by(|a, b| a.category.to_string().cmp(&b.category.to_string()));
        cats
    }
}

impl Default for TclCompatTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a passing test result for the given command.
    fn pass_result(command: &str) -> TclTestResult {
        TclTestResult {
            test_name: format!("{command} basic test"),
            test_file: format!("unit/{}.tcl", command.to_lowercase()),
            command: command.to_string(),
            passed: true,
            duration_ms: 10,
            error_message: None,
            redis_version: "7.2".to_string(),
            tags: vec![],
        }
    }

    /// Helper: create a failing test result for the given command.
    fn fail_result(command: &str, msg: &str) -> TclTestResult {
        TclTestResult {
            test_name: format!("{command} edge case"),
            test_file: format!("unit/{}.tcl", command.to_lowercase()),
            command: command.to_string(),
            passed: false,
            duration_ms: 5,
            error_message: Some(msg.to_string()),
            redis_version: "7.2".to_string(),
            tags: vec!["regression".to_string()],
        }
    }

    // -- Recording & status -------------------------------------------------

    #[test]
    fn test_record_and_status_fully_compatible() {
        let tracker = TclCompatTracker::new();
        tracker.record_test_result(pass_result("GET"));
        tracker.record_test_result(pass_result("GET"));

        assert_eq!(
            tracker.get_command_status("GET"),
            CommandStatus::FullyCompatible
        );
    }

    #[test]
    fn test_record_and_status_partially_compatible() {
        let tracker = TclCompatTracker::new();
        tracker.record_test_result(pass_result("SET"));
        tracker.record_test_result(fail_result("SET", "NX option mismatch"));

        match tracker.get_command_status("SET") {
            CommandStatus::PartiallyCompatible {
                pass_rate,
                known_issues,
            } => {
                assert!((pass_rate - 0.5).abs() < f64::EPSILON);
                assert_eq!(known_issues.len(), 1);
                assert!(known_issues[0].contains("NX option"));
            }
            other => panic!("expected PartiallyCompatible, got {other:?}"),
        }
    }

    #[test]
    fn test_record_and_status_all_fail_is_incompatible() {
        let tracker = TclCompatTracker::new();
        tracker.record_test_result(fail_result("EVAL", "scripting disabled"));

        match tracker.get_command_status("EVAL") {
            CommandStatus::Incompatible { reason } => {
                assert_eq!(reason, "all tests fail");
            }
            other => panic!("expected Incompatible, got {other:?}"),
        }
    }

    #[test]
    fn test_not_tested_by_default() {
        let tracker = TclCompatTracker::new();
        assert_eq!(
            tracker.get_command_status("RANDOMCMD"),
            CommandStatus::NotTested
        );
    }

    // -- Compatibility percentage -------------------------------------------

    #[test]
    fn test_compatibility_percentage_all_pass() {
        let tracker = TclCompatTracker::new();
        tracker.record_test_result(pass_result("GET"));
        tracker.record_test_result(pass_result("SET"));

        assert!((tracker.get_compatibility_percentage() - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_compatibility_percentage_mixed() {
        let tracker = TclCompatTracker::new();
        tracker.record_test_result(pass_result("GET"));
        tracker.record_test_result(fail_result("GET", "err"));

        assert!((tracker.get_compatibility_percentage() - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_compatibility_percentage_empty() {
        let tracker = TclCompatTracker::new();
        assert!((tracker.get_compatibility_percentage() - 0.0).abs() < f64::EPSILON);
    }

    // -- Passing / Failing command lists ------------------------------------

    #[test]
    fn test_get_passing_commands() {
        let tracker = TclCompatTracker::new();
        tracker.record_test_result(pass_result("GET"));
        tracker.record_test_result(pass_result("SET"));
        tracker.record_test_result(fail_result("DEL", "err"));

        let passing = tracker.get_passing_commands();
        assert_eq!(passing.len(), 2);
        assert!(passing.iter().all(|s| s.failed == 0));
    }

    #[test]
    fn test_get_failing_commands() {
        let tracker = TclCompatTracker::new();
        tracker.record_test_result(pass_result("GET"));
        tracker.record_test_result(fail_result("DEL", "timeout"));

        let failing = tracker.get_failing_commands();
        assert_eq!(failing.len(), 1);
        assert_eq!(failing[0].command, "DEL");
    }

    // -- Category summaries -------------------------------------------------

    #[test]
    fn test_category_summaries() {
        let tracker = TclCompatTracker::new();
        tracker.record_test_result(pass_result("GET"));
        tracker.record_test_result(pass_result("SET"));
        tracker.record_test_result(pass_result("LPUSH"));

        let report = tracker.get_compatibility_report();
        // Should have at least String and List categories.
        let cat_names: Vec<String> = report
            .categories
            .iter()
            .map(|c| c.category.to_string())
            .collect();
        assert!(cat_names.contains(&"String".to_string()));
        assert!(cat_names.contains(&"List".to_string()));

        // String category should have 2 commands (GET, SET)
        let string_cat = report
            .categories
            .iter()
            .find(|c| c.category == CommandCategory::String)
            .unwrap();
        assert_eq!(string_cat.commands, 2);
        assert!((string_cat.pass_rate - 1.0).abs() < f64::EPSILON);
    }

    // -- Markdown matrix ----------------------------------------------------

    #[test]
    fn test_generate_matrix_contains_header() {
        let tracker = TclCompatTracker::new();
        tracker.record_test_result(pass_result("PING"));

        let md = tracker.generate_matrix();
        assert!(md.contains("# Ferrite – Redis TCL Test Compatibility Matrix"));
        assert!(md.contains("| Command | Category |"));
        assert!(md.contains("PING"));
        assert!(md.contains("✅"));
    }

    #[test]
    fn test_generate_matrix_empty_tracker() {
        let tracker = TclCompatTracker::new();
        let md = tracker.generate_matrix();
        // Should still have a header even with no data rows.
        assert!(md.contains("| Command |"));
        assert!(md.contains("0.0%"));
    }

    // -- Command registry ---------------------------------------------------

    #[test]
    fn test_registry_known_commands() {
        let reg = RedisCommandRegistry::new();
        assert!(reg.is_known_command("GET"));
        assert!(reg.is_known_command("get")); // case-insensitive
        assert!(reg.is_known_command("ZADD"));
        assert!(!reg.is_known_command("FOOBAR"));
    }

    #[test]
    fn test_registry_categories() {
        let reg = RedisCommandRegistry::new();
        assert_eq!(reg.get_category("GET"), CommandCategory::String);
        assert_eq!(reg.get_category("LPUSH"), CommandCategory::List);
        assert_eq!(reg.get_category("HSET"), CommandCategory::Hash);
        assert_eq!(reg.get_category("SADD"), CommandCategory::Set);
        assert_eq!(reg.get_category("ZADD"), CommandCategory::SortedSet);
        assert_eq!(reg.get_category("XADD"), CommandCategory::Stream);
        assert_eq!(reg.get_category("SETBIT"), CommandCategory::Bitmap);
        assert_eq!(reg.get_category("PFADD"), CommandCategory::HyperLogLog);
        assert_eq!(reg.get_category("GEOADD"), CommandCategory::Geo);
        assert_eq!(reg.get_category("SUBSCRIBE"), CommandCategory::PubSub);
        assert_eq!(reg.get_category("MULTI"), CommandCategory::Transaction);
        assert_eq!(reg.get_category("EVAL"), CommandCategory::Scripting);
        assert_eq!(reg.get_category("PING"), CommandCategory::Server);
        assert_eq!(reg.get_category("DEL"), CommandCategory::Key);
        assert_eq!(reg.get_category("CLUSTER"), CommandCategory::Cluster);
        // Unknown command → Generic
        assert_eq!(reg.get_category("NOSUCHCMD"), CommandCategory::Generic);
    }

    #[test]
    fn test_registry_get_all_commands() {
        let reg = RedisCommandRegistry::new();
        let all = reg.get_all_commands();
        // Should have at least 80 commands registered.
        assert!(
            all.len() >= 80,
            "expected >= 80 commands, got {}",
            all.len()
        );
        // Sorted alphabetically.
        let names: Vec<&str> = all.iter().map(|(n, _)| n.as_str()).collect();
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);
    }

    // -- Full report --------------------------------------------------------

    #[test]
    fn test_full_report_generation() {
        let tracker = TclCompatTracker::new();
        for cmd in ["GET", "SET", "DEL", "LPUSH", "HSET"] {
            tracker.record_test_result(pass_result(cmd));
        }
        tracker.record_test_result(fail_result("EVAL", "not supported"));

        let report = tracker.get_compatibility_report();

        assert_eq!(report.total_commands, 6);
        assert_eq!(report.fully_compatible, 5);
        assert_eq!(report.incompatible, 1);
        assert!(report.overall_pass_rate > 0.8);
        assert!(!report.command_summaries.is_empty());
        assert!(!report.categories.is_empty());
        assert!(!report.ferrite_version.is_empty());
    }

    // -- Stats --------------------------------------------------------------

    #[test]
    fn test_get_stats() {
        let tracker = TclCompatTracker::new();
        tracker.record_test_result(pass_result("GET"));
        tracker.record_test_result(pass_result("GET"));
        tracker.record_test_result(fail_result("SET", "err"));

        let stats = tracker.get_stats();
        assert_eq!(stats.total_tests_run, 3);
        assert_eq!(stats.total_passed, 2);
        assert_eq!(stats.total_failed, 1);
        assert_eq!(stats.total_commands_tested, 2);
        assert!(stats.last_run.is_some());
    }

    #[test]
    fn test_get_stats_empty() {
        let tracker = TclCompatTracker::new();
        let stats = tracker.get_stats();
        assert_eq!(stats.total_tests_run, 0);
        assert_eq!(stats.total_passed, 0);
        assert_eq!(stats.total_failed, 0);
        assert_eq!(stats.total_commands_tested, 0);
        assert!(stats.last_run.is_none());
    }

    // -- Edge cases ---------------------------------------------------------

    #[test]
    fn test_case_insensitive_command() {
        let tracker = TclCompatTracker::new();
        tracker.record_test_result(pass_result("get"));
        tracker.record_test_result(pass_result("Get"));

        // Both should accumulate under "GET"
        let stats = tracker.get_stats();
        assert_eq!(stats.total_commands_tested, 1);

        assert_eq!(
            tracker.get_command_status("GET"),
            CommandStatus::FullyCompatible
        );
    }

    #[test]
    fn test_unknown_command_recorded() {
        let tracker = TclCompatTracker::new();
        tracker.record_test_result(pass_result("CUSTOMCMD"));

        assert_eq!(
            tracker.get_command_status("CUSTOMCMD"),
            CommandStatus::FullyCompatible
        );
        // Category should be Generic for unknown commands.
        let report = tracker.get_compatibility_report();
        let summary = report
            .command_summaries
            .iter()
            .find(|s| s.command == "CUSTOMCMD")
            .unwrap();
        assert_eq!(summary.passed, 1);
    }

    #[test]
    fn test_command_status_display() {
        assert!(format!("{}", CommandStatus::FullyCompatible).contains("Fully Compatible"));
        assert!(format!("{}", CommandStatus::NotTested).contains("Not Tested"));
        assert!(format!(
            "{}",
            CommandStatus::PartiallyCompatible {
                pass_rate: 0.75,
                known_issues: vec![]
            }
        )
        .contains("75.0%"));
    }

    #[test]
    fn test_tcl_compat_error_display() {
        let e = TclCompatError::InvalidTestResult("bad input".into());
        assert_eq!(e.to_string(), "invalid test result: bad input");

        let e = TclCompatError::CommandNotFound("XYZ".into());
        assert_eq!(e.to_string(), "command not found: XYZ");

        let e = TclCompatError::ReportGenerationFailed("IO error".into());
        assert_eq!(e.to_string(), "report generation failed: IO error");

        let e = TclCompatError::Internal("panic".into());
        assert_eq!(e.to_string(), "internal error: panic");
    }

    #[test]
    fn test_category_all_covers_every_variant() {
        let all = CommandCategory::all();
        assert_eq!(all.len(), 17);
        // Bitmap should be present (new vs existing tracker module)
        assert!(all.contains(&CommandCategory::Bitmap));
    }
}
