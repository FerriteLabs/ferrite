//! Redis Protocol Compatibility Tracking and Gap Analysis
//!
//! Provides per-command compatibility status tracking against Redis 7.4,
//! category-level summaries, and a full gap report for planning work.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Per-command compatibility status against Redis.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CompatStatus {
    /// Fully implemented and tested.
    Full,
    /// Partially implemented — the `String` describes what is missing.
    Partial(String),
    /// Dispatch stub exists but no real logic.
    Stub,
    /// Not implemented at all.
    Missing,
    /// Not applicable to Ferrite's architecture.
    NotApplicable,
}

/// Redis command category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CommandCategory {
    /// String commands (GET, SET, APPEND, …)
    String,
    /// List commands (LPUSH, RPUSH, …)
    List,
    /// Hash commands (HSET, HGET, …)
    Hash,
    /// Set commands (SADD, SREM, …)
    Set,
    /// Sorted-set commands (ZADD, ZREM, …)
    SortedSet,
    /// Key-management commands (DEL, EXISTS, …)
    Key,
    /// Server administration commands (INFO, CONFIG, …)
    Server,
    /// Connection commands (AUTH, PING, …)
    Connection,
    /// Pub/Sub commands (SUBSCRIBE, PUBLISH, …)
    PubSub,
    /// Transaction commands (MULTI, EXEC, …)
    Transaction,
    /// Scripting commands (EVAL, EVALSHA, …)
    Scripting,
    /// Stream commands (XADD, XREAD, …)
    Stream,
    /// Geospatial commands (GEOADD, GEODIST, …)
    Geo,
    /// HyperLogLog commands (PFADD, PFCOUNT, …)
    HyperLogLog,
    /// Bitmap commands (SETBIT, GETBIT, …)
    Bitmap,
    /// Cluster commands
    Cluster,
    /// Module commands
    Module,
    /// ACL commands
    ACL,
    /// Function commands
    Function,
    /// Debug commands
    Debug,
}

impl CommandCategory {
    /// Returns a slice of all category variants.
    pub fn all() -> &'static [CommandCategory] {
        &[
            CommandCategory::String,
            CommandCategory::List,
            CommandCategory::Hash,
            CommandCategory::Set,
            CommandCategory::SortedSet,
            CommandCategory::Key,
            CommandCategory::Server,
            CommandCategory::Connection,
            CommandCategory::PubSub,
            CommandCategory::Transaction,
            CommandCategory::Scripting,
            CommandCategory::Stream,
            CommandCategory::Geo,
            CommandCategory::HyperLogLog,
            CommandCategory::Bitmap,
            CommandCategory::Cluster,
            CommandCategory::Module,
            CommandCategory::ACL,
            CommandCategory::Function,
            CommandCategory::Debug,
        ]
    }
}

/// Compatibility metadata for a single Redis command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandCompat {
    /// Canonical command name (upper-case).
    pub name: String,
    /// Category the command belongs to.
    pub category: CommandCategory,
    /// Current implementation status.
    pub status: CompatStatus,
    /// Total number of sub-commands in the Redis spec.
    pub subcommands_total: u32,
    /// Number of sub-commands implemented in Ferrite.
    pub subcommands_implemented: u32,
    /// Optional notes about deviations or TODOs.
    pub notes: Option<String>,
    /// Redis version that first introduced this command (e.g. "1.0.0").
    pub redis_since: String,
}

/// Per-category compatibility summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategorySummary {
    /// The category being summarised.
    pub category: CommandCategory,
    /// Total commands registered in this category.
    pub total_commands: u32,
    /// Commands with [`CompatStatus::Full`].
    pub fully_implemented: u32,
    /// Commands with [`CompatStatus::Partial`].
    pub partially_implemented: u32,
    /// Commands with [`CompatStatus::Stub`].
    pub stub_only: u32,
    /// Commands with [`CompatStatus::Missing`].
    pub missing: u32,
    /// Implementation percentage (Full counts 100 %, Partial 50 %).
    pub percentage: f64,
}

/// Full gap-analysis report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatGapReport {
    /// Weighted overall compatibility percentage.
    pub overall_percentage: f64,
    /// Total registered commands.
    pub total_commands: u32,
    /// Commands with [`CompatStatus::Full`].
    pub fully_implemented: u32,
    /// Commands with [`CompatStatus::Partial`].
    pub partially_implemented: u32,
    /// Commands with [`CompatStatus::Stub`].
    pub stub_only: u32,
    /// Commands with [`CompatStatus::Missing`].
    pub missing: u32,
    /// Per-category breakdown.
    pub categories: Vec<CategorySummary>,
    /// Most impactful missing / stub commands.
    pub high_priority_gaps: Vec<String>,
    /// Intentional behavioural differences from Redis.
    pub known_deviations: Vec<String>,
}

/// Tracks Redis 7.4 command compatibility for the Ferrite engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatTracker {
    /// Per-command compatibility info keyed by upper-case command name.
    pub commands: HashMap<String, CommandCompat>,
}

fn insert_full(
    commands: &mut HashMap<String, CommandCompat>,
    name: &str,
    cat: CommandCategory,
    since: &str,
    subs_total: u32,
    subs_impl: u32,
) {
    commands.insert(
        name.to_uppercase(),
        CommandCompat {
            name: name.to_uppercase(),
            category: cat,
            status: CompatStatus::Full,
            subcommands_total: subs_total,
            subcommands_implemented: subs_impl,
            notes: None,
            redis_since: since.to_string(),
        },
    );
}

fn insert_partial(
    commands: &mut HashMap<String, CommandCompat>,
    name: &str,
    cat: CommandCategory,
    since: &str,
    detail: &str,
    subs_total: u32,
    subs_impl: u32,
) {
    commands.insert(
        name.to_uppercase(),
        CommandCompat {
            name: name.to_uppercase(),
            category: cat,
            status: CompatStatus::Partial(detail.to_string()),
            subcommands_total: subs_total,
            subcommands_implemented: subs_impl,
            notes: Some(detail.to_string()),
            redis_since: since.to_string(),
        },
    );
}

fn insert_stub(
    commands: &mut HashMap<String, CommandCompat>,
    name: &str,
    cat: CommandCategory,
    since: &str,
) {
    commands.insert(
        name.to_uppercase(),
        CommandCompat {
            name: name.to_uppercase(),
            category: cat,
            status: CompatStatus::Stub,
            subcommands_total: 0,
            subcommands_implemented: 0,
            notes: None,
            redis_since: since.to_string(),
        },
    );
}

fn insert_missing(
    commands: &mut HashMap<String, CommandCompat>,
    name: &str,
    cat: CommandCategory,
    since: &str,
) {
    commands.insert(
        name.to_uppercase(),
        CommandCompat {
            name: name.to_uppercase(),
            category: cat,
            status: CompatStatus::Missing,
            subcommands_total: 0,
            subcommands_implemented: 0,
            notes: None,
            redis_since: since.to_string(),
        },
    );
}

impl CompatTracker {
    /// Creates a new tracker pre-populated with Redis 7.4 commands.
    pub fn new() -> Self {
        let mut commands = HashMap::new();

        // ── String commands ─────────────────────────────────────────
        for cmd in &[
            "GET", "SET", "SETNX", "SETEX", "PSETEX", "MGET", "MSET", "MSETNX", "GETSET",
            "GETDEL", "GETEX", "APPEND", "STRLEN", "GETRANGE", "SETRANGE", "INCR", "DECR",
            "INCRBY", "DECRBY", "INCRBYFLOAT",
        ] {
            insert_full(&mut commands, cmd, CommandCategory::String, "1.0.0", 0, 0);
        }

        // ── List commands ───────────────────────────────────────────
        for cmd in &[
            "LPUSH", "RPUSH", "LPOP", "RPOP", "LLEN", "LRANGE", "LINDEX", "LSET", "LREM",
            "LINSERT", "LTRIM", "LPOS", "LMPOP", "RPOPLPUSH", "LMOVE",
        ] {
            insert_full(&mut commands, cmd, CommandCategory::List, "1.0.0", 0, 0);
        }

        // ── Hash commands ───────────────────────────────────────────
        for cmd in &[
            "HSET", "HGET", "HDEL", "HEXISTS", "HLEN", "HKEYS", "HVALS", "HGETALL", "HINCRBY",
            "HINCRBYFLOAT", "HMSET", "HMGET", "HSETNX", "HRANDFIELD", "HSCAN",
        ] {
            insert_full(&mut commands, cmd, CommandCategory::Hash, "2.0.0", 0, 0);
        }

        // ── Set commands ────────────────────────────────────────────
        for cmd in &[
            "SADD", "SREM", "SMEMBERS", "SISMEMBER", "SMISMEMBER", "SCARD", "SRANDMEMBER",
            "SPOP", "SUNION", "SINTER", "SDIFF", "SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE",
            "SINTERCARD", "SSCAN",
        ] {
            insert_full(&mut commands, cmd, CommandCategory::Set, "1.0.0", 0, 0);
        }

        // ── Sorted-set commands ─────────────────────────────────────
        for cmd in &[
            "ZADD",
            "ZREM",
            "ZSCORE",
            "ZRANK",
            "ZREVRANK",
            "ZRANGE",
            "ZRANGEBYSCORE",
            "ZRANGEBYLEX",
            "ZREVRANGE",
            "ZCOUNT",
            "ZLEXCOUNT",
            "ZCARD",
            "ZINCRBY",
            "ZRANGESTORE",
            "ZUNIONSTORE",
            "ZINTERSTORE",
            "ZDIFFSTORE",
            "ZRANDMEMBER",
            "ZMSCORE",
            "ZPOPMIN",
            "ZPOPMAX",
            "ZMPOP",
            "ZSCAN",
        ] {
            insert_full(&mut commands, cmd, CommandCategory::SortedSet, "1.2.0", 0, 0);
        }

        // ── Key commands ────────────────────────────────────────────
        for cmd in &[
            "DEL", "EXISTS", "KEYS", "SCAN", "TTL", "PTTL", "EXPIRE", "PEXPIRE", "EXPIREAT",
            "PERSIST", "TYPE", "RENAME", "RENAMENX", "COPY", "UNLINK", "DUMP", "RESTORE", "SORT",
            "OBJECT", "WAIT", "RANDOMKEY", "TOUCH", "SWAPDB",
        ] {
            insert_full(&mut commands, cmd, CommandCategory::Key, "1.0.0", 0, 0);
        }

        // ── PubSub commands ─────────────────────────────────────────
        for cmd in &[
            "SUBSCRIBE",
            "UNSUBSCRIBE",
            "PSUBSCRIBE",
            "PUNSUBSCRIBE",
            "PUBLISH",
            "SSUBSCRIBE",
            "SUNSUBSCRIBE",
        ] {
            insert_full(&mut commands, cmd, CommandCategory::PubSub, "2.0.0", 0, 0);
        }

        // ── Transaction commands ────────────────────────────────────
        for cmd in &["MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH"] {
            insert_full(&mut commands, cmd, CommandCategory::Transaction, "2.0.0", 0, 0);
        }

        // ── Connection commands ─────────────────────────────────────
        for cmd in &["PING", "ECHO", "SELECT", "AUTH", "QUIT"] {
            insert_full(&mut commands, cmd, CommandCategory::Connection, "1.0.0", 0, 0);
        }

        // ── Server commands (full) ──────────────────────────────────
        for cmd in &[
            "DBSIZE", "FLUSHDB", "FLUSHALL", "INFO", "TIME", "COMMAND",
        ] {
            insert_full(&mut commands, cmd, CommandCategory::Server, "1.0.0", 0, 0);
        }
        insert_full(&mut commands, "COMMAND COUNT", CommandCategory::Server, "2.8.13", 0, 0);
        insert_full(&mut commands, "COMMAND DOCS", CommandCategory::Server, "7.0.0", 0, 0);

        // ── Scripting commands (full) ───────────────────────────────
        for cmd in &["EVAL", "EVALSHA"] {
            insert_full(&mut commands, cmd, CommandCategory::Scripting, "2.6.0", 0, 0);
        }

        // ── HyperLogLog commands ────────────────────────────────────
        for cmd in &["PFADD", "PFCOUNT", "PFMERGE"] {
            insert_full(&mut commands, cmd, CommandCategory::HyperLogLog, "2.8.9", 0, 0);
        }

        // ── Stream commands ─────────────────────────────────────────
        for cmd in &[
            "XADD",
            "XLEN",
            "XRANGE",
            "XREVRANGE",
            "XREAD",
            "XINFO",
            "XTRIM",
            "XDEL",
            "XACK",
            "XGROUP",
            "XREADGROUP",
            "XCLAIM",
            "XPENDING",
            "XAUTOCLAIM",
        ] {
            insert_full(&mut commands, cmd, CommandCategory::Stream, "5.0.0", 0, 0);
        }

        // ── Geo commands ────────────────────────────────────────────
        for cmd in &[
            "GEOADD",
            "GEODIST",
            "GEOHASH",
            "GEOPOS",
            "GEORADIUS",
            "GEORADIUSBYMEMBER",
            "GEOSEARCH",
            "GEOSEARCHSTORE",
        ] {
            insert_full(&mut commands, cmd, CommandCategory::Geo, "3.2.0", 0, 0);
        }

        // ── Bitmap commands ─────────────────────────────────────────
        for cmd in &[
            "SETBIT", "GETBIT", "BITCOUNT", "BITOP", "BITPOS", "BITFIELD",
        ] {
            insert_full(&mut commands, cmd, CommandCategory::Bitmap, "2.2.0", 0, 0);
        }

        // ── Partial implementations ─────────────────────────────────
        insert_partial(&mut commands, 
            "CONFIG",
            CommandCategory::Server,
            "2.0.0",
            "GET/SET implemented, REWRITE/RESETSTAT partial",
            4,
            2,
        );
        insert_partial(&mut commands, 
            "CLIENT",
            CommandCategory::Connection,
            "2.4.0",
            "LIST/ID/SETNAME/INFO implemented, TRACKING not yet",
            10,
            4,
        );
        insert_partial(&mut commands, 
            "CLUSTER",
            CommandCategory::Cluster,
            "3.0.0",
            "Basic slot routing, not full protocol",
            20,
            5,
        );
        insert_partial(&mut commands, 
            "ACL",
            CommandCategory::ACL,
            "6.0.0",
            "SETUSER/GETUSER/DELUSER/LIST/LOG, missing CAT/GENPASS full",
            10,
            5,
        );
        insert_partial(&mut commands, 
            "DEBUG",
            CommandCategory::Debug,
            "1.0.0",
            "SLEEP/SET-ACTIVE-EXPIRE implemented, not OBJECT/CHANGE/etc",
            8,
            2,
        );
        insert_partial(&mut commands, 
            "SLOWLOG",
            CommandCategory::Server,
            "2.2.12",
            "GET/LEN/RESET",
            3,
            3,
        );
        insert_partial(&mut commands, 
            "SCRIPT",
            CommandCategory::Scripting,
            "2.6.0",
            "EXISTS/FLUSH/LOAD",
            3,
            3,
        );

        // ── Stubs ───────────────────────────────────────────────────
        insert_stub(&mut commands, "OBJECT HELP", CommandCategory::Key, "6.2.0");
        insert_stub(&mut commands, "OBJECT FREQ", CommandCategory::Key, "4.0.0");
        insert_stub(&mut commands, "MEMORY USAGE", CommandCategory::Server, "4.0.0");
        insert_stub(&mut commands, "MEMORY DOCTOR", CommandCategory::Server, "4.0.0");
        insert_stub(&mut commands, "LATENCY", CommandCategory::Server, "2.8.13");
        insert_stub(&mut commands, "MODULE", CommandCategory::Module, "4.0.0");
        insert_stub(&mut commands, "FUNCTION", CommandCategory::Function, "7.0.0");

        // ── Missing ─────────────────────────────────────────────────
        insert_missing(&mut commands, "FAILOVER", CommandCategory::Server, "6.2.0");
        insert_missing(&mut commands, "PSYNC", CommandCategory::Server, "2.8.0");
        insert_missing(&mut commands, "REPLCONF", CommandCategory::Server, "2.8.0");
        insert_missing(&mut commands, "CLUSTER FULL PROTOCOL", CommandCategory::Cluster, "3.0.0");
        insert_missing(&mut commands, "SENTINEL", CommandCategory::Server, "2.8.0");

        CompatTracker { commands }
    }

    /// Look up the compatibility status of a single command.
    pub fn get_status(&self, command: &str) -> Option<&CommandCompat> {
        self.commands.get(&command.to_uppercase())
    }

    /// Returns all commands matching the given status variant.
    pub fn commands_by_status(&self, status: CompatStatus) -> Vec<&CommandCompat> {
        self.commands
            .values()
            .filter(|c| std::mem::discriminant(&c.status) == std::mem::discriminant(&status))
            .collect()
    }

    /// Overall weighted compatibility percentage.
    ///
    /// Full = 1.0, Partial = 0.5, Stub = 0.1, Missing/NotApplicable = 0.
    pub fn compatibility_percentage(&self) -> f64 {
        if self.commands.is_empty() {
            return 0.0;
        }
        let (score, count) =
            self.commands
                .values()
                .fold((0.0_f64, 0u32), |(score, count), cmd| {
                    let w = match &cmd.status {
                        CompatStatus::Full => 1.0,
                        CompatStatus::Partial(_) => 0.5,
                        CompatStatus::Stub => 0.1,
                        CompatStatus::Missing | CompatStatus::NotApplicable => 0.0,
                    };
                    (score + w, count + 1)
                });
        (score / f64::from(count)) * 100.0
    }

    /// Per-category breakdown.
    pub fn category_summary(&self) -> Vec<CategorySummary> {
        let mut map: HashMap<CommandCategory, Vec<&CommandCompat>> = HashMap::new();
        for cmd in self.commands.values() {
            map.entry(cmd.category).or_default().push(cmd);
        }

        let mut summaries: Vec<CategorySummary> = map
            .into_iter()
            .map(|(cat, cmds)| {
                let total = cmds.len() as u32;
                let mut full_n = 0u32;
                let mut partial_n = 0u32;
                let mut stub_n = 0u32;
                let mut missing_n = 0u32;
                for c in &cmds {
                    match &c.status {
                        CompatStatus::Full => full_n += 1,
                        CompatStatus::Partial(_) => partial_n += 1,
                        CompatStatus::Stub => stub_n += 1,
                        CompatStatus::Missing => missing_n += 1,
                        CompatStatus::NotApplicable => {}
                    }
                }
                let pct = if total == 0 {
                    0.0
                } else {
                    ((f64::from(full_n) + f64::from(partial_n) * 0.5 + f64::from(stub_n) * 0.1)
                        / f64::from(total))
                        * 100.0
                };
                CategorySummary {
                    category: cat,
                    total_commands: total,
                    fully_implemented: full_n,
                    partially_implemented: partial_n,
                    stub_only: stub_n,
                    missing: missing_n,
                    percentage: pct,
                }
            })
            .collect();

        summaries.sort_by(|a, b| {
            b.percentage
                .partial_cmp(&a.percentage)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        summaries
    }

    /// Produce a full gap-analysis report.
    pub fn gap_report(&self) -> CompatGapReport {
        let total = self.commands.len() as u32;
        let mut full_n = 0u32;
        let mut partial_n = 0u32;
        let mut stub_n = 0u32;
        let mut missing_n = 0u32;
        for cmd in self.commands.values() {
            match &cmd.status {
                CompatStatus::Full => full_n += 1,
                CompatStatus::Partial(_) => partial_n += 1,
                CompatStatus::Stub => stub_n += 1,
                CompatStatus::Missing => missing_n += 1,
                CompatStatus::NotApplicable => {}
            }
        }

        let high_priority_gaps: Vec<String> = {
            let mut gaps: Vec<String> = self
                .commands
                .values()
                .filter(|c| matches!(c.status, CompatStatus::Missing | CompatStatus::Stub))
                .map(|c| c.name.clone())
                .collect();
            gaps.sort();
            gaps
        };

        let known_deviations = vec![
            "Ferrite uses epoch-based reclamation instead of Redis single-thread model".to_string(),
            "CLUSTER implementation uses custom slot-routing, not full Redis Cluster protocol"
                .to_string(),
            "WAIT semantics differ: Ferrite replication is async-first".to_string(),
            "MEMORY commands report Ferrite-specific allocator stats".to_string(),
            "DEBUG subset only — production safety limits apply".to_string(),
        ];

        CompatGapReport {
            overall_percentage: self.compatibility_percentage(),
            total_commands: total,
            fully_implemented: full_n,
            partially_implemented: partial_n,
            stub_only: stub_n,
            missing: missing_n,
            categories: self.category_summary(),
            high_priority_gaps,
            known_deviations,
        }
    }

    /// Serialise key metrics as `(field, value)` pairs suitable for the
    /// `INFO COMPATIBILITY` Redis-protocol response.
    pub fn to_resp_info(&self) -> Vec<(String, String)> {
        let report = self.gap_report();
        vec![
            (
                "overall_compatibility_pct".to_string(),
                format!("{:.1}", report.overall_percentage),
            ),
            (
                "total_commands".to_string(),
                report.total_commands.to_string(),
            ),
            (
                "fully_implemented".to_string(),
                report.fully_implemented.to_string(),
            ),
            (
                "partially_implemented".to_string(),
                report.partially_implemented.to_string(),
            ),
            ("stub_only".to_string(), report.stub_only.to_string()),
            ("missing".to_string(), report.missing.to_string()),
            (
                "high_priority_gap_count".to_string(),
                report.high_priority_gaps.len().to_string(),
            ),
            (
                "known_deviation_count".to_string(),
                report.known_deviations.len().to_string(),
            ),
        ]
    }
}

impl Default for CompatTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compatibility_percentage_above_70() {
        let tracker = CompatTracker::new();
        let pct = tracker.compatibility_percentage();
        assert!(
            pct > 70.0,
            "Expected compatibility > 70%, got {:.1}%",
            pct
        );
    }

    #[test]
    fn category_summary_totals_match() {
        let tracker = CompatTracker::new();
        let summaries = tracker.category_summary();
        let sum: u32 = summaries.iter().map(|s| s.total_commands).sum();
        assert_eq!(
            sum,
            tracker.commands.len() as u32,
            "Category totals should equal total registered commands"
        );
    }

    #[test]
    fn all_commands_have_valid_status() {
        let tracker = CompatTracker::new();
        for cmd in tracker.commands.values() {
            match &cmd.status {
                CompatStatus::Full
                | CompatStatus::Partial(_)
                | CompatStatus::Stub
                | CompatStatus::Missing
                | CompatStatus::NotApplicable => {}
            }
            assert!(!cmd.name.is_empty(), "Command name must not be empty");
            assert!(
                !cmd.redis_since.is_empty(),
                "redis_since must not be empty for {}",
                cmd.name
            );
        }
    }

    #[test]
    fn gap_report_has_high_priority_gaps() {
        let tracker = CompatTracker::new();
        let report = tracker.gap_report();
        assert!(
            !report.high_priority_gaps.is_empty(),
            "There should be high-priority gaps"
        );
    }

    #[test]
    fn string_category_near_100() {
        let tracker = CompatTracker::new();
        let summaries = tracker.category_summary();
        let string_summary = summaries
            .iter()
            .find(|s| s.category == CommandCategory::String)
            .expect("String category should exist");
        assert!(
            string_summary.percentage > 95.0,
            "String category should be near 100%, got {:.1}%",
            string_summary.percentage
        );
    }

    #[test]
    fn to_resp_info_returns_non_empty() {
        let tracker = CompatTracker::new();
        let info = tracker.to_resp_info();
        assert!(!info.is_empty(), "INFO COMPATIBILITY must return fields");
        assert!(
            info.iter().any(|(k, _)| k == "overall_compatibility_pct"),
            "Must contain overall_compatibility_pct"
        );
    }
}
