//! Compatibility Analyzer
//!
//! Analyzes Redis instances for Ferrite compatibility.

use super::{DataType, IssueSeverity, Result, SourceStats};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Compatibility analyzer for Redis to Ferrite migration
pub struct CompatibilityAnalyzer {
    /// Known command compatibility
    command_support: HashMap<String, CommandSupport>,
    /// Module support
    module_support: HashMap<String, ModuleSupport>,
}

impl CompatibilityAnalyzer {
    /// Create a new analyzer
    pub fn new() -> Self {
        let mut analyzer = Self {
            command_support: HashMap::new(),
            module_support: HashMap::new(),
        };
        analyzer.setup_command_support();
        analyzer.setup_module_support();
        analyzer
    }

    /// Setup known command support levels
    fn setup_command_support(&mut self) {
        // Fully supported commands
        let full_support = [
            // Strings
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
            "SETNX",
            "SETEX",
            "PSETEX",
            "GETSET",
            "GETEX",
            "GETDEL",
            // Keys
            "DEL",
            "EXISTS",
            "EXPIRE",
            "EXPIREAT",
            "PEXPIRE",
            "PEXPIREAT",
            "TTL",
            "PTTL",
            "PERSIST",
            "KEYS",
            "SCAN",
            "TYPE",
            "RENAME",
            "RENAMENX",
            "RANDOMKEY",
            "DBSIZE",
            "TOUCH",
            "UNLINK",
            "COPY",
            // Lists
            "LPUSH",
            "RPUSH",
            "LPOP",
            "RPOP",
            "LRANGE",
            "LLEN",
            "LINDEX",
            "LSET",
            "LINSERT",
            "LREM",
            "LTRIM",
            "BLPOP",
            "BRPOP",
            "LPOS",
            "LMPOP",
            "BLMPOP",
            // Hashes
            "HSET",
            "HGET",
            "HMSET",
            "HMGET",
            "HGETALL",
            "HDEL",
            "HEXISTS",
            "HLEN",
            "HKEYS",
            "HVALS",
            "HINCRBY",
            "HINCRBYFLOAT",
            "HSETNX",
            "HSCAN",
            "HRANDFIELD",
            // Sets
            "SADD",
            "SREM",
            "SMEMBERS",
            "SISMEMBER",
            "SCARD",
            "SPOP",
            "SRANDMEMBER",
            "SDIFF",
            "SINTER",
            "SUNION",
            "SDIFFSTORE",
            "SINTERSTORE",
            "SUNIONSTORE",
            "SMOVE",
            "SSCAN",
            "SMISMEMBER",
            // Sorted Sets
            "ZADD",
            "ZREM",
            "ZSCORE",
            "ZRANK",
            "ZREVRANK",
            "ZRANGE",
            "ZREVRANGE",
            "ZRANGEBYSCORE",
            "ZREVRANGEBYSCORE",
            "ZCOUNT",
            "ZCARD",
            "ZINCRBY",
            "ZUNIONSTORE",
            "ZINTERSTORE",
            "ZSCAN",
            "ZPOPMIN",
            "ZPOPMAX",
            "BZPOPMIN",
            "BZPOPMAX",
            "ZRANGESTORE",
            "ZMPOP",
            "BZMPOP",
            // Server
            "PING",
            "ECHO",
            "INFO",
            "CONFIG",
            "COMMAND",
            "CLIENT",
            "DEBUG",
            "FLUSHDB",
            "FLUSHALL",
            "SELECT",
            "SWAPDB",
            "DBSIZE",
            "TIME",
            "LASTSAVE",
            "MEMORY",
            // Transactions
            "MULTI",
            "EXEC",
            "DISCARD",
            "WATCH",
            "UNWATCH",
            // Pub/Sub
            "PUBLISH",
            "SUBSCRIBE",
            "UNSUBSCRIBE",
            "PSUBSCRIBE",
            "PUNSUBSCRIBE",
            "PUBSUB",
            // Scripting
            "EVAL",
            "EVALSHA",
            "SCRIPT",
            // HyperLogLog
            "PFADD",
            "PFCOUNT",
            "PFMERGE",
            // Geo
            "GEOADD",
            "GEOPOS",
            "GEODIST",
            "GEORADIUS",
            "GEORADIUSBYMEMBER",
            "GEOHASH",
            "GEOSEARCH",
            "GEOSEARCHSTORE",
            // Streams
            "XADD",
            "XREAD",
            "XRANGE",
            "XREVRANGE",
            "XLEN",
            "XINFO",
            "XTRIM",
            "XDEL",
            "XGROUP",
            "XREADGROUP",
            "XACK",
            "XCLAIM",
            "XAUTOCLAIM",
            "XPENDING",
            "XSETID",
        ];

        for cmd in full_support {
            self.command_support
                .insert(cmd.to_string(), CommandSupport::Full);
        }

        // Partially supported commands
        let partial_support = [
            ("OBJECT", "Some subcommands may differ"),
            ("DEBUG", "Limited debug commands available"),
            ("SLOWLOG", "Implementation may vary"),
            ("LATENCY", "Ferrite has different latency tracking"),
        ];

        for (cmd, note) in partial_support {
            self.command_support
                .insert(cmd.to_string(), CommandSupport::Partial(note.to_string()));
        }

        // Unsupported commands
        let unsupported = [
            ("CLUSTER", "Ferrite uses different clustering"),
            ("SENTINEL", "Not applicable to Ferrite"),
            ("ACL", "Ferrite has different auth system"),
            ("MODULE", "Ferrite has native modules"),
        ];

        for (cmd, reason) in unsupported {
            self.command_support.insert(
                cmd.to_string(),
                CommandSupport::Unsupported(reason.to_string()),
            );
        }
    }

    /// Setup known module support
    fn setup_module_support(&mut self) {
        self.module_support.insert(
            "ReJSON".to_string(),
            ModuleSupport {
                name: "ReJSON".to_string(),
                ferrite_equivalent: Some("Native JSON support".to_string()),
                compatibility: ModuleCompatibility::Compatible,
                notes: "Ferrite has native JSON support with similar API".to_string(),
            },
        );

        self.module_support.insert(
            "RediSearch".to_string(),
            ModuleSupport {
                name: "RediSearch".to_string(),
                ferrite_equivalent: Some("Native search".to_string()),
                compatibility: ModuleCompatibility::PartiallyCompatible,
                notes: "Ferrite has native full-text search, some syntax may differ".to_string(),
            },
        );

        self.module_support.insert(
            "RedisTimeSeries".to_string(),
            ModuleSupport {
                name: "RedisTimeSeries".to_string(),
                ferrite_equivalent: Some("Native time series".to_string()),
                compatibility: ModuleCompatibility::Compatible,
                notes: "Ferrite has native time series support".to_string(),
            },
        );

        self.module_support.insert(
            "RedisGraph".to_string(),
            ModuleSupport {
                name: "RedisGraph".to_string(),
                ferrite_equivalent: Some("Native graph".to_string()),
                compatibility: ModuleCompatibility::PartiallyCompatible,
                notes: "Ferrite has native graph support, Cypher syntax supported".to_string(),
            },
        );

        self.module_support.insert(
            "RedisBloom".to_string(),
            ModuleSupport {
                name: "RedisBloom".to_string(),
                ferrite_equivalent: None,
                compatibility: ModuleCompatibility::NotSupported,
                notes: "Bloom filters not yet supported in Ferrite".to_string(),
            },
        );

        self.module_support.insert(
            "RedisAI".to_string(),
            ModuleSupport {
                name: "RedisAI".to_string(),
                ferrite_equivalent: Some("Vector search + WASM".to_string()),
                compatibility: ModuleCompatibility::Alternative,
                notes: "Use Ferrite's vector search and WASM plugins for ML workloads".to_string(),
            },
        );
    }

    /// Analyze a Redis instance for compatibility
    pub async fn analyze(&self, source_url: &str) -> Result<CompatibilityReport> {
        // In a real implementation, this would connect to Redis and gather stats
        // For now, we create a mock analysis

        let stats = self.gather_stats(source_url).await?;
        let mut issues = Vec::new();
        let mut recommendations = Vec::new();

        // Check Redis version
        if let Some(version) = parse_version(&stats.redis_version) {
            if version.0 < 6 {
                issues.push(CompatibilityIssue {
                    code: "REDIS_VERSION".to_string(),
                    message: format!(
                        "Redis version {} is old, some features may not migrate correctly",
                        stats.redis_version
                    ),
                    severity: IssueSeverity::Warning,
                    affected_keys: None,
                    recommendation: Some(
                        "Upgrade Redis to 6.x or later before migration".to_string(),
                    ),
                });
            }
        }

        // Check cluster mode
        if stats.cluster_mode {
            issues.push(CompatibilityIssue {
                code: "CLUSTER_MODE".to_string(),
                message: "Source is running in cluster mode".to_string(),
                severity: IssueSeverity::Warning,
                affected_keys: None,
                recommendation: Some(
                    "Ferrite uses different clustering. Plan for topology changes.".to_string(),
                ),
            });
        }

        // Check data types
        for (data_type, count) in &stats.keys_by_type {
            match data_type {
                DataType::Bloom => {
                    issues.push(CompatibilityIssue {
                        code: "UNSUPPORTED_TYPE".to_string(),
                        message: format!("Found {} Bloom filter keys", count),
                        severity: IssueSeverity::Error,
                        affected_keys: Some(*count),
                        recommendation: Some(
                            "Bloom filters are not supported. Consider alternatives.".to_string(),
                        ),
                    });
                }
                DataType::Graph => {
                    if *count > 0 {
                        recommendations.push(MigrationRecommendation {
                            category: "Data Type".to_string(),
                            title: "Graph data migration".to_string(),
                            description: format!(
                                "Found {} graph keys. Ferrite's graph engine uses compatible Cypher syntax.",
                                count
                            ),
                            priority: RecommendationPriority::Medium,
                        });
                    }
                }
                _ => {}
            }
        }

        // Check memory
        let memory_gb = stats.memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        if memory_gb > 100.0 {
            recommendations.push(MigrationRecommendation {
                category: "Performance".to_string(),
                title: "Large dataset".to_string(),
                description: format!(
                    "Dataset is {:.1} GB. Consider using live migration mode to minimize downtime.",
                    memory_gb
                ),
                priority: RecommendationPriority::High,
            });
        }

        // Check for large keys
        let large_key_mb = stats.largest_key_bytes as f64 / (1024.0 * 1024.0);
        if large_key_mb > 10.0 {
            issues.push(CompatibilityIssue {
                code: "LARGE_KEY".to_string(),
                message: format!("Largest key is {:.1} MB", large_key_mb),
                severity: IssueSeverity::Warning,
                affected_keys: Some(1),
                recommendation: Some(
                    "Large keys may slow migration. Consider splitting if possible.".to_string(),
                ),
            });
        }

        // Calculate compatibility score
        let blocking_count = issues
            .iter()
            .filter(|i| i.severity == IssueSeverity::Blocking)
            .count();
        let error_count = issues
            .iter()
            .filter(|i| i.severity == IssueSeverity::Error)
            .count();
        let warning_count = issues
            .iter()
            .filter(|i| i.severity == IssueSeverity::Warning)
            .count();

        let compatibility_score = if blocking_count > 0 {
            0.0
        } else {
            let base = 100.0;
            let penalty = (error_count as f64 * 20.0) + (warning_count as f64 * 5.0);
            (base - penalty).max(0.0)
        };

        // Calculate duration before moving stats
        let estimated_duration_secs = self.estimate_duration(&stats);

        Ok(CompatibilityReport {
            source_url: source_url.to_string(),
            stats,
            issues,
            recommendations,
            compatibility_score,
            can_migrate: blocking_count == 0,
            estimated_duration_secs,
        })
    }

    /// Gather statistics from source
    async fn gather_stats(&self, _source_url: &str) -> Result<SourceStats> {
        // In a real implementation, this would connect to Redis
        // For now, return mock data
        Ok(SourceStats {
            total_keys: 100_000,
            keys_by_type: vec![
                (DataType::String, 50_000),
                (DataType::Hash, 20_000),
                (DataType::List, 15_000),
                (DataType::Set, 10_000),
                (DataType::SortedSet, 5_000),
            ]
            .into_iter()
            .collect(),
            memory_bytes: 512 * 1024 * 1024, // 512 MB
            memory_by_type: HashMap::new(),
            databases_used: 1,
            keys_with_ttl: 30_000,
            largest_key_bytes: 1024 * 1024, // 1 MB
            redis_version: "7.2.0".to_string(),
            cluster_mode: false,
            cluster_nodes: 0,
        })
    }

    /// Estimate migration duration
    fn estimate_duration(&self, stats: &SourceStats) -> u64 {
        // Rough estimate: 10000 keys/second for small values
        let keys_time = stats.total_keys / 10_000;
        // Plus time for data transfer: ~100MB/s
        let data_time = stats.memory_bytes / (100 * 1024 * 1024);
        // Add overhead
        let overhead = 60; // 1 minute overhead

        keys_time.max(data_time) + overhead
    }

    /// Get command support level
    pub fn get_command_support(&self, command: &str) -> CommandSupport {
        self.command_support
            .get(&command.to_uppercase())
            .cloned()
            .unwrap_or(CommandSupport::Unknown)
    }

    /// Get module support
    pub fn get_module_support(&self, module: &str) -> Option<&ModuleSupport> {
        self.module_support.get(module)
    }
}

impl Default for CompatibilityAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

/// Compatibility report
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompatibilityReport {
    /// Source URL analyzed
    pub source_url: String,
    /// Statistics about source data
    pub stats: SourceStats,
    /// Compatibility issues found
    pub issues: Vec<CompatibilityIssue>,
    /// Migration recommendations
    pub recommendations: Vec<MigrationRecommendation>,
    /// Overall compatibility score (0-100)
    pub compatibility_score: f64,
    /// Whether migration can proceed
    pub can_migrate: bool,
    /// Estimated migration duration in seconds
    pub estimated_duration_secs: u64,
}

/// A compatibility issue
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompatibilityIssue {
    /// Issue code
    pub code: String,
    /// Issue message
    pub message: String,
    /// Severity level
    pub severity: IssueSeverity,
    /// Number of affected keys (if applicable)
    pub affected_keys: Option<u64>,
    /// Recommended action
    pub recommendation: Option<String>,
}

/// Migration recommendation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationRecommendation {
    /// Category
    pub category: String,
    /// Title
    pub title: String,
    /// Description
    pub description: String,
    /// Priority
    pub priority: RecommendationPriority,
}

/// Recommendation priority
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecommendationPriority {
    /// Low priority
    Low,
    /// Medium priority
    Medium,
    /// High priority
    High,
    /// Critical
    Critical,
}

/// Command support level
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandSupport {
    /// Fully supported
    Full,
    /// Partially supported with notes
    Partial(String),
    /// Not supported with reason
    Unsupported(String),
    /// Unknown command
    Unknown,
}

/// Module support information
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModuleSupport {
    /// Module name
    pub name: String,
    /// Ferrite equivalent (if any)
    pub ferrite_equivalent: Option<String>,
    /// Compatibility level
    pub compatibility: ModuleCompatibility,
    /// Notes
    pub notes: String,
}

/// Module compatibility level
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModuleCompatibility {
    /// Fully compatible
    Compatible,
    /// Partially compatible
    PartiallyCompatible,
    /// Has alternative in Ferrite
    Alternative,
    /// Not supported
    NotSupported,
}

/// Parse Redis version string
fn parse_version(version: &str) -> Option<(u32, u32, u32)> {
    let parts: Vec<&str> = version.split('.').collect();
    if parts.len() >= 3 {
        let major = parts[0].parse().ok()?;
        let minor = parts[1].parse().ok()?;
        let patch = parts[2].split('-').next()?.parse().ok()?;
        Some((major, minor, patch))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn test_analyzer_creation() {
        let analyzer = CompatibilityAnalyzer::new();
        assert!(!analyzer.command_support.is_empty());
    }

    #[test]
    fn test_command_support() {
        let analyzer = CompatibilityAnalyzer::new();

        assert_eq!(analyzer.get_command_support("GET"), CommandSupport::Full);
        assert_eq!(analyzer.get_command_support("SET"), CommandSupport::Full);
        assert!(matches!(
            analyzer.get_command_support("CLUSTER"),
            CommandSupport::Unsupported(_)
        ));
    }

    #[test]
    fn test_parse_version() {
        assert_eq!(parse_version("7.2.0"), Some((7, 2, 0)));
        assert_eq!(parse_version("6.2.6-v2"), Some((6, 2, 6)));
        assert_eq!(parse_version("invalid"), None);
    }

    #[tokio::test]
    async fn test_analyze() {
        let analyzer = CompatibilityAnalyzer::new();
        let report = analyzer.analyze("redis://localhost:6379").await.unwrap();

        assert!(report.compatibility_score > 0.0);
        assert!(report.can_migrate);
    }
}
