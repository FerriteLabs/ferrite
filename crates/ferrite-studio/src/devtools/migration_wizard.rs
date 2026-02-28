#![forbid(unsafe_code)]
//! Redis-to-Ferrite migration wizard — compatibility checks and migration planning.

use serde::{Deserialize, Serialize};

/// Analyzes Redis compatibility and generates migration plans.
#[derive(Debug, Clone, Default)]
pub struct MigrationWizard;

/// Compatibility analysis result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatibilityReport {
    pub redis_version: String,
    pub total_commands_used: usize,
    pub compatible_commands: usize,
    pub incompatible_commands: Vec<IncompatibleCommand>,
    pub compatibility_pct: f64,
    pub warnings: Vec<String>,
    pub recommendations: Vec<String>,
    pub estimated_migration_time: String,
}

/// A command that is not compatible with Ferrite.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncompatibleCommand {
    pub name: String,
    pub reason: String,
    pub workaround: Option<String>,
}

/// A step-by-step migration plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationPlan {
    pub steps: Vec<MigrationStep>,
    pub prerequisites: Vec<String>,
    pub estimated_downtime: String,
}

/// A single step in a migration plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationStep {
    pub order: u32,
    pub description: String,
    pub command: String,
    pub risk_level: RiskLevel,
}

/// Risk level for a migration step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RiskLevel {
    Low,
    Medium,
    High,
}

impl std::fmt::Display for RiskLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Low => write!(f, "low"),
            Self::Medium => write!(f, "medium"),
            Self::High => write!(f, "high"),
        }
    }
}

/// Core compatible commands list (subset).
const COMPATIBLE_COMMANDS: &[&str] = &[
    "GET",
    "SET",
    "DEL",
    "MGET",
    "MSET",
    "INCR",
    "DECR",
    "APPEND",
    "STRLEN",
    "EXPIRE",
    "TTL",
    "PERSIST",
    "TYPE",
    "EXISTS",
    "KEYS",
    "SCAN",
    "HSET",
    "HGET",
    "HDEL",
    "HGETALL",
    "HMSET",
    "HMGET",
    "HLEN",
    "HEXISTS",
    "HKEYS",
    "HVALS",
    "LPUSH",
    "RPUSH",
    "LPOP",
    "RPOP",
    "LLEN",
    "LRANGE",
    "LINDEX",
    "SADD",
    "SREM",
    "SMEMBERS",
    "SCARD",
    "SISMEMBER",
    "SUNION",
    "SINTER",
    "SDIFF",
    "ZADD",
    "ZREM",
    "ZRANGE",
    "ZRANK",
    "ZSCORE",
    "ZCARD",
    "ZRANGEBYSCORE",
    "PING",
    "ECHO",
    "INFO",
    "DBSIZE",
    "FLUSHDB",
    "FLUSHALL",
    "SELECT",
    "AUTH",
    "SUBSCRIBE",
    "PUBLISH",
    "UNSUBSCRIBE",
    "PSUBSCRIBE",
    "PUNSUBSCRIBE",
];

/// Commands known to be incompatible or unsupported.
const KNOWN_INCOMPATIBLE: &[(&str, &str, Option<&str>)] = &[
    (
        "CLUSTER",
        "Ferrite uses its own cluster protocol",
        Some("Use Ferrite native clustering"),
    ),
    (
        "WAIT",
        "Not supported in Ferrite replication model",
        Some("Use REPLCONF for sync guarantees"),
    ),
    ("DEBUG", "Debug commands are not exposed", None),
    (
        "MODULE",
        "Redis modules are not compatible",
        Some("Check Ferrite extension crates"),
    ),
    (
        "OBJECT",
        "Object encoding details differ",
        Some("Use TYPE command instead"),
    ),
];

impl MigrationWizard {
    /// Analyze Redis INFO output for compatibility.
    #[allow(dead_code)]
    pub fn check_compatibility(redis_info: &str) -> CompatibilityReport {
        let redis_version = Self::extract_version(redis_info);
        let detected_commands = Self::detect_commands(redis_info);

        let total = detected_commands.len().max(COMPATIBLE_COMMANDS.len());
        let mut incompatible = Vec::new();

        for cmd in &detected_commands {
            let upper = cmd.to_uppercase();
            if !COMPATIBLE_COMMANDS.contains(&upper.as_str()) {
                let (reason, workaround) = KNOWN_INCOMPATIBLE
                    .iter()
                    .find(|(name, _, _)| *name == upper)
                    .map(|(_, r, w)| (r.to_string(), w.map(|s| s.to_string())))
                    .unwrap_or_else(|| ("Not yet implemented in Ferrite".to_string(), None));

                incompatible.push(IncompatibleCommand {
                    name: upper,
                    reason,
                    workaround,
                });
            }
        }

        let compatible_count = total.saturating_sub(incompatible.len());
        let compat_pct = if total > 0 {
            (compatible_count as f64 / total as f64) * 100.0
        } else {
            100.0
        };

        let mut warnings = Vec::new();
        if redis_version.starts_with("7.") {
            warnings
                .push("Redis 7.x features like FUNCTION may not be fully supported".to_string());
        }
        if !incompatible.is_empty() {
            warnings.push(format!(
                "{} command(s) require workarounds or are unsupported",
                incompatible.len()
            ));
        }

        let mut recommendations = vec![
            "Run a dry-run migration first with MIGRATE.START ... DRY-RUN".to_string(),
            "Test application compatibility in a staging environment".to_string(),
        ];
        if compat_pct < 95.0 {
            recommendations.push(
                "Review incompatible commands and implement workarounds before migration"
                    .to_string(),
            );
        }

        let estimated_time = if total > 100 {
            "30-60 minutes".to_string()
        } else {
            "5-15 minutes".to_string()
        };

        CompatibilityReport {
            redis_version,
            total_commands_used: total,
            compatible_commands: compatible_count,
            incompatible_commands: incompatible,
            compatibility_pct: compat_pct,
            warnings,
            recommendations,
            estimated_migration_time: estimated_time,
        }
    }

    /// Generate a migration plan from a compatibility report.
    #[allow(dead_code)]
    pub fn generate_migration_plan(report: &CompatibilityReport) -> MigrationPlan {
        let mut steps = vec![
            MigrationStep {
                order: 1,
                description: "Verify Ferrite is running and accessible".to_string(),
                command: "PING".to_string(),
                risk_level: RiskLevel::Low,
            },
            MigrationStep {
                order: 2,
                description: "Perform dry-run migration to detect issues".to_string(),
                command: format!("MIGRATE.START redis://source:6379 DRY-RUN VERIFY"),
                risk_level: RiskLevel::Low,
            },
            MigrationStep {
                order: 3,
                description: "Start live migration with verification".to_string(),
                command: "MIGRATE.START redis://source:6379 BATCH 1000 WORKERS 4 VERIFY"
                    .to_string(),
                risk_level: RiskLevel::Medium,
            },
            MigrationStep {
                order: 4,
                description: "Monitor migration progress".to_string(),
                command: "MIGRATE.STATUS".to_string(),
                risk_level: RiskLevel::Low,
            },
            MigrationStep {
                order: 5,
                description: "Verify data integrity after migration".to_string(),
                command: "MIGRATE.VERIFY SAMPLE 10".to_string(),
                risk_level: RiskLevel::Low,
            },
            MigrationStep {
                order: 6,
                description: "Cutover to Ferrite as primary".to_string(),
                command: "MIGRATE.CUTOVER".to_string(),
                risk_level: RiskLevel::High,
            },
        ];

        if !report.incompatible_commands.is_empty() {
            steps.insert(
                2,
                MigrationStep {
                    order: 0, // will renumber
                    description: format!(
                        "Address {} incompatible commands before proceeding",
                        report.incompatible_commands.len()
                    ),
                    command: "STUDIO.COMPAT".to_string(),
                    risk_level: RiskLevel::Medium,
                },
            );
            // Renumber
            for (i, step) in steps.iter_mut().enumerate() {
                step.order = (i + 1) as u32;
            }
        }

        let prerequisites = vec![
            "Ferrite instance running and accepting connections".to_string(),
            "Network connectivity between Redis source and Ferrite target".to_string(),
            format!("Redis version: {} detected", report.redis_version),
        ];

        let downtime = if report.compatibility_pct >= 99.0 {
            "Near-zero (live migration)".to_string()
        } else if report.compatibility_pct >= 90.0 {
            "< 5 minutes (brief cutover)".to_string()
        } else {
            "15-30 minutes (requires application changes)".to_string()
        };

        MigrationPlan {
            steps,
            prerequisites,
            estimated_downtime: downtime,
        }
    }

    fn extract_version(info: &str) -> String {
        for line in info.lines() {
            let trimmed = line.trim();
            if let Some(ver) = trimmed.strip_prefix("redis_version:") {
                return ver.trim().to_string();
            }
        }
        "unknown".to_string()
    }

    fn detect_commands(info: &str) -> Vec<String> {
        let mut cmds = Vec::new();
        for line in info.lines() {
            let trimmed = line.trim();
            // Redis INFO commandstats format: cmdstat_<cmd>:calls=N,...
            if let Some(rest) = trimmed.strip_prefix("cmdstat_") {
                if let Some(idx) = rest.find(':') {
                    cmds.push(rest[..idx].to_uppercase());
                }
            }
        }
        if cmds.is_empty() {
            // Fallback: assume standard commands
            cmds = COMPATIBLE_COMMANDS.iter().map(|s| s.to_string()).collect();
        }
        cmds
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_compatibility_basic() {
        let info = "redis_version:6.2.14\r\n# Commandstats\r\n";
        let report = MigrationWizard::check_compatibility(info);
        assert_eq!(report.redis_version, "6.2.14");
        assert!(report.compatibility_pct > 0.0);
    }

    #[test]
    fn test_check_compatibility_with_stats() {
        let info = "\
redis_version:7.0.0\r\n\
# Commandstats\r\n\
cmdstat_get:calls=1000,usec=5000\r\n\
cmdstat_set:calls=500,usec=2500\r\n\
cmdstat_cluster:calls=10,usec=50\r\n";
        let report = MigrationWizard::check_compatibility(info);
        assert_eq!(report.redis_version, "7.0.0");
        assert!(!report.incompatible_commands.is_empty());
        let cluster_cmd = report
            .incompatible_commands
            .iter()
            .find(|c| c.name == "CLUSTER");
        assert!(cluster_cmd.is_some());
        assert!(cluster_cmd.unwrap().workaround.is_some());
    }

    #[test]
    fn test_generate_migration_plan() {
        let report = CompatibilityReport {
            redis_version: "6.2.14".to_string(),
            total_commands_used: 50,
            compatible_commands: 50,
            incompatible_commands: vec![],
            compatibility_pct: 100.0,
            warnings: vec![],
            recommendations: vec![],
            estimated_migration_time: "5-15 minutes".to_string(),
        };
        let plan = MigrationWizard::generate_migration_plan(&report);
        assert!(!plan.steps.is_empty());
        assert!(!plan.prerequisites.is_empty());
        assert_eq!(plan.estimated_downtime, "Near-zero (live migration)");
    }

    #[test]
    fn test_extract_version_missing() {
        assert_eq!(
            MigrationWizard::extract_version("no version here"),
            "unknown"
        );
    }

    #[test]
    fn test_risk_level_display() {
        assert_eq!(RiskLevel::Low.to_string(), "low");
        assert_eq!(RiskLevel::Medium.to_string(), "medium");
        assert_eq!(RiskLevel::High.to_string(), "high");
    }
}
