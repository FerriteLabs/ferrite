//! # Time-Travel Queries
//!
//! Query data at any point in time. Ferrite's HybridLog architecture naturally
//! preserves history, enabling powerful temporal queries without additional storage.
//!
//! ## Why Time-Travel?
//!
//! Traditional databases only show current state. Time-travel queries enable:
//!
//! - **Debugging**: What was the value when the bug occurred?
//! - **Audit trails**: Who changed what and when?
//! - **Recovery**: Restore to any previous state
//! - **Analytics**: Track value changes over time
//! - **Compliance**: Prove historical data state for regulations
//!
//! ## Query Types
//!
//! | Query | Description | Use Case |
//! |-------|-------------|----------|
//! | AS OF | Point-in-time snapshot | Debugging, audit |
//! | HISTORY | All changes to a key | Change tracking |
//! | DIFF | Compare two points in time | Incident analysis |
//! | RESTORE | Revert to previous state | Recovery |
//!
//! ## Quick Start
//!
//! ### Point-in-Time Queries
//!
//! ```no_run
//! use ferrite::temporal::{QueryEngine, TemporalQuery, TimestampSpec};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let engine = QueryEngine::new(/* ... */);
//!
//! // What was the user's email 1 hour ago?
//! let query = TemporalQuery::AsOf {
//!     key: b"user:123:email".to_vec(),
//!     timestamp: TimestampSpec::Relative(std::time::Duration::from_secs(3600)),
//! };
//!
//! let result = engine.execute(query).await?;
//! println!("Email 1 hour ago: {:?}", result);
//! # Ok(())
//! # }
//! ```
//!
//! ### History Queries
//!
//! ```no_run
//! use ferrite::temporal::{QueryEngine, TemporalQuery, HistoryEntry};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let engine = QueryEngine::new(/* ... */);
//!
//! // Get all changes to a key
//! let query = TemporalQuery::History {
//!     key: b"order:456:status".to_vec(),
//!     limit: Some(100),
//!     offset: None,
//! };
//!
//! let history: Vec<HistoryEntry> = engine.execute_history(query).await?;
//!
//! for entry in history {
//!     println!(
//!         "[{}] {} -> {:?}",
//!         entry.timestamp,
//!         entry.operation,
//!         entry.value
//!     );
//! }
//! // Output:
//! // [2024-01-15 10:00:00] SET -> "pending"
//! // [2024-01-15 10:05:23] SET -> "processing"
//! // [2024-01-15 10:12:45] SET -> "shipped"
//! // [2024-01-15 10:30:00] SET -> "delivered"
//! # Ok(())
//! # }
//! ```
//!
//! ### Diff Queries
//!
//! ```no_run
//! use ferrite::temporal::{QueryEngine, TemporalQuery, TimestampSpec, DiffResult};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let engine = QueryEngine::new(/* ... */);
//!
//! // What changed between yesterday and now?
//! let query = TemporalQuery::Diff {
//!     pattern: b"user:123:*".to_vec(),
//!     from: TimestampSpec::Relative(std::time::Duration::from_secs(86400)),
//!     to: TimestampSpec::Now,
//! };
//!
//! let diffs: Vec<DiffResult> = engine.execute_diff(query).await?;
//!
//! for diff in diffs {
//!     println!("{}: {:?} -> {:?}", diff.key, diff.old_value, diff.new_value);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Timestamp Formats
//!
//! Multiple timestamp formats are supported:
//!
//! ```no_run
//! use ferrite::temporal::TimestampSpec;
//!
//! // Relative duration (from now)
//! let one_hour_ago = TimestampSpec::parse("-1h");      // 1 hour ago
//! let yesterday = TimestampSpec::parse("-1d");          // 1 day ago
//! let last_week = TimestampSpec::parse("-7d");          // 7 days ago
//!
//! // Unix timestamp (seconds)
//! let unix = TimestampSpec::parse("1702900000");        // Specific moment
//!
//! // Unix timestamp (milliseconds)
//! let unix_ms = TimestampSpec::parse("1702900000000");  // More precision
//!
//! // ISO8601 format
//! let iso = TimestampSpec::parse("2024-01-15T10:30:00Z");
//!
//! // Current time
//! let now = TimestampSpec::parse("NOW");
//! ```
//!
//! ### Duration Units
//!
//! | Unit | Meaning | Example |
//! |------|---------|---------|
//! | s | Seconds | -30s |
//! | m | Minutes | -15m |
//! | h | Hours | -24h |
//! | d | Days | -7d |
//! | w | Weeks | -2w |
//! | y | Years | -1y |
//!
//! ## Redis CLI Commands
//!
//! ```text
//! # Point-in-time query
//! GET user:123:email AS OF -1h
//! GET user:123:email AS OF 1702900000
//! GET user:123:email AS OF "2024-01-15T10:00:00Z"
//!
//! # History query
//! HISTORY order:456:status LIMIT 50
//! HISTORY user:123:* LIMIT 100 OFFSET 200
//!
//! # Diff between timestamps
//! DIFF user:123:* FROM -24h TO NOW
//!
//! # Restore to previous state
//! RESTORE.FROM user:123:settings -1h
//!
//! # Check temporal info
//! TEMPORAL.INFO
//! TEMPORAL.STATS user:123:*
//! ```
//!
//! ## Retention Policies
//!
//! Control how long history is kept:
//!
//! ```no_run
//! use ferrite::temporal::{TemporalConfig, RetentionPolicy, PatternOverride};
//! use std::time::Duration;
//!
//! let config = TemporalConfig {
//!     enabled: true,
//!     retention: RetentionPolicy {
//!         default_duration: Duration::from_secs(7 * 24 * 3600),  // 7 days
//!         max_versions: Some(1000),  // Max versions per key
//!     },
//!     // Pattern-specific overrides
//!     patterns: vec![
//!         PatternOverride {
//!             pattern: "audit:*".to_string(),
//!             duration: Duration::from_secs(365 * 24 * 3600),  // 1 year
//!             max_versions: None,
//!         },
//!         PatternOverride {
//!             pattern: "session:*".to_string(),
//!             duration: Duration::from_secs(24 * 3600),  // 1 day
//!             max_versions: Some(10),
//!         },
//!     ],
//!     ..Default::default()
//! };
//! ```
//!
//! ## Architecture
//!
//! Time-travel leverages HybridLog's natural versioning:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      HybridLog                              │
//! ├─────────────────────────────────────────────────────────────┤
//! │ Mutable (Hot)     │ Read-Only (Warm)   │ Disk (Cold)       │
//! │ [v5] [v4] [v3]    │ [v2] [v1]          │ [v0]              │
//! │     ◀─────────────│───────────────────│─────────────────   │
//! │            Version Chain                                    │
//! └─────────────────────────────────────────────────────────────┘
//!                            │
//!                            ▼
//!                 ┌─────────────────────┐
//!                 │   Temporal Index    │
//!                 │ key → [(ts, offset)]│
//!                 └─────────────────────┘
//! ```
//!
//! ## Performance
//!
//! | Query Type | Latency | Notes |
//! |------------|---------|-------|
//! | AS OF (recent) | <50μs | Hot tier lookup |
//! | AS OF (old) | 100-500μs | May hit disk |
//! | HISTORY (100 entries) | 1-5ms | Scans version chain |
//! | DIFF (pattern) | 10-100ms | Depends on matches |
//!
//! ## Best Practices
//!
//! 1. **Set appropriate retention**: Balance storage vs query needs
//! 2. **Use pattern overrides**: Longer retention for audit keys
//! 3. **Index hot patterns**: Frequently queried keys get dedicated indices
//! 4. **Monitor storage growth**: Time-travel increases storage requirements
//! 5. **Compact periodically**: Clean up expired versions

mod index;
mod query;
mod retention;

pub use index::{
    CompactVersionEntry, LogOffset, OperationType, TemporalIndex, TemporalStats, VersionChain,
    VersionEntry,
};
pub use query::{DiffEntry, DiffResult, HistoryEntry, QueryEngine, TemporalQuery};
pub use retention::{PatternOverride, RetentionPolicy, RetentionStats};

use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

/// Temporal configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TemporalConfig {
    /// Enable time-travel queries
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Default retention policy
    #[serde(default)]
    pub retention: RetentionPolicy,

    /// Index file path
    #[serde(default = "default_index_path")]
    pub index_path: String,

    /// Index cache size in bytes
    #[serde(default = "default_cache_size")]
    pub index_cache_size: usize,

    /// Cleanup interval in seconds
    #[serde(default = "default_cleanup_interval_secs")]
    pub cleanup_interval_secs: u64,

    /// Cleanup batch size
    #[serde(default = "default_cleanup_batch")]
    pub cleanup_batch_size: usize,

    /// Per-pattern retention overrides
    #[serde(default)]
    pub patterns: Vec<PatternOverride>,
}

fn default_enabled() -> bool {
    true
}

fn default_index_path() -> String {
    "./data/temporal.idx".to_string()
}

fn default_cache_size() -> usize {
    256 * 1024 * 1024 // 256MB
}

fn default_cleanup_interval_secs() -> u64 {
    3600 // 1 hour
}

fn default_cleanup_batch() -> usize {
    10000
}

impl Default for TemporalConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            retention: RetentionPolicy::default(),
            index_path: default_index_path(),
            index_cache_size: default_cache_size(),
            cleanup_interval_secs: default_cleanup_interval_secs(),
            cleanup_batch_size: default_cleanup_batch(),
            patterns: vec![],
        }
    }
}

impl TemporalConfig {
    /// Get the cleanup interval as a Duration
    pub fn cleanup_interval(&self) -> Duration {
        Duration::from_secs(self.cleanup_interval_secs)
    }
}

/// Timestamp specification for queries
#[derive(Clone, Debug)]
pub enum TimestampSpec {
    /// Absolute Unix timestamp (seconds)
    Unix(u64),
    /// Absolute Unix timestamp (milliseconds)
    UnixMs(u64),
    /// ISO8601 formatted string
    Iso8601(String),
    /// Relative to now (negative duration)
    Relative(Duration),
    /// Current time
    Now,
}

impl TimestampSpec {
    /// Parse a timestamp specification from string
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();

        // Check for NOW
        if s.eq_ignore_ascii_case("now") {
            return Some(TimestampSpec::Now);
        }

        // Check for relative duration (-1h, -30m, -7d)
        if let Some(rest) = s.strip_prefix('-') {
            if let Some(duration) = parse_duration(rest) {
                return Some(TimestampSpec::Relative(duration));
            }
        }

        // Try to parse as Unix timestamp
        if let Ok(ts) = s.parse::<u64>() {
            // If it looks like milliseconds (> year 2001 in ms)
            if ts > 1_000_000_000_000 {
                return Some(TimestampSpec::UnixMs(ts));
            } else {
                return Some(TimestampSpec::Unix(ts));
            }
        }

        // Try ISO8601 format
        if s.contains('T') || s.contains('-') {
            return Some(TimestampSpec::Iso8601(s.to_string()));
        }

        None
    }

    /// Convert to SystemTime
    pub fn to_system_time(&self) -> Option<SystemTime> {
        match self {
            TimestampSpec::Now => Some(SystemTime::now()),
            TimestampSpec::Unix(ts) => SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(*ts)),
            TimestampSpec::UnixMs(ts) => {
                SystemTime::UNIX_EPOCH.checked_add(Duration::from_millis(*ts))
            }
            TimestampSpec::Relative(duration) => SystemTime::now().checked_sub(*duration),
            TimestampSpec::Iso8601(s) => parse_iso8601(s),
        }
    }
}

/// Parse a duration string like "1h", "30m", "7d"
fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let (num, unit) = s.split_at(s.len() - 1);
    let value: u64 = num.parse().ok()?;

    let secs = match unit {
        "s" => value,
        "m" => value * 60,
        "h" => value * 3600,
        "d" => value * 86400,
        "w" => value * 604800,
        "y" => value * 31536000,
        _ => return None,
    };

    Some(Duration::from_secs(secs))
}

/// Parse ISO8601 timestamp (simplified)
fn parse_iso8601(s: &str) -> Option<SystemTime> {
    // Basic ISO8601 parsing: YYYY-MM-DDTHH:MM:SSZ
    // For full support, consider using chrono crate

    let s = s.trim().trim_end_matches('Z');

    // Try YYYY-MM-DDTHH:MM:SS
    if let Some((date, time)) = s.split_once('T') {
        let date_parts: Vec<&str> = date.split('-').collect();
        let time_parts: Vec<&str> = time.split(':').collect();

        if date_parts.len() == 3 && time_parts.len() >= 2 {
            let year: i32 = date_parts[0].parse().ok()?;
            let month: u32 = date_parts[1].parse().ok()?;
            let day: u32 = date_parts[2].parse().ok()?;
            let hour: u32 = time_parts[0].parse().ok()?;
            let minute: u32 = time_parts[1].parse().ok()?;
            let second: u32 = time_parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);

            // Simplified calculation - assumes UTC, no leap seconds
            let days_since_epoch =
                days_from_year(year) + days_from_month(year, month) + (day as i64 - 1) - 719162; // Days from year 1 to 1970 (matches days_from_year formula)

            let secs =
                days_since_epoch * 86400 + hour as i64 * 3600 + minute as i64 * 60 + second as i64;

            if secs >= 0 {
                return SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(secs as u64));
            }
        }
    }

    None
}

fn days_from_year(year: i32) -> i64 {
    let y = year as i64 - 1;
    365 * y + y / 4 - y / 100 + y / 400
}

fn days_from_month(year: i32, month: u32) -> i64 {
    let days = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let mut d = days.get(month as usize - 1).copied().unwrap_or(0) as i64;
    if month > 2 && is_leap_year(year) {
        d += 1;
    }
    d
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Temporal info for INFO command
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TemporalInfo {
    /// Whether temporal queries are enabled
    pub enabled: bool,
    /// Number of keys with history
    pub keys_tracked: u64,
    /// Total versions stored
    pub total_versions: u64,
    /// Index size in bytes
    pub index_size_bytes: u64,
    /// Oldest version timestamp
    pub oldest_version: Option<u64>,
    /// Newest version timestamp
    pub newest_version: Option<u64>,
    /// Retention policy
    pub retention: RetentionPolicy,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_spec_parse_unix() {
        let spec = TimestampSpec::parse("1702900000").unwrap();
        match spec {
            TimestampSpec::Unix(ts) => assert_eq!(ts, 1702900000),
            _ => panic!("Expected Unix timestamp"),
        }
    }

    #[test]
    fn test_timestamp_spec_parse_unix_ms() {
        let spec = TimestampSpec::parse("1702900000000").unwrap();
        match spec {
            TimestampSpec::UnixMs(ts) => assert_eq!(ts, 1702900000000),
            _ => panic!("Expected Unix ms timestamp"),
        }
    }

    #[test]
    fn test_timestamp_spec_parse_relative() {
        let spec = TimestampSpec::parse("-1h").unwrap();
        match spec {
            TimestampSpec::Relative(d) => assert_eq!(d, Duration::from_secs(3600)),
            _ => panic!("Expected relative timestamp"),
        }

        let spec = TimestampSpec::parse("-7d").unwrap();
        match spec {
            TimestampSpec::Relative(d) => assert_eq!(d, Duration::from_secs(7 * 86400)),
            _ => panic!("Expected relative timestamp"),
        }
    }

    #[test]
    fn test_timestamp_spec_parse_now() {
        let spec = TimestampSpec::parse("NOW").unwrap();
        match spec {
            TimestampSpec::Now => {}
            _ => panic!("Expected NOW"),
        }
    }

    #[test]
    fn test_timestamp_spec_parse_iso8601() {
        let spec = TimestampSpec::parse("2024-12-18T10:00:00Z").unwrap();
        match spec {
            TimestampSpec::Iso8601(s) => assert!(s.contains("2024")),
            _ => panic!("Expected ISO8601"),
        }
    }

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("30s"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_duration("2h"), Some(Duration::from_secs(7200)));
        assert_eq!(parse_duration("1d"), Some(Duration::from_secs(86400)));
        assert_eq!(parse_duration("1w"), Some(Duration::from_secs(604800)));
    }

    #[test]
    fn test_iso8601_to_system_time() {
        let time = parse_iso8601("2024-01-01T00:00:00Z").unwrap();
        let duration = time.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        // 2024-01-01 00:00:00 UTC = 1704067200
        assert_eq!(duration.as_secs(), 1704067200);
    }

    #[test]
    fn test_temporal_config_default() {
        let config = TemporalConfig::default();
        assert!(config.enabled);
        assert_eq!(config.cleanup_interval(), Duration::from_secs(3600));
    }
}
