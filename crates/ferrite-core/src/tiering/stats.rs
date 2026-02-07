//! Access statistics for tiering decisions
//!
//! This module tracks per-key access patterns to enable intelligent
//! tiering decisions based on actual usage.

use serde::{Deserialize, Serialize};

/// Storage tier identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum StorageTier {
    /// In-memory storage - fastest, most expensive
    Memory,
    /// Memory-mapped files - read-only, warm tier
    Mmap,
    /// Local SSD storage
    Ssd,
    /// Cloud object storage (S3/GCS/Azure)
    Cloud,
    /// Archive storage (Glacier)
    Archive,
}

impl StorageTier {
    /// Get all storage tiers in order from fastest to slowest
    pub fn all() -> &'static [StorageTier] {
        &[
            StorageTier::Memory,
            StorageTier::Mmap,
            StorageTier::Ssd,
            StorageTier::Cloud,
            StorageTier::Archive,
        ]
    }

    /// Get display name for the tier
    pub fn name(&self) -> &'static str {
        match self {
            StorageTier::Memory => "memory",
            StorageTier::Mmap => "mmap",
            StorageTier::Ssd => "ssd",
            StorageTier::Cloud => "cloud",
            StorageTier::Archive => "archive",
        }
    }

    /// Parse tier from string
    pub fn parse_str(s: &str) -> Option<StorageTier> {
        match s.to_lowercase().as_str() {
            "memory" | "mem" => Some(StorageTier::Memory),
            "mmap" => Some(StorageTier::Mmap),
            "ssd" | "disk" => Some(StorageTier::Ssd),
            "cloud" | "s3" | "gcs" | "azure" => Some(StorageTier::Cloud),
            "archive" | "glacier" => Some(StorageTier::Archive),
            _ => None,
        }
    }

    /// Check if this tier is slower than another
    pub fn is_colder_than(&self, other: &StorageTier) -> bool {
        *self > *other
    }

    /// Check if this tier is faster than another
    pub fn is_hotter_than(&self, other: &StorageTier) -> bool {
        *self < *other
    }
}

impl std::fmt::Display for StorageTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Key access priority for tiering decisions
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Priority {
    /// Never tier down - always in memory
    Critical,
    /// Prefer memory, reluctant to tier
    High,
    /// Normal tiering behavior (default)
    #[default]
    Normal,
    /// Eager to tier to cheaper storage
    Low,
    /// Archive as soon as possible
    Archive,
}

impl Priority {
    /// Parse priority from string
    pub fn parse_str(s: &str) -> Option<Priority> {
        match s.to_lowercase().as_str() {
            "critical" => Some(Priority::Critical),
            "high" => Some(Priority::High),
            "normal" => Some(Priority::Normal),
            "low" => Some(Priority::Low),
            "archive" => Some(Priority::Archive),
            _ => None,
        }
    }

    /// Get display name
    pub fn name(&self) -> &'static str {
        match self {
            Priority::Critical => "critical",
            Priority::High => "high",
            Priority::Normal => "normal",
            Priority::Low => "low",
            Priority::Archive => "archive",
        }
    }
}

impl std::fmt::Display for Priority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Rolling access counts for different time windows
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AccessCounts {
    /// Reads in last minute
    pub reads_1m: u32,
    /// Reads in last hour
    pub reads_1h: u32,
    /// Reads in last day
    pub reads_1d: u32,
    /// Writes in last minute
    pub writes_1m: u32,
    /// Writes in last hour
    pub writes_1h: u32,
    /// Writes in last day
    pub writes_1d: u32,
}

impl AccessCounts {
    /// Create new empty access counts
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a read access
    pub fn record_read(&mut self) {
        self.reads_1m = self.reads_1m.saturating_add(1);
        self.reads_1h = self.reads_1h.saturating_add(1);
        self.reads_1d = self.reads_1d.saturating_add(1);
    }

    /// Record a write access
    pub fn record_write(&mut self) {
        self.writes_1m = self.writes_1m.saturating_add(1);
        self.writes_1h = self.writes_1h.saturating_add(1);
        self.writes_1d = self.writes_1d.saturating_add(1);
    }

    /// Roll over minute window (call every minute)
    pub fn rollover_minute(&mut self) {
        self.reads_1m = 0;
        self.writes_1m = 0;
    }

    /// Roll over hour window (call every hour)
    pub fn rollover_hour(&mut self) {
        self.reads_1h = 0;
        self.writes_1h = 0;
    }

    /// Roll over day window (call every day)
    pub fn rollover_day(&mut self) {
        self.reads_1d = 0;
        self.writes_1d = 0;
    }

    /// Decay counts (reduce by factor, used for aging)
    pub fn decay(&mut self, factor: f64) {
        self.reads_1m = (self.reads_1m as f64 * factor) as u32;
        self.reads_1h = (self.reads_1h as f64 * factor) as u32;
        self.reads_1d = (self.reads_1d as f64 * factor) as u32;
        self.writes_1m = (self.writes_1m as f64 * factor) as u32;
        self.writes_1h = (self.writes_1h as f64 * factor) as u32;
        self.writes_1d = (self.writes_1d as f64 * factor) as u32;
    }

    /// Get total reads across all windows
    pub fn total_reads(&self) -> u32 {
        self.reads_1d
    }

    /// Get total writes across all windows
    pub fn total_writes(&self) -> u32 {
        self.writes_1d
    }

    /// Get access rate (reads per hour based on daily counts)
    pub fn reads_per_hour(&self) -> f64 {
        self.reads_1d as f64 / 24.0
    }

    /// Get write rate (writes per hour based on daily counts)
    pub fn writes_per_hour(&self) -> f64 {
        self.writes_1d as f64 / 24.0
    }
}

/// Access statistics for a single key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyAccessStats {
    /// Key hash for quick lookup
    pub key_hash: u64,
    /// Key size in bytes
    pub size: u32,
    /// Current storage tier
    pub tier: StorageTier,
    /// Access counts per time window
    pub access_counts: AccessCounts,
    /// Last access timestamp (seconds since engine start)
    pub last_access_secs: u64,
    /// Last write timestamp (seconds since engine start)
    pub last_write_secs: u64,
    /// Custom priority (user-defined)
    pub priority: Priority,
    /// Whether the key is pinned to a specific tier
    pub pinned: bool,
}

impl KeyAccessStats {
    /// Create new stats for a key
    pub fn new(size: u32, tier: StorageTier) -> Self {
        Self {
            key_hash: 0,
            size,
            tier,
            access_counts: AccessCounts::new(),
            last_access_secs: 0,
            last_write_secs: 0,
            priority: Priority::Normal,
            pinned: false,
        }
    }

    /// Create stats with key hash
    pub fn with_hash(key_hash: u64, size: u32, tier: StorageTier) -> Self {
        Self {
            key_hash,
            size,
            tier,
            access_counts: AccessCounts::new(),
            last_access_secs: 0,
            last_write_secs: 0,
            priority: Priority::Normal,
            pinned: false,
        }
    }

    /// Update size (e.g., after append operation)
    pub fn update_size(&mut self, new_size: u32) {
        self.size = new_size;
    }

    /// Record a read access
    pub fn record_read(&mut self, current_secs: u64) {
        self.access_counts.record_read();
        self.last_access_secs = current_secs;
    }

    /// Record a write access
    pub fn record_write(&mut self, current_secs: u64) {
        self.access_counts.record_write();
        self.last_access_secs = current_secs;
        self.last_write_secs = current_secs;
    }

    /// Get age since last access in seconds
    pub fn age_secs(&self, current_secs: u64) -> u64 {
        current_secs.saturating_sub(self.last_access_secs)
    }

    /// Get size in GB
    pub fn size_gb(&self) -> f64 {
        self.size as f64 / (1024.0 * 1024.0 * 1024.0)
    }

    /// Get monthly read count (extrapolated from daily)
    pub fn monthly_reads(&self) -> f64 {
        self.access_counts.reads_1d as f64 * 30.0
    }

    /// Get monthly write count (extrapolated from daily)
    pub fn monthly_writes(&self) -> f64 {
        self.access_counts.writes_1d as f64 * 30.0
    }

    /// Check if key is idle (no access in given seconds)
    pub fn is_idle(&self, current_secs: u64, idle_threshold_secs: u64) -> bool {
        self.age_secs(current_secs) > idle_threshold_secs
    }

    /// Check if key should be promoted based on access rate
    pub fn should_promote(&self, threshold_reads_per_hour: f64) -> bool {
        self.access_counts.reads_per_hour() > threshold_reads_per_hour
    }

    /// Check if key should be demoted based on access rate
    pub fn should_demote(&self, threshold_reads_per_hour: f64) -> bool {
        self.access_counts.reads_per_hour() < threshold_reads_per_hour
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_tier_ordering() {
        assert!(StorageTier::Memory < StorageTier::Mmap);
        assert!(StorageTier::Mmap < StorageTier::Ssd);
        assert!(StorageTier::Ssd < StorageTier::Cloud);
        assert!(StorageTier::Cloud < StorageTier::Archive);
    }

    #[test]
    fn test_storage_tier_from_str() {
        assert_eq!(StorageTier::parse_str("memory"), Some(StorageTier::Memory));
        assert_eq!(StorageTier::parse_str("MEM"), Some(StorageTier::Memory));
        assert_eq!(StorageTier::parse_str("s3"), Some(StorageTier::Cloud));
        assert_eq!(StorageTier::parse_str("invalid"), None);
    }

    #[test]
    fn test_access_counts() {
        let mut counts = AccessCounts::new();

        counts.record_read();
        counts.record_read();
        counts.record_write();

        assert_eq!(counts.reads_1m, 2);
        assert_eq!(counts.writes_1m, 1);
        assert_eq!(counts.total_reads(), 2);
        assert_eq!(counts.total_writes(), 1);
    }

    #[test]
    fn test_access_counts_rollover() {
        let mut counts = AccessCounts::new();
        counts.record_read();
        counts.record_write();

        counts.rollover_minute();
        assert_eq!(counts.reads_1m, 0);
        assert_eq!(counts.reads_1h, 1); // Still in hour window
    }

    #[test]
    fn test_key_access_stats() {
        let mut stats = KeyAccessStats::new(1024, StorageTier::Memory);

        stats.record_read(100);
        stats.record_write(150);

        assert_eq!(stats.last_access_secs, 150);
        assert_eq!(stats.last_write_secs, 150);
        assert_eq!(stats.access_counts.reads_1d, 1);
        assert_eq!(stats.access_counts.writes_1d, 1);
    }

    #[test]
    fn test_priority_ordering() {
        assert!(Priority::Critical < Priority::High);
        assert!(Priority::High < Priority::Normal);
        assert!(Priority::Normal < Priority::Low);
        assert!(Priority::Low < Priority::Archive);
    }
}
