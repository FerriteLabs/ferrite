//! Retention Policy Management
//!
//! Manages data retention with tiered policies for automatic data lifecycle.

use super::sample::Timestamp;
use super::storage::TimeSeriesStorage;
use super::*;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::time::Duration;

/// Retention policy for time series data
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    /// Raw data retention duration
    raw_retention: Duration,
    /// Tiers for downsampled data
    tiers: Vec<RetentionTier>,
    /// Delete or archive expired data
    action: RetentionAction,
    /// Apply to specific metrics (empty = all)
    metric_patterns: Vec<String>,
    /// Maximum number of samples to retain per series
    max_samples: Option<usize>,
    /// Chunk size for time-window based storage
    chunk_size: usize,
}

impl RetentionPolicy {
    /// Create a simple retention policy with duration
    pub fn new(duration: Duration) -> Self {
        Self {
            raw_retention: duration,
            tiers: Vec::new(),
            action: RetentionAction::Delete,
            metric_patterns: Vec::new(),
            max_samples: None,
            chunk_size: 1024,
        }
    }

    /// Create a builder for complex policies
    pub fn builder() -> RetentionPolicyBuilder {
        RetentionPolicyBuilder::new()
    }

    /// Get raw retention duration
    pub fn duration(&self) -> Duration {
        self.raw_retention
    }

    /// Get retention tiers
    pub fn tiers(&self) -> &[RetentionTier] {
        &self.tiers
    }

    /// Get retention action
    pub fn action(&self) -> RetentionAction {
        self.action
    }

    /// Check if policy applies to a metric
    pub fn applies_to(&self, metric: &str) -> bool {
        if self.metric_patterns.is_empty() {
            return true;
        }

        for pattern in &self.metric_patterns {
            if pattern.ends_with('*') {
                let prefix = &pattern[..pattern.len() - 1];
                if metric.starts_with(prefix) {
                    return true;
                }
            } else if pattern == metric {
                return true;
            }
        }

        false
    }

    /// Check if a timestamp is expired according to this policy
    pub fn is_expired(&self, timestamp: Timestamp) -> bool {
        let now = Timestamp::now();
        let age = now.diff(&timestamp);
        age > self.raw_retention
    }

    /// Get the appropriate tier for a given age
    pub fn get_tier_for_age(&self, age: Duration) -> Option<&RetentionTier> {
        self.tiers.iter().find(|tier| age >= tier.after)
    }

    /// Get maximum samples limit
    pub fn max_samples(&self) -> Option<usize> {
        self.max_samples
    }

    /// Get chunk size
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    /// Check if a sample count exceeds the max_samples limit
    pub fn exceeds_max_samples(&self, count: usize) -> bool {
        self.max_samples.is_some_and(|max| count >= max)
    }
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self::new(Duration::from_secs(86400 * 30)) // 30 days default
    }
}

/// Builder for retention policies
#[derive(Debug, Default)]
pub struct RetentionPolicyBuilder {
    raw_retention: Option<Duration>,
    tiers: Vec<RetentionTier>,
    action: RetentionAction,
    metric_patterns: Vec<String>,
    max_samples: Option<usize>,
    chunk_size: Option<usize>,
}

impl RetentionPolicyBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Set raw data retention
    pub fn raw_retention(mut self, duration: Duration) -> Self {
        self.raw_retention = Some(duration);
        self
    }

    /// Add a retention tier
    pub fn add_tier(mut self, tier: RetentionTier) -> Self {
        self.tiers.push(tier);
        self
    }

    /// Set retention action
    pub fn action(mut self, action: RetentionAction) -> Self {
        self.action = action;
        self
    }

    /// Add metric pattern
    pub fn for_metric(mut self, pattern: impl Into<String>) -> Self {
        self.metric_patterns.push(pattern.into());
        self
    }

    /// Set maximum samples per series
    pub fn max_samples(mut self, max: usize) -> Self {
        self.max_samples = Some(max);
        self
    }

    /// Set chunk size
    pub fn chunk_size(mut self, size: usize) -> Self {
        self.chunk_size = Some(size);
        self
    }

    /// Build the policy
    pub fn build(self) -> RetentionPolicy {
        // Sort tiers by age (descending)
        let mut tiers = self.tiers;
        tiers.sort_by(|a, b| b.after.cmp(&a.after));

        RetentionPolicy {
            raw_retention: self.raw_retention.unwrap_or(Duration::from_secs(86400 * 7)),
            tiers,
            action: self.action,
            metric_patterns: self.metric_patterns,
            max_samples: self.max_samples,
            chunk_size: self.chunk_size.unwrap_or(1024),
        }
    }
}

/// A retention tier with downsampling
#[derive(Debug, Clone)]
pub struct RetentionTier {
    /// Name of the tier
    pub name: String,
    /// Start applying after this age
    pub after: Duration,
    /// Keep data for this duration
    pub keep_for: Duration,
    /// Downsample interval
    pub downsample_interval: Duration,
    /// Aggregation for downsampling
    pub aggregation: super::aggregation::Aggregation,
}

impl RetentionTier {
    /// Create a new retention tier
    pub fn new(
        name: impl Into<String>,
        after: Duration,
        keep_for: Duration,
        downsample_interval: Duration,
    ) -> Self {
        Self {
            name: name.into(),
            after,
            keep_for,
            downsample_interval,
            aggregation: super::aggregation::Aggregation::Avg,
        }
    }

    /// Set aggregation
    pub fn with_aggregation(mut self, agg: super::aggregation::Aggregation) -> Self {
        self.aggregation = agg;
        self
    }

    /// Check if timestamp falls in this tier
    pub fn contains(&self, timestamp: Timestamp) -> bool {
        let now = Timestamp::now();
        let age = now.diff(&timestamp);
        age >= self.after && age < (self.after + self.keep_for)
    }
}

/// Action to take on expired data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RetentionAction {
    /// Delete expired data
    #[default]
    Delete,
    /// Archive to cold storage
    Archive,
    /// Move to different tier
    Downsample,
}

/// Manages retention policies and cleanup
pub struct RetentionManager {
    /// Default policy
    default_policy: RetentionPolicy,
    /// Per-metric policies
    policies: RwLock<HashMap<String, RetentionPolicy>>,
    /// Cleanup statistics
    stats: RetentionStats,
}

/// Retention cleanup statistics
#[derive(Debug, Default)]
pub struct RetentionStats {
    /// Samples deleted
    pub samples_deleted: std::sync::atomic::AtomicU64,
    /// Samples downsampled
    pub samples_downsampled: std::sync::atomic::AtomicU64,
    /// Samples archived
    pub samples_archived: std::sync::atomic::AtomicU64,
    /// Last cleanup time
    pub last_cleanup: std::sync::atomic::AtomicI64,
    /// Cleanup runs
    pub cleanup_runs: std::sync::atomic::AtomicU64,
}

impl RetentionManager {
    /// Create a new retention manager
    pub fn new(default_policy: RetentionPolicy) -> Self {
        Self {
            default_policy,
            policies: RwLock::new(HashMap::new()),
            stats: RetentionStats::default(),
        }
    }

    /// Set policy for a metric
    pub fn set_policy(&self, metric: &str, policy: RetentionPolicy) {
        self.policies.write().insert(metric.to_string(), policy);
    }

    /// Get policy for a metric
    pub fn get_policy(&self, metric: &str) -> RetentionPolicy {
        self.policies
            .read()
            .get(metric)
            .cloned()
            .unwrap_or_else(|| self.default_policy.clone())
    }

    /// Remove policy for a metric
    pub fn remove_policy(&self, metric: &str) {
        self.policies.write().remove(metric);
    }

    /// Run cleanup on storage
    pub fn run_cleanup(&self, storage: &TimeSeriesStorage) -> Result<u64> {
        let now = Timestamp::now();
        let mut samples_deleted = 0u64;
        let mut samples_archived = 0u64;

        tracing::debug!("Running retention cleanup");

        // Iterate through all series
        for (id, series) in storage.all_series() {
            let metric = id.metric();
            let policy = self.get_policy(metric);

            // Calculate cutoff timestamp based on retention duration
            let cutoff = now.sub(policy.raw_retention);

            match policy.action {
                RetentionAction::Delete => {
                    // Simply remove expired samples
                    let removed = series.remove_before(cutoff);
                    samples_deleted += removed as u64;
                }
                RetentionAction::Archive => {
                    // Drain samples for archiving (caller would handle actual archiving)
                    let archived = series.drain_before(cutoff);
                    samples_archived += archived.len() as u64;
                    tracing::debug!(
                        "Archived {} samples from {} for cold storage",
                        archived.len(),
                        metric
                    );
                }
                RetentionAction::Downsample => {
                    if let Some(tier) = policy.get_tier_for_age(now.diff(&cutoff)) {
                        let tier_cutoff = now.sub(tier.after);
                        let samples_to_downsample = series.drain_before(tier_cutoff);

                        if !samples_to_downsample.is_empty() {
                            tracing::debug!(
                                "Downsampled {} samples from {} using {} interval",
                                samples_to_downsample.len(),
                                metric,
                                tier.downsample_interval.as_secs()
                            );
                            samples_deleted += samples_to_downsample.len() as u64;
                        }
                    }
                }
            }

            // Enforce max_samples: trim oldest samples if over limit
            if let Some(max) = policy.max_samples() {
                let current_len = series.len();
                if current_len > max {
                    let excess = current_len - max;
                    series.trim_oldest(excess);
                    samples_deleted += excess as u64;
                }
            }
        }

        // Update storage metrics
        storage.update_samples_metric(samples_deleted + samples_archived);

        // Update retention stats
        self.stats
            .cleanup_runs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.stats
            .last_cleanup
            .store(now.as_nanos(), std::sync::atomic::Ordering::Relaxed);
        self.stats
            .samples_deleted
            .fetch_add(samples_deleted, std::sync::atomic::Ordering::Relaxed);
        self.stats
            .samples_archived
            .fetch_add(samples_archived, std::sync::atomic::Ordering::Relaxed);

        Ok(samples_deleted + samples_archived)
    }

    /// Check if a sample should be retained
    pub fn should_retain(&self, metric: &str, timestamp: Timestamp) -> bool {
        let policy = self.get_policy(metric);
        !policy.is_expired(timestamp)
    }

    /// Get the tier a sample should be in
    pub fn get_tier(&self, metric: &str, timestamp: Timestamp) -> Option<RetentionTier> {
        let policy = self.get_policy(metric);
        let now = Timestamp::now();
        let age = now.diff(&timestamp);
        policy.get_tier_for_age(age).cloned()
    }

    /// Get cleanup statistics
    pub fn stats(&self) -> &RetentionStats {
        &self.stats
    }

    /// Get all configured policies
    pub fn list_policies(&self) -> Vec<(String, RetentionPolicy)> {
        self.policies
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

impl Default for RetentionManager {
    fn default() -> Self {
        Self::new(RetentionPolicy::default())
    }
}

/// Preset retention policies for common use cases
pub mod presets {
    use super::*;

    /// Short-term metrics (e.g., real-time dashboards)
    pub fn short_term() -> RetentionPolicy {
        RetentionPolicy::builder()
            .raw_retention(Duration::from_secs(86400 * 7)) // 7 days
            .build()
    }

    /// Medium-term metrics (e.g., weekly/monthly reports)
    pub fn medium_term() -> RetentionPolicy {
        RetentionPolicy::builder()
            .raw_retention(Duration::from_secs(86400 * 30)) // 30 days
            .add_tier(RetentionTier::new(
                "hourly",
                Duration::from_secs(86400 * 7),  // After 7 days
                Duration::from_secs(86400 * 23), // Keep for 23 days
                Duration::from_secs(3600),       // Hourly aggregation
            ))
            .build()
    }

    /// Long-term metrics (e.g., capacity planning)
    pub fn long_term() -> RetentionPolicy {
        RetentionPolicy::builder()
            .raw_retention(Duration::from_secs(86400 * 7)) // 7 days raw
            .add_tier(RetentionTier::new(
                "hourly",
                Duration::from_secs(86400 * 7),  // After 7 days
                Duration::from_secs(86400 * 23), // Keep for 23 days
                Duration::from_secs(3600),       // Hourly
            ))
            .add_tier(RetentionTier::new(
                "daily",
                Duration::from_secs(86400 * 30),  // After 30 days
                Duration::from_secs(86400 * 335), // Keep for ~11 months
                Duration::from_secs(86400),       // Daily
            ))
            .build()
    }

    /// Forever retention with aggressive downsampling
    pub fn forever() -> RetentionPolicy {
        RetentionPolicy::builder()
            .raw_retention(Duration::from_secs(86400 * 7)) // 7 days raw
            .add_tier(RetentionTier::new(
                "hourly",
                Duration::from_secs(86400 * 7),
                Duration::from_secs(86400 * 23),
                Duration::from_secs(3600),
            ))
            .add_tier(RetentionTier::new(
                "daily",
                Duration::from_secs(86400 * 30),
                Duration::from_secs(86400 * 335),
                Duration::from_secs(86400),
            ))
            .add_tier(RetentionTier::new(
                "weekly",
                Duration::from_secs(86400 * 365),
                Duration::from_secs(86400 * 365 * 10), // 10 years
                Duration::from_secs(86400 * 7),        // Weekly
            ))
            .action(RetentionAction::Archive)
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retention_policy_creation() {
        let policy = RetentionPolicy::new(Duration::from_secs(86400));
        assert_eq!(policy.duration(), Duration::from_secs(86400));
    }

    #[test]
    fn test_retention_policy_builder() {
        let policy = RetentionPolicy::builder()
            .raw_retention(Duration::from_secs(86400 * 7))
            .add_tier(RetentionTier::new(
                "hourly",
                Duration::from_secs(86400),
                Duration::from_secs(86400 * 6),
                Duration::from_secs(3600),
            ))
            .action(RetentionAction::Archive)
            .build();

        assert_eq!(policy.duration(), Duration::from_secs(86400 * 7));
        assert_eq!(policy.tiers().len(), 1);
        assert_eq!(policy.action(), RetentionAction::Archive);
    }

    #[test]
    fn test_retention_policy_applies_to() {
        let policy = RetentionPolicy::builder()
            .raw_retention(Duration::from_secs(86400))
            .for_metric("cpu.*")
            .for_metric("memory.usage")
            .build();

        assert!(policy.applies_to("cpu.user"));
        assert!(policy.applies_to("cpu.system"));
        assert!(policy.applies_to("memory.usage"));
        assert!(!policy.applies_to("memory.free"));
        assert!(!policy.applies_to("disk.usage"));
    }

    #[test]
    fn test_retention_tier() {
        let tier = RetentionTier::new(
            "hourly",
            Duration::from_secs(86400 * 7),
            Duration::from_secs(86400 * 23),
            Duration::from_secs(3600),
        );

        assert_eq!(tier.name, "hourly");
        assert_eq!(tier.downsample_interval, Duration::from_secs(3600));
    }

    #[test]
    fn test_retention_manager() {
        let manager = RetentionManager::new(RetentionPolicy::new(Duration::from_secs(86400)));

        manager.set_policy(
            "cpu.usage",
            RetentionPolicy::new(Duration::from_secs(86400 * 30)),
        );

        let cpu_policy = manager.get_policy("cpu.usage");
        assert_eq!(cpu_policy.duration(), Duration::from_secs(86400 * 30));

        let other_policy = manager.get_policy("memory.usage");
        assert_eq!(other_policy.duration(), Duration::from_secs(86400));
    }

    #[test]
    fn test_presets() {
        let short = presets::short_term();
        assert_eq!(short.duration(), Duration::from_secs(86400 * 7));

        let medium = presets::medium_term();
        assert_eq!(medium.tiers().len(), 1);

        let long = presets::long_term();
        assert_eq!(long.tiers().len(), 2);

        let forever = presets::forever();
        assert_eq!(forever.tiers().len(), 3);
        assert_eq!(forever.action(), RetentionAction::Archive);
    }

    #[test]
    fn test_get_tier_for_age() {
        let policy = RetentionPolicy::builder()
            .raw_retention(Duration::from_secs(86400))
            .add_tier(RetentionTier::new(
                "hourly",
                Duration::from_secs(86400),
                Duration::from_secs(86400 * 6),
                Duration::from_secs(3600),
            ))
            .add_tier(RetentionTier::new(
                "daily",
                Duration::from_secs(86400 * 7),
                Duration::from_secs(86400 * 23),
                Duration::from_secs(86400),
            ))
            .build();

        // Fresh data - no tier
        let tier = policy.get_tier_for_age(Duration::from_secs(3600));
        assert!(tier.is_none());

        // 2 days old - hourly tier
        let tier = policy.get_tier_for_age(Duration::from_secs(86400 * 2));
        assert!(tier.is_some());
        assert_eq!(tier.unwrap().name, "hourly");

        // 10 days old - daily tier
        let tier = policy.get_tier_for_age(Duration::from_secs(86400 * 10));
        assert!(tier.is_some());
        assert_eq!(tier.unwrap().name, "daily");
    }

    #[test]
    fn test_run_cleanup() {
        use super::super::sample::Sample;
        use super::super::series::TimeSeriesId;
        use super::super::storage::{StorageConfig, TimeSeriesStorage};

        // Create storage with samples
        let config = StorageConfig::default();
        let storage = TimeSeriesStorage::new(config).unwrap();

        // Add some old samples (1 day ago) and some fresh samples
        let id = TimeSeriesId::from_metric("test.metric");
        let now = Timestamp::now();
        let one_day_ago = now.sub(Duration::from_secs(86400 + 100)); // slightly older than 1 day
        let one_hour_ago = now.sub(Duration::from_secs(3600));

        // Add old sample
        storage
            .add_sample(&id, Sample::new(one_day_ago, 10.0))
            .unwrap();
        // Add fresh sample
        storage
            .add_sample(&id, Sample::new(one_hour_ago, 20.0))
            .unwrap();

        // Create retention manager with 1-day retention
        let manager = RetentionManager::new(RetentionPolicy::new(Duration::from_secs(86400)));

        // Run cleanup
        let cleaned = manager.run_cleanup(&storage).unwrap();

        // Should have cleaned 1 sample (the old one)
        assert_eq!(cleaned, 1);

        // Verify stats were updated
        assert_eq!(
            manager
                .stats()
                .samples_deleted
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            manager
                .stats()
                .cleanup_runs
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );

        // The series should still have 1 sample (the fresh one)
        let series = storage.get_series(&id).unwrap();
        assert_eq!(series.len(), 1);
    }

    #[test]
    fn test_max_samples_policy() {
        let policy = RetentionPolicy::builder()
            .raw_retention(Duration::from_secs(86400 * 30))
            .max_samples(100)
            .build();

        assert_eq!(policy.max_samples(), Some(100));
        assert!(policy.exceeds_max_samples(100));
        assert!(policy.exceeds_max_samples(200));
        assert!(!policy.exceeds_max_samples(50));
    }

    #[test]
    fn test_chunk_size_policy() {
        let policy = RetentionPolicy::builder()
            .raw_retention(Duration::from_secs(86400))
            .chunk_size(2048)
            .build();

        assert_eq!(policy.chunk_size(), 2048);
    }

    #[test]
    fn test_max_samples_enforcement() {
        use super::super::sample::Sample;
        use super::super::series::TimeSeriesId;
        use super::super::storage::{StorageConfig, TimeSeriesStorage};

        let config = StorageConfig::default();
        let storage = TimeSeriesStorage::new(config).unwrap();

        let id = TimeSeriesId::from_metric("test.metric");
        let now = Timestamp::now();

        // Add 20 fresh samples
        for i in 0..20 {
            let ts = now.sub(Duration::from_secs(i));
            storage.add_sample(&id, Sample::new(ts, i as f64)).unwrap();
        }

        // Set policy with max 10 samples
        let policy = RetentionPolicy::builder()
            .raw_retention(Duration::from_secs(86400 * 30)) // long retention
            .max_samples(10)
            .build();

        let manager = RetentionManager::new(policy);
        let cleaned = manager.run_cleanup(&storage).unwrap();

        // Should have trimmed 10 oldest samples
        assert_eq!(cleaned, 10);

        let series = storage.get_series(&id).unwrap();
        assert_eq!(series.len(), 10);
    }
}
