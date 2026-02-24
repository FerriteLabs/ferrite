//! Downsampling for Time Series
//!
//! Automatic downsampling of high-resolution data to save storage.
//! Supports multiple destination keys, background processing, and
//! all standard aggregation types.

use super::aggregation::Aggregation;
use super::pipeline::AggregationType;
use super::sample::{Sample, Timestamp};
use super::storage::TimeSeriesStorage;
use super::*;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::time::Duration;

/// Configuration for downsampling
#[derive(Debug, Clone)]
pub struct DownsampleConfig {
    /// Enable automatic downsampling
    pub enabled: bool,
    /// Default downsampling rules
    pub default_rules: Vec<DownsampleRule>,
    /// Run interval for background downsampling
    pub run_interval: Duration,
    /// Maximum samples to process per run
    pub batch_size: usize,
    /// Trigger mode for when downsampling runs
    pub trigger_mode: TriggerMode,
}

impl Default for DownsampleConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_rules: vec![
                DownsampleRule::new(
                    Duration::from_secs(3600),  // 1 hour intervals
                    Duration::from_secs(86400), // After 1 day
                    Aggregation::Avg,
                ),
                DownsampleRule::new(
                    Duration::from_secs(86400),     // 1 day intervals
                    Duration::from_secs(86400 * 7), // After 7 days
                    Aggregation::Avg,
                ),
            ],
            run_interval: Duration::from_secs(300), // 5 minutes
            batch_size: 10000,
            trigger_mode: TriggerMode::Scheduled,
        }
    }
}

/// A rule for downsampling
#[derive(Debug, Clone)]
pub struct DownsampleRule {
    /// Name of the rule
    pub name: String,
    /// Target interval for downsampled data
    pub interval: Duration,
    /// Apply after this age
    pub after: Duration,
    /// Aggregation function
    pub aggregation: Aggregation,
    /// Metric patterns to match (empty = all)
    pub patterns: Vec<String>,
    /// Keep original data after downsampling
    pub keep_original: bool,
}

impl DownsampleRule {
    /// Create a new downsampling rule
    pub fn new(interval: Duration, after: Duration, aggregation: Aggregation) -> Self {
        Self {
            name: format!("{}s_after_{}s", interval.as_secs(), after.as_secs()),
            interval,
            after,
            aggregation,
            patterns: Vec::new(),
            keep_original: false,
        }
    }

    /// Create a named rule
    pub fn named(name: impl Into<String>, interval: Duration, after: Duration) -> Self {
        Self {
            name: name.into(),
            interval,
            after,
            aggregation: Aggregation::Avg,
            patterns: Vec::new(),
            keep_original: false,
        }
    }

    /// Set aggregation
    pub fn with_aggregation(mut self, agg: Aggregation) -> Self {
        self.aggregation = agg;
        self
    }

    /// Add metric pattern
    pub fn for_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.patterns.push(pattern.into());
        self
    }

    /// Keep original data
    pub fn keep_original(mut self, keep: bool) -> Self {
        self.keep_original = keep;
        self
    }

    /// Check if rule applies to a metric
    pub fn applies_to(&self, metric: &str) -> bool {
        if self.patterns.is_empty() {
            return true;
        }

        for pattern in &self.patterns {
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

    /// Check if a timestamp is old enough for this rule
    pub fn should_apply(&self, timestamp: Timestamp) -> bool {
        let now = Timestamp::now();
        let age = now.diff(&timestamp);
        age >= self.after
    }

    /// Downsample a set of samples
    pub fn downsample(&self, samples: &[Sample]) -> Vec<Sample> {
        if samples.is_empty() {
            return Vec::new();
        }

        let mut result = Vec::new();
        let interval_nanos = self.interval.as_nanos() as i64;

        // Group samples by bucket
        let mut buckets: HashMap<i64, Vec<f64>> = HashMap::new();

        for sample in samples {
            let bucket = (sample.timestamp.as_nanos() / interval_nanos) * interval_nanos;
            if let Some(value) = sample.as_f64() {
                buckets.entry(bucket).or_default().push(value);
            }
        }

        // Aggregate each bucket
        for (bucket_ts, values) in buckets {
            if let Some(agg_value) = aggregate_values(&values, self.aggregation) {
                result.push(Sample::new(Timestamp::from_nanos(bucket_ts), agg_value));
            }
        }

        // Sort by timestamp
        result.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

        result
    }
}

/// A downsampling rule with source/destination key mapping.
///
/// Defines how a source series should be downsampled into a destination series.
/// A single source can have multiple `DownsamplingRule`s targeting different
/// destination keys with different aggregations and bucket sizes.
#[derive(Debug, Clone)]
pub struct DownsamplingRule {
    /// Source series key pattern
    pub source_key: String,
    /// Destination series key (downsampled data is written here)
    pub dest_key: String,
    /// Bucket duration for aggregation
    pub bucket_duration: Duration,
    /// Aggregation type to apply
    pub aggregation: AggregationType,
    /// Optional retention for the destination series
    pub retention: Option<Duration>,
}

impl DownsamplingRule {
    /// Create a new downsampling rule
    pub fn new(
        source_key: impl Into<String>,
        dest_key: impl Into<String>,
        bucket_duration: Duration,
        aggregation: AggregationType,
    ) -> Self {
        Self {
            source_key: source_key.into(),
            dest_key: dest_key.into(),
            bucket_duration,
            aggregation,
            retention: None,
        }
    }

    /// Set retention for the destination series
    pub fn with_retention(mut self, retention: Duration) -> Self {
        self.retention = Some(retention);
        self
    }

    /// Check if this rule applies to a given metric key
    pub fn matches_source(&self, metric: &str) -> bool {
        if self.source_key == "*" {
            return true;
        }
        if self.source_key.ends_with('*') {
            let prefix = &self.source_key[..self.source_key.len() - 1];
            return metric.starts_with(prefix);
        }
        self.source_key == metric
    }

    /// Downsample samples using this rule's aggregation and bucket duration
    pub fn downsample(&self, samples: &[Sample]) -> Vec<Sample> {
        if samples.is_empty() {
            return Vec::new();
        }

        let interval_nanos = self.bucket_duration.as_nanos() as i64;
        let mut buckets: HashMap<i64, Vec<f64>> = HashMap::new();

        for sample in samples {
            let bucket_ts = (sample.timestamp.as_nanos() / interval_nanos) * interval_nanos;
            if let Some(v) = sample.as_f64() {
                buckets.entry(bucket_ts).or_default().push(v);
            }
        }

        let mut result: Vec<Sample> = buckets
            .into_iter()
            .filter_map(|(ts, values)| {
                self.aggregation
                    .aggregate(&values)
                    .map(|v| Sample::new(Timestamp::from_nanos(ts), v))
            })
            .collect();

        result.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        result
    }
}

/// Aggregate a slice of values
fn aggregate_values(values: &[f64], aggregation: Aggregation) -> Option<f64> {
    if values.is_empty() {
        return None;
    }

    match aggregation {
        Aggregation::Sum => Some(values.iter().sum()),
        Aggregation::Count => Some(values.len() as f64),
        Aggregation::Min => values.iter().cloned().reduce(f64::min),
        Aggregation::Max => values.iter().cloned().reduce(f64::max),
        Aggregation::Avg => Some(values.iter().sum::<f64>() / values.len() as f64),
        Aggregation::First => values.first().cloned(),
        Aggregation::Last => values.last().cloned(),
        Aggregation::Median => {
            let mut sorted = values.to_vec();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            sorted.get(sorted.len() / 2).cloned()
        }
        Aggregation::StdDev => {
            let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
            let variance: f64 =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
            Some(variance.sqrt())
        }
        Aggregation::Variance => {
            let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
            Some(values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64)
        }
        _ => Some(values.iter().sum::<f64>() / values.len() as f64), // Default to avg
    }
}

/// Downsampler service
pub struct Downsampler {
    /// Configuration
    config: DownsampleConfig,
    /// Rules (legacy)
    rules: RwLock<Vec<DownsampleRule>>,
    /// Downsampling rules with source/dest mapping
    downsampling_rules: RwLock<Vec<DownsamplingRule>>,
    /// Statistics
    stats: DownsamplerStats,
}

/// Downsampler statistics
#[derive(Debug, Default)]
pub struct DownsamplerStats {
    /// Samples processed
    pub samples_processed: std::sync::atomic::AtomicU64,
    /// Samples created (downsampled)
    pub samples_created: std::sync::atomic::AtomicU64,
    /// Samples removed
    pub samples_removed: std::sync::atomic::AtomicU64,
    /// Run count
    pub runs: std::sync::atomic::AtomicU64,
    /// Last run timestamp
    pub last_run: std::sync::atomic::AtomicI64,
}

impl Downsampler {
    /// Create a new downsampler
    pub fn new(config: DownsampleConfig) -> Self {
        let rules = config.default_rules.clone();
        Self {
            config,
            rules: RwLock::new(rules),
            downsampling_rules: RwLock::new(Vec::new()),
            stats: DownsamplerStats::default(),
        }
    }

    /// Add a downsampling rule
    pub fn add_rule(&self, rule: DownsampleRule) {
        self.rules.write().push(rule);
    }

    /// Add a downsampling rule with source/dest mapping
    pub fn add_downsampling_rule(&self, rule: DownsamplingRule) {
        self.downsampling_rules.write().push(rule);
    }

    /// Remove a rule by name
    pub fn remove_rule(&self, name: &str) {
        self.rules.write().retain(|r| r.name != name);
    }

    /// Get all rules
    pub fn rules(&self) -> Vec<DownsampleRule> {
        self.rules.read().clone()
    }

    /// Get all downsampling rules
    pub fn downsampling_rules(&self) -> Vec<DownsamplingRule> {
        self.downsampling_rules.read().clone()
    }

    /// Run downsampling on storage
    ///
    /// Processes all series, applying applicable rules to downsample old data.
    /// For DownsamplingRules, writes downsampled data to destination keys.
    pub fn run(&self, storage: &TimeSeriesStorage) -> Result<u64> {
        if !self.config.enabled {
            return Ok(0);
        }

        let now = Timestamp::now();
        let mut samples_processed = 0u64;

        tracing::debug!("Running downsampling");

        // Process DownsamplingRules: source -> dest key mapping
        let ds_rules = self.downsampling_rules.read().clone();
        for rule in &ds_rules {
            for (id, series) in storage.all_series() {
                if !rule.matches_source(id.metric()) {
                    continue;
                }

                let all_samples = series.all_samples();
                if all_samples.is_empty() {
                    continue;
                }

                let downsampled = rule.downsample(&all_samples);
                if downsampled.is_empty() {
                    continue;
                }

                // Write to destination key
                let dest_id = super::series::TimeSeriesId::from_metric(&rule.dest_key);
                for sample in &downsampled {
                    storage.add_sample(&dest_id, sample.clone())?;
                }

                samples_processed += downsampled.len() as u64;
            }
        }

        // Process legacy DownsampleRules: in-place aggregation of old data
        let rules = self.rules.read().clone();
        for (id, series) in storage.all_series() {
            for rule in &rules {
                if !rule.applies_to(id.metric()) {
                    continue;
                }

                let all_samples = series.all_samples();
                let (to_downsample, _to_keep): (Vec<_>, Vec<_>) = all_samples
                    .iter()
                    .cloned()
                    .partition(|s| rule.should_apply(s.timestamp));

                if !to_downsample.is_empty() {
                    let downsampled = rule.downsample(&to_downsample);
                    samples_processed += downsampled.len() as u64;
                }
            }
        }

        // Update stats
        self.stats
            .runs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.stats
            .last_run
            .store(now.as_nanos(), std::sync::atomic::Ordering::Relaxed);
        self.stats
            .samples_processed
            .fetch_add(samples_processed, std::sync::atomic::Ordering::Relaxed);

        Ok(samples_processed)
    }

    /// Downsample a series of samples using all applicable rules
    pub fn downsample_series(&self, metric: &str, samples: &[Sample]) -> Vec<Sample> {
        let rules = self.rules.read();
        let mut result = samples.to_vec();

        for rule in rules.iter() {
            if rule.applies_to(metric) {
                // Filter samples that should be downsampled by this rule
                let (to_downsample, to_keep): (Vec<_>, Vec<_>) = result
                    .iter()
                    .cloned()
                    .partition(|s| rule.should_apply(s.timestamp));

                // Downsample eligible samples
                let downsampled = rule.downsample(&to_downsample);

                // Combine results
                result = to_keep;
                result.extend(downsampled);
            }
        }

        // Sort by timestamp
        result.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

        result
    }

    /// Get statistics
    pub fn stats(&self) -> &DownsamplerStats {
        &self.stats
    }
}

impl Default for Downsampler {
    fn default() -> Self {
        Self::new(DownsampleConfig::default())
    }
}

/// Standard time bucket sizes for downsampling
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeBucket {
    /// 1 second
    OneSecond,
    /// 5 seconds
    FiveSeconds,
    /// 15 seconds
    FifteenSeconds,
    /// 1 minute
    OneMinute,
    /// 5 minutes
    FiveMinutes,
    /// 15 minutes
    FifteenMinutes,
    /// 1 hour
    OneHour,
    /// 6 hours
    SixHours,
    /// 1 day
    OneDay,
}

impl TimeBucket {
    /// Convert to Duration
    pub fn as_duration(&self) -> Duration {
        match self {
            TimeBucket::OneSecond => Duration::from_secs(1),
            TimeBucket::FiveSeconds => Duration::from_secs(5),
            TimeBucket::FifteenSeconds => Duration::from_secs(15),
            TimeBucket::OneMinute => Duration::from_secs(60),
            TimeBucket::FiveMinutes => Duration::from_secs(300),
            TimeBucket::FifteenMinutes => Duration::from_secs(900),
            TimeBucket::OneHour => Duration::from_secs(3600),
            TimeBucket::SixHours => Duration::from_secs(21600),
            TimeBucket::OneDay => Duration::from_secs(86400),
        }
    }

    /// All standard bucket sizes in ascending order
    pub fn all() -> &'static [TimeBucket] {
        &[
            TimeBucket::OneSecond,
            TimeBucket::FiveSeconds,
            TimeBucket::FifteenSeconds,
            TimeBucket::OneMinute,
            TimeBucket::FiveMinutes,
            TimeBucket::FifteenMinutes,
            TimeBucket::OneHour,
            TimeBucket::SixHours,
            TimeBucket::OneDay,
        ]
    }

    /// Parse from string like "1s", "5m", "1h", "1d"
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "1s" => Some(TimeBucket::OneSecond),
            "5s" => Some(TimeBucket::FiveSeconds),
            "15s" => Some(TimeBucket::FifteenSeconds),
            "1m" | "60s" => Some(TimeBucket::OneMinute),
            "5m" | "300s" => Some(TimeBucket::FiveMinutes),
            "15m" | "900s" => Some(TimeBucket::FifteenMinutes),
            "1h" | "3600s" => Some(TimeBucket::OneHour),
            "6h" | "21600s" => Some(TimeBucket::SixHours),
            "1d" | "86400s" => Some(TimeBucket::OneDay),
            _ => None,
        }
    }
}

impl std::fmt::Display for TimeBucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeBucket::OneSecond => write!(f, "1s"),
            TimeBucket::FiveSeconds => write!(f, "5s"),
            TimeBucket::FifteenSeconds => write!(f, "15s"),
            TimeBucket::OneMinute => write!(f, "1m"),
            TimeBucket::FiveMinutes => write!(f, "5m"),
            TimeBucket::FifteenMinutes => write!(f, "15m"),
            TimeBucket::OneHour => write!(f, "1h"),
            TimeBucket::SixHours => write!(f, "6h"),
            TimeBucket::OneDay => write!(f, "1d"),
        }
    }
}

impl From<TimeBucket> for Duration {
    fn from(bucket: TimeBucket) -> Self {
        bucket.as_duration()
    }
}

/// Trigger mode for downsampling execution
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TriggerMode {
    /// Run downsampling on a schedule (periodic background task)
    #[default]
    Scheduled,
    /// Run downsampling when new data arrives (inline with ingest)
    OnIngest,
    /// Run both on schedule and on ingest
    Both,
}

/// Preset downsampling configurations
pub mod presets {
    use super::*;

    /// Minimal downsampling for high-precision data
    pub fn minimal() -> DownsampleConfig {
        DownsampleConfig {
            enabled: true,
            default_rules: vec![DownsampleRule::new(
                Duration::from_secs(60),    // 1 minute
                Duration::from_secs(86400), // After 1 day
                Aggregation::Avg,
            )],
            run_interval: Duration::from_secs(600),
            batch_size: 10000,
            trigger_mode: TriggerMode::Scheduled,
        }
    }

    /// Balanced downsampling for typical metrics
    pub fn balanced() -> DownsampleConfig {
        DownsampleConfig {
            enabled: true,
            default_rules: vec![
                DownsampleRule::new(
                    Duration::from_secs(300),   // 5 minutes
                    Duration::from_secs(86400), // After 1 day
                    Aggregation::Avg,
                ),
                DownsampleRule::new(
                    Duration::from_secs(3600),      // 1 hour
                    Duration::from_secs(86400 * 7), // After 7 days
                    Aggregation::Avg,
                ),
            ],
            run_interval: Duration::from_secs(300),
            batch_size: 50000,
            trigger_mode: TriggerMode::Scheduled,
        }
    }

    /// Aggressive downsampling for long-term storage
    pub fn aggressive() -> DownsampleConfig {
        DownsampleConfig {
            enabled: true,
            default_rules: vec![
                DownsampleRule::new(
                    Duration::from_secs(60),   // 1 minute
                    Duration::from_secs(3600), // After 1 hour
                    Aggregation::Avg,
                ),
                DownsampleRule::new(
                    Duration::from_secs(300),   // 5 minutes
                    Duration::from_secs(86400), // After 1 day
                    Aggregation::Avg,
                ),
                DownsampleRule::new(
                    Duration::from_secs(3600),      // 1 hour
                    Duration::from_secs(86400 * 7), // After 7 days
                    Aggregation::Avg,
                ),
                DownsampleRule::new(
                    Duration::from_secs(86400),      // 1 day
                    Duration::from_secs(86400 * 30), // After 30 days
                    Aggregation::Avg,
                ),
            ],
            run_interval: Duration::from_secs(60),
            batch_size: 100000,
            trigger_mode: TriggerMode::Both,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_downsample_rule_creation() {
        let rule = DownsampleRule::new(
            Duration::from_secs(3600),
            Duration::from_secs(86400),
            Aggregation::Avg,
        );

        assert_eq!(rule.interval, Duration::from_secs(3600));
        assert_eq!(rule.after, Duration::from_secs(86400));
    }

    #[test]
    fn test_downsample_rule_applies_to() {
        let rule = DownsampleRule::new(
            Duration::from_secs(3600),
            Duration::from_secs(86400),
            Aggregation::Avg,
        )
        .for_pattern("cpu.*")
        .for_pattern("memory.usage");

        assert!(rule.applies_to("cpu.user"));
        assert!(rule.applies_to("cpu.system"));
        assert!(rule.applies_to("memory.usage"));
        assert!(!rule.applies_to("memory.free"));
        assert!(!rule.applies_to("disk.usage"));
    }

    #[test]
    fn test_downsample_samples() {
        let rule = DownsampleRule::new(
            Duration::from_secs(60),
            Duration::from_secs(0), // Apply immediately for testing
            Aggregation::Avg,
        );

        // Create samples: 6 samples over 120 seconds (2 buckets of 60s)
        let samples = vec![
            Sample::from_secs(0, 10.0),
            Sample::from_secs(20, 20.0),
            Sample::from_secs(40, 30.0),
            Sample::from_secs(60, 40.0),
            Sample::from_secs(80, 50.0),
            Sample::from_secs(100, 60.0),
        ];

        let downsampled = rule.downsample(&samples);

        assert_eq!(downsampled.len(), 2);
        // First bucket (0-59): avg of 10, 20, 30 = 20
        assert_eq!(downsampled[0].as_f64(), Some(20.0));
        // Second bucket (60-119): avg of 40, 50, 60 = 50
        assert_eq!(downsampled[1].as_f64(), Some(50.0));
    }

    #[test]
    fn test_downsample_min_max() {
        let min_rule = DownsampleRule::new(
            Duration::from_secs(60),
            Duration::from_secs(0),
            Aggregation::Min,
        );

        let max_rule = DownsampleRule::new(
            Duration::from_secs(60),
            Duration::from_secs(0),
            Aggregation::Max,
        );

        let samples = vec![
            Sample::from_secs(0, 10.0),
            Sample::from_secs(20, 5.0),
            Sample::from_secs(40, 15.0),
        ];

        let min_ds = min_rule.downsample(&samples);
        let max_ds = max_rule.downsample(&samples);

        assert_eq!(min_ds[0].as_f64(), Some(5.0));
        assert_eq!(max_ds[0].as_f64(), Some(15.0));
    }

    #[test]
    fn test_downsampler() {
        let config = DownsampleConfig::default();
        let downsampler = Downsampler::new(config);

        let rules = downsampler.rules();
        assert!(!rules.is_empty());
    }

    #[test]
    fn test_downsampler_add_rule() {
        let downsampler = Downsampler::new(DownsampleConfig::default());

        let rule = DownsampleRule::named(
            "custom",
            Duration::from_secs(300),
            Duration::from_secs(3600),
        );
        downsampler.add_rule(rule);

        let rules = downsampler.rules();
        assert!(rules.iter().any(|r| r.name == "custom"));
    }

    #[test]
    fn test_presets() {
        let minimal = presets::minimal();
        assert_eq!(minimal.default_rules.len(), 1);

        let balanced = presets::balanced();
        assert_eq!(balanced.default_rules.len(), 2);

        let aggressive = presets::aggressive();
        assert_eq!(aggressive.default_rules.len(), 4);
    }

    // --- TimeBucket tests ---

    #[test]
    fn test_time_bucket_as_duration() {
        assert_eq!(TimeBucket::OneSecond.as_duration(), Duration::from_secs(1));
        assert_eq!(
            TimeBucket::FiveMinutes.as_duration(),
            Duration::from_secs(300)
        );
        assert_eq!(TimeBucket::OneHour.as_duration(), Duration::from_secs(3600));
        assert_eq!(TimeBucket::OneDay.as_duration(), Duration::from_secs(86400));
    }

    #[test]
    fn test_time_bucket_parse() {
        assert_eq!(TimeBucket::parse("1s"), Some(TimeBucket::OneSecond));
        assert_eq!(TimeBucket::parse("5s"), Some(TimeBucket::FiveSeconds));
        assert_eq!(TimeBucket::parse("15s"), Some(TimeBucket::FifteenSeconds));
        assert_eq!(TimeBucket::parse("1m"), Some(TimeBucket::OneMinute));
        assert_eq!(TimeBucket::parse("5m"), Some(TimeBucket::FiveMinutes));
        assert_eq!(TimeBucket::parse("15m"), Some(TimeBucket::FifteenMinutes));
        assert_eq!(TimeBucket::parse("1h"), Some(TimeBucket::OneHour));
        assert_eq!(TimeBucket::parse("6h"), Some(TimeBucket::SixHours));
        assert_eq!(TimeBucket::parse("1d"), Some(TimeBucket::OneDay));
        assert_eq!(TimeBucket::parse("invalid"), None);
    }

    #[test]
    fn test_time_bucket_all() {
        let all = TimeBucket::all();
        assert_eq!(all.len(), 9);
        // Should be ascending order
        for i in 1..all.len() {
            assert!(all[i].as_duration() > all[i - 1].as_duration());
        }
    }

    #[test]
    fn test_time_bucket_into_duration() {
        let d: Duration = TimeBucket::FiveMinutes.into();
        assert_eq!(d, Duration::from_secs(300));
    }

    #[test]
    fn test_time_bucket_display() {
        assert_eq!(TimeBucket::OneMinute.to_string(), "1m");
        assert_eq!(TimeBucket::OneHour.to_string(), "1h");
        assert_eq!(TimeBucket::OneDay.to_string(), "1d");
    }

    // --- TriggerMode tests ---

    #[test]
    fn test_trigger_mode_default() {
        assert_eq!(TriggerMode::default(), TriggerMode::Scheduled);
    }

    #[test]
    fn test_downsample_with_time_bucket() {
        let rule = DownsampleRule::new(
            TimeBucket::OneMinute.as_duration(),
            Duration::from_secs(0),
            Aggregation::Avg,
        );

        let samples = vec![
            Sample::from_secs(0, 10.0),
            Sample::from_secs(20, 20.0),
            Sample::from_secs(40, 30.0),
            Sample::from_secs(60, 40.0),
        ];

        let ds = rule.downsample(&samples);
        assert_eq!(ds.len(), 2);
    }

    // --- DownsamplingRule tests ---

    #[test]
    fn test_downsampling_rule_creation() {
        let rule = DownsamplingRule::new(
            "cpu.raw",
            "cpu.1min_avg",
            Duration::from_secs(60),
            AggregationType::Avg,
        );

        assert_eq!(rule.source_key, "cpu.raw");
        assert_eq!(rule.dest_key, "cpu.1min_avg");
        assert_eq!(rule.aggregation, AggregationType::Avg);
        assert!(rule.retention.is_none());
    }

    #[test]
    fn test_downsampling_rule_with_retention() {
        let rule = DownsamplingRule::new(
            "cpu.raw",
            "cpu.1h_avg",
            Duration::from_secs(3600),
            AggregationType::Avg,
        )
        .with_retention(Duration::from_secs(86400 * 30));

        assert_eq!(rule.retention, Some(Duration::from_secs(86400 * 30)));
    }

    #[test]
    fn test_downsampling_rule_matches_source() {
        let rule = DownsamplingRule::new(
            "cpu.*",
            "cpu.agg",
            Duration::from_secs(60),
            AggregationType::Avg,
        );

        assert!(rule.matches_source("cpu.usage"));
        assert!(rule.matches_source("cpu.system"));
        assert!(!rule.matches_source("memory.usage"));

        let exact = DownsamplingRule::new(
            "cpu.usage",
            "cpu.agg",
            Duration::from_secs(60),
            AggregationType::Avg,
        );
        assert!(exact.matches_source("cpu.usage"));
        assert!(!exact.matches_source("cpu.system"));

        let wildcard = DownsamplingRule::new(
            "*",
            "all.agg",
            Duration::from_secs(60),
            AggregationType::Avg,
        );
        assert!(wildcard.matches_source("anything"));
    }

    #[test]
    fn test_downsampling_rule_downsample() {
        let rule = DownsamplingRule::new(
            "test",
            "test.avg",
            Duration::from_secs(60),
            AggregationType::Avg,
        );

        let samples = vec![
            Sample::from_secs(0, 10.0),
            Sample::from_secs(20, 20.0),
            Sample::from_secs(40, 30.0),
            Sample::from_secs(60, 40.0),
            Sample::from_secs(80, 50.0),
            Sample::from_secs(100, 60.0),
        ];

        let result = rule.downsample(&samples);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].as_f64(), Some(20.0)); // avg(10,20,30)
        assert_eq!(result[1].as_f64(), Some(50.0)); // avg(40,50,60)
    }

    #[test]
    fn test_downsampling_rule_all_aggregations() {
        let samples = vec![
            Sample::from_secs(0, 10.0),
            Sample::from_secs(20, 20.0),
            Sample::from_secs(40, 30.0),
        ];

        for agg in &[
            AggregationType::Avg,
            AggregationType::Min,
            AggregationType::Max,
            AggregationType::Sum,
            AggregationType::Count,
            AggregationType::First,
            AggregationType::Last,
            AggregationType::StdDev,
            AggregationType::Range,
        ] {
            let rule = DownsamplingRule::new("test", "test.out", Duration::from_secs(60), *agg);
            let result = rule.downsample(&samples);
            assert_eq!(result.len(), 1, "Failed for {:?}", agg);
            assert!(result[0].as_f64().is_some(), "Failed for {:?}", agg);
        }
    }

    #[test]
    fn test_downsampling_rule_empty_samples() {
        let rule = DownsamplingRule::new(
            "test",
            "test.avg",
            Duration::from_secs(60),
            AggregationType::Avg,
        );

        let result = rule.downsample(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_downsampling_rule_single_sample() {
        let rule = DownsamplingRule::new(
            "test",
            "test.avg",
            Duration::from_secs(60),
            AggregationType::Avg,
        );

        let result = rule.downsample(&[Sample::from_secs(30, 42.0)]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].as_f64(), Some(42.0));
    }

    #[test]
    fn test_downsampling_multiple_dest_keys() {
        let samples = vec![
            Sample::from_secs(0, 10.0),
            Sample::from_secs(20, 20.0),
            Sample::from_secs(40, 30.0),
        ];

        let avg_rule = DownsamplingRule::new(
            "cpu",
            "cpu.1min_avg",
            Duration::from_secs(60),
            AggregationType::Avg,
        );
        let max_rule = DownsamplingRule::new(
            "cpu",
            "cpu.1min_max",
            Duration::from_secs(60),
            AggregationType::Max,
        );

        let avg_result = avg_rule.downsample(&samples);
        let max_result = max_rule.downsample(&samples);

        assert_eq!(avg_result[0].as_f64(), Some(20.0));
        assert_eq!(max_result[0].as_f64(), Some(30.0));
    }

    #[test]
    fn test_downsampler_with_downsampling_rules() {
        use super::super::series::TimeSeriesId;
        use super::super::storage::{StorageConfig, TimeSeriesStorage};

        let storage = TimeSeriesStorage::new(StorageConfig::default()).unwrap();

        // Add raw samples
        let id = TimeSeriesId::from_metric("cpu.raw");
        for i in 0..6 {
            storage
                .add_sample(&id, Sample::from_secs(i * 20, (i * 10) as f64))
                .unwrap();
        }

        let downsampler = Downsampler::new(DownsampleConfig {
            enabled: true,
            default_rules: vec![],
            run_interval: Duration::from_secs(60),
            batch_size: 10000,
            trigger_mode: TriggerMode::Scheduled,
        });

        downsampler.add_downsampling_rule(DownsamplingRule::new(
            "cpu.raw",
            "cpu.1min_avg",
            Duration::from_secs(60),
            AggregationType::Avg,
        ));

        let processed = downsampler.run(&storage).unwrap();
        assert!(processed > 0);

        // Verify destination key was created
        let dest_id = TimeSeriesId::from_metric("cpu.1min_avg");
        let dest_series = storage.get_series(&dest_id);
        assert!(dest_series.is_some());
        assert!(!dest_series.unwrap().all_samples().is_empty());
    }
}
