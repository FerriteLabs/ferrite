//! Continuous Aggregates (Materialized Views)
//!
//! Pre-computed aggregations that update automatically with new data.

use super::aggregation::Aggregation;
use super::labels::Labels;
use super::sample::{Sample, Timestamp};
use super::*;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Configuration for a continuous aggregate
#[derive(Debug, Clone)]
pub struct ContinuousAggregateConfig {
    /// Source metric pattern
    pub source_metric: String,
    /// Aggregation function
    pub aggregation: Aggregation,
    /// Bucket duration
    pub bucket_duration: Duration,
    /// Group by labels
    pub group_by: Vec<String>,
    /// Filter labels
    pub filter_labels: Vec<(String, String)>,
    /// Start time (when to start computing)
    pub start_time: Option<Timestamp>,
    /// Refresh interval
    pub refresh_interval: Duration,
    /// Lag for late data
    pub lag: Duration,
}

impl ContinuousAggregateConfig {
    /// Create a new config
    pub fn new(source_metric: impl Into<String>, aggregation: Aggregation) -> Self {
        Self {
            source_metric: source_metric.into(),
            aggregation,
            bucket_duration: Duration::from_secs(60),
            group_by: Vec::new(),
            filter_labels: Vec::new(),
            start_time: None,
            refresh_interval: Duration::from_secs(60),
            lag: Duration::from_secs(5),
        }
    }

    /// Set bucket duration
    pub fn bucket_duration(mut self, duration: Duration) -> Self {
        self.bucket_duration = duration;
        self
    }

    /// Add group by label
    pub fn group_by(mut self, label: impl Into<String>) -> Self {
        self.group_by.push(label.into());
        self
    }

    /// Add filter
    pub fn filter(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.filter_labels.push((name.into(), value.into()));
        self
    }

    /// Set start time
    pub fn start_time(mut self, ts: Timestamp) -> Self {
        self.start_time = Some(ts);
        self
    }

    /// Set refresh interval
    pub fn refresh_interval(mut self, interval: Duration) -> Self {
        self.refresh_interval = interval;
        self
    }

    /// Set lag for late data
    pub fn lag(mut self, lag: Duration) -> Self {
        self.lag = lag;
        self
    }
}

impl Default for ContinuousAggregateConfig {
    fn default() -> Self {
        Self::new("*", Aggregation::Avg)
    }
}

/// A continuous aggregate (materialized view)
pub struct ContinuousAggregate {
    /// Name
    name: String,
    /// Configuration
    config: ContinuousAggregateConfig,
    /// Buckets by group key
    buckets: RwLock<HashMap<String, AggregationBucket>>,
    /// Last refresh time
    last_refresh: RwLock<Option<Timestamp>>,
    /// Materialized data
    materialized: RwLock<Vec<MaterializedPoint>>,
    /// Statistics
    stats: ContinuousAggregateStats,
}

impl std::fmt::Debug for ContinuousAggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContinuousAggregate")
            .field("name", &self.name)
            .field("config", &self.config)
            .finish()
    }
}

impl ContinuousAggregate {
    /// Create a new continuous aggregate
    pub fn new(name: String, config: ContinuousAggregateConfig) -> Self {
        Self {
            name,
            config,
            buckets: RwLock::new(HashMap::new()),
            last_refresh: RwLock::new(None),
            materialized: RwLock::new(Vec::new()),
            stats: ContinuousAggregateStats::default(),
        }
    }

    /// Get the name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the config
    pub fn config(&self) -> &ContinuousAggregateConfig {
        &self.config
    }

    /// Check if this aggregate matches a metric
    pub fn matches_metric(&self, metric: &str) -> bool {
        let pattern = &self.config.source_metric;
        if pattern == "*" {
            return true;
        }
        if pattern.ends_with('*') {
            let prefix = &pattern[..pattern.len() - 1];
            return metric.starts_with(prefix);
        }
        pattern == metric
    }

    /// Add a sample to the aggregate
    pub fn add_sample(&self, sample: &Sample, labels: &Labels) -> Result<()> {
        // Check filters
        for (name, value) in &self.config.filter_labels {
            if labels.get(name) != Some(value.as_str()) {
                return Ok(()); // Filter doesn't match
            }
        }

        // Compute group key
        let group_key = self.compute_group_key(labels);

        // Get bucket timestamp
        let bucket_ts = sample.timestamp.truncate(self.config.bucket_duration);

        // Add to bucket
        let mut buckets = self.buckets.write();
        let bucket = buckets
            .entry(format!("{}:{}", group_key, bucket_ts.as_nanos()))
            .or_insert_with(|| AggregationBucket::new(bucket_ts, group_key.clone()));

        if let Some(value) = sample.as_f64() {
            bucket.add(value);
        }

        self.stats
            .samples_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }

    /// Compute group key from labels
    fn compute_group_key(&self, labels: &Labels) -> String {
        if self.config.group_by.is_empty() {
            return String::new();
        }

        let parts: Vec<String> = self
            .config
            .group_by
            .iter()
            .filter_map(|name| labels.get(name).map(|v| format!("{}={}", name, v)))
            .collect();

        parts.join(",")
    }

    /// Refresh the aggregate (compute materialized data)
    pub fn refresh(&self) -> Result<usize> {
        let now = Timestamp::now();
        let cutoff = now.sub(self.config.lag);
        let mut count = 0;

        let mut buckets = self.buckets.write();
        let mut materialized = self.materialized.write();

        // Find buckets that are ready to materialize
        let ready_buckets: Vec<(String, AggregationBucket)> = buckets
            .drain()
            .filter(|(_, bucket)| {
                // Bucket is ready if its end time is before the cutoff
                let bucket_end = bucket.timestamp.add(self.config.bucket_duration);
                bucket_end.as_nanos() < cutoff.as_nanos()
            })
            .collect();

        // Materialize ready buckets
        for (_, bucket) in ready_buckets {
            if let Some(value) = bucket.compute(self.config.aggregation) {
                materialized.push(MaterializedPoint {
                    timestamp: bucket.timestamp,
                    group_key: bucket.group_key,
                    value,
                    sample_count: bucket.count,
                });
                count += 1;
            }
        }

        // Update last refresh time
        *self.last_refresh.write() = Some(now);

        self.stats
            .refresh_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.stats
            .points_materialized
            .fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);

        Ok(count)
    }

    /// Query the materialized data
    pub fn query(&self, start: Timestamp, end: Timestamp) -> Vec<MaterializedPoint> {
        self.materialized
            .read()
            .iter()
            .filter(|p| {
                p.timestamp.as_nanos() >= start.as_nanos()
                    && p.timestamp.as_nanos() <= end.as_nanos()
            })
            .cloned()
            .collect()
    }

    /// Query with group filter
    pub fn query_group(
        &self,
        start: Timestamp,
        end: Timestamp,
        group_key: &str,
    ) -> Vec<MaterializedPoint> {
        self.materialized
            .read()
            .iter()
            .filter(|p| {
                p.group_key == group_key
                    && p.timestamp.as_nanos() >= start.as_nanos()
                    && p.timestamp.as_nanos() <= end.as_nanos()
            })
            .cloned()
            .collect()
    }

    /// Get statistics
    pub fn stats(&self) -> &ContinuousAggregateStats {
        &self.stats
    }

    /// Get materialized point count
    pub fn materialized_count(&self) -> usize {
        self.materialized.read().len()
    }

    /// Get pending bucket count
    pub fn pending_count(&self) -> usize {
        self.buckets.read().len()
    }
}

/// A bucket for aggregation
#[derive(Debug, Clone)]
struct AggregationBucket {
    /// Bucket timestamp
    timestamp: Timestamp,
    /// Group key
    group_key: String,
    /// Values in bucket
    values: Vec<f64>,
    /// Count of values
    count: usize,
    /// Running sum
    sum: f64,
    /// Running min
    min: Option<f64>,
    /// Running max
    max: Option<f64>,
}

impl AggregationBucket {
    fn new(timestamp: Timestamp, group_key: String) -> Self {
        Self {
            timestamp,
            group_key,
            values: Vec::new(),
            count: 0,
            sum: 0.0,
            min: None,
            max: None,
        }
    }

    fn add(&mut self, value: f64) {
        self.values.push(value);
        self.count += 1;
        self.sum += value;
        self.min = Some(self.min.map_or(value, |m| m.min(value)));
        self.max = Some(self.max.map_or(value, |m| m.max(value)));
    }

    fn compute(&self, aggregation: Aggregation) -> Option<f64> {
        if self.count == 0 {
            return None;
        }

        match aggregation {
            Aggregation::Sum => Some(self.sum),
            Aggregation::Count => Some(self.count as f64),
            Aggregation::Min => self.min,
            Aggregation::Max => self.max,
            Aggregation::Avg => Some(self.sum / self.count as f64),
            Aggregation::First => self.values.first().cloned(),
            Aggregation::Last => self.values.last().cloned(),
            Aggregation::Median => {
                let mut sorted = self.values.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                sorted.get(sorted.len() / 2).cloned()
            }
            Aggregation::StdDev => {
                let mean = self.sum / self.count as f64;
                let variance: f64 =
                    self.values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / self.count as f64;
                Some(variance.sqrt())
            }
            _ => Some(self.sum / self.count as f64), // Default to avg
        }
    }
}

/// A materialized data point
#[derive(Debug, Clone)]
pub struct MaterializedPoint {
    /// Bucket timestamp
    pub timestamp: Timestamp,
    /// Group key
    pub group_key: String,
    /// Aggregated value
    pub value: f64,
    /// Number of samples aggregated
    pub sample_count: usize,
}

/// Statistics for continuous aggregates
#[derive(Debug, Default)]
pub struct ContinuousAggregateStats {
    /// Samples processed
    pub samples_processed: std::sync::atomic::AtomicU64,
    /// Points materialized
    pub points_materialized: std::sync::atomic::AtomicU64,
    /// Refresh count
    pub refresh_count: std::sync::atomic::AtomicU64,
    /// Last refresh duration in ms
    pub last_refresh_ms: std::sync::atomic::AtomicU64,
}

/// Manager for continuous aggregates
pub struct ContinuousAggregateManager {
    /// Aggregates by name
    aggregates: RwLock<HashMap<String, Arc<ContinuousAggregate>>>,
    /// Background refresh enabled
    refresh_enabled: std::sync::atomic::AtomicBool,
}

impl ContinuousAggregateManager {
    /// Create a new manager
    pub fn new() -> Self {
        Self {
            aggregates: RwLock::new(HashMap::new()),
            refresh_enabled: std::sync::atomic::AtomicBool::new(true),
        }
    }

    /// Create a continuous aggregate
    pub fn create(
        &self,
        name: &str,
        config: ContinuousAggregateConfig,
    ) -> Arc<ContinuousAggregate> {
        let aggregate = Arc::new(ContinuousAggregate::new(name.to_string(), config));
        self.aggregates
            .write()
            .insert(name.to_string(), Arc::clone(&aggregate));
        aggregate
    }

    /// Get an aggregate by name
    pub fn get(&self, name: &str) -> Option<Arc<ContinuousAggregate>> {
        self.aggregates.read().get(name).cloned()
    }

    /// Delete an aggregate
    pub fn delete(&self, name: &str) -> Option<Arc<ContinuousAggregate>> {
        self.aggregates.write().remove(name)
    }

    /// List all aggregates
    pub fn list(&self) -> Vec<String> {
        self.aggregates.read().keys().cloned().collect()
    }

    /// Refresh all aggregates
    pub fn refresh_all(&self) -> Result<usize> {
        let mut total = 0;
        let aggregates = self.aggregates.read();
        for aggregate in aggregates.values() {
            total += aggregate.refresh()?;
        }
        Ok(total)
    }

    /// Process a sample for all matching aggregates
    pub fn process_sample(&self, metric: &str, sample: &Sample, labels: &Labels) -> Result<()> {
        let aggregates = self.aggregates.read();
        for aggregate in aggregates.values() {
            if aggregate.matches_metric(metric) {
                aggregate.add_sample(sample, labels)?;
            }
        }
        Ok(())
    }
}

impl Default for ContinuousAggregateManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_continuous_aggregate_config() {
        let config = ContinuousAggregateConfig::new("cpu.usage", Aggregation::Avg)
            .bucket_duration(Duration::from_secs(60))
            .group_by("host")
            .filter("env", "prod");

        assert_eq!(config.source_metric, "cpu.usage");
        assert_eq!(config.bucket_duration, Duration::from_secs(60));
        assert_eq!(config.group_by, vec!["host"]);
    }

    #[test]
    fn test_continuous_aggregate_matches() {
        let config = ContinuousAggregateConfig::new("cpu.*", Aggregation::Avg);
        let agg = ContinuousAggregate::new("test".to_string(), config);

        assert!(agg.matches_metric("cpu.usage"));
        assert!(agg.matches_metric("cpu.system"));
        assert!(!agg.matches_metric("memory.usage"));
    }

    #[test]
    fn test_add_sample() {
        let config = ContinuousAggregateConfig::new("cpu.usage", Aggregation::Avg)
            .bucket_duration(Duration::from_secs(60));
        let agg = ContinuousAggregate::new("test".to_string(), config);

        let sample = Sample::from_secs(100, 50.0);
        let labels = Labels::parse(&["host:server1"]).unwrap();

        agg.add_sample(&sample, &labels).unwrap();

        assert_eq!(agg.pending_count(), 1);
    }

    #[test]
    fn test_aggregation_bucket() {
        let mut bucket = AggregationBucket::new(Timestamp::from_secs(0), "test".to_string());

        bucket.add(10.0);
        bucket.add(20.0);
        bucket.add(30.0);

        assert_eq!(bucket.compute(Aggregation::Sum), Some(60.0));
        assert_eq!(bucket.compute(Aggregation::Avg), Some(20.0));
        assert_eq!(bucket.compute(Aggregation::Min), Some(10.0));
        assert_eq!(bucket.compute(Aggregation::Max), Some(30.0));
        assert_eq!(bucket.compute(Aggregation::Count), Some(3.0));
    }

    #[test]
    fn test_group_by() {
        let config = ContinuousAggregateConfig::new("*", Aggregation::Avg)
            .group_by("host")
            .group_by("env");
        let agg = ContinuousAggregate::new("test".to_string(), config);

        let labels = Labels::parse(&["host:server1", "env:prod"]).unwrap();
        let group_key = agg.compute_group_key(&labels);

        assert!(group_key.contains("host=server1"));
        assert!(group_key.contains("env=prod"));
    }

    #[test]
    fn test_manager() {
        let manager = ContinuousAggregateManager::new();

        let config = ContinuousAggregateConfig::new("cpu.usage", Aggregation::Avg);
        manager.create("cpu_avg", config);

        assert!(manager.get("cpu_avg").is_some());
        assert!(manager.get("nonexistent").is_none());

        let list = manager.list();
        assert_eq!(list.len(), 1);
        assert!(list.contains(&"cpu_avg".to_string()));
    }

    #[test]
    fn test_process_sample() {
        let manager = ContinuousAggregateManager::new();

        let config = ContinuousAggregateConfig::new("cpu.*", Aggregation::Avg);
        manager.create("cpu_avg", config);

        let sample = Sample::from_secs(100, 50.0);
        let labels = Labels::parse(&["host:server1"]).unwrap();

        manager
            .process_sample("cpu.usage", &sample, &labels)
            .unwrap();

        let agg = manager.get("cpu_avg").unwrap();
        assert_eq!(agg.pending_count(), 1);
    }
}
