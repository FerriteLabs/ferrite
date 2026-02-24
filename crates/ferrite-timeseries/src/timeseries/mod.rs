//! Time-Series Engine for Ferrite
//!
//! A high-performance time-series database engine supporting:
//! - Automatic downsampling and aggregation
//! - Retention policies with tiered storage
//! - Continuous aggregates (materialized views)
//! - SQL-like query syntax via FerriteQL
//! - Compression for efficient storage
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Time-Series Engine                        │
//! ├─────────────────────────────────────────────────────────────┤
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
//! │  │   Ingestion │  │   Storage   │  │      Query          │  │
//! │  │   Pipeline  │──│   Engine    │──│      Engine         │  │
//! │  └─────────────┘  └─────────────┘  └─────────────────────┘  │
//! │         │                │                    │              │
//! │  ┌──────▼──────┐  ┌──────▼──────┐  ┌─────────▼─────────┐   │
//! │  │  Downsampler│  │  Retention  │  │   Aggregation     │   │
//! │  │             │  │  Manager    │  │   Functions       │   │
//! │  └─────────────┘  └─────────────┘  └───────────────────┘   │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use ferrite::timeseries::{TimeSeriesEngine, Sample, RetentionPolicy};
//! use std::time::Duration;
//!
//! // Create engine with retention policy
//! let engine = TimeSeriesEngine::builder()
//!     .retention_policy(RetentionPolicy::new(Duration::from_secs(86400 * 30)))
//!     .downsample_interval(Duration::from_secs(60))
//!     .build()?;
//!
//! // Add samples
//! engine.add("cpu.usage", Sample::now(75.5), &["host:server1"])?;
//! engine.add("cpu.usage", Sample::now(80.2), &["host:server2"])?;
//!
//! // Query with aggregation
//! let results = engine.query()
//!     .metric("cpu.usage")
//!     .range(start, end)
//!     .aggregate(Aggregation::Avg)
//!     .group_by("host")
//!     .execute()?;
//! ```

#![allow(dead_code)]
pub mod aggregation;
pub mod compression;
pub mod continuous;
pub mod downsample;
pub mod labels;
pub mod pipeline;
pub mod query;
pub mod retention;
pub mod sample;
pub mod series;
pub mod storage;

pub use aggregation::{Aggregation, AggregationFunction, AggregationResult};
pub use compression::{CompressedBlock, Compression, CompressionCodec, CompressionLevel};
pub use continuous::{ContinuousAggregate, ContinuousAggregateConfig};
pub use downsample::{
    DownsampleConfig, DownsampleRule, Downsampler, DownsamplingRule, TimeBucket, TriggerMode,
};
pub use labels::{Label, LabelIndex, LabelMatcher, LabelMatcherType, Labels};
pub use pipeline::{
    AggregationPipeline, AggregationType, FilterExpr, MathOp, PipelineSeries, PipelineStage,
};
pub use query::{QueryBuilder, QueryResult, TimeRange, TimeSeriesQuery};
pub use retention::{RetentionManager, RetentionPolicy, RetentionTier};
pub use sample::{Sample, Timestamp, Value};
pub use series::{TimeSeries, TimeSeriesId, TimeSeriesMetadata};
pub use storage::{StorageConfig, TimeSeriesStorage};

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Time-series engine error types
#[derive(Debug, thiserror::Error)]
pub enum TimeSeriesError {
    /// Metric not found
    #[error("metric not found: {0}")]
    MetricNotFound(String),

    /// Invalid timestamp
    #[error("invalid timestamp: {0}")]
    InvalidTimestamp(String),

    /// Query error
    #[error("query error: {0}")]
    QueryError(String),

    /// Storage error
    #[error("storage error: {0}")]
    StorageError(String),

    /// Compression error
    #[error("compression error: {0}")]
    CompressionError(String),

    /// Configuration error
    #[error("configuration error: {0}")]
    ConfigError(String),

    /// Rate limit exceeded
    #[error("rate limit exceeded: {0}")]
    RateLimitExceeded(String),
}

/// Result type for time-series operations
pub type Result<T> = std::result::Result<T, TimeSeriesError>;

/// Main time-series engine
pub struct TimeSeriesEngine {
    /// Storage backend
    storage: Arc<TimeSeriesStorage>,
    /// Retention manager
    retention_manager: Arc<RetentionManager>,
    /// Downsampler
    downsampler: Arc<Downsampler>,
    /// Continuous aggregates
    continuous_aggregates: RwLock<HashMap<String, ContinuousAggregate>>,
    /// Label index for fast filtering
    label_index: RwLock<LabelIndex>,
    /// Engine configuration
    config: TimeSeriesConfig,
    /// Metrics
    metrics: TimeSeriesMetrics,
}

/// Time-series engine configuration
#[derive(Debug, Clone)]
pub struct TimeSeriesConfig {
    /// Default retention duration
    pub default_retention: Duration,
    /// Enable compression
    pub compression_enabled: bool,
    /// Compression codec
    pub compression_codec: CompressionCodec,
    /// Maximum samples per series in memory
    pub max_samples_in_memory: usize,
    /// Flush interval for persistence
    pub flush_interval: Duration,
    /// Enable continuous aggregates
    pub continuous_aggregates_enabled: bool,
    /// Maximum number of series
    pub max_series: usize,
    /// Maximum samples per second (rate limiting)
    pub max_samples_per_second: Option<u64>,
    /// Data directory for persistence
    pub data_dir: Option<std::path::PathBuf>,
    /// Flush threshold (number of samples before flushing)
    pub flush_threshold: Option<usize>,
    /// Compaction threshold (number of blocks before compacting)
    pub compaction_threshold: Option<usize>,
    /// Target block size for compaction
    pub target_block_size: Option<usize>,
}

impl Default for TimeSeriesConfig {
    fn default() -> Self {
        Self {
            default_retention: Duration::from_secs(86400 * 30), // 30 days
            compression_enabled: true,
            compression_codec: CompressionCodec::Gorilla,
            max_samples_in_memory: 100_000,
            flush_interval: Duration::from_secs(10),
            continuous_aggregates_enabled: true,
            max_series: 1_000_000,
            max_samples_per_second: None,
            data_dir: None,
            flush_threshold: None,
            compaction_threshold: None,
            target_block_size: None,
        }
    }
}

/// Time-series engine metrics
#[derive(Debug, Default)]
pub struct TimeSeriesMetrics {
    /// Total samples ingested
    pub samples_ingested: std::sync::atomic::AtomicU64,
    /// Total series count
    pub series_count: std::sync::atomic::AtomicU64,
    /// Total queries executed
    pub queries_executed: std::sync::atomic::AtomicU64,
    /// Samples dropped due to rate limiting
    pub samples_dropped: std::sync::atomic::AtomicU64,
    /// Compression ratio (percentage)
    pub compression_ratio: std::sync::atomic::AtomicU64,
}

impl TimeSeriesEngine {
    /// Create a new time-series engine with default configuration
    pub fn new() -> Result<Self> {
        Self::with_config(TimeSeriesConfig::default())
    }

    /// Create a new time-series engine with custom configuration
    pub fn with_config(config: TimeSeriesConfig) -> Result<Self> {
        let storage_config = StorageConfig {
            max_samples_in_memory: config.max_samples_in_memory,
            compression_enabled: config.compression_enabled,
            compression_codec: config.compression_codec.clone(),
            data_dir: config.data_dir.clone(),
            flush_threshold: config.flush_threshold.unwrap_or(10000),
            compaction_threshold: config.compaction_threshold.unwrap_or(5),
            target_block_size: config.target_block_size.unwrap_or(64 * 1024),
        };

        let storage = Arc::new(TimeSeriesStorage::new(storage_config)?);

        let retention_manager = Arc::new(RetentionManager::new(RetentionPolicy::new(
            config.default_retention,
        )));

        let downsampler = Arc::new(Downsampler::new(DownsampleConfig::default()));

        Ok(Self {
            storage,
            retention_manager,
            downsampler,
            continuous_aggregates: RwLock::new(HashMap::new()),
            label_index: RwLock::new(LabelIndex::new()),
            config,
            metrics: TimeSeriesMetrics::default(),
        })
    }

    /// Create a builder for the time-series engine
    pub fn builder() -> TimeSeriesEngineBuilder {
        TimeSeriesEngineBuilder::new()
    }

    /// Add a sample to a time series
    pub fn add(&self, metric: &str, sample: Sample, labels: &[&str]) -> Result<()> {
        let parsed_labels = Labels::parse(labels)?;
        self.add_with_labels(metric, sample, parsed_labels)
    }

    /// Add a sample with parsed labels
    pub fn add_with_labels(&self, metric: &str, sample: Sample, labels: Labels) -> Result<()> {
        // Rate limiting check
        if let Some(limit) = self.config.max_samples_per_second {
            let current = self
                .metrics
                .samples_ingested
                .load(std::sync::atomic::Ordering::Relaxed);
            if current > limit {
                self.metrics
                    .samples_dropped
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err(TimeSeriesError::RateLimitExceeded(format!(
                    "exceeded {} samples/second",
                    limit
                )));
            }
        }

        // Retention policy enforcement on insert: reject expired samples
        if !self
            .retention_manager
            .should_retain(metric, sample.timestamp)
        {
            return Ok(()); // silently drop expired data
        }

        // Create or get series
        let series_id = TimeSeriesId::new(metric, &labels);
        self.storage
            .add_sample_with_labels(&series_id, sample.clone(), &labels)?;

        // Update label index
        self.label_index.write().add(&series_id.full_id, &labels);

        // Update metrics
        self.metrics
            .samples_ingested
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Update continuous aggregates if enabled
        if self.config.continuous_aggregates_enabled {
            self.update_continuous_aggregates(metric, &sample, &labels)?;
        }

        Ok(())
    }

    /// Add multiple samples in batch
    pub fn add_batch(&self, metric: &str, samples: Vec<(Sample, Labels)>) -> Result<usize> {
        let mut added = 0;
        for (sample, labels) in samples {
            if self.add_with_labels(metric, sample, labels).is_ok() {
                added += 1;
            }
        }
        Ok(added)
    }

    /// Create a new query builder
    pub fn query(&self) -> QueryBuilder {
        QueryBuilder::new(Arc::clone(&self.storage))
    }

    /// Execute a raw query string (FerriteQL time-series syntax)
    pub fn execute_query(&self, query: &str) -> Result<QueryResult> {
        let parsed = TimeSeriesQuery::parse(query)?;
        self.execute_parsed_query(parsed)
    }

    /// Execute a parsed query
    pub fn execute_parsed_query(&self, query: TimeSeriesQuery) -> Result<QueryResult> {
        self.metrics
            .queries_executed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.storage.execute_query(query)
    }

    /// Create a retention policy for a metric
    pub fn set_retention(&self, metric: &str, policy: RetentionPolicy) -> Result<()> {
        self.retention_manager.set_policy(metric, policy);
        Ok(())
    }

    /// Create a downsampling rule
    pub fn add_downsample_rule(&self, rule: DownsampleRule) -> Result<()> {
        self.downsampler.add_rule(rule);
        Ok(())
    }

    /// Create a continuous aggregate
    pub fn create_continuous_aggregate(
        &self,
        name: &str,
        config: ContinuousAggregateConfig,
    ) -> Result<()> {
        let aggregate = ContinuousAggregate::new(name.to_string(), config);
        self.continuous_aggregates
            .write()
            .insert(name.to_string(), aggregate);
        Ok(())
    }

    /// Get engine metrics
    pub fn metrics(&self) -> &TimeSeriesMetrics {
        &self.metrics
    }

    /// Get series count
    pub fn series_count(&self) -> u64 {
        self.metrics
            .series_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get total samples
    pub fn total_samples(&self) -> u64 {
        self.metrics
            .samples_ingested
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Run retention cleanup
    pub fn run_retention(&self) -> Result<u64> {
        self.retention_manager.run_cleanup(&self.storage)
    }

    /// Run downsampling
    pub fn run_downsample(&self) -> Result<u64> {
        self.downsampler.run(&self.storage)
    }

    /// Flush data to disk
    pub fn flush(&self) -> Result<()> {
        self.storage.flush()
    }

    /// Update continuous aggregates for a new sample
    fn update_continuous_aggregates(
        &self,
        metric: &str,
        sample: &Sample,
        labels: &Labels,
    ) -> Result<()> {
        let aggregates = self.continuous_aggregates.read();
        for aggregate in aggregates.values() {
            if aggregate.matches_metric(metric) {
                aggregate.add_sample(sample, labels)?;
            }
        }
        Ok(())
    }

    /// List all metrics
    pub fn list_metrics(&self) -> Result<Vec<String>> {
        self.storage.list_metrics()
    }

    /// Get metadata for a metric
    pub fn get_metric_metadata(&self, metric: &str) -> Result<TimeSeriesMetadata> {
        self.storage.get_metadata(metric)
    }

    /// Delete a metric and all its data
    pub fn delete_metric(&self, metric: &str) -> Result<()> {
        self.storage.delete_metric(metric)
    }

    /// Compact storage
    pub fn compact(&self) -> Result<()> {
        self.storage.compact()
    }

    /// Add a downsampling rule with source/dest key mapping
    pub fn add_downsampling_rule(&self, rule: downsample::DownsamplingRule) -> Result<()> {
        self.downsampler.add_downsampling_rule(rule);
        Ok(())
    }

    /// Execute an aggregation pipeline on matching series
    pub fn execute_pipeline(
        &self,
        metric_pattern: &str,
        label_matchers: &[LabelMatcher],
        pipeline: &AggregationPipeline,
    ) -> Result<Vec<PipelineSeries>> {
        let mut input = Vec::new();

        for (id, series) in self.storage.all_series() {
            // Match metric pattern
            if metric_pattern != "*" {
                if let Some(prefix) = metric_pattern.strip_suffix('*') {
                    if !id.metric().starts_with(prefix) {
                        continue;
                    }
                } else if id.metric() != metric_pattern {
                    continue;
                }
            }

            // Match labels
            if !label_matchers.is_empty() && !series.labels().matches(label_matchers) {
                continue;
            }

            input.push(PipelineSeries {
                group_key: id.full_id.clone(),
                labels: series.labels().clone(),
                samples: series.all_samples(),
            });
        }

        pipeline.execute(input)
    }

    /// Query series by label filter (MRANGE-style)
    pub fn mrange(
        &self,
        start: Timestamp,
        end: Timestamp,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<(TimeSeriesId, Vec<Sample>)>> {
        let mut results = Vec::new();

        for (id, series) in self.storage.all_series() {
            if !matchers.is_empty() && !series.labels().matches(matchers) {
                continue;
            }
            let samples = series.get_samples(start, end);
            if !samples.is_empty() {
                results.push((id, samples));
            }
        }

        Ok(results)
    }

    /// Get a reference to the label index
    pub fn label_index(&self) -> &RwLock<LabelIndex> {
        &self.label_index
    }

    /// Aggregate a single series over a time range with a bucket size.
    ///
    /// Equivalent to `TS.RANGE key FROM TO AGGREGATION avg 60000`.
    pub fn range_aggregate(
        &self,
        metric: &str,
        start: Timestamp,
        end: Timestamp,
        aggregation: Aggregation,
        bucket_duration: Duration,
    ) -> Result<Vec<Sample>> {
        let mut all_samples = Vec::new();

        for (id, series) in self.storage.all_series() {
            if id.metric() == metric {
                all_samples.extend(series.get_samples(start, end));
            }
        }

        if all_samples.is_empty() {
            return Ok(Vec::new());
        }

        all_samples.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

        let interval_nanos = bucket_duration.as_nanos() as i64;
        let mut buckets: std::collections::BTreeMap<i64, Vec<f64>> =
            std::collections::BTreeMap::new();

        for sample in &all_samples {
            let bucket_ts = (sample.timestamp.as_nanos() / interval_nanos) * interval_nanos;
            if let Some(v) = sample.as_f64() {
                buckets.entry(bucket_ts).or_default().push(v);
            }
        }

        let stat = aggregation.to_statistic();
        let mut result: Vec<Sample> = buckets
            .into_iter()
            .filter_map(|(ts, values)| {
                aggregate_slice(&values, stat).map(|v| Sample::new(Timestamp::from_nanos(ts), v))
            })
            .collect();

        result.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        Ok(result)
    }

    /// Aggregate multiple series matching label filters into a single result.
    ///
    /// Equivalent to `TS.MRANGE FROM TO AGGREGATION avg 60000 FILTER label=value`.
    pub fn multi_series_aggregate(
        &self,
        start: Timestamp,
        end: Timestamp,
        matchers: &[LabelMatcher],
        aggregation: Aggregation,
        bucket_duration: Duration,
    ) -> Result<Vec<Sample>> {
        let mut all_samples = Vec::new();

        for (_, series) in self.storage.all_series() {
            if !matchers.is_empty() && !series.labels().matches(matchers) {
                continue;
            }
            all_samples.extend(series.get_samples(start, end));
        }

        if all_samples.is_empty() {
            return Ok(Vec::new());
        }

        all_samples.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

        let interval_nanos = bucket_duration.as_nanos() as i64;
        let mut buckets: std::collections::BTreeMap<i64, Vec<f64>> =
            std::collections::BTreeMap::new();

        for sample in &all_samples {
            let bucket_ts = (sample.timestamp.as_nanos() / interval_nanos) * interval_nanos;
            if let Some(v) = sample.as_f64() {
                buckets.entry(bucket_ts).or_default().push(v);
            }
        }

        let stat = aggregation.to_statistic();
        let mut result: Vec<Sample> = buckets
            .into_iter()
            .filter_map(|(ts, values)| {
                aggregate_slice(&values, stat).map(|v| Sample::new(Timestamp::from_nanos(ts), v))
            })
            .collect();

        result.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        Ok(result)
    }
}

/// Aggregate a slice of f64 values using a Statistic
fn aggregate_slice(values: &[f64], stat: series::Statistic) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    match stat {
        series::Statistic::Sum => Some(values.iter().sum()),
        series::Statistic::Count => Some(values.len() as f64),
        series::Statistic::Min => values.iter().cloned().reduce(f64::min),
        series::Statistic::Max => values.iter().cloned().reduce(f64::max),
        series::Statistic::Avg => Some(values.iter().sum::<f64>() / values.len() as f64),
        series::Statistic::First => values.first().cloned(),
        series::Statistic::Last => values.last().cloned(),
        series::Statistic::StdDev => {
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let var = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
            Some(var.sqrt())
        }
        series::Statistic::Variance => {
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            Some(values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64)
        }
        series::Statistic::Percentile(p) => {
            let mut sorted = values.to_vec();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
            sorted.get(idx).cloned()
        }
        series::Statistic::Rate => None,
    }
}

impl Default for TimeSeriesEngine {
    fn default() -> Self {
        Self::new().expect("Failed to create default TimeSeriesEngine")
    }
}

/// Builder for TimeSeriesEngine
pub struct TimeSeriesEngineBuilder {
    config: TimeSeriesConfig,
}

impl TimeSeriesEngineBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: TimeSeriesConfig::default(),
        }
    }

    /// Set default retention duration
    pub fn retention(mut self, duration: Duration) -> Self {
        self.config.default_retention = duration;
        self
    }

    /// Set retention policy
    pub fn retention_policy(mut self, policy: RetentionPolicy) -> Self {
        self.config.default_retention = policy.duration();
        self
    }

    /// Enable/disable compression
    pub fn compression(mut self, enabled: bool) -> Self {
        self.config.compression_enabled = enabled;
        self
    }

    /// Set compression codec
    pub fn compression_codec(mut self, codec: CompressionCodec) -> Self {
        self.config.compression_codec = codec;
        self
    }

    /// Set maximum samples in memory
    pub fn max_samples_in_memory(mut self, max: usize) -> Self {
        self.config.max_samples_in_memory = max;
        self
    }

    /// Set flush interval
    pub fn flush_interval(mut self, interval: Duration) -> Self {
        self.config.flush_interval = interval;
        self
    }

    /// Set downsample interval (convenience method)
    pub fn downsample_interval(self, _interval: Duration) -> Self {
        // This will be configured through downsample rules
        self
    }

    /// Enable/disable continuous aggregates
    pub fn continuous_aggregates(mut self, enabled: bool) -> Self {
        self.config.continuous_aggregates_enabled = enabled;
        self
    }

    /// Set maximum number of series
    pub fn max_series(mut self, max: usize) -> Self {
        self.config.max_series = max;
        self
    }

    /// Set rate limit
    pub fn rate_limit(mut self, samples_per_second: u64) -> Self {
        self.config.max_samples_per_second = Some(samples_per_second);
        self
    }

    /// Build the engine
    pub fn build(self) -> Result<TimeSeriesEngine> {
        TimeSeriesEngine::with_config(self.config)
    }
}

impl Default for TimeSeriesEngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Background worker that periodically runs retention cleanup and downsampling.
///
/// Start via [`BackgroundWorker::start`] which spawns a tokio task. The worker
/// runs until the returned [`BackgroundWorkerHandle`] is dropped or
/// [`BackgroundWorkerHandle::shutdown`] is called.
pub struct BackgroundWorker;

impl BackgroundWorker {
    /// Start the background worker.
    ///
    /// `retention_interval` controls how often retention cleanup runs.
    /// `downsample_interval` controls how often downsampling runs.
    pub fn start(
        engine: Arc<TimeSeriesEngine>,
        retention_interval: Duration,
        downsample_interval: Duration,
    ) -> BackgroundWorkerHandle {
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let shutdown_clone = Arc::clone(&shutdown);

        let handle = tokio::spawn(async move {
            let mut retention_tick = tokio::time::interval(retention_interval);
            let mut downsample_tick = tokio::time::interval(downsample_interval);
            retention_tick.tick().await; // skip first immediate tick
            downsample_tick.tick().await;

            loop {
                if shutdown_clone.load(std::sync::atomic::Ordering::Relaxed) {
                    tracing::info!("Background worker shutting down");
                    break;
                }

                tokio::select! {
                    _ = retention_tick.tick() => {
                        match engine.run_retention() {
                            Ok(cleaned) => {
                                if cleaned > 0 {
                                    tracing::debug!("Retention cleanup removed {} samples", cleaned);
                                }
                            }
                            Err(e) => {
                                tracing::warn!("Retention cleanup failed: {}", e);
                            }
                        }
                    }
                    _ = downsample_tick.tick() => {
                        match engine.run_downsample() {
                            Ok(processed) => {
                                if processed > 0 {
                                    tracing::debug!("Downsampling processed {} samples", processed);
                                }
                            }
                            Err(e) => {
                                tracing::warn!("Downsampling failed: {}", e);
                            }
                        }
                    }
                }
            }
        });

        BackgroundWorkerHandle {
            shutdown,
            _handle: handle,
        }
    }
}

/// Handle to a running background worker. Dropping this handle signals shutdown.
pub struct BackgroundWorkerHandle {
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    _handle: tokio::task::JoinHandle<()>,
}

impl BackgroundWorkerHandle {
    /// Signal the worker to stop.
    pub fn shutdown(&self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_engine_creation() {
        let engine = TimeSeriesEngine::new().unwrap();
        assert_eq!(engine.series_count(), 0);
    }

    #[test]
    fn test_engine_builder() {
        let engine = TimeSeriesEngine::builder()
            .retention(Duration::from_secs(86400))
            .compression(true)
            .max_samples_in_memory(50000)
            .build()
            .unwrap();

        assert_eq!(engine.series_count(), 0);
    }

    #[test]
    fn test_add_sample() {
        let engine = TimeSeriesEngine::new().unwrap();

        let sample = Sample::now(42.0);
        engine.add("cpu.usage", sample, &["host:server1"]).unwrap();

        assert_eq!(engine.total_samples(), 1);
    }

    #[test]
    fn test_add_batch() {
        let engine = TimeSeriesEngine::new().unwrap();

        let samples: Vec<(Sample, Labels)> = (0..100)
            .map(|i| {
                let sample = Sample::now(i as f64);
                let labels = Labels::new(vec![Label::new("host", "server1")]);
                (sample, labels)
            })
            .collect();

        let added = engine.add_batch("cpu.usage", samples).unwrap();
        assert_eq!(added, 100);
    }

    #[test]
    fn test_query_builder() {
        let engine = TimeSeriesEngine::new().unwrap();

        // Add some samples
        for i in 0..10 {
            let sample = Sample::now(i as f64 * 10.0);
            engine
                .add("memory.usage", sample, &["host:server1"])
                .unwrap();
        }

        let query = engine.query().metric("memory.usage").build();

        assert!(query.is_ok());
    }

    #[test]
    fn test_retention_policy() {
        let engine = TimeSeriesEngine::new().unwrap();

        let policy = RetentionPolicy::new(Duration::from_secs(3600));
        engine.set_retention("cpu.usage", policy).unwrap();
    }

    #[test]
    fn test_list_metrics() {
        let engine = TimeSeriesEngine::new().unwrap();

        engine
            .add("cpu.usage", Sample::now(50.0), &["host:s1"])
            .unwrap();
        engine
            .add("memory.usage", Sample::now(60.0), &["host:s1"])
            .unwrap();

        let metrics = engine.list_metrics().unwrap();
        assert!(metrics.len() >= 2);
    }

    // --- Downsampling Rule Engine Tests ---

    #[test]
    fn test_downsampling_rule_via_engine() {
        let engine = TimeSeriesEngine::new().unwrap();
        let now = sample::Timestamp::now();

        // Add raw samples with recent timestamps
        for i in 0..6 {
            let ts = now.sub(Duration::from_secs(120 - i * 20));
            let sample = Sample::new(ts, (i * 10) as f64);
            engine.add("cpu.raw", sample, &["host:s1"]).unwrap();
        }

        // Add downsampling rule: cpu.raw -> cpu.1min_avg
        engine
            .add_downsampling_rule(downsample::DownsamplingRule::new(
                "cpu.raw",
                "cpu.1min_avg",
                Duration::from_secs(60),
                AggregationType::Avg,
            ))
            .unwrap();

        let processed = engine.run_downsample().unwrap();
        assert!(processed > 0);
    }

    #[test]
    fn test_downsampling_multiple_destinations() {
        let engine = TimeSeriesEngine::new().unwrap();
        let now = sample::Timestamp::now();

        for i in 0..6 {
            let ts = now.sub(Duration::from_secs(120 - i * 20));
            let sample = Sample::new(ts, (i * 10) as f64);
            engine.add("temp.raw", sample, &["sensor:a"]).unwrap();
        }

        engine
            .add_downsampling_rule(downsample::DownsamplingRule::new(
                "temp.raw",
                "temp.1min_avg",
                Duration::from_secs(60),
                AggregationType::Avg,
            ))
            .unwrap();

        engine
            .add_downsampling_rule(downsample::DownsamplingRule::new(
                "temp.raw",
                "temp.1min_max",
                Duration::from_secs(60),
                AggregationType::Max,
            ))
            .unwrap();

        engine.run_downsample().unwrap();

        let metrics = engine.list_metrics().unwrap();
        assert!(metrics.contains(&"temp.1min_avg".to_string()));
        assert!(metrics.contains(&"temp.1min_max".to_string()));
    }

    // --- Retention Policy Tests ---

    #[test]
    fn test_retention_with_max_samples() {
        let engine = TimeSeriesEngine::new().unwrap();

        let policy = RetentionPolicy::builder()
            .raw_retention(Duration::from_secs(86400 * 30))
            .max_samples(5)
            .build();

        engine.set_retention("test.metric", policy).unwrap();

        // Add 10 samples
        for i in 0..10 {
            engine
                .add("test.metric", Sample::from_secs(i, i as f64), &["host:s1"])
                .unwrap();
        }

        // Run retention - should trim to 5
        engine.run_retention().unwrap();
    }

    // --- Aggregation Pipeline Tests ---

    #[test]
    fn test_engine_execute_pipeline() {
        let engine = TimeSeriesEngine::new().unwrap();
        let now = sample::Timestamp::now();

        for i in 0..6 {
            let ts = now.sub(Duration::from_secs(120 - i * 20));
            engine
                .add(
                    "cpu.usage",
                    Sample::new(ts, (i * 10) as f64),
                    &["host:s1", "region:us-east"],
                )
                .unwrap();
        }

        let pipeline =
            AggregationPipeline::new().aggregate(Duration::from_secs(60), AggregationType::Avg);

        let result = engine
            .execute_pipeline("cpu.usage", &[], &pipeline)
            .unwrap();

        assert!(!result.is_empty());
    }

    #[test]
    fn test_engine_pipeline_with_filter() {
        let engine = TimeSeriesEngine::new().unwrap();
        let now = sample::Timestamp::now();

        for i in 0..5 {
            let ts = now.sub(Duration::from_secs(10 - i));
            engine
                .add(
                    "cpu.usage",
                    Sample::new(ts, (i * 10) as f64),
                    &["host:s1", "region:us-east"],
                )
                .unwrap();
        }
        for i in 0..5 {
            let ts = now.sub(Duration::from_secs(10 - i));
            engine
                .add(
                    "cpu.usage",
                    Sample::new(ts, (i * 20) as f64),
                    &["host:s2", "region:eu-west"],
                )
                .unwrap();
        }

        let pipeline = AggregationPipeline::new()
            .filter(pipeline::FilterExpr::LabelEqual(
                "region".into(),
                "us-east".into(),
            ))
            .aggregate(Duration::from_secs(60), AggregationType::Avg);

        let result = engine.execute_pipeline("cpu.*", &[], &pipeline).unwrap();

        // Should only have us-east series
        for series in &result {
            assert_eq!(series.labels.get("region"), Some("us-east"));
        }
    }

    #[test]
    fn test_engine_pipeline_chain() {
        let engine = TimeSeriesEngine::new().unwrap();
        let now = sample::Timestamp::now();

        for i in 0..10 {
            let ts = now.sub(Duration::from_secs(100 - i * 10));
            engine
                .add(
                    "temp.sensor",
                    Sample::new(ts, 20.0 + (i as f64)),
                    &["location:room1"],
                )
                .unwrap();
        }

        // Aggregate -> Apply (convert C to F)
        let pipeline = AggregationPipeline::new()
            .aggregate(Duration::from_secs(60), AggregationType::Avg)
            .apply(MathOp::Mul(1.8))
            .apply(MathOp::Add(32.0));

        let result = engine
            .execute_pipeline("temp.sensor", &[], &pipeline)
            .unwrap();

        assert!(!result.is_empty());
        for series in &result {
            for sample in &series.samples {
                // Should be in Fahrenheit range (>50)
                assert!(sample.as_f64().unwrap() > 50.0);
            }
        }
    }

    // --- MRANGE / Label Filtering Tests ---

    #[test]
    fn test_mrange_query() {
        let engine = TimeSeriesEngine::new().unwrap();
        let now = sample::Timestamp::now();

        for i in 1..=5 {
            let ts = now.sub(Duration::from_secs(10 - i));
            engine
                .add(
                    "cpu.usage",
                    Sample::new(ts, (i * 10) as f64),
                    &["host:s1", "region:us-east"],
                )
                .unwrap();
            engine
                .add(
                    "cpu.usage",
                    Sample::new(ts, (i * 20) as f64),
                    &["host:s2", "region:eu-west"],
                )
                .unwrap();
        }

        // Query only us-east
        let results = engine
            .mrange(
                now.sub(Duration::from_secs(100)),
                now.add(Duration::from_secs(100)),
                &[LabelMatcher::exact("region", "us-east")],
            )
            .unwrap();

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_label_index_integration() {
        let engine = TimeSeriesEngine::new().unwrap();

        engine
            .add("cpu.usage", Sample::now(50.0), &["host:s1", "region:us"])
            .unwrap();
        engine
            .add("cpu.usage", Sample::now(60.0), &["host:s2", "region:eu"])
            .unwrap();

        let index = engine.label_index().read();
        let us_series = index.find(&LabelMatcher::exact("region", "us"));
        assert!(!us_series.is_empty());
    }

    // --- Edge Case Tests ---

    #[test]
    fn test_empty_series_pipeline() {
        let engine = TimeSeriesEngine::new().unwrap();

        let pipeline =
            AggregationPipeline::new().aggregate(Duration::from_secs(60), AggregationType::Avg);

        let result = engine
            .execute_pipeline("nonexistent", &[], &pipeline)
            .unwrap();

        assert!(result.is_empty());
    }

    #[test]
    fn test_single_point_aggregation() {
        let engine = TimeSeriesEngine::new().unwrap();
        let now = sample::Timestamp::now();

        engine
            .add(
                "solo",
                Sample::new(now.sub(Duration::from_secs(1)), 100.0),
                &["host:s1"],
            )
            .unwrap();

        let pipeline =
            AggregationPipeline::new().aggregate(Duration::from_secs(60), AggregationType::Avg);

        let result = engine.execute_pipeline("solo", &[], &pipeline).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].samples.len(), 1);
        assert_eq!(result[0].samples[0].as_f64(), Some(100.0));
    }

    #[test]
    fn test_boundary_timestamps() {
        let engine = TimeSeriesEngine::new().unwrap();
        let now = sample::Timestamp::now();

        // Add samples at timestamps that will fall in different 60-second buckets
        engine
            .add(
                "boundary",
                Sample::new(now.sub(Duration::from_secs(120)), 50.0),
                &["host:s1"],
            )
            .unwrap();
        engine
            .add(
                "boundary",
                Sample::new(now.sub(Duration::from_secs(30)), 100.0),
                &["host:s1"],
            )
            .unwrap();

        let pipeline =
            AggregationPipeline::new().aggregate(Duration::from_secs(60), AggregationType::Avg);

        let result = engine.execute_pipeline("boundary", &[], &pipeline).unwrap();

        assert!(!result.is_empty());
        assert_eq!(result[0].samples.len(), 2);
    }

    // --- Range Aggregation Tests ---

    #[test]
    fn test_range_aggregate() {
        let engine = TimeSeriesEngine::new().unwrap();
        let now = sample::Timestamp::now();

        // Add 12 samples across 120 seconds
        for i in 0..12 {
            let ts = now.sub(Duration::from_secs(120 - i * 10));
            engine
                .add("cpu.usage", Sample::new(ts, (i * 10) as f64), &["host:s1"])
                .unwrap();
        }

        // Aggregate with 60-second buckets
        let result = engine
            .range_aggregate(
                "cpu.usage",
                now.sub(Duration::from_secs(130)),
                now.add(Duration::from_secs(10)),
                Aggregation::Avg,
                Duration::from_secs(60),
            )
            .unwrap();

        assert!(result.len() >= 2);
    }

    #[test]
    fn test_range_aggregate_sum() {
        let engine = TimeSeriesEngine::new().unwrap();
        let now = sample::Timestamp::now();

        for i in 0..3 {
            let ts = now.sub(Duration::from_secs(3 - i));
            engine
                .add("metric", Sample::new(ts, 10.0), &["host:s1"])
                .unwrap();
        }

        let result = engine
            .range_aggregate(
                "metric",
                now.sub(Duration::from_secs(10)),
                now.add(Duration::from_secs(10)),
                Aggregation::Sum,
                Duration::from_secs(60),
            )
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].as_f64(), Some(30.0));
    }

    #[test]
    fn test_range_aggregate_nonexistent_metric() {
        let engine = TimeSeriesEngine::new().unwrap();

        let result = engine
            .range_aggregate(
                "nonexistent",
                Timestamp::from_secs(0),
                Timestamp::from_secs(100),
                Aggregation::Avg,
                Duration::from_secs(60),
            )
            .unwrap();

        assert!(result.is_empty());
    }

    // --- Multi-Series Aggregation Tests ---

    #[test]
    fn test_multi_series_aggregate() {
        let engine = TimeSeriesEngine::new().unwrap();
        let now = sample::Timestamp::now();

        // Two series, same metric, different hosts
        for i in 0..5 {
            let ts = now.sub(Duration::from_secs(5 - i));
            engine
                .add(
                    "cpu.usage",
                    Sample::new(ts, 10.0),
                    &["host:s1", "region:us"],
                )
                .unwrap();
            engine
                .add(
                    "cpu.usage",
                    Sample::new(ts, 20.0),
                    &["host:s2", "region:us"],
                )
                .unwrap();
        }

        // Aggregate both series matching region=us
        let result = engine
            .multi_series_aggregate(
                now.sub(Duration::from_secs(10)),
                now.add(Duration::from_secs(10)),
                &[LabelMatcher::exact("region", "us")],
                Aggregation::Avg,
                Duration::from_secs(60),
            )
            .unwrap();

        assert_eq!(result.len(), 1);
        // avg(10,10,10,10,10,20,20,20,20,20) = 15.0
        assert_eq!(result[0].as_f64(), Some(15.0));
    }

    #[test]
    fn test_multi_series_aggregate_with_filter() {
        let engine = TimeSeriesEngine::new().unwrap();
        let now = sample::Timestamp::now();

        for i in 0..3 {
            let ts = now.sub(Duration::from_secs(3 - i));
            engine
                .add("temp", Sample::new(ts, 100.0), &["region:us"])
                .unwrap();
            engine
                .add("temp", Sample::new(ts, 200.0), &["region:eu"])
                .unwrap();
        }

        // Only EU
        let result = engine
            .multi_series_aggregate(
                now.sub(Duration::from_secs(10)),
                now.add(Duration::from_secs(10)),
                &[LabelMatcher::exact("region", "eu")],
                Aggregation::Avg,
                Duration::from_secs(60),
            )
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].as_f64(), Some(200.0));
    }

    // --- BackgroundWorker Tests ---

    #[tokio::test]
    async fn test_background_worker_start_shutdown() {
        let engine = Arc::new(TimeSeriesEngine::new().unwrap());

        let handle = BackgroundWorker::start(
            Arc::clone(&engine),
            Duration::from_millis(100),
            Duration::from_millis(100),
        );

        // Add some data
        engine
            .add("bg.test", Sample::now(42.0), &["host:s1"])
            .unwrap();

        // Let it run briefly
        tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;

        // Shutdown
        handle.shutdown();
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
    }
}
