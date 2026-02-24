//! Time Series data structures
//!
//! Provides the core time series container and identification types.

use super::labels::Labels;
use super::sample::{Sample, Timestamp};
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::Arc;

/// Unique identifier for a time series
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TimeSeriesId {
    /// Metric name
    pub metric: String,
    /// Sorted label fingerprint
    pub fingerprint: u64,
    /// Full ID string (metric{labels})
    pub full_id: String,
}

impl TimeSeriesId {
    /// Create a new time series ID
    pub fn new(metric: &str, labels: &Labels) -> Self {
        let fingerprint = labels.fingerprint();
        let full_id = format!("{}{{{}}}", metric, labels);

        Self {
            metric: metric.to_string(),
            fingerprint,
            full_id,
        }
    }

    /// Create from metric name only
    pub fn from_metric(metric: &str) -> Self {
        Self {
            metric: metric.to_string(),
            fingerprint: 0,
            full_id: format!("{}{{}}", metric),
        }
    }

    /// Parse from string format "metric{label=value,...}"
    pub fn parse(s: &str) -> Option<Self> {
        if let Some(idx) = s.find('{') {
            let metric = &s[..idx];
            let labels_str = &s[idx + 1..s.len() - 1];
            let labels = Labels::parse_string(labels_str).ok()?;
            Some(Self::new(metric, &labels))
        } else {
            Some(Self::from_metric(s))
        }
    }

    /// Get the metric name
    pub fn metric(&self) -> &str {
        &self.metric
    }

    /// Get the fingerprint
    pub fn fingerprint(&self) -> u64 {
        self.fingerprint
    }
}

impl std::fmt::Display for TimeSeriesId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.full_id)
    }
}

/// Metadata about a time series
#[derive(Debug, Clone)]
pub struct TimeSeriesMetadata {
    /// Creation time
    pub created_at: Timestamp,
    /// Last update time
    pub last_updated: Timestamp,
    /// Total sample count
    pub sample_count: u64,
    /// Minimum timestamp
    pub min_timestamp: Option<Timestamp>,
    /// Maximum timestamp
    pub max_timestamp: Option<Timestamp>,
    /// Labels
    pub labels: Labels,
    /// Description
    pub description: Option<String>,
    /// Unit of measurement
    pub unit: Option<String>,
    /// Data type
    pub data_type: DataType,
}

impl TimeSeriesMetadata {
    /// Create new metadata
    pub fn new(labels: Labels) -> Self {
        let now = Timestamp::now();
        Self {
            created_at: now,
            last_updated: now,
            sample_count: 0,
            min_timestamp: None,
            max_timestamp: None,
            labels,
            description: None,
            unit: None,
            data_type: DataType::Gauge,
        }
    }

    /// Update metadata with a new sample
    pub fn update_with_sample(&mut self, sample: &Sample) {
        self.last_updated = Timestamp::now();
        self.sample_count += 1;

        match self.min_timestamp {
            Some(min) if sample.timestamp.is_before(&min) => {
                self.min_timestamp = Some(sample.timestamp);
            }
            None => {
                self.min_timestamp = Some(sample.timestamp);
            }
            _ => {}
        }

        match self.max_timestamp {
            Some(max) if sample.timestamp.is_after(&max) => {
                self.max_timestamp = Some(sample.timestamp);
            }
            None => {
                self.max_timestamp = Some(sample.timestamp);
            }
            _ => {}
        }
    }

    /// Get the time range of the series
    pub fn time_range(&self) -> Option<(Timestamp, Timestamp)> {
        match (self.min_timestamp, self.max_timestamp) {
            (Some(min), Some(max)) => Some((min, max)),
            _ => None,
        }
    }
}

impl Default for TimeSeriesMetadata {
    fn default() -> Self {
        Self::new(Labels::empty())
    }
}

/// Type of time series data
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DataType {
    /// Gauge - can go up and down
    #[default]
    Gauge,
    /// Counter - only increases (or resets)
    Counter,
    /// Histogram - distribution of values
    Histogram,
    /// Summary - similar to histogram with quantiles
    Summary,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Gauge => write!(f, "gauge"),
            DataType::Counter => write!(f, "counter"),
            DataType::Histogram => write!(f, "histogram"),
            DataType::Summary => write!(f, "summary"),
        }
    }
}

/// A time series containing samples
#[derive(Debug)]
pub struct TimeSeries {
    /// Series ID
    id: TimeSeriesId,
    /// Metadata
    metadata: RwLock<TimeSeriesMetadata>,
    /// In-memory samples (recent data)
    samples: RwLock<VecDeque<Sample>>,
    /// Maximum samples to keep in memory
    max_samples: usize,
    /// Labels
    labels: Labels,
}

impl TimeSeries {
    /// Create a new time series
    pub fn new(id: TimeSeriesId, labels: Labels) -> Self {
        Self {
            id,
            metadata: RwLock::new(TimeSeriesMetadata::new(labels.clone())),
            samples: RwLock::new(VecDeque::new()),
            max_samples: 10_000,
            labels,
        }
    }

    /// Create with custom max samples
    pub fn with_max_samples(id: TimeSeriesId, labels: Labels, max_samples: usize) -> Self {
        Self {
            id,
            metadata: RwLock::new(TimeSeriesMetadata::new(labels.clone())),
            samples: RwLock::new(VecDeque::new()),
            max_samples,
            labels,
        }
    }

    /// Get the series ID
    pub fn id(&self) -> &TimeSeriesId {
        &self.id
    }

    /// Get the labels
    pub fn labels(&self) -> &Labels {
        &self.labels
    }

    /// Get the metric name
    pub fn metric(&self) -> &str {
        &self.id.metric
    }

    /// Add a sample
    pub fn add_sample(&self, sample: Sample) {
        let mut samples = self.samples.write();

        // Maintain sorted order
        if let Some(last) = samples.back() {
            if sample.timestamp.is_before(&last.timestamp) {
                // Insert in sorted position
                let pos = samples
                    .iter()
                    .position(|s| sample.timestamp.is_before(&s.timestamp))
                    .unwrap_or(samples.len());
                samples.insert(pos, sample.clone());
            } else {
                samples.push_back(sample.clone());
            }
        } else {
            samples.push_back(sample.clone());
        }

        // Evict oldest if over limit
        while samples.len() > self.max_samples {
            samples.pop_front();
        }

        // Update metadata
        self.metadata.write().update_with_sample(&sample);
    }

    /// Get samples in a time range
    pub fn get_samples(&self, start: Timestamp, end: Timestamp) -> Vec<Sample> {
        self.samples
            .read()
            .iter()
            .filter(|s| {
                (s.timestamp.is_after(&start) || s.timestamp == start)
                    && (s.timestamp.is_before(&end) || s.timestamp == end)
            })
            .cloned()
            .collect()
    }

    /// Get all samples
    pub fn all_samples(&self) -> Vec<Sample> {
        self.samples.read().iter().cloned().collect()
    }

    /// Get the latest sample
    pub fn latest(&self) -> Option<Sample> {
        self.samples.read().back().cloned()
    }

    /// Get the oldest sample
    pub fn oldest(&self) -> Option<Sample> {
        self.samples.read().front().cloned()
    }

    /// Get sample count
    pub fn len(&self) -> usize {
        self.samples.read().len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.samples.read().is_empty()
    }

    /// Get metadata
    pub fn metadata(&self) -> TimeSeriesMetadata {
        self.metadata.read().clone()
    }

    /// Clear all samples
    pub fn clear(&self) {
        self.samples.write().clear();
    }

    /// Remove samples older than the given timestamp
    /// Returns the number of samples removed
    pub fn remove_before(&self, cutoff: Timestamp) -> usize {
        let mut samples = self.samples.write();
        let original_len = samples.len();

        // Remove from front while samples are before cutoff
        while let Some(front) = samples.front() {
            if front.timestamp.is_before(&cutoff) {
                samples.pop_front();
            } else {
                break;
            }
        }

        original_len - samples.len()
    }

    /// Remove samples and return them for archiving
    pub fn drain_before(&self, cutoff: Timestamp) -> Vec<Sample> {
        let mut samples = self.samples.write();
        let mut drained = Vec::new();

        while let Some(front) = samples.front() {
            if front.timestamp.is_before(&cutoff) {
                if let Some(sample) = samples.pop_front() {
                    drained.push(sample);
                }
            } else {
                break;
            }
        }

        drained
    }

    /// Trim the oldest N samples from the series
    /// Returns the number of samples actually trimmed
    pub fn trim_oldest(&self, count: usize) -> usize {
        let mut samples = self.samples.write();
        let to_remove = count.min(samples.len());
        for _ in 0..to_remove {
            samples.pop_front();
        }
        to_remove
    }

    /// Compute a statistic over the series
    pub fn compute(&self, stat: Statistic) -> Option<f64> {
        let samples = self.samples.read();
        if samples.is_empty() {
            return None;
        }

        let values: Vec<f64> = samples.iter().filter_map(|s| s.as_f64()).collect();
        if values.is_empty() {
            return None;
        }

        match stat {
            Statistic::Sum => Some(values.iter().sum()),
            Statistic::Count => Some(values.len() as f64),
            Statistic::Min => values.iter().cloned().reduce(f64::min),
            Statistic::Max => values.iter().cloned().reduce(f64::max),
            Statistic::Avg => {
                let sum: f64 = values.iter().sum();
                Some(sum / values.len() as f64)
            }
            Statistic::First => values.first().cloned(),
            Statistic::Last => values.last().cloned(),
            Statistic::StdDev => {
                let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
                let variance: f64 =
                    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
                Some(variance.sqrt())
            }
            Statistic::Variance => {
                let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
                Some(values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64)
            }
            Statistic::Percentile(p) => {
                let mut sorted = values.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
                sorted.get(idx).cloned()
            }
            Statistic::Rate => {
                if values.len() < 2 {
                    return None;
                }
                let first_sample = samples.front()?;
                let last_sample = samples.back()?;
                let time_diff = last_sample
                    .timestamp
                    .diff(&first_sample.timestamp)
                    .as_secs_f64();
                if time_diff == 0.0 {
                    return None;
                }
                let value_diff = last_sample.as_f64()? - first_sample.as_f64()?;
                Some(value_diff / time_diff)
            }
        }
    }

    /// Compute a moving average over a sliding window of N samples
    pub fn moving_average(&self, window_size: usize) -> Vec<Sample> {
        let samples = self.samples.read();
        if samples.is_empty() || window_size == 0 {
            return Vec::new();
        }

        let values: Vec<(Timestamp, f64)> = samples
            .iter()
            .filter_map(|s| s.as_f64().map(|v| (s.timestamp, v)))
            .collect();

        if values.len() < window_size {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(values.len() - window_size + 1);
        let mut window_sum: f64 = values[..window_size].iter().map(|(_, v)| v).sum();

        result.push(Sample::new(
            values[window_size - 1].0,
            window_sum / window_size as f64,
        ));

        for i in window_size..values.len() {
            window_sum += values[i].1 - values[i - window_size].1;
            result.push(Sample::new(values[i].0, window_sum / window_size as f64));
        }

        result
    }

    /// Compute per-second rate of change between consecutive samples
    pub fn rate(&self) -> Vec<Sample> {
        let samples = self.samples.read();
        if samples.len() < 2 {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(samples.len() - 1);
        let mut prev: Option<&Sample> = None;

        for sample in samples.iter() {
            if let Some(p) = prev {
                if let (Some(v1), Some(v2)) = (p.as_f64(), sample.as_f64()) {
                    let dt = sample.timestamp.diff(&p.timestamp).as_secs_f64();
                    if dt > 0.0 {
                        result.push(Sample::new(sample.timestamp, (v2 - v1) / dt));
                    }
                }
            }
            prev = Some(sample);
        }

        result
    }

    /// Downsample the series
    pub fn downsample(&self, interval: std::time::Duration, aggregation: Statistic) -> Vec<Sample> {
        let samples = self.samples.read();
        if samples.is_empty() {
            return Vec::new();
        }

        let mut result = Vec::new();
        let mut bucket_start = samples.front().map(|s| s.timestamp.truncate(interval));
        let mut bucket_values: Vec<f64> = Vec::new();

        for sample in samples.iter() {
            let sample_bucket = sample.timestamp.truncate(interval);

            if Some(sample_bucket) != bucket_start {
                // Process previous bucket
                if !bucket_values.is_empty() {
                    if let Some(agg_value) = aggregate_values(&bucket_values, aggregation) {
                        if let Some(bs) = bucket_start {
                            result.push(Sample::new(bs, agg_value));
                        }
                    }
                }
                bucket_start = Some(sample_bucket);
                bucket_values.clear();
            }

            if let Some(v) = sample.as_f64() {
                bucket_values.push(v);
            }
        }

        // Process last bucket
        if !bucket_values.is_empty() {
            if let Some(agg_value) = aggregate_values(&bucket_values, aggregation) {
                if let Some(bs) = bucket_start {
                    result.push(Sample::new(bs, agg_value));
                }
            }
        }

        result
    }
}

/// Statistics that can be computed over a series
#[derive(Debug, Clone, Copy)]
pub enum Statistic {
    /// Sum of values
    Sum,
    /// Count of values
    Count,
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// Average value
    Avg,
    /// First value
    First,
    /// Last value
    Last,
    /// Standard deviation
    StdDev,
    /// Variance
    Variance,
    /// Percentile (0-100)
    Percentile(f64),
    /// Rate of change (per second)
    Rate,
}

/// Aggregate a vector of values
fn aggregate_values(values: &[f64], stat: Statistic) -> Option<f64> {
    if values.is_empty() {
        return None;
    }

    match stat {
        Statistic::Sum => Some(values.iter().sum()),
        Statistic::Count => Some(values.len() as f64),
        Statistic::Min => values.iter().cloned().reduce(f64::min),
        Statistic::Max => values.iter().cloned().reduce(f64::max),
        Statistic::Avg => Some(values.iter().sum::<f64>() / values.len() as f64),
        Statistic::First => values.first().cloned(),
        Statistic::Last => values.last().cloned(),
        Statistic::StdDev => {
            let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
            let variance: f64 =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
            Some(variance.sqrt())
        }
        Statistic::Variance => {
            let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
            Some(values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64)
        }
        Statistic::Percentile(p) => {
            let mut sorted = values.to_vec();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
            sorted.get(idx).cloned()
        }
        Statistic::Rate => {
            // Rate doesn't make sense for aggregated values
            None
        }
    }
}

/// A collection of time series sharing a metric name
#[derive(Debug)]
pub struct TimeSeriesSet {
    /// Metric name
    metric: String,
    /// Series by fingerprint
    series: RwLock<std::collections::HashMap<u64, Arc<TimeSeries>>>,
}

impl TimeSeriesSet {
    /// Create a new time series set
    pub fn new(metric: impl Into<String>) -> Self {
        Self {
            metric: metric.into(),
            series: RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Get or create a series
    pub fn get_or_create(&self, id: &TimeSeriesId, labels: Labels) -> Arc<TimeSeries> {
        let mut series = self.series.write();
        series
            .entry(id.fingerprint)
            .or_insert_with(|| Arc::new(TimeSeries::new(id.clone(), labels)))
            .clone()
    }

    /// Get a series by fingerprint
    pub fn get(&self, fingerprint: u64) -> Option<Arc<TimeSeries>> {
        self.series.read().get(&fingerprint).cloned()
    }

    /// Get all series
    pub fn all(&self) -> Vec<Arc<TimeSeries>> {
        self.series.read().values().cloned().collect()
    }

    /// Get series count
    pub fn len(&self) -> usize {
        self.series.read().len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.series.read().is_empty()
    }

    /// Remove a series
    pub fn remove(&self, fingerprint: u64) -> Option<Arc<TimeSeries>> {
        self.series.write().remove(&fingerprint)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_time_series_id() {
        let labels = Labels::parse(&["host:server1", "env:prod"]).unwrap();
        let id = TimeSeriesId::new("cpu.usage", &labels);

        assert_eq!(id.metric(), "cpu.usage");
        assert!(id.full_id.contains("cpu.usage"));
    }

    #[test]
    fn test_time_series_id_parse() {
        let id = TimeSeriesId::parse("cpu.usage{host=server1}").unwrap();
        assert_eq!(id.metric(), "cpu.usage");
    }

    #[test]
    fn test_time_series_add_sample() {
        let id = TimeSeriesId::from_metric("test");
        let series = TimeSeries::new(id, Labels::empty());

        series.add_sample(Sample::now(42.0));
        series.add_sample(Sample::now(43.0));

        assert_eq!(series.len(), 2);
    }

    #[test]
    fn test_time_series_statistics() {
        let id = TimeSeriesId::from_metric("test");
        let series = TimeSeries::new(id, Labels::empty());

        for i in 1..=10 {
            series.add_sample(Sample::from_secs(i, i as f64));
        }

        assert_eq!(series.compute(Statistic::Sum), Some(55.0));
        assert_eq!(series.compute(Statistic::Count), Some(10.0));
        assert_eq!(series.compute(Statistic::Min), Some(1.0));
        assert_eq!(series.compute(Statistic::Max), Some(10.0));
        assert_eq!(series.compute(Statistic::Avg), Some(5.5));
        assert_eq!(series.compute(Statistic::First), Some(1.0));
        assert_eq!(series.compute(Statistic::Last), Some(10.0));
    }

    #[test]
    fn test_time_series_downsample() {
        let id = TimeSeriesId::from_metric("test");
        let series = TimeSeries::new(id, Labels::empty());

        // Add samples at 1-second intervals for 10 seconds
        for i in 0..10 {
            series.add_sample(Sample::from_secs(i, (i * 10) as f64));
        }

        // Downsample to 5-second buckets
        let downsampled = series.downsample(Duration::from_secs(5), Statistic::Avg);

        assert_eq!(downsampled.len(), 2);
    }

    #[test]
    fn test_time_series_get_range() {
        let id = TimeSeriesId::from_metric("test");
        let series = TimeSeries::new(id, Labels::empty());

        for i in 1..=10 {
            series.add_sample(Sample::from_secs(i, i as f64));
        }

        let range = series.get_samples(Timestamp::from_secs(3), Timestamp::from_secs(7));

        assert_eq!(range.len(), 5); // 3, 4, 5, 6, 7
    }

    #[test]
    fn test_time_series_set() {
        let set = TimeSeriesSet::new("cpu.usage");

        let labels1 = Labels::parse(&["host:s1"]).unwrap();
        let id1 = TimeSeriesId::new("cpu.usage", &labels1);
        let series1 = set.get_or_create(&id1, labels1);

        let labels2 = Labels::parse(&["host:s2"]).unwrap();
        let id2 = TimeSeriesId::new("cpu.usage", &labels2);
        let series2 = set.get_or_create(&id2, labels2);

        assert_eq!(set.len(), 2);
        assert!(set.get(id1.fingerprint).is_some());
        assert!(set.get(id2.fingerprint).is_some());
    }

    #[test]
    fn test_metadata_update() {
        let id = TimeSeriesId::from_metric("test");
        let series = TimeSeries::new(id, Labels::empty());

        series.add_sample(Sample::from_secs(5, 50.0));
        series.add_sample(Sample::from_secs(10, 100.0));

        let metadata = series.metadata();
        assert_eq!(metadata.sample_count, 2);
        assert_eq!(metadata.min_timestamp.map(|t| t.as_secs()), Some(5));
        assert_eq!(metadata.max_timestamp.map(|t| t.as_secs()), Some(10));
    }

    #[test]
    fn test_moving_average() {
        let id = TimeSeriesId::from_metric("test");
        let series = TimeSeries::new(id, Labels::empty());

        for i in 1..=10 {
            series.add_sample(Sample::from_secs(i, i as f64));
        }

        let ma = series.moving_average(3);
        // Window 3: [1,2,3]=2.0, [2,3,4]=3.0, ... [8,9,10]=9.0 → 8 points
        assert_eq!(ma.len(), 8);
        assert_eq!(ma[0].as_f64(), Some(2.0)); // avg(1,2,3)
        assert_eq!(ma[7].as_f64(), Some(9.0)); // avg(8,9,10)
    }

    #[test]
    fn test_moving_average_window_larger_than_data() {
        let id = TimeSeriesId::from_metric("test");
        let series = TimeSeries::new(id, Labels::empty());

        series.add_sample(Sample::from_secs(1, 10.0));
        series.add_sample(Sample::from_secs(2, 20.0));

        let ma = series.moving_average(5);
        assert!(ma.is_empty());
    }

    #[test]
    fn test_moving_average_window_one() {
        let id = TimeSeriesId::from_metric("test");
        let series = TimeSeries::new(id, Labels::empty());

        for i in 1..=5 {
            series.add_sample(Sample::from_secs(i, i as f64 * 10.0));
        }

        let ma = series.moving_average(1);
        assert_eq!(ma.len(), 5);
        assert_eq!(ma[0].as_f64(), Some(10.0));
        assert_eq!(ma[4].as_f64(), Some(50.0));
    }

    #[test]
    fn test_rate() {
        let id = TimeSeriesId::from_metric("counter");
        let series = TimeSeries::new(id, Labels::empty());

        // Counter incrementing at 10/sec
        series.add_sample(Sample::from_secs(0, 0.0));
        series.add_sample(Sample::from_secs(1, 10.0));
        series.add_sample(Sample::from_secs(2, 20.0));
        series.add_sample(Sample::from_secs(3, 30.0));

        let rates = series.rate();
        assert_eq!(rates.len(), 3);
        for r in &rates {
            assert_eq!(r.as_f64(), Some(10.0));
        }
    }

    #[test]
    fn test_rate_empty_and_single() {
        let id = TimeSeriesId::from_metric("test");
        let series = TimeSeries::new(id, Labels::empty());

        assert!(series.rate().is_empty());

        series.add_sample(Sample::from_secs(1, 100.0));
        assert!(series.rate().is_empty());
    }

    #[test]
    fn test_rate_variable_intervals() {
        let id = TimeSeriesId::from_metric("test");
        let series = TimeSeries::new(id, Labels::empty());

        series.add_sample(Sample::from_secs(0, 0.0));
        series.add_sample(Sample::from_secs(10, 100.0)); // 10 per sec
        series.add_sample(Sample::from_secs(20, 150.0)); // 5 per sec

        let rates = series.rate();
        assert_eq!(rates.len(), 2);
        assert_eq!(rates[0].as_f64(), Some(10.0));
        assert_eq!(rates[1].as_f64(), Some(5.0));
    }
}
