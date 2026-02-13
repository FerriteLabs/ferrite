//! Aggregation Functions for Time Series
//!
//! Provides various aggregation functions for time series data.

use super::sample::{Sample, Timestamp};
use super::series::Statistic;
use std::collections::HashMap;

/// Aggregation types
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Aggregation {
    /// Sum of values
    Sum,
    /// Count of values
    Count,
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// Average/mean value
    Avg,
    /// First value in window
    First,
    /// Last value in window
    Last,
    /// Standard deviation
    StdDev,
    /// Variance
    Variance,
    /// Median (50th percentile)
    Median,
    /// 90th percentile
    P90,
    /// 95th percentile
    P95,
    /// 99th percentile
    P99,
    /// Rate of change per second
    Rate,
    /// Increase (non-negative rate)
    Increase,
    /// Delta (difference)
    Delta,
    /// Absent (1 if no data)
    Absent,
    /// Count of non-null values
    CountValues,
    /// Quantile (0-1)
    Quantile(f64),
}

impl Aggregation {
    /// Convert to statistic for computation
    pub fn to_statistic(&self) -> Statistic {
        match self {
            Aggregation::Sum => Statistic::Sum,
            Aggregation::Count => Statistic::Count,
            Aggregation::Min => Statistic::Min,
            Aggregation::Max => Statistic::Max,
            Aggregation::Avg => Statistic::Avg,
            Aggregation::First => Statistic::First,
            Aggregation::Last => Statistic::Last,
            Aggregation::StdDev => Statistic::StdDev,
            Aggregation::Variance => Statistic::Variance,
            Aggregation::Median => Statistic::Percentile(50.0),
            Aggregation::P90 => Statistic::Percentile(90.0),
            Aggregation::P95 => Statistic::Percentile(95.0),
            Aggregation::P99 => Statistic::Percentile(99.0),
            Aggregation::Rate => Statistic::Rate,
            Aggregation::Increase => Statistic::Rate, // Will be converted in processing
            Aggregation::Delta => Statistic::Last,    // Will be converted in processing
            Aggregation::Absent => Statistic::Count,
            Aggregation::CountValues => Statistic::Count,
            Aggregation::Quantile(q) => Statistic::Percentile(*q * 100.0),
        }
    }

    /// Parse from string
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "sum" => Some(Aggregation::Sum),
            "count" => Some(Aggregation::Count),
            "min" => Some(Aggregation::Min),
            "max" => Some(Aggregation::Max),
            "avg" | "mean" | "average" => Some(Aggregation::Avg),
            "first" => Some(Aggregation::First),
            "last" => Some(Aggregation::Last),
            "stddev" | "std_dev" => Some(Aggregation::StdDev),
            "variance" | "var" => Some(Aggregation::Variance),
            "median" => Some(Aggregation::Median),
            "p90" => Some(Aggregation::P90),
            "p95" => Some(Aggregation::P95),
            "p99" => Some(Aggregation::P99),
            "rate" => Some(Aggregation::Rate),
            "increase" => Some(Aggregation::Increase),
            "delta" => Some(Aggregation::Delta),
            "absent" => Some(Aggregation::Absent),
            "count_values" => Some(Aggregation::CountValues),
            _ if s.starts_with("quantile(") => {
                let inner = s.trim_start_matches("quantile(").trim_end_matches(')');
                inner.parse::<f64>().ok().map(Aggregation::Quantile)
            }
            _ => None,
        }
    }
}

impl std::fmt::Display for Aggregation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Aggregation::Sum => write!(f, "sum"),
            Aggregation::Count => write!(f, "count"),
            Aggregation::Min => write!(f, "min"),
            Aggregation::Max => write!(f, "max"),
            Aggregation::Avg => write!(f, "avg"),
            Aggregation::First => write!(f, "first"),
            Aggregation::Last => write!(f, "last"),
            Aggregation::StdDev => write!(f, "stddev"),
            Aggregation::Variance => write!(f, "variance"),
            Aggregation::Median => write!(f, "median"),
            Aggregation::P90 => write!(f, "p90"),
            Aggregation::P95 => write!(f, "p95"),
            Aggregation::P99 => write!(f, "p99"),
            Aggregation::Rate => write!(f, "rate"),
            Aggregation::Increase => write!(f, "increase"),
            Aggregation::Delta => write!(f, "delta"),
            Aggregation::Absent => write!(f, "absent"),
            Aggregation::CountValues => write!(f, "count_values"),
            Aggregation::Quantile(q) => write!(f, "quantile({})", q),
        }
    }
}

/// Result of an aggregation
#[derive(Debug, Clone)]
pub struct AggregationResult {
    /// Aggregation type used
    pub aggregation: Aggregation,
    /// Result value
    pub value: Option<f64>,
    /// Number of samples aggregated
    pub sample_count: usize,
    /// Start timestamp of aggregation window
    pub start_time: Option<Timestamp>,
    /// End timestamp of aggregation window
    pub end_time: Option<Timestamp>,
}

impl AggregationResult {
    /// Create a new aggregation result
    pub fn new(aggregation: Aggregation, value: Option<f64>, sample_count: usize) -> Self {
        Self {
            aggregation,
            value,
            sample_count,
            start_time: None,
            end_time: None,
        }
    }

    /// Create with time bounds
    pub fn with_bounds(
        aggregation: Aggregation,
        value: Option<f64>,
        sample_count: usize,
        start_time: Timestamp,
        end_time: Timestamp,
    ) -> Self {
        Self {
            aggregation,
            value,
            sample_count,
            start_time: Some(start_time),
            end_time: Some(end_time),
        }
    }
}

/// Aggregation function trait
pub trait AggregationFunction: Send + Sync {
    /// Name of the function
    fn name(&self) -> &str;

    /// Aggregate a slice of values
    fn aggregate(&self, values: &[f64]) -> Option<f64>;

    /// Aggregate with timestamps (for rate-based functions)
    fn aggregate_with_time(&self, samples: &[Sample]) -> Option<f64> {
        let values: Vec<f64> = samples.iter().filter_map(|s| s.as_f64()).collect();
        self.aggregate(&values)
    }

    /// Check if function requires ordered data
    fn requires_order(&self) -> bool {
        false
    }

    /// Check if function is idempotent
    fn is_idempotent(&self) -> bool {
        false
    }
}

/// Sum aggregation function
pub struct SumFunction;

impl AggregationFunction for SumFunction {
    fn name(&self) -> &str {
        "sum"
    }

    fn aggregate(&self, values: &[f64]) -> Option<f64> {
        if values.is_empty() {
            None
        } else {
            Some(values.iter().sum())
        }
    }

    fn is_idempotent(&self) -> bool {
        false
    }
}

/// Average aggregation function
pub struct AvgFunction;

impl AggregationFunction for AvgFunction {
    fn name(&self) -> &str {
        "avg"
    }

    fn aggregate(&self, values: &[f64]) -> Option<f64> {
        if values.is_empty() {
            None
        } else {
            Some(values.iter().sum::<f64>() / values.len() as f64)
        }
    }
}

/// Min aggregation function
pub struct MinFunction;

impl AggregationFunction for MinFunction {
    fn name(&self) -> &str {
        "min"
    }

    fn aggregate(&self, values: &[f64]) -> Option<f64> {
        values.iter().cloned().reduce(f64::min)
    }

    fn is_idempotent(&self) -> bool {
        true
    }
}

/// Max aggregation function
pub struct MaxFunction;

impl AggregationFunction for MaxFunction {
    fn name(&self) -> &str {
        "max"
    }

    fn aggregate(&self, values: &[f64]) -> Option<f64> {
        values.iter().cloned().reduce(f64::max)
    }

    fn is_idempotent(&self) -> bool {
        true
    }
}

/// Count aggregation function
pub struct CountFunction;

impl AggregationFunction for CountFunction {
    fn name(&self) -> &str {
        "count"
    }

    fn aggregate(&self, values: &[f64]) -> Option<f64> {
        Some(values.len() as f64)
    }
}

/// Standard deviation aggregation function
pub struct StdDevFunction;

impl AggregationFunction for StdDevFunction {
    fn name(&self) -> &str {
        "stddev"
    }

    fn aggregate(&self, values: &[f64]) -> Option<f64> {
        if values.is_empty() {
            return None;
        }
        let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
        let variance: f64 =
            values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
        Some(variance.sqrt())
    }
}

/// Percentile aggregation function
pub struct PercentileFunction {
    percentile: f64,
}

impl PercentileFunction {
    pub fn new(percentile: f64) -> Self {
        Self {
            percentile: percentile.clamp(0.0, 100.0),
        }
    }
}

impl AggregationFunction for PercentileFunction {
    fn name(&self) -> &str {
        "percentile"
    }

    fn aggregate(&self, values: &[f64]) -> Option<f64> {
        if values.is_empty() {
            return None;
        }
        let mut sorted = values.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let idx = ((self.percentile / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        sorted.get(idx).cloned()
    }

    fn requires_order(&self) -> bool {
        true
    }
}

/// Rate aggregation function (per-second rate of change)
pub struct RateFunction;

impl AggregationFunction for RateFunction {
    fn name(&self) -> &str {
        "rate"
    }

    fn aggregate(&self, _values: &[f64]) -> Option<f64> {
        // Rate needs timestamps, use aggregate_with_time instead
        None
    }

    fn aggregate_with_time(&self, samples: &[Sample]) -> Option<f64> {
        if samples.len() < 2 {
            return None;
        }

        let first = samples.first()?;
        let last = samples.last()?;

        let time_diff = last.timestamp.diff(&first.timestamp).as_secs_f64();
        if time_diff == 0.0 {
            return None;
        }

        let value_diff = last.as_f64()? - first.as_f64()?;
        Some(value_diff / time_diff)
    }

    fn requires_order(&self) -> bool {
        true
    }
}

/// Registry of aggregation functions
pub struct AggregationRegistry {
    functions: HashMap<String, Box<dyn AggregationFunction>>,
}

impl AggregationRegistry {
    /// Create a new registry with default functions
    pub fn new() -> Self {
        let mut registry = Self {
            functions: HashMap::new(),
        };

        registry.register("sum", Box::new(SumFunction));
        registry.register("avg", Box::new(AvgFunction));
        registry.register("min", Box::new(MinFunction));
        registry.register("max", Box::new(MaxFunction));
        registry.register("count", Box::new(CountFunction));
        registry.register("stddev", Box::new(StdDevFunction));
        registry.register("rate", Box::new(RateFunction));
        registry.register("p50", Box::new(PercentileFunction::new(50.0)));
        registry.register("p90", Box::new(PercentileFunction::new(90.0)));
        registry.register("p95", Box::new(PercentileFunction::new(95.0)));
        registry.register("p99", Box::new(PercentileFunction::new(99.0)));

        registry
    }

    /// Register a function
    pub fn register(&mut self, name: &str, function: Box<dyn AggregationFunction>) {
        self.functions.insert(name.to_string(), function);
    }

    /// Get a function by name
    pub fn get(&self, name: &str) -> Option<&dyn AggregationFunction> {
        self.functions.get(name).map(|f| f.as_ref())
    }

    /// Aggregate values using a named function
    pub fn aggregate(&self, name: &str, values: &[f64]) -> Option<f64> {
        self.get(name).and_then(|f| f.aggregate(values))
    }
}

impl Default for AggregationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Windowed aggregation for streaming data
#[derive(Debug)]
pub struct WindowedAggregator {
    /// Aggregation type
    aggregation: Aggregation,
    /// Window size in samples
    window_size: usize,
    /// Current values in window
    values: Vec<f64>,
    /// Running sum (for efficient avg)
    sum: f64,
    /// Running count
    count: usize,
}

impl WindowedAggregator {
    /// Create a new windowed aggregator
    pub fn new(aggregation: Aggregation, window_size: usize) -> Self {
        Self {
            aggregation,
            window_size,
            values: Vec::with_capacity(window_size),
            sum: 0.0,
            count: 0,
        }
    }

    /// Add a value
    pub fn add(&mut self, value: f64) {
        if self.values.len() >= self.window_size {
            // Remove oldest value
            if let Some(old) = self.values.first() {
                self.sum -= old;
            }
            self.values.remove(0);
        }

        self.values.push(value);
        self.sum += value;
        self.count += 1;
    }

    /// Get current aggregated value
    pub fn value(&self) -> Option<f64> {
        if self.values.is_empty() {
            return None;
        }

        match self.aggregation {
            Aggregation::Sum => Some(self.values.iter().sum()),
            Aggregation::Avg => Some(self.sum / self.values.len() as f64),
            Aggregation::Min => self.values.iter().cloned().reduce(f64::min),
            Aggregation::Max => self.values.iter().cloned().reduce(f64::max),
            Aggregation::Count => Some(self.values.len() as f64),
            Aggregation::First => self.values.first().cloned(),
            Aggregation::Last => self.values.last().cloned(),
            _ => None,
        }
    }

    /// Reset the aggregator
    pub fn reset(&mut self) {
        self.values.clear();
        self.sum = 0.0;
        self.count = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregation_parse() {
        assert_eq!(Aggregation::parse("sum"), Some(Aggregation::Sum));
        assert_eq!(Aggregation::parse("avg"), Some(Aggregation::Avg));
        assert_eq!(Aggregation::parse("mean"), Some(Aggregation::Avg));
        assert_eq!(Aggregation::parse("min"), Some(Aggregation::Min));
        assert_eq!(Aggregation::parse("max"), Some(Aggregation::Max));
        assert_eq!(Aggregation::parse("p99"), Some(Aggregation::P99));
    }

    #[test]
    fn test_sum_function() {
        let func = SumFunction;
        assert_eq!(func.aggregate(&[1.0, 2.0, 3.0]), Some(6.0));
        assert_eq!(func.aggregate(&[]), None);
    }

    #[test]
    fn test_avg_function() {
        let func = AvgFunction;
        assert_eq!(func.aggregate(&[1.0, 2.0, 3.0]), Some(2.0));
        assert_eq!(func.aggregate(&[10.0]), Some(10.0));
    }

    #[test]
    fn test_min_max_functions() {
        let min_func = MinFunction;
        let max_func = MaxFunction;

        let values = vec![3.0, 1.0, 4.0, 1.0, 5.0, 9.0];

        assert_eq!(min_func.aggregate(&values), Some(1.0));
        assert_eq!(max_func.aggregate(&values), Some(9.0));
    }

    #[test]
    fn test_percentile_function() {
        let p50 = PercentileFunction::new(50.0);
        let p99 = PercentileFunction::new(99.0);

        // Values 1-100 at indices 0-99
        // p50: round(0.50 * 99) = round(49.5) = 50 -> values[50] = 51
        // p99: round(0.99 * 99) = round(98.01) = 98 -> values[98] = 99
        let values: Vec<f64> = (1..=100).map(|i| i as f64).collect();

        assert_eq!(p50.aggregate(&values), Some(51.0));
        assert_eq!(p99.aggregate(&values), Some(99.0));
    }

    #[test]
    fn test_stddev_function() {
        let func = StdDevFunction;
        let values = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];

        let result = func.aggregate(&values).unwrap();
        assert!((result - 2.0).abs() < 0.01);
    }

    #[test]
    fn test_rate_function() {
        let func = RateFunction;

        let samples = vec![Sample::from_secs(0, 0.0), Sample::from_secs(10, 100.0)];

        let rate = func.aggregate_with_time(&samples);
        assert_eq!(rate, Some(10.0)); // 100 / 10 = 10 per second
    }

    #[test]
    fn test_aggregation_registry() {
        let registry = AggregationRegistry::new();

        assert_eq!(registry.aggregate("sum", &[1.0, 2.0, 3.0]), Some(6.0));
        assert_eq!(registry.aggregate("avg", &[2.0, 4.0, 6.0]), Some(4.0));
        assert_eq!(registry.aggregate("min", &[3.0, 1.0, 2.0]), Some(1.0));
    }

    #[test]
    fn test_windowed_aggregator() {
        let mut agg = WindowedAggregator::new(Aggregation::Avg, 3);

        agg.add(10.0);
        agg.add(20.0);
        agg.add(30.0);
        assert_eq!(agg.value(), Some(20.0));

        agg.add(40.0);
        assert_eq!(agg.value(), Some(30.0)); // (20 + 30 + 40) / 3
    }

    #[test]
    fn test_windowed_aggregator_min_max() {
        let mut min_agg = WindowedAggregator::new(Aggregation::Min, 5);
        let mut max_agg = WindowedAggregator::new(Aggregation::Max, 5);

        for v in [5.0, 2.0, 8.0, 1.0, 9.0] {
            min_agg.add(v);
            max_agg.add(v);
        }

        assert_eq!(min_agg.value(), Some(1.0));
        assert_eq!(max_agg.value(), Some(9.0));
    }
}
