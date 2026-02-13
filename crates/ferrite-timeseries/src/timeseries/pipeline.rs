//! Aggregation Pipeline for Time Series
//!
//! Composable multi-stage aggregation pipeline supporting filtering,
//! aggregation, grouping, math operations, and limiting.

use super::aggregation::Aggregation;
use super::labels::{LabelMatcher, Labels};
use super::sample::{Sample, Timestamp, Value};
use super::*;
use std::collections::HashMap;
use std::time::Duration;

/// Aggregation type for downsampling and pipeline operations
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggregationType {
    /// Average of values
    Avg,
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// Sum of values
    Sum,
    /// Count of values
    Count,
    /// First value in window
    First,
    /// Last value in window
    Last,
    /// Standard deviation
    StdDev,
    /// Range (max - min)
    Range,
}

impl AggregationType {
    /// Aggregate a slice of f64 values
    pub fn aggregate(&self, values: &[f64]) -> Option<f64> {
        if values.is_empty() {
            return None;
        }
        match self {
            AggregationType::Avg => Some(values.iter().sum::<f64>() / values.len() as f64),
            AggregationType::Min => values.iter().cloned().reduce(f64::min),
            AggregationType::Max => values.iter().cloned().reduce(f64::max),
            AggregationType::Sum => Some(values.iter().sum()),
            AggregationType::Count => Some(values.len() as f64),
            AggregationType::First => values.first().cloned(),
            AggregationType::Last => values.last().cloned(),
            AggregationType::StdDev => {
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let variance =
                    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
                Some(variance.sqrt())
            }
            AggregationType::Range => {
                let min = values.iter().cloned().reduce(f64::min)?;
                let max = values.iter().cloned().reduce(f64::max)?;
                Some(max - min)
            }
        }
    }

    /// Convert to the existing Aggregation enum
    pub fn to_aggregation(&self) -> Aggregation {
        match self {
            AggregationType::Avg => Aggregation::Avg,
            AggregationType::Min => Aggregation::Min,
            AggregationType::Max => Aggregation::Max,
            AggregationType::Sum => Aggregation::Sum,
            AggregationType::Count => Aggregation::Count,
            AggregationType::First => Aggregation::First,
            AggregationType::Last => Aggregation::Last,
            AggregationType::StdDev => Aggregation::StdDev,
            AggregationType::Range => Aggregation::Max, // handled specially
        }
    }

    /// Parse from string
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "avg" | "mean" | "average" => Some(AggregationType::Avg),
            "min" => Some(AggregationType::Min),
            "max" => Some(AggregationType::Max),
            "sum" => Some(AggregationType::Sum),
            "count" => Some(AggregationType::Count),
            "first" => Some(AggregationType::First),
            "last" => Some(AggregationType::Last),
            "stddev" | "std_dev" => Some(AggregationType::StdDev),
            "range" => Some(AggregationType::Range),
            _ => None,
        }
    }
}

impl std::fmt::Display for AggregationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregationType::Avg => write!(f, "avg"),
            AggregationType::Min => write!(f, "min"),
            AggregationType::Max => write!(f, "max"),
            AggregationType::Sum => write!(f, "sum"),
            AggregationType::Count => write!(f, "count"),
            AggregationType::First => write!(f, "first"),
            AggregationType::Last => write!(f, "last"),
            AggregationType::StdDev => write!(f, "stddev"),
            AggregationType::Range => write!(f, "range"),
        }
    }
}

/// Filter expression for pipeline stages
#[derive(Debug, Clone)]
pub enum FilterExpr {
    /// Match label exactly: label = value
    LabelEqual(String, String),
    /// Label not equal: label != value
    LabelNotEqual(String, String),
    /// Label matches regex: label =~ pattern
    LabelRegex(String, String),
    /// Label exists
    LabelExists(String),
    /// Value greater than threshold
    ValueGt(f64),
    /// Value less than threshold
    ValueLt(f64),
    /// Value between bounds (inclusive)
    ValueBetween(f64, f64),
    /// Combine filters with AND
    And(Vec<FilterExpr>),
    /// Combine filters with OR
    Or(Vec<FilterExpr>),
}

impl FilterExpr {
    /// Check if a sample with labels matches this filter
    pub fn matches(&self, sample: &Sample, labels: &Labels) -> bool {
        match self {
            FilterExpr::LabelEqual(name, value) => {
                labels.get(name) == Some(value.as_str())
            }
            FilterExpr::LabelNotEqual(name, value) => {
                labels.get(name).map_or(true, |v| v != value)
            }
            FilterExpr::LabelRegex(name, pattern) => {
                if let Some(v) = labels.get(name) {
                    regex::Regex::new(pattern)
                        .map(|re| re.is_match(v))
                        .unwrap_or(false)
                } else {
                    false
                }
            }
            FilterExpr::LabelExists(name) => labels.contains(name),
            FilterExpr::ValueGt(threshold) => {
                sample.as_f64().is_some_and(|v| v > *threshold)
            }
            FilterExpr::ValueLt(threshold) => {
                sample.as_f64().is_some_and(|v| v < *threshold)
            }
            FilterExpr::ValueBetween(low, high) => {
                sample.as_f64().is_some_and(|v| (*low..=*high).contains(&v))
            }
            FilterExpr::And(filters) => filters.iter().all(|f| f.matches(sample, labels)),
            FilterExpr::Or(filters) => filters.iter().any(|f| f.matches(sample, labels)),
        }
    }

    /// Convert to LabelMatcher (for label-only filters)
    pub fn to_label_matcher(&self) -> Option<LabelMatcher> {
        match self {
            FilterExpr::LabelEqual(name, value) => {
                Some(LabelMatcher::exact(name.clone(), value.clone()))
            }
            FilterExpr::LabelNotEqual(name, value) => {
                Some(LabelMatcher::not_equal(name.clone(), value.clone()))
            }
            FilterExpr::LabelRegex(name, pattern) => {
                Some(LabelMatcher::regex(name.clone(), pattern.clone()))
            }
            FilterExpr::LabelExists(name) => Some(LabelMatcher::exists(name.clone())),
            _ => None,
        }
    }
}

/// Math operations for pipeline Apply stage
#[derive(Debug, Clone, Copy)]
pub enum MathOp {
    /// Add a constant
    Add(f64),
    /// Subtract a constant
    Sub(f64),
    /// Multiply by a constant
    Mul(f64),
    /// Divide by a constant
    Div(f64),
    /// Absolute value
    Abs,
    /// Ceiling
    Ceil,
    /// Floor
    Floor,
    /// Round to N decimal places
    Round(u32),
    /// Clamp between min and max
    Clamp(f64, f64),
}

impl MathOp {
    /// Apply the math operation to a value
    pub fn apply(&self, value: f64) -> f64 {
        match self {
            MathOp::Add(c) => value + c,
            MathOp::Sub(c) => value - c,
            MathOp::Mul(c) => value * c,
            MathOp::Div(c) => {
                if *c != 0.0 {
                    value / c
                } else {
                    f64::NAN
                }
            }
            MathOp::Abs => value.abs(),
            MathOp::Ceil => value.ceil(),
            MathOp::Floor => value.floor(),
            MathOp::Round(places) => {
                let factor = 10f64.powi(*places as i32);
                (value * factor).round() / factor
            }
            MathOp::Clamp(min, max) => value.clamp(*min, *max),
        }
    }
}

/// A stage in the aggregation pipeline
#[derive(Debug, Clone)]
pub enum PipelineStage {
    /// Filter samples by expression
    Filter(FilterExpr),
    /// Aggregate samples within time buckets
    Aggregate {
        /// Bucket duration
        duration: Duration,
        /// Aggregation type
        agg_type: AggregationType,
    },
    /// Group samples by label values
    GroupBy(Vec<String>),
    /// Apply a math operation to all values
    Apply(MathOp),
    /// Limit the number of output samples
    Limit(usize),
    /// Compute moving average with a sliding window of N samples
    MovingAverage(usize),
    /// Compute per-second rate of change between consecutive samples
    Rate,
}

/// A series of samples with labels, used as pipeline intermediate result
#[derive(Debug, Clone)]
pub struct PipelineSeries {
    /// Group key (from GroupBy)
    pub group_key: String,
    /// Labels
    pub labels: Labels,
    /// Samples
    pub samples: Vec<Sample>,
}

/// Composable aggregation pipeline
#[derive(Debug, Clone)]
pub struct AggregationPipeline {
    /// Pipeline stages executed in order
    pub stages: Vec<PipelineStage>,
}

impl AggregationPipeline {
    /// Create an empty pipeline
    pub fn new() -> Self {
        Self {
            stages: Vec::new(),
        }
    }

    /// Add a filter stage
    pub fn filter(mut self, expr: FilterExpr) -> Self {
        self.stages.push(PipelineStage::Filter(expr));
        self
    }

    /// Add an aggregation stage
    pub fn aggregate(mut self, duration: Duration, agg_type: AggregationType) -> Self {
        self.stages.push(PipelineStage::Aggregate {
            duration,
            agg_type,
        });
        self
    }

    /// Add a group-by stage
    pub fn group_by(mut self, labels: Vec<String>) -> Self {
        self.stages.push(PipelineStage::GroupBy(labels));
        self
    }

    /// Add a math operation stage
    pub fn apply(mut self, op: MathOp) -> Self {
        self.stages.push(PipelineStage::Apply(op));
        self
    }

    /// Add a limit stage
    pub fn limit(mut self, n: usize) -> Self {
        self.stages.push(PipelineStage::Limit(n));
        self
    }

    /// Add a moving average stage
    pub fn moving_average(mut self, window_size: usize) -> Self {
        self.stages.push(PipelineStage::MovingAverage(window_size));
        self
    }

    /// Add a rate calculation stage
    pub fn rate(mut self) -> Self {
        self.stages.push(PipelineStage::Rate);
        self
    }

    /// Execute the pipeline on a set of series
    pub fn execute(&self, input: Vec<PipelineSeries>) -> Result<Vec<PipelineSeries>> {
        let mut current = input;

        for stage in &self.stages {
            current = self.execute_stage(stage, current)?;
        }

        Ok(current)
    }

    /// Execute a single pipeline stage
    fn execute_stage(
        &self,
        stage: &PipelineStage,
        input: Vec<PipelineSeries>,
    ) -> Result<Vec<PipelineSeries>> {
        match stage {
            PipelineStage::Filter(expr) => self.execute_filter(expr, input),
            PipelineStage::Aggregate { duration, agg_type } => {
                self.execute_aggregate(*duration, *agg_type, input)
            }
            PipelineStage::GroupBy(labels) => self.execute_group_by(labels, input),
            PipelineStage::Apply(op) => self.execute_apply(*op, input),
            PipelineStage::Limit(n) => self.execute_limit(*n, input),
            PipelineStage::MovingAverage(window) => self.execute_moving_average(*window, input),
            PipelineStage::Rate => self.execute_rate(input),
        }
    }

    /// Execute a filter stage
    fn execute_filter(
        &self,
        expr: &FilterExpr,
        input: Vec<PipelineSeries>,
    ) -> Result<Vec<PipelineSeries>> {
        let mut output = Vec::with_capacity(input.len());

        for mut series in input {
            series.samples.retain(|s| expr.matches(s, &series.labels));
            if !series.samples.is_empty() {
                output.push(series);
            }
        }

        Ok(output)
    }

    /// Execute an aggregation stage: bucket samples by time window and aggregate
    fn execute_aggregate(
        &self,
        duration: Duration,
        agg_type: AggregationType,
        input: Vec<PipelineSeries>,
    ) -> Result<Vec<PipelineSeries>> {
        let mut output = Vec::with_capacity(input.len());
        let interval_nanos = duration.as_nanos() as i64;

        for series in input {
            let mut buckets: HashMap<i64, Vec<f64>> = HashMap::new();

            for sample in &series.samples {
                let bucket_ts =
                    (sample.timestamp.as_nanos() / interval_nanos) * interval_nanos;
                if let Some(v) = sample.as_f64() {
                    buckets.entry(bucket_ts).or_default().push(v);
                }
            }

            let mut aggregated_samples: Vec<Sample> = buckets
                .into_iter()
                .filter_map(|(ts, values)| {
                    agg_type
                        .aggregate(&values)
                        .map(|v| Sample::new(Timestamp::from_nanos(ts), v))
                })
                .collect();

            aggregated_samples.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

            output.push(PipelineSeries {
                group_key: series.group_key,
                labels: series.labels,
                samples: aggregated_samples,
            });
        }

        Ok(output)
    }

    /// Execute a group-by stage: split series by label values
    fn execute_group_by(
        &self,
        group_labels: &[String],
        input: Vec<PipelineSeries>,
    ) -> Result<Vec<PipelineSeries>> {
        let mut groups: HashMap<String, PipelineSeries> = HashMap::new();

        for series in input {
            let key = compute_group_key(&series.labels, group_labels);

            let entry = groups
                .entry(key.clone())
                .or_insert_with(|| PipelineSeries {
                    group_key: key,
                    labels: series.labels.clone(),
                    samples: Vec::new(),
                });

            entry.samples.extend(series.samples);
        }

        // Sort each group's samples by timestamp
        let mut output: Vec<PipelineSeries> = groups.into_values().collect();
        for series in &mut output {
            series.samples.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        }
        output.sort_by(|a, b| a.group_key.cmp(&b.group_key));

        Ok(output)
    }

    /// Execute a math operation stage
    fn execute_apply(
        &self,
        op: MathOp,
        input: Vec<PipelineSeries>,
    ) -> Result<Vec<PipelineSeries>> {
        let mut output = Vec::with_capacity(input.len());

        for mut series in input {
            series.samples = series
                .samples
                .into_iter()
                .map(|s| {
                    let new_value = s.as_f64().map(|v| op.apply(v)).unwrap_or(f64::NAN);
                    Sample::new(s.timestamp, new_value)
                })
                .collect();
            output.push(series);
        }

        Ok(output)
    }

    /// Execute a limit stage
    fn execute_limit(
        &self,
        n: usize,
        input: Vec<PipelineSeries>,
    ) -> Result<Vec<PipelineSeries>> {
        let mut output = Vec::with_capacity(input.len());

        for mut series in input {
            series.samples.truncate(n);
            if !series.samples.is_empty() {
                output.push(series);
            }
        }

        Ok(output)
    }

    /// Execute a moving average stage
    fn execute_moving_average(
        &self,
        window: usize,
        input: Vec<PipelineSeries>,
    ) -> Result<Vec<PipelineSeries>> {
        let mut output = Vec::with_capacity(input.len());

        for series in input {
            let values: Vec<(Timestamp, f64)> = series
                .samples
                .iter()
                .filter_map(|s| s.as_f64().map(|v| (s.timestamp, v)))
                .collect();

            if values.len() < window || window == 0 {
                output.push(PipelineSeries {
                    group_key: series.group_key,
                    labels: series.labels,
                    samples: Vec::new(),
                });
                continue;
            }

            let mut ma_samples = Vec::with_capacity(values.len() - window + 1);
            let mut window_sum: f64 = values[..window].iter().map(|(_, v)| v).sum();
            ma_samples.push(Sample::new(
                values[window - 1].0,
                window_sum / window as f64,
            ));

            for i in window..values.len() {
                window_sum += values[i].1 - values[i - window].1;
                ma_samples.push(Sample::new(values[i].0, window_sum / window as f64));
            }

            output.push(PipelineSeries {
                group_key: series.group_key,
                labels: series.labels,
                samples: ma_samples,
            });
        }

        Ok(output)
    }

    /// Execute a rate calculation stage
    fn execute_rate(&self, input: Vec<PipelineSeries>) -> Result<Vec<PipelineSeries>> {
        let mut output = Vec::with_capacity(input.len());

        for series in input {
            if series.samples.len() < 2 {
                output.push(PipelineSeries {
                    group_key: series.group_key,
                    labels: series.labels,
                    samples: Vec::new(),
                });
                continue;
            }

            let mut rate_samples = Vec::with_capacity(series.samples.len() - 1);
            for pair in series.samples.windows(2) {
                if let (Some(v0), Some(v1)) = (pair[0].as_f64(), pair[1].as_f64()) {
                    let dt = pair[1].timestamp.diff(&pair[0].timestamp).as_secs_f64();
                    if dt > 0.0 {
                        rate_samples.push(Sample::new(pair[1].timestamp, (v1 - v0) / dt));
                    }
                }
            }

            output.push(PipelineSeries {
                group_key: series.group_key,
                labels: series.labels,
                samples: rate_samples,
            });
        }

        Ok(output)
    }
}

impl Default for AggregationPipeline {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute a group key from labels
fn compute_group_key(labels: &Labels, group_labels: &[String]) -> String {
    let parts: Vec<String> = group_labels
        .iter()
        .filter_map(|name| labels.get(name).map(|v| format!("{}={}", name, v)))
        .collect();
    parts.join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_series(values: &[(i64, f64)], labels: Labels) -> PipelineSeries {
        PipelineSeries {
            group_key: String::new(),
            labels,
            samples: values
                .iter()
                .map(|(ts, v)| Sample::from_secs(*ts, *v))
                .collect(),
        }
    }

    // --- AggregationType tests ---

    #[test]
    fn test_aggregation_type_avg() {
        assert_eq!(AggregationType::Avg.aggregate(&[10.0, 20.0, 30.0]), Some(20.0));
    }

    #[test]
    fn test_aggregation_type_min() {
        assert_eq!(AggregationType::Min.aggregate(&[10.0, 5.0, 20.0]), Some(5.0));
    }

    #[test]
    fn test_aggregation_type_max() {
        assert_eq!(AggregationType::Max.aggregate(&[10.0, 5.0, 20.0]), Some(20.0));
    }

    #[test]
    fn test_aggregation_type_sum() {
        assert_eq!(AggregationType::Sum.aggregate(&[10.0, 20.0, 30.0]), Some(60.0));
    }

    #[test]
    fn test_aggregation_type_count() {
        assert_eq!(AggregationType::Count.aggregate(&[10.0, 20.0, 30.0]), Some(3.0));
    }

    #[test]
    fn test_aggregation_type_first() {
        assert_eq!(AggregationType::First.aggregate(&[10.0, 20.0, 30.0]), Some(10.0));
    }

    #[test]
    fn test_aggregation_type_last() {
        assert_eq!(AggregationType::Last.aggregate(&[10.0, 20.0, 30.0]), Some(30.0));
    }

    #[test]
    fn test_aggregation_type_stddev() {
        let vals = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let result = AggregationType::StdDev.aggregate(&vals).unwrap();
        assert!((result - 2.0).abs() < 0.01);
    }

    #[test]
    fn test_aggregation_type_range() {
        assert_eq!(AggregationType::Range.aggregate(&[10.0, 5.0, 20.0]), Some(15.0));
    }

    #[test]
    fn test_aggregation_type_empty() {
        assert_eq!(AggregationType::Avg.aggregate(&[]), None);
        assert_eq!(AggregationType::Range.aggregate(&[]), None);
    }

    #[test]
    fn test_aggregation_type_single_value() {
        assert_eq!(AggregationType::Avg.aggregate(&[42.0]), Some(42.0));
        assert_eq!(AggregationType::Range.aggregate(&[42.0]), Some(0.0));
        assert_eq!(AggregationType::StdDev.aggregate(&[42.0]), Some(0.0));
    }

    #[test]
    fn test_aggregation_type_parse() {
        assert_eq!(AggregationType::parse("avg"), Some(AggregationType::Avg));
        assert_eq!(AggregationType::parse("min"), Some(AggregationType::Min));
        assert_eq!(AggregationType::parse("range"), Some(AggregationType::Range));
        assert_eq!(AggregationType::parse("unknown"), None);
    }

    // --- FilterExpr tests ---

    #[test]
    fn test_filter_label_equal() {
        let labels = Labels::parse(&["region:us-east"]).unwrap();
        let sample = Sample::from_secs(1, 42.0);

        let f = FilterExpr::LabelEqual("region".into(), "us-east".into());
        assert!(f.matches(&sample, &labels));

        let f2 = FilterExpr::LabelEqual("region".into(), "eu-west".into());
        assert!(!f2.matches(&sample, &labels));
    }

    #[test]
    fn test_filter_value_gt() {
        let labels = Labels::empty();
        let sample = Sample::from_secs(1, 42.0);

        assert!(FilterExpr::ValueGt(40.0).matches(&sample, &labels));
        assert!(!FilterExpr::ValueGt(50.0).matches(&sample, &labels));
    }

    #[test]
    fn test_filter_value_between() {
        let labels = Labels::empty();
        let sample = Sample::from_secs(1, 42.0);

        assert!(FilterExpr::ValueBetween(40.0, 50.0).matches(&sample, &labels));
        assert!(!FilterExpr::ValueBetween(50.0, 60.0).matches(&sample, &labels));
    }

    #[test]
    fn test_filter_and() {
        let labels = Labels::parse(&["region:us-east", "env:prod"]).unwrap();
        let sample = Sample::from_secs(1, 42.0);

        let f = FilterExpr::And(vec![
            FilterExpr::LabelEqual("region".into(), "us-east".into()),
            FilterExpr::LabelEqual("env".into(), "prod".into()),
        ]);
        assert!(f.matches(&sample, &labels));

        let f2 = FilterExpr::And(vec![
            FilterExpr::LabelEqual("region".into(), "us-east".into()),
            FilterExpr::LabelEqual("env".into(), "dev".into()),
        ]);
        assert!(!f2.matches(&sample, &labels));
    }

    #[test]
    fn test_filter_or() {
        let labels = Labels::parse(&["region:us-east"]).unwrap();
        let sample = Sample::from_secs(1, 42.0);

        let f = FilterExpr::Or(vec![
            FilterExpr::LabelEqual("region".into(), "us-east".into()),
            FilterExpr::LabelEqual("region".into(), "eu-west".into()),
        ]);
        assert!(f.matches(&sample, &labels));
    }

    // --- MathOp tests ---

    #[test]
    fn test_math_ops() {
        assert_eq!(MathOp::Add(10.0).apply(5.0), 15.0);
        assert_eq!(MathOp::Sub(3.0).apply(10.0), 7.0);
        assert_eq!(MathOp::Mul(2.0).apply(5.0), 10.0);
        assert_eq!(MathOp::Div(4.0).apply(20.0), 5.0);
        assert!(MathOp::Div(0.0).apply(5.0).is_nan());
        assert_eq!(MathOp::Abs.apply(-5.0), 5.0);
        assert_eq!(MathOp::Ceil.apply(4.2), 5.0);
        assert_eq!(MathOp::Floor.apply(4.8), 4.0);
        assert_eq!(MathOp::Round(2).apply(3.14159), 3.14);
        assert_eq!(MathOp::Clamp(0.0, 100.0).apply(150.0), 100.0);
        assert_eq!(MathOp::Clamp(0.0, 100.0).apply(-10.0), 0.0);
    }

    // --- Pipeline tests ---

    #[test]
    fn test_pipeline_filter() {
        let series = make_series(
            &[(1, 10.0), (2, 50.0), (3, 30.0), (4, 80.0)],
            Labels::empty(),
        );

        let pipeline = AggregationPipeline::new()
            .filter(FilterExpr::ValueGt(25.0));

        let result = pipeline.execute(vec![series]).unwrap();
        assert_eq!(result[0].samples.len(), 3); // 50, 30, 80
    }

    #[test]
    fn test_pipeline_aggregate() {
        // 6 samples across 2 60-second buckets
        let series = make_series(
            &[
                (0, 10.0),
                (20, 20.0),
                (40, 30.0),
                (60, 40.0),
                (80, 50.0),
                (100, 60.0),
            ],
            Labels::empty(),
        );

        let pipeline = AggregationPipeline::new()
            .aggregate(Duration::from_secs(60), AggregationType::Avg);

        let result = pipeline.execute(vec![series]).unwrap();
        assert_eq!(result[0].samples.len(), 2);
        assert_eq!(result[0].samples[0].as_f64(), Some(20.0)); // avg(10,20,30)
        assert_eq!(result[0].samples[1].as_f64(), Some(50.0)); // avg(40,50,60)
    }

    #[test]
    fn test_pipeline_group_by() {
        let s1 = PipelineSeries {
            group_key: String::new(),
            labels: Labels::parse(&["host:server1"]).unwrap(),
            samples: vec![Sample::from_secs(1, 10.0), Sample::from_secs(2, 20.0)],
        };
        let s2 = PipelineSeries {
            group_key: String::new(),
            labels: Labels::parse(&["host:server2"]).unwrap(),
            samples: vec![Sample::from_secs(1, 30.0), Sample::from_secs(2, 40.0)],
        };
        let s3 = PipelineSeries {
            group_key: String::new(),
            labels: Labels::parse(&["host:server1"]).unwrap(),
            samples: vec![Sample::from_secs(3, 50.0)],
        };

        let pipeline = AggregationPipeline::new()
            .group_by(vec!["host".to_string()]);

        let result = pipeline.execute(vec![s1, s2, s3]).unwrap();
        assert_eq!(result.len(), 2);

        let server1 = result.iter().find(|s| s.group_key == "host=server1").unwrap();
        assert_eq!(server1.samples.len(), 3); // merged s1 + s3

        let server2 = result.iter().find(|s| s.group_key == "host=server2").unwrap();
        assert_eq!(server2.samples.len(), 2);
    }

    #[test]
    fn test_pipeline_apply_math() {
        let series = make_series(&[(1, 10.0), (2, 20.0), (3, 30.0)], Labels::empty());

        let pipeline = AggregationPipeline::new()
            .apply(MathOp::Mul(2.0));

        let result = pipeline.execute(vec![series]).unwrap();
        assert_eq!(result[0].samples[0].as_f64(), Some(20.0));
        assert_eq!(result[0].samples[1].as_f64(), Some(40.0));
        assert_eq!(result[0].samples[2].as_f64(), Some(60.0));
    }

    #[test]
    fn test_pipeline_limit() {
        let series = make_series(
            &[(1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0), (5, 50.0)],
            Labels::empty(),
        );

        let pipeline = AggregationPipeline::new().limit(3);

        let result = pipeline.execute(vec![series]).unwrap();
        assert_eq!(result[0].samples.len(), 3);
    }

    #[test]
    fn test_pipeline_chaining() {
        // Filter -> Aggregate -> Apply -> Limit
        let series = make_series(
            &[
                (0, 5.0),
                (20, 15.0),
                (40, 25.0),
                (60, 35.0),
                (80, 45.0),
                (100, 55.0),
            ],
            Labels::empty(),
        );

        let pipeline = AggregationPipeline::new()
            .filter(FilterExpr::ValueGt(10.0))
            .aggregate(Duration::from_secs(60), AggregationType::Avg)
            .apply(MathOp::Mul(2.0))
            .limit(10);

        let result = pipeline.execute(vec![series]).unwrap();
        // After filter: 15, 25, 35, 45, 55
        // After aggregate (60s buckets): bucket[0]=(15+25)/2=20, bucket[60]=(35+45)/2=40, bucket[100]=55
        // After apply (*2): 40, 80, 110
        assert!(!result[0].samples.is_empty());
    }

    #[test]
    fn test_pipeline_empty_input() {
        let pipeline = AggregationPipeline::new()
            .aggregate(Duration::from_secs(60), AggregationType::Avg);

        let result = pipeline.execute(vec![]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_pipeline_empty_series() {
        let series = PipelineSeries {
            group_key: String::new(),
            labels: Labels::empty(),
            samples: Vec::new(),
        };

        let pipeline = AggregationPipeline::new()
            .aggregate(Duration::from_secs(60), AggregationType::Avg);

        let result = pipeline.execute(vec![series]).unwrap();
        // Empty series with no samples after aggregation gets dropped
        assert!(result.is_empty() || result[0].samples.is_empty());
    }

    #[test]
    fn test_pipeline_single_point() {
        let series = make_series(&[(100, 42.0)], Labels::empty());

        let pipeline = AggregationPipeline::new()
            .aggregate(Duration::from_secs(60), AggregationType::Avg)
            .apply(MathOp::Add(8.0));

        let result = pipeline.execute(vec![series]).unwrap();
        assert_eq!(result[0].samples.len(), 1);
        assert_eq!(result[0].samples[0].as_f64(), Some(50.0));
    }

    #[test]
    fn test_pipeline_all_aggregation_types() {
        let series = make_series(
            &[(0, 10.0), (20, 20.0), (40, 30.0)],
            Labels::empty(),
        );

        for agg_type in &[
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
            let pipeline = AggregationPipeline::new()
                .aggregate(Duration::from_secs(60), *agg_type);

            let result = pipeline.execute(vec![series.clone()]).unwrap();
            assert_eq!(result[0].samples.len(), 1, "Failed for {:?}", agg_type);
            assert!(result[0].samples[0].as_f64().is_some(), "Failed for {:?}", agg_type);
        }
    }

    #[test]
    fn test_aggregation_accuracy() {
        let series = make_series(
            &[(0, 10.0), (20, 20.0), (40, 30.0)],
            Labels::empty(),
        );

        let check = |agg: AggregationType, expected: f64| {
            let pipeline = AggregationPipeline::new()
                .aggregate(Duration::from_secs(60), agg);
            let result = pipeline.execute(vec![series.clone()]).unwrap();
            let actual = result[0].samples[0].as_f64().unwrap();
            assert!(
                (actual - expected).abs() < 0.001,
                "{:?}: expected {}, got {}",
                agg,
                expected,
                actual,
            );
        };

        check(AggregationType::Avg, 20.0);
        check(AggregationType::Min, 10.0);
        check(AggregationType::Max, 30.0);
        check(AggregationType::Sum, 60.0);
        check(AggregationType::Count, 3.0);
        check(AggregationType::First, 10.0);
        check(AggregationType::Last, 30.0);
        check(AggregationType::Range, 20.0);
    }

    // --- MovingAverage pipeline tests ---

    #[test]
    fn test_pipeline_moving_average() {
        let series = make_series(
            &[(1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0), (5, 50.0)],
            Labels::empty(),
        );

        let pipeline = AggregationPipeline::new().moving_average(3);

        let result = pipeline.execute(vec![series]).unwrap();
        assert_eq!(result[0].samples.len(), 3); // 5 - 3 + 1
        assert_eq!(result[0].samples[0].as_f64(), Some(20.0)); // avg(10,20,30)
        assert_eq!(result[0].samples[1].as_f64(), Some(30.0)); // avg(20,30,40)
        assert_eq!(result[0].samples[2].as_f64(), Some(40.0)); // avg(30,40,50)
    }

    #[test]
    fn test_pipeline_moving_average_window_too_large() {
        let series = make_series(&[(1, 10.0), (2, 20.0)], Labels::empty());

        let pipeline = AggregationPipeline::new().moving_average(5);

        let result = pipeline.execute(vec![series]).unwrap();
        assert!(result[0].samples.is_empty());
    }

    // --- Rate pipeline tests ---

    #[test]
    fn test_pipeline_rate() {
        let series = make_series(
            &[(0, 0.0), (1, 10.0), (2, 30.0), (3, 60.0)],
            Labels::empty(),
        );

        let pipeline = AggregationPipeline::new().rate();

        let result = pipeline.execute(vec![series]).unwrap();
        assert_eq!(result[0].samples.len(), 3);
        assert_eq!(result[0].samples[0].as_f64(), Some(10.0)); // (10-0)/1
        assert_eq!(result[0].samples[1].as_f64(), Some(20.0)); // (30-10)/1
        assert_eq!(result[0].samples[2].as_f64(), Some(30.0)); // (60-30)/1
    }

    #[test]
    fn test_pipeline_rate_single_sample() {
        let series = make_series(&[(1, 42.0)], Labels::empty());

        let pipeline = AggregationPipeline::new().rate();

        let result = pipeline.execute(vec![series]).unwrap();
        assert!(result[0].samples.is_empty());
    }

    // --- Combined pipeline tests ---

    #[test]
    fn test_pipeline_rate_then_moving_average() {
        let series = make_series(
            &[(0, 0.0), (1, 10.0), (2, 30.0), (3, 60.0), (4, 100.0)],
            Labels::empty(),
        );

        let pipeline = AggregationPipeline::new()
            .rate()
            .moving_average(2);

        let result = pipeline.execute(vec![series]).unwrap();
        // Rate: [10, 20, 30, 40]
        // MA(2): [15, 25, 35] → 3 samples
        assert_eq!(result[0].samples.len(), 3);
        assert_eq!(result[0].samples[0].as_f64(), Some(15.0));
        assert_eq!(result[0].samples[1].as_f64(), Some(25.0));
        assert_eq!(result[0].samples[2].as_f64(), Some(35.0));
    }

    #[test]
    fn test_pipeline_aggregate_then_moving_average() {
        // 12 samples across 4 60-second buckets
        let series = make_series(
            &[
                (0, 10.0), (20, 20.0), (40, 30.0),    // bucket 0: avg=20
                (60, 40.0), (80, 50.0), (100, 60.0),   // bucket 60: avg=50
                (120, 70.0), (140, 80.0), (160, 90.0),  // bucket 120: avg=80
                (180, 100.0), (200, 110.0), (220, 120.0), // bucket 180: avg=110
            ],
            Labels::empty(),
        );

        let pipeline = AggregationPipeline::new()
            .aggregate(Duration::from_secs(60), AggregationType::Avg)
            .moving_average(2);

        let result = pipeline.execute(vec![series]).unwrap();
        // After aggregation: [20, 50, 80, 110]
        // After MA(2): [35, 65, 95] → 3 samples
        assert_eq!(result[0].samples.len(), 3);
        assert_eq!(result[0].samples[0].as_f64(), Some(35.0));
        assert_eq!(result[0].samples[1].as_f64(), Some(65.0));
        assert_eq!(result[0].samples[2].as_f64(), Some(95.0));
    }
}
