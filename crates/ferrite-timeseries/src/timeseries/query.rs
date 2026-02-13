//! Time Series Query Engine
//!
//! SQL-like query interface for time series data.

use super::aggregation::Aggregation;
use super::labels::{LabelMatcher, Labels};
use super::sample::Timestamp;
use super::storage::TimeSeriesStorage;
use super::*;
use std::sync::Arc;
use std::time::Duration;

/// Time range for queries
#[derive(Debug, Clone, Copy)]
pub struct TimeRange {
    /// Start timestamp (inclusive)
    pub start: Timestamp,
    /// End timestamp (inclusive)
    pub end: Timestamp,
}

impl TimeRange {
    /// Create a new time range
    pub fn new(start: Timestamp, end: Timestamp) -> Self {
        Self { start, end }
    }

    /// Create range for last N seconds
    pub fn last(duration: Duration) -> Self {
        let end = Timestamp::now();
        let start = end.sub(duration);
        Self { start, end }
    }

    /// Create range for last N minutes
    pub fn last_minutes(minutes: u64) -> Self {
        Self::last(Duration::from_secs(minutes * 60))
    }

    /// Create range for last N hours
    pub fn last_hours(hours: u64) -> Self {
        Self::last(Duration::from_secs(hours * 3600))
    }

    /// Create range for last N days
    pub fn last_days(days: u64) -> Self {
        Self::last(Duration::from_secs(days * 86400))
    }

    /// Get duration of range
    pub fn duration(&self) -> Duration {
        self.end.diff(&self.start)
    }

    /// Check if timestamp is in range
    pub fn contains(&self, ts: Timestamp) -> bool {
        ts.as_nanos() >= self.start.as_nanos() && ts.as_nanos() <= self.end.as_nanos()
    }

    /// Check if ranges overlap
    pub fn overlaps(&self, other: &TimeRange) -> bool {
        self.start.as_nanos() <= other.end.as_nanos()
            && self.end.as_nanos() >= other.start.as_nanos()
    }
}

impl Default for TimeRange {
    fn default() -> Self {
        Self::last_hours(1)
    }
}

/// A parsed time series query
#[derive(Debug, Clone)]
pub struct TimeSeriesQuery {
    /// Metric name or pattern
    pub metric: String,
    /// Label matchers
    pub label_matchers: Vec<LabelMatcher>,
    /// Time range
    pub range: Option<TimeRange>,
    /// Aggregation function
    pub aggregation: Option<Aggregation>,
    /// Step interval for range queries
    pub step: Option<Duration>,
    /// Group by labels
    pub group_by: Vec<String>,
    /// Limit results
    pub limit: Option<usize>,
    /// Offset for pagination
    pub offset: Option<usize>,
    /// Order by
    pub order_by: Option<OrderBy>,
    /// Subqueries
    pub subqueries: Vec<TimeSeriesQuery>,
    /// Mathematical expression
    pub expression: Option<String>,
}

impl TimeSeriesQuery {
    /// Create a new query for a metric
    pub fn new(metric: impl Into<String>) -> Self {
        Self {
            metric: metric.into(),
            label_matchers: Vec::new(),
            range: None,
            aggregation: None,
            step: None,
            group_by: Vec::new(),
            limit: None,
            offset: None,
            order_by: None,
            subqueries: Vec::new(),
            expression: None,
        }
    }

    /// Parse a query string
    pub fn parse(query: &str) -> Result<Self> {
        // Simple parser for basic queries
        // Format: metric{label=value,...}[time_range] or aggregation(metric{...}[range])

        let query = query.trim();

        // Check for aggregation function
        if let Some(open_paren) = query.find('(') {
            if open_paren > 0 {
                let func_name = &query[..open_paren];
                if let Some(aggregation) = Aggregation::parse(func_name) {
                    let inner = &query[open_paren + 1..query.len() - 1];
                    let mut parsed = Self::parse(inner)?;
                    parsed.aggregation = Some(aggregation);
                    return Ok(parsed);
                }
            }
        }

        // Parse metric name and labels
        let (metric, rest) = if let Some(brace_idx) = query.find('{') {
            (&query[..brace_idx], &query[brace_idx..])
        } else if let Some(bracket_idx) = query.find('[') {
            (&query[..bracket_idx], &query[bracket_idx..])
        } else {
            (query, "")
        };

        let mut result = Self::new(metric);

        // Parse labels
        if let Some(brace_start) = rest.find('{') {
            if let Some(brace_end) = rest.find('}') {
                let labels_str = &rest[brace_start + 1..brace_end];
                for label_part in labels_str.split(',') {
                    if let Ok(matcher) = LabelMatcher::parse(label_part.trim()) {
                        result.label_matchers.push(matcher);
                    }
                }
            }
        }

        // Parse time range
        if let Some(bracket_start) = rest.find('[') {
            if let Some(bracket_end) = rest.find(']') {
                let range_str = &rest[bracket_start + 1..bracket_end];
                if let Some(range) = parse_duration_range(range_str) {
                    result.range = Some(TimeRange::last(range));
                }
            }
        }

        Ok(result)
    }

    /// Check if metric name matches
    pub fn matches_metric(&self, metric: &str) -> bool {
        if self.metric == "*" {
            return true;
        }
        if self.metric.ends_with('*') {
            let prefix = &self.metric[..self.metric.len() - 1];
            return metric.starts_with(prefix);
        }
        self.metric == metric
    }

    /// Check if labels match
    pub fn matches_labels(&self, labels: &Labels) -> bool {
        labels.matches(&self.label_matchers)
    }

    /// Get time range
    pub fn time_range(&self) -> Option<(Timestamp, Timestamp)> {
        self.range.map(|r| (r.start, r.end))
    }

    /// Get aggregation
    pub fn aggregation(&self) -> Option<Aggregation> {
        self.aggregation
    }

    /// Get step interval
    pub fn step_interval(&self) -> Option<Duration> {
        self.step
    }

    /// Get limit
    pub fn limit(&self) -> Option<usize> {
        self.limit
    }
}

/// Parse duration string like "5m", "1h", "7d"
fn parse_duration_range(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let (num_str, unit) = if let Some(stripped) = s.strip_suffix("ms") {
        (stripped, "ms")
    } else {
        (&s[..s.len() - 1], &s[s.len() - 1..])
    };

    let num: u64 = num_str.parse().ok()?;

    match unit {
        "ms" => Some(Duration::from_millis(num)),
        "s" => Some(Duration::from_secs(num)),
        "m" => Some(Duration::from_secs(num * 60)),
        "h" => Some(Duration::from_secs(num * 3600)),
        "d" => Some(Duration::from_secs(num * 86400)),
        "w" => Some(Duration::from_secs(num * 86400 * 7)),
        _ => None,
    }
}

/// Ordering for query results
#[derive(Debug, Clone)]
pub enum OrderBy {
    /// Order by time ascending
    TimeAsc,
    /// Order by time descending
    TimeDesc,
    /// Order by value ascending
    ValueAsc,
    /// Order by value descending
    ValueDesc,
    /// Order by label
    Label(String),
}

/// Query builder for fluent API
pub struct QueryBuilder {
    storage: Arc<TimeSeriesStorage>,
    query: TimeSeriesQuery,
}

impl QueryBuilder {
    /// Create a new query builder
    pub fn new(storage: Arc<TimeSeriesStorage>) -> Self {
        Self {
            storage,
            query: TimeSeriesQuery::new(""),
        }
    }

    /// Set metric name
    pub fn metric(mut self, metric: impl Into<String>) -> Self {
        self.query.metric = metric.into();
        self
    }

    /// Add label matcher
    pub fn filter(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.query
            .label_matchers
            .push(LabelMatcher::exact(name, value));
        self
    }

    /// Add label not-equal matcher
    pub fn filter_not(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.query
            .label_matchers
            .push(LabelMatcher::not_equal(name, value));
        self
    }

    /// Add regex filter
    pub fn filter_regex(mut self, name: impl Into<String>, pattern: impl Into<String>) -> Self {
        self.query
            .label_matchers
            .push(LabelMatcher::regex(name, pattern));
        self
    }

    /// Set time range
    pub fn range(mut self, start: Timestamp, end: Timestamp) -> Self {
        self.query.range = Some(TimeRange::new(start, end));
        self
    }

    /// Set time range for last duration
    pub fn last(mut self, duration: Duration) -> Self {
        self.query.range = Some(TimeRange::last(duration));
        self
    }

    /// Set aggregation function
    pub fn aggregate(mut self, agg: Aggregation) -> Self {
        self.query.aggregation = Some(agg);
        self
    }

    /// Set step interval
    pub fn step(mut self, duration: Duration) -> Self {
        self.query.step = Some(duration);
        self
    }

    /// Add group by label
    pub fn group_by(mut self, label: impl Into<String>) -> Self {
        self.query.group_by.push(label.into());
        self
    }

    /// Set limit
    pub fn limit(mut self, limit: usize) -> Self {
        self.query.limit = Some(limit);
        self
    }

    /// Set offset
    pub fn offset(mut self, offset: usize) -> Self {
        self.query.offset = Some(offset);
        self
    }

    /// Set ordering
    pub fn order_by(mut self, order: OrderBy) -> Self {
        self.query.order_by = Some(order);
        self
    }

    /// Build the query
    pub fn build(self) -> Result<TimeSeriesQuery> {
        if self.query.metric.is_empty() {
            return Err(TimeSeriesError::QueryError(
                "metric name required".to_string(),
            ));
        }
        Ok(self.query)
    }

    /// Execute the query
    pub fn execute(self) -> Result<QueryResult> {
        let storage = self.storage.clone();
        let query = self.build()?;
        storage.execute_query(query)
    }
}

/// Result of a time series query
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Matching series with samples
    pub series: Vec<super::storage::SeriesResult>,
    /// Total samples returned
    pub total_samples: usize,
    /// Query execution time in milliseconds
    pub query_time_ms: u64,
}

impl QueryResult {
    /// Create an empty result
    pub fn empty() -> Self {
        Self {
            series: Vec::new(),
            total_samples: 0,
            query_time_ms: 0,
        }
    }

    /// Get number of series
    pub fn series_count(&self) -> usize {
        self.series.len()
    }

    /// Get total sample count
    pub fn sample_count(&self) -> usize {
        self.total_samples
    }

    /// Check if result is empty
    pub fn is_empty(&self) -> bool {
        self.series.is_empty()
    }

    /// Get first series
    pub fn first(&self) -> Option<&super::storage::SeriesResult> {
        self.series.first()
    }

    /// Iterate over series
    pub fn iter(&self) -> impl Iterator<Item = &super::storage::SeriesResult> {
        self.series.iter()
    }
}

/// Instant query (single point in time)
pub struct InstantQuery {
    query: TimeSeriesQuery,
    timestamp: Timestamp,
}

impl InstantQuery {
    /// Create a new instant query
    pub fn new(metric: impl Into<String>) -> Self {
        Self {
            query: TimeSeriesQuery::new(metric),
            timestamp: Timestamp::now(),
        }
    }

    /// Set timestamp
    pub fn at(mut self, timestamp: Timestamp) -> Self {
        self.timestamp = timestamp;
        self
    }

    /// Get the query
    pub fn query(&self) -> &TimeSeriesQuery {
        &self.query
    }
}

/// Range query (time series over range)
pub struct RangeQuery {
    query: TimeSeriesQuery,
    start: Timestamp,
    end: Timestamp,
    step: Duration,
}

impl RangeQuery {
    /// Create a new range query
    pub fn new(metric: impl Into<String>) -> Self {
        let now = Timestamp::now();
        Self {
            query: TimeSeriesQuery::new(metric),
            start: now.sub(Duration::from_secs(3600)),
            end: now,
            step: Duration::from_secs(60),
        }
    }

    /// Set time range
    pub fn range(mut self, start: Timestamp, end: Timestamp) -> Self {
        self.start = start;
        self.end = end;
        self
    }

    /// Set step
    pub fn step(mut self, step: Duration) -> Self {
        self.step = step;
        self
    }

    /// Get the query
    pub fn query(&self) -> &TimeSeriesQuery {
        &self.query
    }

    /// Calculate number of steps
    pub fn step_count(&self) -> usize {
        let range = self.end.diff(&self.start);
        (range.as_nanos() / self.step.as_nanos()) as usize + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_range() {
        let range = TimeRange::last_hours(1);
        assert!(range.duration().as_secs() <= 3601);

        let now = Timestamp::now();
        assert!(range.contains(now.sub(Duration::from_secs(1800))));
        assert!(!range.contains(now.sub(Duration::from_secs(7200))));
    }

    #[test]
    fn test_time_range_overlap() {
        let range1 = TimeRange::new(Timestamp::from_secs(100), Timestamp::from_secs(200));
        let range2 = TimeRange::new(Timestamp::from_secs(150), Timestamp::from_secs(250));
        let range3 = TimeRange::new(Timestamp::from_secs(300), Timestamp::from_secs(400));

        assert!(range1.overlaps(&range2));
        assert!(!range1.overlaps(&range3));
    }

    #[test]
    fn test_query_parse_simple() {
        let query = TimeSeriesQuery::parse("cpu.usage").unwrap();
        assert_eq!(query.metric, "cpu.usage");
    }

    #[test]
    fn test_query_parse_with_labels() {
        let query = TimeSeriesQuery::parse("cpu.usage{host=server1,env=prod}").unwrap();
        assert_eq!(query.metric, "cpu.usage");
        assert_eq!(query.label_matchers.len(), 2);
    }

    #[test]
    fn test_query_parse_with_range() {
        let query = TimeSeriesQuery::parse("cpu.usage[5m]").unwrap();
        assert_eq!(query.metric, "cpu.usage");
        assert!(query.range.is_some());
    }

    #[test]
    fn test_query_parse_with_aggregation() {
        let query = TimeSeriesQuery::parse("avg(cpu.usage[1h])").unwrap();
        assert_eq!(query.metric, "cpu.usage");
        assert_eq!(query.aggregation, Some(Aggregation::Avg));
    }

    #[test]
    fn test_query_matches_metric() {
        let query = TimeSeriesQuery::new("cpu.*");
        assert!(query.matches_metric("cpu.usage"));
        assert!(query.matches_metric("cpu.system"));
        assert!(!query.matches_metric("memory.usage"));

        let exact = TimeSeriesQuery::new("cpu.usage");
        assert!(exact.matches_metric("cpu.usage"));
        assert!(!exact.matches_metric("cpu.system"));

        let wildcard = TimeSeriesQuery::new("*");
        assert!(wildcard.matches_metric("anything"));
    }

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration_range("5s"), Some(Duration::from_secs(5)));
        assert_eq!(parse_duration_range("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_duration_range("1h"), Some(Duration::from_secs(3600)));
        assert_eq!(
            parse_duration_range("7d"),
            Some(Duration::from_secs(604800))
        );
        assert_eq!(
            parse_duration_range("100ms"),
            Some(Duration::from_millis(100))
        );
    }

    #[test]
    fn test_query_result() {
        let result = QueryResult::empty();
        assert!(result.is_empty());
        assert_eq!(result.series_count(), 0);
        assert_eq!(result.sample_count(), 0);
    }

    #[test]
    fn test_range_query() {
        let query = RangeQuery::new("cpu.usage")
            .range(Timestamp::from_secs(0), Timestamp::from_secs(3600))
            .step(Duration::from_secs(60));

        assert_eq!(query.step_count(), 61);
    }
}
