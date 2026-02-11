//! Faceted search and aggregations
//!
//! Provides faceting capabilities for search results.

use super::*;
use std::collections::{BTreeMap, HashMap};

/// A facet for categorizing search results
#[derive(Debug, Clone)]
pub struct Facet {
    /// Facet field name
    pub field: String,
    /// Facet type
    pub facet_type: FacetType,
    /// Facet values with counts
    pub values: Vec<FacetValue>,
    /// Total count
    pub total: u64,
    /// Missing count (documents without this field)
    pub missing: u64,
}

impl Facet {
    /// Create a new facet
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            facet_type: FacetType::Terms,
            values: Vec::new(),
            total: 0,
            missing: 0,
        }
    }

    /// Create a terms facet
    pub fn terms(field: impl Into<String>) -> Self {
        Self {
            facet_type: FacetType::Terms,
            ..Self::new(field)
        }
    }

    /// Create a range facet
    pub fn range(field: impl Into<String>) -> Self {
        Self {
            facet_type: FacetType::Range,
            ..Self::new(field)
        }
    }

    /// Create a histogram facet
    pub fn histogram(field: impl Into<String>, interval: f64) -> Self {
        Self {
            facet_type: FacetType::Histogram { interval },
            ..Self::new(field)
        }
    }

    /// Create a date histogram facet
    pub fn date_histogram(field: impl Into<String>, interval: DateInterval) -> Self {
        Self {
            facet_type: FacetType::DateHistogram { interval },
            ..Self::new(field)
        }
    }

    /// Add a value
    pub fn add_value(&mut self, value: FacetValue) {
        self.total += value.count;
        self.values.push(value);
    }

    /// Sort values by count descending
    pub fn sort_by_count(&mut self) {
        self.values.sort_by(|a, b| b.count.cmp(&a.count));
    }

    /// Sort values by key ascending
    pub fn sort_by_key(&mut self) {
        self.values.sort_by(|a, b| a.key.cmp(&b.key));
    }

    /// Get top N values
    pub fn top(&self, n: usize) -> Vec<&FacetValue> {
        self.values.iter().take(n).collect()
    }
}

/// Facet type
#[derive(Debug, Clone)]
pub enum FacetType {
    /// Terms aggregation
    Terms,
    /// Numeric range
    Range,
    /// Numeric histogram
    Histogram { interval: f64 },
    /// Date histogram
    DateHistogram { interval: DateInterval },
    /// Geo distance
    GeoDistance,
}

/// Date interval for histograms
#[derive(Debug, Clone, Copy)]
pub enum DateInterval {
    /// Second
    Second,
    /// Minute
    Minute,
    /// Hour
    Hour,
    /// Day
    Day,
    /// Week
    Week,
    /// Month
    Month,
    /// Quarter
    Quarter,
    /// Year
    Year,
}

impl DateInterval {
    /// Get interval in seconds
    pub fn to_seconds(&self) -> i64 {
        match self {
            DateInterval::Second => 1,
            DateInterval::Minute => 60,
            DateInterval::Hour => 3600,
            DateInterval::Day => 86400,
            DateInterval::Week => 604800,
            DateInterval::Month => 2592000,   // 30 days
            DateInterval::Quarter => 7776000, // 90 days
            DateInterval::Year => 31536000,   // 365 days
        }
    }
}

/// A single facet value
#[derive(Debug, Clone)]
pub struct FacetValue {
    /// Value key (string representation)
    pub key: String,
    /// Document count
    pub count: u64,
    /// Optional numeric value
    pub value: Option<f64>,
    /// Sub-facets
    pub sub_facets: Option<Vec<Facet>>,
}

impl FacetValue {
    /// Create a new facet value
    pub fn new(key: impl Into<String>, count: u64) -> Self {
        Self {
            key: key.into(),
            count,
            value: None,
            sub_facets: None,
        }
    }

    /// Create with numeric value
    pub fn with_value(key: impl Into<String>, count: u64, value: f64) -> Self {
        Self {
            key: key.into(),
            count,
            value: Some(value),
            sub_facets: None,
        }
    }

    /// Add sub-facet
    pub fn add_sub_facet(&mut self, facet: Facet) {
        self.sub_facets.get_or_insert_with(Vec::new).push(facet);
    }
}

/// Result of facet computation
#[derive(Debug, Clone)]
pub struct FacetResult {
    /// Facets by field name
    pub facets: HashMap<String, Facet>,
    /// Computation time in milliseconds
    pub took_ms: u64,
}

impl FacetResult {
    /// Create empty result
    pub fn new() -> Self {
        Self {
            facets: HashMap::new(),
            took_ms: 0,
        }
    }

    /// Add a facet
    pub fn add(&mut self, facet: Facet) {
        self.facets.insert(facet.field.clone(), facet);
    }

    /// Get facet by field
    pub fn get(&self, field: &str) -> Option<&Facet> {
        self.facets.get(field)
    }

    /// Get all facet fields
    pub fn fields(&self) -> Vec<&str> {
        self.facets.keys().map(|s| s.as_str()).collect()
    }
}

impl Default for FacetResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Facet builder for constructing facet requests
#[derive(Debug, Clone)]
pub struct FacetBuilder {
    facets: Vec<FacetRequest>,
}

impl FacetBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self { facets: Vec::new() }
    }

    /// Add a terms facet
    pub fn terms(mut self, field: &str) -> Self {
        self.facets.push(FacetRequest::Terms {
            field: field.to_string(),
            size: None,
            min_count: None,
            order: None,
        });
        self
    }

    /// Add a terms facet with size
    pub fn terms_with_size(mut self, field: &str, size: usize) -> Self {
        self.facets.push(FacetRequest::Terms {
            field: field.to_string(),
            size: Some(size),
            min_count: None,
            order: None,
        });
        self
    }

    /// Add a range facet
    pub fn range(mut self, field: &str, ranges: Vec<Range>) -> Self {
        self.facets.push(FacetRequest::Range {
            field: field.to_string(),
            ranges,
        });
        self
    }

    /// Add a histogram facet
    pub fn histogram(mut self, field: &str, interval: f64) -> Self {
        self.facets.push(FacetRequest::Histogram {
            field: field.to_string(),
            interval,
            min_bounds: None,
            max_bounds: None,
        });
        self
    }

    /// Add a date histogram facet
    pub fn date_histogram(mut self, field: &str, interval: DateInterval) -> Self {
        self.facets.push(FacetRequest::DateHistogram {
            field: field.to_string(),
            interval,
            format: None,
            time_zone: None,
        });
        self
    }

    /// Build facet requests
    pub fn build(self) -> Vec<FacetRequest> {
        self.facets
    }
}

impl Default for FacetBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Facet request
#[derive(Debug, Clone)]
pub enum FacetRequest {
    /// Terms aggregation
    Terms {
        field: String,
        size: Option<usize>,
        min_count: Option<u64>,
        order: Option<FacetOrder>,
    },
    /// Range aggregation
    Range { field: String, ranges: Vec<Range> },
    /// Histogram aggregation
    Histogram {
        field: String,
        interval: f64,
        min_bounds: Option<f64>,
        max_bounds: Option<f64>,
    },
    /// Date histogram aggregation
    DateHistogram {
        field: String,
        interval: DateInterval,
        format: Option<String>,
        time_zone: Option<String>,
    },
    /// Stats aggregation
    Stats { field: String },
    /// Cardinality aggregation
    Cardinality {
        field: String,
        precision_threshold: Option<u32>,
    },
}

/// Range definition
#[derive(Debug, Clone)]
pub struct Range {
    /// Range key/label
    pub key: Option<String>,
    /// From value (inclusive)
    pub from: Option<f64>,
    /// To value (exclusive)
    pub to: Option<f64>,
}

impl Range {
    /// Create a range
    pub fn new(from: Option<f64>, to: Option<f64>) -> Self {
        Self {
            key: None,
            from,
            to,
        }
    }

    /// Create a range with key
    pub fn with_key(key: &str, from: Option<f64>, to: Option<f64>) -> Self {
        Self {
            key: Some(key.to_string()),
            from,
            to,
        }
    }

    /// Create "less than" range
    pub fn lt(to: f64) -> Self {
        Self::new(None, Some(to))
    }

    /// Create "greater than or equal" range
    pub fn gte(from: f64) -> Self {
        Self::new(Some(from), None)
    }

    /// Create "between" range
    pub fn between(from: f64, to: f64) -> Self {
        Self::new(Some(from), Some(to))
    }

    /// Check if value is in range
    pub fn contains(&self, value: f64) -> bool {
        let above_from = self.from.map(|f| value >= f).unwrap_or(true);
        let below_to = self.to.map(|t| value < t).unwrap_or(true);
        above_from && below_to
    }
}

/// Facet ordering
#[derive(Debug, Clone, Copy)]
pub enum FacetOrder {
    /// By count descending
    CountDesc,
    /// By count ascending
    CountAsc,
    /// By key ascending
    KeyAsc,
    /// By key descending
    KeyDesc,
}

/// Facet collector for computing facets
pub struct FacetCollector {
    /// Terms collectors
    terms_collectors: HashMap<String, TermsCollector>,
    /// Range collectors
    range_collectors: HashMap<String, RangeCollector>,
    /// Histogram collectors
    histogram_collectors: HashMap<String, HistogramCollector>,
    /// Date histogram collectors
    date_histogram_collectors: HashMap<String, DateHistogramCollector>,
    /// Stats collectors
    stats_collectors: HashMap<String, StatsCollector>,
    /// Cardinality collectors
    cardinality_collectors: HashMap<String, CardinalityCollector>,
}

impl FacetCollector {
    /// Create a new collector
    pub fn new(requests: Vec<FacetRequest>) -> Self {
        let mut terms_collectors = HashMap::new();
        let mut range_collectors = HashMap::new();
        let mut histogram_collectors = HashMap::new();
        let mut date_histogram_collectors = HashMap::new();
        let mut stats_collectors = HashMap::new();
        let mut cardinality_collectors = HashMap::new();

        for request in requests {
            match request {
                FacetRequest::Terms {
                    field,
                    size,
                    min_count,
                    order,
                } => {
                    terms_collectors.insert(
                        field.clone(),
                        TermsCollector::new(
                            field,
                            size.unwrap_or(10),
                            min_count.unwrap_or(1),
                            order.unwrap_or(FacetOrder::CountDesc),
                        ),
                    );
                }
                FacetRequest::Range { field, ranges } => {
                    range_collectors.insert(field.clone(), RangeCollector::new(field, ranges));
                }
                FacetRequest::Histogram {
                    field, interval, ..
                } => {
                    histogram_collectors
                        .insert(field.clone(), HistogramCollector::new(field, interval));
                }
                FacetRequest::DateHistogram {
                    field,
                    interval,
                    format,
                    ..
                } => {
                    date_histogram_collectors.insert(
                        field.clone(),
                        DateHistogramCollector::new(field, interval, format),
                    );
                }
                FacetRequest::Stats { field } => {
                    stats_collectors.insert(field.clone(), StatsCollector::new(field));
                }
                FacetRequest::Cardinality {
                    field,
                    precision_threshold,
                } => {
                    cardinality_collectors.insert(
                        field.clone(),
                        CardinalityCollector::new(field, precision_threshold.unwrap_or(3000)),
                    );
                }
            }
        }

        Self {
            terms_collectors,
            range_collectors,
            histogram_collectors,
            date_histogram_collectors,
            stats_collectors,
            cardinality_collectors,
        }
    }

    /// Collect a document
    pub fn collect(&mut self, doc: &Document) {
        for (field_name, field) in &doc.fields {
            // Terms
            if let Some(collector) = self.terms_collectors.get_mut(field_name) {
                if let Some(text) = field.as_text() {
                    collector.collect(text);
                }
            }

            // Range
            if let Some(collector) = self.range_collectors.get_mut(field_name) {
                if let Some(num) = field.as_number() {
                    collector.collect(num);
                }
            }

            // Histogram
            if let Some(collector) = self.histogram_collectors.get_mut(field_name) {
                if let Some(num) = field.as_number() {
                    collector.collect(num);
                }
            }

            // Date Histogram
            if let Some(collector) = self.date_histogram_collectors.get_mut(field_name) {
                if let Some(timestamp) = field.as_date() {
                    collector.collect(timestamp);
                }
            }

            // Stats
            if let Some(collector) = self.stats_collectors.get_mut(field_name) {
                if let Some(num) = field.as_number() {
                    collector.collect(num);
                }
            }

            // Cardinality
            if let Some(collector) = self.cardinality_collectors.get_mut(field_name) {
                if let Some(text) = field.as_text() {
                    collector.collect(text);
                }
            }
        }
    }

    /// Finalize and get results
    pub fn finalize(self) -> FacetResult {
        let mut result = FacetResult::new();

        for (_, collector) in self.terms_collectors {
            result.add(collector.finalize());
        }

        for (_, collector) in self.range_collectors {
            result.add(collector.finalize());
        }

        for (_, collector) in self.histogram_collectors {
            result.add(collector.finalize());
        }

        for (_, collector) in self.date_histogram_collectors {
            result.add(collector.finalize());
        }

        for (_, collector) in self.cardinality_collectors {
            result.add(collector.finalize());
        }

        result
    }
}

/// Terms facet collector
struct TermsCollector {
    field: String,
    counts: HashMap<String, u64>,
    size: usize,
    min_count: u64,
    order: FacetOrder,
}

impl TermsCollector {
    fn new(field: String, size: usize, min_count: u64, order: FacetOrder) -> Self {
        Self {
            field,
            counts: HashMap::new(),
            size,
            min_count,
            order,
        }
    }

    fn collect(&mut self, value: &str) {
        *self.counts.entry(value.to_string()).or_insert(0) += 1;
    }

    fn finalize(self) -> Facet {
        let mut facet = Facet::terms(&self.field);

        let mut values: Vec<_> = self
            .counts
            .into_iter()
            .filter(|(_, count)| *count >= self.min_count)
            .map(|(key, count)| FacetValue::new(key, count))
            .collect();

        match self.order {
            FacetOrder::CountDesc => values.sort_by(|a, b| b.count.cmp(&a.count)),
            FacetOrder::CountAsc => values.sort_by(|a, b| a.count.cmp(&b.count)),
            FacetOrder::KeyAsc => values.sort_by(|a, b| a.key.cmp(&b.key)),
            FacetOrder::KeyDesc => values.sort_by(|a, b| b.key.cmp(&a.key)),
        }

        values.truncate(self.size);

        for value in values {
            facet.add_value(value);
        }

        facet
    }
}

/// Range facet collector
struct RangeCollector {
    field: String,
    ranges: Vec<Range>,
    counts: Vec<u64>,
}

impl RangeCollector {
    fn new(field: String, ranges: Vec<Range>) -> Self {
        let counts = vec![0; ranges.len()];
        Self {
            field,
            ranges,
            counts,
        }
    }

    fn collect(&mut self, value: f64) {
        for (i, range) in self.ranges.iter().enumerate() {
            if range.contains(value) {
                self.counts[i] += 1;
            }
        }
    }

    fn finalize(self) -> Facet {
        let mut facet = Facet::range(&self.field);

        for (i, range) in self.ranges.iter().enumerate() {
            let key = range
                .key
                .clone()
                .unwrap_or_else(|| match (range.from, range.to) {
                    (Some(from), Some(to)) => format!("{}-{}", from, to),
                    (Some(from), None) => format!("{}+", from),
                    (None, Some(to)) => format!("<{}", to),
                    (None, None) => "*".to_string(),
                });

            facet.add_value(FacetValue::new(key, self.counts[i]));
        }

        facet
    }
}

/// Histogram facet collector
struct HistogramCollector {
    field: String,
    interval: f64,
    buckets: BTreeMap<i64, u64>,
}

impl HistogramCollector {
    fn new(field: String, interval: f64) -> Self {
        Self {
            field,
            interval,
            buckets: BTreeMap::new(),
        }
    }

    fn collect(&mut self, value: f64) {
        let bucket = (value / self.interval).floor() as i64;
        *self.buckets.entry(bucket).or_insert(0) += 1;
    }

    fn finalize(self) -> Facet {
        let mut facet = Facet::histogram(&self.field, self.interval);

        for (bucket, count) in self.buckets {
            let key = (bucket as f64 * self.interval).to_string();
            facet.add_value(FacetValue::with_value(
                key,
                count,
                bucket as f64 * self.interval,
            ));
        }

        facet
    }
}

/// Date histogram facet collector
struct DateHistogramCollector {
    field: String,
    interval: DateInterval,
    format: Option<String>,
    buckets: BTreeMap<i64, u64>,
}

impl DateHistogramCollector {
    fn new(field: String, interval: DateInterval, format: Option<String>) -> Self {
        Self {
            field,
            interval,
            format,
            buckets: BTreeMap::new(),
        }
    }

    fn collect(&mut self, timestamp: i64) {
        let interval_secs = self.interval.to_seconds();
        let bucket = timestamp / interval_secs;
        *self.buckets.entry(bucket).or_insert(0) += 1;
    }

    fn finalize(self) -> Facet {
        let mut facet = Facet::date_histogram(&self.field, self.interval);
        let interval_secs = self.interval.to_seconds();

        for (bucket, count) in self.buckets {
            let timestamp = bucket * interval_secs;
            let key = match &self.format {
                Some(_fmt) => {
                    // For now, use ISO format; full format parsing would require chrono
                    format!("{}", timestamp)
                }
                None => timestamp.to_string(),
            };
            facet.add_value(FacetValue::with_value(key, count, timestamp as f64));
        }

        facet
    }
}

/// Cardinality collector using HyperLogLog-like approximation
struct CardinalityCollector {
    field: String,
    // Reserved for HyperLogLog-based cardinality estimation
    #[allow(dead_code)]
    precision_threshold: u32,
    /// Use a HashSet for exact cardinality when under threshold
    unique_values: std::collections::HashSet<u64>,
}

impl CardinalityCollector {
    fn new(field: String, precision_threshold: u32) -> Self {
        Self {
            field,
            precision_threshold,
            unique_values: std::collections::HashSet::new(),
        }
    }

    fn collect(&mut self, value: &str) {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        self.unique_values.insert(hasher.finish());
    }

    fn finalize(self) -> Facet {
        let mut facet = Facet::new(&self.field);
        facet.facet_type = FacetType::Terms; // Cardinality is a single-value aggregation

        // Add single value with cardinality count
        facet.add_value(FacetValue::with_value(
            "cardinality",
            self.unique_values.len() as u64,
            self.unique_values.len() as f64,
        ));

        facet
    }
}

/// Stats collector for numeric fields
struct StatsCollector {
    field: String,
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
    sum_of_squares: f64,
}

impl StatsCollector {
    fn new(field: String) -> Self {
        Self {
            field,
            count: 0,
            sum: 0.0,
            min: f64::MAX,
            max: f64::MIN,
            sum_of_squares: 0.0,
        }
    }

    fn collect(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.sum_of_squares += value * value;
    }

    // Used by upcoming aggregation pipeline
    #[allow(dead_code)]
    fn finalize(self) -> FieldStats {
        let avg = if self.count > 0 {
            self.sum / self.count as f64
        } else {
            0.0
        };
        let variance = if self.count > 0 {
            self.sum_of_squares / self.count as f64 - avg * avg
        } else {
            0.0
        };

        FieldStats {
            field: self.field,
            count: self.count,
            sum: self.sum,
            min: if self.count > 0 { Some(self.min) } else { None },
            max: if self.count > 0 { Some(self.max) } else { None },
            avg: if self.count > 0 { Some(avg) } else { None },
            std_dev: if self.count > 0 {
                Some(variance.sqrt())
            } else {
                None
            },
        }
    }
}

/// Statistics for a numeric field
#[derive(Debug, Clone)]
pub struct FieldStats {
    /// Field name
    pub field: String,
    /// Document count
    pub count: u64,
    /// Sum of values
    pub sum: f64,
    /// Minimum value
    pub min: Option<f64>,
    /// Maximum value
    pub max: Option<f64>,
    /// Average value
    pub avg: Option<f64>,
    /// Standard deviation
    pub std_dev: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_facet_creation() {
        let facet = Facet::terms("category");
        assert_eq!(facet.field, "category");
        assert!(matches!(facet.facet_type, FacetType::Terms));
    }

    #[test]
    fn test_facet_add_values() {
        let mut facet = Facet::terms("category");
        facet.add_value(FacetValue::new("electronics", 100));
        facet.add_value(FacetValue::new("books", 50));

        assert_eq!(facet.values.len(), 2);
        assert_eq!(facet.total, 150);
    }

    #[test]
    fn test_facet_sort() {
        let mut facet = Facet::terms("category");
        facet.add_value(FacetValue::new("a", 10));
        facet.add_value(FacetValue::new("b", 100));
        facet.add_value(FacetValue::new("c", 50));

        facet.sort_by_count();
        assert_eq!(facet.values[0].count, 100);

        facet.sort_by_key();
        assert_eq!(facet.values[0].key, "a");
    }

    #[test]
    fn test_facet_top() {
        let mut facet = Facet::terms("category");
        for i in 0..10 {
            facet.add_value(FacetValue::new(format!("cat{}", i), (i * 10) as u64));
        }

        facet.sort_by_count();
        let top3 = facet.top(3);
        assert_eq!(top3.len(), 3);
    }

    #[test]
    fn test_facet_result() {
        let mut result = FacetResult::new();
        result.add(Facet::terms("category"));
        result.add(Facet::terms("brand"));

        assert!(result.get("category").is_some());
        assert!(result.get("brand").is_some());
        assert!(result.get("missing").is_none());
    }

    #[test]
    fn test_facet_builder() {
        let requests = FacetBuilder::new()
            .terms("category")
            .terms_with_size("brand", 20)
            .range(
                "price",
                vec![
                    Range::lt(100.0),
                    Range::between(100.0, 500.0),
                    Range::gte(500.0),
                ],
            )
            .histogram("rating", 1.0)
            .build();

        assert_eq!(requests.len(), 4);
    }

    #[test]
    fn test_range() {
        let lt = Range::lt(100.0);
        assert!(lt.contains(50.0));
        assert!(!lt.contains(150.0));

        let gte = Range::gte(100.0);
        assert!(!gte.contains(50.0));
        assert!(gte.contains(100.0));
        assert!(gte.contains(150.0));

        let between = Range::between(100.0, 200.0);
        assert!(!between.contains(50.0));
        assert!(between.contains(100.0));
        assert!(between.contains(150.0));
        assert!(!between.contains(200.0));
    }

    #[test]
    fn test_range_with_key() {
        let range = Range::with_key("cheap", None, Some(100.0));
        assert_eq!(range.key, Some("cheap".to_string()));
    }

    #[test]
    fn test_date_interval() {
        assert_eq!(DateInterval::Second.to_seconds(), 1);
        assert_eq!(DateInterval::Minute.to_seconds(), 60);
        assert_eq!(DateInterval::Hour.to_seconds(), 3600);
        assert_eq!(DateInterval::Day.to_seconds(), 86400);
    }

    #[test]
    fn test_facet_value_with_subfacets() {
        let mut value = FacetValue::new("electronics", 100);
        value.add_sub_facet(Facet::terms("brand"));

        assert!(value.sub_facets.is_some());
        assert_eq!(value.sub_facets.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn test_terms_collector() {
        let mut collector =
            TermsCollector::new("category".to_string(), 10, 1, FacetOrder::CountDesc);

        collector.collect("electronics");
        collector.collect("electronics");
        collector.collect("books");

        let facet = collector.finalize();
        assert_eq!(facet.values.len(), 2);
        assert_eq!(facet.values[0].key, "electronics");
        assert_eq!(facet.values[0].count, 2);
    }

    #[test]
    fn test_range_collector() {
        let mut collector = RangeCollector::new(
            "price".to_string(),
            vec![
                Range::lt(100.0),
                Range::between(100.0, 500.0),
                Range::gte(500.0),
            ],
        );

        collector.collect(50.0);
        collector.collect(150.0);
        collector.collect(200.0);
        collector.collect(600.0);

        let facet = collector.finalize();
        assert_eq!(facet.values.len(), 3);
    }

    #[test]
    fn test_histogram_collector() {
        let mut collector = HistogramCollector::new("rating".to_string(), 1.0);

        collector.collect(1.5);
        collector.collect(2.3);
        collector.collect(2.7);
        collector.collect(4.0);

        let facet = collector.finalize();
        // Buckets: 1, 2, 4
        assert_eq!(facet.values.len(), 3);
    }

    #[test]
    fn test_stats_collector() {
        let mut collector = StatsCollector::new("price".to_string());

        collector.collect(10.0);
        collector.collect(20.0);
        collector.collect(30.0);

        let stats = collector.finalize();
        assert_eq!(stats.count, 3);
        assert_eq!(stats.sum, 60.0);
        assert_eq!(stats.min, Some(10.0));
        assert_eq!(stats.max, Some(30.0));
        assert_eq!(stats.avg, Some(20.0));
    }

    #[test]
    fn test_facet_collector() {
        let requests = vec![FacetRequest::Terms {
            field: "category".to_string(),
            size: Some(10),
            min_count: Some(1),
            order: Some(FacetOrder::CountDesc),
        }];

        let mut collector = FacetCollector::new(requests);

        let doc = Document::new("doc1").field("category", "electronics");
        collector.collect(&doc);

        let result = collector.finalize();
        assert!(result.get("category").is_some());
    }

    #[test]
    fn test_field_stats() {
        let stats = FieldStats {
            field: "price".to_string(),
            count: 100,
            sum: 5000.0,
            min: Some(10.0),
            max: Some(100.0),
            avg: Some(50.0),
            std_dev: Some(25.0),
        };

        assert_eq!(stats.field, "price");
        assert_eq!(stats.count, 100);
    }
}
