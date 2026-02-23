//! Filtered vector search
//!
//! Supports pre-filtering (apply metadata filters before vector search),
//! post-filtering (apply after candidate retrieval), and auto mode
//! (choose based on estimated selectivity).

use super::distance::distance;
use super::types::{AttributeValue, DistanceMetric, SearchResult, VectorEntry};
use super::VectorError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::debug;

/// Filter comparison operator
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum FilterOp {
    /// Exact equality
    Eq(AttributeValue),
    /// Not equal
    Ne(AttributeValue),
    /// Greater than (numeric)
    Gt(f64),
    /// Greater than or equal (numeric)
    Gte(f64),
    /// Less than (numeric)
    Lt(f64),
    /// Less than or equal (numeric)
    Lte(f64),
    /// Range (inclusive): value >= low AND value <= high
    Range { low: f64, high: f64 },
    /// Set membership: value IN set
    In(Vec<AttributeValue>),
    /// Set non-membership: value NOT IN set
    NotIn(Vec<AttributeValue>),
    /// String prefix match
    StartsWith(String),
    /// String contains
    Contains(String),
}

/// A single filter condition on a metadata attribute
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FilterCondition {
    /// Attribute key
    pub key: String,
    /// Comparison operator
    pub op: FilterOp,
}

impl FilterCondition {
    /// Create an equality filter
    pub fn eq(key: impl Into<String>, value: AttributeValue) -> Self {
        Self {
            key: key.into(),
            op: FilterOp::Eq(value),
        }
    }

    /// Create a range filter (inclusive)
    pub fn range(key: impl Into<String>, low: f64, high: f64) -> Self {
        Self {
            key: key.into(),
            op: FilterOp::Range { low, high },
        }
    }

    /// Create a set membership filter
    pub fn in_set(key: impl Into<String>, values: Vec<AttributeValue>) -> Self {
        Self {
            key: key.into(),
            op: FilterOp::In(values),
        }
    }

    /// Create a greater-than filter
    pub fn gt(key: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            op: FilterOp::Gt(value),
        }
    }

    /// Create a less-than filter
    pub fn lt(key: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            op: FilterOp::Lt(value),
        }
    }

    /// Evaluate this condition against an attribute map
    pub fn matches(&self, attributes: &HashMap<String, AttributeValue>) -> bool {
        let value = match attributes.get(&self.key) {
            Some(v) => v,
            None => return false,
        };

        match &self.op {
            FilterOp::Eq(expected) => value == expected,
            FilterOp::Ne(expected) => value != expected,
            FilterOp::Gt(threshold) => numeric_value(value).is_some_and(|v| v > *threshold),
            FilterOp::Gte(threshold) => numeric_value(value).is_some_and(|v| v >= *threshold),
            FilterOp::Lt(threshold) => numeric_value(value).is_some_and(|v| v < *threshold),
            FilterOp::Lte(threshold) => numeric_value(value).is_some_and(|v| v <= *threshold),
            FilterOp::Range { low, high } => {
                numeric_value(value).is_some_and(|v| v >= *low && v <= *high)
            }
            FilterOp::In(set) => set.contains(value),
            FilterOp::NotIn(set) => !set.contains(value),
            FilterOp::StartsWith(prefix) => {
                if let AttributeValue::String(s) = value {
                    s.starts_with(prefix.as_str())
                } else {
                    false
                }
            }
            FilterOp::Contains(substr) => {
                if let AttributeValue::String(s) = value {
                    s.contains(substr.as_str())
                } else {
                    false
                }
            }
        }
    }
}

/// Extract numeric value from AttributeValue
fn numeric_value(v: &AttributeValue) -> Option<f64> {
    match v {
        AttributeValue::Integer(i) => Some(*i as f64),
        AttributeValue::Float(f) => Some(*f),
        _ => None,
    }
}

/// Composite filter combining multiple conditions
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MetadataFilter {
    /// All conditions must match
    And(Vec<FilterCondition>),
    /// Any condition must match
    Or(Vec<FilterCondition>),
    /// Single condition
    Single(FilterCondition),
}

impl MetadataFilter {
    /// Evaluate the filter against an attribute map
    pub fn matches(&self, attributes: &HashMap<String, AttributeValue>) -> bool {
        match self {
            MetadataFilter::And(conditions) => conditions.iter().all(|c| c.matches(attributes)),
            MetadataFilter::Or(conditions) => conditions.iter().any(|c| c.matches(attributes)),
            MetadataFilter::Single(condition) => condition.matches(attributes),
        }
    }
}

/// Strategy for applying filters relative to vector search
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum FilterStrategy {
    /// Apply filter before vector search (reduces search space)
    PreFilter,
    /// Apply filter after vector search (may discard good candidates)
    PostFilter,
    /// Automatically choose based on estimated selectivity
    #[default]
    Auto,
}

/// Options for a filtered search
#[derive(Clone, Debug)]
pub struct FilteredSearchOptions {
    /// Metadata filter
    pub filter: MetadataFilter,
    /// Filter strategy
    pub strategy: FilterStrategy,
    /// For post-filter: over-fetch factor to compensate for filtered-out results
    pub overfetch_factor: f32,
}

impl FilteredSearchOptions {
    /// Create with auto strategy
    pub fn new(filter: MetadataFilter) -> Self {
        Self {
            filter,
            strategy: FilterStrategy::Auto,
            overfetch_factor: 3.0,
        }
    }

    /// Set the filter strategy
    pub fn with_strategy(mut self, strategy: FilterStrategy) -> Self {
        self.strategy = strategy;
        self
    }
}

/// Execute a filtered search over a set of vector entries
///
/// This is a general-purpose filtered search that can be used with any index.
pub fn filtered_search(
    entries: &[VectorEntry],
    query: &[f32],
    k: usize,
    metric: DistanceMetric,
    options: &FilteredSearchOptions,
) -> Result<Vec<SearchResult>, VectorError> {
    let total = entries.len();
    let strategy = resolve_strategy(&options.strategy, entries, &options.filter);

    match strategy {
        FilterStrategy::PreFilter => {
            debug!("pre-filter search: {} entries, k={}", total, k);
            pre_filter_search(entries, query, k, metric, &options.filter)
        }
        FilterStrategy::PostFilter => {
            debug!(
                "post-filter search: {} entries, k={}, overfetch={:.1}x",
                total, k, options.overfetch_factor
            );
            post_filter_search(
                entries,
                query,
                k,
                metric,
                &options.filter,
                options.overfetch_factor,
            )
        }
        FilterStrategy::Auto => {
            // Should not reach here, resolve_strategy handles Auto
            pre_filter_search(entries, query, k, metric, &options.filter)
        }
    }
}

/// Choose the best strategy based on estimated selectivity
fn resolve_strategy(
    strategy: &FilterStrategy,
    entries: &[VectorEntry],
    filter: &MetadataFilter,
) -> FilterStrategy {
    if *strategy != FilterStrategy::Auto {
        return strategy.clone();
    }

    // Estimate selectivity by sampling
    let sample_size = entries.len().min(100);
    if sample_size == 0 {
        return FilterStrategy::PreFilter;
    }

    let step = entries.len() / sample_size;
    let mut matches = 0;
    for i in (0..entries.len()).step_by(step.max(1)).take(sample_size) {
        if filter.matches(&entries[i].attributes) {
            matches += 1;
        }
    }

    let selectivity = matches as f64 / sample_size as f64;

    // If filter is very selective (< 10% pass), use pre-filter
    // If filter passes most entries (> 50%), use post-filter
    if selectivity < 0.1 {
        FilterStrategy::PreFilter
    } else if selectivity > 0.5 {
        FilterStrategy::PostFilter
    } else {
        FilterStrategy::PreFilter
    }
}

/// Pre-filter: filter entries first, then search over matching subset
fn pre_filter_search(
    entries: &[VectorEntry],
    query: &[f32],
    k: usize,
    metric: DistanceMetric,
    filter: &MetadataFilter,
) -> Result<Vec<SearchResult>, VectorError> {
    let mut results: Vec<SearchResult> = entries
        .iter()
        .filter(|entry| filter.matches(&entry.attributes))
        .filter_map(|entry| {
            entry.data.as_f32().map(|vec| {
                let dist = distance(query, vec, metric);
                SearchResult::new(entry.id.clone(), dist).with_attributes(entry.attributes.clone())
            })
        })
        .collect();

    results.sort_by(|a, b| {
        a.score
            .partial_cmp(&b.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    results.truncate(k);
    Ok(results)
}

/// Post-filter: search over all entries, then filter results
fn post_filter_search(
    entries: &[VectorEntry],
    query: &[f32],
    k: usize,
    metric: DistanceMetric,
    filter: &MetadataFilter,
    overfetch_factor: f32,
) -> Result<Vec<SearchResult>, VectorError> {
    let overfetch_k = ((k as f32) * overfetch_factor).ceil() as usize;

    // Get top-overfetch_k candidates without filter
    let mut candidates: Vec<(usize, f32)> = entries
        .iter()
        .enumerate()
        .filter_map(|(i, entry)| {
            entry
                .data
                .as_f32()
                .map(|vec| (i, distance(query, vec, metric)))
        })
        .collect();

    candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    let mut results: Vec<SearchResult> = Vec::with_capacity(k);
    for (idx, dist) in candidates.into_iter().take(overfetch_k) {
        let entry = &entries[idx];
        if filter.matches(&entry.attributes) {
            results.push(
                SearchResult::new(entry.id.clone(), dist).with_attributes(entry.attributes.clone()),
            );
            if results.len() >= k {
                break;
            }
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entries() -> Vec<VectorEntry> {
        vec![
            VectorEntry::from_f32("doc1", &[1.0, 0.0, 0.0])
                .with_attribute("category", AttributeValue::String("tech".to_string()))
                .with_attribute("score", AttributeValue::Integer(90)),
            VectorEntry::from_f32("doc2", &[0.9, 0.1, 0.0])
                .with_attribute("category", AttributeValue::String("tech".to_string()))
                .with_attribute("score", AttributeValue::Integer(70)),
            VectorEntry::from_f32("doc3", &[0.0, 1.0, 0.0])
                .with_attribute("category", AttributeValue::String("science".to_string()))
                .with_attribute("score", AttributeValue::Integer(85)),
            VectorEntry::from_f32("doc4", &[0.0, 0.0, 1.0])
                .with_attribute("category", AttributeValue::String("art".to_string()))
                .with_attribute("score", AttributeValue::Integer(60)),
            VectorEntry::from_f32("doc5", &[0.7, 0.7, 0.0])
                .with_attribute("category", AttributeValue::String("tech".to_string()))
                .with_attribute("score", AttributeValue::Integer(95)),
        ]
    }

    #[test]
    fn test_equality_filter() {
        let cond = FilterCondition::eq("category", AttributeValue::String("tech".to_string()));
        let entries = make_entries();
        assert!(cond.matches(&entries[0].attributes));
        assert!(!cond.matches(&entries[2].attributes));
    }

    #[test]
    fn test_range_filter() {
        let cond = FilterCondition::range("score", 80.0, 100.0);
        let entries = make_entries();
        assert!(cond.matches(&entries[0].attributes)); // score=90
        assert!(!cond.matches(&entries[1].attributes)); // score=70
        assert!(cond.matches(&entries[2].attributes)); // score=85
    }

    #[test]
    fn test_in_set_filter() {
        let cond = FilterCondition::in_set(
            "category",
            vec![
                AttributeValue::String("tech".to_string()),
                AttributeValue::String("science".to_string()),
            ],
        );
        let entries = make_entries();
        assert!(cond.matches(&entries[0].attributes));
        assert!(cond.matches(&entries[2].attributes));
        assert!(!cond.matches(&entries[3].attributes));
    }

    #[test]
    fn test_gt_lt_filter() {
        let entries = make_entries();
        let gt = FilterCondition::gt("score", 80.0);
        assert!(gt.matches(&entries[0].attributes)); // 90 > 80
        assert!(!gt.matches(&entries[1].attributes)); // 70 > 80 = false

        let lt = FilterCondition::lt("score", 80.0);
        assert!(!lt.matches(&entries[0].attributes)); // 90 < 80 = false
        assert!(lt.matches(&entries[1].attributes)); // 70 < 80
    }

    #[test]
    fn test_and_filter() {
        let filter = MetadataFilter::And(vec![
            FilterCondition::eq("category", AttributeValue::String("tech".to_string())),
            FilterCondition::gt("score", 80.0),
        ]);
        let entries = make_entries();
        assert!(filter.matches(&entries[0].attributes)); // tech + 90
        assert!(!filter.matches(&entries[1].attributes)); // tech + 70
        assert!(!filter.matches(&entries[2].attributes)); // science + 85
    }

    #[test]
    fn test_or_filter() {
        let filter = MetadataFilter::Or(vec![
            FilterCondition::eq("category", AttributeValue::String("art".to_string())),
            FilterCondition::gt("score", 90.0),
        ]);
        let entries = make_entries();
        assert!(!filter.matches(&entries[0].attributes)); // tech + 90 (not > 90)
        assert!(filter.matches(&entries[3].attributes)); // art
        assert!(filter.matches(&entries[4].attributes)); // tech + 95
    }

    #[test]
    fn test_pre_filter_search() {
        let entries = make_entries();
        let filter = MetadataFilter::Single(FilterCondition::eq(
            "category",
            AttributeValue::String("tech".to_string()),
        ));
        let options = FilteredSearchOptions::new(filter).with_strategy(FilterStrategy::PreFilter);

        let results = filtered_search(
            &entries,
            &[1.0, 0.0, 0.0],
            2,
            DistanceMetric::Cosine,
            &options,
        )
        .unwrap();

        assert_eq!(results.len(), 2);
        // All results should be "tech"
        for r in &results {
            let attrs = r.attributes.as_ref().unwrap();
            assert_eq!(
                attrs.get("category"),
                Some(&AttributeValue::String("tech".to_string()))
            );
        }
    }

    #[test]
    fn test_post_filter_search() {
        let entries = make_entries();
        let filter = MetadataFilter::Single(FilterCondition::eq(
            "category",
            AttributeValue::String("tech".to_string()),
        ));
        let options = FilteredSearchOptions::new(filter).with_strategy(FilterStrategy::PostFilter);

        let results = filtered_search(
            &entries,
            &[1.0, 0.0, 0.0],
            2,
            DistanceMetric::Cosine,
            &options,
        )
        .unwrap();

        assert_eq!(results.len(), 2);
        for r in &results {
            let attrs = r.attributes.as_ref().unwrap();
            assert_eq!(
                attrs.get("category"),
                Some(&AttributeValue::String("tech".to_string()))
            );
        }
    }

    #[test]
    fn test_auto_filter_strategy() {
        let entries = make_entries();
        let filter = MetadataFilter::Single(FilterCondition::eq(
            "category",
            AttributeValue::String("tech".to_string()),
        ));
        let options = FilteredSearchOptions::new(filter);

        // Auto should work regardless of which strategy it picks
        let results = filtered_search(
            &entries,
            &[1.0, 0.0, 0.0],
            2,
            DistanceMetric::Cosine,
            &options,
        )
        .unwrap();
        assert!(!results.is_empty());
        assert!(results.len() <= 2);
    }

    #[test]
    fn test_filter_no_matches() {
        let entries = make_entries();
        let filter = MetadataFilter::Single(FilterCondition::eq(
            "category",
            AttributeValue::String("nonexistent".to_string()),
        ));
        let options = FilteredSearchOptions::new(filter).with_strategy(FilterStrategy::PreFilter);

        let results = filtered_search(
            &entries,
            &[1.0, 0.0, 0.0],
            5,
            DistanceMetric::Cosine,
            &options,
        )
        .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_filter_missing_attribute() {
        let mut attrs = HashMap::new();
        attrs.insert(
            "existing".to_string(),
            AttributeValue::String("yes".to_string()),
        );

        let cond = FilterCondition::eq("missing", AttributeValue::String("no".to_string()));
        assert!(!cond.matches(&attrs));
    }

    #[test]
    fn test_starts_with_filter() {
        let mut attrs = HashMap::new();
        attrs.insert(
            "name".to_string(),
            AttributeValue::String("hello_world".to_string()),
        );

        let cond = FilterCondition {
            key: "name".to_string(),
            op: FilterOp::StartsWith("hello".to_string()),
        };
        assert!(cond.matches(&attrs));

        let cond2 = FilterCondition {
            key: "name".to_string(),
            op: FilterOp::StartsWith("world".to_string()),
        };
        assert!(!cond2.matches(&attrs));
    }

    #[test]
    fn test_contains_filter() {
        let mut attrs = HashMap::new();
        attrs.insert(
            "description".to_string(),
            AttributeValue::String("the quick brown fox".to_string()),
        );

        let cond = FilterCondition {
            key: "description".to_string(),
            op: FilterOp::Contains("quick".to_string()),
        };
        assert!(cond.matches(&attrs));
    }

    #[test]
    fn test_not_in_filter() {
        let entries = make_entries();
        let cond = FilterCondition {
            key: "category".to_string(),
            op: FilterOp::NotIn(vec![AttributeValue::String("art".to_string())]),
        };
        assert!(cond.matches(&entries[0].attributes)); // tech
        assert!(!cond.matches(&entries[3].attributes)); // art
    }
}
