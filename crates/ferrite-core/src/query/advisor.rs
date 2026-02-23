//! AI-Powered Query Advisor
//!
//! Recommends indices, query rewrites, and materialized views based on
//! observed workload patterns. Detects anti-patterns such as N+1 queries,
//! full table scans, and unbounded result sets.
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────────────────┐
//! │                   Query Advisor                        │
//! │  ┌──────────────┐  ┌───────────────┐  ┌────────────┐  │
//! │  │ Query        │  │ Anti-Pattern  │  │ Recommend- │  │
//! │  │ Recorder     │──▶ Detector      │──▶ ation      │  │
//! │  │ (capture,    │  │ (N+1, scans, │  │ Engine     │  │
//! │  │  aggregate)  │  │  unbounded)  │  │            │  │
//! │  └──────────────┘  └───────────────┘  └────────────┘  │
//! └────────────────────────────────────────────────────────┘
//! ```

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors produced by the query advisor.
#[derive(Debug, Clone, thiserror::Error)]
pub enum AdvisorError {
    /// Not enough queries have been recorded for meaningful analysis.
    #[error("insufficient data: need at least {0} queries")]
    InsufficientData(u32),
    /// The analysis process encountered an error.
    #[error("analysis failed: {0}")]
    AnalysisFailed(String),
    /// An internal advisor error occurred.
    #[error("internal advisor error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the query advisor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorConfig {
    /// Minimum queries before analysis is meaningful.
    pub min_queries_for_analysis: u32,
    /// Threshold in milliseconds to consider a query slow.
    pub slow_query_threshold_ms: u64,
    /// Number of same-fingerprint queries within a window that signals N+1.
    pub n_plus_one_threshold: u32,
    /// Maximum number of recommendations to return.
    pub recommendation_limit: usize,
    /// Whether to auto-apply safe recommendations.
    pub auto_apply: bool,
    /// Minimum confidence score to surface a recommendation.
    pub confidence_threshold: f64,
}

impl Default for AdvisorConfig {
    fn default() -> Self {
        Self {
            min_queries_for_analysis: 100,
            slow_query_threshold_ms: 100,
            n_plus_one_threshold: 5,
            recommendation_limit: 20,
            auto_apply: false,
            confidence_threshold: 0.7,
        }
    }
}

// ---------------------------------------------------------------------------
// Query execution record
// ---------------------------------------------------------------------------

/// A single observed query execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExecution {
    /// The raw query string.
    pub query: String,
    /// Normalized query fingerprint for grouping.
    pub fingerprint: String,
    /// Execution duration in microseconds.
    pub execution_time_us: u64,
    /// Number of rows scanned during execution.
    pub rows_scanned: u64,
    /// Number of rows returned by the query.
    pub rows_returned: u64,
    /// Whether an index was used for this query.
    pub used_index: bool,
    /// When the query was executed.
    pub timestamp: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Workload analysis types
// ---------------------------------------------------------------------------

/// Summary of a recurring query pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternSummary {
    /// Normalized query fingerprint.
    pub fingerprint: String,
    /// An example raw query matching this pattern.
    pub example_query: String,
    /// Total number of executions for this pattern.
    pub count: u64,
    /// Average latency in microseconds.
    pub avg_latency_us: f64,
    /// Total rows scanned across all executions.
    pub total_rows_scanned: u64,
}

/// Result of a full workload analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadAnalysis {
    /// Total number of queries analyzed.
    pub total_queries: u64,
    /// Number of distinct query fingerprints.
    pub unique_patterns: u64,
    /// Average query latency in microseconds.
    pub avg_latency_us: f64,
    /// 99th percentile latency in microseconds.
    pub p99_latency_us: f64,
    /// Count of queries exceeding the slow threshold.
    pub slow_queries: u64,
    /// Most frequent query patterns.
    pub top_patterns: Vec<PatternSummary>,
    /// Key patterns with the highest total execution time.
    pub hot_keys: Vec<String>,
}

// ---------------------------------------------------------------------------
// Recommendation types
// ---------------------------------------------------------------------------

/// Classification of a recommendation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RecommendationType {
    /// Suggests creating a new index.
    CreateIndex,
    /// Suggests creating a materialized view.
    CreateView,
    /// Suggests rewriting the query for efficiency.
    RewriteQuery,
    /// Suggests adding a LIMIT clause.
    AddLimit,
    /// Suggests removing an unused index.
    RemoveUnusedIndex,
    /// Suggests creating a composite index.
    CompositIndex,
    /// Suggests a partition key change.
    PartitionKey,
}

/// Priority level for a recommendation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    /// Must be addressed immediately.
    Critical,
    /// Should be addressed soon.
    High,
    /// Worth addressing when possible.
    Medium,
    /// Minor improvement opportunity.
    Low,
}

/// Estimated impact of applying a recommendation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Impact {
    /// Expected latency reduction as a percentage.
    pub latency_reduction_percent: f64,
    /// Expected reduction in rows scanned as a percentage.
    pub scan_reduction_percent: f64,
    /// Estimated additional memory cost in bytes.
    pub memory_cost_bytes: u64,
    /// Priority level of this impact.
    pub priority: Priority,
}

/// A concrete recommendation from the advisor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Recommendation {
    /// Unique identifier for this recommendation.
    pub id: String,
    /// The type of recommendation.
    pub rec_type: RecommendationType,
    /// Short human-readable title.
    pub title: String,
    /// Detailed explanation of the recommendation.
    pub description: String,
    /// Estimated impact of applying this recommendation.
    pub impact: Impact,
    /// Confidence score between 0.0 and 1.0.
    pub confidence: f64,
    /// Optional SQL statement to apply the recommendation.
    pub sql: Option<String>,
    /// Whether this recommendation can be applied automatically.
    pub auto_applicable: bool,
}

// ---------------------------------------------------------------------------
// Index suggestion
// ---------------------------------------------------------------------------

/// A suggested index to create.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSuggestion {
    /// The key pattern that would benefit from an index.
    pub key_pattern: String,
    /// Fields to include in the index.
    pub fields: Vec<String>,
    /// The type of index to create (e.g., hash).
    pub index_type: String,
    /// Estimated query speedup factor.
    pub estimated_speedup: f64,
    /// Estimated memory cost of the index in bytes.
    pub estimated_memory_bytes: u64,
    /// Confidence score between 0.0 and 1.0.
    pub confidence: f64,
    /// Human-readable explanation of why this index is suggested.
    pub reason: String,
}

// ---------------------------------------------------------------------------
// Anti-pattern detection
// ---------------------------------------------------------------------------

/// Classification of an anti-pattern.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AntiPatternType {
    /// Repeated identical queries in a short time window.
    NPlusOne,
    /// Query missing a scan optimization.
    MissingScan,
    /// Query scanning all rows without an index.
    FullTableScan,
    /// Query without a LIMIT clause returning many rows.
    UnboundedQuery,
    /// Unnecessary sort operation in the query.
    RedundantSort,
    /// Redundant DISTINCT clause in the query.
    UnnecessaryDistinct,
    /// Unintended Cartesian product between tables.
    CartesianJoin,
}

/// A detected anti-pattern in the workload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AntiPattern {
    /// Classification of the detected anti-pattern.
    pub pattern_type: AntiPatternType,
    /// Human-readable description of the issue.
    pub description: String,
    /// Number of queries affected by this anti-pattern.
    pub affected_queries: u64,
    /// An example query exhibiting the anti-pattern.
    pub example: String,
    /// Suggested fix for the anti-pattern.
    pub fix: String,
}

// ---------------------------------------------------------------------------
// Improvement estimate
// ---------------------------------------------------------------------------

/// Before/after estimate for a recommendation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImprovementEstimate {
    /// Average latency before applying the recommendation (µs).
    pub before_latency_us: f64,
    /// Estimated latency after applying the recommendation (µs).
    pub after_latency_us: f64,
    /// Percentage improvement in latency.
    pub improvement_percent: f64,
    /// Confidence score of the estimate.
    pub confidence: f64,
}

// ---------------------------------------------------------------------------
// Advisor stats
// ---------------------------------------------------------------------------

/// Cumulative statistics for the advisor.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AdvisorStats {
    /// Total number of queries analyzed.
    pub queries_analyzed: u64,
    /// Number of recommendations produced.
    pub recommendations_generated: u64,
    /// Number of recommendations that have been applied.
    pub recommendations_applied: u64,
    /// Number of anti-patterns detected.
    pub antipatterns_detected: u64,
    /// Number of index suggestions produced.
    pub indices_suggested: u64,
    /// Average improvement percentage across recommendations.
    pub avg_improvement_percent: f64,
}

// ---------------------------------------------------------------------------
// Internal per-pattern tracking
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct PatternTracker {
    fingerprint: String,
    example_query: String,
    count: u64,
    total_latency_us: u64,
    total_rows_scanned: u64,
    total_rows_returned: u64,
    used_index_count: u64,
    /// Timestamps of recent executions for N+1 detection (microsecond epoch).
    recent_timestamps_us: Vec<i64>,
    has_limit: bool,
}

// ---------------------------------------------------------------------------
// QueryAdvisor
// ---------------------------------------------------------------------------

/// AI-powered query recommendation engine.
///
/// Records observed query executions, detects anti-patterns, and produces
/// actionable recommendations (indices, rewrites, views).
pub struct QueryAdvisor {
    config: AdvisorConfig,
    patterns: DashMap<String, PatternTracker>,
    queries_analyzed: AtomicU64,
    latencies: Arc<RwLock<Vec<u64>>>,
}

impl QueryAdvisor {
    /// Creates a new advisor with the given configuration.
    pub fn new(config: AdvisorConfig) -> Self {
        Self {
            config,
            patterns: DashMap::new(),
            queries_analyzed: AtomicU64::new(0),
            latencies: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Records a single query execution for analysis.
    pub fn record_query(&self, query: &QueryExecution) {
        self.queries_analyzed.fetch_add(1, Ordering::Relaxed);

        {
            let mut latencies = self.latencies.write();
            latencies.push(query.execution_time_us);
        }

        let has_limit = query.query.to_ascii_uppercase().contains("LIMIT");

        let ts_us = query.timestamp.timestamp_micros();

        let mut entry = self
            .patterns
            .entry(query.fingerprint.clone())
            .or_insert_with(|| PatternTracker {
                fingerprint: query.fingerprint.clone(),
                example_query: query.query.clone(),
                count: 0,
                total_latency_us: 0,
                total_rows_scanned: 0,
                total_rows_returned: 0,
                used_index_count: 0,
                recent_timestamps_us: Vec::new(),
                has_limit,
            });

        let tracker = entry.value_mut();
        tracker.count += 1;
        tracker.total_latency_us += query.execution_time_us;
        tracker.total_rows_scanned += query.rows_scanned;
        tracker.total_rows_returned += query.rows_returned;
        if query.used_index {
            tracker.used_index_count += 1;
        }
        if !has_limit {
            tracker.has_limit = false;
        }
        tracker.recent_timestamps_us.push(ts_us);

        // Keep only timestamps within the last 100ms window for N+1 detection.
        let window_us: i64 = 100_000; // 100ms
        let cutoff = ts_us - window_us;
        tracker.recent_timestamps_us.retain(|&t| t >= cutoff);
    }

    /// Analyzes the full recorded workload and returns a summary.
    pub fn analyze_workload(&self) -> Result<WorkloadAnalysis, AdvisorError> {
        let total = self.queries_analyzed.load(Ordering::Relaxed);
        if total < self.config.min_queries_for_analysis as u64 {
            return Err(AdvisorError::InsufficientData(
                self.config.min_queries_for_analysis,
            ));
        }

        let latencies = self.latencies.read();
        let avg_latency_us = if latencies.is_empty() {
            0.0
        } else {
            latencies.iter().sum::<u64>() as f64 / latencies.len() as f64
        };

        let p99_latency_us = percentile(&latencies, 99.0);

        let slow_threshold_us = self.config.slow_query_threshold_ms * 1000;
        let slow_queries = latencies
            .iter()
            .filter(|&&l| l >= slow_threshold_us)
            .count() as u64;

        let mut summaries: Vec<PatternSummary> = self
            .patterns
            .iter()
            .map(|entry| {
                let t = entry.value();
                PatternSummary {
                    fingerprint: t.fingerprint.clone(),
                    example_query: t.example_query.clone(),
                    count: t.count,
                    avg_latency_us: if t.count > 0 {
                        t.total_latency_us as f64 / t.count as f64
                    } else {
                        0.0
                    },
                    total_rows_scanned: t.total_rows_scanned,
                }
            })
            .collect();

        summaries.sort_by(|a, b| b.count.cmp(&a.count));
        summaries.truncate(10);

        // Hot keys: patterns with highest total execution time.
        let mut hot: Vec<(String, u64)> = self
            .patterns
            .iter()
            .map(|e| (e.value().fingerprint.clone(), e.value().total_latency_us))
            .collect();
        hot.sort_by(|a, b| b.1.cmp(&a.1));
        hot.truncate(5);
        let hot_keys = hot.into_iter().map(|(k, _)| k).collect();

        Ok(WorkloadAnalysis {
            total_queries: total,
            unique_patterns: self.patterns.len() as u64,
            avg_latency_us,
            p99_latency_us,
            slow_queries,
            top_patterns: summaries,
            hot_keys,
        })
    }

    /// Returns all recommendations that meet the confidence threshold.
    pub fn get_recommendations(&self) -> Vec<Recommendation> {
        let mut recs = Vec::new();

        // Index suggestions → recommendations
        for suggestion in self.get_index_suggestions() {
            if suggestion.confidence >= self.config.confidence_threshold {
                recs.push(Recommendation {
                    id: format!("idx-{}", suggestion.key_pattern),
                    rec_type: RecommendationType::CreateIndex,
                    title: format!("Create index on {}", suggestion.key_pattern),
                    description: suggestion.reason.clone(),
                    impact: Impact {
                        latency_reduction_percent: (1.0 - 1.0 / suggestion.estimated_speedup)
                            * 100.0,
                        scan_reduction_percent: 90.0,
                        memory_cost_bytes: suggestion.estimated_memory_bytes,
                        priority: if suggestion.estimated_speedup > 10.0 {
                            Priority::Critical
                        } else if suggestion.estimated_speedup > 5.0 {
                            Priority::High
                        } else {
                            Priority::Medium
                        },
                    },
                    confidence: suggestion.confidence,
                    sql: Some(format!(
                        "CREATE INDEX ON {} ({})",
                        suggestion.key_pattern,
                        suggestion.fields.join(", ")
                    )),
                    auto_applicable: false,
                });
            }
        }

        // Anti-pattern recommendations
        for ap in self.detect_antipatterns() {
            let (rec_type, confidence) = match ap.pattern_type {
                AntiPatternType::NPlusOne => (RecommendationType::RewriteQuery, 0.9),
                AntiPatternType::FullTableScan | AntiPatternType::MissingScan => {
                    (RecommendationType::CreateIndex, 0.85)
                }
                AntiPatternType::UnboundedQuery => (RecommendationType::AddLimit, 0.8),
                AntiPatternType::RedundantSort => (RecommendationType::RewriteQuery, 0.75),
                AntiPatternType::UnnecessaryDistinct => (RecommendationType::RewriteQuery, 0.7),
                AntiPatternType::CartesianJoin => (RecommendationType::RewriteQuery, 0.95),
            };

            if confidence >= self.config.confidence_threshold {
                recs.push(Recommendation {
                    id: format!("ap-{:?}-{}", ap.pattern_type, recs.len()),
                    rec_type,
                    title: format!("Fix {:?} anti-pattern", ap.pattern_type),
                    description: ap.description.clone(),
                    impact: Impact {
                        latency_reduction_percent: match ap.pattern_type {
                            AntiPatternType::NPlusOne => 80.0,
                            AntiPatternType::FullTableScan => 70.0,
                            AntiPatternType::UnboundedQuery => 50.0,
                            _ => 30.0,
                        },
                        scan_reduction_percent: match ap.pattern_type {
                            AntiPatternType::FullTableScan | AntiPatternType::MissingScan => 90.0,
                            _ => 0.0,
                        },
                        memory_cost_bytes: 0,
                        priority: match ap.pattern_type {
                            AntiPatternType::NPlusOne | AntiPatternType::CartesianJoin => {
                                Priority::Critical
                            }
                            AntiPatternType::FullTableScan => Priority::High,
                            AntiPatternType::UnboundedQuery => Priority::Medium,
                            _ => Priority::Low,
                        },
                    },
                    confidence,
                    sql: None,
                    auto_applicable: ap.pattern_type == AntiPatternType::UnboundedQuery,
                });
            }
        }

        recs.sort_by(|a, b| a.impact.priority.cmp(&b.impact.priority));
        recs.truncate(self.config.recommendation_limit);
        recs
    }

    /// Suggests indices based on full-scan patterns.
    pub fn get_index_suggestions(&self) -> Vec<IndexSuggestion> {
        let mut suggestions = Vec::new();

        for entry in self.patterns.iter() {
            let t = entry.value();
            if t.count == 0 || t.total_rows_returned == 0 {
                continue;
            }

            let scan_ratio = t.total_rows_scanned as f64 / t.total_rows_returned as f64;
            let index_usage_rate = t.used_index_count as f64 / t.count as f64;

            // Recommend an index if scan ratio is high and index usage is low.
            if scan_ratio > 100.0 && index_usage_rate < 0.5 {
                let estimated_speedup = (scan_ratio / 10.0).min(100.0);
                let avg_rows = t.total_rows_scanned / t.count;
                let estimated_memory = avg_rows * 64; // rough 64 bytes per index entry

                suggestions.push(IndexSuggestion {
                    key_pattern: t.fingerprint.clone(),
                    fields: vec![t.fingerprint.clone()],
                    index_type: "hash".to_string(),
                    estimated_speedup,
                    estimated_memory_bytes: estimated_memory,
                    confidence: compute_index_confidence(scan_ratio, t.count),
                    reason: format!(
                        "Pattern '{}' scans {}x more rows than returned (avg {}/{} rows) \
                         with {:.0}% index usage across {} executions",
                        t.fingerprint,
                        scan_ratio as u64,
                        t.total_rows_scanned / t.count,
                        t.total_rows_returned / t.count,
                        index_usage_rate * 100.0,
                        t.count,
                    ),
                });
            }
        }

        suggestions.sort_by(|a, b| {
            b.estimated_speedup
                .partial_cmp(&a.estimated_speedup)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        suggestions
    }

    /// Detects anti-patterns in the recorded workload.
    pub fn detect_antipatterns(&self) -> Vec<AntiPattern> {
        let mut results = Vec::new();

        for entry in self.patterns.iter() {
            let t = entry.value();

            // N+1 detection: many identical fingerprints in a short time window.
            if t.recent_timestamps_us.len() as u32 >= self.config.n_plus_one_threshold {
                results.push(AntiPattern {
                    pattern_type: AntiPatternType::NPlusOne,
                    description: format!(
                        "Pattern '{}' executed {} times within 100ms window — likely N+1 query",
                        t.fingerprint,
                        t.recent_timestamps_us.len()
                    ),
                    affected_queries: t.count,
                    example: t.example_query.clone(),
                    fix: format!(
                        "Batch '{}' into a single MGET or pipeline command",
                        t.fingerprint
                    ),
                });
            }

            // Full table scan detection: rows_scanned / rows_returned > 100.
            if t.total_rows_returned > 0 {
                let scan_ratio = t.total_rows_scanned as f64 / t.total_rows_returned as f64;
                if scan_ratio > 100.0 {
                    results.push(AntiPattern {
                        pattern_type: AntiPatternType::FullTableScan,
                        description: format!(
                            "Pattern '{}' scans {}x more rows than returned — consider adding an index",
                            t.fingerprint, scan_ratio as u64
                        ),
                        affected_queries: t.count,
                        example: t.example_query.clone(),
                        fix: format!(
                            "Create an index on the filter fields for pattern '{}'",
                            t.fingerprint
                        ),
                    });
                }
            }

            // Unbounded query detection: no LIMIT clause.
            if !t.has_limit && t.total_rows_returned > 0 {
                let avg_returned = t.total_rows_returned / t.count.max(1);
                if avg_returned > 100 {
                    results.push(AntiPattern {
                        pattern_type: AntiPatternType::UnboundedQuery,
                        description: format!(
                            "Pattern '{}' returns avg {} rows without LIMIT",
                            t.fingerprint, avg_returned
                        ),
                        affected_queries: t.count,
                        example: t.example_query.clone(),
                        fix: "Add a LIMIT clause to bound the result set".to_string(),
                    });
                }
            }
        }

        results
    }

    /// Estimates the improvement from applying a recommendation.
    pub fn estimate_improvement(&self, rec: &Recommendation) -> ImprovementEstimate {
        let latencies = self.latencies.read();
        let current_avg = if latencies.is_empty() {
            0.0
        } else {
            latencies.iter().sum::<u64>() as f64 / latencies.len() as f64
        };

        let reduction = rec.impact.latency_reduction_percent / 100.0;
        let after = current_avg * (1.0 - reduction);
        let improvement = if current_avg > 0.0 {
            ((current_avg - after) / current_avg) * 100.0
        } else {
            0.0
        };

        ImprovementEstimate {
            before_latency_us: current_avg,
            after_latency_us: after,
            improvement_percent: improvement,
            confidence: rec.confidence,
        }
    }

    /// Returns cumulative advisor statistics.
    pub fn get_stats(&self) -> AdvisorStats {
        let recs = self.get_recommendations();
        let antipatterns = self.detect_antipatterns();
        let suggestions = self.get_index_suggestions();

        let avg_improvement = if recs.is_empty() {
            0.0
        } else {
            recs.iter()
                .map(|r| r.impact.latency_reduction_percent)
                .sum::<f64>()
                / recs.len() as f64
        };

        AdvisorStats {
            queries_analyzed: self.queries_analyzed.load(Ordering::Relaxed),
            recommendations_generated: recs.len() as u64,
            recommendations_applied: 0,
            antipatterns_detected: antipatterns.len() as u64,
            indices_suggested: suggestions.len() as u64,
            avg_improvement_percent: avg_improvement,
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Computes the p-th percentile from a slice of latency values.
fn percentile(values: &[u64], p: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).ceil() as usize;
    let idx = idx.min(sorted.len() - 1);
    sorted[idx] as f64
}

/// Heuristic confidence score for an index suggestion based on scan ratio and
/// observation count.
fn compute_index_confidence(scan_ratio: f64, count: u64) -> f64 {
    let ratio_score = (scan_ratio / 1000.0).min(1.0);
    let count_score = (count as f64 / 100.0).min(1.0);
    (ratio_score * 0.6 + count_score * 0.4).min(1.0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn test_config() -> AdvisorConfig {
        AdvisorConfig {
            min_queries_for_analysis: 3,
            slow_query_threshold_ms: 10,
            n_plus_one_threshold: 5,
            recommendation_limit: 20,
            auto_apply: false,
            confidence_threshold: 0.1,
        }
    }

    fn make_query(
        fingerprint: &str,
        query: &str,
        time_us: u64,
        scanned: u64,
        returned: u64,
        used_index: bool,
    ) -> QueryExecution {
        QueryExecution {
            query: query.to_string(),
            fingerprint: fingerprint.to_string(),
            execution_time_us: time_us,
            rows_scanned: scanned,
            rows_returned: returned,
            used_index,
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn test_query_recording() {
        let advisor = QueryAdvisor::new(test_config());
        let q = make_query("GET user:$1", "GET user:42", 500, 1, 1, true);

        advisor.record_query(&q);
        advisor.record_query(&q);

        let stats = advisor.get_stats();
        assert_eq!(stats.queries_analyzed, 2);
        assert_eq!(
            advisor.patterns.get("GET user:$1").map(|e| e.value().count),
            Some(2)
        );
    }

    #[test]
    fn test_workload_analysis() {
        let advisor = QueryAdvisor::new(test_config());
        for i in 0..5 {
            advisor.record_query(&make_query(
                "SCAN keys:*",
                &format!("SCAN keys:{}", i),
                1000 + i * 100,
                500,
                5,
                false,
            ));
        }

        let analysis = advisor.analyze_workload().unwrap();
        assert_eq!(analysis.total_queries, 5);
        assert!(analysis.avg_latency_us > 0.0);
        assert!(analysis.p99_latency_us >= analysis.avg_latency_us);
        assert!(analysis.unique_patterns >= 1);
    }

    #[test]
    fn test_workload_analysis_insufficient_data() {
        let config = AdvisorConfig {
            min_queries_for_analysis: 100,
            ..test_config()
        };
        let advisor = QueryAdvisor::new(config);
        advisor.record_query(&make_query("q", "q", 1, 1, 1, true));

        let result = advisor.analyze_workload();
        assert!(matches!(result, Err(AdvisorError::InsufficientData(100))));
    }

    #[test]
    fn test_n_plus_one_detection() {
        let advisor = QueryAdvisor::new(test_config());
        let now = Utc::now();

        // Record 6 queries with the same fingerprint at nearly the same time.
        for i in 0..6 {
            let q = QueryExecution {
                query: format!("GET user:{}", i),
                fingerprint: "GET user:$1".to_string(),
                execution_time_us: 200,
                rows_scanned: 1,
                rows_returned: 1,
                used_index: true,
                timestamp: now + chrono::Duration::microseconds(i * 10),
            };
            advisor.record_query(&q);
        }

        let antipatterns = advisor.detect_antipatterns();
        let n_plus_one = antipatterns
            .iter()
            .find(|a| a.pattern_type == AntiPatternType::NPlusOne);
        assert!(n_plus_one.is_some(), "should detect N+1 pattern");
        assert!(n_plus_one.unwrap().affected_queries >= 6);
    }

    #[test]
    fn test_full_scan_detection() {
        let advisor = QueryAdvisor::new(test_config());

        // Pattern with very high scan ratio (10000 scanned / 1 returned = 10000x).
        for _ in 0..3 {
            advisor.record_query(&make_query(
                "SELECT * FROM users WHERE name = $1",
                "SELECT * FROM users WHERE name = 'alice'",
                50000,
                10000,
                1,
                false,
            ));
        }

        let antipatterns = advisor.detect_antipatterns();
        let full_scan = antipatterns
            .iter()
            .find(|a| a.pattern_type == AntiPatternType::FullTableScan);
        assert!(full_scan.is_some(), "should detect full table scan");
    }

    #[test]
    fn test_unbounded_query_detection() {
        let advisor = QueryAdvisor::new(test_config());

        // Query without LIMIT returning many rows.
        for _ in 0..3 {
            advisor.record_query(&make_query(
                "SELECT * FROM orders",
                "SELECT * FROM orders",
                5000,
                1000,
                1000,
                true,
            ));
        }

        let antipatterns = advisor.detect_antipatterns();
        let unbounded = antipatterns
            .iter()
            .find(|a| a.pattern_type == AntiPatternType::UnboundedQuery);
        assert!(unbounded.is_some(), "should detect unbounded query");
    }

    #[test]
    fn test_no_unbounded_with_limit() {
        let advisor = QueryAdvisor::new(test_config());

        for _ in 0..3 {
            advisor.record_query(&make_query(
                "SELECT * FROM orders LIMIT 10",
                "SELECT * FROM orders LIMIT 10",
                500,
                10,
                10,
                true,
            ));
        }

        let antipatterns = advisor.detect_antipatterns();
        let unbounded = antipatterns
            .iter()
            .find(|a| a.pattern_type == AntiPatternType::UnboundedQuery);
        assert!(unbounded.is_none(), "should not flag query with LIMIT");
    }

    #[test]
    fn test_index_suggestion() {
        let advisor = QueryAdvisor::new(test_config());

        // High scan ratio pattern without index usage.
        for _ in 0..10 {
            advisor.record_query(&make_query(
                "SELECT * FROM events WHERE type = $1",
                "SELECT * FROM events WHERE type = 'click'",
                20000,
                50000,
                5,
                false,
            ));
        }

        let suggestions = advisor.get_index_suggestions();
        assert!(!suggestions.is_empty(), "should suggest at least one index");
        assert!(suggestions[0].estimated_speedup > 1.0);
        assert!(suggestions[0].confidence > 0.0);
    }

    #[test]
    fn test_improvement_estimation() {
        let advisor = QueryAdvisor::new(test_config());
        for _ in 0..5 {
            advisor.record_query(&make_query("q", "q", 10000, 100, 10, false));
        }

        let rec = Recommendation {
            id: "test".to_string(),
            rec_type: RecommendationType::CreateIndex,
            title: "test".to_string(),
            description: "test".to_string(),
            impact: Impact {
                latency_reduction_percent: 50.0,
                scan_reduction_percent: 80.0,
                memory_cost_bytes: 1024,
                priority: Priority::High,
            },
            confidence: 0.9,
            sql: None,
            auto_applicable: false,
        };

        let estimate = advisor.estimate_improvement(&rec);
        assert!(estimate.before_latency_us > 0.0);
        assert!(estimate.after_latency_us < estimate.before_latency_us);
        assert!((estimate.improvement_percent - 50.0).abs() < 0.1);
        assert_eq!(estimate.confidence, 0.9);
    }

    #[test]
    fn test_antipattern_listing() {
        let advisor = QueryAdvisor::new(test_config());
        let now = Utc::now();

        // N+1 pattern
        for i in 0..6 {
            advisor.record_query(&QueryExecution {
                query: format!("GET user:{}", i),
                fingerprint: "GET user:$1".to_string(),
                execution_time_us: 100,
                rows_scanned: 1,
                rows_returned: 1,
                used_index: true,
                timestamp: now + chrono::Duration::microseconds(i * 5),
            });
        }

        // Full scan pattern
        for _ in 0..3 {
            advisor.record_query(&make_query(
                "SCAN all", "SCAN all", 50000, 100000, 10, false,
            ));
        }

        let antipatterns = advisor.detect_antipatterns();
        let types: Vec<_> = antipatterns.iter().map(|a| &a.pattern_type).collect();
        assert!(types.contains(&&AntiPatternType::NPlusOne));
        assert!(types.contains(&&AntiPatternType::FullTableScan));
    }

    #[test]
    fn test_stats() {
        let advisor = QueryAdvisor::new(test_config());

        for _ in 0..5 {
            advisor.record_query(&make_query(
                "heavy_scan",
                "heavy_scan",
                10000,
                50000,
                5,
                false,
            ));
        }

        let stats = advisor.get_stats();
        assert_eq!(stats.queries_analyzed, 5);
        assert!(stats.indices_suggested > 0);
        assert!(stats.antipatterns_detected > 0);
    }

    #[test]
    fn test_default_config() {
        let config = AdvisorConfig::default();
        assert_eq!(config.min_queries_for_analysis, 100);
        assert_eq!(config.slow_query_threshold_ms, 100);
        assert_eq!(config.n_plus_one_threshold, 5);
        assert_eq!(config.recommendation_limit, 20);
        assert!(!config.auto_apply);
        assert!((config.confidence_threshold - 0.7).abs() < f64::EPSILON);
    }

    #[test]
    fn test_recommendations_respect_confidence_threshold() {
        let config = AdvisorConfig {
            confidence_threshold: 0.99,
            min_queries_for_analysis: 1,
            ..test_config()
        };
        let advisor = QueryAdvisor::new(config);

        for _ in 0..5 {
            advisor.record_query(&make_query("low_conf", "low_conf", 500, 200, 2, false));
        }

        // With a very high threshold most recommendations should be filtered out.
        let recs = advisor.get_recommendations();
        for r in &recs {
            assert!(r.confidence >= 0.99);
        }
    }
}
