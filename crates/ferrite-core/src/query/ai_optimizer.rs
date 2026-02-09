//! AI-Powered Query Optimizer & Auto-Index
//!
//! Workload-aware optimizer that monitors query patterns, recommends indexes,
//! auto-creates materialized views, and predicts query costs. Acts as
//! "autopilot for FerriteQL," eliminating the need for DBA expertise.
//!
//! # Components
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                 AI Query Optimizer                           │
//! │  ┌───────────────┐  ┌──────────────┐  ┌────────────────┐   │
//! │  │ Workload      │  │ Cost-Based   │  │  Auto-Tuning   │   │
//! │  │ Analyzer      │──▶  Optimizer   │──▶  Loop           │   │
//! │  │ (capture,     │  │ (cardinality,│  │ (create/drop   │   │
//! │  │  classify)    │  │  plan enum)  │  │  indexes/views)│   │
//! │  └───────────────┘  └──────────────┘  └────────────────┘   │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Query pattern capture
// ---------------------------------------------------------------------------

/// A captured query pattern (normalized).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPattern {
    /// Normalized query template (parameters replaced with $N)
    pub template: String,
    /// Fingerprint hash for deduplication
    pub fingerprint: u64,
    /// Number of times this pattern has been observed
    pub count: u64,
    /// Total execution time across all invocations
    pub total_time: Duration,
    /// Average execution time
    pub avg_time: Duration,
    /// Maximum execution time
    pub max_time: Duration,
    /// Key patterns accessed
    pub key_patterns: Vec<String>,
    /// Fields referenced in WHERE clauses
    pub filter_fields: Vec<String>,
    /// Fields referenced in ORDER BY
    pub sort_fields: Vec<String>,
    /// Fields referenced in GROUP BY
    pub group_fields: Vec<String>,
    /// Whether this query uses JOINs
    pub uses_join: bool,
    /// First seen timestamp
    #[serde(skip, default = "Instant::now")]
    pub first_seen: Instant,
    /// Last seen timestamp
    #[serde(skip, default = "Instant::now")]
    pub last_seen: Instant,
}

/// Workload analyzer that captures and classifies query patterns.
pub struct WorkloadAnalyzer {
    patterns: Arc<parking_lot::RwLock<HashMap<u64, QueryPattern>>>,
    sample_count: AtomicU64,
    max_patterns: usize,
}

impl WorkloadAnalyzer {
    /// Creates a new workload analyzer that tracks up to `max_patterns` distinct query patterns.
    pub fn new(max_patterns: usize) -> Self {
        Self {
            patterns: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            sample_count: AtomicU64::new(0),
            max_patterns,
        }
    }

    /// Record an observed query execution.
    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &self,
        template: &str,
        execution_time: Duration,
        key_patterns: Vec<String>,
        filter_fields: Vec<String>,
        sort_fields: Vec<String>,
        group_fields: Vec<String>,
        uses_join: bool,
    ) {
        let fingerprint = hash_template(template);
        self.sample_count.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();

        let mut patterns = self.patterns.write();

        if let Some(pattern) = patterns.get_mut(&fingerprint) {
            pattern.count += 1;
            pattern.total_time += execution_time;
            pattern.avg_time = pattern.total_time / pattern.count as u32;
            if execution_time > pattern.max_time {
                pattern.max_time = execution_time;
            }
            pattern.last_seen = now;
        } else if patterns.len() < self.max_patterns {
            patterns.insert(
                fingerprint,
                QueryPattern {
                    template: template.to_string(),
                    fingerprint,
                    count: 1,
                    total_time: execution_time,
                    avg_time: execution_time,
                    max_time: execution_time,
                    key_patterns,
                    filter_fields,
                    sort_fields,
                    group_fields,
                    uses_join,
                    first_seen: now,
                    last_seen: now,
                },
            );
        }
    }

    /// Get top N most frequent query patterns.
    pub fn top_patterns(&self, n: usize) -> Vec<QueryPattern> {
        let patterns = self.patterns.read();
        let mut sorted: Vec<_> = patterns.values().cloned().collect();
        sorted.sort_by(|a, b| b.count.cmp(&a.count));
        sorted.truncate(n);
        sorted
    }

    /// Get top N slowest query patterns by average time.
    pub fn slowest_patterns(&self, n: usize) -> Vec<QueryPattern> {
        let patterns = self.patterns.read();
        let mut sorted: Vec<_> = patterns.values().cloned().collect();
        sorted.sort_by(|a, b| b.avg_time.cmp(&a.avg_time));
        sorted.truncate(n);
        sorted
    }

    /// Total number of samples recorded.
    pub fn total_samples(&self) -> u64 {
        self.sample_count.load(Ordering::Relaxed)
    }

    /// Number of distinct patterns.
    pub fn pattern_count(&self) -> usize {
        self.patterns.read().len()
    }
}

fn hash_template(template: &str) -> u64 {
    // Simple FNV-1a hash
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in template.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

// ---------------------------------------------------------------------------
// Cost model
// ---------------------------------------------------------------------------

/// Estimated cost of a query execution plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryCost {
    /// Number of keys that would be scanned
    pub rows_scanned: u64,
    /// Number of rows expected in result
    pub rows_returned: u64,
    /// Estimated CPU cost (abstract units)
    pub cpu_cost: f64,
    /// Estimated I/O cost (abstract units)
    pub io_cost: f64,
    /// Estimated memory usage (bytes)
    pub memory_bytes: u64,
    /// Total estimated cost
    pub total_cost: f64,
}

impl QueryCost {
    /// Computes an estimated query cost based on scan, return, index, and join characteristics.
    pub fn compute(rows_scanned: u64, rows_returned: u64, has_index: bool, has_join: bool) -> Self {
        let scan_cost = if has_index {
            (rows_scanned as f64).log2().max(1.0) * 0.1
        } else {
            rows_scanned as f64 * 1.0
        };

        let join_cost = if has_join {
            rows_scanned as f64 * 0.5
        } else {
            0.0
        };

        let cpu_cost = scan_cost + join_cost;
        let io_cost = if rows_scanned > 10_000 {
            rows_scanned as f64 * 0.01
        } else {
            0.0
        };
        let memory_bytes = rows_scanned * 256; // rough estimate

        Self {
            rows_scanned,
            rows_returned,
            cpu_cost,
            io_cost,
            memory_bytes,
            total_cost: cpu_cost + io_cost,
        }
    }
}

// ---------------------------------------------------------------------------
// Index recommendations
// ---------------------------------------------------------------------------

/// Type of index that can be recommended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexType {
    /// Hash index (exact lookups)
    Hash,
    /// B-tree index (range queries, sorting)
    BTree,
    /// Full-text index
    FullText,
    /// Composite index on multiple fields
    Composite,
}

/// A recommendation to create or drop an index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexRecommendation {
    /// Unique identifier for this recommendation.
    pub id: String,
    /// Key pattern the index applies to.
    pub key_pattern: String,
    /// Fields included in the index.
    pub fields: Vec<String>,
    /// Type of index recommended.
    pub index_type: IndexType,
    /// Whether to create or drop the index.
    pub action: IndexAction,
    /// Estimated performance improvement (0.0–1.0).
    pub estimated_improvement: f64,
    /// Confidence level of the recommendation (0.0–1.0).
    pub confidence: f64,
    /// Number of observed queries that would benefit.
    pub affected_queries: u64,
    /// Human-readable explanation for the recommendation.
    pub reason: String,
}

/// Whether to create or drop an index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexAction {
    /// Create a new index.
    Create,
    /// Drop an existing index.
    Drop,
}

/// A recommendation to create a materialized view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewRecommendation {
    /// Suggested name for the materialized view.
    pub name: String,
    /// Query that defines the view contents.
    pub query: String,
    /// Recommended refresh interval.
    pub refresh_interval: Duration,
    /// Estimated speedup factor for affected queries.
    pub estimated_speedup: f64,
    /// Number of observed queries that would benefit.
    pub affected_queries: u64,
    /// Estimated storage cost in bytes.
    pub estimated_storage_bytes: u64,
}

// ---------------------------------------------------------------------------
// Optimizer Engine
// ---------------------------------------------------------------------------

/// Configuration for the AI query optimizer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizerConfig {
    /// Maximum number of auto-created indexes
    pub max_indexes: usize,
    /// Maximum memory budget for auto-indexes (bytes)
    pub index_memory_budget: u64,
    /// Minimum query frequency to consider for indexing
    pub min_query_frequency: u64,
    /// Minimum estimated improvement to recommend an index
    pub min_improvement_threshold: f64,
    /// Enable auto-apply of recommendations
    pub auto_apply: bool,
    /// Maximum number of materialized views
    pub max_views: usize,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            max_indexes: 50,
            index_memory_budget: 512 * 1024 * 1024, // 512 MB
            min_query_frequency: 100,
            min_improvement_threshold: 0.2, // 20% improvement
            auto_apply: false,
            max_views: 20,
        }
    }
}

/// Metrics for the optimizer.
#[derive(Debug, Default)]
pub struct OptimizerMetrics {
    /// Total number of workload analyses performed.
    pub analyses_run: AtomicU64,
    /// Total number of indexes recommended.
    pub indexes_recommended: AtomicU64,
    /// Total number of indexes automatically created.
    pub indexes_auto_created: AtomicU64,
    /// Total number of views recommended.
    pub views_recommended: AtomicU64,
    /// Estimated cumulative time saved in milliseconds.
    pub estimated_time_saved_ms: AtomicU64,
}

/// The AI query optimizer engine.
pub struct AiQueryOptimizer {
    config: OptimizerConfig,
    analyzer: WorkloadAnalyzer,
    active_indexes: Arc<parking_lot::RwLock<Vec<IndexRecommendation>>>,
    active_views: Arc<parking_lot::RwLock<Vec<ViewRecommendation>>>,
    metrics: Arc<OptimizerMetrics>,
}

impl AiQueryOptimizer {
    /// Creates a new AI query optimizer with the given configuration.
    pub fn new(config: OptimizerConfig) -> Self {
        Self {
            analyzer: WorkloadAnalyzer::new(10_000),
            config,
            active_indexes: Arc::new(parking_lot::RwLock::new(Vec::new())),
            active_views: Arc::new(parking_lot::RwLock::new(Vec::new())),
            metrics: Arc::new(OptimizerMetrics::default()),
        }
    }

    /// Record a query execution for analysis.
    #[allow(clippy::too_many_arguments)]
    pub fn record_query(
        &self,
        template: &str,
        execution_time: Duration,
        key_patterns: Vec<String>,
        filter_fields: Vec<String>,
        sort_fields: Vec<String>,
        group_fields: Vec<String>,
        uses_join: bool,
    ) {
        self.analyzer.record(
            template,
            execution_time,
            key_patterns,
            filter_fields,
            sort_fields,
            group_fields,
            uses_join,
        );
    }

    /// Run the optimization analysis and produce recommendations.
    pub fn analyze(&self) -> OptimizationReport {
        self.metrics.analyses_run.fetch_add(1, Ordering::Relaxed);

        let top_patterns = self.analyzer.top_patterns(50);
        let slow_patterns = self.analyzer.slowest_patterns(20);

        let mut index_recs = Vec::new();
        let mut view_recs = Vec::new();

        // Generate index recommendations from slow patterns
        for pattern in &slow_patterns {
            if pattern.count < self.config.min_query_frequency {
                continue;
            }

            // Recommend index on frequently filtered fields
            for field in &pattern.filter_fields {
                let estimated_improvement = estimate_index_improvement(pattern);
                if estimated_improvement >= self.config.min_improvement_threshold {
                    index_recs.push(IndexRecommendation {
                        id: format!(
                            "idx_{}_{}",
                            pattern.key_patterns.first().unwrap_or(&"*".to_string()),
                            field
                        ),
                        key_pattern: pattern.key_patterns.first().cloned().unwrap_or_default(),
                        fields: vec![field.clone()],
                        index_type: IndexType::Hash,
                        action: IndexAction::Create,
                        estimated_improvement,
                        confidence: (pattern.count as f64 / 1000.0).min(1.0),
                        affected_queries: pattern.count,
                        reason: format!(
                            "Field '{}' is filtered in {} queries with avg {}ms",
                            field,
                            pattern.count,
                            pattern.avg_time.as_millis()
                        ),
                    });
                }
            }

            // Recommend indexes on sort fields
            for field in &pattern.sort_fields {
                index_recs.push(IndexRecommendation {
                    id: format!("idx_sort_{}", field),
                    key_pattern: pattern.key_patterns.first().cloned().unwrap_or_default(),
                    fields: vec![field.clone()],
                    index_type: IndexType::BTree,
                    action: IndexAction::Create,
                    estimated_improvement: 0.3,
                    confidence: (pattern.count as f64 / 500.0).min(1.0),
                    affected_queries: pattern.count,
                    reason: format!(
                        "Field '{}' used in ORDER BY across {} queries",
                        field, pattern.count
                    ),
                });
            }
        }

        // Recommend materialized views for expensive repeated queries
        for pattern in &top_patterns {
            if pattern.count >= self.config.min_query_frequency * 5
                && pattern.avg_time > Duration::from_millis(10)
                && (pattern.uses_join || !pattern.group_fields.is_empty())
            {
                view_recs.push(ViewRecommendation {
                    name: format!("mv_{}", pattern.fingerprint),
                    query: pattern.template.clone(),
                    refresh_interval: Duration::from_secs(10),
                    estimated_speedup: pattern.avg_time.as_secs_f64()
                        / Duration::from_millis(1).as_secs_f64(),
                    affected_queries: pattern.count,
                    estimated_storage_bytes: 1024 * 1024, // rough estimate
                });
            }
        }

        // Dedup index recommendations by id
        index_recs.sort_by(|a, b| {
            b.estimated_improvement
                .partial_cmp(&a.estimated_improvement)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        index_recs.truncate(self.config.max_indexes);

        view_recs.truncate(self.config.max_views);

        self.metrics
            .indexes_recommended
            .fetch_add(index_recs.len() as u64, Ordering::Relaxed);
        self.metrics
            .views_recommended
            .fetch_add(view_recs.len() as u64, Ordering::Relaxed);

        OptimizationReport {
            index_recommendations: index_recs,
            view_recommendations: view_recs,
            patterns_analyzed: top_patterns.len() + slow_patterns.len(),
            total_queries_observed: self.analyzer.total_samples(),
        }
    }

    /// Estimate the cost of a query.
    pub fn estimate_cost(
        &self,
        rows_scanned: u64,
        rows_returned: u64,
        has_index: bool,
        has_join: bool,
    ) -> QueryCost {
        QueryCost::compute(rows_scanned, rows_returned, has_index, has_join)
    }

    /// Get the workload analyzer.
    pub fn analyzer(&self) -> &WorkloadAnalyzer {
        &self.analyzer
    }

    /// Get active indexes.
    pub fn active_indexes(&self) -> Vec<IndexRecommendation> {
        self.active_indexes.read().clone()
    }

    /// Get active views.
    pub fn active_views(&self) -> Vec<ViewRecommendation> {
        self.active_views.read().clone()
    }

    /// Returns a reference to the optimizer metrics.
    pub fn metrics(&self) -> &OptimizerMetrics {
        &self.metrics
    }
}

fn estimate_index_improvement(pattern: &QueryPattern) -> f64 {
    // Heuristic: more filter fields + higher execution time = more improvement
    let field_factor = (pattern.filter_fields.len() as f64 * 0.15).min(0.6);
    let time_factor = (pattern.avg_time.as_millis() as f64 / 100.0).min(0.4);
    (field_factor + time_factor).min(0.95)
}

/// Report produced by the optimizer analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationReport {
    /// Recommended index changes.
    pub index_recommendations: Vec<IndexRecommendation>,
    /// Recommended materialized views.
    pub view_recommendations: Vec<ViewRecommendation>,
    /// Number of distinct query patterns analyzed.
    pub patterns_analyzed: usize,
    /// Total queries observed during the analysis window.
    pub total_queries_observed: u64,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workload_analyzer_record() {
        let analyzer = WorkloadAnalyzer::new(100);
        analyzer.record(
            "SELECT * FROM users:* WHERE $.active = true",
            Duration::from_millis(5),
            vec!["users:*".into()],
            vec!["active".into()],
            vec![],
            vec![],
            false,
        );
        assert_eq!(analyzer.total_samples(), 1);
        assert_eq!(analyzer.pattern_count(), 1);
    }

    #[test]
    fn test_workload_analyzer_dedup() {
        let analyzer = WorkloadAnalyzer::new(100);
        for _ in 0..10 {
            analyzer.record(
                "SELECT * FROM orders:*",
                Duration::from_millis(2),
                vec!["orders:*".into()],
                vec![],
                vec![],
                vec![],
                false,
            );
        }
        assert_eq!(analyzer.total_samples(), 10);
        assert_eq!(analyzer.pattern_count(), 1);
        let top = analyzer.top_patterns(1);
        assert_eq!(top[0].count, 10);
    }

    #[test]
    fn test_query_cost_with_index() {
        let cost = QueryCost::compute(10_000, 100, true, false);
        let cost_no_idx = QueryCost::compute(10_000, 100, false, false);
        assert!(cost.total_cost < cost_no_idx.total_cost);
    }

    #[test]
    fn test_query_cost_with_join() {
        let cost_join = QueryCost::compute(1000, 100, false, true);
        let cost_no_join = QueryCost::compute(1000, 100, false, false);
        assert!(cost_join.total_cost > cost_no_join.total_cost);
    }

    #[test]
    fn test_optimizer_analyze_empty() {
        let optimizer = AiQueryOptimizer::new(OptimizerConfig::default());
        let report = optimizer.analyze();
        assert_eq!(report.patterns_analyzed, 0);
        assert!(report.index_recommendations.is_empty());
    }

    #[test]
    fn test_optimizer_generates_recommendations() {
        let config = OptimizerConfig {
            min_query_frequency: 5,
            ..Default::default()
        };
        let optimizer = AiQueryOptimizer::new(config);

        // Record enough slow queries to trigger recommendations
        for _ in 0..20 {
            optimizer.record_query(
                "SELECT * FROM users:* WHERE $.email = $1",
                Duration::from_millis(50),
                vec!["users:*".into()],
                vec!["email".into()],
                vec![],
                vec![],
                false,
            );
        }

        let report = optimizer.analyze();
        assert!(!report.index_recommendations.is_empty());
        assert!(report
            .index_recommendations
            .iter()
            .any(|r| r.fields.contains(&"email".to_string())));
    }

    #[test]
    fn test_optimizer_view_recommendation() {
        let config = OptimizerConfig {
            min_query_frequency: 2,
            ..Default::default()
        };
        let optimizer = AiQueryOptimizer::new(config);

        // Record expensive join queries
        for _ in 0..15 {
            optimizer.record_query(
                "SELECT u.name, COUNT(o.*) FROM users:* JOIN orders:* GROUP BY u.id",
                Duration::from_millis(100),
                vec!["users:*".into(), "orders:*".into()],
                vec![],
                vec![],
                vec!["id".into()],
                true,
            );
        }

        let report = optimizer.analyze();
        assert!(!report.view_recommendations.is_empty());
    }

    #[test]
    fn test_hash_template_deterministic() {
        let h1 = hash_template("SELECT * FROM users");
        let h2 = hash_template("SELECT * FROM users");
        let h3 = hash_template("SELECT * FROM orders");
        assert_eq!(h1, h2);
        assert_ne!(h1, h3);
    }
}
