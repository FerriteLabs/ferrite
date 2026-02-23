//! AI-Powered Recommendations Engine
//!
//! Generates performance recommendations based on query analysis.

use super::analyzer::{PatternType, QueryAnalysis};
use serde::{Deserialize, Serialize};

/// Recommendation engine for generating performance suggestions
pub struct RecommendationEngine {
    rules: Vec<Box<dyn RecommendationRule + Send + Sync>>,
}

impl RecommendationEngine {
    /// Create a new recommendation engine with default rules
    pub fn new() -> Self {
        let rules: Vec<Box<dyn RecommendationRule + Send + Sync>> = vec![
            Box::new(SlowQueryRule),
            Box::new(NPlusOneRule),
            Box::new(HotKeyRule),
            Box::new(LargeScanRule),
            Box::new(CommandOptimizationRule),
            Box::new(KeyPatternRule),
            Box::new(MemoryUsageRule),
        ];

        Self { rules }
    }

    /// Generate recommendations based on query analysis
    pub fn generate(&self, analysis: &QueryAnalysis) -> Vec<Recommendation> {
        let mut recommendations = Vec::new();

        for rule in &self.rules {
            if let Some(recs) = rule.evaluate(analysis) {
                recommendations.extend(recs);
            }
        }

        // Sort by severity (critical first)
        recommendations.sort_by(|a, b| {
            let severity_order = |s: &Severity| match s {
                Severity::Critical => 0,
                Severity::High => 1,
                Severity::Medium => 2,
                Severity::Low => 3,
                Severity::Info => 4,
            };
            severity_order(&a.severity).cmp(&severity_order(&b.severity))
        });

        // Deduplicate similar recommendations
        recommendations.dedup_by(|a, b| {
            a.recommendation_type == b.recommendation_type && a.target == b.target
        });

        recommendations
    }
}

impl Default for RecommendationEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// A performance recommendation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Recommendation {
    /// Recommendation type
    pub recommendation_type: RecommendationType,
    /// Severity level
    pub severity: Severity,
    /// Short title
    pub title: String,
    /// Detailed description
    pub description: String,
    /// Target (key pattern, command, etc.)
    pub target: Option<String>,
    /// Suggested action
    pub action: String,
    /// Expected impact
    pub impact: String,
    /// Related documentation URL
    pub docs_url: Option<String>,
}

/// Types of recommendations
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecommendationType {
    /// Add an index
    AddIndex,
    /// Use batch operations
    UseBatching,
    /// Optimize key pattern
    OptimizeKeyPattern,
    /// Change data structure
    ChangeDataStructure,
    /// Add caching
    AddCaching,
    /// Reduce scan operations
    ReduceScans,
    /// Optimize memory usage
    OptimizeMemory,
    /// Use pipelining
    UsePipelining,
    /// Split hot key
    SplitHotKey,
    /// General optimization
    GeneralOptimization,
}

/// Severity levels for recommendations
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Severity {
    /// Critical - immediate action required
    Critical,
    /// High priority
    High,
    /// Medium priority
    Medium,
    /// Low priority
    Low,
    /// Informational
    Info,
}

/// Trait for recommendation rules
trait RecommendationRule {
    fn evaluate(&self, analysis: &QueryAnalysis) -> Option<Vec<Recommendation>>;
}

/// Rule for slow query recommendations
struct SlowQueryRule;

impl RecommendationRule for SlowQueryRule {
    fn evaluate(&self, analysis: &QueryAnalysis) -> Option<Vec<Recommendation>> {
        if analysis.slow_query_count == 0 {
            return None;
        }

        let percentage =
            (analysis.slow_query_count as f64 / analysis.total_queries.max(1) as f64) * 100.0;

        if percentage > 10.0 {
            Some(vec![Recommendation {
                recommendation_type: RecommendationType::GeneralOptimization,
                severity: Severity::High,
                title: "High percentage of slow queries".to_string(),
                description: format!(
                    "{:.1}% of queries ({} out of {}) are slow (>10ms). This indicates systemic performance issues.",
                    percentage, analysis.slow_query_count, analysis.total_queries
                ),
                target: None,
                action: "Review slow queries and consider adding indexes, using batch operations, or optimizing key patterns".to_string(),
                impact: "Could improve overall latency by 50% or more".to_string(),
                docs_url: Some("/docs/performance/slow-queries".to_string()),
            }])
        } else if percentage > 5.0 {
            Some(vec![Recommendation {
                recommendation_type: RecommendationType::GeneralOptimization,
                severity: Severity::Medium,
                title: "Moderate slow query rate".to_string(),
                description: format!(
                    "{:.1}% of queries are slow. Consider investigating the slowest operations.",
                    percentage
                ),
                target: None,
                action: "Review the slow query log and optimize the most frequent slow operations"
                    .to_string(),
                impact: "Could improve P99 latency".to_string(),
                docs_url: Some("/docs/performance/slow-queries".to_string()),
            }])
        } else {
            None
        }
    }
}

/// Rule for N+1 query pattern
struct NPlusOneRule;

impl RecommendationRule for NPlusOneRule {
    fn evaluate(&self, analysis: &QueryAnalysis) -> Option<Vec<Recommendation>> {
        let n_plus_one_patterns: Vec<_> = analysis
            .detected_patterns
            .iter()
            .filter(|p| p.pattern_type == PatternType::NPlusOne)
            .collect();

        if n_plus_one_patterns.is_empty() {
            return None;
        }

        let mut recommendations = Vec::new();
        for pattern in n_plus_one_patterns {
            recommendations.push(Recommendation {
                recommendation_type: RecommendationType::UseBatching,
                severity: Severity::High,
                title: "N+1 query pattern detected".to_string(),
                description: pattern.description.clone(),
                target: pattern.affected_keys.first().cloned(),
                action:
                    "Use MGET for multiple key retrieval, or implement application-level batching"
                        .to_string(),
                impact: "Could reduce query count by 90% and improve throughput significantly"
                    .to_string(),
                docs_url: Some("/docs/commands/mget".to_string()),
            });
        }

        Some(recommendations)
    }
}

/// Rule for hot key recommendations
struct HotKeyRule;

impl RecommendationRule for HotKeyRule {
    fn evaluate(&self, analysis: &QueryAnalysis) -> Option<Vec<Recommendation>> {
        let hot_key_patterns: Vec<_> = analysis
            .detected_patterns
            .iter()
            .filter(|p| p.pattern_type == PatternType::HotKey)
            .collect();

        if hot_key_patterns.is_empty() {
            return None;
        }

        let mut recommendations = Vec::new();
        for pattern in hot_key_patterns {
            recommendations.push(Recommendation {
                recommendation_type: RecommendationType::SplitHotKey,
                severity: Severity::Medium,
                title: "Hot key detected".to_string(),
                description: pattern.description.clone(),
                target: pattern.affected_keys.first().cloned(),
                action: "Consider: 1) Local caching with short TTL, 2) Splitting the key across multiple shards, 3) Using read replicas".to_string(),
                impact: "Reduces single-key contention and improves scalability".to_string(),
                docs_url: Some("/docs/performance/hot-keys".to_string()),
            });
        }

        Some(recommendations)
    }
}

/// Rule for large scan recommendations
struct LargeScanRule;

impl RecommendationRule for LargeScanRule {
    fn evaluate(&self, analysis: &QueryAnalysis) -> Option<Vec<Recommendation>> {
        let scan_patterns: Vec<_> = analysis
            .detected_patterns
            .iter()
            .filter(|p| p.pattern_type == PatternType::LargeScan)
            .collect();

        if scan_patterns.is_empty() {
            return None;
        }

        Some(vec![Recommendation {
            recommendation_type: RecommendationType::ReduceScans,
            severity: Severity::High,
            title: "Excessive key scanning detected".to_string(),
            description: "Multiple SCAN or KEYS operations detected. These operations can be expensive on large datasets.".to_string(),
            target: None,
            action: "Use specific key patterns, secondary indexes, or maintain explicit key lists".to_string(),
            impact: "Reduces CPU usage and improves response times for scan-heavy workloads".to_string(),
            docs_url: Some("/docs/commands/scan".to_string()),
        }])
    }
}

/// Rule for command optimization
struct CommandOptimizationRule;

impl RecommendationRule for CommandOptimizationRule {
    fn evaluate(&self, analysis: &QueryAnalysis) -> Option<Vec<Recommendation>> {
        let mut recommendations = Vec::new();

        for cmd_stats in &analysis.command_breakdown {
            // Check for inefficient command usage
            if cmd_stats.command == "KEYS" && cmd_stats.count > 0 {
                recommendations.push(Recommendation {
                    recommendation_type: RecommendationType::ReduceScans,
                    severity: Severity::Critical,
                    title: "KEYS command usage detected".to_string(),
                    description: format!(
                        "KEYS command was used {} times. KEYS blocks the server and should never be used in production.",
                        cmd_stats.count
                    ),
                    target: Some("KEYS".to_string()),
                    action: "Replace KEYS with SCAN for iterating keys in production".to_string(),
                    impact: "Prevents server blocking and improves availability".to_string(),
                    docs_url: Some("/docs/commands/scan".to_string()),
                });
            }

            // Check for commands that could benefit from pipelining
            if (cmd_stats.command == "GET" || cmd_stats.command == "SET") && cmd_stats.count > 100 {
                let avg_latency = cmd_stats.avg_duration_us;
                if avg_latency < 500 {
                    // If individual operations are fast, pipelining would help
                    recommendations.push(Recommendation {
                        recommendation_type: RecommendationType::UsePipelining,
                        severity: Severity::Low,
                        title: format!("Consider pipelining for {} operations", cmd_stats.command),
                        description: format!(
                            "{} {} operations detected with low individual latency ({}μs avg). Pipelining could reduce round-trips.",
                            cmd_stats.count, cmd_stats.command, avg_latency
                        ),
                        target: Some(cmd_stats.command.clone()),
                        action: "Use pipelining or MULTI/EXEC to batch operations".to_string(),
                        impact: "Could reduce total execution time by reducing network round-trips".to_string(),
                        docs_url: Some("/docs/performance/pipelining".to_string()),
                    });
                }
            }
        }

        if recommendations.is_empty() {
            None
        } else {
            Some(recommendations)
        }
    }
}

/// Rule for key pattern optimization
struct KeyPatternRule;

impl RecommendationRule for KeyPatternRule {
    fn evaluate(&self, analysis: &QueryAnalysis) -> Option<Vec<Recommendation>> {
        let mut recommendations = Vec::new();

        for pattern in &analysis.key_pattern_breakdown {
            // Check for very long key patterns
            if pattern.pattern.len() > 100 {
                recommendations.push(Recommendation {
                    recommendation_type: RecommendationType::OptimizeKeyPattern,
                    severity: Severity::Low,
                    title: "Long key pattern detected".to_string(),
                    description: format!(
                        "Key pattern '{}...' is {} characters. Long keys consume more memory and bandwidth.",
                        &pattern.pattern[..50.min(pattern.pattern.len())],
                        pattern.pattern.len()
                    ),
                    target: Some(pattern.pattern.clone()),
                    action: "Consider using shorter key names or hash-based identifiers".to_string(),
                    impact: "Reduces memory usage and network bandwidth".to_string(),
                    docs_url: Some("/docs/best-practices/key-naming".to_string()),
                });
            }

            // Check for patterns that might benefit from different data structures
            if pattern.pattern.contains(":list:") && pattern.count > 1000 {
                recommendations.push(Recommendation {
                    recommendation_type: RecommendationType::ChangeDataStructure,
                    severity: Severity::Info,
                    title: "Potential list optimization".to_string(),
                    description: format!(
                        "Pattern '{}' accessed {} times. If accessing by index frequently, consider sorted sets.",
                        pattern.pattern, pattern.count
                    ),
                    target: Some(pattern.pattern.clone()),
                    action: "Evaluate if sorted sets (ZSET) would be more efficient for your access pattern".to_string(),
                    impact: "Could improve read performance for range and index-based access".to_string(),
                    docs_url: Some("/docs/data-types/sorted-sets".to_string()),
                });
            }
        }

        if recommendations.is_empty() {
            None
        } else {
            Some(recommendations)
        }
    }
}

/// Rule for memory usage optimization
struct MemoryUsageRule;

impl RecommendationRule for MemoryUsageRule {
    fn evaluate(&self, analysis: &QueryAnalysis) -> Option<Vec<Recommendation>> {
        // Check if there are patterns suggesting memory issues
        let memory_patterns: Vec<_> = analysis
            .detected_patterns
            .iter()
            .filter(|p| p.pattern_type == PatternType::MemoryPressure)
            .collect();

        if memory_patterns.is_empty() {
            return None;
        }

        Some(vec![Recommendation {
            recommendation_type: RecommendationType::OptimizeMemory,
            severity: Severity::High,
            title: "Memory pressure detected".to_string(),
            description: "Query patterns indicate potential memory pressure or inefficient memory usage.".to_string(),
            target: None,
            action: "Consider: 1) Enabling tiered storage, 2) Setting TTLs on keys, 3) Using more efficient data structures".to_string(),
            impact: "Prevents OOM conditions and improves overall system stability".to_string(),
            docs_url: Some("/docs/configuration/memory".to_string()),
        }])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::analyzer::{DetectedPattern, Severity as AnalyzerSeverity};

    #[test]
    fn test_recommendation_engine_creation() {
        let engine = RecommendationEngine::new();
        let analysis = QueryAnalysis::default();
        let recommendations = engine.generate(&analysis);
        assert!(recommendations.is_empty());
    }

    #[test]
    fn test_slow_query_recommendations() {
        let engine = RecommendationEngine::new();

        let analysis = QueryAnalysis {
            total_queries: 100,
            slow_query_count: 15, // 15% slow
            ..Default::default()
        };

        let recommendations = engine.generate(&analysis);
        assert!(!recommendations.is_empty());
        assert!(recommendations
            .iter()
            .any(|r| r.title.contains("slow queries")));
    }

    #[test]
    fn test_n_plus_one_recommendations() {
        let engine = RecommendationEngine::new();

        let analysis = QueryAnalysis {
            total_queries: 100,
            detected_patterns: vec![DetectedPattern {
                pattern_type: PatternType::NPlusOne,
                description: "N+1 detected".to_string(),
                severity: AnalyzerSeverity::Warning,
                affected_keys: vec!["user:*".to_string()],
                suggestion: "Use MGET".to_string(),
            }],
            ..Default::default()
        };

        let recommendations = engine.generate(&analysis);
        assert!(!recommendations.is_empty());
        assert!(recommendations
            .iter()
            .any(|r| r.recommendation_type == RecommendationType::UseBatching));
    }

    #[test]
    fn test_keys_command_recommendation() {
        let engine = RecommendationEngine::new();

        let analysis = QueryAnalysis {
            total_queries: 100,
            command_breakdown: vec![crate::observability::analyzer::CommandBreakdown {
                command: "KEYS".to_string(),
                count: 5,
                total_duration_us: 50000,
                avg_duration_us: 10000,
                max_duration_us: 20000,
                min_duration_us: 5000,
            }],
            ..Default::default()
        };

        let recommendations = engine.generate(&analysis);
        assert!(!recommendations.is_empty());
        assert!(recommendations
            .iter()
            .any(|r| r.severity == Severity::Critical));
    }
}
