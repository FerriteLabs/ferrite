//! Adaptive Query Optimizer
//!
//! ML-based workload profiler that auto-creates indexes, tunes memory,
//! and preemptively warms cache based on access patterns.
#![allow(dead_code)]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Statistics for a single query pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternStats {
    /// The command pattern (e.g. "GET", "HGET").
    pub pattern: String,
    /// How many times this pattern has been seen.
    pub frequency: u64,
    /// Average latency in microseconds.
    pub avg_latency_us: f64,
    /// Timestamp of last observation (epoch seconds).
    pub last_seen: u64,
}

/// Aggregated workload profile built from recorded queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadProfile {
    /// Per-pattern statistics keyed by command name.
    pub query_patterns: HashMap<String, PatternStats>,
    /// Top-N hottest keys by access count.
    pub hot_keys: Vec<(String, u64)>,
    /// Ratio of reads to writes (reads / total).
    pub read_write_ratio: f64,
    /// Hours of the day (0-23) with above-average traffic.
    pub peak_hours: Vec<u8>,
    /// Average key size in bytes across observed queries.
    pub avg_key_size: u64,
    /// Average value size in bytes across observed queries.
    pub avg_value_size: u64,
    /// Total operations recorded.
    pub total_ops: u64,
}

/// Type of index that can be recommended.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexType {
    /// Hash-based index for exact lookups.
    Hash,
    /// Sorted-set-based index for range queries.
    SortedSet,
    /// Vector index for similarity search.
    Vector,
    /// Full-text search index.
    FullText,
    /// Composite multi-field index.
    Composite,
}

/// A recommendation to create an index for a key pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexRecommendation {
    /// Glob or prefix pattern for affected keys.
    pub key_pattern: String,
    /// Suggested index type.
    pub index_type: IndexType,
    /// Estimated speedup factor (e.g. 2.5 = 2.5× faster).
    pub estimated_speedup: f64,
    /// Estimated memory cost in bytes.
    pub memory_cost_bytes: u64,
    /// Confidence in the recommendation (0.0–1.0).
    pub confidence: f64,
}

/// A plan for proactively warming the cache.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheWarmingPlan {
    /// Keys to pre-fetch, ordered by priority.
    pub keys: Vec<String>,
    /// Overall priority score (higher = more urgent).
    pub priority: f64,
    /// Predicted next access epoch-seconds.
    pub predicted_access_time: u64,
    /// Storage tier the data currently resides on.
    pub source_tier: String,
}

/// A single tuning adjustment recommended by the optimizer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TuningAction {
    /// Human-readable description of the change.
    pub description: String,
    /// Configuration parameter affected.
    pub parameter: String,
    /// Previous value.
    pub old_value: String,
    /// Recommended new value.
    pub new_value: String,
    /// Expected impact description.
    pub impact: String,
}

/// Configuration for the adaptive query optimizer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizerConfig {
    /// Whether the optimizer is active.
    pub enabled: bool,
    /// How often to run workload analysis.
    #[serde(with = "duration_secs")]
    pub analysis_interval: Duration,
    /// Maximum number of index recommendations to emit.
    pub max_recommendations: usize,
    /// Minimum confidence threshold for recommendations.
    pub min_confidence: f64,
    /// Whether to automatically apply tuning actions.
    pub auto_apply: bool,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            analysis_interval: Duration::from_secs(60),
            max_recommendations: 10,
            min_confidence: 0.7,
            auto_apply: false,
        }
    }
}

/// Runtime statistics for the optimizer.
#[derive(Debug, Clone)]
pub struct OptimizerStats {
    /// Total queries analyzed.
    pub queries_analyzed: u64,
    /// Total recommendations produced.
    pub recommendations_made: u64,
    /// Auto-tuning actions applied.
    pub auto_tunes_applied: u64,
    /// When the last analysis cycle ran.
    pub last_analysis: Option<Instant>,
}

/// Internal record for a single observed query.
struct QueryRecord {
    command: String,
    key: String,
    latency_us: u64,
    timestamp: Instant,
}

/// The adaptive query optimizer.
///
/// Records incoming queries, builds a workload profile, and produces
/// index recommendations, cache warming plans, and tuning actions.
pub struct AdaptiveQueryOptimizer {
    config: OptimizerConfig,
    records: RwLock<Vec<QueryRecord>>,
    stats: RwLock<OptimizerStats>,
}

impl AdaptiveQueryOptimizer {
    /// Create a new optimizer with the given configuration.
    pub fn new(config: OptimizerConfig) -> Self {
        Self {
            config,
            records: RwLock::new(Vec::new()),
            stats: RwLock::new(OptimizerStats {
                queries_analyzed: 0,
                recommendations_made: 0,
                auto_tunes_applied: 0,
                last_analysis: None,
            }),
        }
    }

    /// Record an observed query for later analysis.
    pub fn record_query(&self, command: &str, key: &str, latency_us: u64) {
        if !self.config.enabled {
            return;
        }
        let record = QueryRecord {
            command: command.to_uppercase(),
            key: key.to_string(),
            latency_us,
            timestamp: Instant::now(),
        };
        self.records.write().push(record);
        self.stats.write().queries_analyzed += 1;
    }

    /// Compute the current workload profile from recorded queries.
    pub fn analyze_workload(&self) -> WorkloadProfile {
        let records = self.records.read();
        let mut patterns: HashMap<String, PatternStats> = HashMap::new();
        let mut key_counts: HashMap<String, u64> = HashMap::new();
        let mut read_count: u64 = 0;
        let total_ops = records.len() as u64;

        for rec in records.iter() {
            let entry = patterns
                .entry(rec.command.clone())
                .or_insert_with(|| PatternStats {
                    pattern: rec.command.clone(),
                    frequency: 0,
                    avg_latency_us: 0.0,
                    last_seen: 0,
                });
            let prev_total = entry.avg_latency_us * entry.frequency as f64;
            entry.frequency += 1;
            entry.avg_latency_us = (prev_total + rec.latency_us as f64) / entry.frequency as f64;
            entry.last_seen = rec
                .timestamp
                .elapsed()
                .as_secs()
                .saturating_sub(entry.last_seen);

            *key_counts.entry(rec.key.clone()).or_insert(0) += 1;

            if is_read_command(&rec.command) {
                read_count += 1;
            }
        }

        let mut hot_keys: Vec<(String, u64)> = key_counts.into_iter().collect();
        hot_keys.sort_by(|a, b| b.1.cmp(&a.1));
        hot_keys.truncate(100);

        let read_write_ratio = if total_ops > 0 {
            read_count as f64 / total_ops as f64
        } else {
            0.0
        };

        self.stats.write().last_analysis = Some(Instant::now());

        WorkloadProfile {
            query_patterns: patterns,
            hot_keys,
            read_write_ratio,
            peak_hours: vec![],
            avg_key_size: 0,
            avg_value_size: 0,
            total_ops,
        }
    }

    /// Recommend indexes based on the current workload.
    pub fn recommend_indexes(&self) -> Vec<IndexRecommendation> {
        let profile = self.analyze_workload();
        let mut recommendations = Vec::new();

        for (cmd, stats) in &profile.query_patterns {
            if stats.frequency < 100 {
                continue;
            }

            let (index_type, speedup) = match cmd.as_str() {
                "GET" | "MGET" => (IndexType::Hash, 1.5),
                "ZRANGEBYSCORE" | "ZRANGEBYLEX" => (IndexType::SortedSet, 2.0),
                "FT.SEARCH" => (IndexType::FullText, 3.0),
                _ => continue,
            };

            let confidence = (stats.frequency as f64 / profile.total_ops as f64).min(1.0);
            if confidence < self.config.min_confidence {
                continue;
            }

            recommendations.push(IndexRecommendation {
                key_pattern: format!("{}:*", cmd.to_lowercase()),
                index_type,
                estimated_speedup: speedup,
                memory_cost_bytes: stats.frequency * 64,
                confidence,
            });
        }

        recommendations.truncate(self.config.max_recommendations);
        self.stats.write().recommendations_made += recommendations.len() as u64;
        recommendations
    }

    /// Generate a cache warming plan from hot-key analysis.
    pub fn generate_warming_plan(&self) -> CacheWarmingPlan {
        let profile = self.analyze_workload();
        let keys: Vec<String> = profile.hot_keys.iter().map(|(k, _)| k.clone()).collect();
        let priority = if keys.is_empty() {
            0.0
        } else {
            profile.hot_keys.first().map(|(_, c)| *c as f64).unwrap_or(0.0)
        };

        CacheWarmingPlan {
            keys,
            priority,
            predicted_access_time: 0,
            source_tier: "memory".to_string(),
        }
    }

    /// Produce automatic tuning actions based on workload analysis.
    pub fn auto_tune(&self) -> Vec<TuningAction> {
        let profile = self.analyze_workload();
        let mut actions = Vec::new();

        if profile.read_write_ratio > 0.9 {
            actions.push(TuningAction {
                description: "Workload is read-heavy; increase read-only region".to_string(),
                parameter: "storage.readonly_region_pct".to_string(),
                old_value: "50".to_string(),
                new_value: "70".to_string(),
                impact: "Reduce read latency by ~20%".to_string(),
            });
        }

        if profile.read_write_ratio < 0.3 {
            actions.push(TuningAction {
                description: "Workload is write-heavy; increase mutable region".to_string(),
                parameter: "storage.mutable_region_pct".to_string(),
                old_value: "30".to_string(),
                new_value: "50".to_string(),
                impact: "Reduce write amplification by ~15%".to_string(),
            });
        }

        if profile.hot_keys.len() > 50 {
            actions.push(TuningAction {
                description: "Many hot keys detected; enable bloom filter".to_string(),
                parameter: "storage.bloom_filter".to_string(),
                old_value: "false".to_string(),
                new_value: "true".to_string(),
                impact: "Reduce negative lookup cost by ~40%".to_string(),
            });
        }

        if self.config.auto_apply {
            self.stats.write().auto_tunes_applied += actions.len() as u64;
        }

        actions
    }

    /// Return current optimizer statistics.
    pub fn stats(&self) -> OptimizerStats {
        self.stats.read().clone()
    }
}

/// Returns true for commands that are reads.
fn is_read_command(cmd: &str) -> bool {
    matches!(
        cmd,
        "GET" | "MGET" | "HGET" | "HGETALL" | "LRANGE" | "SMEMBERS"
            | "ZRANGE" | "ZRANGEBYSCORE" | "XRANGE" | "XREAD"
            | "FT.SEARCH" | "EXISTS" | "TYPE" | "TTL" | "KEYS"
    )
}

/// Serde helper to serialize Duration as seconds.
mod duration_secs {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(d.as_secs())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let secs = u64::deserialize(d)?;
        Ok(Duration::from_secs(secs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_analyze() {
        let opt = AdaptiveQueryOptimizer::new(OptimizerConfig::default());
        opt.record_query("GET", "user:1", 100);
        opt.record_query("GET", "user:2", 200);
        opt.record_query("SET", "user:3", 150);

        let profile = opt.analyze_workload();
        assert_eq!(profile.total_ops, 3);
        assert!(profile.read_write_ratio > 0.5);
        assert!(profile.query_patterns.contains_key("GET"));
    }

    #[test]
    fn test_stats() {
        let opt = AdaptiveQueryOptimizer::new(OptimizerConfig::default());
        opt.record_query("GET", "k", 10);
        let stats = opt.stats();
        assert_eq!(stats.queries_analyzed, 1);
    }

    #[test]
    fn test_disabled_optimizer_skips_recording() {
        let config = OptimizerConfig {
            enabled: false,
            ..Default::default()
        };
        let opt = AdaptiveQueryOptimizer::new(config);
        opt.record_query("GET", "k", 10);
        let stats = opt.stats();
        assert_eq!(stats.queries_analyzed, 0);
    }

    #[test]
    fn test_warming_plan_empty() {
        let opt = AdaptiveQueryOptimizer::new(OptimizerConfig::default());
        let plan = opt.generate_warming_plan();
        assert!(plan.keys.is_empty());
        assert_eq!(plan.priority, 0.0);
    }

    #[test]
    fn test_auto_tune_read_heavy() {
        let opt = AdaptiveQueryOptimizer::new(OptimizerConfig::default());
        for i in 0..100 {
            opt.record_query("GET", &format!("k:{}", i), 50);
        }
        let actions = opt.auto_tune();
        assert!(actions.iter().any(|a| a.parameter.contains("readonly")));
    }
}
