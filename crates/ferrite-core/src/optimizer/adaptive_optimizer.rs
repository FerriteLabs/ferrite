#![forbid(unsafe_code)]
//! Adaptive optimizer — analyzes workload snapshots and produces optimization plans.

use super::profiler::WorkloadSnapshot;
use super::recommendation::{Action, OptimizationPlan, Recommendation, RecommendationPriority};

/// A heuristic rule that inspects a workload snapshot and optionally emits a recommendation.
pub struct Rule {
    /// Human-readable rule name.
    pub name: &'static str,
    /// Short description.
    pub description: &'static str,
    /// Condition check + recommendation generator.
    pub check: fn(&WorkloadSnapshot) -> Option<Recommendation>,
}

/// The adaptive optimizer: applies a set of heuristic rules to a workload snapshot.
pub struct AdaptiveOptimizer {
    rules: Vec<Rule>,
}

impl Default for AdaptiveOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl AdaptiveOptimizer {
    /// Create a new optimizer with the default rule set.
    pub fn new() -> Self {
        Self {
            rules: default_rules(),
        }
    }

    /// Analyze a snapshot and produce an optimization plan.
    pub fn analyze(&self, snapshot: &WorkloadSnapshot) -> OptimizationPlan {
        let mut plan = OptimizationPlan::new();

        for rule in &self.rules {
            if let Some(rec) = (rule.check)(snapshot) {
                plan.add(rec);
            }
        }

        if plan.overall_estimated_impact > 50.0 {
            plan.warnings.push(
                "Large cumulative impact — consider applying recommendations incrementally."
                    .to_string(),
            );
        }

        plan
    }

    /// List all registered rules.
    pub fn rules(&self) -> Vec<(&'static str, &'static str)> {
        self.rules.iter().map(|r| (r.name, r.description)).collect()
    }

    /// Number of rules.
    pub fn rule_count(&self) -> usize {
        self.rules.len()
    }
}

// ---------------------------------------------------------------------------
// Default heuristic rules
// ---------------------------------------------------------------------------

fn default_rules() -> Vec<Rule> {
    vec![
        Rule {
            name: "hot_key_promotion",
            description: "Promote keys accessed >10x/min to hot tier",
            check: |snap| {
                let hot_count = snap.hot_keys.iter().filter(|(_, c)| *c > 10).count();
                if hot_count > 0 {
                    Some(Recommendation {
                        id: "hot-key-promote".to_string(),
                        rule_name: "hot_key_promotion".to_string(),
                        action: Action::PromoteHotKeys {
                            key_count: hot_count,
                        },
                        priority: RecommendationPriority::High,
                        confidence: 0.85,
                        estimated_impact: (hot_count as f64).min(30.0),
                        description: format!(
                            "Promote {} hot keys to faster tier for lower latency",
                            hot_count
                        ),
                    })
                } else {
                    None
                }
            },
        },
        Rule {
            name: "cold_key_demotion",
            description: "Demote keys accessed <1x/min to cold tier",
            check: |snap| {
                let cold_count = snap.hot_keys.iter().filter(|(_, c)| *c <= 1).count();
                if cold_count > 5 {
                    Some(Recommendation {
                        id: "cold-key-demote".to_string(),
                        rule_name: "cold_key_demotion".to_string(),
                        action: Action::DemoteColdKeys {
                            key_count: cold_count,
                        },
                        priority: RecommendationPriority::Medium,
                        confidence: 0.75,
                        estimated_impact: 5.0,
                        description: format!(
                            "Demote {} cold keys to reduce memory pressure",
                            cold_count
                        ),
                    })
                } else {
                    None
                }
            },
        },
        Rule {
            name: "high_memory_eviction",
            description: "Increase eviction batch when memory >80%",
            check: |snap| {
                if snap.memory_usage_fraction > 0.80 {
                    Some(Recommendation {
                        id: "high-mem-evict".to_string(),
                        rule_name: "high_memory_eviction".to_string(),
                        action: Action::ModifyEvictionPolicy { batch_size: 256 },
                        priority: RecommendationPriority::Critical,
                        confidence: 0.95,
                        estimated_impact: 15.0,
                        description: format!(
                            "Memory at {:.0}% — increase eviction batch size",
                            snap.memory_usage_fraction * 100.0
                        ),
                    })
                } else {
                    None
                }
            },
        },
        Rule {
            name: "enable_compression",
            description: "Enable compression when avg value size >1KB",
            check: |snap| {
                if snap.avg_value_size > 1024.0 {
                    Some(Recommendation {
                        id: "enable-compression".to_string(),
                        rule_name: "enable_compression".to_string(),
                        action: Action::EnableCompression {
                            min_value_size: 1024,
                        },
                        priority: RecommendationPriority::Medium,
                        confidence: 0.80,
                        estimated_impact: 10.0,
                        description: format!(
                            "Avg value size {:.0}B — compression can reduce memory ~40%",
                            snap.avg_value_size
                        ),
                    })
                } else {
                    None
                }
            },
        },
        Rule {
            name: "disable_compression_small_values",
            description: "Disable compression when avg value size <256B",
            check: |snap| {
                if snap.avg_value_size > 0.0 && snap.avg_value_size < 256.0 {
                    Some(Recommendation {
                        id: "disable-compression".to_string(),
                        rule_name: "disable_compression_small_values".to_string(),
                        action: Action::DisableCompression,
                        priority: RecommendationPriority::Low,
                        confidence: 0.70,
                        estimated_impact: 3.0,
                        description: "Small values — compression overhead exceeds savings"
                            .to_string(),
                    })
                } else {
                    None
                }
            },
        },
        Rule {
            name: "read_heavy_tier_boundary",
            description: "Lower hot tier threshold for read-heavy workloads (>80% reads)",
            check: |snap| {
                if snap.read_write_ratio > 0.80 && snap.ops_per_sec > 100.0 {
                    Some(Recommendation {
                        id: "read-heavy-tier".to_string(),
                        rule_name: "read_heavy_tier_boundary".to_string(),
                        action: Action::AdjustTierBoundary {
                            tier: "hot".to_string(),
                            threshold: 5.0,
                        },
                        priority: RecommendationPriority::High,
                        confidence: 0.82,
                        estimated_impact: 12.0,
                        description: format!(
                            "Read-heavy workload ({:.0}% reads) — expand hot tier",
                            snap.read_write_ratio * 100.0
                        ),
                    })
                } else {
                    None
                }
            },
        },
        Rule {
            name: "write_heavy_buffer",
            description: "Increase write buffer for write-heavy workloads (>60% writes)",
            check: |snap| {
                if snap.read_write_ratio < 0.40 && snap.ops_per_sec > 100.0 {
                    Some(Recommendation {
                        id: "write-heavy-buffer".to_string(),
                        rule_name: "write_heavy_buffer".to_string(),
                        action: Action::AdjustWriteBuffer {
                            size_bytes: 64 * 1024 * 1024,
                        },
                        priority: RecommendationPriority::High,
                        confidence: 0.78,
                        estimated_impact: 15.0,
                        description: "Write-heavy workload — increase write buffer to 64MB"
                            .to_string(),
                    })
                } else {
                    None
                }
            },
        },
        Rule {
            name: "memory_budget_low_usage",
            description: "Reduce memory budget when usage consistently <40%",
            check: |snap| {
                if snap.memory_usage_fraction > 0.0 && snap.memory_usage_fraction < 0.40 {
                    Some(Recommendation {
                        id: "reduce-memory-budget".to_string(),
                        rule_name: "memory_budget_low_usage".to_string(),
                        action: Action::AdjustMemoryBudget {
                            budget_bytes: 512 * 1024 * 1024,
                        },
                        priority: RecommendationPriority::Low,
                        confidence: 0.60,
                        estimated_impact: 2.0,
                        description: format!(
                            "Memory at {:.0}% — consider reducing budget to save resources",
                            snap.memory_usage_fraction * 100.0
                        ),
                    })
                } else {
                    None
                }
            },
        },
        Rule {
            name: "high_ops_connection_pool",
            description: "Increase connection pool when ops/sec >10K",
            check: |snap| {
                if snap.ops_per_sec > 10_000.0 {
                    Some(Recommendation {
                        id: "increase-conn-pool".to_string(),
                        rule_name: "high_ops_connection_pool".to_string(),
                        action: Action::AdjustConnectionPool { size: 256 },
                        priority: RecommendationPriority::Medium,
                        confidence: 0.72,
                        estimated_impact: 8.0,
                        description: format!(
                            "High throughput ({:.0} ops/sec) — enlarge connection pool",
                            snap.ops_per_sec
                        ),
                    })
                } else {
                    None
                }
            },
        },
        Rule {
            name: "prefetch_sequential",
            description: "Enable prefetch when sequential access pattern detected",
            check: |snap| {
                // Heuristic: many unique keys + high ops = likely sequential scan.
                if snap.unique_keys_accessed > 500 && snap.ops_per_sec > 1000.0 {
                    Some(Recommendation {
                        id: "enable-prefetch".to_string(),
                        rule_name: "prefetch_sequential".to_string(),
                        action: Action::EnablePrefetch { depth: 16 },
                        priority: RecommendationPriority::Medium,
                        confidence: 0.65,
                        estimated_impact: 10.0,
                        description: "Sequential access pattern detected — enable prefetching"
                            .to_string(),
                    })
                } else {
                    None
                }
            },
        },
        Rule {
            name: "shard_imbalance",
            description: "Rebalance shards when hot key concentration is high",
            check: |snap| {
                if snap.hot_keys.len() >= 2 {
                    let top = snap.hot_keys[0].1;
                    let second = snap.hot_keys[1].1;
                    if top > second * 5 && top > 50 {
                        return Some(Recommendation {
                            id: "rebalance-shards".to_string(),
                            rule_name: "shard_imbalance".to_string(),
                            action: Action::RebalanceShards { target_shards: 16 },
                            priority: RecommendationPriority::High,
                            confidence: 0.70,
                            estimated_impact: 12.0,
                            description: "Hot key skew detected — rebalance shards".to_string(),
                        });
                    }
                }
                None
            },
        },
        Rule {
            name: "critical_memory_pressure",
            description: "Emergency memory reduction when usage >95%",
            check: |snap| {
                if snap.memory_usage_fraction > 0.95 {
                    Some(Recommendation {
                        id: "emergency-memory".to_string(),
                        rule_name: "critical_memory_pressure".to_string(),
                        action: Action::AdjustMemoryBudget {
                            budget_bytes: 0, // signal to free as much as possible
                        },
                        priority: RecommendationPriority::Critical,
                        confidence: 0.99,
                        estimated_impact: 25.0,
                        description: format!(
                            "CRITICAL: Memory at {:.0}% — immediate eviction needed",
                            snap.memory_usage_fraction * 100.0
                        ),
                    })
                } else {
                    None
                }
            },
        },
        Rule {
            name: "low_throughput_pool_reduction",
            description: "Reduce connection pool when ops/sec <100",
            check: |snap| {
                if snap.ops_per_sec > 0.0 && snap.ops_per_sec < 100.0 {
                    Some(Recommendation {
                        id: "reduce-conn-pool".to_string(),
                        rule_name: "low_throughput_pool_reduction".to_string(),
                        action: Action::AdjustConnectionPool { size: 16 },
                        priority: RecommendationPriority::Low,
                        confidence: 0.60,
                        estimated_impact: 2.0,
                        description: "Low throughput — reduce connection pool to save resources"
                            .to_string(),
                    })
                } else {
                    None
                }
            },
        },
        Rule {
            name: "index_frequent_pattern",
            description: "Create index for frequently accessed key patterns",
            check: |snap| {
                // Detect common prefixes in hot keys.
                if snap.hot_keys.len() >= 5 {
                    let prefixes: Vec<&str> = snap
                        .hot_keys
                        .iter()
                        .filter_map(|(k, _)| k.split(':').next())
                        .collect();
                    if !prefixes.is_empty() {
                        let first = prefixes[0];
                        let count = prefixes.iter().filter(|&&p| p == first).count();
                        if count >= 3 {
                            return Some(Recommendation {
                                id: format!("create-index-{}", first),
                                rule_name: "index_frequent_pattern".to_string(),
                                action: Action::CreateIndex {
                                    name: format!("idx_{}", first),
                                    pattern: format!("{}:*", first),
                                },
                                priority: RecommendationPriority::Medium,
                                confidence: 0.68,
                                estimated_impact: 8.0,
                                description: format!(
                                    "Pattern '{}:*' accessed frequently — create index",
                                    first
                                ),
                            });
                        }
                    }
                }
                None
            },
        },
        Rule {
            name: "tier_boundary_balanced",
            description: "Adjust tier boundary for balanced workloads (40-60% reads)",
            check: |snap| {
                if snap.read_write_ratio >= 0.40
                    && snap.read_write_ratio <= 0.60
                    && snap.ops_per_sec > 500.0
                {
                    Some(Recommendation {
                        id: "balanced-tier".to_string(),
                        rule_name: "tier_boundary_balanced".to_string(),
                        action: Action::AdjustTierBoundary {
                            tier: "warm".to_string(),
                            threshold: 15.0,
                        },
                        priority: RecommendationPriority::Low,
                        confidence: 0.55,
                        estimated_impact: 4.0,
                        description: "Balanced workload — optimize warm tier boundary".to_string(),
                    })
                } else {
                    None
                }
            },
        },
        Rule {
            name: "large_value_write_buffer",
            description: "Increase write buffer when avg value >4KB",
            check: |snap| {
                if snap.avg_value_size > 4096.0 && snap.read_write_ratio < 0.60 {
                    Some(Recommendation {
                        id: "large-value-buffer".to_string(),
                        rule_name: "large_value_write_buffer".to_string(),
                        action: Action::AdjustWriteBuffer {
                            size_bytes: 128 * 1024 * 1024,
                        },
                        priority: RecommendationPriority::Medium,
                        confidence: 0.74,
                        estimated_impact: 10.0,
                        description: format!(
                            "Large values ({:.0}B avg) with writes — increase write buffer to 128MB",
                            snap.avg_value_size
                        ),
                    })
                } else {
                    None
                }
            },
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_snapshot(overrides: impl FnOnce(&mut WorkloadSnapshot)) -> WorkloadSnapshot {
        let mut snap = WorkloadSnapshot::default();
        overrides(&mut snap);
        snap
    }

    #[test]
    fn test_optimizer_new_has_rules() {
        let opt = AdaptiveOptimizer::new();
        assert!(opt.rule_count() >= 15);
    }

    #[test]
    fn test_empty_snapshot_produces_no_critical() {
        let opt = AdaptiveOptimizer::new();
        let snap = WorkloadSnapshot::default();
        let plan = opt.analyze(&snap);
        assert!(plan
            .recommendations
            .iter()
            .all(|r| r.priority != RecommendationPriority::Critical));
    }

    #[test]
    fn test_high_memory_triggers_eviction() {
        let opt = AdaptiveOptimizer::new();
        let snap = make_snapshot(|s| {
            s.memory_usage_fraction = 0.85;
        });
        let plan = opt.analyze(&snap);
        assert!(plan
            .recommendations
            .iter()
            .any(|r| r.rule_name == "high_memory_eviction"));
    }

    #[test]
    fn test_compression_recommendation() {
        let opt = AdaptiveOptimizer::new();
        let snap = make_snapshot(|s| {
            s.avg_value_size = 2048.0;
        });
        let plan = opt.analyze(&snap);
        assert!(plan
            .recommendations
            .iter()
            .any(|r| r.rule_name == "enable_compression"));
    }

    #[test]
    fn test_hot_key_promotion() {
        let opt = AdaptiveOptimizer::new();
        let snap = make_snapshot(|s| {
            s.hot_keys = vec![("key1".to_string(), 100), ("key2".to_string(), 50)];
        });
        let plan = opt.analyze(&snap);
        assert!(plan
            .recommendations
            .iter()
            .any(|r| r.rule_name == "hot_key_promotion"));
    }

    #[test]
    fn test_rules_listing() {
        let opt = AdaptiveOptimizer::new();
        let rules = opt.rules();
        assert!(rules.len() >= 15);
        assert!(rules.iter().any(|(name, _)| *name == "hot_key_promotion"));
    }

    #[test]
    fn test_critical_memory_pressure() {
        let opt = AdaptiveOptimizer::new();
        let snap = make_snapshot(|s| {
            s.memory_usage_fraction = 0.97;
        });
        let plan = opt.analyze(&snap);
        assert!(plan
            .recommendations
            .iter()
            .any(|r| r.rule_name == "critical_memory_pressure"));
    }

    #[test]
    fn test_plan_warning_on_large_impact() {
        let opt = AdaptiveOptimizer::new();
        let snap = make_snapshot(|s| {
            s.memory_usage_fraction = 0.97;
            s.avg_value_size = 2048.0;
            s.hot_keys = vec![("key1".to_string(), 200), ("key2".to_string(), 20)];
        });
        let plan = opt.analyze(&snap);
        assert!(!plan.warnings.is_empty());
    }
}
