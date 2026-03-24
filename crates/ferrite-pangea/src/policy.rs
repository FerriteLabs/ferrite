//! Tier promotion/demotion policy — decides when to move data between DRAM and CXL tiers.

use std::collections::HashMap;
use std::sync::RwLock;

/// Access frequency tracker per key.
#[derive(Debug, Clone)]
pub struct AccessStats {
    pub reads: u64,
    pub last_access_ms: u64,
    pub tier: Tier,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Tier {
    Dram, // Fast, limited capacity
    Cxl,  // Slower, larger capacity
    Disk, // Slowest, unlimited
}

/// Policy configuration.
#[derive(Debug, Clone)]
pub struct TierPolicy {
    /// Promote from CXL to DRAM if access count exceeds this in the window.
    pub promote_threshold: u64,
    /// Demote from DRAM to CXL if no access for this duration (ms).
    pub demote_after_ms: u64,
    /// DRAM pressure threshold (0.0–1.0): start demoting when DRAM usage exceeds this fraction.
    pub dram_pressure_threshold: f64,
    /// Maximum number of promotions per evaluation cycle.
    pub max_promotions_per_cycle: usize,
    /// Maximum number of demotions per evaluation cycle.
    pub max_demotions_per_cycle: usize,
}

impl Default for TierPolicy {
    fn default() -> Self {
        Self {
            promote_threshold: 10,
            demote_after_ms: 60_000, // 1 minute
            dram_pressure_threshold: 0.8,
            max_promotions_per_cycle: 100,
            max_demotions_per_cycle: 100,
        }
    }
}

/// Result of a policy evaluation cycle.
#[derive(Debug, Default)]
pub struct MigrationPlan {
    /// Keys to promote CXL→DRAM.
    pub promotions: Vec<String>,
    /// Keys to demote DRAM→CXL.
    pub demotions: Vec<String>,
}

/// Policy engine that tracks access patterns and recommends migrations.
pub struct PolicyEngine {
    stats: RwLock<HashMap<String, AccessStats>>,
    policy: TierPolicy,
}

impl PolicyEngine {
    pub fn new(policy: TierPolicy) -> Self {
        Self {
            stats: RwLock::new(HashMap::new()),
            policy,
        }
    }

    /// Record an access to a key.
    pub fn record_access(&self, key: &str, now_ms: u64, tier: Tier) {
        let mut stats = self.stats.write().expect("stats lock poisoned");
        let entry = stats.entry(key.to_string()).or_insert_with(|| AccessStats {
            reads: 0,
            last_access_ms: now_ms,
            tier,
        });
        entry.reads += 1;
        entry.last_access_ms = now_ms;
        entry.tier = tier;
    }

    /// Evaluate the policy and produce a migration plan.
    pub fn evaluate(&self, now_ms: u64, dram_usage_fraction: f64) -> MigrationPlan {
        let stats = self.stats.read().expect("stats lock poisoned");
        let mut plan = MigrationPlan::default();

        for (key, s) in stats.iter() {
            match s.tier {
                Tier::Cxl => {
                    // Promote hot CXL keys to DRAM
                    if s.reads >= self.policy.promote_threshold
                        && plan.promotions.len() < self.policy.max_promotions_per_cycle
                    {
                        plan.promotions.push(key.clone());
                    }
                }
                Tier::Dram => {
                    // Demote cold DRAM keys under pressure
                    let idle_ms = now_ms.saturating_sub(s.last_access_ms);
                    if (idle_ms >= self.policy.demote_after_ms
                        || dram_usage_fraction >= self.policy.dram_pressure_threshold)
                        && plan.demotions.len() < self.policy.max_demotions_per_cycle
                    {
                        plan.demotions.push(key.clone());
                    }
                }
                Tier::Disk => {} // Not managed by this policy
            }
        }

        plan
    }

    /// Reset access counters (after a migration cycle).
    pub fn reset_counters(&self, keys: &[String]) {
        let mut stats = self.stats.write().expect("stats lock poisoned");
        for key in keys {
            if let Some(entry) = stats.get_mut(key) {
                entry.reads = 0;
            }
        }
    }

    /// Get current policy.
    pub fn policy(&self) -> &TierPolicy {
        &self.policy
    }

    /// Update policy.
    pub fn set_policy(&mut self, policy: TierPolicy) {
        self.policy = policy;
    }

    /// Get stats for a key.
    pub fn stats(&self, key: &str) -> Option<AccessStats> {
        self.stats
            .read()
            .expect("stats lock poisoned")
            .get(key)
            .cloned()
    }

    /// Number of tracked keys.
    pub fn tracked_count(&self) -> usize {
        self.stats.read().expect("stats lock poisoned").len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hot_key_exceeding_threshold_gets_promoted() {
        let engine = PolicyEngine::new(TierPolicy {
            promote_threshold: 3,
            ..TierPolicy::default()
        });
        // Simulate 5 reads on a CXL key
        for i in 0..5 {
            engine.record_access("hot_key", 1000 + i, Tier::Cxl);
        }
        let plan = engine.evaluate(2000, 0.5);
        assert!(plan.promotions.contains(&"hot_key".to_string()));
        assert!(plan.demotions.is_empty());
    }

    #[test]
    fn cold_key_exceeding_idle_time_gets_demoted() {
        let engine = PolicyEngine::new(TierPolicy {
            demote_after_ms: 5000,
            dram_pressure_threshold: 0.99, // high so pressure doesn't trigger
            ..TierPolicy::default()
        });
        engine.record_access("cold_key", 1000, Tier::Dram);
        // Evaluate well past demote_after_ms
        let plan = engine.evaluate(10_000, 0.1);
        assert!(plan.demotions.contains(&"cold_key".to_string()));
        assert!(plan.promotions.is_empty());
    }

    #[test]
    fn dram_pressure_triggers_early_demotion() {
        let engine = PolicyEngine::new(TierPolicy {
            demote_after_ms: 60_000,
            dram_pressure_threshold: 0.7,
            ..TierPolicy::default()
        });
        // Key was accessed very recently — would NOT be demoted by idle time
        engine.record_access("pressured_key", 9990, Tier::Dram);
        let plan = engine.evaluate(10_000, 0.85); // pressure above 0.7
        assert!(plan.demotions.contains(&"pressured_key".to_string()));
    }

    #[test]
    fn max_promotions_per_cycle_respected() {
        let engine = PolicyEngine::new(TierPolicy {
            promote_threshold: 1,
            max_promotions_per_cycle: 2,
            ..TierPolicy::default()
        });
        for i in 0..5 {
            engine.record_access(&format!("k{i}"), 1000, Tier::Cxl);
        }
        let plan = engine.evaluate(2000, 0.5);
        assert_eq!(plan.promotions.len(), 2);
    }

    #[test]
    fn max_demotions_per_cycle_respected() {
        let engine = PolicyEngine::new(TierPolicy {
            demote_after_ms: 100,
            dram_pressure_threshold: 0.99,
            max_demotions_per_cycle: 2,
            ..TierPolicy::default()
        });
        for i in 0..5 {
            engine.record_access(&format!("d{i}"), 1000, Tier::Dram);
        }
        let plan = engine.evaluate(5000, 0.1);
        assert_eq!(plan.demotions.len(), 2);
    }

    #[test]
    fn reset_counters_clears_access_stats() {
        let engine = PolicyEngine::new(TierPolicy {
            promote_threshold: 5,
            ..TierPolicy::default()
        });
        for i in 0..10 {
            engine.record_access("reset_me", 1000 + i, Tier::Cxl);
        }
        assert_eq!(engine.stats("reset_me").unwrap().reads, 10);

        engine.reset_counters(&["reset_me".to_string()]);
        assert_eq!(engine.stats("reset_me").unwrap().reads, 0);

        // After reset, key should NOT be promoted (reads < threshold)
        let plan = engine.evaluate(2000, 0.5);
        assert!(!plan.promotions.contains(&"reset_me".to_string()));
    }

    #[test]
    fn disk_tier_not_managed() {
        let engine = PolicyEngine::new(TierPolicy::default());
        engine.record_access("disk_key", 1000, Tier::Disk);
        let plan = engine.evaluate(999_999, 0.99);
        assert!(plan.promotions.is_empty());
        assert!(plan.demotions.is_empty());
    }

    #[test]
    fn stats_returns_none_for_unknown_key() {
        let engine = PolicyEngine::new(TierPolicy::default());
        assert!(engine.stats("unknown").is_none());
    }

    #[test]
    fn tracked_count_reflects_keys() {
        let engine = PolicyEngine::new(TierPolicy::default());
        assert_eq!(engine.tracked_count(), 0);
        engine.record_access("a", 100, Tier::Dram);
        engine.record_access("b", 200, Tier::Cxl);
        assert_eq!(engine.tracked_count(), 2);
    }
}
