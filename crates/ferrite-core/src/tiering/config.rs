//! Configuration for cost-aware tiering
//!
//! This module defines the configuration structures for the tiering engine,
//! including cost models for each tier and optimization policies.

use super::stats::{Priority, StorageTier};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Cost configuration for a single storage tier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierCost {
    /// Storage cost per GB per month
    pub storage_per_gb_month: f64,
    /// Cost per 1000 read operations
    pub read_per_1k: f64,
    /// Cost per 1000 write operations
    pub write_per_1k: f64,
    /// Network egress per GB (for cloud tiers)
    pub egress_per_gb: f64,
    /// Typical read latency in milliseconds
    pub read_latency_ms: f64,
    /// Typical write latency in milliseconds
    pub write_latency_ms: f64,
}

impl TierCost {
    /// Create a new tier cost configuration
    pub fn new(
        storage_per_gb_month: f64,
        read_per_1k: f64,
        write_per_1k: f64,
        egress_per_gb: f64,
        read_latency_ms: f64,
        write_latency_ms: f64,
    ) -> Self {
        Self {
            storage_per_gb_month,
            read_per_1k,
            write_per_1k,
            egress_per_gb,
            read_latency_ms,
            write_latency_ms,
        }
    }
}

/// Cost configuration for all tiers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierCostConfig {
    /// Memory tier costs
    pub memory: TierCost,
    /// Mmap tier costs
    pub mmap: TierCost,
    /// SSD tier costs
    pub ssd: TierCost,
    /// Cloud tier costs
    pub cloud: TierCost,
    /// Archive tier costs
    pub archive: TierCost,
}

impl Default for TierCostConfig {
    fn default() -> Self {
        Self {
            memory: TierCost {
                storage_per_gb_month: 10.0,
                read_per_1k: 0.0,
                write_per_1k: 0.0,
                egress_per_gb: 0.0,
                read_latency_ms: 0.1,
                write_latency_ms: 0.1,
            },
            mmap: TierCost {
                storage_per_gb_month: 5.0,
                read_per_1k: 0.0,
                write_per_1k: 0.0,
                egress_per_gb: 0.0,
                read_latency_ms: 0.5,
                write_latency_ms: 1.0,
            },
            ssd: TierCost {
                storage_per_gb_month: 0.15,
                read_per_1k: 0.0,
                write_per_1k: 0.0,
                egress_per_gb: 0.0,
                read_latency_ms: 1.0,
                write_latency_ms: 2.0,
            },
            cloud: TierCost {
                storage_per_gb_month: 0.023, // S3 Standard
                read_per_1k: 0.0004,         // S3 GET
                write_per_1k: 0.005,         // S3 PUT
                egress_per_gb: 0.09,
                read_latency_ms: 50.0,
                write_latency_ms: 100.0,
            },
            archive: TierCost {
                storage_per_gb_month: 0.004, // S3 Glacier
                read_per_1k: 0.01,
                write_per_1k: 0.05,
                egress_per_gb: 0.09,
                read_latency_ms: 3600000.0, // Hours
                write_latency_ms: 1000.0,
            },
        }
    }
}

impl TierCostConfig {
    /// Get cost configuration for a specific tier
    pub fn cost_for_tier(&self, tier: StorageTier) -> &TierCost {
        match tier {
            StorageTier::Memory => &self.memory,
            StorageTier::Mmap => &self.mmap,
            StorageTier::Ssd => &self.ssd,
            StorageTier::Cloud => &self.cloud,
            StorageTier::Archive => &self.archive,
        }
    }

    /// Update cost for a specific tier
    pub fn set_tier_cost(&mut self, tier: StorageTier, cost: TierCost) {
        match tier {
            StorageTier::Memory => self.memory = cost,
            StorageTier::Mmap => self.mmap = cost,
            StorageTier::Ssd => self.ssd = cost,
            StorageTier::Cloud => self.cloud = cost,
            StorageTier::Archive => self.archive = cost,
        }
    }

    /// Load AWS pricing for a given region
    pub fn load_aws_pricing(&mut self, region: &str) {
        // AWS S3 pricing varies by region, but these are typical US values
        match region {
            "us-east-1" | "us-east-2" | "us-west-1" | "us-west-2" => {
                self.cloud = TierCost {
                    storage_per_gb_month: 0.023,
                    read_per_1k: 0.0004,
                    write_per_1k: 0.005,
                    egress_per_gb: 0.09,
                    read_latency_ms: 50.0,
                    write_latency_ms: 100.0,
                };
                self.archive = TierCost {
                    storage_per_gb_month: 0.004,
                    read_per_1k: 0.01,
                    write_per_1k: 0.05,
                    egress_per_gb: 0.09,
                    read_latency_ms: 3600000.0,
                    write_latency_ms: 1000.0,
                };
            }
            "eu-west-1" | "eu-central-1" => {
                self.cloud = TierCost {
                    storage_per_gb_month: 0.024,
                    read_per_1k: 0.00043,
                    write_per_1k: 0.0054,
                    egress_per_gb: 0.09,
                    read_latency_ms: 50.0,
                    write_latency_ms: 100.0,
                };
                self.archive = TierCost {
                    storage_per_gb_month: 0.0045,
                    read_per_1k: 0.011,
                    write_per_1k: 0.055,
                    egress_per_gb: 0.09,
                    read_latency_ms: 3600000.0,
                    write_latency_ms: 1000.0,
                };
            }
            _ => {
                // Use default US pricing for unknown regions
            }
        }
    }

    /// Load GCP pricing for a given region
    pub fn load_gcp_pricing(&mut self, region: &str) {
        match region {
            "us-central1" | "us-east1" | "us-west1" => {
                self.cloud = TierCost {
                    storage_per_gb_month: 0.020,
                    read_per_1k: 0.0004,
                    write_per_1k: 0.005,
                    egress_per_gb: 0.12,
                    read_latency_ms: 50.0,
                    write_latency_ms: 100.0,
                };
                self.archive = TierCost {
                    storage_per_gb_month: 0.0025,
                    read_per_1k: 0.01,
                    write_per_1k: 0.05,
                    egress_per_gb: 0.12,
                    read_latency_ms: 3600000.0,
                    write_latency_ms: 1000.0,
                };
            }
            _ => {}
        }
    }

    /// Load Azure pricing for a given region
    pub fn load_azure_pricing(&mut self, region: &str) {
        match region {
            "eastus" | "eastus2" | "westus" | "westus2" => {
                self.cloud = TierCost {
                    storage_per_gb_month: 0.0184,
                    read_per_1k: 0.0004,
                    write_per_1k: 0.005,
                    egress_per_gb: 0.087,
                    read_latency_ms: 50.0,
                    write_latency_ms: 100.0,
                };
                self.archive = TierCost {
                    storage_per_gb_month: 0.00099,
                    read_per_1k: 0.01,
                    write_per_1k: 0.10,
                    egress_per_gb: 0.087,
                    read_latency_ms: 3600000.0,
                    write_latency_ms: 1000.0,
                };
            }
            _ => {}
        }
    }
}

/// Optimization target for the tiering engine
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum OptimizationTarget {
    /// Minimize total cost
    Cost,
    /// Minimize latency
    Latency,
    /// Balance cost and latency
    #[default]
    Balanced,
}

impl OptimizationTarget {
    /// Parse from string
    pub fn parse_str(s: &str) -> Option<OptimizationTarget> {
        match s.to_lowercase().as_str() {
            "cost" => Some(OptimizationTarget::Cost),
            "latency" => Some(OptimizationTarget::Latency),
            "balanced" => Some(OptimizationTarget::Balanced),
            _ => None,
        }
    }

    /// Get name
    pub fn name(&self) -> &'static str {
        match self {
            OptimizationTarget::Cost => "cost",
            OptimizationTarget::Latency => "latency",
            OptimizationTarget::Balanced => "balanced",
        }
    }
}

/// Placement constraints for tiering decisions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementConstraints {
    /// Maximum acceptable read latency in milliseconds
    pub max_latency_ms: f64,
    /// Maximum tier allowed (don't go colder than this)
    pub max_tier: StorageTier,
    /// Minimum tier allowed (don't go hotter than this)
    pub min_tier: StorageTier,
}

impl Default for PlacementConstraints {
    fn default() -> Self {
        Self {
            max_latency_ms: f64::MAX,
            max_tier: StorageTier::Archive,
            min_tier: StorageTier::Memory,
        }
    }
}

impl PlacementConstraints {
    /// Create with max latency constraint
    pub fn with_max_latency(max_latency_ms: f64) -> Self {
        Self {
            max_latency_ms,
            ..Default::default()
        }
    }

    /// Check if a tier satisfies constraints
    pub fn allows_tier(&self, tier: StorageTier, tier_latency_ms: f64) -> bool {
        tier_latency_ms <= self.max_latency_ms && tier >= self.min_tier && tier <= self.max_tier
    }
}

/// Pattern-based policy for key prefixes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternPolicy {
    /// Glob pattern to match keys
    pub pattern: String,
    /// Priority override
    pub priority: Option<Priority>,
    /// Maximum tier constraint
    pub max_tier: Option<StorageTier>,
    /// Minimum tier constraint
    pub min_tier: Option<StorageTier>,
}

impl PatternPolicy {
    /// Create a new pattern policy
    pub fn new(pattern: &str) -> Self {
        Self {
            pattern: pattern.to_string(),
            priority: None,
            max_tier: None,
            min_tier: None,
        }
    }

    /// Set priority for this pattern
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Set max tier for this pattern
    pub fn with_max_tier(mut self, tier: StorageTier) -> Self {
        self.max_tier = Some(tier);
        self
    }

    /// Set min tier for this pattern
    pub fn with_min_tier(mut self, tier: StorageTier) -> Self {
        self.min_tier = Some(tier);
        self
    }

    /// Check if pattern matches a key
    pub fn matches(&self, key: &str) -> bool {
        // Simple glob matching supporting * and ?
        glob_match(&self.pattern, key)
    }

    /// Get placement constraints for this pattern
    pub fn constraints(&self) -> PlacementConstraints {
        PlacementConstraints {
            max_latency_ms: f64::MAX,
            max_tier: self.max_tier.unwrap_or(StorageTier::Archive),
            min_tier: self.min_tier.unwrap_or(StorageTier::Memory),
        }
    }
}

/// Simple glob pattern matching
fn glob_match(pattern: &str, text: &str) -> bool {
    let mut p = pattern.chars().peekable();
    let mut t = text.chars().peekable();

    fn match_impl(
        pattern: &mut std::iter::Peekable<std::str::Chars<'_>>,
        text: &mut std::iter::Peekable<std::str::Chars<'_>>,
    ) -> bool {
        while let Some(&pc) = pattern.peek() {
            match pc {
                '*' => {
                    pattern.next();
                    // Skip consecutive *s
                    while pattern.peek() == Some(&'*') {
                        pattern.next();
                    }
                    // If * is at end, match everything
                    if pattern.peek().is_none() {
                        return true;
                    }
                    // Try matching the rest from each position
                    while text.peek().is_some() {
                        let mut p_clone = pattern.clone();
                        let mut t_clone = text.clone();
                        if match_impl(&mut p_clone, &mut t_clone) {
                            return true;
                        }
                        text.next();
                    }
                    return false;
                }
                '?' => {
                    pattern.next();
                    if text.next().is_none() {
                        return false;
                    }
                }
                c => {
                    pattern.next();
                    if text.next() != Some(c) {
                        return false;
                    }
                }
            }
        }
        text.peek().is_none()
    }

    match_impl(&mut p, &mut t)
}

/// Main tiering configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringConfig {
    /// Enable cost-aware tiering
    pub enabled: bool,
    /// Optimization target
    pub optimize_for: OptimizationTarget,
    /// Global max latency constraint
    pub max_latency_ms: f64,
    /// Memory budget in bytes
    pub memory_budget: u64,
    /// Cost configuration per tier
    pub costs: TierCostConfig,
    /// How often to recalculate optimal tiers
    pub calculation_interval: Duration,
    /// Maximum migrations per second
    pub migration_rate_limit: u32,
    /// Pattern-based policies
    pub patterns: Vec<PatternPolicy>,
    /// Minimum age before considering demotion (seconds)
    pub min_age_for_demotion_secs: u64,
    /// Access count threshold for promotion
    pub promotion_threshold_reads_per_hour: f64,
    /// Access count threshold for demotion
    pub demotion_threshold_reads_per_hour: f64,
}

impl Default for TieringConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            optimize_for: OptimizationTarget::Balanced,
            max_latency_ms: 10.0,
            memory_budget: 8 * 1024 * 1024 * 1024, // 8 GB
            costs: TierCostConfig::default(),
            calculation_interval: Duration::from_secs(300), // 5 minutes
            migration_rate_limit: 100,
            patterns: vec![],
            min_age_for_demotion_secs: 3600, // 1 hour
            promotion_threshold_reads_per_hour: 10.0,
            demotion_threshold_reads_per_hour: 1.0,
        }
    }
}

impl TieringConfig {
    /// Create a cost-optimized configuration
    pub fn cost_optimized() -> Self {
        Self {
            optimize_for: OptimizationTarget::Cost,
            max_latency_ms: 100.0,
            demotion_threshold_reads_per_hour: 5.0,
            ..Default::default()
        }
    }

    /// Create a latency-optimized configuration
    pub fn latency_optimized() -> Self {
        Self {
            optimize_for: OptimizationTarget::Latency,
            max_latency_ms: 1.0,
            promotion_threshold_reads_per_hour: 5.0,
            ..Default::default()
        }
    }

    /// Add a pattern policy
    pub fn add_pattern(&mut self, policy: PatternPolicy) {
        self.patterns.push(policy);
    }

    /// Find matching pattern for a key
    pub fn find_pattern(&self, key: &str) -> Option<&PatternPolicy> {
        self.patterns.iter().find(|p| p.matches(key))
    }

    /// Get constraints for a key
    pub fn constraints_for_key(&self, key: &str) -> PlacementConstraints {
        if let Some(pattern) = self.find_pattern(key) {
            pattern.constraints()
        } else {
            PlacementConstraints::with_max_latency(self.max_latency_ms)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tier_cost_config_default() {
        let config = TierCostConfig::default();
        assert_eq!(config.memory.storage_per_gb_month, 10.0);
        assert!(config.memory.storage_per_gb_month > config.ssd.storage_per_gb_month);
        assert!(config.ssd.storage_per_gb_month > config.cloud.storage_per_gb_month);
    }

    #[test]
    fn test_tier_cost_for_tier() {
        let config = TierCostConfig::default();
        assert_eq!(
            config
                .cost_for_tier(StorageTier::Memory)
                .storage_per_gb_month,
            10.0
        );
        assert_eq!(
            config
                .cost_for_tier(StorageTier::Cloud)
                .storage_per_gb_month,
            0.023
        );
    }

    #[test]
    fn test_glob_match_simple() {
        assert!(glob_match("session:*", "session:123"));
        assert!(glob_match("session:*", "session:abc:def"));
        assert!(!glob_match("session:*", "cache:123"));
    }

    #[test]
    fn test_glob_match_question() {
        assert!(glob_match("user:???", "user:123"));
        assert!(!glob_match("user:???", "user:12"));
        assert!(!glob_match("user:???", "user:1234"));
    }

    #[test]
    fn test_glob_match_complex() {
        assert!(glob_match("*:user:*", "cache:user:123"));
        assert!(glob_match("session:*:data", "session:123:data"));
    }

    #[test]
    fn test_pattern_policy() {
        let policy = PatternPolicy::new("session:*")
            .with_priority(Priority::Critical)
            .with_max_tier(StorageTier::Memory);

        assert!(policy.matches("session:123"));
        assert!(!policy.matches("cache:123"));
        assert_eq!(policy.priority, Some(Priority::Critical));
    }

    #[test]
    fn test_tiering_config_find_pattern() {
        let mut config = TieringConfig::default();
        config.add_pattern(PatternPolicy::new("session:*").with_priority(Priority::Critical));
        config.add_pattern(PatternPolicy::new("cache:*").with_priority(Priority::Low));

        let pattern = config.find_pattern("session:123");
        assert!(pattern.is_some());
        assert_eq!(pattern.unwrap().priority, Some(Priority::Critical));

        let pattern = config.find_pattern("unknown:123");
        assert!(pattern.is_none());
    }
}
