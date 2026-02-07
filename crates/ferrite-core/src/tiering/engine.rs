//! Main tiering engine that coordinates all tiering operations
//!
//! The TieringEngine is the central coordinator for cost-aware tiering,
//! integrating access tracking, cost calculation, optimization, and migration.

use super::access::{AccessPattern, AccessPatternAnalyzer, AccessType};
use super::config::{PlacementConstraints, TieringConfig};
use super::cost::{CostSummary, KeyCostAnalysis, TierCostCalculator};
use super::migration::{MigrationExecutor, MigrationStats};
use super::optimizer::{PlacementDecision, PlacementOptimizer};
use super::stats::{KeyAccessStats, Priority, StorageTier};
use bytes::Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Distribution of keys across tiers
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TierDistribution {
    /// Keys per tier
    pub keys: Vec<(StorageTier, u64)>,
    /// Size per tier (bytes)
    pub size: Vec<(StorageTier, u64)>,
    /// Cost per tier (monthly)
    pub cost: Vec<(StorageTier, f64)>,
}

impl TierDistribution {
    /// Create new empty distribution
    pub fn new() -> Self {
        Self::default()
    }

    /// Add stats for a tier
    pub fn add(&mut self, tier: StorageTier, keys: u64, size: u64, cost: f64) {
        self.keys.push((tier, keys));
        self.size.push((tier, size));
        self.cost.push((tier, cost));
    }

    /// Get total keys
    pub fn total_keys(&self) -> u64 {
        self.keys.iter().map(|(_, k)| k).sum()
    }

    /// Get total size
    pub fn total_size(&self) -> u64 {
        self.size.iter().map(|(_, s)| s).sum()
    }

    /// Get total cost
    pub fn total_cost(&self) -> f64 {
        self.cost.iter().map(|(_, c)| c).sum()
    }

    /// Get percentage of keys in a tier
    pub fn key_percentage(&self, tier: StorageTier) -> f64 {
        let total = self.total_keys() as f64;
        if total == 0.0 {
            return 0.0;
        }
        self.keys
            .iter()
            .find(|(t, _)| *t == tier)
            .map(|(_, k)| *k as f64 / total * 100.0)
            .unwrap_or(0.0)
    }
}

/// Overall tiering information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringInfo {
    /// Total keys tracked
    pub total_keys: u64,
    /// Total size in bytes
    pub total_size_bytes: u64,
    /// Current monthly cost
    pub monthly_cost_current: f64,
    /// Optimal monthly cost
    pub monthly_cost_optimal: f64,
    /// Potential savings percentage
    pub potential_savings_pct: f64,
    /// Distribution across tiers
    pub tier_distribution: TierDistribution,
    /// Pending migrations
    pub migrations_pending: u64,
    /// Migration rate per second
    pub migrations_rate_per_sec: u32,
}

impl TieringInfo {
    /// Get total size in GB
    pub fn total_size_gb(&self) -> f64 {
        self.total_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    }

    /// Get potential savings amount
    pub fn potential_savings(&self) -> f64 {
        self.monthly_cost_current - self.monthly_cost_optimal
    }
}

/// Main tiering engine
pub struct TieringEngine {
    /// Configuration
    config: TieringConfig,
    /// Per-key access statistics
    stats: DashMap<u64, KeyAccessStats>,
    /// Key hash to key mapping (for reverse lookup)
    key_map: DashMap<u64, Bytes>,
    /// Cost calculator
    cost_calculator: TierCostCalculator,
    /// Placement optimizer
    optimizer: PlacementOptimizer,
    /// Migration executor
    migration_executor: Arc<MigrationExecutor>,
    /// Pattern analyzer
    pattern_analyzer: AccessPatternAnalyzer,
    /// Engine start time for relative timestamps
    start_time: Instant,
    /// Current memory usage
    memory_usage: AtomicU64,
}

impl TieringEngine {
    /// Create a new tiering engine
    pub fn new(config: TieringConfig) -> Self {
        let cost_calculator = TierCostCalculator::new(config.costs.clone());
        let optimizer = PlacementOptimizer::new(&config);
        let migration_executor = Arc::new(MigrationExecutor::new(config.migration_rate_limit));
        let pattern_analyzer = AccessPatternAnalyzer::new();

        Self {
            config,
            stats: DashMap::new(),
            key_map: DashMap::new(),
            cost_calculator,
            optimizer,
            migration_executor,
            pattern_analyzer,
            start_time: Instant::now(),
            memory_usage: AtomicU64::new(0),
        }
    }

    /// Create with default config
    pub fn new_default() -> Self {
        Self::new(TieringConfig::default())
    }

    /// Get current timestamp in seconds since engine start
    fn current_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    /// Compute hash for a key
    fn hash_key(key: &[u8]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Record an access to a key
    pub fn record_access(&self, key: &Bytes, access_type: AccessType, size: u32) {
        let hash = Self::hash_key(key);
        let current_secs = self.current_secs();

        let mut stats = self.stats.entry(hash).or_insert_with(|| {
            // Store key mapping
            self.key_map.insert(hash, key.clone());
            KeyAccessStats::with_hash(hash, size, StorageTier::Memory)
        });

        match access_type {
            AccessType::Read => stats.record_read(current_secs),
            AccessType::Write => {
                stats.record_write(current_secs);
                stats.update_size(size);
            }
        }
    }

    /// Get stats for a key
    pub fn get_stats(&self, key: &Bytes) -> Option<KeyAccessStats> {
        let hash = Self::hash_key(key);
        self.stats.get(&hash).map(|s| s.clone())
    }

    /// Set key priority
    pub fn set_priority(&self, key: &Bytes, priority: Priority) {
        let hash = Self::hash_key(key);
        if let Some(mut stats) = self.stats.get_mut(&hash) {
            stats.priority = priority;
        }
    }

    /// Pin key to a tier
    pub fn pin_to_tier(&self, key: &Bytes, tier: StorageTier) {
        let hash = Self::hash_key(key);
        if let Some(mut stats) = self.stats.get_mut(&hash) {
            stats.tier = tier;
            stats.pinned = true;
        }
    }

    /// Unpin key
    pub fn unpin(&self, key: &Bytes) {
        let hash = Self::hash_key(key);
        if let Some(mut stats) = self.stats.get_mut(&hash) {
            stats.pinned = false;
        }
    }

    /// Get current tier for a key
    pub fn get_tier(&self, key: &Bytes) -> Option<StorageTier> {
        let hash = Self::hash_key(key);
        self.stats.get(&hash).map(|s| s.tier)
    }

    /// Update key's current tier
    pub fn set_tier(&self, key: &Bytes, tier: StorageTier) {
        let hash = Self::hash_key(key);
        if let Some(mut stats) = self.stats.get_mut(&hash) {
            stats.tier = tier;
        }
    }

    /// Get optimal tier for a key
    pub fn optimal_tier(
        &self,
        stats: &KeyAccessStats,
        constraints: &PlacementConstraints,
    ) -> StorageTier {
        self.optimizer
            .optimal_tier_with_pattern(stats, constraints)
            .tier
    }

    /// Get full placement decision for a key
    pub fn placement_decision(
        &self,
        key: &Bytes,
        constraints: &PlacementConstraints,
    ) -> Option<PlacementDecision> {
        let hash = Self::hash_key(key);
        self.stats
            .get(&hash)
            .map(|s| self.optimizer.optimal_tier_with_pattern(&s, constraints))
    }

    /// Calculate cost breakdown for a key
    pub fn key_cost(&self, key: &Bytes) -> Option<KeyCostAnalysis> {
        let hash = Self::hash_key(key);
        let stats = self.stats.get(&hash)?;

        let constraints = self
            .config
            .constraints_for_key(std::str::from_utf8(key).unwrap_or(""));
        let decision = self
            .optimizer
            .optimal_tier_with_pattern(&stats, &constraints);
        let current_cost = self
            .cost_calculator
            .calculate_monthly_cost(&stats, stats.tier);
        let pattern = self.pattern_analyzer.detect_pattern(&stats);

        Some(KeyCostAnalysis {
            key: String::from_utf8_lossy(key).to_string(),
            size_bytes: stats.size,
            current_tier: stats.tier,
            current_cost_monthly: current_cost.total,
            access_pattern: pattern.name().to_string(),
            reads_per_day: stats.access_counts.reads_1d,
            writes_per_day: stats.access_counts.writes_1d,
            optimal_tier: decision.tier,
            optimal_cost_monthly: decision.cost.total,
            potential_savings_pct: if current_cost.total > 0.0 {
                ((current_cost.total - decision.cost.total) / current_cost.total) * 100.0
            } else {
                0.0
            },
            tier_breakdown: decision.alternatives,
        })
    }

    /// Get tiering info summary
    pub async fn info(&self) -> TieringInfo {
        let mut distribution = TierDistribution::new();
        let mut total_current_cost = 0.0;
        let mut total_optimal_cost = 0.0;
        let mut total_size = 0u64;

        // Group by tier
        let mut tier_stats: std::collections::HashMap<StorageTier, (u64, u64, f64)> =
            std::collections::HashMap::new();

        let constraints = PlacementConstraints::with_max_latency(self.config.max_latency_ms);

        for entry in self.stats.iter() {
            let stats = entry.value();
            let current_cost = self
                .cost_calculator
                .calculate_monthly_cost(stats, stats.tier);
            let optimal = self.optimizer.optimal_tier(stats, &constraints);

            total_current_cost += current_cost.total;
            total_optimal_cost += optimal.cost.total;
            total_size += stats.size as u64;

            let tier_entry = tier_stats.entry(stats.tier).or_insert((0, 0, 0.0));
            tier_entry.0 += 1;
            tier_entry.1 += stats.size as u64;
            tier_entry.2 += current_cost.total;
        }

        for tier in StorageTier::all() {
            if let Some((keys, size, cost)) = tier_stats.get(tier) {
                distribution.add(*tier, *keys, *size, *cost);
            }
        }

        let migrations_pending = self.migration_executor.pending_count().await as u64;

        TieringInfo {
            total_keys: self.stats.len() as u64,
            total_size_bytes: total_size,
            monthly_cost_current: total_current_cost,
            monthly_cost_optimal: total_optimal_cost,
            potential_savings_pct: if total_current_cost > 0.0 {
                ((total_current_cost - total_optimal_cost) / total_current_cost) * 100.0
            } else {
                0.0
            },
            tier_distribution: distribution,
            migrations_pending,
            migrations_rate_per_sec: self.config.migration_rate_limit,
        }
    }

    /// Get cost summary
    pub fn cost_summary(&self) -> CostSummary {
        let mut summary = CostSummary::new();
        let constraints = PlacementConstraints::with_max_latency(self.config.max_latency_ms);

        // Group by tier
        let mut tier_data: std::collections::HashMap<StorageTier, (u64, u64, f64)> =
            std::collections::HashMap::new();

        for entry in self.stats.iter() {
            let stats = entry.value();
            let cost = self
                .cost_calculator
                .calculate_monthly_cost(stats, stats.tier);
            let optimal = self.optimizer.optimal_tier(stats, &constraints);

            summary.monthly_cost_optimal += optimal.cost.total;

            let tier_entry = tier_data.entry(stats.tier).or_insert((0, 0, 0.0));
            tier_entry.0 += 1;
            tier_entry.1 += stats.size as u64;
            tier_entry.2 += cost.total;
        }

        for (tier, (keys, size, cost)) in tier_data {
            summary.add_tier_stats(tier, keys, size, cost);
        }

        summary
    }

    /// Get top N keys by cost
    pub fn top_by_cost(&self, limit: usize) -> Vec<(Bytes, KeyAccessStats, f64)> {
        let mut costs: Vec<_> = self
            .stats
            .iter()
            .map(|entry| {
                let hash = *entry.key();
                let stats = entry.value().clone();
                let cost = self
                    .cost_calculator
                    .calculate_monthly_cost(&stats, stats.tier)
                    .total;
                let key = self
                    .key_map
                    .get(&hash)
                    .map(|k| k.clone())
                    .unwrap_or_else(|| Bytes::from(format!("hash:{}", hash)));
                (key, stats, cost)
            })
            .collect();

        costs.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
        costs.truncate(limit);
        costs
    }

    /// Get top N keys with potential savings
    pub fn top_savings(&self, limit: usize) -> Vec<(Bytes, KeyAccessStats, f64)> {
        let constraints = PlacementConstraints::with_max_latency(self.config.max_latency_ms);

        let mut savings: Vec<_> = self
            .stats
            .iter()
            .filter_map(|entry| {
                let hash = *entry.key();
                let stats = entry.value().clone();
                let current_cost = self
                    .cost_calculator
                    .calculate_monthly_cost(&stats, stats.tier)
                    .total;
                let optimal = self.optimizer.optimal_tier(&stats, &constraints);
                let saving = current_cost - optimal.cost.total;

                if saving > 0.0 {
                    let key = self
                        .key_map
                        .get(&hash)
                        .map(|k| k.clone())
                        .unwrap_or_else(|| Bytes::from(format!("hash:{}", hash)));
                    Some((key, stats, saving))
                } else {
                    None
                }
            })
            .collect();

        savings.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
        savings.truncate(limit);
        savings
    }

    /// Get suboptimal keys
    pub fn suboptimal_keys(&self, limit: usize) -> Vec<(Bytes, KeyAccessStats, PlacementDecision)> {
        let constraints = PlacementConstraints::with_max_latency(self.config.max_latency_ms);

        let mut suboptimal: Vec<_> = self
            .stats
            .iter()
            .filter_map(|entry| {
                let hash = *entry.key();
                let stats = entry.value().clone();
                let decision = self
                    .optimizer
                    .optimal_tier_with_pattern(&stats, &constraints);

                if decision.tier != stats.tier {
                    let key = self
                        .key_map
                        .get(&hash)
                        .map(|k| k.clone())
                        .unwrap_or_else(|| Bytes::from(format!("hash:{}", hash)));
                    Some((key, stats, decision))
                } else {
                    None
                }
            })
            .collect();

        // Sort by potential savings
        suboptimal.sort_by(|a, b| {
            let savings_a = self
                .cost_calculator
                .calculate_monthly_cost(&a.1, a.1.tier)
                .total
                - a.2.cost.total;
            let savings_b = self
                .cost_calculator
                .calculate_monthly_cost(&b.1, b.1.tier)
                .total
                - b.2.cost.total;
            savings_b.partial_cmp(&savings_a).unwrap_or(std::cmp::Ordering::Equal)
        });

        suboptimal.truncate(limit);
        suboptimal
    }

    /// Queue a migration
    pub async fn queue_migration(&self, key: &Bytes, to_tier: StorageTier) -> Option<u64> {
        let hash = Self::hash_key(key);
        let stats = self.stats.get(&hash)?;

        let id = self
            .migration_executor
            .queue_migration(key.clone(), stats.tier, to_tier, stats.size as u64)
            .await;

        Some(id)
    }

    /// Get migration statistics
    pub async fn migration_stats(&self) -> MigrationStats {
        self.migration_executor.stats().await
    }

    /// Detect access pattern for a key
    pub fn detect_pattern(&self, key: &Bytes) -> Option<AccessPattern> {
        let hash = Self::hash_key(key);
        self.stats
            .get(&hash)
            .map(|s| self.pattern_analyzer.detect_pattern(&s))
    }

    /// Get configuration
    pub fn config(&self) -> &TieringConfig {
        &self.config
    }

    /// Update configuration
    pub fn set_config(&mut self, config: TieringConfig) {
        self.optimizer = PlacementOptimizer::new(&config);
        self.cost_calculator = TierCostCalculator::new(config.costs.clone());
        self.config = config;
    }

    /// Update memory usage
    pub fn set_memory_usage(&self, bytes: u64) {
        self.memory_usage.store(bytes, Ordering::Relaxed);
    }

    /// Get current memory usage
    pub fn memory_usage(&self) -> u64 {
        self.memory_usage.load(Ordering::Relaxed)
    }

    /// Remove stats for a key (e.g., when deleted)
    pub fn remove_key(&self, key: &Bytes) {
        let hash = Self::hash_key(key);
        self.stats.remove(&hash);
        self.key_map.remove(&hash);
    }

    /// Clear all stats
    pub fn clear(&self) {
        self.stats.clear();
        self.key_map.clear();
    }

    /// Get total tracked keys
    pub fn key_count(&self) -> usize {
        self.stats.len()
    }

    /// Run periodic maintenance (call this from a background task)
    pub fn maintenance(&self) {
        let current_secs = self.current_secs();

        // Decay access counts for keys not accessed recently
        for mut entry in self.stats.iter_mut() {
            let stats = entry.value_mut();

            // If no access in the last hour, decay counts
            if stats.age_secs(current_secs) > 3600 {
                stats.access_counts.decay(0.9);
            }

            // If no access in the last day, decay more aggressively
            if stats.age_secs(current_secs) > 86400 {
                stats.access_counts.decay(0.5);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tiering_engine_new() {
        let config = TieringConfig::default();
        let engine = TieringEngine::new(config);
        assert_eq!(engine.key_count(), 0);
    }

    #[test]
    fn test_record_access() {
        let engine = TieringEngine::new(TieringConfig::default());
        let key = Bytes::from("test:key");

        engine.record_access(&key, AccessType::Read, 1024);
        let stats = engine.get_stats(&key);

        assert!(stats.is_some());
        let stats = stats.unwrap();
        assert_eq!(stats.access_counts.reads_1d, 1);
        assert_eq!(stats.size, 1024);
    }

    #[test]
    fn test_set_priority() {
        let engine = TieringEngine::new(TieringConfig::default());
        let key = Bytes::from("test:key");

        engine.record_access(&key, AccessType::Write, 1024);
        engine.set_priority(&key, Priority::Critical);

        let stats = engine.get_stats(&key).unwrap();
        assert_eq!(stats.priority, Priority::Critical);
    }

    #[test]
    fn test_pin_to_tier() {
        let engine = TieringEngine::new(TieringConfig::default());
        let key = Bytes::from("test:key");

        engine.record_access(&key, AccessType::Write, 1024);
        engine.pin_to_tier(&key, StorageTier::Ssd);

        let stats = engine.get_stats(&key).unwrap();
        assert!(stats.pinned);
        assert_eq!(stats.tier, StorageTier::Ssd);
    }

    #[test]
    fn test_key_cost() {
        let engine = TieringEngine::new(TieringConfig::default());
        let key = Bytes::from("test:key");

        engine.record_access(&key, AccessType::Write, 1024 * 1024); // 1 MB
        let analysis = engine.key_cost(&key);

        assert!(analysis.is_some());
        let analysis = analysis.unwrap();
        assert_eq!(analysis.size_bytes, 1024 * 1024);
        assert!(analysis.current_cost_monthly > 0.0);
    }

    #[test]
    fn test_top_by_cost() {
        let engine = TieringEngine::new(TieringConfig::default());

        // Add keys of different sizes
        for i in 0..10 {
            let key = Bytes::from(format!("key:{}", i));
            let size = (i + 1) * 1024 * 100; // Varying sizes
            engine.record_access(&key, AccessType::Write, size as u32);
        }

        let top = engine.top_by_cost(5);
        assert_eq!(top.len(), 5);

        // Should be sorted by cost (larger keys = higher cost)
        for i in 1..top.len() {
            assert!(top[i - 1].2 >= top[i].2);
        }
    }

    #[test]
    fn test_suboptimal_keys() {
        let engine = TieringEngine::new(TieringConfig::default());

        // Add a cold key in memory (suboptimal)
        let key = Bytes::from("cold:key");
        engine.record_access(&key, AccessType::Write, 1024 * 1024);
        // Don't access it again - it's "cold"

        let suboptimal = engine.suboptimal_keys(10);
        // Cold data in memory is suboptimal
        assert!(!suboptimal.is_empty());
    }

    #[test]
    fn test_remove_key() {
        let engine = TieringEngine::new(TieringConfig::default());
        let key = Bytes::from("test:key");

        engine.record_access(&key, AccessType::Write, 1024);
        assert!(engine.get_stats(&key).is_some());

        engine.remove_key(&key);
        assert!(engine.get_stats(&key).is_none());
    }

    #[tokio::test]
    async fn test_info() {
        let engine = TieringEngine::new(TieringConfig::default());
        let key = Bytes::from("test:key");

        engine.record_access(&key, AccessType::Write, 1024);
        let info = engine.info().await;

        assert_eq!(info.total_keys, 1);
        assert_eq!(info.total_size_bytes, 1024);
    }

    #[test]
    fn test_detect_pattern() {
        let engine = TieringEngine::new(TieringConfig::default());
        let key = Bytes::from("test:key");

        // Cold key (no access)
        engine.record_access(&key, AccessType::Write, 1024);
        let pattern = engine.detect_pattern(&key);

        assert!(pattern.is_some());
        // With only writes and no reads, it should be detected as cold or write-heavy
        let pattern = pattern.unwrap();
        assert!(
            matches!(pattern, AccessPattern::Cold)
                || matches!(pattern, AccessPattern::WriteHeavy { .. })
        );
    }
}
