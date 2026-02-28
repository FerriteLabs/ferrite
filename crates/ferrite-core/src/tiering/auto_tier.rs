//! Autonomous auto-tiering engine
//!
//! Combines access pattern analysis, cost modeling, and migration scheduling
//! to autonomously move data between storage tiers.

#![allow(dead_code)]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use super::config::TierCostConfig;
use super::stats::StorageTier;

/// Configuration for autonomous tiering
pub struct AutoTierConfig {
    /// Enable autonomous tier migration
    pub enabled: bool,
    /// How often to evaluate keys for migration
    pub evaluation_interval: Duration,
    /// Maximum migrations per evaluation cycle
    pub max_migrations_per_cycle: usize,
    /// Minimum access count before considering promotion
    pub promotion_threshold: u64,
    /// Maximum idle time before considering demotion
    pub demotion_idle_threshold: Duration,
    /// Aggressiveness: 0.0 (conservative) to 1.0 (aggressive)
    pub aggressiveness: f64,
    /// Maximum latency impact tolerance (milliseconds)
    pub max_latency_impact_ms: f64,
    /// Cost optimization weight vs performance (0.0 = pure performance, 1.0 = pure cost)
    pub cost_weight: f64,
    /// Cost configuration per tier
    pub tier_costs: TierCostConfig,
}

impl Default for AutoTierConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            evaluation_interval: Duration::from_secs(60),
            max_migrations_per_cycle: 100,
            promotion_threshold: 10,
            demotion_idle_threshold: Duration::from_secs(3600),
            aggressiveness: 0.5,
            max_latency_impact_ms: 10.0,
            cost_weight: 0.5,
            tier_costs: TierCostConfig::default(),
        }
    }
}

/// Per-key access statistics with time-decay
pub struct KeyAccessStats {
    /// The key bytes
    pub key: Vec<u8>,
    /// Current storage tier
    pub current_tier: StorageTier,
    /// Total lifetime reads
    pub total_reads: u64,
    /// Total lifetime writes
    pub total_writes: u64,
    /// Value size in bytes
    pub size_bytes: u64,
    /// Timestamp of last access
    pub last_access: Instant,
    /// Timestamp of creation
    pub created_at: Instant,
    /// Exponentially-decayed access frequency (accesses per minute)
    pub decayed_frequency: f64,
    /// Recent access history for pattern detection (timestamp, is_write)
    pub recent_accesses: Vec<(Instant, bool)>,
}

impl KeyAccessStats {
    /// Create new stats for a key
    pub fn new(key: Vec<u8>, tier: StorageTier, size_bytes: u64) -> Self {
        let now = Instant::now();
        Self {
            key,
            current_tier: tier,
            total_reads: 0,
            total_writes: 0,
            size_bytes,
            last_access: now,
            created_at: now,
            decayed_frequency: 0.0,
            recent_accesses: Vec::new(),
        }
    }

    /// Compute the read/write ratio (reads per total ops), returns 0.5 when no ops
    pub fn read_write_ratio(&self) -> f64 {
        let total = self.total_reads + self.total_writes;
        if total == 0 {
            return 0.5;
        }
        self.total_reads as f64 / total as f64
    }

    /// Duration since last access
    pub fn idle_duration(&self) -> Duration {
        self.last_access.elapsed()
    }
}

/// Result of a tiering decision
#[derive(Debug, Clone)]
pub enum TierDecision {
    /// Keep in current tier
    Stay,
    /// Promote to a faster tier
    Promote {
        /// Target tier to promote to
        target: StorageTier,
        /// Human-readable reason
        reason: String,
    },
    /// Demote to a cheaper tier
    Demote {
        /// Target tier to demote to
        target: StorageTier,
        /// Human-readable reason
        reason: String,
    },
}

/// Cost savings report produced by the auto-tier engine
pub struct TieringSavingsReport {
    /// Estimated current monthly cost
    pub current_monthly_cost: f64,
    /// Estimated monthly cost after optimization
    pub optimized_monthly_cost: f64,
    /// Absolute savings per month
    pub savings_absolute: f64,
    /// Savings as a percentage of current cost
    pub savings_percentage: f64,
    /// Number of keys recommended for promotion
    pub keys_to_promote: usize,
    /// Number of keys recommended for demotion
    pub keys_to_demote: usize,
    /// Individual recommendations
    pub recommendations: Vec<TieringRecommendation>,
}

/// A single tiering recommendation
pub struct TieringRecommendation {
    /// Key pattern or key identifier
    pub key_pattern: String,
    /// Current tier of the key
    pub current_tier: StorageTier,
    /// Recommended target tier
    pub recommended_tier: StorageTier,
    /// Estimated monthly savings from this move
    pub estimated_savings: f64,
    /// Expected latency impact in milliseconds
    pub latency_impact_ms: f64,
}

/// Autonomous auto-tiering engine
///
/// Tracks per-key access patterns with exponential time-decay and uses
/// cost modeling to decide when to promote or demote keys between tiers.
pub struct AutoTierEngine {
    /// Engine configuration
    config: AutoTierConfig,
    /// Per-key access statistics, keyed by the raw key bytes
    key_stats: RwLock<HashMap<Vec<u8>, KeyAccessStats>>,
}

impl AutoTierEngine {
    /// Create a new auto-tier engine with the given configuration
    pub fn new(config: AutoTierConfig) -> Self {
        Self {
            config,
            key_stats: RwLock::new(HashMap::new()),
        }
    }

    /// Record an access to a key, updating decay-weighted frequency
    pub fn record_access(&self, key: &[u8], is_write: bool, size_bytes: u64) {
        let now = Instant::now();
        let mut stats = self.key_stats.write();

        let entry = stats
            .entry(key.to_vec())
            .or_insert_with(|| KeyAccessStats::new(key.to_vec(), StorageTier::Memory, size_bytes));

        if is_write {
            entry.total_writes += 1;
        } else {
            entry.total_reads += 1;
        }

        entry.size_bytes = size_bytes;
        entry.last_access = now;
        // Bump decayed frequency by 1.0 for this access
        entry.decayed_frequency += 1.0;

        // Keep recent access history bounded (last 1000 entries)
        entry.recent_accesses.push((now, is_write));
        if entry.recent_accesses.len() > 1000 {
            entry
                .recent_accesses
                .drain(0..entry.recent_accesses.len() - 1000);
        }
    }

    /// Evaluate a single key and produce a tier decision
    ///
    /// Decision factors:
    /// - Access frequency vs tier cost ratio
    /// - Time since last access
    /// - Key size (larger keys benefit more from cheaper tiers)
    /// - Read/write ratio (write-heavy keys stay in memory)
    /// - Configurable aggressiveness
    pub fn evaluate_key(&self, stats: &KeyAccessStats) -> TierDecision {
        let idle = stats.idle_duration();
        let rw_ratio = stats.read_write_ratio();
        let freq = stats.decayed_frequency;

        // --- Promotion check ---
        // High frequency + above promotion threshold → promote
        if freq >= self.config.promotion_threshold as f64 * (1.0 + self.config.aggressiveness) {
            if let Some(target) = self.next_hotter_tier(stats.current_tier) {
                let latency_impact =
                    self.tier_latency(target) - self.tier_latency(stats.current_tier);
                if latency_impact.abs() <= self.config.max_latency_impact_ms {
                    return TierDecision::Promote {
                        target,
                        reason: format!(
                            "High access frequency ({:.1}/min), promoting for lower latency",
                            freq
                        ),
                    };
                }
            }
        }

        // Write-heavy keys (write ratio > 60%) should stay in fast tiers
        if rw_ratio < 0.4 && stats.current_tier.is_colder_than(&StorageTier::Mmap) {
            if let Some(target) = self.next_hotter_tier(stats.current_tier) {
                return TierDecision::Promote {
                    target,
                    reason: format!(
                        "Write-heavy workload (read ratio {:.0}%), promoting for write performance",
                        rw_ratio * 100.0
                    ),
                };
            }
        }

        // --- Demotion check ---
        // Idle beyond threshold → demote
        if idle >= self.config.demotion_idle_threshold {
            if let Some(target) = self.next_colder_tier(stats.current_tier) {
                let latency_impact =
                    self.tier_latency(target) - self.tier_latency(stats.current_tier);
                if latency_impact <= self.config.max_latency_impact_ms {
                    return TierDecision::Demote {
                        target,
                        reason: format!(
                            "Idle for {:.0}s (threshold {}s), demoting to save cost",
                            idle.as_secs_f64(),
                            self.config.demotion_idle_threshold.as_secs()
                        ),
                    };
                }
            }
        }

        // Low frequency with cost-weight bias → demote
        let cost_threshold = self.config.promotion_threshold as f64
            * (1.0 - self.config.aggressiveness)
            * self.config.cost_weight;
        if freq < cost_threshold && stats.current_tier.is_hotter_than(&StorageTier::Cloud) {
            // Larger keys benefit more from cheaper tiers
            let size_factor = (stats.size_bytes as f64 / (1024.0 * 1024.0)).max(1.0);
            let adjusted_threshold = cost_threshold / size_factor.sqrt();

            if freq < adjusted_threshold {
                if let Some(target) = self.next_colder_tier(stats.current_tier) {
                    let latency_impact =
                        self.tier_latency(target) - self.tier_latency(stats.current_tier);
                    if latency_impact <= self.config.max_latency_impact_ms {
                        return TierDecision::Demote {
                            target,
                            reason: format!(
                                "Low access frequency ({:.2}/min) below threshold ({:.2}), size {:.1} KB",
                                freq,
                                adjusted_threshold,
                                stats.size_bytes as f64 / 1024.0
                            ),
                        };
                    }
                }
            }
        }

        TierDecision::Stay
    }

    /// Batch-evaluate all tracked keys and return those that need migration
    pub fn evaluate_all(&self) -> Vec<(Vec<u8>, TierDecision)> {
        let stats = self.key_stats.read();
        let mut decisions: Vec<(Vec<u8>, TierDecision)> = stats
            .values()
            .filter_map(|s| {
                let decision = self.evaluate_key(s);
                match &decision {
                    TierDecision::Stay => None,
                    _ => Some((s.key.clone(), decision)),
                }
            })
            .collect();

        // Respect max migrations per cycle
        decisions.truncate(self.config.max_migrations_per_cycle);
        decisions
    }

    /// Compute a savings report comparing current placement to optimal
    pub fn calculate_savings(&self) -> TieringSavingsReport {
        let stats = self.key_stats.read();
        let mut current_cost = 0.0;
        let mut optimized_cost = 0.0;
        let mut keys_to_promote = 0usize;
        let mut keys_to_demote = 0usize;
        let mut recommendations = Vec::new();

        for s in stats.values() {
            let cur = self.monthly_cost_for_key(s, s.current_tier);
            current_cost += cur;

            let decision = self.evaluate_key(s);
            match &decision {
                TierDecision::Promote { target, .. } => {
                    let opt = self.monthly_cost_for_key(s, *target);
                    optimized_cost += opt;
                    keys_to_promote += 1;
                    let latency_impact =
                        self.tier_latency(*target) - self.tier_latency(s.current_tier);
                    recommendations.push(TieringRecommendation {
                        key_pattern: String::from_utf8_lossy(&s.key).to_string(),
                        current_tier: s.current_tier,
                        recommended_tier: *target,
                        estimated_savings: cur - opt,
                        latency_impact_ms: latency_impact,
                    });
                }
                TierDecision::Demote { target, .. } => {
                    let opt = self.monthly_cost_for_key(s, *target);
                    optimized_cost += opt;
                    keys_to_demote += 1;
                    let latency_impact =
                        self.tier_latency(*target) - self.tier_latency(s.current_tier);
                    recommendations.push(TieringRecommendation {
                        key_pattern: String::from_utf8_lossy(&s.key).to_string(),
                        current_tier: s.current_tier,
                        recommended_tier: *target,
                        estimated_savings: cur - opt,
                        latency_impact_ms: latency_impact,
                    });
                }
                TierDecision::Stay => {
                    optimized_cost += cur;
                }
            }
        }

        let savings_absolute = current_cost - optimized_cost;
        let savings_percentage = if current_cost > 0.0 {
            (savings_absolute / current_cost) * 100.0
        } else {
            0.0
        };

        TieringSavingsReport {
            current_monthly_cost: current_cost,
            optimized_monthly_cost: optimized_cost,
            savings_absolute,
            savings_percentage,
            keys_to_promote,
            keys_to_demote,
            recommendations,
        }
    }

    /// Apply exponential decay to all frequency counters (half-life = 1 hour)
    pub fn update_decay(&self) {
        let mut stats = self.key_stats.write();
        // half-life = 1 hour = 3600s
        // decay_factor = 0.5^(dt / 3600)
        for s in stats.values_mut() {
            let elapsed = s.last_access.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                let decay = 0.5_f64.powf(elapsed / 3600.0);
                s.decayed_frequency *= decay;
            }
        }
    }

    // --- private helpers ---

    /// Get the next hotter (faster) tier, if any
    fn next_hotter_tier(&self, tier: StorageTier) -> Option<StorageTier> {
        match tier {
            StorageTier::Archive => Some(StorageTier::Cloud),
            StorageTier::Cloud => Some(StorageTier::Ssd),
            StorageTier::Ssd => Some(StorageTier::Mmap),
            StorageTier::Mmap => Some(StorageTier::Memory),
            StorageTier::Memory => None,
        }
    }

    /// Get the next colder (cheaper) tier, if any
    fn next_colder_tier(&self, tier: StorageTier) -> Option<StorageTier> {
        match tier {
            StorageTier::Memory => Some(StorageTier::Mmap),
            StorageTier::Mmap => Some(StorageTier::Ssd),
            StorageTier::Ssd => Some(StorageTier::Cloud),
            StorageTier::Cloud => Some(StorageTier::Archive),
            StorageTier::Archive => None,
        }
    }

    /// Retrieve the read latency (ms) for a tier from the cost config
    fn tier_latency(&self, tier: StorageTier) -> f64 {
        self.config.tier_costs.cost_for_tier(tier).read_latency_ms
    }

    /// Estimate monthly cost of hosting a key in a given tier
    fn monthly_cost_for_key(&self, stats: &KeyAccessStats, tier: StorageTier) -> f64 {
        let tc = self.config.tier_costs.cost_for_tier(tier);
        let size_gb = stats.size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);

        let storage_cost = size_gb * tc.storage_per_gb_month;
        let monthly_reads = stats.total_reads as f64; // approximate recent window
        let monthly_writes = stats.total_writes as f64;
        let read_cost = (monthly_reads / 1000.0) * tc.read_per_1k;
        let write_cost = (monthly_writes / 1000.0) * tc.write_per_1k;
        let egress_cost = if tier == StorageTier::Cloud || tier == StorageTier::Archive {
            size_gb * monthly_reads * tc.egress_per_gb
        } else {
            0.0
        };

        storage_cost + read_cost + write_cost + egress_cost
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_tier_config_default() {
        let config = AutoTierConfig::default();
        assert!(config.enabled);
        assert_eq!(config.evaluation_interval, Duration::from_secs(60));
        assert_eq!(config.max_migrations_per_cycle, 100);
        assert!((config.aggressiveness - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_key_access_stats_new() {
        let stats = KeyAccessStats::new(b"test:key".to_vec(), StorageTier::Memory, 1024);
        assert_eq!(stats.total_reads, 0);
        assert_eq!(stats.total_writes, 0);
        assert_eq!(stats.size_bytes, 1024);
        assert_eq!(stats.current_tier, StorageTier::Memory);
    }

    #[test]
    fn test_read_write_ratio_no_ops() {
        let stats = KeyAccessStats::new(b"k".to_vec(), StorageTier::Memory, 64);
        assert!((stats.read_write_ratio() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_read_write_ratio() {
        let mut stats = KeyAccessStats::new(b"k".to_vec(), StorageTier::Memory, 64);
        stats.total_reads = 80;
        stats.total_writes = 20;
        assert!((stats.read_write_ratio() - 0.8).abs() < f64::EPSILON);
    }

    #[test]
    fn test_record_access() {
        let engine = AutoTierEngine::new(AutoTierConfig::default());
        engine.record_access(b"key1", false, 256);
        engine.record_access(b"key1", true, 256);
        engine.record_access(b"key1", false, 256);

        let stats = engine.key_stats.read();
        let s = stats.get(&b"key1".to_vec()).unwrap();
        assert_eq!(s.total_reads, 2);
        assert_eq!(s.total_writes, 1);
        assert_eq!(s.recent_accesses.len(), 3);
    }

    #[test]
    fn test_evaluate_key_stay() {
        let config = AutoTierConfig {
            promotion_threshold: 100,
            demotion_idle_threshold: Duration::from_secs(7200),
            ..AutoTierConfig::default()
        };
        let engine = AutoTierEngine::new(config);
        let stats = KeyAccessStats::new(b"k".to_vec(), StorageTier::Ssd, 512);

        let decision = engine.evaluate_key(&stats);
        assert!(matches!(decision, TierDecision::Stay));
    }

    #[test]
    fn test_evaluate_key_promote_high_frequency() {
        let config = AutoTierConfig {
            promotion_threshold: 5,
            aggressiveness: 0.5,
            max_latency_impact_ms: 1000.0,
            ..AutoTierConfig::default()
        };
        let engine = AutoTierEngine::new(config);

        let mut stats = KeyAccessStats::new(b"hot".to_vec(), StorageTier::Ssd, 256);
        stats.decayed_frequency = 100.0;
        stats.total_reads = 500;

        let decision = engine.evaluate_key(&stats);
        assert!(matches!(decision, TierDecision::Promote { .. }));
    }

    #[test]
    fn test_evaluate_all_respects_limit() {
        let config = AutoTierConfig {
            max_migrations_per_cycle: 2,
            promotion_threshold: 1,
            aggressiveness: 0.0,
            max_latency_impact_ms: 100000.0,
            ..AutoTierConfig::default()
        };
        let engine = AutoTierEngine::new(config);

        // Insert many hot keys in a cold tier
        for i in 0..10 {
            let key = format!("key:{}", i).into_bytes();
            {
                let mut stats = engine.key_stats.write();
                let mut s = KeyAccessStats::new(key.clone(), StorageTier::Cloud, 128);
                s.decayed_frequency = 999.0;
                s.total_reads = 999;
                stats.insert(key, s);
            }
        }

        let decisions = engine.evaluate_all();
        assert!(decisions.len() <= 2);
    }

    #[test]
    fn test_calculate_savings() {
        let engine = AutoTierEngine::new(AutoTierConfig::default());
        // With no keys, savings should be zero
        let report = engine.calculate_savings();
        assert_eq!(report.current_monthly_cost, 0.0);
        assert_eq!(report.optimized_monthly_cost, 0.0);
        assert_eq!(report.keys_to_promote, 0);
        assert_eq!(report.keys_to_demote, 0);
    }

    #[test]
    fn test_update_decay() {
        let engine = AutoTierEngine::new(AutoTierConfig::default());
        engine.record_access(b"k", false, 64);

        // Decay should not panic and frequency should remain non-negative
        engine.update_decay();
        let stats = engine.key_stats.read();
        let s = stats.get(&b"k".to_vec()).unwrap();
        assert!(s.decayed_frequency >= 0.0);
    }
}
