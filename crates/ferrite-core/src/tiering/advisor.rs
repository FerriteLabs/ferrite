//! Adaptive Tiering Advisor
//!
//! This module provides an automated tiering advisor that analyzes workload
//! patterns and generates actionable recommendations with budget awareness
//! and SLA constraints. It bridges the gap between raw access statistics
//! and intelligent, cost-optimized tier placement decisions.
//!
//! ## Architecture
//!
//! The advisor operates in three phases:
//! 1. **Analysis** — Examines workload profiles to classify key access patterns
//! 2. **Constraint Application** — Adjusts recommendations for budget and SLA limits
//! 3. **Plan Generation** — Produces ordered migration steps for safe execution
//!
//! ## Usage
//!
//! ```rust,ignore
//! use ferrite::tiering::advisor::{TieringAdvisor, AdvisorConfig, WorkloadProfile};
//!
//! let advisor = TieringAdvisor::new(AdvisorConfig::default());
//! let recommendation = advisor.analyze(&workload);
//!
//! // Apply constraints
//! let mut rec = recommendation;
//! advisor.apply_budget_constraints(&mut rec, &budget);
//! advisor.apply_sla_constraints(&mut rec, &sla);
//!
//! // Generate executable plan
//! let plan = advisor.generate_migration_plan(&rec);
//! ```

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// TierName enum
// ---------------------------------------------------------------------------

/// Named storage tier for advisor recommendations.
///
/// This is a simplified view of the storage hierarchy used in advisor
/// output. It maps directly to the underlying [`super::stats::StorageTier`]
/// variants relevant to cost-aware tiering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TierName {
    /// In-memory storage — fastest, most expensive
    Memory,
    /// Memory-mapped files — warm tier
    Mmap,
    /// Local disk (SSD) storage
    Disk,
    /// Cloud object storage (S3/GCS/Azure)
    Cloud,
}

impl TierName {
    /// Get display name
    pub fn name(&self) -> &'static str {
        match self {
            TierName::Memory => "memory",
            TierName::Mmap => "mmap",
            TierName::Disk => "disk",
            TierName::Cloud => "cloud",
        }
    }

    /// Approximate read latency in milliseconds for this tier
    fn typical_latency_ms(&self) -> f64 {
        match self {
            TierName::Memory => 0.1,
            TierName::Mmap => 0.5,
            TierName::Disk => 1.0,
            TierName::Cloud => 50.0,
        }
    }
}

impl std::fmt::Display for TierName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors that can occur during advisor operations
#[derive(Debug, thiserror::Error)]
pub enum AdvisorError {
    /// Insufficient observation data to make a recommendation
    #[error("insufficient observation data: need {required_secs}s, have {available_secs}s")]
    InsufficientData {
        /// Seconds of observation required
        required_secs: u64,
        /// Seconds of observation available
        available_secs: u64,
    },

    /// Invalid configuration parameter
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// Budget constraint makes the recommendation infeasible
    #[error("budget constraint infeasible: monthly cost ${cost:.2} exceeds budget ${budget:.2}")]
    BudgetInfeasible {
        /// Computed minimum monthly cost
        cost: f64,
        /// Allowed budget
        budget: f64,
    },
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the tiering advisor.
///
/// Controls how often the advisor re-evaluates, the minimum observation
/// window before producing recommendations, confidence thresholds, and
/// migration limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorConfig {
    /// How often to re-evaluate recommendations (seconds)
    pub evaluation_interval_secs: u64,
    /// Minimum observation window before making recommendations (seconds)
    pub min_observation_window_secs: u64,
    /// Confidence threshold (0.0–1.0) below which recommendations are suppressed
    pub confidence_threshold: f64,
    /// Maximum key movements allowed per evaluation cycle
    pub max_migrations_per_cycle: usize,
    /// When true, recommendations are generated but not executed
    pub dry_run: bool,
}

impl Default for AdvisorConfig {
    fn default() -> Self {
        Self {
            evaluation_interval_secs: 300,
            min_observation_window_secs: 3600,
            confidence_threshold: 0.7,
            max_migrations_per_cycle: 1000,
            dry_run: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Workload description types
// ---------------------------------------------------------------------------

/// Distribution of keys and bytes across storage tiers.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TierDistribution {
    /// Keys in memory tier
    pub memory_keys: u64,
    /// Bytes in memory tier
    pub memory_bytes: u64,
    /// Keys in mmap tier
    pub mmap_keys: u64,
    /// Bytes in mmap tier
    pub mmap_bytes: u64,
    /// Keys in disk tier
    pub disk_keys: u64,
    /// Bytes in disk tier
    pub disk_bytes: u64,
    /// Keys in cloud tier
    pub cloud_keys: u64,
    /// Bytes in cloud tier
    pub cloud_bytes: u64,
}

impl TierDistribution {
    /// Total keys across all tiers
    pub fn total_keys(&self) -> u64 {
        self.memory_keys + self.mmap_keys + self.disk_keys + self.cloud_keys
    }

    /// Total bytes across all tiers
    pub fn total_bytes(&self) -> u64 {
        self.memory_bytes + self.mmap_bytes + self.disk_bytes + self.cloud_bytes
    }

    /// Get keys for a specific tier
    pub fn keys_for_tier(&self, tier: TierName) -> u64 {
        match tier {
            TierName::Memory => self.memory_keys,
            TierName::Mmap => self.mmap_keys,
            TierName::Disk => self.disk_keys,
            TierName::Cloud => self.cloud_keys,
        }
    }

    /// Get bytes for a specific tier
    pub fn bytes_for_tier(&self, tier: TierName) -> u64 {
        match tier {
            TierName::Memory => self.memory_bytes,
            TierName::Mmap => self.mmap_bytes,
            TierName::Disk => self.disk_bytes,
            TierName::Cloud => self.cloud_bytes,
        }
    }

    /// Set keys for a specific tier
    pub fn set_keys(&mut self, tier: TierName, keys: u64) {
        match tier {
            TierName::Memory => self.memory_keys = keys,
            TierName::Mmap => self.mmap_keys = keys,
            TierName::Disk => self.disk_keys = keys,
            TierName::Cloud => self.cloud_keys = keys,
        }
    }

    /// Set bytes for a specific tier
    pub fn set_bytes(&mut self, tier: TierName, bytes: u64) {
        match tier {
            TierName::Memory => self.memory_bytes = bytes,
            TierName::Mmap => self.mmap_bytes = bytes,
            TierName::Disk => self.disk_bytes = bytes,
            TierName::Cloud => self.cloud_bytes = bytes,
        }
    }
}

/// Hourly access pattern snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessPattern {
    /// Hour of day (0–23)
    pub hour: u8,
    /// Number of read operations in this hour
    pub reads: u64,
    /// Number of write operations in this hour
    pub writes: u64,
}

/// Describes the current workload characteristics.
///
/// Captures everything the advisor needs to produce a recommendation:
/// key counts, memory footprint, access ratios, size statistics, hourly
/// access patterns, and the current tier distribution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadProfile {
    /// Total number of keys
    pub total_keys: u64,
    /// Total memory consumption in bytes
    pub total_memory_bytes: u64,
    /// Fraction of keys accessed recently (0.0–1.0)
    pub hot_key_ratio: f64,
    /// Ratio of reads to writes (reads per write)
    pub read_write_ratio: f64,
    /// Average key size in bytes
    pub avg_key_size_bytes: u64,
    /// Average value size in bytes
    pub avg_value_size_bytes: u64,
    /// Hourly access counts (up to 24 entries)
    pub access_patterns: Vec<AccessPattern>,
    /// Current distribution of keys/bytes across tiers
    pub current_distribution: TierDistribution,
}

// ---------------------------------------------------------------------------
// Recommendation types
// ---------------------------------------------------------------------------

/// A recommended movement of keys between tiers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyMovement {
    /// Key pattern (e.g. `"session:*"`)
    pub pattern: String,
    /// Current tier
    pub from_tier: TierName,
    /// Recommended tier
    pub to_tier: TierName,
    /// Estimated number of keys matching the pattern
    pub estimated_keys: u64,
    /// Estimated total bytes for those keys
    pub estimated_bytes: u64,
    /// Human-readable explanation for this movement
    pub reason: String,
}

/// Cost savings estimate.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SavingsEstimate {
    /// Current monthly cost in USD
    pub current_monthly_cost_usd: f64,
    /// Recommended monthly cost in USD
    pub recommended_monthly_cost_usd: f64,
    /// Absolute savings in USD
    pub savings_usd: f64,
    /// Savings as a percentage of current cost
    pub savings_percent: f64,
}

/// Full recommendation produced by the advisor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringRecommendation {
    /// Recommended tier distribution
    pub recommended_distribution: TierDistribution,
    /// Specific key movements
    pub key_movements: Vec<KeyMovement>,
    /// Estimated cost savings
    pub estimated_savings: SavingsEstimate,
    /// Confidence score (0.0–1.0)
    pub confidence: f64,
    /// Human-readable reasoning
    pub reasoning: Vec<String>,
    /// Warnings (e.g. data quality, constraint conflicts)
    pub warnings: Vec<String>,
    /// When the recommendation was generated
    pub generated_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Constraint types
// ---------------------------------------------------------------------------

/// Budget constraint for the advisor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetConstraint {
    /// Maximum acceptable monthly cost in USD
    pub max_monthly_cost_usd: f64,
    /// Cost per GB of memory per month
    pub memory_cost_per_gb_usd: f64,
    /// Cost per GB of disk per month
    pub disk_cost_per_gb_usd: f64,
    /// Cost per GB of cloud storage per month
    pub cloud_cost_per_gb_usd: f64,
}

impl Default for BudgetConstraint {
    fn default() -> Self {
        Self {
            max_monthly_cost_usd: 1000.0,
            memory_cost_per_gb_usd: 10.0,
            disk_cost_per_gb_usd: 0.15,
            cloud_cost_per_gb_usd: 0.023,
        }
    }
}

/// SLA constraint for the advisor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlaConstraint {
    /// Maximum p99 read latency in milliseconds
    pub max_p99_latency_ms: f64,
    /// Maximum p99.9 read latency in milliseconds
    pub max_p999_latency_ms: f64,
    /// Minimum availability (e.g. 0.999 for three nines)
    pub min_availability: f64,
}

impl Default for SlaConstraint {
    fn default() -> Self {
        Self {
            max_p99_latency_ms: 10.0,
            max_p999_latency_ms: 50.0,
            min_availability: 0.999,
        }
    }
}

// ---------------------------------------------------------------------------
// Migration plan types
// ---------------------------------------------------------------------------

/// A single step in a migration plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationStep {
    /// Execution order (starting from 1)
    pub order: u32,
    /// The key movement to execute
    pub movement: KeyMovement,
    /// Whether this step can run in parallel with the next step
    pub can_parallel: bool,
}

/// An ordered migration plan derived from a recommendation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationPlan {
    /// Ordered migration steps
    pub steps: Vec<MigrationStep>,
    /// Total bytes that will be moved
    pub total_bytes_to_move: u64,
    /// Estimated wall-clock time to complete in seconds
    pub estimated_duration_secs: u64,
}

// ---------------------------------------------------------------------------
// Advisor implementation
// ---------------------------------------------------------------------------

/// The main tiering advisor that produces cost-aware placement recommendations.
///
/// The advisor examines a [`WorkloadProfile`] and produces a
/// [`TieringRecommendation`] that can be further refined with budget and SLA
/// constraints before being turned into a [`MigrationPlan`].
pub struct TieringAdvisor {
    config: AdvisorConfig,
}

/// Threshold: keys accessed more than 10 times/hour are considered hot.
const HOT_ACCESS_THRESHOLD: f64 = 10.0;
/// Threshold: keys accessed 1–10 times/hour are warm.
const WARM_ACCESS_THRESHOLD: f64 = 1.0;
/// Large value threshold in bytes (1 MB) — large values prefer colder tiers.
const LARGE_VALUE_THRESHOLD: u64 = 1_048_576;

impl TieringAdvisor {
    /// Create a new advisor with the given configuration.
    pub fn new(config: AdvisorConfig) -> Self {
        Self { config }
    }

    /// Analyze a workload profile and produce a tiering recommendation.
    ///
    /// The heuristics are:
    /// - Keys accessed >10 times/hour → Memory
    /// - Keys accessed 1–10 times/hour → Mmap
    /// - Keys accessed <1 time/hour but recently active → Disk
    /// - Keys not accessed in 7+ days → Cloud
    /// - Large values (>1 MB) are biased toward Disk/Cloud regardless of access
    pub fn analyze(&self, workload: &WorkloadProfile) -> TieringRecommendation {
        let mut recommended = TierDistribution::default();
        let mut movements = Vec::new();
        let mut reasoning = Vec::new();
        let mut warnings = Vec::new();

        if workload.total_keys == 0 {
            warnings.push("Empty workload — no keys to analyze".to_string());
            return TieringRecommendation {
                recommended_distribution: recommended,
                key_movements: movements,
                estimated_savings: SavingsEstimate::default(),
                confidence: 1.0,
                reasoning,
                warnings,
                generated_at: Utc::now(),
            };
        }

        // --- Derive hourly access rate ------------------------------------------------
        let avg_hourly_access = self.average_hourly_accesses(workload);

        // --- Classify keys by access temperature --------------------------------------
        let hot_keys = (workload.hot_key_ratio * workload.total_keys as f64) as u64;
        let cold_keys = workload.total_keys.saturating_sub(hot_keys);

        // Estimate how many hot keys fall into each temperature bucket
        let (memory_keys, mmap_keys, disk_keys, cloud_keys) =
            self.classify_keys(workload, hot_keys, cold_keys, avg_hourly_access);

        let avg_entry_bytes = workload
            .avg_key_size_bytes
            .saturating_add(workload.avg_value_size_bytes);

        // Apply large-value bias: if avg value > threshold, shift hot keys to mmap/disk
        let (memory_keys, mmap_keys, disk_keys, cloud_keys) =
            if workload.avg_value_size_bytes > LARGE_VALUE_THRESHOLD {
                let shifted = memory_keys / 2;
                reasoning.push(format!(
                    "Large average value size ({} bytes) — shifting {} keys from memory to mmap",
                    workload.avg_value_size_bytes, shifted
                ));
                (
                    memory_keys.saturating_sub(shifted),
                    mmap_keys + shifted,
                    disk_keys,
                    cloud_keys,
                )
            } else {
                (memory_keys, mmap_keys, disk_keys, cloud_keys)
            };

        recommended.memory_keys = memory_keys;
        recommended.memory_bytes = memory_keys * avg_entry_bytes;
        recommended.mmap_keys = mmap_keys;
        recommended.mmap_bytes = mmap_keys * avg_entry_bytes;
        recommended.disk_keys = disk_keys;
        recommended.disk_bytes = disk_keys * avg_entry_bytes;
        recommended.cloud_keys = cloud_keys;
        recommended.cloud_bytes = cloud_keys * avg_entry_bytes;

        // --- Generate key movements ---------------------------------------------------
        self.generate_movements(
            workload,
            &recommended,
            avg_entry_bytes,
            &mut movements,
            &mut reasoning,
        );

        // --- Compute confidence -------------------------------------------------------
        let confidence = self.compute_confidence(workload);
        if confidence < self.config.confidence_threshold {
            warnings.push(format!(
                "Confidence {:.2} is below threshold {:.2} — recommendation may be unreliable",
                confidence, self.config.confidence_threshold
            ));
        }

        // --- Estimate savings ---------------------------------------------------------
        let estimated_savings = self.estimate_savings(&workload.current_distribution, &recommended);

        reasoning.push(format!(
            "Analyzed {} keys with {:.1}% hot key ratio",
            workload.total_keys,
            workload.hot_key_ratio * 100.0
        ));
        reasoning.push(format!(
            "Recommended distribution: {} memory, {} mmap, {} disk, {} cloud",
            memory_keys, mmap_keys, disk_keys, cloud_keys
        ));

        info!(
            total_keys = workload.total_keys,
            confidence = %format!("{:.2}", confidence),
            savings_pct = %format!("{:.1}", estimated_savings.savings_percent),
            "Tiering advisor analysis complete"
        );

        TieringRecommendation {
            recommended_distribution: recommended,
            key_movements: movements,
            estimated_savings,
            confidence,
            reasoning,
            warnings,
            generated_at: Utc::now(),
        }
    }

    /// Apply budget constraints to a recommendation.
    ///
    /// If the recommended distribution exceeds the budget, keys are
    /// progressively shifted from expensive tiers (memory) to cheaper
    /// tiers (disk, cloud) until the budget is satisfied.
    pub fn apply_budget_constraints(
        &self,
        rec: &mut TieringRecommendation,
        budget: &BudgetConstraint,
    ) {
        let cost = self.compute_cost(&rec.recommended_distribution, budget);

        if cost <= budget.max_monthly_cost_usd {
            debug!(
                cost = %format!("${:.2}", cost),
                budget = %format!("${:.2}", budget.max_monthly_cost_usd),
                "Recommendation within budget"
            );
            return;
        }

        rec.reasoning.push(format!(
            "Budget constraint active: estimated ${:.2}/mo exceeds ${:.2}/mo limit",
            cost, budget.max_monthly_cost_usd
        ));

        // Iteratively move keys from memory → mmap → disk → cloud until budget met
        let dist = &mut rec.recommended_distribution;

        // Phase 1: memory → mmap
        self.shift_keys_for_budget(dist, TierName::Memory, TierName::Mmap, budget);

        // Phase 2: mmap → disk
        if self.compute_cost(dist, budget) > budget.max_monthly_cost_usd {
            self.shift_keys_for_budget(dist, TierName::Mmap, TierName::Disk, budget);
        }

        // Phase 3: disk → cloud
        if self.compute_cost(dist, budget) > budget.max_monthly_cost_usd {
            self.shift_keys_for_budget(dist, TierName::Disk, TierName::Cloud, budget);
        }

        let final_cost = self.compute_cost(dist, budget);
        if final_cost > budget.max_monthly_cost_usd {
            rec.warnings.push(format!(
                "Cannot satisfy budget ${:.2}/mo — minimum achievable is ${:.2}/mo",
                budget.max_monthly_cost_usd, final_cost
            ));
        }

        // Recalculate savings
        rec.estimated_savings = SavingsEstimate {
            recommended_monthly_cost_usd: final_cost,
            savings_usd: rec.estimated_savings.current_monthly_cost_usd - final_cost,
            savings_percent: if rec.estimated_savings.current_monthly_cost_usd > 0.0 {
                ((rec.estimated_savings.current_monthly_cost_usd - final_cost)
                    / rec.estimated_savings.current_monthly_cost_usd)
                    * 100.0
            } else {
                0.0
            },
            ..rec.estimated_savings.clone()
        };

        warn!(
            final_cost = %format!("${:.2}", final_cost),
            "Budget constraints applied to recommendation"
        );
    }

    /// Apply SLA constraints to a recommendation.
    ///
    /// Ensures that enough keys remain in low-latency tiers to satisfy
    /// the p99 and p99.9 latency targets. Hot keys are promoted back to
    /// memory or mmap if the current distribution would violate the SLA.
    pub fn apply_sla_constraints(&self, rec: &mut TieringRecommendation, sla: &SlaConstraint) {
        let dist = &mut rec.recommended_distribution;
        let total_keys = dist.total_keys();
        if total_keys == 0 {
            return;
        }

        // p99: 99% of accesses must be under max_p99_latency_ms
        // If cloud latency exceeds p99, those keys must not serve the p99 path.
        // We ensure at least 99% of keys are in tiers meeting p99.
        let p99_fraction = 0.99;
        let min_fast_keys = (total_keys as f64 * p99_fraction).ceil() as u64;
        let fast_keys = self.keys_meeting_latency(dist, sla.max_p99_latency_ms);

        if fast_keys < min_fast_keys {
            let deficit = min_fast_keys - fast_keys;
            rec.reasoning.push(format!(
                "SLA p99 constraint: promoting {} keys to meet {:.1}ms latency target",
                deficit, sla.max_p99_latency_ms
            ));
            self.promote_keys_for_latency(dist, deficit, sla.max_p99_latency_ms);
        }

        // p99.9: ensure 99.9% of keys are in tiers meeting p999
        let p999_fraction = 0.999;
        let min_fast_keys_999 = (total_keys as f64 * p999_fraction).ceil() as u64;
        let fast_keys_999 = self.keys_meeting_latency(dist, sla.max_p999_latency_ms);

        if fast_keys_999 < min_fast_keys_999 {
            let deficit = min_fast_keys_999 - fast_keys_999;
            rec.reasoning.push(format!(
                "SLA p99.9 constraint: promoting {} keys to meet {:.1}ms latency target",
                deficit, sla.max_p999_latency_ms
            ));
            self.promote_keys_for_latency(dist, deficit, sla.max_p999_latency_ms);
        }

        // Availability: if min_availability > 0.999, add warning for cloud tier
        if sla.min_availability > 0.999 && dist.cloud_keys > 0 {
            rec.warnings.push(format!(
                "Cloud tier ({} keys) may not meet {:.4} availability target — consider replication",
                dist.cloud_keys, sla.min_availability
            ));
        }

        debug!(
            p99_ms = sla.max_p99_latency_ms,
            p999_ms = sla.max_p999_latency_ms,
            "SLA constraints applied"
        );
    }

    /// Estimate cost savings between two tier distributions.
    ///
    /// Uses default per-GB costs: memory $10/GB, mmap $5/GB, disk $0.15/GB,
    /// cloud $0.023/GB.
    pub fn estimate_savings(
        &self,
        current: &TierDistribution,
        recommended: &TierDistribution,
    ) -> SavingsEstimate {
        let default_budget = BudgetConstraint::default();
        let current_cost = self.compute_cost(current, &default_budget);
        let recommended_cost = self.compute_cost(recommended, &default_budget);
        let savings = current_cost - recommended_cost;
        let percent = if current_cost > 0.0 {
            (savings / current_cost) * 100.0
        } else {
            0.0
        };

        SavingsEstimate {
            current_monthly_cost_usd: current_cost,
            recommended_monthly_cost_usd: recommended_cost,
            savings_usd: savings,
            savings_percent: percent,
        }
    }

    /// Generate an ordered migration plan from a recommendation.
    ///
    /// Steps are ordered so that promotions (cold → hot) execute before
    /// demotions (hot → cold) to avoid temporary capacity gaps.
    /// Movements within the same direction can run in parallel.
    pub fn generate_migration_plan(&self, rec: &TieringRecommendation) -> MigrationPlan {
        let mut movements = rec.key_movements.clone();
        if movements.len() > self.config.max_migrations_per_cycle {
            movements.truncate(self.config.max_migrations_per_cycle);
        }

        // Sort: promotions first (lower to_tier latency), then demotions
        movements.sort_by(|a, b| {
            let a_promote = a.to_tier.typical_latency_ms() < a.from_tier.typical_latency_ms();
            let b_promote = b.to_tier.typical_latency_ms() < b.from_tier.typical_latency_ms();
            b_promote.cmp(&a_promote)
        });

        let mut steps = Vec::with_capacity(movements.len());
        let mut total_bytes = 0u64;

        for (i, movement) in movements.into_iter().enumerate() {
            total_bytes = total_bytes.saturating_add(movement.estimated_bytes);

            // Movements of the same direction can run in parallel
            let can_parallel = if i + 1 < rec.key_movements.len() {
                let next = &rec.key_movements[i + 1];
                let this_is_promo =
                    movement.to_tier.typical_latency_ms() < movement.from_tier.typical_latency_ms();
                let next_is_promo =
                    next.to_tier.typical_latency_ms() < next.from_tier.typical_latency_ms();
                this_is_promo == next_is_promo
            } else {
                false
            };

            steps.push(MigrationStep {
                order: (i + 1) as u32,
                movement,
                can_parallel,
            });
        }

        // Estimate duration: ~100 MB/s for local moves, ~10 MB/s for cloud
        let estimated_duration_secs = if total_bytes == 0 {
            0
        } else {
            let mb = total_bytes as f64 / (1024.0 * 1024.0);
            // Blend of local and cloud speeds
            let avg_speed_mb_s = 50.0;
            (mb / avg_speed_mb_s).ceil() as u64
        };

        MigrationPlan {
            steps,
            total_bytes_to_move: total_bytes,
            estimated_duration_secs,
        }
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Compute average hourly access rate across all reported hours.
    fn average_hourly_accesses(&self, workload: &WorkloadProfile) -> f64 {
        if workload.access_patterns.is_empty() {
            return 0.0;
        }
        let total: u64 = workload
            .access_patterns
            .iter()
            .map(|p| p.reads + p.writes)
            .sum();
        total as f64 / workload.access_patterns.len() as f64
    }

    /// Classify keys into temperature buckets.
    fn classify_keys(
        &self,
        workload: &WorkloadProfile,
        hot_keys: u64,
        cold_keys: u64,
        avg_hourly: f64,
    ) -> (u64, u64, u64, u64) {
        if workload.total_keys == 0 {
            return (0, 0, 0, 0);
        }

        // Hot keys split between memory and mmap based on access rate
        let (memory_keys, mmap_keys) = if avg_hourly > HOT_ACCESS_THRESHOLD {
            // Very hot — most hot keys go to memory
            let memory = (hot_keys as f64 * 0.8).ceil() as u64;
            (memory, hot_keys.saturating_sub(memory))
        } else if avg_hourly > WARM_ACCESS_THRESHOLD {
            // Warm — split evenly
            let memory = (hot_keys as f64 * 0.3).ceil() as u64;
            (memory, hot_keys.saturating_sub(memory))
        } else {
            // Low overall access — minimal memory
            let memory = (hot_keys as f64 * 0.1).ceil() as u64;
            (memory, hot_keys.saturating_sub(memory))
        };

        // Cold keys split between disk and cloud
        // Use hot_key_ratio as proxy: very cold workloads have more cloud candidates
        let cloud_ratio = if workload.hot_key_ratio < 0.1 {
            0.7 // Mostly cold → heavy cloud
        } else if workload.hot_key_ratio < 0.3 {
            0.4
        } else {
            0.2 // Warm workload → less cloud
        };

        let cloud_keys = (cold_keys as f64 * cloud_ratio).ceil() as u64;
        let disk_keys = cold_keys.saturating_sub(cloud_keys);

        (memory_keys, mmap_keys, disk_keys, cloud_keys)
    }

    /// Generate key movement entries by comparing current vs recommended.
    fn generate_movements(
        &self,
        workload: &WorkloadProfile,
        recommended: &TierDistribution,
        avg_entry_bytes: u64,
        movements: &mut Vec<KeyMovement>,
        reasoning: &mut Vec<String>,
    ) {
        let current = &workload.current_distribution;
        let tiers = [
            (TierName::Memory, "hot:*"),
            (TierName::Mmap, "warm:*"),
            (TierName::Disk, "cold:*"),
            (TierName::Cloud, "archive:*"),
        ];

        for &(tier, pattern) in &tiers {
            let cur_keys = current.keys_for_tier(tier);
            let rec_keys = recommended.keys_for_tier(tier);

            if rec_keys > cur_keys {
                // Need to move keys INTO this tier — find source
                let deficit = rec_keys - cur_keys;
                if let Some((source_tier, reason)) = self.find_source_tier(tier, current) {
                    movements.push(KeyMovement {
                        pattern: pattern.to_string(),
                        from_tier: source_tier,
                        to_tier: tier,
                        estimated_keys: deficit,
                        estimated_bytes: deficit * avg_entry_bytes,
                        reason: reason.clone(),
                    });
                    reasoning.push(format!(
                        "Move {} keys from {} to {}: {}",
                        deficit, source_tier, tier, reason
                    ));
                }
            } else if cur_keys > rec_keys {
                // Need to move keys OUT of this tier — find destination
                let surplus = cur_keys - rec_keys;
                if let Some((dest_tier, reason)) = self.find_dest_tier(tier, recommended) {
                    movements.push(KeyMovement {
                        pattern: pattern.to_string(),
                        from_tier: tier,
                        to_tier: dest_tier,
                        estimated_keys: surplus,
                        estimated_bytes: surplus * avg_entry_bytes,
                        reason: reason.clone(),
                    });
                    reasoning.push(format!(
                        "Move {} keys from {} to {}: {}",
                        surplus, tier, dest_tier, reason
                    ));
                }
            }
        }
    }

    /// Find the best source tier to pull keys from when promoting.
    fn find_source_tier(
        &self,
        target: TierName,
        current: &TierDistribution,
    ) -> Option<(TierName, String)> {
        // Pull from the next colder tier with available keys
        let order = [
            TierName::Cloud,
            TierName::Disk,
            TierName::Mmap,
            TierName::Memory,
        ];
        for &tier in &order {
            if tier != target
                && tier.typical_latency_ms() > target.typical_latency_ms()
                && current.keys_for_tier(tier) > 0
            {
                return Some((
                    tier,
                    format!(
                        "Promoting frequently accessed keys from {} to {}",
                        tier, target
                    ),
                ));
            }
        }
        None
    }

    /// Find the best destination tier for demoted keys.
    fn find_dest_tier(
        &self,
        source: TierName,
        _recommended: &TierDistribution,
    ) -> Option<(TierName, String)> {
        // Push to the next colder tier
        let order = [
            TierName::Memory,
            TierName::Mmap,
            TierName::Disk,
            TierName::Cloud,
        ];
        for &tier in &order {
            if tier.typical_latency_ms() > source.typical_latency_ms() {
                return Some((
                    tier,
                    format!(
                        "Demoting infrequently accessed keys from {} to {}",
                        source, tier
                    ),
                ));
            }
        }
        None
    }

    /// Compute total monthly cost for a distribution.
    fn compute_cost(&self, dist: &TierDistribution, budget: &BudgetConstraint) -> f64 {
        let bytes_to_gb = |b: u64| b as f64 / (1024.0 * 1024.0 * 1024.0);
        bytes_to_gb(dist.memory_bytes) * budget.memory_cost_per_gb_usd
            + bytes_to_gb(dist.mmap_bytes) * budget.memory_cost_per_gb_usd * 0.5
            + bytes_to_gb(dist.disk_bytes) * budget.disk_cost_per_gb_usd
            + bytes_to_gb(dist.cloud_bytes) * budget.cloud_cost_per_gb_usd
    }

    /// Shift keys from an expensive tier to a cheaper one to meet budget.
    fn shift_keys_for_budget(
        &self,
        dist: &mut TierDistribution,
        from: TierName,
        to: TierName,
        budget: &BudgetConstraint,
    ) {
        let total_bytes = dist.total_bytes();
        if total_bytes == 0 {
            return;
        }

        let from_keys = dist.keys_for_tier(from);
        let from_bytes = dist.bytes_for_tier(from);
        if from_keys == 0 {
            return;
        }

        let avg_bytes_per_key = if from_keys > 0 {
            from_bytes / from_keys
        } else {
            return;
        };

        // Binary-search style: shift keys until under budget
        let mut to_shift = from_keys / 2;
        let mut best_shift = 0u64;

        while to_shift > 0 {
            let mut trial = dist.clone();
            let shift = best_shift + to_shift;
            let shift = shift.min(trial.keys_for_tier(from));
            let shift_bytes = shift * avg_bytes_per_key;

            trial.set_keys(from, trial.keys_for_tier(from).saturating_sub(shift));
            trial.set_bytes(from, trial.bytes_for_tier(from).saturating_sub(shift_bytes));
            trial.set_keys(to, trial.keys_for_tier(to) + shift);
            trial.set_bytes(to, trial.bytes_for_tier(to) + shift_bytes);

            if self.compute_cost(&trial, budget) <= budget.max_monthly_cost_usd {
                // This shift was enough
                best_shift = shift.saturating_sub(to_shift);
            } else {
                best_shift += to_shift;
            }
            to_shift /= 2;
        }

        // Apply best_shift
        if best_shift > 0 {
            let shift = best_shift.min(dist.keys_for_tier(from));
            let shift_bytes = shift * avg_bytes_per_key;
            dist.set_keys(from, dist.keys_for_tier(from).saturating_sub(shift));
            dist.set_bytes(from, dist.bytes_for_tier(from).saturating_sub(shift_bytes));
            dist.set_keys(to, dist.keys_for_tier(to) + shift);
            dist.set_bytes(to, dist.bytes_for_tier(to) + shift_bytes);
        }
    }

    /// Count keys in tiers that meet the given latency target.
    fn keys_meeting_latency(&self, dist: &TierDistribution, max_latency_ms: f64) -> u64 {
        let mut count = 0;
        let tiers = [
            (TierName::Memory, dist.memory_keys),
            (TierName::Mmap, dist.mmap_keys),
            (TierName::Disk, dist.disk_keys),
            (TierName::Cloud, dist.cloud_keys),
        ];
        for (tier, keys) in tiers {
            if tier.typical_latency_ms() <= max_latency_ms {
                count += keys;
            }
        }
        count
    }

    /// Promote keys from cold tiers to meet latency requirements.
    fn promote_keys_for_latency(
        &self,
        dist: &mut TierDistribution,
        mut deficit: u64,
        max_latency_ms: f64,
    ) {
        // Promote from coldest to warmest
        let cold_tiers = [TierName::Cloud, TierName::Disk, TierName::Mmap];

        for tier in cold_tiers {
            if deficit == 0 {
                break;
            }
            if tier.typical_latency_ms() <= max_latency_ms {
                continue;
            }

            let available = dist.keys_for_tier(tier);
            let to_promote = available.min(deficit);
            if to_promote == 0 {
                continue;
            }

            let bytes_per_key = if available > 0 {
                dist.bytes_for_tier(tier) / available
            } else {
                0
            };
            let promote_bytes = to_promote * bytes_per_key;

            dist.set_keys(tier, available - to_promote);
            dist.set_bytes(
                tier,
                dist.bytes_for_tier(tier).saturating_sub(promote_bytes),
            );

            // Find the hottest tier that meets latency
            let target = if TierName::Memory.typical_latency_ms() <= max_latency_ms {
                TierName::Memory
            } else if TierName::Mmap.typical_latency_ms() <= max_latency_ms {
                TierName::Mmap
            } else {
                TierName::Disk
            };

            dist.set_keys(target, dist.keys_for_tier(target) + to_promote);
            dist.set_bytes(target, dist.bytes_for_tier(target) + promote_bytes);

            deficit -= to_promote;
        }
    }

    /// Compute confidence score based on data quality.
    fn compute_confidence(&self, workload: &WorkloadProfile) -> f64 {
        let mut score: f64 = 1.0;

        // Fewer keys → less statistical significance
        if workload.total_keys < 100 {
            score *= 0.5;
        } else if workload.total_keys < 1000 {
            score *= 0.8;
        }

        // Fewer access pattern data points → less confidence
        if workload.access_patterns.is_empty() {
            score *= 0.4;
        } else if workload.access_patterns.len() < 12 {
            score *= 0.7;
        }

        // Very extreme hot key ratio → suspicious
        if workload.hot_key_ratio > 0.95 || workload.hot_key_ratio < 0.01 {
            score *= 0.8;
        }

        score.clamp(0.0, 1.0)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to build a basic workload profile
    fn make_workload(
        total_keys: u64,
        hot_ratio: f64,
        rw_ratio: f64,
        avg_value_bytes: u64,
    ) -> WorkloadProfile {
        let avg_entry = 64 + avg_value_bytes;
        WorkloadProfile {
            total_keys,
            total_memory_bytes: total_keys * avg_entry,
            hot_key_ratio: hot_ratio,
            read_write_ratio: rw_ratio,
            avg_key_size_bytes: 64,
            avg_value_size_bytes: avg_value_bytes,
            access_patterns: (0..24)
                .map(|h| AccessPattern {
                    hour: h,
                    reads: 500,
                    writes: 50,
                })
                .collect(),
            current_distribution: TierDistribution {
                memory_keys: total_keys,
                memory_bytes: total_keys * avg_entry,
                ..Default::default()
            },
        }
    }

    #[test]
    fn test_analyze_basic_workload() {
        let advisor = TieringAdvisor::new(AdvisorConfig::default());
        let workload = make_workload(10_000, 0.2, 10.0, 256);

        let rec = advisor.analyze(&workload);

        assert!(rec.confidence > 0.0);
        assert!(!rec.reasoning.is_empty());
        assert_eq!(
            rec.recommended_distribution.total_keys(),
            workload.total_keys
        );
        // Should recommend moving cold keys off memory
        assert!(rec.recommended_distribution.memory_keys < workload.total_keys);
    }

    #[test]
    fn test_analyze_empty_workload() {
        let advisor = TieringAdvisor::new(AdvisorConfig::default());
        let workload = WorkloadProfile {
            total_keys: 0,
            total_memory_bytes: 0,
            hot_key_ratio: 0.0,
            read_write_ratio: 0.0,
            avg_key_size_bytes: 0,
            avg_value_size_bytes: 0,
            access_patterns: vec![],
            current_distribution: TierDistribution::default(),
        };

        let rec = advisor.analyze(&workload);

        assert_eq!(rec.recommended_distribution.total_keys(), 0);
        assert!(rec.warnings.iter().any(|w| w.contains("Empty workload")));
        assert_eq!(rec.confidence, 1.0);
    }

    #[test]
    fn test_analyze_all_hot_workload() {
        let advisor = TieringAdvisor::new(AdvisorConfig::default());
        let workload = make_workload(1_000, 1.0, 100.0, 128);

        let rec = advisor.analyze(&workload);

        // All keys are hot — most should stay in memory or mmap
        let fast_keys =
            rec.recommended_distribution.memory_keys + rec.recommended_distribution.mmap_keys;
        assert!(
            fast_keys >= 900,
            "Expected most hot keys in fast tiers, got {}",
            fast_keys
        );
    }

    #[test]
    fn test_analyze_all_cold_workload() {
        let advisor = TieringAdvisor::new(AdvisorConfig::default());
        let workload = WorkloadProfile {
            total_keys: 10_000,
            total_memory_bytes: 10_000 * 320,
            hot_key_ratio: 0.0,
            read_write_ratio: 1.0,
            avg_key_size_bytes: 64,
            avg_value_size_bytes: 256,
            access_patterns: (0..24)
                .map(|h| AccessPattern {
                    hour: h,
                    reads: 1,
                    writes: 0,
                })
                .collect(),
            current_distribution: TierDistribution {
                memory_keys: 10_000,
                memory_bytes: 10_000 * 320,
                ..Default::default()
            },
        };

        let rec = advisor.analyze(&workload);

        // All cold — should recommend mostly disk/cloud
        let cold_keys =
            rec.recommended_distribution.disk_keys + rec.recommended_distribution.cloud_keys;
        assert!(
            cold_keys > rec.recommended_distribution.memory_keys,
            "Cold workload should have more keys in cold tiers"
        );
    }

    #[test]
    fn test_budget_constraint_reduces_memory() {
        let advisor = TieringAdvisor::new(AdvisorConfig::default());
        let workload = make_workload(100_000, 0.5, 10.0, 1024);

        let mut rec = advisor.analyze(&workload);
        let memory_before = rec.recommended_distribution.memory_keys;

        let tight_budget = BudgetConstraint {
            max_monthly_cost_usd: 1.0,
            memory_cost_per_gb_usd: 10.0,
            disk_cost_per_gb_usd: 0.15,
            cloud_cost_per_gb_usd: 0.023,
        };

        advisor.apply_budget_constraints(&mut rec, &tight_budget);

        // Budget constraint should reduce memory usage
        assert!(
            rec.recommended_distribution.memory_keys <= memory_before,
            "Budget should not increase memory keys"
        );
    }

    #[test]
    fn test_sla_constraint_promotes_keys() {
        let advisor = TieringAdvisor::new(AdvisorConfig::default());

        // Start with everything in cloud
        let workload = WorkloadProfile {
            total_keys: 1_000,
            total_memory_bytes: 1_000 * 320,
            hot_key_ratio: 0.5,
            read_write_ratio: 10.0,
            avg_key_size_bytes: 64,
            avg_value_size_bytes: 256,
            access_patterns: (0..24)
                .map(|h| AccessPattern {
                    hour: h,
                    reads: 100,
                    writes: 10,
                })
                .collect(),
            current_distribution: TierDistribution {
                cloud_keys: 1_000,
                cloud_bytes: 1_000 * 320,
                ..Default::default()
            },
        };

        let mut rec = advisor.analyze(&workload);

        // Force everything into cloud to test SLA promotion
        rec.recommended_distribution = TierDistribution {
            cloud_keys: 1_000,
            cloud_bytes: 1_000 * 320,
            ..Default::default()
        };

        let strict_sla = SlaConstraint {
            max_p99_latency_ms: 1.0,
            max_p999_latency_ms: 10.0,
            min_availability: 0.9999,
        };

        advisor.apply_sla_constraints(&mut rec, &strict_sla);

        // SLA should have promoted keys out of cloud
        assert!(
            rec.recommended_distribution.cloud_keys < 1_000,
            "SLA constraint should promote keys from cloud"
        );
        // At least 99% should be in tiers with ≤1ms latency (memory or mmap or disk)
        let fast_keys = rec.recommended_distribution.memory_keys
            + rec.recommended_distribution.mmap_keys
            + rec.recommended_distribution.disk_keys;
        assert!(
            fast_keys >= 990,
            "Expected ≥990 keys in fast tiers, got {}",
            fast_keys
        );
    }

    #[test]
    fn test_estimate_savings() {
        let advisor = TieringAdvisor::new(AdvisorConfig::default());

        let current = TierDistribution {
            memory_keys: 10_000,
            memory_bytes: 10 * 1024 * 1024 * 1024, // 10 GB in memory
            ..Default::default()
        };

        let recommended = TierDistribution {
            memory_keys: 2_000,
            memory_bytes: 2 * 1024 * 1024 * 1024,
            disk_keys: 6_000,
            disk_bytes: 6 * 1024 * 1024 * 1024,
            cloud_keys: 2_000,
            cloud_bytes: 2 * 1024 * 1024 * 1024,
            ..Default::default()
        };

        let savings = advisor.estimate_savings(&current, &recommended);

        assert!(savings.current_monthly_cost_usd > 0.0);
        assert!(savings.recommended_monthly_cost_usd < savings.current_monthly_cost_usd);
        assert!(savings.savings_usd > 0.0);
        assert!(savings.savings_percent > 0.0);
    }

    #[test]
    fn test_generate_migration_plan() {
        let advisor = TieringAdvisor::new(AdvisorConfig::default());
        let workload = make_workload(10_000, 0.3, 10.0, 512);

        let rec = advisor.analyze(&workload);
        let plan = advisor.generate_migration_plan(&rec);

        // Plan should have steps if there are movements
        if !rec.key_movements.is_empty() {
            assert!(!plan.steps.is_empty());
            // Steps should be ordered
            for (i, step) in plan.steps.iter().enumerate() {
                assert_eq!(step.order, (i + 1) as u32);
            }
        }

        // Total bytes should match sum of step bytes
        let step_bytes: u64 = plan.steps.iter().map(|s| s.movement.estimated_bytes).sum();
        assert_eq!(plan.total_bytes_to_move, step_bytes);
    }

    #[test]
    fn test_migration_plan_respects_max_per_cycle() {
        let config = AdvisorConfig {
            max_migrations_per_cycle: 2,
            ..Default::default()
        };
        let advisor = TieringAdvisor::new(config);
        let workload = make_workload(100_000, 0.2, 10.0, 512);

        let rec = advisor.analyze(&workload);
        let plan = advisor.generate_migration_plan(&rec);

        assert!(plan.steps.len() <= 2);
    }

    #[test]
    fn test_tier_distribution_helpers() {
        let mut dist = TierDistribution::default();
        dist.set_keys(TierName::Memory, 100);
        dist.set_bytes(TierName::Memory, 1024);
        dist.set_keys(TierName::Cloud, 200);
        dist.set_bytes(TierName::Cloud, 2048);

        assert_eq!(dist.keys_for_tier(TierName::Memory), 100);
        assert_eq!(dist.bytes_for_tier(TierName::Cloud), 2048);
        assert_eq!(dist.total_keys(), 300);
        assert_eq!(dist.total_bytes(), 3072);
    }

    #[test]
    fn test_tier_name_display() {
        assert_eq!(TierName::Memory.to_string(), "memory");
        assert_eq!(TierName::Mmap.to_string(), "mmap");
        assert_eq!(TierName::Disk.to_string(), "disk");
        assert_eq!(TierName::Cloud.to_string(), "cloud");
    }

    #[test]
    fn test_confidence_low_with_few_keys() {
        let advisor = TieringAdvisor::new(AdvisorConfig::default());
        let workload = make_workload(10, 0.5, 5.0, 128);

        let rec = advisor.analyze(&workload);

        // Low key count → low confidence
        assert!(rec.confidence < 0.7);
    }

    #[test]
    fn test_savings_estimate_no_cost() {
        let advisor = TieringAdvisor::new(AdvisorConfig::default());
        let empty = TierDistribution::default();

        let savings = advisor.estimate_savings(&empty, &empty);

        assert_eq!(savings.current_monthly_cost_usd, 0.0);
        assert_eq!(savings.savings_usd, 0.0);
        assert_eq!(savings.savings_percent, 0.0);
    }

    #[test]
    fn test_large_value_bias() {
        let advisor = TieringAdvisor::new(AdvisorConfig::default());

        let small_values = make_workload(10_000, 0.5, 10.0, 256);
        let large_values = make_workload(10_000, 0.5, 10.0, 2_000_000);

        let rec_small = advisor.analyze(&small_values);
        let rec_large = advisor.analyze(&large_values);

        // Large values should result in fewer memory keys
        assert!(
            rec_large.recommended_distribution.memory_keys
                <= rec_small.recommended_distribution.memory_keys,
            "Large values should bias away from memory"
        );
    }
}
