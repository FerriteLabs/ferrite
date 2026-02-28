//! Cost-Aware Auto-Scaling for Ferrite Clusters
//!
//! Makes scaling decisions that balance performance requirements against cloud
//! infrastructure costs. The autoscaler considers CPU, memory, latency, and
//! tier utilization metrics alongside configurable cost parameters to find
//! the optimal replica count.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────┐    ┌──────────────────────┐    ┌─────────────────┐
//! │ MetricsSnapshot  │───▶│ CostAwareAutoscaler  │───▶│ ScalingDecision │
//! │  (CPU, mem, p99, │    │  (evaluate window,   │    │  (direction,    │
//! │   tiers, conns)  │    │   cost constraints)  │    │   cost delta,   │
//! └──────────────────┘    └──────────┬───────────┘    │   confidence)   │
//!                                    │                └─────────────────┘
//!                                    ▼
//!                           ┌─────────────────┐
//!                           │  CostEstimate   │
//!                           │  (compute,      │
//!                           │   storage, net) │
//!                           └─────────────────┘
//! ```

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the cost-aware autoscaler.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutoscalerConfig {
    /// Whether the autoscaler is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Minimum number of replicas.
    #[serde(default = "default_min_replicas")]
    pub min_replicas: u32,

    /// Maximum number of replicas.
    #[serde(default = "default_max_replicas")]
    pub max_replicas: u32,

    /// Target CPU utilisation percentage (0–100).
    #[serde(default = "default_target_cpu_pct")]
    pub target_cpu_pct: f64,

    /// Target memory utilisation percentage (0–100).
    #[serde(default = "default_target_memory_pct")]
    pub target_memory_pct: f64,

    /// Target p99 latency in microseconds.
    #[serde(default = "default_target_p99_latency_us")]
    pub target_p99_latency_us: u64,

    /// Scale-up threshold – scale up when metric exceeds `target * threshold`.
    #[serde(default = "default_scale_up_threshold")]
    pub scale_up_threshold: f64,

    /// Scale-down threshold – scale down when metric falls below `target * threshold`.
    #[serde(default = "default_scale_down_threshold")]
    pub scale_down_threshold: f64,

    /// Cooldown period in seconds after a scaling action.
    #[serde(default = "default_cooldown_seconds")]
    pub cooldown_seconds: u64,

    /// Number of metric snapshots to keep in the sliding window.
    #[serde(default = "default_metrics_window_size")]
    pub metrics_window_size: usize,

    /// Hourly cost per replica in USD.
    #[serde(default = "default_cost_per_replica_hour")]
    pub cost_per_replica_hour: f64,

    /// Monthly cost per GB of storage in USD.
    #[serde(default = "default_cost_per_gb_storage")]
    pub cost_per_gb_storage: f64,

    /// When `true`, the autoscaler aggressively favours cost savings over
    /// performance headroom.
    #[serde(default)]
    pub prefer_cost_optimization: bool,
}

fn default_true() -> bool {
    true
}
fn default_min_replicas() -> u32 {
    1
}
fn default_max_replicas() -> u32 {
    10
}
fn default_target_cpu_pct() -> f64 {
    70.0
}
fn default_target_memory_pct() -> f64 {
    80.0
}
fn default_target_p99_latency_us() -> u64 {
    5000
}
fn default_scale_up_threshold() -> f64 {
    0.8
}
fn default_scale_down_threshold() -> f64 {
    0.3
}
fn default_cooldown_seconds() -> u64 {
    300
}
fn default_metrics_window_size() -> usize {
    60
}
fn default_cost_per_replica_hour() -> f64 {
    0.50
}
fn default_cost_per_gb_storage() -> f64 {
    0.10
}

impl Default for AutoscalerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_replicas: 1,
            max_replicas: 10,
            target_cpu_pct: 70.0,
            target_memory_pct: 80.0,
            target_p99_latency_us: 5000,
            scale_up_threshold: 0.8,
            scale_down_threshold: 0.3,
            cooldown_seconds: 300,
            metrics_window_size: 60,
            cost_per_replica_hour: 0.50,
            cost_per_gb_storage: 0.10,
            prefer_cost_optimization: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Metrics & decisions
// ---------------------------------------------------------------------------

/// A point-in-time snapshot of cluster metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricsSnapshot {
    /// Unix timestamp in seconds.
    pub timestamp: u64,
    /// CPU utilisation percentage (0–100).
    pub cpu_utilization_pct: f64,
    /// Memory utilisation percentage (0–100).
    pub memory_utilization_pct: f64,
    /// Throughput in operations per second.
    pub ops_per_second: f64,
    /// p99 latency in microseconds.
    pub p99_latency_us: u64,
    /// Number of active client connections.
    pub active_connections: u32,
    /// Percentage of data residing in the memory tier.
    pub tier_memory_pct: f64,
    /// Percentage of data residing in the disk tier.
    pub tier_disk_pct: f64,
    /// Current replica count.
    pub current_replicas: u32,
    /// Number of pending data migrations.
    pub pending_migrations: u32,
}

/// The direction of a scaling action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScaleDirection {
    /// Increase replica count.
    ScaleUp,
    /// Decrease replica count.
    ScaleDown,
    /// No scaling change required.
    NoChange,
}

/// The reason behind a scaling decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScalingReason {
    /// CPU utilisation exceeded the scale-up threshold.
    HighCpuUtilization(f64),
    /// CPU utilisation fell below the scale-down threshold.
    LowCpuUtilization(f64),
    /// Memory pressure exceeded the scale-up threshold.
    HighMemoryPressure(f64),
    /// Memory usage fell below the scale-down threshold.
    LowMemoryUsage(f64),
    /// p99 latency exceeded the target.
    HighLatency(u64),
    /// Active connections exceeded a sustainable level.
    ConnectionPressure(u32),
    /// Cost optimisation triggered the decision.
    CostOptimization,
    /// Predictive model forecasted increased load.
    PredictiveScaleUp,
    /// Manual override.
    Manual(String),
}

/// A scaling decision produced by the autoscaler.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScalingDecision {
    /// Unix timestamp when the decision was made.
    pub timestamp: u64,
    /// Direction of the scaling action.
    pub direction: ScaleDirection,
    /// Replica count before the action.
    pub from_replicas: u32,
    /// Replica count after the action.
    pub to_replicas: u32,
    /// Reason for the decision.
    pub reason: ScalingReason,
    /// Estimated change in monthly cost (USD). Positive = cost increase.
    pub estimated_cost_change: f64,
    /// Confidence in the decision (0.0–1.0).
    pub confidence: f64,
    /// Whether the decision has been applied.
    pub applied: bool,
}

/// Estimated monthly infrastructure cost for a given replica count.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CostEstimate {
    /// Number of replicas.
    pub replicas: u32,
    /// Monthly compute cost in USD.
    pub monthly_compute_cost: f64,
    /// Monthly storage cost in USD.
    pub monthly_storage_cost: f64,
    /// Monthly network cost in USD.
    pub monthly_network_cost: f64,
    /// Total monthly cost in USD.
    pub total_monthly_cost: f64,
    /// Cost per million operations in USD.
    pub cost_per_million_ops: f64,
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Atomic counters for autoscaler statistics.
pub struct AutoscalerStats {
    evaluations: AtomicU64,
    scale_ups: AtomicU64,
    scale_downs: AtomicU64,
    cooldown_hits: AtomicU64,
    estimated_monthly_savings: RwLock<f64>,
}

impl AutoscalerStats {
    fn new() -> Self {
        Self {
            evaluations: AtomicU64::new(0),
            scale_ups: AtomicU64::new(0),
            scale_downs: AtomicU64::new(0),
            cooldown_hits: AtomicU64::new(0),
            estimated_monthly_savings: RwLock::new(0.0),
        }
    }
}

/// A point-in-time snapshot of autoscaler statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutoscalerStatsSnapshot {
    /// Total number of evaluations performed.
    pub evaluations: u64,
    /// Number of scale-up decisions.
    pub scale_ups: u64,
    /// Number of scale-down decisions.
    pub scale_downs: u64,
    /// Number of evaluations skipped due to cooldown.
    pub cooldown_hits: u64,
    /// Estimated cumulative monthly savings in USD.
    pub estimated_monthly_savings: f64,
}

// ---------------------------------------------------------------------------
// Autoscaler
// ---------------------------------------------------------------------------

/// Hours in a month (730 h ≈ 365.25 / 12 × 24).
const HOURS_PER_MONTH: f64 = 730.0;

/// Cost-aware autoscaler for Ferrite clusters.
///
/// Combines real-time cluster metrics with cloud pricing information to
/// produce scaling decisions that balance performance and cost.
pub struct CostAwareAutoscaler {
    /// Configuration for the autoscaler.
    config: AutoscalerConfig,
    /// Sliding window of recorded metrics snapshots.
    metrics_history: RwLock<VecDeque<MetricsSnapshot>>,
    /// History of scaling decisions.
    scaling_decisions: RwLock<VecDeque<ScalingDecision>>,
    /// If set, no scaling actions are taken until this unix timestamp.
    cooldown_until: RwLock<Option<u64>>,
    /// Cumulative statistics.
    stats: AutoscalerStats,
}

impl CostAwareAutoscaler {
    /// Create a new autoscaler with the given configuration.
    pub fn new(config: AutoscalerConfig) -> Self {
        Self {
            config,
            metrics_history: RwLock::new(VecDeque::new()),
            scaling_decisions: RwLock::new(VecDeque::new()),
            cooldown_until: RwLock::new(None),
            stats: AutoscalerStats::new(),
        }
    }

    /// Record a new metrics observation.
    ///
    /// Old snapshots beyond `metrics_window_size` are automatically evicted.
    pub fn record_metrics(&self, snapshot: MetricsSnapshot) {
        let mut history = self.metrics_history.write();
        history.push_back(snapshot);
        while history.len() > self.config.metrics_window_size {
            history.pop_front();
        }
    }

    /// Evaluate the current metrics window and return a scaling decision if
    /// one is warranted.
    ///
    /// Returns `None` when the autoscaler is disabled, in cooldown, or when
    /// there are no metrics to evaluate.
    pub fn evaluate(&self) -> Option<ScalingDecision> {
        if !self.config.enabled {
            return None;
        }

        self.stats.evaluations.fetch_add(1, Ordering::Relaxed);

        // --- cooldown check ---
        if self.is_in_cooldown() {
            self.stats.cooldown_hits.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        let history = self.metrics_history.read();
        if history.is_empty() {
            return None;
        }

        let count = history.len() as f64;
        let latest = match history.back() {
            Some(s) => s,
            None => return None,
        };
        let current_replicas = latest.current_replicas;

        // --- compute averages ---
        let avg_cpu: f64 = history.iter().map(|s| s.cpu_utilization_pct).sum::<f64>() / count;
        let avg_mem: f64 = history.iter().map(|s| s.memory_utilization_pct).sum::<f64>() / count;
        let avg_p99: f64 = history.iter().map(|s| s.p99_latency_us as f64).sum::<f64>() / count;
        let avg_ops: f64 = history.iter().map(|s| s.ops_per_second).sum::<f64>() / count;

        // --- compute variance for confidence ---
        let cpu_var = variance(history.iter().map(|s| s.cpu_utilization_pct), avg_cpu);
        let mem_var = variance(history.iter().map(|s| s.memory_utilization_pct), avg_mem);
        let combined_var = (cpu_var + mem_var) / 2.0;
        // Low variance → high confidence.  var = 0 → confidence 1.0
        let confidence = 1.0 / (1.0 + combined_var.sqrt() / 10.0);

        let now = latest.timestamp;

        // --- scale-up checks (any single metric can trigger) ---
        let cpu_up_limit = self.config.target_cpu_pct * self.config.scale_up_threshold;
        if avg_cpu > cpu_up_limit && current_replicas < self.config.max_replicas {
            let to = (current_replicas + 1).min(self.config.max_replicas);
            let cost_delta = self.cost_change(current_replicas, to);
            return Some(ScalingDecision {
                timestamp: now,
                direction: ScaleDirection::ScaleUp,
                from_replicas: current_replicas,
                to_replicas: to,
                reason: ScalingReason::HighCpuUtilization(avg_cpu),
                estimated_cost_change: cost_delta,
                confidence,
                applied: false,
            });
        }

        let mem_up_limit = self.config.target_memory_pct * self.config.scale_up_threshold;
        if avg_mem > mem_up_limit && current_replicas < self.config.max_replicas {
            let to = (current_replicas + 1).min(self.config.max_replicas);
            let cost_delta = self.cost_change(current_replicas, to);
            return Some(ScalingDecision {
                timestamp: now,
                direction: ScaleDirection::ScaleUp,
                from_replicas: current_replicas,
                to_replicas: to,
                reason: ScalingReason::HighMemoryPressure(avg_mem),
                estimated_cost_change: cost_delta,
                confidence,
                applied: false,
            });
        }

        if avg_p99 > self.config.target_p99_latency_us as f64
            && current_replicas < self.config.max_replicas
        {
            let to = (current_replicas + 1).min(self.config.max_replicas);
            let cost_delta = self.cost_change(current_replicas, to);
            return Some(ScalingDecision {
                timestamp: now,
                direction: ScaleDirection::ScaleUp,
                from_replicas: current_replicas,
                to_replicas: to,
                reason: ScalingReason::HighLatency(avg_p99 as u64),
                estimated_cost_change: cost_delta,
                confidence,
                applied: false,
            });
        }

        // --- scale-down check (ALL metrics must be below threshold) ---
        let cpu_down_limit = self.config.target_cpu_pct * self.config.scale_down_threshold;
        let mem_down_limit = self.config.target_memory_pct * self.config.scale_down_threshold;
        let p99_below = avg_p99 < self.config.target_p99_latency_us as f64 * self.config.scale_down_threshold;

        if avg_cpu < cpu_down_limit
            && avg_mem < mem_down_limit
            && p99_below
            && current_replicas > self.config.min_replicas
        {
            let to = (current_replicas - 1).max(self.config.min_replicas);
            let cost_delta = self.cost_change(current_replicas, to);

            let reason = if self.config.prefer_cost_optimization {
                ScalingReason::CostOptimization
            } else {
                ScalingReason::LowCpuUtilization(avg_cpu)
            };

            return Some(ScalingDecision {
                timestamp: now,
                direction: ScaleDirection::ScaleDown,
                from_replicas: current_replicas,
                to_replicas: to,
                reason,
                estimated_cost_change: cost_delta,
                confidence,
                applied: false,
            });
        }

        // --- prefer_cost_optimization: aggressive scale-down ---
        if self.config.prefer_cost_optimization
            && current_replicas > self.config.min_replicas
            && avg_cpu < self.config.target_cpu_pct * 0.5
            && avg_mem < self.config.target_memory_pct * 0.5
        {
            let to = (current_replicas - 1).max(self.config.min_replicas);
            let cost_delta = self.cost_change(current_replicas, to);
            return Some(ScalingDecision {
                timestamp: now,
                direction: ScaleDirection::ScaleDown,
                from_replicas: current_replicas,
                to_replicas: to,
                reason: ScalingReason::CostOptimization,
                estimated_cost_change: cost_delta,
                confidence,
                applied: false,
            });
        }

        None
    }

    /// Record that a scaling decision has been applied and start the cooldown
    /// timer.
    pub fn apply_decision(&self, decision: &ScalingDecision) {
        let now = decision.timestamp;
        *self.cooldown_until.write() = Some(now + self.config.cooldown_seconds);

        match decision.direction {
            ScaleDirection::ScaleUp => {
                self.stats.scale_ups.fetch_add(1, Ordering::Relaxed);
            }
            ScaleDirection::ScaleDown => {
                self.stats.scale_downs.fetch_add(1, Ordering::Relaxed);
                if decision.estimated_cost_change < 0.0 {
                    let mut savings = self.stats.estimated_monthly_savings.write();
                    *savings += decision.estimated_cost_change.abs();
                }
            }
            ScaleDirection::NoChange => {}
        }

        let mut applied = decision.clone();
        applied.applied = true;
        let mut decisions = self.scaling_decisions.write();
        decisions.push_back(applied);
        // Keep a bounded history
        while decisions.len() > 1000 {
            decisions.pop_front();
        }
    }

    /// Returns `true` if the autoscaler is currently in a cooldown period.
    pub fn is_in_cooldown(&self) -> bool {
        let cooldown = self.cooldown_until.read();
        match *cooldown {
            Some(until) => {
                let now = self.current_timestamp();
                now < until
            }
            None => false,
        }
    }

    /// Estimate the monthly infrastructure cost for a given replica count.
    pub fn cost_estimate(&self, replicas: u32) -> CostEstimate {
        let monthly_compute = replicas as f64 * self.config.cost_per_replica_hour * HOURS_PER_MONTH;
        // Estimate 50 GB storage per replica as a baseline.
        let monthly_storage = replicas as f64 * 50.0 * self.config.cost_per_gb_storage;
        // Network cost: rough estimate at 10% of compute.
        let monthly_network = monthly_compute * 0.10;
        let total = monthly_compute + monthly_storage + monthly_network;

        // cost-per-million-ops: use latest observed ops/s if available.
        let ops_per_sec = self
            .metrics_history
            .read()
            .back()
            .map(|s| s.ops_per_second)
            .unwrap_or(1000.0);

        let ops_per_month = ops_per_sec * 3600.0 * HOURS_PER_MONTH;
        let cost_per_million = if ops_per_month > 0.0 {
            total / (ops_per_month / 1_000_000.0)
        } else {
            0.0
        };

        CostEstimate {
            replicas,
            monthly_compute_cost: monthly_compute,
            monthly_storage_cost: monthly_storage,
            monthly_network_cost: monthly_network,
            total_monthly_cost: total,
            cost_per_million_ops: cost_per_million,
        }
    }

    /// Compute the cost-optimal replica count given the current metrics.
    ///
    /// Finds the smallest replica count within `[min_replicas, max_replicas]`
    /// that keeps average CPU below the scale-up threshold.
    pub fn optimal_replicas(&self) -> u32 {
        let history = self.metrics_history.read();

        if history.is_empty() {
            return self.config.min_replicas;
        }

        let current_replicas = match history.back() {
            Some(s) => s.current_replicas.max(1) as f64,
            None => return self.config.min_replicas,
        };
        let avg_cpu: f64 =
            history.iter().map(|s| s.cpu_utilization_pct).sum::<f64>() / history.len() as f64;

        // total_cpu_work = avg_cpu * current_replicas (abstract units)
        let total_work = avg_cpu * current_replicas;
        let target_cpu = self.config.target_cpu_pct * self.config.scale_up_threshold;

        // replicas needed so that per-replica CPU ≤ target
        let needed = if target_cpu > 0.0 {
            (total_work / target_cpu).ceil() as u32
        } else {
            self.config.min_replicas
        };

        needed.clamp(self.config.min_replicas, self.config.max_replicas)
    }

    /// Return a snapshot of current autoscaler statistics.
    pub fn stats(&self) -> AutoscalerStatsSnapshot {
        AutoscalerStatsSnapshot {
            evaluations: self.stats.evaluations.load(Ordering::Relaxed),
            scale_ups: self.stats.scale_ups.load(Ordering::Relaxed),
            scale_downs: self.stats.scale_downs.load(Ordering::Relaxed),
            cooldown_hits: self.stats.cooldown_hits.load(Ordering::Relaxed),
            estimated_monthly_savings: *self.stats.estimated_monthly_savings.read(),
        }
    }

    /// Return the most recent scaling decisions, newest first.
    pub fn recent_decisions(&self, limit: usize) -> Vec<ScalingDecision> {
        let decisions = self.scaling_decisions.read();
        decisions.iter().rev().take(limit).cloned().collect()
    }

    // -- private helpers ----------------------------------------------------

    /// Monthly cost change when going from `from` replicas to `to` replicas.
    fn cost_change(&self, from: u32, to: u32) -> f64 {
        let from_cost = self.cost_estimate(from).total_monthly_cost;
        let to_cost = self.cost_estimate(to).total_monthly_cost;
        to_cost - from_cost
    }

    /// Best-effort current unix timestamp.
    fn current_timestamp(&self) -> u64 {
        let history = self.metrics_history.read();
        history.back().map(|s| s.timestamp).unwrap_or(0)
    }
}

/// Compute the population variance for an iterator of `f64` values.
fn variance(values: impl Iterator<Item = f64>, mean: f64) -> f64 {
    let mut count = 0u64;
    let mut sum_sq = 0.0f64;
    for v in values {
        sum_sq += (v - mean).powi(2);
        count += 1;
    }
    if count == 0 {
        0.0
    } else {
        sum_sq / count as f64
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a metrics snapshot with sensible defaults.
    fn snapshot(
        timestamp: u64,
        cpu: f64,
        mem: f64,
        p99: u64,
        replicas: u32,
    ) -> MetricsSnapshot {
        MetricsSnapshot {
            timestamp,
            cpu_utilization_pct: cpu,
            memory_utilization_pct: mem,
            ops_per_second: 10_000.0,
            p99_latency_us: p99,
            active_connections: 100,
            tier_memory_pct: 80.0,
            tier_disk_pct: 20.0,
            current_replicas: replicas,
            pending_migrations: 0,
        }
    }

    #[test]
    fn test_default_config_values() {
        let config = AutoscalerConfig::default();
        assert!(config.enabled);
        assert_eq!(config.min_replicas, 1);
        assert_eq!(config.max_replicas, 10);
        assert!((config.target_cpu_pct - 70.0).abs() < f64::EPSILON);
        assert!((config.target_memory_pct - 80.0).abs() < f64::EPSILON);
        assert_eq!(config.target_p99_latency_us, 5000);
        assert!((config.scale_up_threshold - 0.8).abs() < f64::EPSILON);
        assert!((config.scale_down_threshold - 0.3).abs() < f64::EPSILON);
        assert_eq!(config.cooldown_seconds, 300);
        assert_eq!(config.metrics_window_size, 60);
        assert!((config.cost_per_replica_hour - 0.50).abs() < f64::EPSILON);
        assert!((config.cost_per_gb_storage - 0.10).abs() < f64::EPSILON);
        assert!(!config.prefer_cost_optimization);
    }

    #[test]
    fn test_record_metrics_and_evaluate_no_change() {
        let autoscaler = CostAwareAutoscaler::new(AutoscalerConfig::default());
        // CPU=30%, mem=30% → well within thresholds, but not low enough for
        // scale-down because p99 isn't below threshold either.
        for i in 0..5 {
            autoscaler.record_metrics(snapshot(1000 + i, 30.0, 30.0, 2000, 3));
        }
        let decision = autoscaler.evaluate();
        assert!(decision.is_none());
    }

    #[test]
    fn test_scale_up_on_high_cpu() {
        let autoscaler = CostAwareAutoscaler::new(AutoscalerConfig::default());
        // CPU=90% > 70 * 0.8 = 56 → should scale up
        for i in 0..5 {
            autoscaler.record_metrics(snapshot(1000 + i, 90.0, 30.0, 1000, 3));
        }
        let decision = autoscaler.evaluate().expect("should recommend scale-up");
        assert_eq!(decision.direction, ScaleDirection::ScaleUp);
        assert_eq!(decision.from_replicas, 3);
        assert_eq!(decision.to_replicas, 4);
        assert!(decision.estimated_cost_change > 0.0);
        assert!(matches!(decision.reason, ScalingReason::HighCpuUtilization(_)));
    }

    #[test]
    fn test_scale_down_on_low_utilization() {
        let autoscaler = CostAwareAutoscaler::new(AutoscalerConfig::default());
        // CPU=5% < 70*0.3=21, mem=5% < 80*0.3=24, p99=100 < 5000*0.3=1500
        for i in 0..5 {
            autoscaler.record_metrics(snapshot(1000 + i, 5.0, 5.0, 100, 3));
        }
        let decision = autoscaler.evaluate().expect("should recommend scale-down");
        assert_eq!(decision.direction, ScaleDirection::ScaleDown);
        assert_eq!(decision.from_replicas, 3);
        assert_eq!(decision.to_replicas, 2);
        assert!(decision.estimated_cost_change < 0.0);
    }

    #[test]
    fn test_cooldown_prevents_rapid_scaling() {
        let autoscaler = CostAwareAutoscaler::new(AutoscalerConfig {
            cooldown_seconds: 600,
            ..AutoscalerConfig::default()
        });

        // First evaluation triggers scale-up
        for i in 0..5 {
            autoscaler.record_metrics(snapshot(1000 + i, 90.0, 30.0, 1000, 3));
        }
        let decision = autoscaler.evaluate().expect("should scale up");
        autoscaler.apply_decision(&decision);
        assert!(autoscaler.is_in_cooldown());

        // Second evaluation (still inside cooldown window) should be blocked
        for i in 0..5 {
            autoscaler.record_metrics(snapshot(1100 + i, 90.0, 30.0, 1000, 4));
        }
        assert!(autoscaler.evaluate().is_none());

        // After cooldown expires
        for i in 0..5 {
            autoscaler.record_metrics(snapshot(1700 + i, 90.0, 30.0, 1000, 4));
        }
        assert!(!autoscaler.is_in_cooldown());
        assert!(autoscaler.evaluate().is_some());
    }

    #[test]
    fn test_cost_estimate_calculation() {
        let autoscaler = CostAwareAutoscaler::new(AutoscalerConfig::default());
        let estimate = autoscaler.cost_estimate(3);

        assert_eq!(estimate.replicas, 3);
        // 3 replicas × $0.50/h × 730 h = $1095.00
        let expected_compute = 3.0 * 0.50 * HOURS_PER_MONTH;
        assert!((estimate.monthly_compute_cost - expected_compute).abs() < 0.01);
        // 3 × 50 GB × $0.10 = $15.00
        let expected_storage = 3.0 * 50.0 * 0.10;
        assert!((estimate.monthly_storage_cost - expected_storage).abs() < 0.01);
        // Network = 10% of compute
        assert!((estimate.monthly_network_cost - expected_compute * 0.10).abs() < 0.01);
        let expected_total = expected_compute + expected_storage + expected_compute * 0.10;
        assert!((estimate.total_monthly_cost - expected_total).abs() < 0.01);
    }

    #[test]
    fn test_optimal_replicas_computation() {
        let autoscaler = CostAwareAutoscaler::new(AutoscalerConfig::default());
        // 3 replicas each at 90% CPU → total_work = 270
        // target = 70 * 0.8 = 56 → ceil(270/56) = 5
        for i in 0..5 {
            autoscaler.record_metrics(snapshot(1000 + i, 90.0, 30.0, 1000, 3));
        }
        let optimal = autoscaler.optimal_replicas();
        assert_eq!(optimal, 5);

        // With very low CPU the optimal should clamp to min_replicas
        let low = CostAwareAutoscaler::new(AutoscalerConfig::default());
        for i in 0..5 {
            low.record_metrics(snapshot(1000 + i, 5.0, 10.0, 100, 3));
        }
        assert_eq!(low.optimal_replicas(), 1);
    }

    #[test]
    fn test_stats_tracking() {
        let autoscaler = CostAwareAutoscaler::new(AutoscalerConfig::default());

        // Trigger a scale-up
        for i in 0..5 {
            autoscaler.record_metrics(snapshot(1000 + i, 90.0, 30.0, 1000, 3));
        }
        let decision = autoscaler.evaluate().unwrap();
        autoscaler.apply_decision(&decision);

        let stats = autoscaler.stats();
        assert_eq!(stats.evaluations, 1);
        assert_eq!(stats.scale_ups, 1);
        assert_eq!(stats.scale_downs, 0);

        // Verify decision was recorded
        let recent = autoscaler.recent_decisions(10);
        assert_eq!(recent.len(), 1);
        assert!(recent[0].applied);
    }

    #[test]
    fn test_scale_up_on_high_memory() {
        let autoscaler = CostAwareAutoscaler::new(AutoscalerConfig::default());
        // mem=85% > 80*0.8=64 → should scale up
        for i in 0..5 {
            autoscaler.record_metrics(snapshot(1000 + i, 20.0, 85.0, 1000, 2));
        }
        let decision = autoscaler.evaluate().expect("should recommend scale-up on memory");
        assert_eq!(decision.direction, ScaleDirection::ScaleUp);
        assert!(matches!(decision.reason, ScalingReason::HighMemoryPressure(_)));
    }

    #[test]
    fn test_scale_up_on_high_latency() {
        let autoscaler = CostAwareAutoscaler::new(AutoscalerConfig::default());
        // p99=6000 > target 5000 → should scale up
        for i in 0..5 {
            autoscaler.record_metrics(snapshot(1000 + i, 20.0, 20.0, 6000, 2));
        }
        let decision = autoscaler.evaluate().expect("should recommend scale-up on latency");
        assert_eq!(decision.direction, ScaleDirection::ScaleUp);
        assert!(matches!(decision.reason, ScalingReason::HighLatency(_)));
    }

    #[test]
    fn test_disabled_autoscaler_returns_none() {
        let autoscaler = CostAwareAutoscaler::new(AutoscalerConfig {
            enabled: false,
            ..AutoscalerConfig::default()
        });
        for i in 0..5 {
            autoscaler.record_metrics(snapshot(1000 + i, 90.0, 90.0, 9000, 3));
        }
        assert!(autoscaler.evaluate().is_none());
    }

    #[test]
    fn test_respects_max_replicas() {
        let autoscaler = CostAwareAutoscaler::new(AutoscalerConfig {
            max_replicas: 5,
            ..AutoscalerConfig::default()
        });
        // Already at max
        for i in 0..5 {
            autoscaler.record_metrics(snapshot(1000 + i, 95.0, 30.0, 1000, 5));
        }
        assert!(autoscaler.evaluate().is_none());
    }

    #[test]
    fn test_respects_min_replicas() {
        let autoscaler = CostAwareAutoscaler::new(AutoscalerConfig {
            min_replicas: 3,
            ..AutoscalerConfig::default()
        });
        for i in 0..5 {
            autoscaler.record_metrics(snapshot(1000 + i, 5.0, 5.0, 100, 3));
        }
        // At min_replicas, scale-down should return None
        assert!(autoscaler.evaluate().is_none());
    }
}
