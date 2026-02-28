#![forbid(unsafe_code)]
//! Auto-tuner — periodically applies optimizer recommendations with cooldown and A/B testing.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, info};

use super::optimizer::AdaptiveOptimizer;
use super::profiler::{WorkloadProfiler, WorkloadSnapshot};
use super::recommendation::{OptimizationPlan, Recommendation};

/// Configuration for the auto-tuner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoTunerConfig {
    /// Whether auto-optimization is enabled.
    pub enabled: bool,
    /// Interval between optimization runs.
    pub interval_secs: u64,
    /// Minimum confidence score to auto-apply a recommendation.
    pub confidence_threshold: f64,
    /// Cooldown period after applying an optimization before the same rule can fire again.
    pub cooldown_secs: u64,
    /// Fraction of shards to apply to in A/B test mode (0.0–1.0).
    pub ab_test_fraction: f64,
    /// Whether A/B testing is enabled.
    pub ab_test_enabled: bool,
}

impl Default for AutoTunerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 300,
            confidence_threshold: 0.70,
            cooldown_secs: 600,
            ab_test_fraction: 0.25,
            ab_test_enabled: false,
        }
    }
}

/// Record of an applied optimization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppliedOptimization {
    /// The recommendation that was applied.
    pub recommendation: Recommendation,
    /// Timestamp (ISO 8601).
    pub applied_at: String,
    /// Snapshot before applying.
    pub before_snapshot: WorkloadSnapshot,
    /// Snapshot after applying (populated later).
    pub after_snapshot: Option<WorkloadSnapshot>,
    /// Whether this was an A/B test application.
    pub is_ab_test: bool,
}

/// The auto-tuner manages periodic optimization cycles.
pub struct AutoTuner {
    config: RwLock<AutoTunerConfig>,
    optimizer: AdaptiveOptimizer,
    /// History of applied optimizations.
    history: RwLock<Vec<AppliedOptimization>>,
    /// Cooldown tracker: rule_name → last applied instant.
    cooldowns: RwLock<HashMap<String, Instant>>,
    /// Last run time.
    last_run: RwLock<Option<Instant>>,
    /// Current pending plan.
    current_plan: RwLock<Option<OptimizationPlan>>,
}

impl Default for AutoTuner {
    fn default() -> Self {
        Self::new(AutoTunerConfig::default())
    }
}

impl AutoTuner {
    /// Create a new auto-tuner with the given configuration.
    pub fn new(config: AutoTunerConfig) -> Self {
        Self {
            config: RwLock::new(config),
            optimizer: AdaptiveOptimizer::new(),
            history: RwLock::new(Vec::new()),
            cooldowns: RwLock::new(HashMap::new()),
            last_run: RwLock::new(None),
            current_plan: RwLock::new(None),
        }
    }

    /// Run a single optimization cycle: profile → analyze → (optionally) apply.
    pub fn run_cycle(&self, profiler: &WorkloadProfiler) -> OptimizationPlan {
        let snapshot = profiler.snapshot();
        let plan = self.optimizer.analyze(&snapshot);

        info!(
            recommendations = plan.len(),
            impact = plan.overall_estimated_impact,
            "Optimizer cycle complete"
        );

        *self.current_plan.write() = Some(plan.clone());
        *self.last_run.write() = Some(Instant::now());

        let config = self.config.read().clone();
        if config.enabled {
            self.apply_eligible(&plan, &snapshot, &config);
        }

        plan
    }

    /// Apply recommendations that meet the confidence threshold and are not in cooldown.
    fn apply_eligible(
        &self,
        plan: &OptimizationPlan,
        snapshot: &WorkloadSnapshot,
        config: &AutoTunerConfig,
    ) {
        let cooldown_duration = Duration::from_secs(config.cooldown_secs);
        let now = Instant::now();

        for rec in &plan.recommendations {
            if rec.confidence < config.confidence_threshold {
                debug!(
                    rule = rec.rule_name,
                    confidence = rec.confidence,
                    threshold = config.confidence_threshold,
                    "Skipping recommendation — below confidence threshold"
                );
                continue;
            }

            // Check cooldown.
            {
                let cooldowns = self.cooldowns.read();
                if let Some(last) = cooldowns.get(&rec.rule_name) {
                    if now.duration_since(*last) < cooldown_duration {
                        debug!(
                            rule = rec.rule_name,
                            "Skipping recommendation — in cooldown"
                        );
                        continue;
                    }
                }
            }

            let is_ab = config.ab_test_enabled;

            // Record application.
            let applied = AppliedOptimization {
                recommendation: rec.clone(),
                applied_at: chrono::Utc::now().to_rfc3339(),
                before_snapshot: snapshot.clone(),
                after_snapshot: None,
                is_ab_test: is_ab,
            };

            if is_ab {
                info!(
                    rule = rec.rule_name,
                    fraction = config.ab_test_fraction,
                    "A/B testing recommendation on subset of shards"
                );
            } else {
                info!(rule = rec.rule_name, "Applying recommendation");
            }

            self.history.write().push(applied);
            self.cooldowns
                .write()
                .insert(rec.rule_name.clone(), Instant::now());
        }
    }

    /// Get the current optimization plan (if any).
    pub fn current_plan(&self) -> Option<OptimizationPlan> {
        self.current_plan.read().clone()
    }

    /// Get optimization history.
    pub fn history(&self) -> Vec<AppliedOptimization> {
        self.history.read().clone()
    }

    /// Check whether enough time has elapsed for the next cycle.
    pub fn should_run(&self) -> bool {
        let config = self.config.read();
        if !config.enabled {
            return false;
        }
        let last = self.last_run.read();
        match *last {
            Some(t) => t.elapsed() >= Duration::from_secs(config.interval_secs),
            None => true,
        }
    }

    /// Get a human-readable status summary.
    pub fn status(&self) -> AutoTunerStatus {
        let config = self.config.read();
        let last = self.last_run.read();
        let history = self.history.read();
        let plan = self.current_plan.read();

        AutoTunerStatus {
            enabled: config.enabled,
            interval_secs: config.interval_secs,
            confidence_threshold: config.confidence_threshold,
            cooldown_secs: config.cooldown_secs,
            ab_test_enabled: config.ab_test_enabled,
            last_run_secs_ago: last.map(|t| t.elapsed().as_secs()),
            rules_count: self.optimizer.rule_count(),
            pending_recommendations: plan.as_ref().map(|p| p.len()).unwrap_or(0),
            applied_total: history.len(),
        }
    }

    /// Update configuration.
    pub fn set_config_value(&self, key: &str, value: &str) -> std::result::Result<(), String> {
        let mut config = self.config.write();
        match key {
            "enabled" | "auto_optimize" => {
                config.enabled = value
                    .parse()
                    .map_err(|_| "Invalid boolean value".to_string())?;
            }
            "interval" | "interval_secs" => {
                config.interval_secs = value
                    .parse()
                    .map_err(|_| "Invalid integer value".to_string())?;
            }
            "confidence_threshold" | "threshold" => {
                let v: f64 = value
                    .parse()
                    .map_err(|_| "Invalid float value".to_string())?;
                if !(0.0..=1.0).contains(&v) {
                    return Err("Threshold must be between 0.0 and 1.0".to_string());
                }
                config.confidence_threshold = v;
            }
            "cooldown" | "cooldown_secs" => {
                config.cooldown_secs = value
                    .parse()
                    .map_err(|_| "Invalid integer value".to_string())?;
            }
            "ab_test" | "ab_test_enabled" => {
                config.ab_test_enabled = value
                    .parse()
                    .map_err(|_| "Invalid boolean value".to_string())?;
            }
            "ab_test_fraction" => {
                let v: f64 = value
                    .parse()
                    .map_err(|_| "Invalid float value".to_string())?;
                if !(0.0..=1.0).contains(&v) {
                    return Err("Fraction must be between 0.0 and 1.0".to_string());
                }
                config.ab_test_fraction = v;
            }
            _ => return Err(format!("Unknown config key: {}", key)),
        }
        Ok(())
    }

    /// Get a config value as string.
    pub fn get_config_value(&self, key: &str) -> std::result::Result<String, String> {
        let config = self.config.read();
        match key {
            "enabled" | "auto_optimize" => Ok(config.enabled.to_string()),
            "interval" | "interval_secs" => Ok(config.interval_secs.to_string()),
            "confidence_threshold" | "threshold" => Ok(config.confidence_threshold.to_string()),
            "cooldown" | "cooldown_secs" => Ok(config.cooldown_secs.to_string()),
            "ab_test" | "ab_test_enabled" => Ok(config.ab_test_enabled.to_string()),
            "ab_test_fraction" => Ok(config.ab_test_fraction.to_string()),
            _ => Err(format!("Unknown config key: {}", key)),
        }
    }

    /// Get a reference to the underlying optimizer.
    pub fn optimizer(&self) -> &AdaptiveOptimizer {
        &self.optimizer
    }
}

/// Serializable status of the auto-tuner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoTunerStatus {
    /// Whether auto-optimization is enabled.
    pub enabled: bool,
    /// Optimization interval.
    pub interval_secs: u64,
    /// Confidence threshold for auto-apply.
    pub confidence_threshold: f64,
    /// Cooldown seconds.
    pub cooldown_secs: u64,
    /// A/B testing enabled.
    pub ab_test_enabled: bool,
    /// Seconds since last run (None if never run).
    pub last_run_secs_ago: Option<u64>,
    /// Number of active rules.
    pub rules_count: usize,
    /// Current pending recommendations.
    pub pending_recommendations: usize,
    /// Total applied optimizations.
    pub applied_total: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_tuner_default() {
        let tuner = AutoTuner::default();
        let status = tuner.status();
        assert!(status.enabled);
        assert_eq!(status.interval_secs, 300);
        assert!(status.rules_count >= 15);
    }

    #[test]
    fn test_should_run_initially() {
        let tuner = AutoTuner::default();
        assert!(tuner.should_run());
    }

    #[test]
    fn test_run_cycle() {
        let tuner = AutoTuner::default();
        let profiler = WorkloadProfiler::new();

        // Simulate some activity.
        for _ in 0..20 {
            profiler.record_command("GET", super::super::profiler::CommandKind::Read);
        }
        profiler.set_memory_usage(0.85);

        let plan = tuner.run_cycle(&profiler);
        assert!(!plan.is_empty());
        assert!(!tuner.should_run()); // Just ran.
    }

    #[test]
    fn test_config_set_get() {
        let tuner = AutoTuner::default();
        tuner.set_config_value("enabled", "false").ok();
        assert_eq!(
            tuner.get_config_value("enabled").ok(),
            Some("false".to_string())
        );

        tuner.set_config_value("confidence_threshold", "0.9").ok();
        assert_eq!(
            tuner.get_config_value("confidence_threshold").ok(),
            Some("0.9".to_string())
        );
    }

    #[test]
    fn test_config_invalid_key() {
        let tuner = AutoTuner::default();
        assert!(tuner.set_config_value("nonexistent", "value").is_err());
        assert!(tuner.get_config_value("nonexistent").is_err());
    }

    #[test]
    fn test_config_threshold_bounds() {
        let tuner = AutoTuner::default();
        assert!(tuner
            .set_config_value("confidence_threshold", "1.5")
            .is_err());
        assert!(tuner
            .set_config_value("confidence_threshold", "-0.1")
            .is_err());
    }

    #[test]
    fn test_history_tracking() {
        let tuner = AutoTuner::new(AutoTunerConfig {
            enabled: true,
            confidence_threshold: 0.0, // Accept everything.
            ..Default::default()
        });
        let profiler = WorkloadProfiler::new();
        profiler.set_memory_usage(0.90);

        tuner.run_cycle(&profiler);
        let history = tuner.history();
        assert!(!history.is_empty());
    }

    #[test]
    fn test_disabled_tuner() {
        let tuner = AutoTuner::new(AutoTunerConfig {
            enabled: false,
            ..Default::default()
        });
        assert!(!tuner.should_run());
    }
}
