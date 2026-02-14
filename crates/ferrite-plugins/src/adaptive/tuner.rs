//! Auto Tuning
//!
//! Automatic parameter tuning based on workload analysis.

use super::detector::{AccessPattern, TrafficClassification, WorkloadPattern, WorkloadType};
use super::engine::AdaptationType;
use super::predictor::PredictionResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Auto tuner for dynamic parameter adjustment
pub struct AutoTuner {
    /// Tuning parameters and their current values
    parameters: HashMap<String, TuningParameter>,
    /// Tuning rules
    rules: Vec<TuningRule>,
    /// History of tuning decisions
    history: Vec<TuningDecision>,
    /// Configuration
    config: TunerConfig,
}

impl AutoTuner {
    /// Create a new auto tuner
    pub fn new() -> Self {
        let mut tuner = Self {
            parameters: HashMap::new(),
            rules: Vec::new(),
            history: Vec::new(),
            config: TunerConfig::default(),
        };
        tuner.setup_default_parameters();
        tuner.setup_default_rules();
        tuner
    }

    /// Setup default tuning parameters
    fn setup_default_parameters(&mut self) {
        // Memory tier parameters
        self.parameters.insert(
            "hot-tier-percent".to_string(),
            TuningParameter {
                name: "hot-tier-percent".to_string(),
                current_value: 20.0,
                min_value: 5.0,
                max_value: 50.0,
                step_size: 5.0,
                category: ParameterCategory::Memory,
            },
        );

        self.parameters.insert(
            "warm-tier-percent".to_string(),
            TuningParameter {
                name: "warm-tier-percent".to_string(),
                current_value: 50.0,
                min_value: 20.0,
                max_value: 70.0,
                step_size: 5.0,
                category: ParameterCategory::Memory,
            },
        );

        // Thread pool parameters
        self.parameters.insert(
            "io-threads".to_string(),
            TuningParameter {
                name: "io-threads".to_string(),
                current_value: num_cpus::get() as f64,
                min_value: 1.0,
                max_value: 128.0,
                step_size: 1.0,
                category: ParameterCategory::Compute,
            },
        );

        // Cache parameters
        self.parameters.insert(
            "max-memory-percent".to_string(),
            TuningParameter {
                name: "max-memory-percent".to_string(),
                current_value: 75.0,
                min_value: 50.0,
                max_value: 95.0,
                step_size: 5.0,
                category: ParameterCategory::Memory,
            },
        );
    }

    /// Setup default tuning rules
    fn setup_default_rules(&mut self) {
        // Rule: Read-heavy workload -> Increase hot tier
        self.rules.push(TuningRule {
            name: "read-heavy-hot-tier".to_string(),
            condition: RuleCondition::WorkloadType(WorkloadType::ReadHeavy),
            action: RuleAction::AdjustParameter {
                parameter: "hot-tier-percent".to_string(),
                adjustment: ParameterAdjustment::Increase(10.0),
            },
            priority: 10,
            cooldown_secs: 300,
            last_triggered: None,
        });

        // Rule: Write-heavy workload -> Adjust eviction policy
        self.rules.push(TuningRule {
            name: "write-heavy-eviction".to_string(),
            condition: RuleCondition::WorkloadType(WorkloadType::WriteHeavy),
            action: RuleAction::ChangeEvictionPolicy("allkeys-lru".to_string()),
            priority: 10,
            cooldown_secs: 300,
            last_triggered: None,
        });

        // Rule: High throughput -> Increase IO threads
        self.rules.push(TuningRule {
            name: "high-throughput-threads".to_string(),
            condition: RuleCondition::WorkloadType(WorkloadType::HighThroughput),
            action: RuleAction::AdjustParameter {
                parameter: "io-threads".to_string(),
                adjustment: ParameterAdjustment::Increase(2.0),
            },
            priority: 8,
            cooldown_secs: 600,
            last_triggered: None,
        });

        // Rule: Latency sensitive -> Decrease batch sizes
        self.rules.push(TuningRule {
            name: "latency-sensitive-batch".to_string(),
            condition: RuleCondition::WorkloadType(WorkloadType::LatencySensitive),
            action: RuleAction::ChangeConfig {
                parameter: "batch-size".to_string(),
                value: "100".to_string(),
            },
            priority: 9,
            cooldown_secs: 300,
            last_triggered: None,
        });

        // Rule: HotSpot access -> Increase hot tier significantly
        self.rules.push(TuningRule {
            name: "hotspot-hot-tier".to_string(),
            condition: RuleCondition::AccessPattern(AccessPattern::HotSpot),
            action: RuleAction::AdjustParameter {
                parameter: "hot-tier-percent".to_string(),
                adjustment: ParameterAdjustment::SetTo(40.0),
            },
            priority: 11,
            cooldown_secs: 300,
            last_triggered: None,
        });

        // Rule: Bursty traffic -> Enable request buffering
        self.rules.push(TuningRule {
            name: "bursty-buffering".to_string(),
            condition: RuleCondition::TrafficPattern(TrafficClassification::Bursty),
            action: RuleAction::ChangeConfig {
                parameter: "request-buffer-size".to_string(),
                value: "10000".to_string(),
            },
            priority: 7,
            cooldown_secs: 600,
            last_triggered: None,
        });
    }

    /// Analyze workload and generate tuning decisions
    pub fn analyze(
        &mut self,
        pattern: &WorkloadPattern,
        prediction: Option<&PredictionResult>,
    ) -> Vec<TuningDecision> {
        let mut decisions = Vec::new();
        let now = current_timestamp_ms();

        // Collect matching rules and their indices
        let mut matching_rules: Vec<(usize, String, RuleAction)> = Vec::new();

        // Sort rules by priority and collect matching ones
        let mut rule_indices: Vec<(usize, u32)> = self
            .rules
            .iter()
            .enumerate()
            .map(|(i, r)| (i, r.priority))
            .collect();
        rule_indices.sort_by(|a, b| b.1.cmp(&a.1));

        for (idx, _) in rule_indices {
            let rule = &self.rules[idx];

            // Check cooldown
            if let Some(last) = rule.last_triggered {
                if now - last < rule.cooldown_secs * 1000 {
                    continue;
                }
            }

            // Check condition
            if !rule.matches(pattern) {
                continue;
            }

            matching_rules.push((idx, rule.name.clone(), rule.action.clone()));
        }

        // Generate decisions for matching rules
        for (idx, rule_name, action) in matching_rules {
            let decision = self.create_decision(&rule_name, &action, pattern);
            if let Some(d) = decision {
                self.rules[idx].last_triggered = Some(now);
                decisions.push(d);
            }
        }

        // Add predictive decisions if prediction suggests action
        if let Some(pred) = prediction {
            if pred.requires_action() {
                if let Some(d) = self.generate_predictive_decision(pred) {
                    decisions.push(d);
                }
            }
        }

        // Record decisions
        self.history.extend(decisions.clone());

        decisions
    }

    /// Create a decision from rule action
    fn create_decision(
        &self,
        rule_name: &str,
        action: &RuleAction,
        pattern: &WorkloadPattern,
    ) -> Option<TuningDecision> {
        match action {
            RuleAction::AdjustParameter {
                parameter,
                adjustment,
            } => {
                let param = self.parameters.get(parameter)?;
                let new_value =
                    adjustment.apply(param.current_value, param.min_value, param.max_value);

                if (new_value - param.current_value).abs() < 0.001 {
                    return None; // No change needed
                }

                Some(TuningDecision {
                    rule_name: rule_name.to_string(),
                    decision_type: DecisionType::ParameterAdjustment,
                    parameter: Some(parameter.clone()),
                    old_value: Some(param.current_value.to_string()),
                    new_value: Some(new_value.to_string()),
                    reason: format!(
                        "Workload pattern {:?} detected, adjusting {}",
                        pattern.workload_type, parameter
                    ),
                    confidence: pattern.confidence,
                    expected_improvement: self.estimate_improvement(action),
                    timestamp: current_timestamp_ms(),
                })
            }
            RuleAction::ChangeEvictionPolicy(policy) => Some(TuningDecision {
                rule_name: rule_name.to_string(),
                decision_type: DecisionType::PolicyChange,
                parameter: Some("eviction-policy".to_string()),
                old_value: None,
                new_value: Some(policy.clone()),
                reason: format!(
                    "Workload pattern {:?} detected, changing eviction policy",
                    pattern.workload_type
                ),
                confidence: pattern.confidence,
                expected_improvement: self.estimate_improvement(action),
                timestamp: current_timestamp_ms(),
            }),
            RuleAction::ChangeConfig { parameter, value } => Some(TuningDecision {
                rule_name: rule_name.to_string(),
                decision_type: DecisionType::ConfigChange,
                parameter: Some(parameter.clone()),
                old_value: None,
                new_value: Some(value.clone()),
                reason: format!("Workload pattern detected, changing config {}", parameter),
                confidence: pattern.confidence,
                expected_improvement: self.estimate_improvement(action),
                timestamp: current_timestamp_ms(),
            }),
            RuleAction::Prewarm { key_patterns } => Some(TuningDecision {
                rule_name: rule_name.to_string(),
                decision_type: DecisionType::Prewarm,
                parameter: None,
                old_value: None,
                new_value: Some(key_patterns.join(",")),
                reason: "Predictive cache warming based on access patterns".to_string(),
                confidence: pattern.confidence,
                expected_improvement: self.estimate_improvement(action),
                timestamp: current_timestamp_ms(),
            }),
        }
    }

    /// Generate a predictive tuning decision
    fn generate_predictive_decision(
        &self,
        prediction: &PredictionResult,
    ) -> Option<TuningDecision> {
        if prediction.predicts_increase() && prediction.confidence > 0.7 {
            Some(TuningDecision {
                rule_name: "predictive-scaling".to_string(),
                decision_type: DecisionType::ParameterAdjustment,
                parameter: Some("max-memory-percent".to_string()),
                old_value: self
                    .parameters
                    .get("max-memory-percent")
                    .map(|p| p.current_value.to_string()),
                new_value: Some("85".to_string()),
                reason: format!(
                    "Predicted load increase ({:.0} ops/s), proactively scaling",
                    prediction.predicted_ops_per_second
                ),
                confidence: prediction.confidence,
                expected_improvement: 5.0,
                timestamp: current_timestamp_ms(),
            })
        } else {
            None
        }
    }

    /// Estimate improvement from an action
    fn estimate_improvement(&self, action: &RuleAction) -> f64 {
        match action {
            RuleAction::AdjustParameter { parameter, .. } => match parameter.as_str() {
                "hot-tier-percent" => 10.0,
                "io-threads" => 8.0,
                "max-memory-percent" => 5.0,
                _ => 3.0,
            },
            RuleAction::ChangeEvictionPolicy(_) => 7.0,
            RuleAction::ChangeConfig { .. } => 5.0,
            RuleAction::Prewarm { .. } => 15.0,
        }
    }

    /// Get current parameters
    pub fn parameters(&self) -> &HashMap<String, TuningParameter> {
        &self.parameters
    }

    /// Update a parameter value
    pub fn update_parameter(&mut self, name: &str, value: f64) {
        if let Some(param) = self.parameters.get_mut(name) {
            param.current_value = value.clamp(param.min_value, param.max_value);
        }
    }

    /// Get tuning history
    pub fn history(&self) -> &[TuningDecision] {
        &self.history
    }

    /// Clear history
    pub fn clear_history(&mut self) {
        self.history.clear();
    }

    /// Add a custom rule
    pub fn add_rule(&mut self, rule: TuningRule) {
        self.rules.push(rule);
    }
}

impl Default for AutoTuner {
    fn default() -> Self {
        Self::new()
    }
}

/// Tuning parameter
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TuningParameter {
    /// Parameter name
    pub name: String,
    /// Current value
    pub current_value: f64,
    /// Minimum value
    pub min_value: f64,
    /// Maximum value
    pub max_value: f64,
    /// Step size for adjustments
    pub step_size: f64,
    /// Category
    pub category: ParameterCategory,
}

/// Parameter category
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParameterCategory {
    /// Memory related
    Memory,
    /// Compute related
    Compute,
    /// Network related
    Network,
    /// Storage related
    Storage,
}

/// Tuning rule
pub struct TuningRule {
    /// Rule name
    pub name: String,
    /// Condition to trigger
    pub condition: RuleCondition,
    /// Action to take
    pub action: RuleAction,
    /// Priority (higher = more important)
    pub priority: u32,
    /// Cooldown period in seconds
    pub cooldown_secs: u64,
    /// Last triggered timestamp
    pub last_triggered: Option<u64>,
}

impl TuningRule {
    /// Check if the rule matches the workload pattern
    fn matches(&self, pattern: &WorkloadPattern) -> bool {
        match &self.condition {
            RuleCondition::WorkloadType(wt) => &pattern.workload_type == wt,
            RuleCondition::AccessPattern(ap) => &pattern.access_pattern == ap,
            RuleCondition::TrafficPattern(tp) => &pattern.traffic_pattern == tp,
            RuleCondition::ReadRatioAbove(threshold) => pattern.read_ratio > *threshold,
            RuleCondition::ReadRatioBelow(threshold) => pattern.read_ratio < *threshold,
            RuleCondition::OpsAbove(threshold) => pattern.avg_ops_per_second > *threshold,
            RuleCondition::LatencyAbove(threshold) => pattern.avg_latency_us > *threshold,
            RuleCondition::CacheHitBelow(threshold) => pattern.cache_hit_rate < *threshold,
            RuleCondition::And(conditions) => conditions.iter().all(|c| {
                let temp_rule = TuningRule {
                    name: String::new(),
                    condition: c.clone(),
                    action: RuleAction::ChangeConfig {
                        parameter: String::new(),
                        value: String::new(),
                    },
                    priority: 0,
                    cooldown_secs: 0,
                    last_triggered: None,
                };
                temp_rule.matches(pattern)
            }),
            RuleCondition::Or(conditions) => conditions.iter().any(|c| {
                let temp_rule = TuningRule {
                    name: String::new(),
                    condition: c.clone(),
                    action: RuleAction::ChangeConfig {
                        parameter: String::new(),
                        value: String::new(),
                    },
                    priority: 0,
                    cooldown_secs: 0,
                    last_triggered: None,
                };
                temp_rule.matches(pattern)
            }),
        }
    }
}

/// Rule condition
#[derive(Clone, Debug)]
pub enum RuleCondition {
    /// Match workload type
    WorkloadType(WorkloadType),
    /// Match access pattern
    AccessPattern(AccessPattern),
    /// Match traffic pattern
    TrafficPattern(TrafficClassification),
    /// Read ratio above threshold
    ReadRatioAbove(f64),
    /// Read ratio below threshold
    ReadRatioBelow(f64),
    /// Ops per second above threshold
    OpsAbove(f64),
    /// Latency above threshold (microseconds)
    LatencyAbove(f64),
    /// Cache hit rate below threshold
    CacheHitBelow(f64),
    /// All conditions must match
    And(Vec<RuleCondition>),
    /// Any condition must match
    Or(Vec<RuleCondition>),
}

/// Rule action
#[derive(Clone, Debug)]
pub enum RuleAction {
    /// Adjust a parameter
    AdjustParameter {
        /// Parameter name
        parameter: String,
        /// Adjustment
        adjustment: ParameterAdjustment,
    },
    /// Change eviction policy
    ChangeEvictionPolicy(String),
    /// Change configuration
    ChangeConfig {
        /// Parameter name
        parameter: String,
        /// New value
        value: String,
    },
    /// Pre-warm cache
    Prewarm {
        /// Key patterns to warm
        key_patterns: Vec<String>,
    },
}

/// Parameter adjustment
#[derive(Clone, Debug)]
pub enum ParameterAdjustment {
    /// Increase by amount
    Increase(f64),
    /// Decrease by amount
    Decrease(f64),
    /// Set to specific value
    SetTo(f64),
    /// Increase by percentage
    IncreasePercent(f64),
    /// Decrease by percentage
    DecreasePercent(f64),
}

impl ParameterAdjustment {
    /// Apply the adjustment
    fn apply(&self, current: f64, min: f64, max: f64) -> f64 {
        let new_value = match self {
            ParameterAdjustment::Increase(amount) => current + amount,
            ParameterAdjustment::Decrease(amount) => current - amount,
            ParameterAdjustment::SetTo(value) => *value,
            ParameterAdjustment::IncreasePercent(percent) => current * (1.0 + percent / 100.0),
            ParameterAdjustment::DecreasePercent(percent) => current * (1.0 - percent / 100.0),
        };
        new_value.clamp(min, max)
    }
}

/// Tuning decision
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TuningDecision {
    /// Rule that generated this decision
    pub rule_name: String,
    /// Type of decision
    pub decision_type: DecisionType,
    /// Parameter affected (if applicable)
    pub parameter: Option<String>,
    /// Old value (if applicable)
    pub old_value: Option<String>,
    /// New value (if applicable)
    pub new_value: Option<String>,
    /// Reason for the decision
    pub reason: String,
    /// Confidence in the decision
    pub confidence: f64,
    /// Expected improvement percentage
    pub expected_improvement: f64,
    /// Timestamp
    pub timestamp: u64,
}

impl TuningDecision {
    /// Get the adaptation type for this decision
    pub fn adaptation_type(&self) -> AdaptationType {
        match self.decision_type {
            DecisionType::ParameterAdjustment => {
                if let Some(param) = &self.parameter {
                    if param.contains("tier") {
                        let tier = if param.contains("hot") {
                            "hot"
                        } else if param.contains("warm") {
                            "warm"
                        } else {
                            "cold"
                        };
                        let percent = self
                            .new_value
                            .as_ref()
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(20.0);
                        AdaptationType::MemoryTierAdjustment {
                            tier: tier.to_string(),
                            allocation_percent: percent,
                        }
                    } else if param.contains("thread") {
                        let size = self
                            .new_value
                            .as_ref()
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(4);
                        AdaptationType::ThreadPoolResize {
                            pool: "io".to_string(),
                            new_size: size,
                        }
                    } else {
                        AdaptationType::ConfigurationChange {
                            parameter: param.clone(),
                            value: self.new_value.clone().unwrap_or_default(),
                        }
                    }
                } else {
                    AdaptationType::ConfigurationChange {
                        parameter: "unknown".to_string(),
                        value: self.new_value.clone().unwrap_or_default(),
                    }
                }
            }
            DecisionType::PolicyChange => AdaptationType::EvictionPolicyChange {
                policy: self
                    .new_value
                    .clone()
                    .unwrap_or_else(|| "allkeys-lru".to_string()),
            },
            DecisionType::ConfigChange => AdaptationType::ConfigurationChange {
                parameter: self.parameter.clone().unwrap_or_default(),
                value: self.new_value.clone().unwrap_or_default(),
            },
            DecisionType::Prewarm => {
                let patterns = self
                    .new_value
                    .as_ref()
                    .map(|v| v.split(',').map(|s| s.to_string()).collect())
                    .unwrap_or_default();
                AdaptationType::CachePrewarm {
                    key_patterns: patterns,
                }
            }
        }
    }

    /// Get description
    pub fn description(&self) -> String {
        self.reason.clone()
    }

    /// Get parameters as map
    pub fn parameters(&self) -> HashMap<String, String> {
        let mut params = HashMap::new();
        if let Some(p) = &self.parameter {
            params.insert("parameter".to_string(), p.clone());
        }
        if let Some(v) = &self.old_value {
            params.insert("old_value".to_string(), v.clone());
        }
        if let Some(v) = &self.new_value {
            params.insert("new_value".to_string(), v.clone());
        }
        params.insert("confidence".to_string(), self.confidence.to_string());
        params
    }

    /// Get expected improvement
    pub fn expected_improvement(&self) -> f64 {
        self.expected_improvement
    }
}

/// Type of tuning decision
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DecisionType {
    /// Parameter adjustment
    ParameterAdjustment,
    /// Policy change
    PolicyChange,
    /// Configuration change
    ConfigChange,
    /// Cache pre-warming
    Prewarm,
}

/// Tuner configuration
#[derive(Clone, Debug)]
pub struct TunerConfig {
    /// Minimum confidence to apply decisions
    pub min_confidence: f64,
    /// Maximum decisions per cycle
    pub max_decisions_per_cycle: usize,
    /// Enable aggressive tuning
    pub aggressive: bool,
}

impl Default for TunerConfig {
    fn default() -> Self {
        Self {
            min_confidence: 0.7,
            max_decisions_per_cycle: 5,
            aggressive: false,
        }
    }
}

/// Get current timestamp in milliseconds
fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_read_heavy_pattern() -> WorkloadPattern {
        WorkloadPattern {
            workload_type: WorkloadType::ReadHeavy,
            access_pattern: AccessPattern::Random,
            traffic_pattern: TrafficClassification::Steady,
            read_ratio: 0.98,
            avg_ops_per_second: 10000.0,
            avg_latency_us: 100.0,
            cache_hit_rate: 0.9,
            confidence: 0.85,
        }
    }

    #[test]
    fn test_tuner_creation() {
        let tuner = AutoTuner::new();
        assert!(!tuner.parameters.is_empty());
        assert!(!tuner.rules.is_empty());
    }

    #[test]
    fn test_read_heavy_tuning() {
        let mut tuner = AutoTuner::new();
        let pattern = create_read_heavy_pattern();

        let decisions = tuner.analyze(&pattern, None);

        // Should generate at least one decision for read-heavy workload
        assert!(!decisions.is_empty());

        // Should include hot tier adjustment
        let has_hot_tier = decisions.iter().any(|d| {
            d.parameter
                .as_ref()
                .map(|p| p.contains("hot"))
                .unwrap_or(false)
        });
        assert!(has_hot_tier);
    }

    #[test]
    fn test_parameter_adjustment() {
        let adj = ParameterAdjustment::Increase(10.0);
        let result = adj.apply(50.0, 0.0, 100.0);
        assert_eq!(result, 60.0);

        let adj = ParameterAdjustment::SetTo(80.0);
        let result = adj.apply(50.0, 0.0, 100.0);
        assert_eq!(result, 80.0);

        // Test clamping
        let adj = ParameterAdjustment::Increase(100.0);
        let result = adj.apply(50.0, 0.0, 100.0);
        assert_eq!(result, 100.0);
    }

    #[test]
    fn test_decision_to_adaptation() {
        let decision = TuningDecision {
            rule_name: "test".to_string(),
            decision_type: DecisionType::PolicyChange,
            parameter: None,
            old_value: None,
            new_value: Some("allkeys-lfu".to_string()),
            reason: "Test".to_string(),
            confidence: 0.9,
            expected_improvement: 10.0,
            timestamp: 0,
        };

        let adaptation = decision.adaptation_type();
        assert!(matches!(
            adaptation,
            AdaptationType::EvictionPolicyChange { .. }
        ));
    }
}
