#![allow(dead_code)]
//! Auto-scaling engine for managed Ferrite Cloud
//!
//! Evaluates instance metrics and makes scaling decisions to maintain
//! performance targets while optimizing resource usage.

use std::time::Duration;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::provisioner::InstanceType;

/// Configuration for the auto-scaler
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingConfig {
    /// Minimum number of replicas
    pub min_replicas: u8,
    /// Maximum number of replicas
    pub max_replicas: u8,
    /// Target CPU utilization percentage
    pub target_cpu_pct: f64,
    /// Target memory utilization percentage
    pub target_memory_pct: f64,
    /// Target operations per second
    pub target_ops_per_sec: u64,
    /// Cooldown period between scaling events
    #[serde(with = "humantime_serde_compat")]
    pub cooldown_period: Duration,
    /// Threshold above target to trigger scale-up (e.g., 0.8 = 80%)
    pub scale_up_threshold: f64,
    /// Threshold below target to trigger scale-down (e.g., 0.3 = 30%)
    pub scale_down_threshold: f64,
}

/// Serde helper for Duration as seconds
mod humantime_serde_compat {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(duration.as_secs())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        let secs = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(secs))
    }
}

impl Default for ScalingConfig {
    fn default() -> Self {
        Self {
            min_replicas: 1,
            max_replicas: 10,
            target_cpu_pct: 70.0,
            target_memory_pct: 75.0,
            target_ops_per_sec: 10_000,
            cooldown_period: Duration::from_secs(300),
            scale_up_threshold: 0.8,
            scale_down_threshold: 0.3,
        }
    }
}

/// Current metrics for an instance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceMetrics {
    /// CPU utilization percentage (0.0 - 100.0)
    pub cpu_pct: f64,
    /// Memory utilization percentage (0.0 - 100.0)
    pub memory_pct: f64,
    /// Operations per second
    pub ops_per_sec: u64,
    /// Active connections
    pub connections: u64,
    /// P99 latency in milliseconds
    pub latency_p99_ms: f64,
    /// Current number of replicas
    pub current_replicas: u8,
}

/// Scaling decision made by the auto-scaler
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ScalingDecision {
    /// No scaling action needed
    NoChange,
    /// Scale out by adding replicas
    ScaleUp {
        /// Target replica count
        to_replicas: u8,
        /// Reason for scaling
        reason: String,
    },
    /// Scale in by removing replicas
    ScaleDown {
        /// Target replica count
        to_replicas: u8,
        /// Reason for scaling
        reason: String,
    },
    /// Scale vertically to a different instance type
    ScaleVertical {
        /// New instance type
        new_type: InstanceType,
        /// Reason for scaling
        reason: String,
    },
}

/// Outcome of a scaling event
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ScalingOutcome {
    /// Scaling completed successfully
    Success,
    /// Scaling failed
    Failed,
    /// Scaling was blocked by cooldown
    Cooldown,
}

/// Record of a scaling event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingEvent {
    /// When the scaling event occurred
    pub timestamp: DateTime<Utc>,
    /// Previous replica count
    pub from_replicas: u8,
    /// New replica count
    pub to_replicas: u8,
    /// Reason for scaling
    pub reason: String,
    /// Outcome of the scaling event
    pub outcome: ScalingOutcome,
}

/// Scaling errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum ScalingError {
    /// Cooldown period is still active
    #[error("cooldown active, {0}s remaining")]
    CooldownActive(u64),

    /// Maximum replicas already reached
    #[error("maximum replicas reached: {0}")]
    MaxReplicasReached(u8),

    /// Minimum replicas already reached
    #[error("minimum replicas reached: {0}")]
    MinReplicasReached(u8),

    /// Invalid scaling target
    #[error("invalid scaling target: {0}")]
    InvalidTarget(String),
}

/// Auto-scaler that evaluates metrics and makes scaling decisions
pub struct AutoScaler {
    config: ScalingConfig,
    last_scale_time: RwLock<Option<DateTime<Utc>>>,
    history: RwLock<Vec<ScalingEvent>>,
}

impl AutoScaler {
    /// Create a new auto-scaler with the given configuration
    pub fn new(config: ScalingConfig) -> Self {
        Self {
            config,
            last_scale_time: RwLock::new(None),
            history: RwLock::new(Vec::new()),
        }
    }

    /// Evaluate current metrics and return a scaling decision
    pub fn evaluate(&self, metrics: &InstanceMetrics) -> ScalingDecision {
        let cpu_ratio = metrics.cpu_pct / self.config.target_cpu_pct;
        let memory_ratio = metrics.memory_pct / self.config.target_memory_pct;

        // Scale up if any metric exceeds the threshold
        if cpu_ratio > self.config.scale_up_threshold
            || memory_ratio > self.config.scale_up_threshold
        {
            let new_replicas = (metrics.current_replicas + 1).min(self.config.max_replicas);
            if new_replicas > metrics.current_replicas {
                let reason = format!(
                    "high utilization: cpu={:.1}%, memory={:.1}%",
                    metrics.cpu_pct, metrics.memory_pct
                );
                return ScalingDecision::ScaleUp {
                    to_replicas: new_replicas,
                    reason,
                };
            }
        }

        // Scale down if all metrics are below the threshold
        if cpu_ratio < self.config.scale_down_threshold
            && memory_ratio < self.config.scale_down_threshold
        {
            let new_replicas =
                (metrics.current_replicas.saturating_sub(1)).max(self.config.min_replicas);
            if new_replicas < metrics.current_replicas {
                let reason = format!(
                    "low utilization: cpu={:.1}%, memory={:.1}%",
                    metrics.cpu_pct, metrics.memory_pct
                );
                return ScalingDecision::ScaleDown {
                    to_replicas: new_replicas,
                    reason,
                };
            }
        }

        ScalingDecision::NoChange
    }

    /// Apply a scaling decision, recording the event
    pub fn apply_decision(
        &self,
        instance_id: &str,
        decision: ScalingDecision,
    ) -> Result<(), ScalingError> {
        let _ = instance_id;

        // Check cooldown
        let now = Utc::now();
        let last = self.last_scale_time.read();
        if let Some(last_time) = *last {
            let elapsed = (now - last_time).num_seconds() as u64;
            let cooldown_secs = self.config.cooldown_period.as_secs();
            if elapsed < cooldown_secs {
                let remaining = cooldown_secs - elapsed;
                let event = ScalingEvent {
                    timestamp: now,
                    from_replicas: 0,
                    to_replicas: 0,
                    reason: "cooldown active".to_string(),
                    outcome: ScalingOutcome::Cooldown,
                };
                self.history.write().push(event);
                return Err(ScalingError::CooldownActive(remaining));
            }
        }
        drop(last);

        match &decision {
            ScalingDecision::NoChange => Ok(()),
            ScalingDecision::ScaleUp {
                to_replicas,
                reason,
            } => {
                if *to_replicas > self.config.max_replicas {
                    return Err(ScalingError::MaxReplicasReached(self.config.max_replicas));
                }
                let event = ScalingEvent {
                    timestamp: now,
                    from_replicas: to_replicas.saturating_sub(1),
                    to_replicas: *to_replicas,
                    reason: reason.clone(),
                    outcome: ScalingOutcome::Success,
                };
                self.history.write().push(event);
                *self.last_scale_time.write() = Some(now);
                Ok(())
            }
            ScalingDecision::ScaleDown {
                to_replicas,
                reason,
            } => {
                if *to_replicas < self.config.min_replicas {
                    return Err(ScalingError::MinReplicasReached(self.config.min_replicas));
                }
                let event = ScalingEvent {
                    timestamp: now,
                    from_replicas: to_replicas + 1,
                    to_replicas: *to_replicas,
                    reason: reason.clone(),
                    outcome: ScalingOutcome::Success,
                };
                self.history.write().push(event);
                *self.last_scale_time.write() = Some(now);
                Ok(())
            }
            ScalingDecision::ScaleVertical { reason, .. } => {
                let event = ScalingEvent {
                    timestamp: now,
                    from_replicas: 0,
                    to_replicas: 0,
                    reason: reason.clone(),
                    outcome: ScalingOutcome::Success,
                };
                self.history.write().push(event);
                *self.last_scale_time.write() = Some(now);
                Ok(())
            }
        }
    }

    /// Get the scaling history for an instance
    pub fn scaling_history(&self, _instance_id: &str) -> Vec<ScalingEvent> {
        self.history.read().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evaluate_no_change() {
        let scaler = AutoScaler::new(ScalingConfig::default());
        let metrics = InstanceMetrics {
            cpu_pct: 50.0,
            memory_pct: 50.0,
            ops_per_sec: 5000,
            connections: 100,
            latency_p99_ms: 5.0,
            current_replicas: 2,
        };

        let decision = scaler.evaluate(&metrics);
        assert_eq!(decision, ScalingDecision::NoChange);
    }

    #[test]
    fn test_evaluate_scale_up() {
        let scaler = AutoScaler::new(ScalingConfig::default());
        let metrics = InstanceMetrics {
            cpu_pct: 90.0,
            memory_pct: 85.0,
            ops_per_sec: 15000,
            connections: 500,
            latency_p99_ms: 50.0,
            current_replicas: 2,
        };

        let decision = scaler.evaluate(&metrics);
        assert!(matches!(decision, ScalingDecision::ScaleUp { .. }));
    }

    #[test]
    fn test_evaluate_scale_down() {
        let scaler = AutoScaler::new(ScalingConfig::default());
        let metrics = InstanceMetrics {
            cpu_pct: 5.0,
            memory_pct: 10.0,
            ops_per_sec: 100,
            connections: 5,
            latency_p99_ms: 1.0,
            current_replicas: 5,
        };

        let decision = scaler.evaluate(&metrics);
        assert!(matches!(decision, ScalingDecision::ScaleDown { .. }));
    }

    #[test]
    fn test_apply_decision_and_history() {
        let config = ScalingConfig {
            cooldown_period: Duration::from_secs(0),
            ..Default::default()
        };
        let scaler = AutoScaler::new(config);

        let decision = ScalingDecision::ScaleUp {
            to_replicas: 3,
            reason: "test scale up".to_string(),
        };

        scaler.apply_decision("inst-1", decision).unwrap();
        let history = scaler.scaling_history("inst-1");
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].outcome, ScalingOutcome::Success);
    }

    #[test]
    fn test_max_replicas_not_exceeded() {
        let config = ScalingConfig {
            max_replicas: 3,
            ..Default::default()
        };
        let scaler = AutoScaler::new(config);
        let metrics = InstanceMetrics {
            cpu_pct: 90.0,
            memory_pct: 85.0,
            ops_per_sec: 15000,
            connections: 500,
            latency_p99_ms: 50.0,
            current_replicas: 3,
        };

        let decision = scaler.evaluate(&metrics);
        assert_eq!(decision, ScalingDecision::NoChange);
    }
}
