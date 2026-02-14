//! Health monitoring for multi-cloud regions

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Health check configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthConfig {
    /// Health check interval in milliseconds
    pub interval_ms: u64,
    /// Health check timeout in milliseconds
    pub timeout_ms: u64,
    /// Number of consecutive failures before marking unhealthy
    pub threshold: u32,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            interval_ms: 5000,
            timeout_ms: 3000,
            threshold: 3,
        }
    }
}

/// Health status
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// Target is healthy
    Healthy,
    /// Target is degraded (some checks failing)
    Degraded,
    /// Target is unhealthy
    Unhealthy,
    /// Health status is unknown
    #[default]
    Unknown,
}

impl HealthStatus {
    /// Get string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            HealthStatus::Healthy => "healthy",
            HealthStatus::Degraded => "degraded",
            HealthStatus::Unhealthy => "unhealthy",
            HealthStatus::Unknown => "unknown",
        }
    }
}

/// Health check result
#[derive(Debug, Clone)]
pub struct HealthCheckResult {
    /// Target name
    pub target: String,
    /// Status
    pub status: HealthStatus,
    /// Latency in milliseconds
    pub latency_ms: Option<f64>,
    /// Error message if unhealthy
    pub error: Option<String>,
    /// Timestamp
    pub timestamp: u64,
}

/// Internal health state for a target
#[derive(Debug, Clone, Default)]
struct TargetHealth {
    status: HealthStatus,
    consecutive_failures: u32,
    consecutive_successes: u32,
    last_check: u64,
    last_latency_ms: Option<f64>,
    total_checks: u64,
    total_failures: u64,
}


/// Health monitor for tracking region health
pub struct HealthMonitor {
    config: HealthConfig,
    targets: RwLock<HashMap<String, TargetHealth>>,
}

impl HealthMonitor {
    /// Create a new health monitor
    pub fn new(config: HealthConfig) -> Self {
        Self {
            config,
            targets: RwLock::new(HashMap::new()),
        }
    }

    /// Add a target to monitor
    pub fn add_target(&self, name: &str) {
        self.targets
            .write()
            .insert(name.to_string(), TargetHealth::default());
    }

    /// Remove a target
    pub fn remove_target(&self, name: &str) {
        self.targets.write().remove(name);
    }

    /// Record a health check result
    pub fn record_check(&self, name: &str, success: bool, latency_ms: Option<f64>) {
        let now = current_timestamp();
        let mut targets = self.targets.write();

        let target = targets.entry(name.to_string()).or_default();
        target.last_check = now;
        target.last_latency_ms = latency_ms;
        target.total_checks += 1;

        if success {
            target.consecutive_failures = 0;
            target.consecutive_successes += 1;

            // Transition to healthy after threshold successes
            if target.consecutive_successes >= self.config.threshold {
                target.status = HealthStatus::Healthy;
            } else if target.status == HealthStatus::Unhealthy {
                target.status = HealthStatus::Degraded;
            }
        } else {
            target.consecutive_successes = 0;
            target.consecutive_failures += 1;
            target.total_failures += 1;

            // Transition to unhealthy after threshold failures
            if target.consecutive_failures >= self.config.threshold {
                target.status = HealthStatus::Unhealthy;
            } else if target.status == HealthStatus::Healthy {
                target.status = HealthStatus::Degraded;
            }
        }
    }

    /// Get health status for a target
    pub fn get_status(&self, name: &str) -> HealthStatus {
        self.targets
            .read()
            .get(name)
            .map(|t| t.status)
            .unwrap_or(HealthStatus::Unknown)
    }

    /// Get all health states
    pub fn get_all_states(&self) -> HashMap<String, HealthStatus> {
        self.targets
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.status))
            .collect()
    }

    /// Get detailed health info for a target
    pub fn get_details(&self, name: &str) -> Option<HealthDetails> {
        self.targets.read().get(name).map(|t| HealthDetails {
            status: t.status,
            consecutive_failures: t.consecutive_failures,
            consecutive_successes: t.consecutive_successes,
            last_check: t.last_check,
            last_latency_ms: t.last_latency_ms,
            total_checks: t.total_checks,
            total_failures: t.total_failures,
        })
    }

    /// Get healthy targets
    pub fn healthy_targets(&self) -> Vec<String> {
        self.targets
            .read()
            .iter()
            .filter(|(_, v)| v.status == HealthStatus::Healthy)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Get unhealthy targets
    pub fn unhealthy_targets(&self) -> Vec<String> {
        self.targets
            .read()
            .iter()
            .filter(|(_, v)| v.status == HealthStatus::Unhealthy)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Force a target to a specific status (for manual override)
    pub fn force_status(&self, name: &str, status: HealthStatus) {
        if let Some(target) = self.targets.write().get_mut(name) {
            target.status = status;
            target.consecutive_failures = 0;
            target.consecutive_successes = 0;
        }
    }

    /// Get configuration
    pub fn config(&self) -> &HealthConfig {
        &self.config
    }
}

impl Default for HealthMonitor {
    fn default() -> Self {
        Self::new(HealthConfig::default())
    }
}

/// Detailed health information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthDetails {
    /// Current status
    pub status: HealthStatus,
    /// Consecutive failures
    pub consecutive_failures: u32,
    /// Consecutive successes
    pub consecutive_successes: u32,
    /// Last check timestamp
    pub last_check: u64,
    /// Last latency measurement
    pub last_latency_ms: Option<f64>,
    /// Total checks performed
    pub total_checks: u64,
    /// Total failures
    pub total_failures: u64,
}

/// Get current timestamp in seconds
fn current_timestamp() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_transitions() {
        let config = HealthConfig {
            threshold: 2,
            ..Default::default()
        };
        let monitor = HealthMonitor::new(config);

        monitor.add_target("region1");
        assert_eq!(monitor.get_status("region1"), HealthStatus::Unknown);

        // Two successes should make it healthy
        monitor.record_check("region1", true, Some(10.0));
        monitor.record_check("region1", true, Some(11.0));
        assert_eq!(monitor.get_status("region1"), HealthStatus::Healthy);

        // One failure should degrade it
        monitor.record_check("region1", false, None);
        assert_eq!(monitor.get_status("region1"), HealthStatus::Degraded);

        // Two failures should make it unhealthy
        monitor.record_check("region1", false, None);
        assert_eq!(monitor.get_status("region1"), HealthStatus::Unhealthy);
    }

    #[test]
    fn test_healthy_targets() {
        let monitor = HealthMonitor::new(HealthConfig {
            threshold: 1,
            ..Default::default()
        });

        monitor.add_target("healthy1");
        monitor.add_target("healthy2");
        monitor.add_target("unhealthy");

        monitor.record_check("healthy1", true, Some(5.0));
        monitor.record_check("healthy2", true, Some(6.0));
        monitor.record_check("unhealthy", false, None);

        let healthy = monitor.healthy_targets();
        assert_eq!(healthy.len(), 2);
        assert!(healthy.contains(&"healthy1".to_string()));
        assert!(healthy.contains(&"healthy2".to_string()));
    }
}
