//! Quota Enforcement
//!
//! Enforces resource quotas for tenants with configurable behavior:
//! reject, throttle, or warn on quota violations.

use super::metrics::TenantUsage;
use super::{OperationType, TenancyError, TenantLimits};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// Action to take when a quota is approaching or exceeded
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuotaAction {
    /// Operation is allowed without restriction
    Allow,
    /// Operation is allowed but a warning should be emitted
    Warn(String),
    /// Operation should be throttled (caller should add delay)
    Throttle { delay_ms: u64 },
}

/// Policy for how quota violations are handled
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuotaPolicy {
    /// Behavior when a hard limit is exceeded
    pub on_exceeded: QuotaBehavior,
    /// Behavior when usage is above warning threshold (percentage 0-100)
    pub warning_threshold_percent: f64,
    /// Behavior when warning threshold is hit
    pub on_warning: QuotaBehavior,
}

impl Default for QuotaPolicy {
    fn default() -> Self {
        Self {
            on_exceeded: QuotaBehavior::Reject,
            warning_threshold_percent: 80.0,
            on_warning: QuotaBehavior::Warn,
        }
    }
}

/// Configurable behavior on quota violation
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuotaBehavior {
    /// Reject the operation with an error
    Reject,
    /// Allow with a warning log
    Warn,
    /// Throttle by adding artificial delay
    Throttle { delay_ms: u64 },
}

/// Quota enforcer for a tenant
pub struct QuotaEnforcer {
    /// Tenant limits
    limits: TenantLimits,
    /// Quota enforcement policy
    policy: QuotaPolicy,
    /// Current memory usage (bytes)
    memory_used: AtomicI64,
    /// Current key count
    key_count: AtomicI64,
    /// Current connection count
    connection_count: AtomicU64,
    /// Violations count
    violations: AtomicU64,
}

impl std::fmt::Debug for QuotaEnforcer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::sync::atomic::Ordering;
        f.debug_struct("QuotaEnforcer")
            .field("limits", &self.limits)
            .field("policy", &self.policy)
            .field("memory_used", &self.memory_used.load(Ordering::Relaxed))
            .field("key_count", &self.key_count.load(Ordering::Relaxed))
            .field(
                "connection_count",
                &self.connection_count.load(Ordering::Relaxed),
            )
            .field("violations", &self.violations.load(Ordering::Relaxed))
            .finish()
    }
}

impl QuotaEnforcer {
    /// Create a new quota enforcer
    pub fn new(limits: TenantLimits, policy: QuotaPolicy) -> Self {
        Self {
            limits,
            policy,
            memory_used: AtomicI64::new(0),
            key_count: AtomicI64::new(0),
            connection_count: AtomicU64::new(0),
            violations: AtomicU64::new(0),
        }
    }

    /// Check if an operation is allowed, returning the action to take
    pub fn check(
        &self,
        op_type: OperationType,
        tenant_id: &str,
    ) -> Result<QuotaAction, TenancyError> {
        match op_type {
            OperationType::Write => self.check_write(tenant_id),
            OperationType::Read => Ok(self.check_read_warning()),
            OperationType::Delete => Ok(QuotaAction::Allow),
            OperationType::Scan => Ok(QuotaAction::Allow),
            OperationType::Admin => Ok(QuotaAction::Allow),
        }
    }

    /// Check if a write operation is allowed
    fn check_write(&self, tenant_id: &str) -> Result<QuotaAction, TenancyError> {
        let keys = self.key_count.load(Ordering::Relaxed);
        if keys >= self.limits.max_keys as i64 {
            self.violations.fetch_add(1, Ordering::Relaxed);
            return self.apply_exceeded_policy(TenancyError::KeyQuotaExceeded {
                tenant_id: tenant_id.to_string(),
                count: keys as u64,
                limit: self.limits.max_keys,
            });
        }

        let memory = self.memory_used.load(Ordering::Relaxed);
        if memory >= self.limits.memory_bytes as i64 {
            self.violations.fetch_add(1, Ordering::Relaxed);
            return self.apply_exceeded_policy(TenancyError::MemoryQuotaExceeded {
                tenant_id: tenant_id.to_string(),
                used: memory as u64,
                limit: self.limits.memory_bytes,
            });
        }

        // Check warning thresholds
        let memory_pct =
            (memory.max(0) as f64 / self.limits.memory_bytes as f64) * 100.0;
        let key_pct = (keys.max(0) as f64 / self.limits.max_keys as f64) * 100.0;

        if memory_pct >= self.policy.warning_threshold_percent
            || key_pct >= self.policy.warning_threshold_percent
        {
            return Ok(self.apply_warning_policy(memory_pct, key_pct));
        }

        Ok(QuotaAction::Allow)
    }

    /// Check read operations for warning-level usage
    fn check_read_warning(&self) -> QuotaAction {
        let memory = self.memory_used.load(Ordering::Relaxed);
        let memory_pct =
            (memory.max(0) as f64 / self.limits.memory_bytes as f64) * 100.0;
        if memory_pct >= self.policy.warning_threshold_percent {
            match &self.policy.on_warning {
                QuotaBehavior::Warn => QuotaAction::Warn(format!(
                    "memory usage at {memory_pct:.1}%"
                )),
                _ => QuotaAction::Allow,
            }
        } else {
            QuotaAction::Allow
        }
    }

    /// Apply the exceeded policy — may reject or downgrade to warn/throttle
    fn apply_exceeded_policy(
        &self,
        error: TenancyError,
    ) -> Result<QuotaAction, TenancyError> {
        match &self.policy.on_exceeded {
            QuotaBehavior::Reject => Err(error),
            QuotaBehavior::Warn => {
                tracing::warn!(%error, "quota exceeded (warn mode)");
                Ok(QuotaAction::Warn(error.to_string()))
            }
            QuotaBehavior::Throttle { delay_ms } => {
                tracing::warn!(%error, delay_ms, "quota exceeded (throttle mode)");
                Ok(QuotaAction::Throttle {
                    delay_ms: *delay_ms,
                })
            }
        }
    }

    /// Apply the warning policy
    fn apply_warning_policy(&self, memory_pct: f64, key_pct: f64) -> QuotaAction {
        match &self.policy.on_warning {
            QuotaBehavior::Reject => QuotaAction::Allow, // warnings don't reject
            QuotaBehavior::Warn => QuotaAction::Warn(format!(
                "approaching quota: memory {memory_pct:.1}%, keys {key_pct:.1}%"
            )),
            QuotaBehavior::Throttle { delay_ms } => QuotaAction::Throttle {
                delay_ms: *delay_ms,
            },
        }
    }

    /// Check if a new connection is allowed
    pub fn check_connection(&self, tenant_id: &str) -> Result<(), TenancyError> {
        let connections = self.connection_count.load(Ordering::Relaxed);
        if connections >= self.limits.max_connections {
            self.violations.fetch_add(1, Ordering::Relaxed);
            return Err(TenancyError::ConnectionQuotaExceeded {
                tenant_id: tenant_id.to_string(),
                count: connections,
                limit: self.limits.max_connections,
            });
        }
        Ok(())
    }

    /// Check key size against limit
    pub fn check_key_size(&self, size: usize, tenant_id: &str) -> Result<(), TenancyError> {
        if size > self.limits.max_key_size {
            return Err(TenancyError::KeyTooLarge {
                tenant_id: tenant_id.to_string(),
                size,
                limit: self.limits.max_key_size,
            });
        }
        Ok(())
    }

    /// Check value size against limit
    pub fn check_value_size(&self, size: usize, tenant_id: &str) -> Result<(), TenancyError> {
        if size > self.limits.max_value_size {
            return Err(TenancyError::ValueTooLarge {
                tenant_id: tenant_id.to_string(),
                size,
                limit: self.limits.max_value_size,
            });
        }
        Ok(())
    }

    /// Record memory change
    pub fn record_memory(&self, delta: i64) {
        self.memory_used.fetch_add(delta, Ordering::Relaxed);
    }

    /// Record key count change
    pub fn record_keys(&self, delta: i64) {
        self.key_count.fetch_add(delta, Ordering::Relaxed);
    }

    /// Record connection change
    pub fn record_connection(&self, delta: i64) {
        if delta > 0 {
            self.connection_count
                .fetch_add(delta as u64, Ordering::Relaxed);
        } else {
            self.connection_count
                .fetch_sub((-delta) as u64, Ordering::Relaxed);
        }
    }

    /// Get current usage
    pub fn get_usage(&self) -> TenantUsage {
        TenantUsage {
            memory_bytes: self.memory_used.load(Ordering::Relaxed).max(0) as u64,
            key_count: self.key_count.load(Ordering::Relaxed).max(0) as u64,
            connection_count: self.connection_count.load(Ordering::Relaxed),
            memory_limit: self.limits.memory_bytes,
            key_limit: self.limits.max_keys,
            connection_limit: self.limits.max_connections,
        }
    }

    /// Get quota status
    pub fn status(&self) -> QuotaStatus {
        let memory_used = self.memory_used.load(Ordering::Relaxed).max(0) as u64;
        let key_count = self.key_count.load(Ordering::Relaxed).max(0) as u64;
        let connections = self.connection_count.load(Ordering::Relaxed);

        QuotaStatus {
            memory_usage_percent: (memory_used as f64 / self.limits.memory_bytes as f64) * 100.0,
            key_usage_percent: (key_count as f64 / self.limits.max_keys as f64) * 100.0,
            connection_usage_percent: (connections as f64 / self.limits.max_connections as f64)
                * 100.0,
            is_memory_exceeded: memory_used >= self.limits.memory_bytes,
            is_key_exceeded: key_count >= self.limits.max_keys,
            is_connection_exceeded: connections >= self.limits.max_connections,
            total_violations: self.violations.load(Ordering::Relaxed),
        }
    }

    /// Get limits
    pub fn limits(&self) -> &TenantLimits {
        &self.limits
    }

    /// Get policy
    pub fn policy(&self) -> &QuotaPolicy {
        &self.policy
    }

    /// Reset usage counters (for testing)
    pub fn reset(&self) {
        self.memory_used.store(0, Ordering::Relaxed);
        self.key_count.store(0, Ordering::Relaxed);
        self.connection_count.store(0, Ordering::Relaxed);
    }
}

/// Quota status summary
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuotaStatus {
    /// Memory usage percentage
    pub memory_usage_percent: f64,
    /// Key usage percentage
    pub key_usage_percent: f64,
    /// Connection usage percentage
    pub connection_usage_percent: f64,
    /// Whether memory quota is exceeded
    pub is_memory_exceeded: bool,
    /// Whether key quota is exceeded
    pub is_key_exceeded: bool,
    /// Whether connection quota is exceeded
    pub is_connection_exceeded: bool,
    /// Total quota violations
    pub total_violations: u64,
}

impl QuotaStatus {
    /// Check if any quota is exceeded
    pub fn any_exceeded(&self) -> bool {
        self.is_memory_exceeded || self.is_key_exceeded || self.is_connection_exceeded
    }

    /// Check if any quota is near limit (>80%)
    pub fn any_warning(&self) -> bool {
        self.memory_usage_percent > 80.0
            || self.key_usage_percent > 80.0
            || self.connection_usage_percent > 80.0
    }
}

/// Quota violation record
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuotaViolation {
    /// Timestamp
    pub timestamp: u64,
    /// Violation type
    pub violation_type: ViolationType,
    /// Current value
    pub current_value: u64,
    /// Limit value
    pub limit_value: u64,
}

/// Types of quota violations
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ViolationType {
    /// Memory limit exceeded
    Memory,
    /// Key limit exceeded
    Keys,
    /// Connection limit exceeded
    Connections,
    /// Rate limit exceeded
    RateLimit,
    /// Key size exceeded
    KeySize,
    /// Value size exceeded
    ValueSize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_limits() -> TenantLimits {
        TenantLimits {
            memory_bytes: 1000,
            max_keys: 100,
            ops_per_second: 1000,
            max_connections: 10,
            max_databases: 16,
            max_key_size: 256,
            max_value_size: 1024,
        }
    }

    #[test]
    fn test_quota_enforcer_creation() {
        let enforcer = QuotaEnforcer::new(create_test_limits(), QuotaPolicy::default());
        let status = enforcer.status();

        assert!(!status.any_exceeded());
        assert_eq!(status.total_violations, 0);
    }

    #[test]
    fn test_memory_quota_reject() {
        let enforcer = QuotaEnforcer::new(create_test_limits(), QuotaPolicy::default());

        enforcer.record_memory(1000);

        let result = enforcer.check(OperationType::Write, "t1");
        assert!(result.is_err());

        let status = enforcer.status();
        assert!(status.is_memory_exceeded);
    }

    #[test]
    fn test_memory_quota_warn_mode() {
        let policy = QuotaPolicy {
            on_exceeded: QuotaBehavior::Warn,
            ..Default::default()
        };
        let enforcer = QuotaEnforcer::new(create_test_limits(), policy);

        enforcer.record_memory(1000);

        // In warn mode, exceeded quota returns Ok(Warn) instead of Err
        let result = enforcer.check(OperationType::Write, "t1");
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), QuotaAction::Warn(_)));
    }

    #[test]
    fn test_memory_quota_throttle_mode() {
        let policy = QuotaPolicy {
            on_exceeded: QuotaBehavior::Throttle { delay_ms: 100 },
            ..Default::default()
        };
        let enforcer = QuotaEnforcer::new(create_test_limits(), policy);

        enforcer.record_memory(1000);

        let result = enforcer.check(OperationType::Write, "t1");
        assert!(result.is_ok());
        assert!(matches!(
            result.unwrap(),
            QuotaAction::Throttle { delay_ms: 100 }
        ));
    }

    #[test]
    fn test_key_quota() {
        let enforcer = QuotaEnforcer::new(create_test_limits(), QuotaPolicy::default());

        enforcer.record_keys(100);

        let result = enforcer.check(OperationType::Write, "t1");
        assert!(result.is_err());

        let status = enforcer.status();
        assert!(status.is_key_exceeded);
    }

    #[test]
    fn test_connection_quota() {
        let enforcer = QuotaEnforcer::new(create_test_limits(), QuotaPolicy::default());

        for _ in 0..10 {
            enforcer.record_connection(1);
        }

        let result = enforcer.check_connection("t1");
        assert!(result.is_err());

        let status = enforcer.status();
        assert!(status.is_connection_exceeded);
    }

    #[test]
    fn test_key_size_validation() {
        let enforcer = QuotaEnforcer::new(create_test_limits(), QuotaPolicy::default());

        assert!(enforcer.check_key_size(100, "t1").is_ok());
        assert!(enforcer.check_key_size(300, "t1").is_err());
    }

    #[test]
    fn test_value_size_validation() {
        let enforcer = QuotaEnforcer::new(create_test_limits(), QuotaPolicy::default());

        assert!(enforcer.check_value_size(512, "t1").is_ok());
        assert!(enforcer.check_value_size(2048, "t1").is_err());
    }

    #[test]
    fn test_read_operations_allowed() {
        let enforcer = QuotaEnforcer::new(create_test_limits(), QuotaPolicy::default());

        // Even at quota, reads should be allowed
        enforcer.record_memory(1000);
        enforcer.record_keys(100);

        let result = enforcer.check(OperationType::Read, "t1");
        assert!(result.is_ok());
    }

    #[test]
    fn test_usage_tracking() {
        let enforcer = QuotaEnforcer::new(create_test_limits(), QuotaPolicy::default());

        enforcer.record_memory(500);
        enforcer.record_keys(50);
        enforcer.record_connection(5);

        let usage = enforcer.get_usage();
        assert_eq!(usage.memory_bytes, 500);
        assert_eq!(usage.key_count, 50);
        assert_eq!(usage.connection_count, 5);
    }

    #[test]
    fn test_quota_warning_threshold() {
        let policy = QuotaPolicy {
            warning_threshold_percent: 80.0,
            on_warning: QuotaBehavior::Warn,
            ..Default::default()
        };
        let enforcer = QuotaEnforcer::new(create_test_limits(), policy);

        // Set memory to 85% of limit
        enforcer.record_memory(850);

        let result = enforcer.check(OperationType::Write, "t1");
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), QuotaAction::Warn(_)));

        let status = enforcer.status();
        assert!(status.any_warning());
        assert!(!status.any_exceeded());
    }
}
