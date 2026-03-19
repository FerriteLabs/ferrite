//! Per-tenant quota enforcement for memory records.

use std::collections::HashMap;
use std::fmt;

/// Quota limits for a single tenant.
pub struct TenantQuota {
    pub max_records: u64,
    pub max_bytes: u64,
}

/// Per-tenant quota enforcer.  Stores per-tenant overrides and a default
/// quota applied to tenants without an explicit override.
pub struct QuotaEnforcer {
    quotas: HashMap<String, TenantQuota>,
    default_quota: TenantQuota,
}

impl QuotaEnforcer {
    /// Create a new enforcer with the given defaults applied to all tenants
    /// that do not have an explicit override.
    pub fn new(default_max_records: u64, default_max_bytes: u64) -> Self {
        Self {
            quotas: HashMap::new(),
            default_quota: TenantQuota {
                max_records: default_max_records,
                max_bytes: default_max_bytes,
            },
        }
    }

    /// Set (or replace) the quota for a specific tenant.
    pub fn set_quota(&mut self, tenant: &str, quota: TenantQuota) {
        self.quotas.insert(tenant.to_string(), quota);
    }

    /// Check whether the tenant's current usage is within quota.
    pub fn check(
        &self,
        tenant: &str,
        current_records: u64,
        current_bytes: u64,
    ) -> Result<(), QuotaError> {
        let q = self.quotas.get(tenant).unwrap_or(&self.default_quota);
        if current_records >= q.max_records {
            return Err(QuotaError::RecordLimitExceeded {
                current: current_records,
                max: q.max_records,
            });
        }
        if current_bytes >= q.max_bytes {
            return Err(QuotaError::ByteLimitExceeded {
                current: current_bytes,
                max: q.max_bytes,
            });
        }
        Ok(())
    }
}

/// Error returned when a tenant exceeds a quota.
#[derive(Debug)]
pub enum QuotaError {
    RecordLimitExceeded { current: u64, max: u64 },
    ByteLimitExceeded { current: u64, max: u64 },
}

impl fmt::Display for QuotaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QuotaError::RecordLimitExceeded { current, max } => {
                write!(f, "record limit exceeded: {current}/{max}")
            }
            QuotaError::ByteLimitExceeded { current, max } => {
                write!(f, "byte limit exceeded: {current}/{max}")
            }
        }
    }
}

impl std::error::Error for QuotaError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn within_quota_passes() {
        let enforcer = QuotaEnforcer::new(100, 1024);
        assert!(enforcer.check("acme", 50, 512).is_ok());
    }

    #[test]
    fn record_limit_exceeded() {
        let enforcer = QuotaEnforcer::new(100, 1024);
        let err = enforcer.check("acme", 100, 0).unwrap_err();
        assert!(matches!(
            err,
            QuotaError::RecordLimitExceeded {
                current: 100,
                max: 100
            }
        ));
    }

    #[test]
    fn per_tenant_override_applies() {
        let mut enforcer = QuotaEnforcer::new(100, 1024);
        enforcer.set_quota(
            "vip",
            TenantQuota {
                max_records: 1_000_000,
                max_bytes: 10_000_000,
            },
        );
        // VIP tenant passes with high usage
        assert!(enforcer.check("vip", 500_000, 5_000_000).is_ok());
        // Regular tenant fails at the same usage
        let err = enforcer.check("regular", 500_000, 5_000_000).unwrap_err();
        assert!(matches!(err, QuotaError::RecordLimitExceeded { .. }));
    }
}
