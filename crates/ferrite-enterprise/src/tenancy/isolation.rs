//! Tenant Isolation
//!
//! Cross-tenant access prevention and namespace enforcement at the
//! protocol level. Every key operation must pass through the
//! [`IsolationEnforcer`] to guarantee tenants cannot read or write
//! each other's data.

use super::{TenancyError, Tenant, TenantContext};

/// Validates that a key belongs to the current tenant's namespace.
pub struct KeyValidator;

impl KeyValidator {
    /// Ensure `scoped_key` starts with the expected tenant prefix.
    /// Returns `Ok(())` on success or a cross-tenant access error.
    pub fn validate(tenant: &Tenant, scoped_key: &[u8]) -> Result<(), TenancyError> {
        if !tenant.owns_key(scoped_key) {
            return Err(TenancyError::CrossTenantAccess {
                requesting: tenant.id.clone(),
                target: Self::extract_tenant_id(scoped_key)
                    .unwrap_or_else(|| "<unknown>".to_string()),
            });
        }
        Ok(())
    }

    /// Try to extract the tenant ID from a scoped key.
    fn extract_tenant_id(scoped_key: &[u8]) -> Option<String> {
        let s = std::str::from_utf8(scoped_key).ok()?;
        let rest = s.strip_prefix("__tenant:")?;
        let end = rest.find(':')?;
        Some(rest[..end].to_string())
    }
}

/// Protocol-level isolation enforcer that wraps all key access.
#[derive(Clone)]
pub struct IsolationEnforcer {
    /// Whether isolation is enabled
    enabled: bool,
    /// Whether admin cross-tenant access is allowed
    allow_cross_tenant_admin: bool,
}

impl IsolationEnforcer {
    /// Create a new isolation enforcer.
    pub fn new(enabled: bool, allow_cross_tenant_admin: bool) -> Self {
        Self {
            enabled,
            allow_cross_tenant_admin,
        }
    }

    /// Scope a user-provided key into the tenant's namespace.
    /// Returns the scoped key bytes.
    pub fn scope_key(&self, ctx: &TenantContext, user_key: &[u8]) -> Vec<u8> {
        let tenant = ctx.tenant.read();
        tenant.scope_key(user_key)
    }

    /// Unscope a stored key back to the user-visible form.
    /// Returns `None` if the key does not belong to the tenant.
    pub fn unscope_key(&self, ctx: &TenantContext, scoped_key: &[u8]) -> Option<Vec<u8>> {
        let tenant = ctx.tenant.read();
        tenant.unscope_key(scoped_key).map(|k| k.to_vec())
    }

    /// Validate that a scoped key belongs to the given tenant.
    /// Used before any read/write to prevent cross-tenant leakage.
    pub fn validate_access(
        &self,
        ctx: &TenantContext,
        scoped_key: &[u8],
        is_admin: bool,
    ) -> Result<(), TenancyError> {
        if !self.enabled {
            return Ok(());
        }

        if is_admin && self.allow_cross_tenant_admin {
            return Ok(());
        }

        let tenant = ctx.tenant.read();
        KeyValidator::validate(&tenant, scoped_key)
    }

    /// Validate key and value sizes against tenant limits.
    pub fn validate_kv_sizes(
        &self,
        ctx: &TenantContext,
        key_len: usize,
        value_len: usize,
    ) -> Result<(), TenancyError> {
        ctx.validate_key_size(key_len)?;
        ctx.validate_value_size(value_len)?;
        Ok(())
    }

    /// Convenience: scope + validate sizes in one call for writes.
    pub fn prepare_write(
        &self,
        ctx: &TenantContext,
        user_key: &[u8],
        value_len: usize,
    ) -> Result<Vec<u8>, TenancyError> {
        self.validate_kv_sizes(ctx, user_key.len(), value_len)?;
        Ok(self.scope_key(ctx, user_key))
    }

    /// Convenience: scope key for reads (no value-size check needed).
    pub fn prepare_read(
        &self,
        ctx: &TenantContext,
        user_key: &[u8],
    ) -> Result<Vec<u8>, TenancyError> {
        ctx.validate_key_size(user_key.len())?;
        Ok(self.scope_key(ctx, user_key))
    }
}

impl Default for IsolationEnforcer {
    fn default() -> Self {
        Self::new(true, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenancy::{Tenant, TenantContext, TenantLimits};
    use std::sync::Arc;

    fn make_ctx(id: &str) -> Arc<TenantContext> {
        let tenant = Tenant::new(id, id).with_limits(TenantLimits {
            max_key_size: 64,
            max_value_size: 256,
            ..TenantLimits::default()
        });
        Arc::new(TenantContext::new(tenant))
    }

    #[test]
    fn test_scope_unscope_roundtrip() {
        let enforcer = IsolationEnforcer::default();
        let ctx = make_ctx("acme");

        let scoped = enforcer.scope_key(&ctx, b"user:1");
        assert!(scoped.starts_with(b"__tenant:acme:"));

        let unscoped = enforcer.unscope_key(&ctx, &scoped).unwrap();
        assert_eq!(unscoped, b"user:1");
    }

    #[test]
    fn test_cross_tenant_access_denied() {
        let enforcer = IsolationEnforcer::default();
        let ctx_a = make_ctx("tenant-a");
        let ctx_b = make_ctx("tenant-b");

        let key_b = enforcer.scope_key(&ctx_b, b"secret");

        // tenant-a trying to access tenant-b's key
        let result = enforcer.validate_access(&ctx_a, &key_b, false);
        assert!(matches!(
            result,
            Err(TenancyError::CrossTenantAccess { .. })
        ));
    }

    #[test]
    fn test_admin_cross_tenant_allowed() {
        let enforcer = IsolationEnforcer::new(true, true);
        let ctx_a = make_ctx("tenant-a");
        let ctx_b = make_ctx("tenant-b");

        let key_b = enforcer.scope_key(&ctx_b, b"secret");

        // admin access allowed
        assert!(enforcer.validate_access(&ctx_a, &key_b, true).is_ok());
        // non-admin still denied
        assert!(enforcer.validate_access(&ctx_a, &key_b, false).is_err());
    }

    #[test]
    fn test_isolation_disabled() {
        let enforcer = IsolationEnforcer::new(false, false);
        let ctx = make_ctx("t1");

        // Any key passes when isolation is disabled
        assert!(enforcer.validate_access(&ctx, b"random-key", false).is_ok());
    }

    #[test]
    fn test_key_size_enforcement() {
        let enforcer = IsolationEnforcer::default();
        let ctx = make_ctx("t1"); // max_key_size = 64

        assert!(enforcer.prepare_read(&ctx, &[0u8; 32]).is_ok());
        assert!(enforcer.prepare_read(&ctx, &[0u8; 128]).is_err());
    }

    #[test]
    fn test_value_size_enforcement() {
        let enforcer = IsolationEnforcer::default();
        let ctx = make_ctx("t1"); // max_value_size = 256

        assert!(enforcer.prepare_write(&ctx, b"key", 100).is_ok());
        assert!(enforcer.prepare_write(&ctx, b"key", 512).is_err());
    }

    #[test]
    fn test_key_validator_extract_tenant_id() {
        assert_eq!(
            KeyValidator::extract_tenant_id(b"__tenant:acme:foo"),
            Some("acme".to_string())
        );
        assert_eq!(KeyValidator::extract_tenant_id(b"random-key"), None);
    }
}
