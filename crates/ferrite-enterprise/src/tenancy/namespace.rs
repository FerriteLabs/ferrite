//! Tenant namespace isolation
//!
//! Provides automatic key prefixing, namespace-scoped commands,
//! and cross-tenant data leak prevention.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Separator used between tenant prefix and user key
const TENANT_PREFIX: &str = "__tenant:";
const TENANT_SEP: char = ':';

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors related to tenant isolation and namespace enforcement
#[derive(Debug, Clone, thiserror::Error)]
pub enum TenantIsolationError {
    /// Attempted to access a key belonging to another tenant
    #[error(
        "cross-tenant access denied: tenant '{requesting}' cannot access keys of tenant '{target}'"
    )]
    CrossTenantAccess {
        /// The tenant that made the request
        requesting: String,
        /// The tenant whose key was targeted
        target: String,
    },

    /// The namespace identifier is invalid (empty, contains separator, etc.)
    #[error("invalid namespace: {reason}")]
    InvalidNamespace {
        /// Description of why the namespace is invalid
        reason: String,
    },

    /// A key does not belong to the current tenant's scope
    #[error("key not in scope for tenant '{tenant_id}'")]
    KeyNotInScope {
        /// Tenant that attempted the access
        tenant_id: String,
    },

    /// A pub/sub channel does not belong to the current tenant's scope
    #[error("channel not in scope for tenant '{tenant_id}': {channel}")]
    ChannelNotInScope {
        /// Tenant that attempted the access
        tenant_id: String,
        /// The channel name
        channel: String,
    },
}

// ---------------------------------------------------------------------------
// NamespacePolicy
// ---------------------------------------------------------------------------

/// Policy governing how namespaces are enforced for a tenant
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NamespacePolicy {
    /// All keys must carry the tenant prefix; cross-tenant access is always denied.
    Strict,
    /// Certain key patterns are shared across tenants (e.g. global config).
    Shared {
        /// Set of key prefixes that are accessible by every tenant.
        shared_prefixes: HashSet<String>,
    },
    /// Per-key-pattern rules evaluated in order; first match wins.
    Custom {
        /// Rules evaluated in declaration order.
        rules: Vec<NamespaceRule>,
    },
}

impl Default for NamespacePolicy {
    fn default() -> Self {
        Self::Strict
    }
}

/// A single rule inside a [`NamespacePolicy::Custom`] policy.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NamespaceRule {
    /// Glob-style pattern matched against the *user-visible* key (without tenant prefix).
    pub pattern: String,
    /// Whether matching keys are accessible.
    pub allow: bool,
}

// ---------------------------------------------------------------------------
// NamespaceManager
// ---------------------------------------------------------------------------

/// Manages tenant key namespace isolation.
///
/// All key operations are routed through this struct to guarantee that
/// each tenant's data lives in a separate namespace and that
/// cross-tenant data leakage is impossible.
#[derive(Clone, Debug)]
pub struct NamespaceManager {
    /// The policy in effect.
    policy: NamespacePolicy,
}

impl NamespaceManager {
    /// Create a new `NamespaceManager` with the given policy.
    pub fn new(policy: NamespacePolicy) -> Self {
        Self { policy }
    }

    /// Create a `NamespaceManager` with [`NamespacePolicy::Strict`].
    pub fn strict() -> Self {
        Self::new(NamespacePolicy::Strict)
    }

    /// Return the prefix string for a given tenant (e.g. `__tenant:acme:`).
    fn prefix(tenant_id: &str) -> String {
        format!("{TENANT_PREFIX}{tenant_id}{TENANT_SEP}")
    }

    // -- Key scoping --------------------------------------------------------

    /// Prefix `key` with the tenant namespace.
    ///
    /// ```text
    /// scope_key("acme", b"user:1") => b"__tenant:acme:user:1"
    /// ```
    pub fn scope_key(tenant_id: &str, key: &[u8]) -> Vec<u8> {
        let prefix = Self::prefix(tenant_id);
        let mut scoped = Vec::with_capacity(prefix.len() + key.len());
        scoped.extend_from_slice(prefix.as_bytes());
        scoped.extend_from_slice(key);
        scoped
    }

    /// Strip the tenant prefix from `key`, returning the user-visible key.
    ///
    /// Returns `None` if the key does not belong to `tenant_id`.
    pub fn unscope_key(tenant_id: &str, key: &[u8]) -> Option<Vec<u8>> {
        let prefix = Self::prefix(tenant_id);
        let prefix_bytes = prefix.as_bytes();
        if key.starts_with(prefix_bytes) {
            Some(key[prefix_bytes.len()..].to_vec())
        } else {
            None
        }
    }

    /// Scope a `KEYS` / `SCAN` glob pattern into the tenant namespace.
    ///
    /// ```text
    /// scope_pattern("acme", "user:*") => "__tenant:acme:user:*"
    /// ```
    pub fn scope_pattern(tenant_id: &str, pattern: &str) -> String {
        format!("{}{}", Self::prefix(tenant_id), pattern)
    }

    /// Check whether `key` belongs to `tenant_id`.
    pub fn validate_key_access(tenant_id: &str, key: &[u8]) -> bool {
        let prefix = Self::prefix(tenant_id);
        key.starts_with(prefix.as_bytes())
    }

    // -- Channel scoping ----------------------------------------------------

    /// Scope a pub/sub channel name into the tenant namespace.
    ///
    /// ```text
    /// scope_channel("acme", "notifications") => "__tenant:acme:notifications"
    /// ```
    pub fn scope_channel(tenant_id: &str, channel: &str) -> String {
        format!("{}{}", Self::prefix(tenant_id), channel)
    }

    /// Strip the tenant prefix from a scoped channel name.
    ///
    /// Returns `None` if the channel does not belong to `tenant_id`.
    pub fn unscope_channel(tenant_id: &str, channel: &str) -> Option<String> {
        let prefix = Self::prefix(tenant_id);
        channel.strip_prefix(&prefix).map(|s| s.to_string())
    }

    // -- Policy-aware helpers -----------------------------------------------

    /// Check if a *user-visible* key is allowed under the current policy.
    ///
    /// For [`NamespacePolicy::Strict`] this always returns `true` (the
    /// namespace prefix itself provides the isolation). For `Shared` and
    /// `Custom` policies the key is checked against the configured rules.
    pub fn is_key_allowed(&self, _tenant_id: &str, user_key: &[u8]) -> bool {
        match &self.policy {
            NamespacePolicy::Strict => true,
            NamespacePolicy::Shared { shared_prefixes } => {
                // Shared prefixes bypass isolation
                if let Ok(key_str) = std::str::from_utf8(user_key) {
                    for prefix in shared_prefixes {
                        if key_str.starts_with(prefix.as_str()) {
                            return true;
                        }
                    }
                }
                // Non-shared keys are scoped normally and always allowed for
                // the owning tenant.
                true
            }
            NamespacePolicy::Custom { rules } => {
                if let Ok(key_str) = std::str::from_utf8(user_key) {
                    for rule in rules {
                        if glob_match(&rule.pattern, key_str) {
                            return rule.allow;
                        }
                    }
                }
                // No rule matched → default allow (scoped to tenant anyway)
                true
            }
        }
    }
}

impl Default for NamespaceManager {
    fn default() -> Self {
        Self::strict()
    }
}

// ---------------------------------------------------------------------------
// CrossTenantGuard
// ---------------------------------------------------------------------------

/// Guard that prevents multi-key operations (MGET, MSET, Lua scripts, etc.)
/// from touching keys belonging to different tenants.
#[derive(Clone, Debug)]
pub struct CrossTenantGuard;

impl CrossTenantGuard {
    /// Validate that **all** keys in a multi-key operation belong to
    /// `tenant_id`.
    ///
    /// Used for MGET, MSET, DEL with multiple keys, RENAME, etc.
    pub fn validate_multi_key_op(
        tenant_id: &str,
        keys: &[&[u8]],
    ) -> Result<(), TenantIsolationError> {
        let prefix = NamespaceManager::prefix(tenant_id);
        let prefix_bytes = prefix.as_bytes();

        for key in keys {
            if !key.starts_with(prefix_bytes) {
                let target = extract_tenant_id(key).unwrap_or_else(|| "<unknown>".to_string());
                return Err(TenantIsolationError::CrossTenantAccess {
                    requesting: tenant_id.to_string(),
                    target,
                });
            }
        }
        Ok(())
    }

    /// Validate that all keys accessed by a Lua/scripting call belong to
    /// `tenant_id`.
    ///
    /// Semantically identical to [`validate_multi_key_op`](Self::validate_multi_key_op)
    /// but kept as a separate entry point for auditing purposes.
    pub fn validate_script(tenant_id: &str, keys: &[&[u8]]) -> Result<(), TenantIsolationError> {
        Self::validate_multi_key_op(tenant_id, keys)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Try to extract the tenant id from a scoped key (`__tenant:<id>:…`).
fn extract_tenant_id(scoped_key: &[u8]) -> Option<String> {
    let s = std::str::from_utf8(scoped_key).ok()?;
    let rest = s.strip_prefix(TENANT_PREFIX)?;
    let end = rest.find(TENANT_SEP)?;
    Some(rest[..end].to_string())
}

/// Minimal glob matcher supporting `*` (any chars) and `?` (single char).
fn glob_match(pattern: &str, value: &str) -> bool {
    let mut p = pattern.chars().peekable();
    let mut v = value.chars().peekable();

    while p.peek().is_some() || v.peek().is_some() {
        match p.peek() {
            Some('*') => {
                p.next();
                // '*' at end matches everything
                if p.peek().is_none() {
                    return true;
                }
                // Try to match the rest starting from every remaining position
                let rest_pattern: String = p.collect();
                let remaining: String = v.collect();
                for i in 0..=remaining.len() {
                    if glob_match(&rest_pattern, &remaining[i..]) {
                        return true;
                    }
                }
                return false;
            }
            Some('?') => {
                p.next();
                if v.next().is_none() {
                    return false;
                }
            }
            Some(&pc) => {
                p.next();
                match v.next() {
                    Some(vc) if vc == pc => {}
                    _ => return false,
                }
            }
            None => return false,
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scope_unscope_key_roundtrip() {
        let scoped = NamespaceManager::scope_key("acme", b"user:1");
        assert!(scoped.starts_with(b"__tenant:acme:"));
        assert!(scoped.ends_with(b"user:1"));

        let unscoped = NamespaceManager::unscope_key("acme", &scoped).unwrap();
        assert_eq!(unscoped, b"user:1");
    }

    #[test]
    fn test_unscope_wrong_tenant_returns_none() {
        let scoped = NamespaceManager::scope_key("acme", b"key");
        assert!(NamespaceManager::unscope_key("other", &scoped).is_none());
    }

    #[test]
    fn test_scope_pattern() {
        let pat = NamespaceManager::scope_pattern("acme", "user:*");
        assert_eq!(pat, "__tenant:acme:user:*");
    }

    #[test]
    fn test_validate_key_access() {
        let scoped = NamespaceManager::scope_key("acme", b"k");
        assert!(NamespaceManager::validate_key_access("acme", &scoped));
        assert!(!NamespaceManager::validate_key_access("other", &scoped));
    }

    #[test]
    fn test_scope_unscope_channel() {
        let scoped = NamespaceManager::scope_channel("acme", "notifications");
        assert_eq!(scoped, "__tenant:acme:notifications");

        let unscoped = NamespaceManager::unscope_channel("acme", &scoped).unwrap();
        assert_eq!(unscoped, "notifications");
        assert!(NamespaceManager::unscope_channel("other", &scoped).is_none());
    }

    #[test]
    fn test_cross_tenant_guard_all_same_tenant() {
        let k1 = NamespaceManager::scope_key("acme", b"a");
        let k2 = NamespaceManager::scope_key("acme", b"b");
        let keys: Vec<&[u8]> = vec![&k1, &k2];
        assert!(CrossTenantGuard::validate_multi_key_op("acme", &keys).is_ok());
    }

    #[test]
    fn test_cross_tenant_guard_mixed_tenants() {
        let k1 = NamespaceManager::scope_key("acme", b"a");
        let k2 = NamespaceManager::scope_key("other", b"b");
        let keys: Vec<&[u8]> = vec![&k1, &k2];
        let err = CrossTenantGuard::validate_multi_key_op("acme", &keys).unwrap_err();
        assert!(matches!(
            err,
            TenantIsolationError::CrossTenantAccess { .. }
        ));
    }

    #[test]
    fn test_cross_tenant_guard_script() {
        let k1 = NamespaceManager::scope_key("acme", b"x");
        let keys: Vec<&[u8]> = vec![&k1];
        assert!(CrossTenantGuard::validate_script("acme", &keys).is_ok());
    }

    #[test]
    fn test_namespace_policy_shared() {
        let mut shared = HashSet::new();
        shared.insert("__global:".to_string());
        let mgr = NamespaceManager::new(NamespacePolicy::Shared {
            shared_prefixes: shared,
        });
        assert!(mgr.is_key_allowed("acme", b"__global:config"));
        assert!(mgr.is_key_allowed("acme", b"user:123"));
    }

    #[test]
    fn test_namespace_policy_custom_deny() {
        let mgr = NamespaceManager::new(NamespacePolicy::Custom {
            rules: vec![NamespaceRule {
                pattern: "admin:*".to_string(),
                allow: false,
            }],
        });
        assert!(!mgr.is_key_allowed("acme", b"admin:secret"));
        assert!(mgr.is_key_allowed("acme", b"user:1")); // no rule → allow
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("user:*", "user:123"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("us?r", "user"));
        assert!(!glob_match("us?r", "useer"));
        assert!(glob_match("a*b", "aXYZb"));
        assert!(!glob_match("a*b", "aXYZc"));
    }

    #[test]
    fn test_extract_tenant_id() {
        assert_eq!(
            extract_tenant_id(b"__tenant:acme:foo"),
            Some("acme".to_string())
        );
        assert_eq!(extract_tenant_id(b"random-key"), None);
    }
}
