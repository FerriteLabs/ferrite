//! Multi-Tenancy Native Support
//!
//! First-class tenant isolation with resource limits, billing metrics,
//! and zero-downtime tenant migration.
//!
//! # Features
//!
//! - Namespace-based key isolation per tenant
//! - Per-tenant memory, connection, key count and key/value size limits
//! - Token-bucket rate limiting per tenant
//! - Configurable quota enforcement (reject, throttle, warn)
//! - Tenant lifecycle: create, suspend, resume, delete with cleanup
//! - Auth-based tenant routing with tenant switching
//! - Per-tenant Prometheus metrics (memory, ops, connections, keys)
//! - Zero-copy tenant migration
//!
//! # Example
//!
//! ```ignore
//! // Create tenant with resource limits
//! TENANT CREATE acme MEMORY 1GB OPS_LIMIT 10000/s KEYS_LIMIT 100000 CONNECTIONS 100
//!
//! // Tenant-scoped operations
//! TENANT USE acme
//! SET foo bar  -- Stored in tenant namespace
//!
//! // Cross-tenant admin operations
//! TENANT STATS acme
//! TENANT MIGRATE acme TO other-cluster --zero-downtime
//! ```

pub mod isolation;
pub mod limits;
pub mod manager;
pub mod metrics;
pub mod migration;
pub mod quota;
pub mod routing;

pub use isolation::{IsolationEnforcer, KeyValidator};
pub use limits::{RateLimiter, ResourceLimits, TenantLimits};
pub use manager::{TenantInfo, TenantManager};
pub use metrics::{TenantMetrics, TenantMetricsCollector, TenantUsage};
pub use migration::{MigrationPlan, MigrationProgress, MigrationStatus, TenantMigrator};
pub use quota::{QuotaAction, QuotaEnforcer, QuotaPolicy, QuotaStatus, QuotaViolation};
pub use routing::{TenantIdentity, TenantRouter, TenantSession};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Configuration for multi-tenancy
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TenancyConfig {
    /// Enable multi-tenancy
    pub enabled: bool,
    /// Default resource limits for new tenants
    pub default_limits: ResourceLimits,
    /// Maximum number of tenants
    pub max_tenants: usize,
    /// Enable tenant isolation (separate namespaces)
    pub isolation_enabled: bool,
    /// Enable per-tenant metrics
    pub metrics_enabled: bool,
    /// Metrics collection interval in seconds
    pub metrics_interval_secs: u64,
    /// Enable tenant billing
    pub billing_enabled: bool,
    /// Allow cross-tenant operations (admin only)
    pub allow_cross_tenant: bool,
    /// Default quota enforcement policy
    pub default_quota_policy: QuotaPolicy,
}

impl Default for TenancyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_limits: ResourceLimits::default(),
            max_tenants: 10_000,
            isolation_enabled: true,
            metrics_enabled: true,
            metrics_interval_secs: 60,
            billing_enabled: true,
            allow_cross_tenant: false,
            default_quota_policy: QuotaPolicy::default(),
        }
    }
}

/// A tenant in the system
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Tenant {
    /// Unique tenant identifier
    pub id: String,
    /// Display name
    pub name: String,
    /// Creation timestamp (ms since epoch)
    pub created_at: u64,
    /// Last update timestamp (ms since epoch)
    pub updated_at: u64,
    /// Last activity timestamp
    pub last_active_at: u64,
    /// Tenant state
    pub state: TenantState,
    /// Resource limits
    pub limits: TenantLimits,
    /// Custom metadata
    pub metadata: HashMap<String, String>,
    /// Eviction policy
    pub eviction_policy: EvictionPolicy,
    /// Tier (for billing)
    pub tier: TenantTier,
    /// Quota enforcement policy
    pub quota_policy: QuotaPolicy,
}

impl Tenant {
    /// Create a new tenant
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        let now = current_timestamp_ms();
        Self {
            id: id.into(),
            name: name.into(),
            created_at: now,
            updated_at: now,
            last_active_at: now,
            state: TenantState::Active,
            limits: TenantLimits::default(),
            metadata: HashMap::new(),
            eviction_policy: EvictionPolicy::Lru,
            tier: TenantTier::Free,
            quota_policy: QuotaPolicy::default(),
        }
    }

    /// Create a tenant with specific limits
    pub fn with_limits(mut self, limits: TenantLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Set the eviction policy
    pub fn with_eviction_policy(mut self, policy: EvictionPolicy) -> Self {
        self.eviction_policy = policy;
        self
    }

    /// Set the tier
    pub fn with_tier(mut self, tier: TenantTier) -> Self {
        self.tier = tier;
        self
    }

    /// Set the quota enforcement policy
    pub fn with_quota_policy(mut self, policy: QuotaPolicy) -> Self {
        self.quota_policy = policy;
        self
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Check if tenant is active
    pub fn is_active(&self) -> bool {
        self.state == TenantState::Active
    }

    /// Check if tenant is suspended
    pub fn is_suspended(&self) -> bool {
        self.state == TenantState::Suspended
    }

    /// Get the key prefix for this tenant
    pub fn key_prefix(&self) -> String {
        format!("__tenant:{}:", self.id)
    }

    /// Transform a user key to a tenant-scoped key
    pub fn scope_key(&self, key: &[u8]) -> Vec<u8> {
        let prefix = self.key_prefix();
        let mut scoped = Vec::with_capacity(prefix.len() + key.len());
        scoped.extend_from_slice(prefix.as_bytes());
        scoped.extend_from_slice(key);
        scoped
    }

    /// Extract the user key from a tenant-scoped key
    pub fn unscope_key<'a>(&self, scoped_key: &'a [u8]) -> Option<&'a [u8]> {
        let prefix = self.key_prefix();
        let prefix_bytes = prefix.as_bytes();
        if scoped_key.starts_with(prefix_bytes) {
            Some(&scoped_key[prefix_bytes.len()..])
        } else {
            None
        }
    }

    /// Check if a scoped key belongs to this tenant
    pub fn owns_key(&self, scoped_key: &[u8]) -> bool {
        let prefix = self.key_prefix();
        scoped_key.starts_with(prefix.as_bytes())
    }
}

/// Tenant state
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TenantState {
    /// Tenant is active and can process requests
    Active,
    /// Tenant is suspended (no operations allowed)
    Suspended,
    /// Tenant is being migrated
    Migrating,
    /// Tenant is being deleted
    Deleting,
    /// Tenant is archived (read-only)
    Archived,
}

impl std::fmt::Display for TenantState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Suspended => write!(f, "suspended"),
            Self::Migrating => write!(f, "migrating"),
            Self::Deleting => write!(f, "deleting"),
            Self::Archived => write!(f, "archived"),
        }
    }
}

/// Tenant tier for billing
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TenantTier {
    /// Free tier
    Free,
    /// Basic tier
    Basic,
    /// Professional tier
    Pro,
    /// Enterprise tier
    Enterprise,
    /// Custom tier
    Custom(String),
}

/// Eviction policy for tenant data
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvictionPolicy {
    /// Least Recently Used
    Lru,
    /// Least Frequently Used
    Lfu,
    /// Time-based TTL
    Ttl,
    /// No eviction (error when full)
    NoEviction,
    /// Random eviction
    Random,
    /// Volatile LRU (only keys with TTL)
    VolatileLru,
}

/// Tenant context for request processing
#[derive(Clone, Debug)]
pub struct TenantContext {
    /// Tenant ID
    pub tenant_id: String,
    /// Tenant info (wrapped in Arc<parking_lot::RwLock> for mutable state updates)
    pub tenant: Arc<parking_lot::RwLock<Tenant>>,
    /// Rate limiter
    rate_limiter: Arc<RateLimiter>,
    /// Quota enforcer
    quota_enforcer: Arc<QuotaEnforcer>,
    /// Request counter
    request_count: Arc<AtomicU64>,
    /// Per-tenant metrics
    metrics: Arc<TenantMetrics>,
}

impl TenantContext {
    /// Create a new tenant context
    pub fn new(tenant: Tenant) -> Self {
        let rate_limiter = Arc::new(RateLimiter::new(tenant.limits.ops_per_second));
        let quota_enforcer = Arc::new(QuotaEnforcer::new(
            tenant.limits.clone(),
            tenant.quota_policy.clone(),
        ));

        Self {
            tenant_id: tenant.id.clone(),
            rate_limiter,
            quota_enforcer,
            request_count: Arc::new(AtomicU64::new(0)),
            metrics: Arc::new(TenantMetrics::new()),
            tenant: Arc::new(parking_lot::RwLock::new(tenant)),
        }
    }

    /// Check if an operation is allowed (rate limiting + quotas)
    pub fn check_operation(&self, op_type: OperationType) -> Result<QuotaAction, TenancyError> {
        let tenant = self.tenant.read();

        // Check if tenant is active
        if tenant.is_suspended() {
            return Err(TenancyError::TenantSuspended(self.tenant_id.clone()));
        }
        if !tenant.is_active() {
            return Err(TenancyError::TenantInactive(self.tenant_id.clone()));
        }
        drop(tenant);

        // Check rate limit
        if !self.rate_limiter.check() {
            self.metrics.record_rate_limit();
            return Err(TenancyError::RateLimitExceeded {
                tenant_id: self.tenant_id.clone(),
                limit: self.rate_limiter.capacity(),
            });
        }

        // Check quotas — returns the action to take
        let action = self.quota_enforcer.check(op_type, &self.tenant_id)?;

        // Increment request counter
        self.request_count.fetch_add(1, Ordering::Relaxed);
        self.metrics.record_command(0);

        Ok(action)
    }

    /// Validate key size against tenant limits
    pub fn validate_key_size(&self, key_len: usize) -> Result<(), TenancyError> {
        self.quota_enforcer.check_key_size(key_len, &self.tenant_id)
    }

    /// Validate value size against tenant limits
    pub fn validate_value_size(&self, value_len: usize) -> Result<(), TenancyError> {
        self.quota_enforcer
            .check_value_size(value_len, &self.tenant_id)
    }

    /// Check if a new connection is allowed
    pub fn check_connection(&self) -> Result<(), TenancyError> {
        self.quota_enforcer.check_connection(&self.tenant_id)
    }

    /// Record a new connection
    pub fn record_connection_open(&self) {
        self.quota_enforcer.record_connection(1);
        let usage = self.quota_enforcer.get_usage();
        self.metrics
            .update_peak_connections(usage.connection_count);
    }

    /// Record a connection close
    pub fn record_connection_close(&self) {
        self.quota_enforcer.record_connection(-1);
    }

    /// Record memory usage
    pub fn record_memory(&self, bytes: i64) {
        self.quota_enforcer.record_memory(bytes);
        let usage = self.quota_enforcer.get_usage();
        self.metrics.update_peak_memory(usage.memory_bytes);
    }

    /// Record key count change
    pub fn record_keys(&self, delta: i64) {
        self.quota_enforcer.record_keys(delta);
    }

    /// Get current usage
    pub fn get_usage(&self) -> TenantUsage {
        self.quota_enforcer.get_usage()
    }

    /// Get request count
    pub fn request_count(&self) -> u64 {
        self.request_count.load(Ordering::Relaxed)
    }

    /// Get per-tenant metrics
    pub fn metrics(&self) -> &Arc<TenantMetrics> {
        &self.metrics
    }

    /// Update tenant state
    pub fn set_state(&self, state: TenantState) {
        let mut tenant = self.tenant.write();
        tenant.state = state;
        tenant.updated_at = current_timestamp_ms();
    }

    /// Get a snapshot of the tenant (cloned)
    pub fn tenant_snapshot(&self) -> Tenant {
        self.tenant.read().clone()
    }
}

/// Operation types for quota enforcement
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OperationType {
    /// Read operation
    Read,
    /// Write operation
    Write,
    /// Delete operation
    Delete,
    /// Scan/iteration operation
    Scan,
    /// Admin operation
    Admin,
}

/// Multi-tenancy errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum TenancyError {
    /// Tenant not found
    #[error("tenant not found: {0}")]
    TenantNotFound(String),

    /// Tenant already exists
    #[error("tenant already exists: {0}")]
    TenantExists(String),

    /// Tenant is inactive
    #[error("tenant is inactive: {0}")]
    TenantInactive(String),

    /// Tenant is suspended
    #[error("tenant is suspended: {0}")]
    TenantSuspended(String),

    /// Rate limit exceeded
    #[error("rate limit exceeded for tenant {tenant_id}: {limit} ops/s")]
    RateLimitExceeded {
        /// Tenant ID
        tenant_id: String,
        /// Limit that was exceeded
        limit: u64,
    },

    /// Memory quota exceeded
    #[error("memory quota exceeded for tenant {tenant_id}: {used} / {limit} bytes")]
    MemoryQuotaExceeded {
        /// Tenant ID
        tenant_id: String,
        /// Current usage
        used: u64,
        /// Limit
        limit: u64,
    },

    /// Key quota exceeded
    #[error("key quota exceeded for tenant {tenant_id}: {count} / {limit} keys")]
    KeyQuotaExceeded {
        /// Tenant ID
        tenant_id: String,
        /// Current count
        count: u64,
        /// Limit
        limit: u64,
    },

    /// Connection quota exceeded
    #[error("connection quota exceeded for tenant {tenant_id}: {count} / {limit}")]
    ConnectionQuotaExceeded {
        /// Tenant ID
        tenant_id: String,
        /// Current count
        count: u64,
        /// Limit
        limit: u64,
    },

    /// Key size exceeded
    #[error("key too large for tenant {tenant_id}: {size} / {limit} bytes")]
    KeyTooLarge {
        /// Tenant ID
        tenant_id: String,
        /// Actual size
        size: usize,
        /// Limit
        limit: usize,
    },

    /// Value size exceeded
    #[error("value too large for tenant {tenant_id}: {size} / {limit} bytes")]
    ValueTooLarge {
        /// Tenant ID
        tenant_id: String,
        /// Actual size
        size: usize,
        /// Limit
        limit: usize,
    },

    /// Cross-tenant access denied
    #[error("cross-tenant access denied: tenant {requesting} cannot access keys of tenant {target}")]
    CrossTenantAccess {
        /// Requesting tenant
        requesting: String,
        /// Target tenant
        target: String,
    },

    /// Maximum tenants reached
    #[error("maximum tenants reached: {0}")]
    MaxTenantsReached(usize),

    /// Migration error
    #[error("migration error: {0}")]
    Migration(String),

    /// Invalid configuration
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// Permission denied
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// No tenant selected for this session
    #[error("no tenant selected: authenticate with TENANT USE <id>")]
    NoTenantSelected,

    /// Internal error
    #[error("internal error: {0}")]
    Internal(String),
}

/// Get current timestamp in milliseconds
pub(crate) fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_creation() {
        let tenant = Tenant::new("acme", "Acme Corp");
        assert_eq!(tenant.id, "acme");
        assert_eq!(tenant.name, "Acme Corp");
        assert!(tenant.is_active());
        assert!(!tenant.is_suspended());
        assert!(tenant.updated_at >= tenant.created_at);
    }

    #[test]
    fn test_tenant_key_scoping() {
        let tenant = Tenant::new("test", "Test");
        let key = b"user:123";
        let scoped = tenant.scope_key(key);

        assert!(scoped.starts_with(b"__tenant:test:"));
        assert!(scoped.ends_with(b"user:123"));

        let unscoped = tenant.unscope_key(&scoped);
        assert_eq!(unscoped, Some(key.as_slice()));
    }

    #[test]
    fn test_tenant_owns_key() {
        let tenant = Tenant::new("test", "Test");
        let scoped = tenant.scope_key(b"mykey");
        assert!(tenant.owns_key(&scoped));
        assert!(!tenant.owns_key(b"__tenant:other:mykey"));
    }

    #[test]
    fn test_tenancy_config_default() {
        let config = TenancyConfig::default();
        assert!(config.enabled);
        assert!(config.isolation_enabled);
        assert!(config.metrics_enabled);
    }

    #[test]
    fn test_tenant_with_limits() {
        let tenant = Tenant::new("acme", "Acme Corp")
            .with_limits(TenantLimits {
                memory_bytes: 1024 * 1024 * 1024, // 1GB
                max_keys: 100_000,
                ops_per_second: 10_000,
                max_connections: 100,
                max_databases: 16,
                max_key_size: 512,
                max_value_size: 1024 * 1024,
            })
            .with_tier(TenantTier::Pro);

        assert_eq!(tenant.limits.memory_bytes, 1024 * 1024 * 1024);
        assert_eq!(tenant.tier, TenantTier::Pro);
    }

    #[test]
    fn test_tenant_state_display() {
        assert_eq!(TenantState::Active.to_string(), "active");
        assert_eq!(TenantState::Suspended.to_string(), "suspended");
    }

    #[test]
    fn test_tenant_context_suspend_blocks_ops() {
        let tenant = Tenant::new("t1", "Test");
        let ctx = TenantContext::new(tenant);

        // Active — should work
        assert!(ctx.check_operation(OperationType::Read).is_ok());

        // Suspend
        ctx.set_state(TenantState::Suspended);
        assert!(ctx.check_operation(OperationType::Read).is_err());

        // Resume
        ctx.set_state(TenantState::Active);
        assert!(ctx.check_operation(OperationType::Read).is_ok());
    }
}
