//! Tenant Manager
//!
//! Central management for tenant lifecycle and operations.

use super::{
    current_timestamp_ms, OperationType, QuotaAction, QuotaPolicy, TenancyConfig, TenancyError,
    Tenant, TenantContext, TenantLimits, TenantState, TenantTier,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Callback invoked when a tenant is deleted so the caller can clean up keys.
pub type TenantCleanupFn = Box<dyn Fn(&str) + Send + Sync>;

/// Tenant manager for managing all tenants
pub struct TenantManager {
    /// Configuration
    config: TenancyConfig,
    /// All tenants
    tenants: RwLock<HashMap<String, Arc<TenantContext>>>,
    /// Tenant count
    tenant_count: AtomicU64,
    /// Statistics
    stats: TenantManagerStats,
    /// Optional cleanup hook called with tenant key-prefix on delete
    cleanup_fn: Option<TenantCleanupFn>,
}

impl TenantManager {
    /// Create a new tenant manager
    pub fn new(config: TenancyConfig) -> Self {
        Self {
            config,
            tenants: RwLock::new(HashMap::new()),
            tenant_count: AtomicU64::new(0),
            stats: TenantManagerStats::default(),
            cleanup_fn: None,
        }
    }

    /// Register a cleanup callback invoked with the tenant key-prefix on delete
    pub fn set_cleanup_fn(&mut self, f: TenantCleanupFn) {
        self.cleanup_fn = Some(f);
    }

    /// Create a new tenant
    pub async fn create_tenant(
        &self,
        id: impl Into<String>,
        name: impl Into<String>,
        limits: Option<TenantLimits>,
    ) -> Result<Arc<TenantContext>, TenancyError> {
        let id = id.into();
        let name = name.into();

        let mut tenants = self.tenants.write().await;

        // Check if tenant already exists
        if tenants.contains_key(&id) {
            return Err(TenancyError::TenantExists(id));
        }

        // Check max tenants
        if tenants.len() >= self.config.max_tenants {
            return Err(TenancyError::MaxTenantsReached(self.config.max_tenants));
        }

        // Create tenant with provided or default limits
        let mut tenant = Tenant::new(&id, name);
        if let Some(limits) = limits {
            tenant = tenant.with_limits(limits);
        } else {
            tenant = tenant.with_limits(TenantLimits::from_resource_limits(
                &self.config.default_limits,
            ));
        }
        tenant = tenant.with_quota_policy(self.config.default_quota_policy.clone());

        let context = Arc::new(TenantContext::new(tenant));
        tenants.insert(id.clone(), context.clone());

        self.tenant_count.fetch_add(1, Ordering::Relaxed);
        self.stats.tenants_created.fetch_add(1, Ordering::Relaxed);

        tracing::info!(tenant_id = %id, "tenant created");

        // Emit Prometheus metric
        metrics::gauge!("ferrite_tenant_count").set(self.tenant_count.load(Ordering::Relaxed) as f64);

        Ok(context)
    }

    /// Get a tenant by ID
    pub async fn get_tenant(&self, id: &str) -> Option<Arc<TenantContext>> {
        let tenants = self.tenants.read().await;
        tenants.get(id).cloned()
    }

    /// Get or create a tenant (for auto-provisioning)
    pub async fn get_or_create(&self, id: &str) -> Result<Arc<TenantContext>, TenancyError> {
        // Try to get existing tenant first
        if let Some(context) = self.get_tenant(id).await {
            return Ok(context);
        }

        // Create new tenant with default limits
        self.create_tenant(id, id, None).await
    }

    /// Update tenant limits
    pub async fn update_limits(&self, id: &str, limits: TenantLimits) -> Result<(), TenancyError> {
        let tenants = self.tenants.read().await;
        let context = tenants
            .get(id)
            .ok_or_else(|| TenancyError::TenantNotFound(id.to_string()))?;

        {
            let mut tenant = context.tenant.write();
            tenant.limits = limits.clone();
            tenant.updated_at = current_timestamp_ms();
        }

        tracing::info!(tenant_id = %id, ?limits, "tenant limits updated");
        Ok(())
    }

    /// Update tenant quota policy
    pub async fn update_quota_policy(
        &self,
        id: &str,
        policy: QuotaPolicy,
    ) -> Result<(), TenancyError> {
        let tenants = self.tenants.read().await;
        let context = tenants
            .get(id)
            .ok_or_else(|| TenancyError::TenantNotFound(id.to_string()))?;

        {
            let mut tenant = context.tenant.write();
            tenant.quota_policy = policy;
            tenant.updated_at = current_timestamp_ms();
        }

        tracing::info!(tenant_id = %id, "tenant quota policy updated");
        Ok(())
    }

    /// Suspend a tenant — blocks all operations until resumed
    pub async fn suspend_tenant(&self, id: &str) -> Result<(), TenancyError> {
        let tenants = self.tenants.read().await;
        let context = tenants
            .get(id)
            .ok_or_else(|| TenancyError::TenantNotFound(id.to_string()))?;

        context.set_state(TenantState::Suspended);
        tracing::info!(tenant_id = %id, "tenant suspended");
        Ok(())
    }

    /// Resume (activate) a suspended tenant
    pub async fn activate_tenant(&self, id: &str) -> Result<(), TenancyError> {
        let tenants = self.tenants.read().await;
        let context = tenants
            .get(id)
            .ok_or_else(|| TenancyError::TenantNotFound(id.to_string()))?;

        context.set_state(TenantState::Active);
        tracing::info!(tenant_id = %id, "tenant activated");
        Ok(())
    }

    /// Delete a tenant, invoking the cleanup callback to remove all keys
    pub async fn delete_tenant(&self, id: &str) -> Result<(), TenancyError> {
        let mut tenants = self.tenants.write().await;

        let context = tenants
            .remove(id)
            .ok_or_else(|| TenancyError::TenantNotFound(id.to_string()))?;

        // Mark as deleting
        context.set_state(TenantState::Deleting);

        // Invoke cleanup hook to remove all tenant keys
        let prefix = context.tenant.read().key_prefix();
        if let Some(ref cleanup) = self.cleanup_fn {
            tracing::info!(tenant_id = %id, prefix = %prefix, "cleaning up tenant keys");
            cleanup(&prefix);
        }

        self.tenant_count.fetch_sub(1, Ordering::Relaxed);
        self.stats.tenants_deleted.fetch_add(1, Ordering::Relaxed);

        tracing::info!(tenant_id = %id, "tenant deleted");
        metrics::gauge!("ferrite_tenant_count").set(self.tenant_count.load(Ordering::Relaxed) as f64);

        Ok(())
    }

    /// List all tenants
    pub async fn list_tenants(&self, filter: Option<TenantFilter>) -> Vec<TenantInfo> {
        let tenants = self.tenants.read().await;

        tenants
            .values()
            .filter(|ctx| {
                let tenant = ctx.tenant.read();
                if let Some(ref filter) = filter {
                    filter.matches(&tenant)
                } else {
                    true
                }
            })
            .map(|ctx| {
                let tenant = ctx.tenant.read();
                TenantInfo::from_tenant(&tenant)
            })
            .collect()
    }

    /// Get tenant count
    pub fn tenant_count(&self) -> u64 {
        self.tenant_count.load(Ordering::Relaxed)
    }

    /// Get manager statistics
    pub fn stats(&self) -> TenantManagerStatsSnapshot {
        TenantManagerStatsSnapshot {
            tenants_created: self.stats.tenants_created.load(Ordering::Relaxed),
            tenants_deleted: self.stats.tenants_deleted.load(Ordering::Relaxed),
            active_tenants: self.tenant_count.load(Ordering::Relaxed),
            operations_allowed: self.stats.operations_allowed.load(Ordering::Relaxed),
            operations_denied: self.stats.operations_denied.load(Ordering::Relaxed),
        }
    }

    /// Check an operation for a tenant
    pub async fn check_operation(
        &self,
        tenant_id: &str,
        op_type: OperationType,
    ) -> Result<QuotaAction, TenancyError> {
        let tenants = self.tenants.read().await;
        let context = tenants
            .get(tenant_id)
            .ok_or_else(|| TenancyError::TenantNotFound(tenant_id.to_string()))?;

        match context.check_operation(op_type) {
            Ok(action) => {
                self.stats
                    .operations_allowed
                    .fetch_add(1, Ordering::Relaxed);
                Ok(action)
            }
            Err(e) => {
                self.stats.operations_denied.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Get configuration
    pub fn config(&self) -> &TenancyConfig {
        &self.config
    }

    /// Emit per-tenant Prometheus metrics for all tenants
    pub async fn emit_prometheus_metrics(&self) {
        let tenants = self.tenants.read().await;
        for (id, ctx) in tenants.iter() {
            let usage = ctx.get_usage();
            let labels = [("tenant", id.clone())];
            metrics::gauge!("ferrite_tenant_memory_bytes", &labels).set(usage.memory_bytes as f64);
            metrics::gauge!("ferrite_tenant_keys", &labels).set(usage.key_count as f64);
            metrics::gauge!("ferrite_tenant_connections", &labels)
                .set(usage.connection_count as f64);
            metrics::counter!("ferrite_tenant_ops_total", &labels)
                .absolute(ctx.request_count());
        }
    }
}

impl Default for TenantManager {
    fn default() -> Self {
        Self::new(TenancyConfig::default())
    }
}

/// Filter for listing tenants
#[derive(Clone, Debug)]
pub struct TenantFilter {
    /// Filter by state
    pub state: Option<TenantState>,
    /// Filter by tier
    pub tier: Option<TenantTier>,
    /// Filter by name pattern
    pub name_pattern: Option<String>,
}

impl TenantFilter {
    /// Check if a tenant matches this filter
    pub fn matches(&self, tenant: &Tenant) -> bool {
        if let Some(ref state) = self.state {
            if &tenant.state != state {
                return false;
            }
        }

        if let Some(ref tier) = self.tier {
            if &tenant.tier != tier {
                return false;
            }
        }

        if let Some(ref pattern) = self.name_pattern {
            if !tenant.name.contains(pattern) {
                return false;
            }
        }

        true
    }
}

/// Tenant information (for API responses)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TenantInfo {
    /// Tenant ID
    pub id: String,
    /// Display name
    pub name: String,
    /// State
    pub state: TenantState,
    /// Tier
    pub tier: TenantTier,
    /// Creation time
    pub created_at: u64,
    /// Last update time
    pub updated_at: u64,
    /// Last activity time
    pub last_active_at: u64,
    /// Memory limit
    pub memory_limit_bytes: u64,
    /// Key limit
    pub key_limit: u64,
    /// Ops per second limit
    pub ops_limit: u64,
    /// Connection limit
    pub connection_limit: u64,
}

impl TenantInfo {
    /// Create from tenant
    pub fn from_tenant(tenant: &Tenant) -> Self {
        Self {
            id: tenant.id.clone(),
            name: tenant.name.clone(),
            state: tenant.state.clone(),
            tier: tenant.tier.clone(),
            created_at: tenant.created_at,
            updated_at: tenant.updated_at,
            last_active_at: tenant.last_active_at,
            memory_limit_bytes: tenant.limits.memory_bytes,
            key_limit: tenant.limits.max_keys,
            ops_limit: tenant.limits.ops_per_second,
            connection_limit: tenant.limits.max_connections,
        }
    }
}

/// Internal statistics
#[derive(Default)]
struct TenantManagerStats {
    tenants_created: AtomicU64,
    tenants_deleted: AtomicU64,
    operations_allowed: AtomicU64,
    operations_denied: AtomicU64,
}

/// Statistics snapshot
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TenantManagerStatsSnapshot {
    /// Total tenants created
    pub tenants_created: u64,
    /// Total tenants deleted
    pub tenants_deleted: u64,
    /// Currently active tenants
    pub active_tenants: u64,
    /// Operations allowed
    pub operations_allowed: u64,
    /// Operations denied
    pub operations_denied: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_tenant() {
        let manager = TenantManager::default();

        let context = manager
            .create_tenant("test", "Test Tenant", None)
            .await
            .unwrap();

        assert_eq!(context.tenant_id, "test");
        assert_eq!(context.tenant.read().name, "Test Tenant");
    }

    #[tokio::test]
    async fn test_duplicate_tenant() {
        let manager = TenantManager::default();

        manager.create_tenant("test", "Test 1", None).await.unwrap();
        let result = manager.create_tenant("test", "Test 2", None).await;

        assert!(matches!(result, Err(TenancyError::TenantExists(_))));
    }

    #[tokio::test]
    async fn test_get_tenant() {
        let manager = TenantManager::default();

        manager.create_tenant("test", "Test", None).await.unwrap();
        let context = manager.get_tenant("test").await;

        assert!(context.is_some());
        assert_eq!(context.unwrap().tenant_id, "test");
    }

    #[tokio::test]
    async fn test_delete_tenant() {
        let manager = TenantManager::default();

        manager.create_tenant("test", "Test", None).await.unwrap();
        assert!(manager.get_tenant("test").await.is_some());

        manager.delete_tenant("test").await.unwrap();
        assert!(manager.get_tenant("test").await.is_none());
    }

    #[tokio::test]
    async fn test_delete_tenant_with_cleanup() {
        use std::sync::Mutex;
        let cleaned = Arc::new(Mutex::new(Vec::<String>::new()));
        let cleaned_clone = cleaned.clone();

        let mut manager = TenantManager::default();
        manager.set_cleanup_fn(Box::new(move |prefix| {
            cleaned_clone.lock().unwrap().push(prefix.to_string());
        }));

        manager.create_tenant("acme", "Acme", None).await.unwrap();
        manager.delete_tenant("acme").await.unwrap();

        let prefixes = cleaned.lock().unwrap();
        assert_eq!(prefixes.len(), 1);
        assert!(prefixes[0].contains("acme"));
    }

    #[tokio::test]
    async fn test_suspend_resume_tenant() {
        let manager = TenantManager::default();
        manager.create_tenant("t1", "T1", None).await.unwrap();

        // Suspend
        manager.suspend_tenant("t1").await.unwrap();
        let ctx = manager.get_tenant("t1").await.unwrap();
        assert!(ctx.tenant.read().is_suspended());

        // Should block operations
        let result = ctx.check_operation(OperationType::Read);
        assert!(matches!(result, Err(TenancyError::TenantSuspended(_))));

        // Resume
        manager.activate_tenant("t1").await.unwrap();
        assert!(ctx.tenant.read().is_active());
        assert!(ctx.check_operation(OperationType::Read).is_ok());
    }

    #[tokio::test]
    async fn test_update_limits() {
        let manager = TenantManager::default();
        manager.create_tenant("t1", "T1", None).await.unwrap();

        let new_limits = TenantLimits {
            memory_bytes: 999,
            ..TenantLimits::default()
        };
        manager.update_limits("t1", new_limits).await.unwrap();

        let ctx = manager.get_tenant("t1").await.unwrap();
        assert_eq!(ctx.tenant.read().limits.memory_bytes, 999);
    }

    #[tokio::test]
    async fn test_list_tenants() {
        let manager = TenantManager::default();

        manager
            .create_tenant("tenant1", "Tenant 1", None)
            .await
            .unwrap();
        manager
            .create_tenant("tenant2", "Tenant 2", None)
            .await
            .unwrap();

        let tenants = manager.list_tenants(None).await;
        assert_eq!(tenants.len(), 2);
    }

    #[tokio::test]
    async fn test_tenant_stats() {
        let manager = TenantManager::default();

        manager
            .create_tenant("test1", "Test 1", None)
            .await
            .unwrap();
        manager
            .create_tenant("test2", "Test 2", None)
            .await
            .unwrap();
        manager.delete_tenant("test1").await.unwrap();

        let stats = manager.stats();
        assert_eq!(stats.tenants_created, 2);
        assert_eq!(stats.tenants_deleted, 1);
        assert_eq!(stats.active_tenants, 1);
    }

    #[tokio::test]
    async fn test_tenant_info_has_updated_at() {
        let manager = TenantManager::default();
        manager.create_tenant("t1", "T1", None).await.unwrap();

        let infos = manager.list_tenants(None).await;
        assert_eq!(infos.len(), 1);
        assert!(infos[0].updated_at > 0);
        assert!(infos[0].updated_at >= infos[0].created_at);
    }
}
