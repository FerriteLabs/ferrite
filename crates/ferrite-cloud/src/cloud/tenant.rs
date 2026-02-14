//! Multi-Tenant Isolation
//!
//! Manages tenant lifecycle, resource limits, and isolation levels
//! for the managed Ferrite Cloud service.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::CloudError;

/// Isolation level for tenant workloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationLevel {
    /// Tenants share underlying compute and memory resources.
    Shared,
    /// Tenant gets dedicated compute but may share storage infrastructure.
    Dedicated,
    /// Tenant is fully isolated with dedicated compute, memory, and storage.
    Isolated,
}

impl Default for IsolationLevel {
    fn default() -> Self {
        Self::Shared
    }
}

/// Resource limits enforced per tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceLimits {
    /// Maximum memory in bytes.
    pub max_memory: u64,
    /// Maximum concurrent connections.
    pub max_connections: u32,
    /// Maximum operations per second.
    pub max_ops_per_sec: u64,
    /// Maximum storage in bytes.
    pub max_storage: u64,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_memory: 1024 * 1024 * 1024, // 1 GB
            max_connections: 1000,
            max_ops_per_sec: 100_000,
            max_storage: 10 * 1024 * 1024 * 1024, // 10 GB
        }
    }
}

/// Configuration for a single tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantConfig {
    /// Unique tenant identifier.
    pub id: String,
    /// Human-readable tenant name.
    pub name: String,
    /// Resource limits for this tenant.
    pub resource_limits: ResourceLimits,
    /// Isolation level.
    pub isolation_level: IsolationLevel,
    /// Timestamp when the tenant was created.
    pub created_at: DateTime<Utc>,
}

/// Manages tenant lifecycle and limit enforcement.
pub struct TenantManager {
    tenants: Arc<RwLock<HashMap<String, TenantConfig>>>,
}

impl TenantManager {
    /// Creates a new [`TenantManager`].
    pub fn new() -> Self {
        Self {
            tenants: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Creates a new tenant with the given configuration.
    ///
    /// Returns an error if a tenant with the same id already exists.
    pub async fn create_tenant(&self, config: TenantConfig) -> Result<TenantConfig, CloudError> {
        let mut tenants = self.tenants.write();
        if tenants.contains_key(&config.id) {
            return Err(CloudError::TenantAlreadyExists(config.id));
        }
        let stored = config.clone();
        tenants.insert(config.id.clone(), config);
        Ok(stored)
    }

    /// Removes a tenant by id.
    ///
    /// Returns the removed [`TenantConfig`] or an error if not found.
    pub async fn remove_tenant(&self, tenant_id: &str) -> Result<TenantConfig, CloudError> {
        let mut tenants = self.tenants.write();
        tenants
            .remove(tenant_id)
            .ok_or_else(|| CloudError::TenantNotFound(tenant_id.to_string()))
    }

    /// Retrieves a tenant configuration by id.
    pub async fn get_tenant(&self, tenant_id: &str) -> Result<TenantConfig, CloudError> {
        let tenants = self.tenants.read();
        tenants
            .get(tenant_id)
            .cloned()
            .ok_or_else(|| CloudError::TenantNotFound(tenant_id.to_string()))
    }

    /// Checks whether the given usage values are within the tenant's limits.
    ///
    /// Returns `Ok(())` if within limits, or a [`CloudError::ResourceLimitExceeded`]
    /// describing which limit was violated.
    pub async fn enforce_limits(
        &self,
        tenant_id: &str,
        current_memory: u64,
        current_connections: u32,
        current_ops_per_sec: u64,
        current_storage: u64,
    ) -> Result<(), CloudError> {
        let tenants = self.tenants.read();
        let config = tenants
            .get(tenant_id)
            .ok_or_else(|| CloudError::TenantNotFound(tenant_id.to_string()))?;

        let limits = &config.resource_limits;

        if current_memory > limits.max_memory {
            return Err(CloudError::ResourceLimitExceeded(format!(
                "memory {current_memory} exceeds limit {}",
                limits.max_memory
            )));
        }
        if current_connections > limits.max_connections {
            return Err(CloudError::ResourceLimitExceeded(format!(
                "connections {current_connections} exceeds limit {}",
                limits.max_connections
            )));
        }
        if current_ops_per_sec > limits.max_ops_per_sec {
            return Err(CloudError::ResourceLimitExceeded(format!(
                "ops/sec {current_ops_per_sec} exceeds limit {}",
                limits.max_ops_per_sec
            )));
        }
        if current_storage > limits.max_storage {
            return Err(CloudError::ResourceLimitExceeded(format!(
                "storage {current_storage} exceeds limit {}",
                limits.max_storage
            )));
        }

        Ok(())
    }
}

impl Default for TenantManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config(id: &str) -> TenantConfig {
        TenantConfig {
            id: id.to_string(),
            name: format!("Tenant {id}"),
            resource_limits: ResourceLimits::default(),
            isolation_level: IsolationLevel::Shared,
            created_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_create_and_get_tenant() {
        let mgr = TenantManager::new();
        let config = sample_config("t1");
        mgr.create_tenant(config.clone()).await.unwrap();

        let fetched = mgr.get_tenant("t1").await.unwrap();
        assert_eq!(fetched.id, "t1");
        assert_eq!(fetched.name, config.name);
    }

    #[tokio::test]
    async fn test_duplicate_tenant_error() {
        let mgr = TenantManager::new();
        mgr.create_tenant(sample_config("t1")).await.unwrap();
        let err = mgr.create_tenant(sample_config("t1")).await.unwrap_err();
        assert!(matches!(err, CloudError::TenantAlreadyExists(_)));
    }

    #[tokio::test]
    async fn test_remove_tenant() {
        let mgr = TenantManager::new();
        mgr.create_tenant(sample_config("t1")).await.unwrap();
        let removed = mgr.remove_tenant("t1").await.unwrap();
        assert_eq!(removed.id, "t1");
        assert!(mgr.get_tenant("t1").await.is_err());
    }

    #[tokio::test]
    async fn test_enforce_limits_within() {
        let mgr = TenantManager::new();
        mgr.create_tenant(sample_config("t1")).await.unwrap();
        mgr.enforce_limits("t1", 100, 10, 500, 1024).await.unwrap();
    }

    #[tokio::test]
    async fn test_enforce_limits_exceeded() {
        let mgr = TenantManager::new();
        mgr.create_tenant(sample_config("t1")).await.unwrap();
        let err = mgr
            .enforce_limits("t1", u64::MAX, 10, 500, 1024)
            .await
            .unwrap_err();
        assert!(matches!(err, CloudError::ResourceLimitExceeded(_)));
    }

    #[test]
    fn test_isolation_level_default() {
        assert_eq!(IsolationLevel::default(), IsolationLevel::Shared);
    }
}
