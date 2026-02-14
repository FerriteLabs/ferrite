//! Instance Provisioning
//!
//! Manages the lifecycle of managed Ferrite Cloud instances including
//! provisioning, deprovisioning, and scaling.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{CloudError, CloudProvider};

/// Specification for a new Ferrite Cloud instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceSpec {
    /// Number of vCPUs.
    pub cpu: u32,
    /// Memory in bytes.
    pub memory: u64,
    /// Persistent storage in bytes.
    pub storage: u64,
    /// Deployment region (e.g. `"us-east-1"`).
    pub region: String,
    /// Service tier (e.g. `"standard"`, `"premium"`).
    pub tier: String,
}

/// Current lifecycle status of an instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InstanceStatus {
    /// Instance is being created.
    Provisioning,
    /// Instance is healthy and serving traffic.
    Running,
    /// Instance is actively scaling resources.
    Scaling,
    /// Instance has been stopped.
    Stopped,
    /// Instance entered an error state.
    Failed(String),
}

/// A provisioned Ferrite Cloud instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvisionedInstance {
    /// Unique instance identifier.
    pub id: String,
    /// Specification used to create this instance.
    pub spec: InstanceSpec,
    /// Current status.
    pub status: InstanceStatus,
    /// Cloud provider hosting this instance.
    pub provider: CloudProvider,
    /// Connection endpoints (e.g. host:port).
    pub endpoints: Vec<String>,
    /// Timestamp when the instance was created.
    pub created_at: DateTime<Utc>,
}

/// Service that manages instance lifecycle.
pub struct ProvisioningService {
    instances: Arc<RwLock<HashMap<String, ProvisionedInstance>>>,
    default_provider: CloudProvider,
}

impl ProvisioningService {
    /// Creates a new [`ProvisioningService`] with the given default provider.
    pub fn new(default_provider: CloudProvider) -> Self {
        Self {
            instances: Arc::new(RwLock::new(HashMap::new())),
            default_provider,
        }
    }

    /// Provisions a new instance from the given specification.
    ///
    /// Returns the [`ProvisionedInstance`] in `Provisioning` status.
    pub async fn provision(&self, spec: InstanceSpec) -> Result<ProvisionedInstance, CloudError> {
        if spec.cpu == 0 {
            return Err(CloudError::InvalidConfig(
                "cpu must be greater than 0".to_string(),
            ));
        }
        if spec.memory == 0 {
            return Err(CloudError::InvalidConfig(
                "memory must be greater than 0".to_string(),
            ));
        }

        let instance = ProvisionedInstance {
            id: Uuid::new_v4().to_string(),
            spec: spec.clone(),
            status: InstanceStatus::Provisioning,
            provider: self.default_provider.clone(),
            endpoints: vec![format!("{}.ferrite.cloud:6379", spec.region)],
            created_at: Utc::now(),
        };

        self.instances
            .write()
            .insert(instance.id.clone(), instance.clone());
        Ok(instance)
    }

    /// Deprovisions (removes) an instance by id.
    pub async fn deprovision(&self, instance_id: &str) -> Result<(), CloudError> {
        let mut instances = self.instances.write();
        if instances.remove(instance_id).is_none() {
            return Err(CloudError::InstanceNotFound(instance_id.to_string()));
        }
        Ok(())
    }

    /// Scales an existing instance to a new specification.
    pub async fn scale(
        &self,
        instance_id: &str,
        new_spec: InstanceSpec,
    ) -> Result<ProvisionedInstance, CloudError> {
        let mut instances = self.instances.write();
        let instance = instances
            .get_mut(instance_id)
            .ok_or_else(|| CloudError::InstanceNotFound(instance_id.to_string()))?;

        instance.spec = new_spec;
        instance.status = InstanceStatus::Scaling;
        Ok(instance.clone())
    }

    /// Returns the current status of an instance.
    pub async fn status(&self, instance_id: &str) -> Result<ProvisionedInstance, CloudError> {
        let instances = self.instances.read();
        instances
            .get(instance_id)
            .cloned()
            .ok_or_else(|| CloudError::InstanceNotFound(instance_id.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_spec() -> InstanceSpec {
        InstanceSpec {
            cpu: 4,
            memory: 8 * 1024 * 1024 * 1024,
            storage: 100 * 1024 * 1024 * 1024,
            region: "us-east-1".to_string(),
            tier: "standard".to_string(),
        }
    }

    #[tokio::test]
    async fn test_provision_and_status() {
        let svc = ProvisioningService::new(CloudProvider::Aws);
        let instance = svc.provision(sample_spec()).await.unwrap();
        assert_eq!(instance.status, InstanceStatus::Provisioning);
        assert!(!instance.endpoints.is_empty());

        let fetched = svc.status(&instance.id).await.unwrap();
        assert_eq!(fetched.id, instance.id);
    }

    #[tokio::test]
    async fn test_deprovision() {
        let svc = ProvisioningService::new(CloudProvider::Aws);
        let instance = svc.provision(sample_spec()).await.unwrap();
        svc.deprovision(&instance.id).await.unwrap();
        assert!(svc.status(&instance.id).await.is_err());
    }

    #[tokio::test]
    async fn test_deprovision_not_found() {
        let svc = ProvisioningService::new(CloudProvider::Aws);
        let err = svc.deprovision("nonexistent").await.unwrap_err();
        assert!(matches!(err, CloudError::InstanceNotFound(_)));
    }

    #[tokio::test]
    async fn test_scale() {
        let svc = ProvisioningService::new(CloudProvider::Aws);
        let instance = svc.provision(sample_spec()).await.unwrap();

        let mut bigger = sample_spec();
        bigger.cpu = 8;
        let scaled = svc.scale(&instance.id, bigger).await.unwrap();
        assert_eq!(scaled.status, InstanceStatus::Scaling);
        assert_eq!(scaled.spec.cpu, 8);
    }

    #[tokio::test]
    async fn test_provision_invalid_cpu() {
        let svc = ProvisioningService::new(CloudProvider::Aws);
        let mut spec = sample_spec();
        spec.cpu = 0;
        assert!(svc.provision(spec).await.is_err());
    }
}
