//! Control Plane API
//!
//! Provides a centralized management API for Ferrite Cloud that orchestrates
//! instance lifecycle, tenant management, and operational tasks through a
//! unified interface.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::metering::{MeteringService, MetricType, UsageRecord};
use super::provisioning::{InstanceSpec, ProvisioningService};
use super::tenant::TenantManager;
use super::{CloudConfig, CloudError, CloudProvider};
use chrono::Utc;

/// Deployment request from the self-service portal or API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentRequest {
    /// Unique deployment name
    pub name: String,
    /// Owning tenant ID
    pub tenant_id: String,
    /// Target region
    pub region: String,
    /// Service tier (free, standard, premium)
    pub tier: String,
    /// Requested CPU cores
    pub cpu: u32,
    /// Requested memory in bytes
    pub memory: u64,
    /// Requested storage in bytes
    pub storage: u64,
    /// High-availability mode
    pub ha_enabled: bool,
    /// Number of replicas (only used when ha_enabled)
    pub replica_count: u32,
    /// Backup schedule cron expression
    pub backup_schedule: Option<String>,
    /// Cloud provider override
    pub provider: Option<CloudProvider>,
}

/// A managed deployment (composed of one or more instances)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Deployment {
    /// Unique deployment ID
    pub id: String,
    /// Deployment name
    pub name: String,
    /// Owning tenant
    pub tenant_id: String,
    /// Primary instance ID
    pub primary_instance_id: String,
    /// Replica instance IDs
    pub replica_instance_ids: Vec<String>,
    /// Current status
    pub status: DeploymentStatus,
    /// Connection endpoint
    pub endpoint: String,
    /// When the deployment was created
    pub created_at: SystemTime,
    /// Region
    pub region: String,
    /// Tier
    pub tier: String,
}

/// Status of a managed deployment
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeploymentStatus {
    /// Being provisioned
    Provisioning,
    /// Running and healthy
    Running,
    /// Scaling up or down
    Scaling,
    /// Temporarily stopped
    Stopped,
    /// Being deprovisioned
    Deprovisioning,
    /// Deployment failed
    Failed,
}

/// Control plane managing all Ferrite Cloud deployments
pub struct ControlPlane {
    config: CloudConfig,
    provisioning: Arc<ProvisioningService>,
    tenants: Arc<TenantManager>,
    metering: Arc<MeteringService>,
    deployments: RwLock<HashMap<String, Deployment>>,
    deployment_counter: RwLock<u64>,
}

impl ControlPlane {
    /// Create a new control plane
    pub fn new(config: CloudConfig) -> Self {
        Self {
            provisioning: Arc::new(ProvisioningService::new(config.default_provider.clone())),
            tenants: Arc::new(TenantManager::new()),
            metering: Arc::new(MeteringService::new()),
            deployments: RwLock::new(HashMap::new()),
            deployment_counter: RwLock::new(0),
            config,
        }
    }

    /// Create a new deployment
    pub async fn create_deployment(
        &self,
        request: DeploymentRequest,
    ) -> Result<Deployment, CloudError> {
        // Verify tenant exists
        self.tenants.get_tenant(&request.tenant_id).await?;

        // Generate deployment ID
        let deploy_id = {
            let mut counter = self.deployment_counter.write();
            *counter += 1;
            format!("deploy-{:08x}", *counter)
        };

        // Provision primary instance
        let primary_spec = InstanceSpec {
            cpu: request.cpu,
            memory: request.memory,
            storage: request.storage,
            region: request.region.clone(),
            tier: request.tier.clone(),
        };

        let primary = self.provisioning.provision(primary_spec).await?;

        // Provision replicas if HA is enabled
        let mut replica_ids = Vec::new();
        if request.ha_enabled {
            for i in 0..request.replica_count {
                let replica_spec = InstanceSpec {
                    cpu: request.cpu,
                    memory: request.memory,
                    storage: request.storage,
                    region: request.region.clone(),
                    tier: request.tier.clone(),
                };
                match self.provisioning.provision(replica_spec).await {
                    Ok(replica) => replica_ids.push(replica.id),
                    Err(e) => {
                        warn!(
                            "Failed to provision replica {} for deployment {}: {}",
                            i, deploy_id, e
                        );
                    }
                }
            }
        }

        let endpoint = format!("{}.{}.ferrite.cloud:6379", request.name, request.region);

        let deployment = Deployment {
            id: deploy_id.clone(),
            name: request.name,
            tenant_id: request.tenant_id.clone(),
            primary_instance_id: primary.id,
            replica_instance_ids: replica_ids,
            status: DeploymentStatus::Running,
            endpoint,
            created_at: SystemTime::now(),
            region: request.region,
            tier: request.tier,
        };

        self.deployments
            .write()
            .insert(deploy_id.clone(), deployment.clone());

        // Record metering event
        let _ = self
            .metering
            .record(UsageRecord {
                tenant_id: request.tenant_id,
                metric_type: MetricType::StorageBytes,
                value: 1.0,
                timestamp: Utc::now(),
            })
            .await;

        info!(
            "Created deployment {} ({} endpoint)",
            deploy_id, deployment.endpoint
        );
        Ok(deployment)
    }

    /// Get deployment status
    pub fn get_deployment(&self, deployment_id: &str) -> Result<Deployment, CloudError> {
        self.deployments
            .read()
            .get(deployment_id)
            .cloned()
            .ok_or_else(|| CloudError::InstanceNotFound(deployment_id.to_string()))
    }

    /// List all deployments for a tenant
    pub fn list_deployments(&self, tenant_id: &str) -> Vec<Deployment> {
        self.deployments
            .read()
            .values()
            .filter(|d| d.tenant_id == tenant_id)
            .cloned()
            .collect()
    }

    /// Scale a deployment
    pub async fn scale_deployment(
        &self,
        deployment_id: &str,
        new_cpu: u32,
        new_memory: u64,
    ) -> Result<Deployment, CloudError> {
        let (new_spec, primary_instance_id, deployment_result) = {
            let deployments = self.deployments.write();
            let deployment = deployments
                .get(deployment_id)
                .ok_or_else(|| CloudError::InstanceNotFound(deployment_id.to_string()))?;

            let new_spec = InstanceSpec {
                cpu: new_cpu,
                memory: new_memory,
                storage: 0,
                region: deployment.region.clone(),
                tier: deployment.tier.clone(),
            };

            let primary_instance_id = deployment.primary_instance_id.clone();
            let deployment_result = deployment.clone();
            (new_spec, primary_instance_id, deployment_result)
        };

        self.provisioning
            .scale(&primary_instance_id, new_spec)
            .await?;

        let mut deployments = self.deployments.write();
        if let Some(dep) = deployments.get_mut(deployment_id) {
            dep.status = DeploymentStatus::Running;
        }
        info!(
            "Scaled deployment {} to {} CPU, {} memory",
            deployment_id, new_cpu, new_memory
        );
        Ok(deployment_result)
    }

    /// Delete a deployment
    pub async fn delete_deployment(&self, deployment_id: &str) -> Result<(), CloudError> {
        let deployment = {
            let deployments = self.deployments.read();
            deployments
                .get(deployment_id)
                .cloned()
                .ok_or_else(|| CloudError::InstanceNotFound(deployment_id.to_string()))?
        };

        // Deprovision all instances
        self.provisioning
            .deprovision(&deployment.primary_instance_id)
            .await?;
        for replica_id in &deployment.replica_instance_ids {
            let _ = self.provisioning.deprovision(replica_id).await;
        }

        self.deployments.write().remove(deployment_id);
        info!("Deleted deployment {}", deployment_id);
        Ok(())
    }

    /// Get all deployments across all tenants
    pub fn list_all_deployments(&self) -> Vec<Deployment> {
        self.deployments.read().values().cloned().collect()
    }

    /// Get the provisioning service
    pub fn provisioning(&self) -> &Arc<ProvisioningService> {
        &self.provisioning
    }

    /// Get the tenant manager
    pub fn tenants(&self) -> &Arc<TenantManager> {
        &self.tenants
    }

    /// Get the metering service
    pub fn metering(&self) -> &Arc<MeteringService> {
        &self.metering
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cloud::tenant::{IsolationLevel, ResourceLimits, TenantConfig};
    use chrono::Utc;

    #[tokio::test]
    async fn test_create_deployment() {
        let cp = ControlPlane::new(CloudConfig::default());

        // First create a tenant
        let tenant = TenantConfig {
            id: "t1".to_string(),
            name: "Test Tenant".to_string(),
            isolation_level: IsolationLevel::Shared,
            resource_limits: ResourceLimits::default(),
            created_at: Utc::now(),
        };
        cp.tenants().create_tenant(tenant).await.unwrap();

        let request = DeploymentRequest {
            name: "mydb".to_string(),
            tenant_id: "t1".to_string(),
            region: "us-east-1".to_string(),
            tier: "standard".to_string(),
            cpu: 2,
            memory: 4 * 1024 * 1024 * 1024,
            storage: 50 * 1024 * 1024 * 1024,
            ha_enabled: false,
            replica_count: 0,
            backup_schedule: None,
            provider: None,
        };

        let deploy = cp.create_deployment(request).await.unwrap();
        assert_eq!(deploy.status, DeploymentStatus::Running);
        assert!(deploy.endpoint.contains("mydb"));
    }

    #[tokio::test]
    async fn test_list_deployments() {
        let cp = ControlPlane::new(CloudConfig::default());
        let tenant = TenantConfig {
            id: "t1".to_string(),
            name: "Test".to_string(),
            isolation_level: IsolationLevel::Shared,
            resource_limits: ResourceLimits::default(),
            created_at: Utc::now(),
        };
        cp.tenants().create_tenant(tenant).await.unwrap();

        let request = DeploymentRequest {
            name: "db1".to_string(),
            tenant_id: "t1".to_string(),
            region: "us-east-1".to_string(),
            tier: "standard".to_string(),
            cpu: 1,
            memory: 1024,
            storage: 1024,
            ha_enabled: false,
            replica_count: 0,
            backup_schedule: None,
            provider: None,
        };
        cp.create_deployment(request).await.unwrap();

        let deployments = cp.list_deployments("t1");
        assert_eq!(deployments.len(), 1);
        assert_eq!(deployments[0].name, "db1");
    }

    #[tokio::test]
    async fn test_delete_deployment() {
        let cp = ControlPlane::new(CloudConfig::default());
        let tenant = TenantConfig {
            id: "t1".to_string(),
            name: "Test".to_string(),
            isolation_level: IsolationLevel::Shared,
            resource_limits: ResourceLimits::default(),
            created_at: Utc::now(),
        };
        cp.tenants().create_tenant(tenant).await.unwrap();

        let request = DeploymentRequest {
            name: "db1".to_string(),
            tenant_id: "t1".to_string(),
            region: "us-east-1".to_string(),
            tier: "standard".to_string(),
            cpu: 1,
            memory: 1024,
            storage: 1024,
            ha_enabled: false,
            replica_count: 0,
            backup_schedule: None,
            provider: None,
        };
        let deploy = cp.create_deployment(request).await.unwrap();
        cp.delete_deployment(&deploy.id).await.unwrap();

        assert!(cp.get_deployment(&deploy.id).is_err());
    }

    #[tokio::test]
    async fn test_create_deployment_missing_tenant() {
        let cp = ControlPlane::new(CloudConfig::default());
        let request = DeploymentRequest {
            name: "db1".to_string(),
            tenant_id: "nonexistent".to_string(),
            region: "us-east-1".to_string(),
            tier: "standard".to_string(),
            cpu: 1,
            memory: 1024,
            storage: 1024,
            ha_enabled: false,
            replica_count: 0,
            backup_schedule: None,
            provider: None,
        };
        assert!(cp.create_deployment(request).await.is_err());
    }
}
