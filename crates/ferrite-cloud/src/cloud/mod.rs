//! Cloud Service Module
//!
//! Provides managed Ferrite Cloud service scaffolding including instance
//! provisioning, usage metering, and multi-tenant isolation.
//!
//! # Features
//!
//! - **Provisioning**: Create, scale, and deprovision managed instances
//! - **Metering**: Record and aggregate per-tenant resource usage
//! - **Tenant Isolation**: Enforce resource limits and isolation levels
//! - **Multi-Provider**: Support for AWS, GCP, Azure, and custom providers
//!
//! # Example
//!
//! ```ignore
//! use ferrite::cloud::{CloudConfig, CloudProvider, ProvisioningService};
//! use ferrite::cloud::provisioning::InstanceSpec;
//!
//! let config = CloudConfig::default();
//! let svc = ProvisioningService::new(config.default_provider.clone());
//!
//! let spec = InstanceSpec {
//!     cpu: 4,
//!     memory: 8 * 1024 * 1024 * 1024,
//!     storage: 100 * 1024 * 1024 * 1024,
//!     region: "us-east-1".to_string(),
//!     tier: "standard".to_string(),
//! };
//!
//! let instance = svc.provision(spec).await?;
//! ```

#![allow(dead_code)]
pub mod api_gateway;
pub mod billing;
pub mod control_plane;
pub mod deploy_sdk;
pub mod metering;
pub mod provisioning;
pub mod sla_monitor;
pub mod tenant;
pub mod terraform;

pub use api_gateway::{
    ApiError, ApiGateway, ApiKeyInfo, InstanceInfo, InstanceMetrics, Permission, RateLimiter,
    ScaleSpec,
};
pub use billing::{BillingService, Invoice, InvoiceLineItem, InvoiceStatus, TierPricing};
pub use control_plane::{ControlPlane, Deployment, DeploymentRequest, DeploymentStatus};
pub use deploy_sdk::{
    CloudProviderType, DeployError, DeploySDK, DeploymentPlan, DeploymentSpec, InstanceSize,
    NodeEndpoint, PlannedResource,
};
pub use metering::{MeteringService, MetricType, UsageRecord, UsageSummary};
pub use provisioning::{InstanceSpec, InstanceStatus, ProvisionedInstance, ProvisioningService};
pub use sla_monitor::{
    SlaMonitor, SlaReport, SlaTarget, SlaViolation, UptimeTracker, ViolationType,
};
pub use tenant::{IsolationLevel, ResourceLimits, TenantConfig, TenantManager};

use serde::{Deserialize, Serialize};

/// Supported cloud providers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CloudProvider {
    /// Amazon Web Services.
    Aws,
    /// Google Cloud Platform.
    Gcp,
    /// Microsoft Azure.
    Azure,
    /// A custom or on-premises provider.
    Custom(String),
}

impl Default for CloudProvider {
    fn default() -> Self {
        Self::Aws
    }
}

/// Auto-scaling settings for managed instances.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoScalingConfig {
    /// Enable auto-scaling.
    pub enabled: bool,
    /// Minimum number of instances.
    pub min_instances: u32,
    /// Maximum number of instances.
    pub max_instances: u32,
    /// CPU utilization target percentage (0–100) that triggers scaling.
    pub target_cpu_percent: u32,
}

impl Default for AutoScalingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_instances: 1,
            max_instances: 10,
            target_cpu_percent: 70,
        }
    }
}

/// Top-level configuration for the Ferrite Cloud service layer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudConfig {
    /// Default deployment region.
    pub region: String,
    /// Service tier (e.g. `"free"`, `"standard"`, `"premium"`).
    pub tier: String,
    /// Default cloud provider.
    pub default_provider: CloudProvider,
    /// Auto-scaling settings.
    pub auto_scaling: AutoScalingConfig,
}

impl Default for CloudConfig {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            tier: "standard".to_string(),
            default_provider: CloudProvider::default(),
            auto_scaling: AutoScalingConfig::default(),
        }
    }
}

/// Errors specific to the cloud service layer.
#[derive(Debug, Clone, thiserror::Error)]
pub enum CloudError {
    /// Requested instance was not found.
    #[error("instance not found: {0}")]
    InstanceNotFound(String),

    /// Tenant was not found.
    #[error("tenant not found: {0}")]
    TenantNotFound(String),

    /// Tenant already exists.
    #[error("tenant already exists: {0}")]
    TenantAlreadyExists(String),

    /// A resource limit has been exceeded.
    #[error("resource limit exceeded: {0}")]
    ResourceLimitExceeded(String),

    /// Invalid configuration value.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// Provisioning failure.
    #[error("provisioning failed: {0}")]
    ProvisioningFailed(String),

    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cloud_config_default() {
        let config = CloudConfig::default();
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.tier, "standard");
        assert_eq!(config.default_provider, CloudProvider::Aws);
        assert!(config.auto_scaling.enabled);
    }

    #[test]
    fn test_cloud_provider_default() {
        assert_eq!(CloudProvider::default(), CloudProvider::Aws);
    }

    #[test]
    fn test_cloud_error_display() {
        let err = CloudError::InstanceNotFound("abc".to_string());
        assert_eq!(err.to_string(), "instance not found: abc");
    }

    #[test]
    fn test_auto_scaling_defaults() {
        let cfg = AutoScalingConfig::default();
        assert_eq!(cfg.min_instances, 1);
        assert_eq!(cfg.max_instances, 10);
        assert_eq!(cfg.target_cpu_percent, 70);
    }
}
