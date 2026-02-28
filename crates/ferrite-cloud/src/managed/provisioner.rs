#![allow(dead_code)]
//! Instance provisioning for managed Ferrite Cloud
//!
//! Handles the lifecycle of managed Ferrite instances including
//! creation, scaling, health checking, and termination.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Configuration for the instance provisioner
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvisionerConfig {
    /// Cloud region for provisioning
    pub region: String,
    /// Availability zones to use
    pub availability_zones: Vec<String>,
    /// Default instance type for new instances
    pub default_instance_type: InstanceType,
    /// VPC identifier (if applicable)
    pub vpc_id: Option<String>,
    /// Subnet identifiers for network placement
    pub subnet_ids: Vec<String>,
}

impl Default for ProvisionerConfig {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            availability_zones: vec![
                "us-east-1a".to_string(),
                "us-east-1b".to_string(),
                "us-east-1c".to_string(),
            ],
            default_instance_type: InstanceType::Small,
            vpc_id: None,
            subnet_ids: Vec::new(),
        }
    }
}

/// Instance type defining compute resources
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum InstanceType {
    /// Development: 1 vCPU, 1GB RAM
    Dev,
    /// Small: 2 vCPU, 4GB RAM
    Small,
    /// Medium: 4 vCPU, 16GB RAM
    Medium,
    /// Large: 8 vCPU, 32GB RAM
    Large,
    /// Extra Large: 16 vCPU, 64GB RAM
    XLarge,
    /// Custom resource allocation
    Custom {
        /// Number of virtual CPUs
        vcpus: u32,
        /// Memory in gigabytes
        memory_gb: u32,
    },
}

impl InstanceType {
    /// Returns the number of vCPUs for this instance type
    pub fn vcpus(&self) -> u32 {
        match self {
            Self::Dev => 1,
            Self::Small => 2,
            Self::Medium => 4,
            Self::Large => 8,
            Self::XLarge => 16,
            Self::Custom { vcpus, .. } => *vcpus,
        }
    }

    /// Returns the memory in GB for this instance type
    pub fn memory_gb(&self) -> u32 {
        match self {
            Self::Dev => 1,
            Self::Small => 4,
            Self::Medium => 16,
            Self::Large => 32,
            Self::XLarge => 64,
            Self::Custom { memory_gb, .. } => *memory_gb,
        }
    }
}

/// Request to provision a new managed instance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvisionRequest {
    /// Instance ID (auto-generated if None)
    pub instance_id: Option<String>,
    /// Human-readable instance name
    pub name: String,
    /// Desired instance type
    pub instance_type: InstanceType,
    /// Number of replicas
    pub replicas: u8,
    /// Storage capacity in GB
    pub storage_gb: u32,
    /// Enable TLS encryption
    pub enable_tls: bool,
    /// Enable persistence to disk
    pub enable_persistence: bool,
    /// Enable cloud tiering for cold data
    pub cloud_tiering: bool,
    /// User-defined tags
    pub tags: HashMap<String, String>,
}

/// Current state of a managed instance
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum InstanceState {
    /// Instance is being provisioned
    Provisioning,
    /// Instance is running and accepting connections
    Running,
    /// Instance is scaling up or down
    Scaling,
    /// Instance is stopped
    Stopped,
    /// Instance is being terminated
    Terminating,
    /// Instance has been terminated
    Terminated,
    /// Instance is in a failed state
    Failed,
}

/// Record of a provisioned instance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceRecord {
    /// Unique instance identifier
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Current instance state
    pub state: InstanceState,
    /// Instance type (compute resources)
    pub instance_type: InstanceType,
    /// Connection endpoint (host:port)
    pub endpoint: String,
    /// Number of active replicas
    pub replicas: u8,
    /// Timestamp when instance was created
    pub created_at: DateTime<Utc>,
    /// Cloud region
    pub region: String,
    /// Latest health status
    pub health: InstanceHealth,
}

/// Health status of an instance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceHealth {
    /// Overall health status
    pub status: HealthStatus,
    /// Response latency in milliseconds
    pub latency_ms: f64,
    /// Memory usage percentage (0.0 - 100.0)
    pub memory_usage_pct: f64,
    /// Number of active connections
    pub connections: u64,
    /// Uptime in seconds
    pub uptime_seconds: u64,
}

/// Health status classification
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum HealthStatus {
    /// All systems operational
    Healthy,
    /// Some degradation detected
    Degraded,
    /// Instance is unhealthy
    Unhealthy,
}

impl Default for InstanceHealth {
    fn default() -> Self {
        Self {
            status: HealthStatus::Healthy,
            latency_ms: 0.0,
            memory_usage_pct: 0.0,
            connections: 0,
            uptime_seconds: 0,
        }
    }
}

/// Filter criteria for listing instances
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InstanceFilter {
    /// Filter by instance state
    pub state: Option<InstanceState>,
    /// Filter by region
    pub region: Option<String>,
    /// Filter by name prefix
    pub name_prefix: Option<String>,
}

/// Provisioning errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum ProvisionError {
    /// Instance with this ID already exists
    #[error("instance already exists: {0}")]
    InstanceExists(String),

    /// Instance not found
    #[error("instance not found: {0}")]
    InstanceNotFound(String),

    /// Insufficient capacity in the region
    #[error("insufficient capacity in region {0}")]
    InsufficientCapacity(String),

    /// Invalid configuration
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// Provisioning timed out
    #[error("provisioning timed out after {0}s")]
    ProvisionTimeout(u64),

    /// Region is unavailable
    #[error("region unavailable: {0}")]
    RegionUnavailable(String),
}

/// Instance provisioner for managed Ferrite Cloud
pub struct Provisioner {
    config: ProvisionerConfig,
    instances: RwLock<HashMap<String, InstanceRecord>>,
}

impl Provisioner {
    /// Create a new provisioner with the given configuration
    pub fn new(config: ProvisionerConfig) -> Self {
        Self {
            config,
            instances: RwLock::new(HashMap::new()),
        }
    }

    /// Provision a new managed Ferrite instance
    pub fn provision(&self, request: ProvisionRequest) -> Result<InstanceRecord, ProvisionError> {
        let instance_id = request
            .instance_id
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let instances = self.instances.read();
        if instances.contains_key(&instance_id) {
            return Err(ProvisionError::InstanceExists(instance_id));
        }
        drop(instances);

        let endpoint = format!("{}.ferrite.cloud:6379", instance_id);

        let record = InstanceRecord {
            id: instance_id.clone(),
            name: request.name,
            state: InstanceState::Provisioning,
            instance_type: request.instance_type,
            endpoint,
            replicas: request.replicas,
            created_at: Utc::now(),
            region: self.config.region.clone(),
            health: InstanceHealth::default(),
        };

        let mut instances = self.instances.write();
        instances.insert(instance_id, record.clone());

        Ok(record)
    }

    /// Terminate a managed instance
    pub fn terminate(&self, instance_id: &str) -> Result<(), ProvisionError> {
        let mut instances = self.instances.write();
        let record = instances
            .get_mut(instance_id)
            .ok_or_else(|| ProvisionError::InstanceNotFound(instance_id.to_string()))?;

        record.state = InstanceState::Terminating;
        Ok(())
    }

    /// Scale an instance to a new type and replica count
    pub fn scale(
        &self,
        instance_id: &str,
        new_type: InstanceType,
        replicas: u8,
    ) -> Result<(), ProvisionError> {
        let mut instances = self.instances.write();
        let record = instances
            .get_mut(instance_id)
            .ok_or_else(|| ProvisionError::InstanceNotFound(instance_id.to_string()))?;

        if record.state != InstanceState::Running {
            return Err(ProvisionError::InvalidConfig(format!(
                "cannot scale instance in {:?} state",
                record.state
            )));
        }

        record.state = InstanceState::Scaling;
        record.instance_type = new_type;
        record.replicas = replicas;
        Ok(())
    }

    /// Get an instance by ID
    pub fn get_instance(&self, instance_id: &str) -> Option<InstanceRecord> {
        let instances = self.instances.read();
        instances.get(instance_id).cloned()
    }

    /// List instances matching the given filter
    pub fn list_instances(&self, filter: InstanceFilter) -> Vec<InstanceRecord> {
        let instances = self.instances.read();
        instances
            .values()
            .filter(|inst| {
                if let Some(ref state) = filter.state {
                    if inst.state != *state {
                        return false;
                    }
                }
                if let Some(ref region) = filter.region {
                    if inst.region != *region {
                        return false;
                    }
                }
                if let Some(ref prefix) = filter.name_prefix {
                    if !inst.name.starts_with(prefix) {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect()
    }

    /// Perform a health check on an instance
    pub fn health_check(&self, instance_id: &str) -> InstanceHealth {
        let instances = self.instances.read();
        instances
            .get(instance_id)
            .map(|inst| inst.health.clone())
            .unwrap_or(InstanceHealth {
                status: HealthStatus::Unhealthy,
                latency_ms: 0.0,
                memory_usage_pct: 0.0,
                connections: 0,
                uptime_seconds: 0,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provision_and_get() {
        let provisioner = Provisioner::new(ProvisionerConfig::default());
        let request = ProvisionRequest {
            instance_id: Some("test-1".to_string()),
            name: "my-instance".to_string(),
            instance_type: InstanceType::Small,
            replicas: 1,
            storage_gb: 10,
            enable_tls: true,
            enable_persistence: true,
            cloud_tiering: false,
            tags: HashMap::new(),
        };

        let record = provisioner.provision(request).unwrap();
        assert_eq!(record.id, "test-1");
        assert_eq!(record.state, InstanceState::Provisioning);

        let fetched = provisioner.get_instance("test-1").unwrap();
        assert_eq!(fetched.name, "my-instance");
    }

    #[test]
    fn test_duplicate_provision_fails() {
        let provisioner = Provisioner::new(ProvisionerConfig::default());
        let request = ProvisionRequest {
            instance_id: Some("dup-1".to_string()),
            name: "dup".to_string(),
            instance_type: InstanceType::Dev,
            replicas: 1,
            storage_gb: 5,
            enable_tls: false,
            enable_persistence: false,
            cloud_tiering: false,
            tags: HashMap::new(),
        };

        provisioner.provision(request.clone()).unwrap();
        let result = provisioner.provision(request);
        assert!(matches!(result, Err(ProvisionError::InstanceExists(_))));
    }

    #[test]
    fn test_terminate() {
        let provisioner = Provisioner::new(ProvisionerConfig::default());
        let request = ProvisionRequest {
            instance_id: Some("term-1".to_string()),
            name: "to-terminate".to_string(),
            instance_type: InstanceType::Medium,
            replicas: 1,
            storage_gb: 20,
            enable_tls: true,
            enable_persistence: true,
            cloud_tiering: false,
            tags: HashMap::new(),
        };

        provisioner.provision(request).unwrap();
        provisioner.terminate("term-1").unwrap();

        let inst = provisioner.get_instance("term-1").unwrap();
        assert_eq!(inst.state, InstanceState::Terminating);
    }

    #[test]
    fn test_instance_type_resources() {
        assert_eq!(InstanceType::Dev.vcpus(), 1);
        assert_eq!(InstanceType::Dev.memory_gb(), 1);
        assert_eq!(InstanceType::XLarge.vcpus(), 16);
        assert_eq!(InstanceType::XLarge.memory_gb(), 64);

        let custom = InstanceType::Custom {
            vcpus: 32,
            memory_gb: 128,
        };
        assert_eq!(custom.vcpus(), 32);
        assert_eq!(custom.memory_gb(), 128);
    }
}
