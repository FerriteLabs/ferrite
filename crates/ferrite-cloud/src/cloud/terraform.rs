//! Terraform and Infrastructure-as-Code provider support.
//!
//! Generates Terraform resource schemas and handles CRUD operations
//! for managing Ferrite Cloud instances declaratively.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Terraform resource type for a Ferrite Cloud instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerraformResource {
    /// Resource type identifier (e.g., `ferrite_instance`).
    pub resource_type: String,
    /// Resource name.
    pub name: String,
    /// Resource attributes.
    pub attributes: HashMap<String, TerraformValue>,
    /// Read-only computed attributes.
    pub computed: HashMap<String, TerraformValue>,
}

/// Terraform-compatible value types.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TerraformValue {
    String(String),
    Number(f64),
    Bool(bool),
    List(Vec<TerraformValue>),
    Map(HashMap<String, TerraformValue>),
}

/// Supported Terraform resource types for Ferrite Cloud.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceType {
    /// A managed Ferrite instance.
    Instance,
    /// A backup schedule.
    BackupSchedule,
    /// A VPC peering connection.
    VpcPeering,
    /// An ACL user.
    AclUser,
    /// A replication group.
    ReplicationGroup,
}

/// Schema definition for a Terraform resource type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceSchema {
    pub resource_type: ResourceType,
    pub attributes: Vec<AttributeSchema>,
}

/// Schema for a single attribute.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributeSchema {
    pub name: String,
    pub attr_type: AttributeType,
    pub required: bool,
    pub computed: bool,
    pub description: String,
}

/// Attribute value types for schema definitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttributeType {
    String,
    Number,
    Bool,
    ListOfStrings,
    MapOfStrings,
}

/// Generates the Terraform provider schema for all Ferrite Cloud resources.
pub fn generate_provider_schema() -> Vec<ResourceSchema> {
    vec![
        ResourceSchema {
            resource_type: ResourceType::Instance,
            attributes: vec![
                AttributeSchema {
                    name: "name".to_string(),
                    attr_type: AttributeType::String,
                    required: true,
                    computed: false,
                    description: "Name of the Ferrite instance".to_string(),
                },
                AttributeSchema {
                    name: "region".to_string(),
                    attr_type: AttributeType::String,
                    required: true,
                    computed: false,
                    description: "Cloud region for deployment".to_string(),
                },
                AttributeSchema {
                    name: "tier".to_string(),
                    attr_type: AttributeType::String,
                    required: false,
                    computed: false,
                    description: "Service tier (free, standard, premium)".to_string(),
                },
                AttributeSchema {
                    name: "memory_gb".to_string(),
                    attr_type: AttributeType::Number,
                    required: false,
                    computed: false,
                    description: "Memory allocation in GB".to_string(),
                },
                AttributeSchema {
                    name: "endpoint".to_string(),
                    attr_type: AttributeType::String,
                    required: false,
                    computed: true,
                    description: "Connection endpoint (computed)".to_string(),
                },
                AttributeSchema {
                    name: "id".to_string(),
                    attr_type: AttributeType::String,
                    required: false,
                    computed: true,
                    description: "Instance ID (computed)".to_string(),
                },
            ],
        },
        ResourceSchema {
            resource_type: ResourceType::BackupSchedule,
            attributes: vec![
                AttributeSchema {
                    name: "instance_id".to_string(),
                    attr_type: AttributeType::String,
                    required: true,
                    computed: false,
                    description: "ID of the instance to back up".to_string(),
                },
                AttributeSchema {
                    name: "cron".to_string(),
                    attr_type: AttributeType::String,
                    required: true,
                    computed: false,
                    description: "Cron expression for backup schedule".to_string(),
                },
                AttributeSchema {
                    name: "retention_days".to_string(),
                    attr_type: AttributeType::Number,
                    required: false,
                    computed: false,
                    description: "Number of days to retain backups".to_string(),
                },
            ],
        },
    ]
}

/// Free-tier resource limits for Ferrite Cloud.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreeTierLimits {
    pub max_memory_mb: u64,
    pub max_connections: u32,
    pub max_keys: u64,
    pub max_bandwidth_mbps: u32,
    pub max_instances: u32,
    pub data_transfer_gb_per_month: u64,
}

impl Default for FreeTierLimits {
    fn default() -> Self {
        Self {
            max_memory_mb: 256,
            max_connections: 100,
            max_keys: 100_000,
            max_bandwidth_mbps: 10,
            max_instances: 1,
            data_transfer_gb_per_month: 5,
        }
    }
}

/// Service tier definitions with resource limits and pricing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceTier {
    pub name: String,
    pub max_memory_gb: u64,
    pub max_connections: u32,
    pub max_ops_per_second: u64,
    pub price_per_hour_usd: f64,
    pub sla_uptime_percent: f64,
    pub support_level: SupportLevel,
}

/// Support tier for cloud service plans.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SupportLevel {
    Community,
    Standard,
    Premium,
    Enterprise,
}

/// Returns all available service tiers.
pub fn available_tiers() -> Vec<ServiceTier> {
    vec![
        ServiceTier {
            name: "free".to_string(),
            max_memory_gb: 0,
            max_connections: 100,
            max_ops_per_second: 1_000,
            price_per_hour_usd: 0.0,
            sla_uptime_percent: 99.0,
            support_level: SupportLevel::Community,
        },
        ServiceTier {
            name: "standard".to_string(),
            max_memory_gb: 16,
            max_connections: 10_000,
            max_ops_per_second: 100_000,
            price_per_hour_usd: 0.10,
            sla_uptime_percent: 99.9,
            support_level: SupportLevel::Standard,
        },
        ServiceTier {
            name: "premium".to_string(),
            max_memory_gb: 128,
            max_connections: 50_000,
            max_ops_per_second: 1_000_000,
            price_per_hour_usd: 0.50,
            sla_uptime_percent: 99.99,
            support_level: SupportLevel::Premium,
        },
        ServiceTier {
            name: "enterprise".to_string(),
            max_memory_gb: 1024,
            max_connections: 200_000,
            max_ops_per_second: 10_000_000,
            price_per_hour_usd: 2.00,
            sla_uptime_percent: 99.999,
            support_level: SupportLevel::Enterprise,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provider_schema_generation() {
        let schemas = generate_provider_schema();
        assert_eq!(schemas.len(), 2);
        assert_eq!(schemas[0].resource_type, ResourceType::Instance);
        assert_eq!(schemas[1].resource_type, ResourceType::BackupSchedule);
    }

    #[test]
    fn test_instance_schema_has_required_fields() {
        let schemas = generate_provider_schema();
        let instance = &schemas[0];
        let required: Vec<_> = instance
            .attributes
            .iter()
            .filter(|a| a.required)
            .map(|a| a.name.as_str())
            .collect();
        assert!(required.contains(&"name"));
        assert!(required.contains(&"region"));
    }

    #[test]
    fn test_instance_schema_has_computed_fields() {
        let schemas = generate_provider_schema();
        let instance = &schemas[0];
        let computed: Vec<_> = instance
            .attributes
            .iter()
            .filter(|a| a.computed)
            .map(|a| a.name.as_str())
            .collect();
        assert!(computed.contains(&"endpoint"));
        assert!(computed.contains(&"id"));
    }

    #[test]
    fn test_free_tier_limits() {
        let limits = FreeTierLimits::default();
        assert_eq!(limits.max_memory_mb, 256);
        assert_eq!(limits.max_instances, 1);
        assert_eq!(limits.max_connections, 100);
    }

    #[test]
    fn test_available_tiers() {
        let tiers = available_tiers();
        assert_eq!(tiers.len(), 4);
        assert_eq!(tiers[0].name, "free");
        assert_eq!(tiers[0].price_per_hour_usd, 0.0);
        assert_eq!(tiers[3].name, "enterprise");
        assert_eq!(tiers[3].support_level, SupportLevel::Enterprise);
    }

    #[test]
    fn test_terraform_value_serialization() {
        let val = TerraformValue::String("test".to_string());
        let json = serde_json::to_string(&val).unwrap();
        assert_eq!(json, "\"test\"");

        let val = TerraformValue::Number(42.0);
        let json = serde_json::to_string(&val).unwrap();
        assert_eq!(json, "42.0");
    }
}
