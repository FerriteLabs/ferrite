//! Data contracts for the Data Mesh Gateway.

#![forbid(unsafe_code)]

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use super::datasource::DataSchema;

/// Service-level agreement for a data contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractSla {
    pub max_latency_ms: u64,
    pub availability_pct: f64,
    pub max_stale_secs: u64,
}

impl Default for ContractSla {
    fn default() -> Self {
        Self {
            max_latency_ms: 100,
            availability_pct: 99.9,
            max_stale_secs: 60,
        }
    }
}

/// Lifecycle status of a data contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContractStatus {
    Active,
    Deprecated,
    Draft,
    Violated,
}

impl std::fmt::Display for ContractStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Deprecated => write!(f, "deprecated"),
            Self::Draft => write!(f, "draft"),
            Self::Violated => write!(f, "violated"),
        }
    }
}

/// A data contract binding a data source to a schema with SLA guarantees.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataContract {
    pub name: String,
    pub source_id: String,
    pub schema: DataSchema,
    pub owner: String,
    pub sla: ContractSla,
    pub created_at: DateTime<Utc>,
    pub version: u32,
    pub status: ContractStatus,
}

impl DataContract {
    /// Create a new contract in `Draft` status.
    pub fn new(name: String, source_id: String, schema: DataSchema) -> Self {
        Self {
            name,
            source_id,
            schema,
            owner: String::new(),
            sla: ContractSla::default(),
            created_at: Utc::now(),
            version: 1,
            status: ContractStatus::Draft,
        }
    }
}

/// Thread-safe registry of data contracts.
pub struct ContractRegistry {
    contracts: DashMap<String, DataContract>,
}

impl ContractRegistry {
    pub fn new() -> Self {
        Self {
            contracts: DashMap::new(),
        }
    }

    /// Register a contract.
    pub fn add(&self, contract: DataContract) -> Result<(), String> {
        if self.contracts.contains_key(&contract.name) {
            return Err(format!("contract '{}' already exists", contract.name));
        }
        self.contracts.insert(contract.name.clone(), contract);
        Ok(())
    }

    /// Remove a contract by name.
    pub fn remove(&self, name: &str) -> Result<(), String> {
        self.contracts
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| format!("contract '{name}' not found"))
    }

    /// List all registered contracts.
    pub fn list(&self) -> Vec<DataContract> {
        self.contracts.iter().map(|r| r.value().clone()).collect()
    }

    /// Validate that a contract's schema is non-empty.
    pub fn validate(&self, name: &str) -> Result<bool, String> {
        let contract = self
            .contracts
            .get(name)
            .ok_or_else(|| format!("contract '{name}' not found"))?;
        Ok(!contract.schema.fields.is_empty())
    }
}

impl Default for ContractRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mesh::datasource::{DataSchema, FieldType, SchemaField};

    fn test_schema() -> DataSchema {
        DataSchema {
            namespace: "test".to_string(),
            fields: vec![SchemaField {
                name: "id".to_string(),
                field_type: FieldType::Integer,
                nullable: false,
            }],
            version: 1,
        }
    }

    #[test]
    fn test_add_and_list_contracts() {
        let reg = ContractRegistry::new();
        let c = DataContract::new("c1".into(), "src1".into(), test_schema());
        reg.add(c).unwrap();
        assert_eq!(reg.list().len(), 1);
    }

    #[test]
    fn test_duplicate_contract_rejected() {
        let reg = ContractRegistry::new();
        let c1 = DataContract::new("c1".into(), "src1".into(), test_schema());
        let c2 = DataContract::new("c1".into(), "src1".into(), test_schema());
        reg.add(c1).unwrap();
        assert!(reg.add(c2).is_err());
    }

    #[test]
    fn test_remove_contract() {
        let reg = ContractRegistry::new();
        let c = DataContract::new("c1".into(), "src1".into(), test_schema());
        reg.add(c).unwrap();
        reg.remove("c1").unwrap();
        assert!(reg.list().is_empty());
    }

    #[test]
    fn test_validate_contract() {
        let reg = ContractRegistry::new();
        let c = DataContract::new("c1".into(), "src1".into(), test_schema());
        reg.add(c).unwrap();
        assert!(reg.validate("c1").unwrap());
    }
}
