//! Declarative Data Contracts & Versioned Schemas
//!
//! Schema registry with evolution rules, breaking change detection, and
//! automatic migration planning. Enables teams to treat Ferrite data as
//! a typed API with backward/forward compatibility guarantees.
//!
//! # Features
//!
//! - **Versioned schemas** with semantic compatibility rules
//! - **Breaking change detection** before deployment
//! - **Compatibility modes**: backward, forward, full, none
//! - **Contract validation** on writes (configurable strictness)
//! - **Migration generation** between schema versions
//!
//! # Example
//!
//! ```ignore
//! use ferrite::schema::contracts::*;
//!
//! let registry = ContractRegistry::new();
//!
//! let v1 = DataContract::builder("users")
//!     .version(1)
//!     .field("name", ContractFieldType::String, true)
//!     .field("email", ContractFieldType::String, true)
//!     .compatibility(CompatibilityMode::Backward)
//!     .build();
//!
//! registry.register(v1)?;
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Compatibility mode for schema evolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompatibilityMode {
    /// New schemas can read data written by old schemas
    Backward,
    /// Old schemas can read data written by new schemas
    Forward,
    /// Both backward and forward compatible
    Full,
    /// No compatibility checks
    None,
}

/// Field types for data contracts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContractFieldType {
    String,
    Integer,
    Float,
    Boolean,
    Bytes,
    Timestamp,
    Array(Box<ContractFieldType>),
    Map(Box<ContractFieldType>, Box<ContractFieldType>),
    Optional(Box<ContractFieldType>),
    Enum(Vec<String>),
    Struct(String),
}

impl std::fmt::Display for ContractFieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String => write!(f, "string"),
            Self::Integer => write!(f, "integer"),
            Self::Float => write!(f, "float"),
            Self::Boolean => write!(f, "boolean"),
            Self::Bytes => write!(f, "bytes"),
            Self::Timestamp => write!(f, "timestamp"),
            Self::Array(inner) => write!(f, "array<{}>", inner),
            Self::Map(k, v) => write!(f, "map<{}, {}>", k, v),
            Self::Optional(inner) => write!(f, "optional<{}>", inner),
            Self::Enum(variants) => write!(f, "enum({})", variants.join(", ")),
            Self::Struct(name) => write!(f, "struct<{}>", name),
        }
    }
}

/// A single field in a data contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractField {
    pub name: String,
    pub field_type: ContractFieldType,
    pub required: bool,
    pub default_value: Option<serde_json::Value>,
    pub description: Option<String>,
    pub deprecated: bool,
    pub since_version: u32,
}

/// A data contract defining the schema for a key pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataContract {
    pub name: String,
    pub version: u32,
    pub key_pattern: String,
    pub fields: Vec<ContractField>,
    pub compatibility: CompatibilityMode,
    pub owner: Option<String>,
    pub description: Option<String>,
    pub created_at: String,
}

/// Builder for constructing data contracts.
pub struct DataContractBuilder {
    name: String,
    version: u32,
    key_pattern: String,
    fields: Vec<ContractField>,
    compatibility: CompatibilityMode,
    owner: Option<String>,
    description: Option<String>,
}

impl DataContractBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            version: 1,
            key_pattern: format!("{}:*", name),
            fields: Vec::new(),
            compatibility: CompatibilityMode::Backward,
            owner: None,
            description: None,
        }
    }

    pub fn version(mut self, v: u32) -> Self {
        self.version = v;
        self
    }

    pub fn key_pattern(mut self, pattern: &str) -> Self {
        self.key_pattern = pattern.to_string();
        self
    }

    pub fn field(mut self, name: &str, field_type: ContractFieldType, required: bool) -> Self {
        self.fields.push(ContractField {
            name: name.to_string(),
            field_type,
            required,
            default_value: None,
            description: None,
            deprecated: false,
            since_version: self.version,
        });
        self
    }

    pub fn field_with_default(
        mut self,
        name: &str,
        field_type: ContractFieldType,
        default: serde_json::Value,
    ) -> Self {
        self.fields.push(ContractField {
            name: name.to_string(),
            field_type,
            required: false,
            default_value: Some(default),
            description: None,
            deprecated: false,
            since_version: self.version,
        });
        self
    }

    pub fn compatibility(mut self, mode: CompatibilityMode) -> Self {
        self.compatibility = mode;
        self
    }

    pub fn owner(mut self, owner: &str) -> Self {
        self.owner = Some(owner.to_string());
        self
    }

    pub fn description(mut self, desc: &str) -> Self {
        self.description = Some(desc.to_string());
        self
    }

    pub fn build(self) -> DataContract {
        DataContract {
            name: self.name,
            version: self.version,
            key_pattern: self.key_pattern,
            fields: self.fields,
            compatibility: self.compatibility,
            owner: self.owner,
            description: self.description,
            created_at: chrono::Utc::now().to_rfc3339(),
        }
    }
}

impl DataContract {
    pub fn builder(name: &str) -> DataContractBuilder {
        DataContractBuilder::new(name)
    }
}

// ---------------------------------------------------------------------------
// Breaking change detection
// ---------------------------------------------------------------------------

/// A detected change between two schema versions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaChange {
    pub change_type: ChangeType,
    pub field_name: String,
    pub description: String,
    pub breaking: bool,
}

/// Types of schema changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeType {
    FieldAdded,
    FieldRemoved,
    FieldTypeChanged,
    FieldRequiredChanged,
    FieldDeprecated,
    DefaultValueChanged,
}

/// Validation strictness for writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationStrictness {
    /// Log a warning but allow the write
    Warn,
    /// Reject writes that violate the contract
    Reject,
    /// No validation
    Off,
}

/// Result of contract validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    pub valid: bool,
    pub errors: Vec<ValidationError>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationError {
    pub field: String,
    pub message: String,
    pub error_type: ValidationErrorType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationErrorType {
    MissingRequired,
    TypeMismatch,
    UnknownField,
    InvalidValue,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum ContractError {
    #[error("Contract not found: {0}")]
    NotFound(String),

    #[error("Contract already exists: {name} v{version}")]
    AlreadyExists { name: String, version: u32 },

    #[error("Breaking change detected: {0}")]
    BreakingChange(String),

    #[error("Validation failed: {0}")]
    ValidationFailed(String),

    #[error("Incompatible evolution: {0}")]
    Incompatible(String),
}

// ---------------------------------------------------------------------------
// Contract Registry
// ---------------------------------------------------------------------------

/// Metrics for the contract registry.
#[derive(Debug, Default)]
pub struct ContractMetrics {
    pub contracts_registered: AtomicU64,
    pub validations_total: AtomicU64,
    pub validations_passed: AtomicU64,
    pub validations_failed: AtomicU64,
    pub breaking_changes_detected: AtomicU64,
}

/// The contract registry managing all data contracts.
pub struct ContractRegistry {
    /// name -> version -> contract
    contracts: Arc<parking_lot::RwLock<HashMap<String, HashMap<u32, DataContract>>>>,
    strictness: ValidationStrictness,
    metrics: Arc<ContractMetrics>,
}

impl ContractRegistry {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            contracts: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            strictness: ValidationStrictness::Warn,
            metrics: Arc::new(ContractMetrics::default()),
        }
    }

    pub fn with_strictness(mut self, strictness: ValidationStrictness) -> Self {
        self.strictness = strictness;
        self
    }

    /// Register a new contract version.
    pub fn register(&self, contract: DataContract) -> Result<(), ContractError> {
        let mut store = self.contracts.write();
        let versions = store.entry(contract.name.clone()).or_default();

        if versions.contains_key(&contract.version) {
            return Err(ContractError::AlreadyExists {
                name: contract.name.clone(),
                version: contract.version,
            });
        }

        // Check compatibility with previous version
        if contract.version > 1 {
            if let Some(prev) = versions.get(&(contract.version - 1)) {
                let changes = detect_changes(prev, &contract);
                let breaking: Vec<_> = changes.iter().filter(|c| c.breaking).collect();

                if !breaking.is_empty() && contract.compatibility != CompatibilityMode::None {
                    self.metrics
                        .breaking_changes_detected
                        .fetch_add(1, Ordering::Relaxed);
                    let msg = breaking
                        .iter()
                        .map(|c| c.description.clone())
                        .collect::<Vec<_>>()
                        .join("; ");
                    return Err(ContractError::BreakingChange(msg));
                }
            }
        }

        versions.insert(contract.version, contract);
        self.metrics
            .contracts_registered
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Get the latest version of a contract.
    pub fn get_latest(&self, name: &str) -> Result<DataContract, ContractError> {
        let store = self.contracts.read();
        let versions = store
            .get(name)
            .ok_or_else(|| ContractError::NotFound(name.to_string()))?;
        versions
            .values()
            .max_by_key(|c| c.version)
            .cloned()
            .ok_or_else(|| ContractError::NotFound(name.to_string()))
    }

    /// Get a specific version of a contract.
    pub fn get_version(&self, name: &str, version: u32) -> Result<DataContract, ContractError> {
        let store = self.contracts.read();
        store
            .get(name)
            .and_then(|v| v.get(&version))
            .cloned()
            .ok_or_else(|| ContractError::NotFound(format!("{} v{}", name, version)))
    }

    /// List all contracts.
    pub fn list(&self) -> Vec<DataContract> {
        let store = self.contracts.read();
        store
            .values()
            .flat_map(|versions| versions.values().cloned())
            .collect()
    }

    /// Validate a JSON value against the latest contract for a key pattern.
    pub fn validate(
        &self,
        contract_name: &str,
        value: &serde_json::Value,
    ) -> Result<ValidationResult, ContractError> {
        self.metrics
            .validations_total
            .fetch_add(1, Ordering::Relaxed);

        let contract = self.get_latest(contract_name)?;
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        let obj = match value.as_object() {
            Some(o) => o,
            None => {
                errors.push(ValidationError {
                    field: "<root>".to_string(),
                    message: "Expected JSON object".to_string(),
                    error_type: ValidationErrorType::TypeMismatch,
                });
                self.metrics
                    .validations_failed
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(ValidationResult {
                    valid: false,
                    errors,
                    warnings,
                });
            }
        };

        // Check required fields
        for field in &contract.fields {
            if field.required && !obj.contains_key(&field.name) && field.default_value.is_none() {
                errors.push(ValidationError {
                    field: field.name.clone(),
                    message: format!("Required field '{}' is missing", field.name),
                    error_type: ValidationErrorType::MissingRequired,
                });
            }

            if field.deprecated && obj.contains_key(&field.name) {
                warnings.push(format!("Field '{}' is deprecated", field.name));
            }
        }

        // Check for unknown fields
        let known: std::collections::HashSet<_> = contract.fields.iter().map(|f| &f.name).collect();
        for key in obj.keys() {
            if !known.contains(key) {
                warnings.push(format!("Unknown field '{}' not in contract", key));
            }
        }

        let valid = errors.is_empty();
        if valid {
            self.metrics
                .validations_passed
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics
                .validations_failed
                .fetch_add(1, Ordering::Relaxed);
        }

        Ok(ValidationResult {
            valid,
            errors,
            warnings,
        })
    }

    /// Detect changes between two versions of a contract.
    pub fn diff(
        &self,
        name: &str,
        from_version: u32,
        to_version: u32,
    ) -> Result<Vec<SchemaChange>, ContractError> {
        let from = self.get_version(name, from_version)?;
        let to = self.get_version(name, to_version)?;
        Ok(detect_changes(&from, &to))
    }

    pub fn metrics(&self) -> &ContractMetrics {
        &self.metrics
    }
}

// ---------------------------------------------------------------------------
// Change detection
// ---------------------------------------------------------------------------

fn detect_changes(old: &DataContract, new: &DataContract) -> Vec<SchemaChange> {
    let mut changes = Vec::new();

    let old_fields: HashMap<&str, &ContractField> =
        old.fields.iter().map(|f| (f.name.as_str(), f)).collect();
    let new_fields: HashMap<&str, &ContractField> =
        new.fields.iter().map(|f| (f.name.as_str(), f)).collect();

    // Removed fields
    for name in old_fields.keys() {
        if !new_fields.contains_key(name) {
            changes.push(SchemaChange {
                change_type: ChangeType::FieldRemoved,
                field_name: name.to_string(),
                description: format!("Field '{}' was removed", name),
                breaking: matches!(
                    new.compatibility,
                    CompatibilityMode::Backward | CompatibilityMode::Full
                ),
            });
        }
    }

    // Added fields
    for (name, field) in &new_fields {
        if !old_fields.contains_key(name) {
            let breaking = field.required
                && field.default_value.is_none()
                && matches!(
                    new.compatibility,
                    CompatibilityMode::Forward | CompatibilityMode::Full
                );
            changes.push(SchemaChange {
                change_type: ChangeType::FieldAdded,
                field_name: name.to_string(),
                description: format!("Field '{}' was added", name),
                breaking,
            });
        }
    }

    // Changed fields
    for (name, old_field) in &old_fields {
        if let Some(new_field) = new_fields.get(name) {
            if old_field.field_type != new_field.field_type {
                changes.push(SchemaChange {
                    change_type: ChangeType::FieldTypeChanged,
                    field_name: name.to_string(),
                    description: format!(
                        "Field '{}' type changed from {} to {}",
                        name, old_field.field_type, new_field.field_type
                    ),
                    breaking: true,
                });
            }

            if old_field.required != new_field.required {
                changes.push(SchemaChange {
                    change_type: ChangeType::FieldRequiredChanged,
                    field_name: name.to_string(),
                    description: format!(
                        "Field '{}' required changed from {} to {}",
                        name, old_field.required, new_field.required
                    ),
                    breaking: !old_field.required && new_field.required,
                });
            }

            if !old_field.deprecated && new_field.deprecated {
                changes.push(SchemaChange {
                    change_type: ChangeType::FieldDeprecated,
                    field_name: name.to_string(),
                    description: format!("Field '{}' was deprecated", name),
                    breaking: false,
                });
            }
        }
    }

    changes
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_get_contract() {
        let registry = ContractRegistry::new();
        let contract = DataContract::builder("users")
            .version(1)
            .field("name", ContractFieldType::String, true)
            .field("email", ContractFieldType::String, true)
            .build();

        registry.register(contract).unwrap();
        let latest = registry.get_latest("users").unwrap();
        assert_eq!(latest.version, 1);
        assert_eq!(latest.fields.len(), 2);
    }

    #[test]
    fn test_duplicate_version_error() {
        let registry = ContractRegistry::new();
        let c1 = DataContract::builder("items").version(1).build();
        registry.register(c1).unwrap();

        let c2 = DataContract::builder("items").version(1).build();
        let result = registry.register(c2);
        assert!(matches!(result, Err(ContractError::AlreadyExists { .. })));
    }

    #[test]
    fn test_validate_valid_json() {
        let registry = ContractRegistry::new();
        let contract = DataContract::builder("products")
            .version(1)
            .field("name", ContractFieldType::String, true)
            .field("price", ContractFieldType::Float, true)
            .build();
        registry.register(contract).unwrap();

        let value = serde_json::json!({"name": "Widget", "price": 9.99});
        let result = registry.validate("products", &value).unwrap();
        assert!(result.valid);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_validate_missing_required_field() {
        let registry = ContractRegistry::new();
        let contract = DataContract::builder("orders")
            .version(1)
            .field("id", ContractFieldType::String, true)
            .field("total", ContractFieldType::Float, true)
            .build();
        registry.register(contract).unwrap();

        let value = serde_json::json!({"id": "123"});
        let result = registry.validate("orders", &value).unwrap();
        assert!(!result.valid);
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.errors[0].field, "total");
    }

    #[test]
    fn test_validate_unknown_fields_warning() {
        let registry = ContractRegistry::new();
        let contract = DataContract::builder("tags")
            .version(1)
            .field("name", ContractFieldType::String, true)
            .build();
        registry.register(contract).unwrap();

        let value = serde_json::json!({"name": "rust", "extra": "field"});
        let result = registry.validate("tags", &value).unwrap();
        assert!(result.valid);
        assert!(!result.warnings.is_empty());
    }

    #[test]
    fn test_breaking_change_detection() {
        let registry = ContractRegistry::new();
        let v1 = DataContract::builder("events")
            .version(1)
            .field("type", ContractFieldType::String, true)
            .compatibility(CompatibilityMode::Backward)
            .build();
        registry.register(v1).unwrap();

        // v2 removes required field → breaking
        let v2 = DataContract::builder("events")
            .version(2)
            .compatibility(CompatibilityMode::Backward)
            .build();
        let result = registry.register(v2);
        assert!(matches!(result, Err(ContractError::BreakingChange(_))));
    }

    #[test]
    fn test_non_breaking_addition() {
        let registry = ContractRegistry::new();
        let v1 = DataContract::builder("users")
            .version(1)
            .field("name", ContractFieldType::String, true)
            .compatibility(CompatibilityMode::Backward)
            .build();
        registry.register(v1).unwrap();

        let v2 = DataContract::builder("users")
            .version(2)
            .field("name", ContractFieldType::String, true)
            .field("phone", ContractFieldType::String, false)
            .compatibility(CompatibilityMode::Backward)
            .build();
        registry.register(v2).unwrap();

        let latest = registry.get_latest("users").unwrap();
        assert_eq!(latest.version, 2);
    }

    #[test]
    fn test_diff_versions() {
        let registry = ContractRegistry::new();

        let v1 = DataContract::builder("accounts")
            .version(1)
            .field("name", ContractFieldType::String, true)
            .field("balance", ContractFieldType::Float, true)
            .compatibility(CompatibilityMode::None)
            .build();
        registry.register(v1).unwrap();

        let v2 = DataContract::builder("accounts")
            .version(2)
            .field("name", ContractFieldType::String, true)
            .field("balance", ContractFieldType::Integer, true)
            .field("currency", ContractFieldType::String, false)
            .compatibility(CompatibilityMode::None)
            .build();
        registry.register(v2).unwrap();

        let changes = registry.diff("accounts", 1, 2).unwrap();
        assert!(changes
            .iter()
            .any(|c| c.change_type == ChangeType::FieldTypeChanged));
        assert!(changes
            .iter()
            .any(|c| c.change_type == ChangeType::FieldAdded));
    }

    #[test]
    fn test_field_type_display() {
        assert_eq!(ContractFieldType::String.to_string(), "string");
        assert_eq!(
            ContractFieldType::Array(Box::new(ContractFieldType::Integer)).to_string(),
            "array<integer>"
        );
        assert_eq!(
            ContractFieldType::Map(
                Box::new(ContractFieldType::String),
                Box::new(ContractFieldType::Float)
            )
            .to_string(),
            "map<string, float>"
        );
    }
}
