//! # Automatic Schema Evolution
//!
//! Detect and migrate data schemas without downtime. Ferrite tracks field additions,
//! type changes, and provides migration scripts for seamless schema evolution.
//!
//! ## Why Schema Evolution?
//!
//! As applications evolve, data structures change. Traditional approaches require:
//! - Manual migration scripts
//! - Downtime for schema changes
//! - Complex versioning logic in application code
//!
//! Ferrite's schema evolution provides:
//! - **Automatic Detection**: Infer schema from data patterns
//! - **Version Tracking**: Keep history of all schema changes
//! - **Online Migration**: Migrate data without downtime
//! - **Backward Compatibility**: Old clients can still read data
//! - **Validation**: Enforce schema constraints on writes
//!
//! ## Quick Start
//!
//! ### Define a Schema
//!
//! ```no_run
//! use ferrite::schema::{Schema, SchemaField, FieldType, SchemaRegistry};
//!
//! // Create a schema for user documents
//! let user_schema = Schema::builder("user")
//!     .version(1)
//!     .field("id", FieldType::String, true)  // required
//!     .field("email", FieldType::String, true)
//!     .field("name", FieldType::String, false)
//!     .field("age", FieldType::Integer, false)
//!     .field("created_at", FieldType::Timestamp, true)
//!     .build();
//!
//! // Register the schema
//! let registry = SchemaRegistry::new();
//! registry.register(user_schema)?;
//! ```
//!
//! ### Evolve a Schema
//!
//! ```no_run
//! use ferrite::schema::{SchemaEvolution, EvolutionStrategy, FieldChange};
//!
//! // Define schema evolution from v1 to v2
//! let evolution = SchemaEvolution::new("user", 1, 2)
//!     .add_field("phone", FieldType::String, None)  // Add optional field
//!     .add_field("verified", FieldType::Boolean, Some("false"))  // With default
//!     .rename_field("name", "display_name")
//!     .deprecate_field("age")  // Mark for removal
//!     .build();
//!
//! // Apply the evolution
//! registry.evolve(evolution, EvolutionStrategy::Online)?;
//! ```
//!
//! ### Automatic Schema Inference
//!
//! ```no_run
//! use ferrite::schema::SchemaInferrer;
//!
//! // Infer schema from existing data
//! let inferrer = SchemaInferrer::new();
//! let inferred = inferrer.infer_from_pattern("users:*")?;
//!
//! println!("Detected fields:");
//! for field in inferred.fields() {
//!     println!("  {} ({:?}): {}% present",
//!         field.name, field.field_type, field.presence_ratio * 100.0);
//! }
//! ```
//!
//! ## Redis CLI Commands
//!
//! ```text
//! # Schema management
//! SCHEMA.CREATE user VERSION 1 FIELDS id:string:required email:string:required name:string
//! SCHEMA.GET user
//! SCHEMA.LIST
//! SCHEMA.VERSION user
//!
//! # Schema evolution
//! SCHEMA.EVOLVE user TO 2 ADD phone:string RENAME name display_name DEFAULT verified false
//! SCHEMA.MIGRATE user FROM 1 TO 2 [BATCH 1000] [DRY-RUN]
//! SCHEMA.MIGRATION.STATUS user
//!
//! # Schema inference
//! SCHEMA.INFER users:* [SAMPLE 1000]
//! SCHEMA.DIFF user INFERRED
//!
//! # Validation
//! SCHEMA.VALIDATE user KEY users:123
//! SCHEMA.ENFORCE user [ON|OFF]
//! ```
//!
//! ## Schema Types
//!
//! | Type | Description | JSON Mapping |
//! |------|-------------|--------------|
//! | String | Text data | string |
//! | Integer | 64-bit signed integer | number (integer) |
//! | Float | 64-bit floating point | number |
//! | Boolean | true/false | boolean |
//! | Timestamp | Unix timestamp or ISO8601 | string/number |
//! | Array | List of values | array |
//! | Object | Nested object | object |
//! | Binary | Raw bytes (base64 in JSON) | string |
//! | Any | No type constraint | any |
//!
//! ## Evolution Strategies
//!
//! | Strategy | Description | Use Case |
//! |----------|-------------|----------|
//! | Online | Migrate in background | Large datasets |
//! | Lazy | Migrate on read | Infrequent access |
//! | Immediate | Migrate all at once | Small datasets |
//! | DualWrite | Write both versions | Zero-downtime |
//!
//! ## Best Practices
//!
//! 1. **Always add fields as optional first**: Required fields need defaults
//! 2. **Use semantic versioning**: Major.Minor for breaking/non-breaking
//! 3. **Test migrations on replica**: Verify before production
//! 4. **Keep deprecation window**: Don't remove fields immediately
//! 5. **Document schema changes**: Use the description field

pub mod contracts;
mod evolution;
mod field;
mod inference;
mod migration;
mod registry;
mod types;
mod validation;

pub use evolution::{EvolutionStrategy, FieldChange, SchemaEvolution};
pub use field::{FieldConstraint, FieldDefault, SchemaField};
pub use inference::{InferredField, InferredSchema, SchemaInferrer};
pub use migration::{
    MigrationPlan, MigrationProgress, MigrationStateType, MigrationStatus, SchemaMigrator,
};
pub use registry::{SchemaRegistry, SchemaVersion};
pub use types::FieldType;
pub use validation::{SchemaValidator, ValidationError, ValidationResult};

use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// Schema configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaConfig {
    /// Enable schema tracking
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Enforce schema validation on writes
    #[serde(default)]
    pub enforce_validation: bool,

    /// Auto-infer schemas from data patterns
    #[serde(default = "default_auto_infer")]
    pub auto_infer: bool,

    /// Sample size for schema inference
    #[serde(default = "default_sample_size")]
    pub inference_sample_size: usize,

    /// Schema storage path
    #[serde(default = "default_schema_path")]
    pub schema_path: String,

    /// Maximum schema versions to keep
    #[serde(default = "default_max_versions")]
    pub max_versions: usize,

    /// Migration batch size
    #[serde(default = "default_batch_size")]
    pub migration_batch_size: usize,

    /// Migration parallelism
    #[serde(default = "default_parallelism")]
    pub migration_parallelism: usize,
}

fn default_enabled() -> bool {
    true
}

fn default_auto_infer() -> bool {
    false
}

fn default_sample_size() -> usize {
    1000
}

fn default_schema_path() -> String {
    "./data/schemas".to_string()
}

fn default_max_versions() -> usize {
    100
}

fn default_batch_size() -> usize {
    1000
}

fn default_parallelism() -> usize {
    4
}

impl Default for SchemaConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            enforce_validation: false,
            auto_infer: default_auto_infer(),
            inference_sample_size: default_sample_size(),
            schema_path: default_schema_path(),
            max_versions: default_max_versions(),
            migration_batch_size: default_batch_size(),
            migration_parallelism: default_parallelism(),
        }
    }
}

/// A schema definition for a data type
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Schema {
    /// Schema name/identifier
    pub name: String,

    /// Schema version
    pub version: u32,

    /// Schema fields
    pub fields: Vec<SchemaField>,

    /// Key pattern this schema applies to (e.g., "users:*")
    pub key_pattern: Option<String>,

    /// Human-readable description
    pub description: Option<String>,

    /// Schema creation timestamp
    pub created_at: u64,

    /// Schema modification timestamp
    pub updated_at: u64,

    /// Whether this schema is deprecated
    pub deprecated: bool,

    /// Deprecation message if deprecated
    pub deprecation_message: Option<String>,
}

impl Schema {
    /// Create a new schema builder
    pub fn builder(name: &str) -> SchemaBuilder {
        SchemaBuilder::new(name)
    }

    /// Get a field by name
    pub fn field(&self, name: &str) -> Option<&SchemaField> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Get all required fields
    pub fn required_fields(&self) -> impl Iterator<Item = &SchemaField> {
        self.fields.iter().filter(|f| f.required)
    }

    /// Check if schema has a field
    pub fn has_field(&self, name: &str) -> bool {
        self.fields.iter().any(|f| f.name == name)
    }

    /// Get the number of fields
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }

    /// Check compatibility with another schema version
    pub fn is_compatible_with(&self, other: &Schema) -> bool {
        // Check that all required fields in other exist in self
        for field in other.required_fields() {
            if !self.has_field(&field.name) {
                return false;
            }
            if let Some(self_field) = self.field(&field.name) {
                if !self_field.field_type.is_compatible_with(&field.field_type) {
                    return false;
                }
            }
        }
        true
    }
}

/// Builder for creating schemas
pub struct SchemaBuilder {
    name: String,
    version: u32,
    fields: Vec<SchemaField>,
    key_pattern: Option<String>,
    description: Option<String>,
}

impl SchemaBuilder {
    /// Create a new schema builder
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            version: 1,
            fields: Vec::new(),
            key_pattern: None,
            description: None,
        }
    }

    /// Set the schema version
    pub fn version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }

    /// Add a field to the schema
    pub fn field(mut self, name: &str, field_type: FieldType, required: bool) -> Self {
        self.fields.push(SchemaField {
            name: name.to_string(),
            field_type,
            required,
            default: None,
            description: None,
            constraints: Vec::new(),
            deprecated: false,
        });
        self
    }

    /// Add a field with a default value
    pub fn field_with_default(
        mut self,
        name: &str,
        field_type: FieldType,
        default: FieldDefault,
    ) -> Self {
        self.fields.push(SchemaField {
            name: name.to_string(),
            field_type,
            required: false,
            default: Some(default),
            description: None,
            constraints: Vec::new(),
            deprecated: false,
        });
        self
    }

    /// Set the key pattern
    pub fn key_pattern(mut self, pattern: &str) -> Self {
        self.key_pattern = Some(pattern.to_string());
        self
    }

    /// Set the description
    pub fn description(mut self, desc: &str) -> Self {
        self.description = Some(desc.to_string());
        self
    }

    /// Build the schema
    pub fn build(self) -> Schema {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Schema {
            name: self.name,
            version: self.version,
            fields: self.fields,
            key_pattern: self.key_pattern,
            description: self.description,
            created_at: now,
            updated_at: now,
            deprecated: false,
            deprecation_message: None,
        }
    }
}

/// Schema information for INFO command
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaInfo {
    /// Whether schema tracking is enabled
    pub enabled: bool,
    /// Number of registered schemas
    pub schema_count: u64,
    /// Total schema versions
    pub version_count: u64,
    /// Active migrations
    pub active_migrations: u64,
    /// Keys with schema violations
    pub violation_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_builder() {
        let schema = Schema::builder("user")
            .version(1)
            .field("id", FieldType::String, true)
            .field("email", FieldType::String, true)
            .field("name", FieldType::String, false)
            .field("age", FieldType::Integer, false)
            .key_pattern("users:*")
            .description("User profile schema")
            .build();

        assert_eq!(schema.name, "user");
        assert_eq!(schema.version, 1);
        assert_eq!(schema.field_count(), 4);
        assert!(schema.has_field("id"));
        assert!(schema.has_field("email"));
        assert!(!schema.has_field("password"));
        assert_eq!(schema.key_pattern, Some("users:*".to_string()));
    }

    #[test]
    fn test_schema_required_fields() {
        let schema = Schema::builder("user")
            .field("id", FieldType::String, true)
            .field("email", FieldType::String, true)
            .field("name", FieldType::String, false)
            .build();

        let required: Vec<_> = schema.required_fields().collect();
        assert_eq!(required.len(), 2);
        assert!(required.iter().any(|f| f.name == "id"));
        assert!(required.iter().any(|f| f.name == "email"));
    }

    #[test]
    fn test_schema_config_default() {
        let config = SchemaConfig::default();
        assert!(config.enabled);
        assert!(!config.enforce_validation);
        assert_eq!(config.inference_sample_size, 1000);
    }
}
