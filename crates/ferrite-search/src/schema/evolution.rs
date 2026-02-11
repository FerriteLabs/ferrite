//! Schema evolution for tracking and applying schema changes

use super::{FieldDefault, FieldType, Schema, SchemaField};
use serde::{Deserialize, Serialize};

/// Schema evolution definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaEvolution {
    /// Schema name
    pub schema_name: String,
    /// Source version
    pub from_version: u32,
    /// Target version
    pub to_version: u32,
    /// List of changes
    pub changes: Vec<FieldChange>,
    /// Evolution description
    pub description: Option<String>,
    /// Whether this is a breaking change
    pub breaking: bool,
}

impl SchemaEvolution {
    /// Create a new schema evolution
    #[allow(clippy::new_ret_no_self)]
    pub fn new(schema_name: &str, from_version: u32, to_version: u32) -> SchemaEvolutionBuilder {
        SchemaEvolutionBuilder {
            schema_name: schema_name.to_string(),
            from_version,
            to_version,
            changes: Vec::new(),
            description: None,
        }
    }

    /// Check if this evolution is backward compatible
    pub fn is_backward_compatible(&self) -> bool {
        !self.breaking && self.changes.iter().all(|c| c.is_backward_compatible())
    }

    /// Apply evolution to a schema to produce new version
    pub fn apply(&self, schema: &Schema) -> Result<Schema, EvolutionError> {
        if schema.version != self.from_version {
            return Err(EvolutionError::VersionMismatch {
                expected: self.from_version,
                actual: schema.version,
            });
        }

        let mut new_fields: Vec<SchemaField> = schema.fields.clone();

        for change in &self.changes {
            match change {
                FieldChange::Add {
                    name,
                    field_type,
                    required,
                    default,
                } => {
                    // Check if field already exists
                    if new_fields.iter().any(|f| f.name == *name) {
                        return Err(EvolutionError::FieldAlreadyExists(name.clone()));
                    }

                    // Required fields must have defaults
                    if *required && default.is_none() {
                        return Err(EvolutionError::RequiredFieldNeedsDefault(name.clone()));
                    }

                    new_fields.push(SchemaField {
                        name: name.clone(),
                        field_type: field_type.clone(),
                        required: *required,
                        default: default.clone(),
                        description: None,
                        constraints: Vec::new(),
                        deprecated: false,
                    });
                }

                FieldChange::Remove { name, .. } => {
                    let idx = new_fields
                        .iter()
                        .position(|f| f.name == *name)
                        .ok_or_else(|| EvolutionError::FieldNotFound(name.clone()))?;
                    new_fields.remove(idx);
                }

                FieldChange::Rename { from, to } => {
                    // Check target doesn't exist
                    if new_fields.iter().any(|f| f.name == *to) {
                        return Err(EvolutionError::FieldAlreadyExists(to.clone()));
                    }

                    let field = new_fields
                        .iter_mut()
                        .find(|f| f.name == *from)
                        .ok_or_else(|| EvolutionError::FieldNotFound(from.clone()))?;

                    field.name = to.clone();
                }

                FieldChange::ChangeType { name, new_type, .. } => {
                    let field = new_fields
                        .iter_mut()
                        .find(|f| f.name == *name)
                        .ok_or_else(|| EvolutionError::FieldNotFound(name.clone()))?;

                    field.field_type = new_type.clone();
                }

                FieldChange::MakeOptional { name } => {
                    let field = new_fields
                        .iter_mut()
                        .find(|f| f.name == *name)
                        .ok_or_else(|| EvolutionError::FieldNotFound(name.clone()))?;

                    field.required = false;
                }

                FieldChange::MakeRequired { name, default } => {
                    let field = new_fields
                        .iter_mut()
                        .find(|f| f.name == *name)
                        .ok_or_else(|| EvolutionError::FieldNotFound(name.clone()))?;

                    // Must provide default for existing records
                    if default.is_none() {
                        return Err(EvolutionError::RequiredFieldNeedsDefault(name.clone()));
                    }

                    field.required = true;
                    field.default = default.clone();
                }

                FieldChange::SetDefault { name, default } => {
                    let field = new_fields
                        .iter_mut()
                        .find(|f| f.name == *name)
                        .ok_or_else(|| EvolutionError::FieldNotFound(name.clone()))?;

                    field.default = default.clone();
                }

                FieldChange::Deprecate { name, message } => {
                    let field = new_fields
                        .iter_mut()
                        .find(|f| f.name == *name)
                        .ok_or_else(|| EvolutionError::FieldNotFound(name.clone()))?;

                    field.deprecated = true;
                    if let Some(msg) = message {
                        field.description = Some(format!(
                            "{} [DEPRECATED: {}]",
                            field.description.as_deref().unwrap_or(""),
                            msg
                        ));
                    }
                }
            }
        }

        Ok(Schema {
            name: schema.name.clone(),
            version: self.to_version,
            fields: new_fields,
            key_pattern: schema.key_pattern.clone(),
            description: schema.description.clone(),
            created_at: schema.created_at,
            updated_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            deprecated: false,
            deprecation_message: None,
        })
    }

    /// Generate migration script for this evolution
    pub fn generate_migration_script(&self) -> String {
        let mut script = String::new();

        script.push_str(&format!(
            "-- Schema Evolution: {} v{} -> v{}\n",
            self.schema_name, self.from_version, self.to_version
        ));

        if let Some(ref desc) = self.description {
            script.push_str(&format!("-- {}\n", desc));
        }

        script.push_str(&format!(
            "-- Breaking: {}\n\n",
            if self.breaking { "YES" } else { "NO" }
        ));

        for change in &self.changes {
            script.push_str(&format!("{}\n", change.to_migration_command()));
        }

        script
    }
}

/// Builder for schema evolution
pub struct SchemaEvolutionBuilder {
    schema_name: String,
    from_version: u32,
    to_version: u32,
    changes: Vec<FieldChange>,
    description: Option<String>,
}

impl SchemaEvolutionBuilder {
    /// Add a new field
    pub fn add_field(
        mut self,
        name: &str,
        field_type: FieldType,
        default: Option<FieldDefault>,
    ) -> Self {
        self.changes.push(FieldChange::Add {
            name: name.to_string(),
            field_type,
            required: false,
            default,
        });
        self
    }

    /// Add a required field (must have default)
    pub fn add_required_field(
        mut self,
        name: &str,
        field_type: FieldType,
        default: FieldDefault,
    ) -> Self {
        self.changes.push(FieldChange::Add {
            name: name.to_string(),
            field_type,
            required: true,
            default: Some(default),
        });
        self
    }

    /// Remove a field
    pub fn remove_field(mut self, name: &str) -> Self {
        self.changes.push(FieldChange::Remove {
            name: name.to_string(),
            archive: false,
        });
        self
    }

    /// Remove a field but archive data
    pub fn remove_and_archive_field(mut self, name: &str) -> Self {
        self.changes.push(FieldChange::Remove {
            name: name.to_string(),
            archive: true,
        });
        self
    }

    /// Rename a field
    pub fn rename_field(mut self, from: &str, to: &str) -> Self {
        self.changes.push(FieldChange::Rename {
            from: from.to_string(),
            to: to.to_string(),
        });
        self
    }

    /// Change field type
    pub fn change_type(mut self, name: &str, new_type: FieldType) -> Self {
        self.changes.push(FieldChange::ChangeType {
            name: name.to_string(),
            new_type,
            transform: None,
        });
        self
    }

    /// Change field type with transformation
    pub fn change_type_with_transform(
        mut self,
        name: &str,
        new_type: FieldType,
        transform: &str,
    ) -> Self {
        self.changes.push(FieldChange::ChangeType {
            name: name.to_string(),
            new_type,
            transform: Some(transform.to_string()),
        });
        self
    }

    /// Make a field optional
    pub fn make_optional(mut self, name: &str) -> Self {
        self.changes.push(FieldChange::MakeOptional {
            name: name.to_string(),
        });
        self
    }

    /// Make a field required
    pub fn make_required(mut self, name: &str, default: FieldDefault) -> Self {
        self.changes.push(FieldChange::MakeRequired {
            name: name.to_string(),
            default: Some(default),
        });
        self
    }

    /// Set a default value
    pub fn set_default(mut self, name: &str, default: FieldDefault) -> Self {
        self.changes.push(FieldChange::SetDefault {
            name: name.to_string(),
            default: Some(default),
        });
        self
    }

    /// Deprecate a field
    pub fn deprecate_field(mut self, name: &str) -> Self {
        self.changes.push(FieldChange::Deprecate {
            name: name.to_string(),
            message: None,
        });
        self
    }

    /// Deprecate a field with message
    pub fn deprecate_field_with_message(mut self, name: &str, message: &str) -> Self {
        self.changes.push(FieldChange::Deprecate {
            name: name.to_string(),
            message: Some(message.to_string()),
        });
        self
    }

    /// Set description
    pub fn description(mut self, desc: &str) -> Self {
        self.description = Some(desc.to_string());
        self
    }

    /// Build the evolution
    pub fn build(self) -> SchemaEvolution {
        let breaking = self.changes.iter().any(|c| !c.is_backward_compatible());

        SchemaEvolution {
            schema_name: self.schema_name,
            from_version: self.from_version,
            to_version: self.to_version,
            changes: self.changes,
            description: self.description,
            breaking,
        }
    }
}

/// A single field change in a schema evolution
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FieldChange {
    /// Add a new field
    Add {
        name: String,
        field_type: FieldType,
        required: bool,
        default: Option<FieldDefault>,
    },
    /// Remove a field
    Remove { name: String, archive: bool },
    /// Rename a field
    Rename { from: String, to: String },
    /// Change field type
    ChangeType {
        name: String,
        new_type: FieldType,
        transform: Option<String>,
    },
    /// Make required field optional
    MakeOptional { name: String },
    /// Make optional field required
    MakeRequired {
        name: String,
        default: Option<FieldDefault>,
    },
    /// Set or change default value
    SetDefault {
        name: String,
        default: Option<FieldDefault>,
    },
    /// Deprecate a field
    Deprecate {
        name: String,
        message: Option<String>,
    },
}

impl FieldChange {
    /// Check if this change is backward compatible
    pub fn is_backward_compatible(&self) -> bool {
        match self {
            // Adding optional fields is always safe
            FieldChange::Add { required, .. } => !required,
            // Removing fields breaks existing readers
            FieldChange::Remove { .. } => false,
            // Renaming breaks existing code
            FieldChange::Rename { .. } => false,
            // Type changes may break serialization
            FieldChange::ChangeType { .. } => false,
            // Making required -> optional is safe
            FieldChange::MakeOptional { .. } => true,
            // Making optional -> required needs defaults
            FieldChange::MakeRequired { default, .. } => default.is_some(),
            // Setting defaults is safe
            FieldChange::SetDefault { .. } => true,
            // Deprecation is safe (just a warning)
            FieldChange::Deprecate { .. } => true,
        }
    }

    /// Generate migration command for this change
    pub fn to_migration_command(&self) -> String {
        match self {
            FieldChange::Add {
                name,
                field_type,
                required,
                default,
            } => {
                let mut cmd = format!("ADD FIELD {} {}", name, field_type);
                if *required {
                    cmd.push_str(" REQUIRED");
                }
                if let Some(d) = default {
                    cmd.push_str(&format!(" DEFAULT {:?}", d));
                }
                cmd
            }
            FieldChange::Remove { name, archive } => {
                if *archive {
                    format!("REMOVE FIELD {} ARCHIVE", name)
                } else {
                    format!("REMOVE FIELD {}", name)
                }
            }
            FieldChange::Rename { from, to } => {
                format!("RENAME FIELD {} TO {}", from, to)
            }
            FieldChange::ChangeType {
                name,
                new_type,
                transform,
            } => {
                let mut cmd = format!("ALTER FIELD {} TYPE {}", name, new_type);
                if let Some(t) = transform {
                    cmd.push_str(&format!(" USING {}", t));
                }
                cmd
            }
            FieldChange::MakeOptional { name } => {
                format!("ALTER FIELD {} DROP NOT NULL", name)
            }
            FieldChange::MakeRequired { name, default } => {
                let mut cmd = format!("ALTER FIELD {} SET NOT NULL", name);
                if let Some(d) = default {
                    cmd.push_str(&format!(" DEFAULT {:?}", d));
                }
                cmd
            }
            FieldChange::SetDefault { name, default } => match default {
                Some(d) => format!("ALTER FIELD {} SET DEFAULT {:?}", name, d),
                None => format!("ALTER FIELD {} DROP DEFAULT", name),
            },
            FieldChange::Deprecate { name, message } => {
                let mut cmd = format!("DEPRECATE FIELD {}", name);
                if let Some(m) = message {
                    cmd.push_str(&format!(" MESSAGE '{}'", m));
                }
                cmd
            }
        }
    }
}

/// Evolution strategy
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EvolutionStrategy {
    /// Migrate all data immediately
    Immediate,
    /// Migrate data lazily on read
    Lazy,
    /// Migrate in background batches
    Online,
    /// Write to both old and new format
    DualWrite,
}

/// Evolution error
#[derive(Debug)]
pub enum EvolutionError {
    /// Version mismatch
    VersionMismatch { expected: u32, actual: u32 },
    /// Field not found
    FieldNotFound(String),
    /// Field already exists
    FieldAlreadyExists(String),
    /// Required field needs default
    RequiredFieldNeedsDefault(String),
    /// Incompatible type change
    IncompatibleTypeChange {
        field: String,
        from: String,
        to: String,
    },
}

impl std::fmt::Display for EvolutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvolutionError::VersionMismatch { expected, actual } => {
                write!(f, "Version mismatch: expected {}, got {}", expected, actual)
            }
            EvolutionError::FieldNotFound(name) => {
                write!(f, "Field '{}' not found", name)
            }
            EvolutionError::FieldAlreadyExists(name) => {
                write!(f, "Field '{}' already exists", name)
            }
            EvolutionError::RequiredFieldNeedsDefault(name) => {
                write!(f, "Required field '{}' needs a default value", name)
            }
            EvolutionError::IncompatibleTypeChange { field, from, to } => {
                write!(
                    f,
                    "Incompatible type change for '{}': {} -> {}",
                    field, from, to
                )
            }
        }
    }
}

impl std::error::Error for EvolutionError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evolution_add_field() {
        let schema = Schema::builder("user")
            .version(1)
            .field("id", FieldType::String, true)
            .build();

        let evolution = SchemaEvolution::new("user", 1, 2)
            .add_field("email", FieldType::String, None)
            .build();

        assert!(evolution.is_backward_compatible());

        let new_schema = evolution.apply(&schema).unwrap();
        assert_eq!(new_schema.version, 2);
        assert!(new_schema.has_field("email"));
    }

    #[test]
    fn test_evolution_rename_field() {
        let schema = Schema::builder("user")
            .version(1)
            .field("name", FieldType::String, false)
            .build();

        let evolution = SchemaEvolution::new("user", 1, 2)
            .rename_field("name", "display_name")
            .build();

        assert!(!evolution.is_backward_compatible()); // Breaking change

        let new_schema = evolution.apply(&schema).unwrap();
        assert!(!new_schema.has_field("name"));
        assert!(new_schema.has_field("display_name"));
    }

    #[test]
    fn test_evolution_deprecate_field() {
        let schema = Schema::builder("user")
            .version(1)
            .field("age", FieldType::Integer, false)
            .build();

        let evolution = SchemaEvolution::new("user", 1, 2)
            .deprecate_field_with_message("age", "Use birth_date instead")
            .build();

        assert!(evolution.is_backward_compatible());

        let new_schema = evolution.apply(&schema).unwrap();
        let age_field = new_schema.field("age").unwrap();
        assert!(age_field.deprecated);
    }

    #[test]
    fn test_evolution_version_mismatch() {
        let schema = Schema::builder("user").version(2).build();

        let evolution = SchemaEvolution::new("user", 1, 2).build();

        let result = evolution.apply(&schema);
        assert!(matches!(
            result,
            Err(EvolutionError::VersionMismatch { .. })
        ));
    }

    #[test]
    fn test_migration_script_generation() {
        let evolution = SchemaEvolution::new("user", 1, 2)
            .add_field("phone", FieldType::String, None)
            .rename_field("name", "display_name")
            .deprecate_field("age")
            .description("Add phone, rename name, deprecate age")
            .build();

        let script = evolution.generate_migration_script();
        assert!(script.contains("ADD FIELD phone"));
        assert!(script.contains("RENAME FIELD name TO display_name"));
        assert!(script.contains("DEPRECATE FIELD age"));
    }
}
