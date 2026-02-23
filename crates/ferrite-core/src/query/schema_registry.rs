use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors returned by schema registry operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SchemaError {
    /// The requested schema was not found in the registry.
    #[error("schema not found: {0}")]
    SchemaNotFound(String),

    /// The requested schema version does not exist.
    #[error("version not found")]
    VersionNotFound,

    /// A schema with the given name is already registered.
    #[error("schema already exists: {0}")]
    AlreadyExists(String),

    /// The proposed evolution is incompatible with the current compatibility mode.
    #[error("incompatible evolution: {0}")]
    IncompatibleEvolution(String),

    /// A migration is already in progress for this schema.
    #[error("migration in progress")]
    MigrationInProgress,

    /// The data migration failed with the given reason.
    #[error("migration failed: {0}")]
    MigrationFailed(String),

    /// Rolling back to a previous schema version failed.
    #[error("rollback failed: {0}")]
    RollbackFailed(String),

    /// The maximum number of schemas in the registry has been reached.
    #[error("max schemas exceeded")]
    MaxSchemasExceeded,

    /// The maximum number of versions for a single schema has been reached.
    #[error("max versions exceeded")]
    MaxVersionsExceeded,

    /// Schema field validation failed.
    #[error("validation failed: {0}")]
    ValidationFailed(String),

    /// An unexpected internal error occurred.
    #[error("internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Identifiers & small types
// ---------------------------------------------------------------------------

/// Unique identifier for a schema, backed by a UUID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SchemaId(
    /// Inner UUID value.
    pub Uuid,
);

impl Serialize for SchemaId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SchemaId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Uuid::parse_str(&s)
            .map(SchemaId)
            .map_err(serde::de::Error::custom)
    }
}

impl SchemaId {
    /// Creates a new random schema identifier.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for SchemaId {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// CompatMode
// ---------------------------------------------------------------------------

/// Compatibility mode governing schema evolution rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CompatMode {
    /// No compatibility checks are enforced.
    None,
    /// New schemas must be readable by old consumers.
    Backward,
    /// Old data must be readable by new consumers.
    Forward,
    /// Both backward and forward compatibility are required.
    Full,
    /// Backward compatibility checked against all prior versions.
    BackwardTransitive,
    /// Forward compatibility checked against all prior versions.
    ForwardTransitive,
    /// Full compatibility checked against all prior versions.
    FullTransitive,
}

impl Default for CompatMode {
    fn default() -> Self {
        Self::Backward
    }
}

// ---------------------------------------------------------------------------
// SchemaFieldType
// ---------------------------------------------------------------------------

/// Supported data types for schema fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SchemaFieldType {
    /// UTF-8 string value.
    String,
    /// 64-bit signed integer.
    Int64,
    /// 64-bit floating-point number.
    Float64,
    /// Boolean value.
    Bool,
    /// Raw byte sequence.
    Bytes,
    /// Date-time timestamp.
    Timestamp,
    /// Arbitrary JSON value.
    Json,
    /// Ordered list of elements with the given element type.
    Array(Box<SchemaFieldType>),
    /// Key-value map with the given key and value types.
    Map(Box<SchemaFieldType>, Box<SchemaFieldType>),
    /// Fixed-dimension numeric vector for embeddings.
    Vector {
        /// Number of dimensions in the vector.
        dimensions: u32,
    },
    /// Reference to another schema by name.
    Reference {
        /// Name of the referenced schema.
        target_schema: String,
    },
    /// Enumerated type with a fixed set of allowed values.
    Enum {
        /// Allowed variant names.
        variants: Vec<String>,
    },
}

impl SchemaFieldType {
    /// Returns `true` when `self` can be safely widened to `target`.
    fn is_widening_to(&self, target: &SchemaFieldType) -> bool {
        matches!(
            (self, target),
            (SchemaFieldType::Int64, SchemaFieldType::Float64)
                | (SchemaFieldType::String, SchemaFieldType::Json)
        )
    }
}

// ---------------------------------------------------------------------------
// SchemaField
// ---------------------------------------------------------------------------

/// A single field definition within a schema version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaField {
    /// Field name used as the key in serialized data.
    pub name: String,
    /// Data type of this field.
    pub field_type: SchemaFieldType,
    /// Whether this field must be present in every record.
    pub required: bool,
    /// Optional default value serialized as a string.
    pub default_value: Option<String>,
    /// Optional human-readable description of this field.
    pub description: Option<String>,
    /// Whether this field has been marked as deprecated.
    pub deprecated: bool,
    /// Schema version in which this field was introduced.
    pub added_in_version: u32,
    /// Schema version in which this field was removed, if any.
    pub removed_in_version: Option<u32>,
}

// ---------------------------------------------------------------------------
// SchemaVersion
// ---------------------------------------------------------------------------

/// A versioned snapshot of a schema's field definitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersion {
    /// Unique identifier for this schema version.
    pub id: SchemaId,
    /// Name of the schema this version belongs to.
    pub name: String,
    /// Monotonically increasing version number.
    pub version: u32,
    /// Ordered list of fields defined in this version.
    pub fields: Vec<SchemaField>,
    /// Timestamp when this version was created.
    pub created_at: DateTime<Utc>,
    /// Optional description of this schema version.
    pub description: Option<String>,
    /// Arbitrary key-value metadata attached to this version.
    pub metadata: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// SchemaEvolution
// ---------------------------------------------------------------------------

/// Describes changes to apply when evolving a schema to a new version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaEvolution {
    /// New fields to add in this evolution.
    pub add_fields: Vec<SchemaField>,
    /// Names of fields to remove in this evolution.
    pub remove_fields: Vec<String>,
    /// Field renames as (old_name, new_name) pairs.
    pub rename_fields: Vec<(String, String)>,
    /// Field type changes as (field_name, new_type) pairs.
    pub change_types: Vec<(String, SchemaFieldType)>,
    /// Human-readable description of this evolution.
    pub description: String,
}

// ---------------------------------------------------------------------------
// Compatibility
// ---------------------------------------------------------------------------

/// Category of a schema compatibility issue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompatIssueType {
    /// A previously existing field was removed.
    FieldRemoved,
    /// A field's type was changed incompatibly.
    TypeChanged,
    /// A new required field was added without a default value.
    RequiredFieldAdded,
    /// A required field is missing a default value for forward compatibility.
    DefaultMissing,
}

/// A single compatibility issue detected during schema evolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatIssue {
    /// Name of the field that caused the issue.
    pub field: String,
    /// Category of the compatibility issue.
    pub issue_type: CompatIssueType,
    /// Human-readable description of the issue.
    pub message: String,
}

/// Result of a schema compatibility check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatibilityResult {
    /// Whether the schema change is compatible under the current mode.
    pub compatible: bool,
    /// List of compatibility issues found.
    pub issues: Vec<CompatIssue>,
    /// Non-breaking warnings (e.g. safe type widenings).
    pub warnings: Vec<String>,
}

// ---------------------------------------------------------------------------
// Migration
// ---------------------------------------------------------------------------

/// Current progress state of a schema data migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MigrationProgress {
    /// Migration has been created but not yet started.
    Pending,
    /// Migration is actively running.
    InProgress {
        /// Completion percentage (0.0–100.0).
        percent: f64,
    },
    /// Migration finished successfully.
    Completed,
    /// Migration failed with the given error message.
    Failed(String),
    /// Migration was rolled back to the previous version.
    RolledBack,
}

/// Status details for an in-flight or completed migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationStatus {
    /// Name of the schema being migrated.
    pub schema_name: String,
    /// Source schema version number.
    pub from_version: u32,
    /// Target schema version number.
    pub to_version: u32,
    /// Current progress of the migration.
    pub progress: MigrationProgress,
    /// Timestamp when the migration was initiated.
    pub started_at: DateTime<Utc>,
    /// Estimated completion time, if available.
    pub estimated_completion: Option<DateTime<Utc>>,
    /// Number of keys that have been migrated so far.
    pub keys_processed: u64,
    /// Total number of keys to migrate.
    pub keys_total: u64,
    /// Errors encountered during migration.
    pub errors: Vec<String>,
}

// ---------------------------------------------------------------------------
// Info / Stats
// ---------------------------------------------------------------------------

/// Summary information about a registered schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaInfo {
    /// Name of the schema.
    pub name: String,
    /// Latest version number.
    pub current_version: u32,
    /// Total number of versions registered.
    pub total_versions: u32,
    /// Timestamp when the first version was created.
    pub created_at: DateTime<Utc>,
    /// Timestamp of the most recent evolution, if any.
    pub last_evolved: Option<DateTime<Utc>>,
    /// Current migration status, if a migration exists.
    pub migration_status: Option<MigrationProgress>,
}

/// Aggregate statistics for the schema registry.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RegistryStats {
    /// Total number of registered schemas.
    pub total_schemas: u64,
    /// Total number of schema versions across all schemas.
    pub total_versions: u64,
    /// Number of migrations currently in progress.
    pub active_migrations: u64,
    /// Number of migrations that completed successfully.
    pub completed_migrations: u64,
    /// Number of migrations that failed.
    pub failed_migrations: u64,
    /// Number of schema rollbacks performed.
    pub rollbacks: u64,
}

// ---------------------------------------------------------------------------
// RegistryConfig
// ---------------------------------------------------------------------------

/// Configuration options for the schema registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryConfig {
    /// Maximum number of schemas the registry will accept.
    pub max_schemas: usize,
    /// Maximum number of versions allowed per schema.
    pub max_versions_per_schema: usize,
    /// Default compatibility mode for schema evolutions.
    pub compatibility_mode: CompatMode,
    /// Whether to automatically create a migration on evolution.
    pub auto_migrate: bool,
    /// Number of keys to process per migration batch.
    pub migration_batch_size: usize,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            max_schemas: 1024,
            max_versions_per_schema: 256,
            compatibility_mode: CompatMode::Backward,
            auto_migrate: false,
            migration_batch_size: 1000,
        }
    }
}

// ---------------------------------------------------------------------------
// Internal entry stored per schema name
// ---------------------------------------------------------------------------

struct SchemaEntry {
    versions: Vec<SchemaVersion>,
    migration: Option<MigrationStatus>,
}

// ---------------------------------------------------------------------------
// SchemaRegistry
// ---------------------------------------------------------------------------

/// Thread-safe schema registry managing versioned schemas and migrations.
pub struct SchemaRegistry {
    config: RegistryConfig,
    schemas: DashMap<String, RwLock<SchemaEntry>>,
    completed_migrations: AtomicU64,
    failed_migrations: AtomicU64,
    rollback_count: AtomicU64,
}

impl SchemaRegistry {
    /// Creates a new schema registry with the given configuration.
    pub fn new(config: RegistryConfig) -> Self {
        Self {
            config,
            schemas: DashMap::new(),
            completed_migrations: AtomicU64::new(0),
            failed_migrations: AtomicU64::new(0),
            rollback_count: AtomicU64::new(0),
        }
    }

    // -- register --------------------------------------------------------

    /// Registers a new schema, returning its identifier.
    pub fn register_schema(&self, schema: SchemaVersion) -> Result<SchemaId, SchemaError> {
        if self.schemas.len() >= self.config.max_schemas {
            return Err(SchemaError::MaxSchemasExceeded);
        }

        Self::validate_fields(&schema.fields)?;

        let name = schema.name.clone();
        let id = schema.id;

        if self.schemas.contains_key(&name) {
            return Err(SchemaError::AlreadyExists(name));
        }

        let entry = SchemaEntry {
            versions: vec![schema],
            migration: None,
        };
        self.schemas.insert(name, RwLock::new(entry));
        Ok(id)
    }

    // -- get / query -----------------------------------------------------

    /// Returns the latest version of the named schema, if it exists.
    pub fn get_schema(&self, name: &str) -> Option<SchemaVersion> {
        let ref_entry = self.schemas.get(name)?;
        let entry = ref_entry.read();
        entry.versions.last().cloned()
    }

    /// Returns a specific version of the named schema, if it exists.
    pub fn get_version(&self, name: &str, version: u32) -> Option<SchemaVersion> {
        let ref_entry = self.schemas.get(name)?;
        let entry = ref_entry.read();
        entry
            .versions
            .iter()
            .find(|v| v.version == version)
            .cloned()
    }

    /// Returns all versions of the named schema in chronological order.
    pub fn get_history(&self, name: &str) -> Vec<SchemaVersion> {
        self.schemas
            .get(name)
            .map(|r| r.read().versions.clone())
            .unwrap_or_default()
    }

    /// Lists summary information for all registered schemas.
    pub fn list_schemas(&self) -> Vec<SchemaInfo> {
        self.schemas
            .iter()
            .map(|r| {
                let entry = r.value().read();
                let first = entry.versions.first().unwrap();
                let last = entry.versions.last().unwrap();
                let last_evolved = if entry.versions.len() > 1 {
                    Some(last.created_at)
                } else {
                    None
                };
                SchemaInfo {
                    name: r.key().clone(),
                    current_version: last.version,
                    total_versions: entry.versions.len() as u32,
                    created_at: first.created_at,
                    last_evolved,
                    migration_status: entry.migration.as_ref().map(|m| m.progress.clone()),
                }
            })
            .collect()
    }

    /// Returns the current migration status for the named schema, if any.
    pub fn migrate_status(&self, name: &str) -> Option<MigrationStatus> {
        let ref_entry = self.schemas.get(name)?;
        let entry = ref_entry.read();
        entry.migration.clone()
    }

    /// Returns aggregate statistics for the registry.
    pub fn get_stats(&self) -> RegistryStats {
        let total_schemas = self.schemas.len() as u64;
        let total_versions: u64 = self
            .schemas
            .iter()
            .map(|r| r.value().read().versions.len() as u64)
            .sum();

        let active_migrations = self
            .schemas
            .iter()
            .filter(|r| {
                let entry = r.value().read();
                matches!(
                    entry.migration.as_ref().map(|m| &m.progress),
                    Some(MigrationProgress::Pending) | Some(MigrationProgress::InProgress { .. })
                )
            })
            .count() as u64;

        RegistryStats {
            total_schemas,
            total_versions,
            active_migrations,
            completed_migrations: self.completed_migrations.load(Ordering::Relaxed),
            failed_migrations: self.failed_migrations.load(Ordering::Relaxed),
            rollbacks: self.rollback_count.load(Ordering::Relaxed),
        }
    }

    // -- evolve ----------------------------------------------------------

    /// Evolves the named schema by applying the given changes.
    pub fn evolve(
        &self,
        name: &str,
        evolution: SchemaEvolution,
    ) -> Result<SchemaVersion, SchemaError> {
        let ref_entry = self
            .schemas
            .get(name)
            .ok_or_else(|| SchemaError::SchemaNotFound(name.to_string()))?;

        let mut entry = ref_entry.write();

        // Block evolution while a migration is active.
        if let Some(m) = &entry.migration {
            if matches!(
                m.progress,
                MigrationProgress::Pending | MigrationProgress::InProgress { .. }
            ) {
                return Err(SchemaError::MigrationInProgress);
            }
        }

        if entry.versions.len() >= self.config.max_versions_per_schema {
            return Err(SchemaError::MaxVersionsExceeded);
        }

        let current = entry.versions.last().unwrap().clone();
        let new_version = current.version + 1;

        // Build evolved field set.
        let mut fields: Vec<SchemaField> = current.fields.clone();

        // Renames first – keeps indices stable.
        for (old_name, new_name) in &evolution.rename_fields {
            if let Some(f) = fields.iter_mut().find(|f| &f.name == old_name) {
                f.name = new_name.clone();
            }
        }

        // Type changes.
        for (field_name, new_type) in &evolution.change_types {
            if let Some(f) = fields.iter_mut().find(|f| &f.name == field_name) {
                f.field_type = new_type.clone();
            }
        }

        // Removals.
        for field_name in &evolution.remove_fields {
            if let Some(f) = fields.iter_mut().find(|f| &f.name == field_name) {
                f.removed_in_version = Some(new_version);
                f.deprecated = true;
            }
        }

        // Additions.
        for field in &evolution.add_fields {
            let mut f = field.clone();
            f.added_in_version = new_version;
            fields.push(f);
        }

        let new_schema = SchemaVersion {
            id: SchemaId::new(),
            name: name.to_string(),
            version: new_version,
            fields,
            created_at: Utc::now(),
            description: Some(evolution.description.clone()),
            metadata: HashMap::new(),
        };

        // Compatibility check.
        let compat = Self::check_compat(&current, &new_schema, self.config.compatibility_mode);
        if !compat.compatible {
            let msgs: Vec<String> = compat.issues.iter().map(|i| i.message.clone()).collect();
            return Err(SchemaError::IncompatibleEvolution(msgs.join("; ")));
        }

        // Migration status.
        if self.config.auto_migrate {
            entry.migration = Some(MigrationStatus {
                schema_name: name.to_string(),
                from_version: current.version,
                to_version: new_version,
                progress: MigrationProgress::Pending,
                started_at: Utc::now(),
                estimated_completion: None,
                keys_processed: 0,
                keys_total: 0,
                errors: Vec::new(),
            });
        }

        entry.versions.push(new_schema.clone());
        Ok(new_schema)
    }

    // -- compatibility ---------------------------------------------------

    /// Checks whether a proposed schema is compatible with the current version.
    pub fn check_compatibility(
        &self,
        name: &str,
        new_schema: &SchemaVersion,
    ) -> CompatibilityResult {
        let current = match self.get_schema(name) {
            Some(s) => s,
            None => {
                return CompatibilityResult {
                    compatible: true,
                    issues: Vec::new(),
                    warnings: Vec::new(),
                }
            }
        };
        Self::check_compat(&current, new_schema, self.config.compatibility_mode)
    }

    fn check_compat(
        current: &SchemaVersion,
        new_schema: &SchemaVersion,
        mode: CompatMode,
    ) -> CompatibilityResult {
        if mode == CompatMode::None {
            return CompatibilityResult {
                compatible: true,
                issues: Vec::new(),
                warnings: Vec::new(),
            };
        }

        let mut issues: Vec<CompatIssue> = Vec::new();
        let mut warnings: Vec<String> = Vec::new();

        let check_backward = matches!(
            mode,
            CompatMode::Backward
                | CompatMode::BackwardTransitive
                | CompatMode::Full
                | CompatMode::FullTransitive
        );

        let check_forward = matches!(
            mode,
            CompatMode::Forward
                | CompatMode::ForwardTransitive
                | CompatMode::Full
                | CompatMode::FullTransitive
        );

        // Backward: readers using the OLD schema must still be able to read
        // data written with the NEW schema.
        // → removing a field or changing its type is breaking.
        // → adding a required field without a default is breaking.
        if check_backward {
            // Removed fields.
            for old_field in &current.fields {
                if old_field.removed_in_version.is_some() {
                    continue; // already removed in a prior version
                }
                let exists_in_new = new_schema
                    .fields
                    .iter()
                    .any(|f| f.name == old_field.name && f.removed_in_version.is_none());
                if !exists_in_new {
                    issues.push(CompatIssue {
                        field: old_field.name.clone(),
                        issue_type: CompatIssueType::FieldRemoved,
                        message: format!("field '{}' was removed", old_field.name),
                    });
                }
            }

            // Type changes.
            for old_field in &current.fields {
                if old_field.removed_in_version.is_some() {
                    continue;
                }
                if let Some(new_field) = new_schema
                    .fields
                    .iter()
                    .find(|f| f.name == old_field.name && f.removed_in_version.is_none())
                {
                    if old_field.field_type != new_field.field_type
                        && !old_field.field_type.is_widening_to(&new_field.field_type)
                    {
                        issues.push(CompatIssue {
                            field: old_field.name.clone(),
                            issue_type: CompatIssueType::TypeChanged,
                            message: format!(
                                "field '{}' type changed incompatibly",
                                old_field.name
                            ),
                        });
                    }

                    if old_field.field_type != new_field.field_type
                        && old_field.field_type.is_widening_to(&new_field.field_type)
                    {
                        warnings.push(format!(
                            "field '{}' type widened (safe but noted)",
                            old_field.name
                        ));
                    }
                }
            }

            // New required fields without default.
            for new_field in &new_schema.fields {
                if new_field.removed_in_version.is_some() {
                    continue;
                }
                let is_new = !current
                    .fields
                    .iter()
                    .any(|f| f.name == new_field.name && f.removed_in_version.is_none());
                if is_new && new_field.required && new_field.default_value.is_none() {
                    issues.push(CompatIssue {
                        field: new_field.name.clone(),
                        issue_type: CompatIssueType::RequiredFieldAdded,
                        message: format!(
                            "required field '{}' added without default",
                            new_field.name
                        ),
                    });
                }
            }
        }

        // Forward: readers using the NEW schema must be able to read data
        // written with the OLD schema.
        // → adding a required field without a default is breaking (old data
        //   won't have it).
        if check_forward {
            for new_field in &new_schema.fields {
                if new_field.removed_in_version.is_some() {
                    continue;
                }
                let is_new = !current
                    .fields
                    .iter()
                    .any(|f| f.name == new_field.name && f.removed_in_version.is_none());
                if is_new && new_field.required && new_field.default_value.is_none() {
                    let already = issues.iter().any(|i| {
                        i.field == new_field.name
                            && i.issue_type == CompatIssueType::RequiredFieldAdded
                    });
                    if !already {
                        issues.push(CompatIssue {
                            field: new_field.name.clone(),
                            issue_type: CompatIssueType::DefaultMissing,
                            message: format!(
                                "new required field '{}' has no default (forward compat)",
                                new_field.name
                            ),
                        });
                    }
                }
            }
        }

        CompatibilityResult {
            compatible: issues.is_empty(),
            issues,
            warnings,
        }
    }

    // -- rollback --------------------------------------------------------

    /// Rolls back the named schema to its previous version.
    pub fn rollback(&self, name: &str) -> Result<SchemaVersion, SchemaError> {
        let ref_entry = self
            .schemas
            .get(name)
            .ok_or_else(|| SchemaError::SchemaNotFound(name.to_string()))?;

        let mut entry = ref_entry.write();

        if entry.versions.len() < 2 {
            return Err(SchemaError::RollbackFailed(
                "no previous version to roll back to".into(),
            ));
        }

        entry.versions.pop();

        if let Some(m) = &mut entry.migration {
            m.progress = MigrationProgress::RolledBack;
        }

        self.rollback_count.fetch_add(1, Ordering::Relaxed);

        let current = entry.versions.last().unwrap().clone();
        Ok(current)
    }

    // -- helpers ---------------------------------------------------------

    fn validate_fields(fields: &[SchemaField]) -> Result<(), SchemaError> {
        let mut seen = std::collections::HashSet::new();
        for f in fields {
            if f.name.is_empty() {
                return Err(SchemaError::ValidationFailed(
                    "field name cannot be empty".into(),
                ));
            }
            if !seen.insert(&f.name) {
                return Err(SchemaError::ValidationFailed(format!(
                    "duplicate field name '{}'",
                    f.name
                )));
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> RegistryConfig {
        RegistryConfig::default()
    }

    fn make_field(name: &str, ft: SchemaFieldType, required: bool) -> SchemaField {
        SchemaField {
            name: name.to_string(),
            field_type: ft,
            required,
            default_value: None,
            description: None,
            deprecated: false,
            added_in_version: 1,
            removed_in_version: None,
        }
    }

    fn make_schema(name: &str, version: u32, fields: Vec<SchemaField>) -> SchemaVersion {
        SchemaVersion {
            id: SchemaId::new(),
            name: name.to_string(),
            version,
            fields,
            created_at: Utc::now(),
            description: None,
            metadata: HashMap::new(),
        }
    }

    // -- registration ----------------------------------------------------

    #[test]
    fn test_register_and_get_schema() {
        let reg = SchemaRegistry::new(default_config());
        let schema = make_schema(
            "user",
            1,
            vec![
                make_field("id", SchemaFieldType::Int64, true),
                make_field("name", SchemaFieldType::String, true),
            ],
        );

        let id = reg.register_schema(schema.clone()).unwrap();
        assert_eq!(id, schema.id);

        let fetched = reg.get_schema("user").unwrap();
        assert_eq!(fetched.version, 1);
        assert_eq!(fetched.fields.len(), 2);
    }

    #[test]
    fn test_register_duplicate_fails() {
        let reg = SchemaRegistry::new(default_config());
        let s1 = make_schema(
            "user",
            1,
            vec![make_field("id", SchemaFieldType::Int64, true)],
        );
        reg.register_schema(s1).unwrap();

        let s2 = make_schema(
            "user",
            1,
            vec![make_field("id", SchemaFieldType::Int64, true)],
        );
        let err = reg.register_schema(s2).unwrap_err();
        assert!(matches!(err, SchemaError::AlreadyExists(_)));
    }

    #[test]
    fn test_register_max_schemas_exceeded() {
        let config = RegistryConfig {
            max_schemas: 1,
            ..default_config()
        };
        let reg = SchemaRegistry::new(config);
        reg.register_schema(make_schema(
            "a",
            1,
            vec![make_field("id", SchemaFieldType::Int64, true)],
        ))
        .unwrap();

        let err = reg
            .register_schema(make_schema(
                "b",
                1,
                vec![make_field("id", SchemaFieldType::Int64, true)],
            ))
            .unwrap_err();
        assert!(matches!(err, SchemaError::MaxSchemasExceeded));
    }

    #[test]
    fn test_register_duplicate_field_names_fails() {
        let reg = SchemaRegistry::new(default_config());
        let schema = make_schema(
            "bad",
            1,
            vec![
                make_field("id", SchemaFieldType::Int64, true),
                make_field("id", SchemaFieldType::String, false),
            ],
        );
        let err = reg.register_schema(schema).unwrap_err();
        assert!(matches!(err, SchemaError::ValidationFailed(_)));
    }

    // -- evolution -------------------------------------------------------

    #[test]
    fn test_evolve_add_optional_field() {
        let reg = SchemaRegistry::new(default_config());
        reg.register_schema(make_schema(
            "user",
            1,
            vec![make_field("id", SchemaFieldType::Int64, true)],
        ))
        .unwrap();

        let evo = SchemaEvolution {
            add_fields: vec![make_field("email", SchemaFieldType::String, false)],
            remove_fields: Vec::new(),
            rename_fields: Vec::new(),
            change_types: Vec::new(),
            description: "add email".into(),
        };

        let v2 = reg.evolve("user", evo).unwrap();
        assert_eq!(v2.version, 2);
        assert_eq!(v2.fields.len(), 2);
    }

    #[test]
    fn test_evolve_backward_incompat_required_field_no_default() {
        let config = RegistryConfig {
            compatibility_mode: CompatMode::Backward,
            ..default_config()
        };
        let reg = SchemaRegistry::new(config);
        reg.register_schema(make_schema(
            "user",
            1,
            vec![make_field("id", SchemaFieldType::Int64, true)],
        ))
        .unwrap();

        let evo = SchemaEvolution {
            add_fields: vec![make_field("email", SchemaFieldType::String, true)],
            remove_fields: Vec::new(),
            rename_fields: Vec::new(),
            change_types: Vec::new(),
            description: "add required email without default".into(),
        };

        let err = reg.evolve("user", evo).unwrap_err();
        assert!(matches!(err, SchemaError::IncompatibleEvolution(_)));
    }

    #[test]
    fn test_evolve_backward_compat_required_field_with_default() {
        let config = RegistryConfig {
            compatibility_mode: CompatMode::Backward,
            ..default_config()
        };
        let reg = SchemaRegistry::new(config);
        reg.register_schema(make_schema(
            "user",
            1,
            vec![make_field("id", SchemaFieldType::Int64, true)],
        ))
        .unwrap();

        let mut field = make_field("email", SchemaFieldType::String, true);
        field.default_value = Some("unknown@example.com".into());

        let evo = SchemaEvolution {
            add_fields: vec![field],
            remove_fields: Vec::new(),
            rename_fields: Vec::new(),
            change_types: Vec::new(),
            description: "add required email with default".into(),
        };

        let v2 = reg.evolve("user", evo).unwrap();
        assert_eq!(v2.version, 2);
    }

    #[test]
    fn test_evolve_forward_compat_mode() {
        let config = RegistryConfig {
            compatibility_mode: CompatMode::Forward,
            ..default_config()
        };
        let reg = SchemaRegistry::new(config);
        reg.register_schema(make_schema(
            "user",
            1,
            vec![make_field("id", SchemaFieldType::Int64, true)],
        ))
        .unwrap();

        // Adding a required field without default is also forward-incompatible.
        let evo = SchemaEvolution {
            add_fields: vec![make_field("email", SchemaFieldType::String, true)],
            remove_fields: Vec::new(),
            rename_fields: Vec::new(),
            change_types: Vec::new(),
            description: "add required email".into(),
        };

        let err = reg.evolve("user", evo).unwrap_err();
        assert!(matches!(err, SchemaError::IncompatibleEvolution(_)));
    }

    #[test]
    fn test_evolve_remove_field_backward_incompat() {
        let config = RegistryConfig {
            compatibility_mode: CompatMode::Backward,
            ..default_config()
        };
        let reg = SchemaRegistry::new(config);
        reg.register_schema(make_schema(
            "user",
            1,
            vec![
                make_field("id", SchemaFieldType::Int64, true),
                make_field("name", SchemaFieldType::String, true),
            ],
        ))
        .unwrap();

        let evo = SchemaEvolution {
            add_fields: Vec::new(),
            remove_fields: vec!["name".into()],
            rename_fields: Vec::new(),
            change_types: Vec::new(),
            description: "remove name".into(),
        };

        let err = reg.evolve("user", evo).unwrap_err();
        assert!(matches!(err, SchemaError::IncompatibleEvolution(_)));
    }

    #[test]
    fn test_evolve_compat_mode_none_allows_anything() {
        let config = RegistryConfig {
            compatibility_mode: CompatMode::None,
            ..default_config()
        };
        let reg = SchemaRegistry::new(config);
        reg.register_schema(make_schema(
            "user",
            1,
            vec![
                make_field("id", SchemaFieldType::Int64, true),
                make_field("name", SchemaFieldType::String, true),
            ],
        ))
        .unwrap();

        let evo = SchemaEvolution {
            add_fields: Vec::new(),
            remove_fields: vec!["name".into()],
            rename_fields: Vec::new(),
            change_types: Vec::new(),
            description: "remove name (no compat mode)".into(),
        };

        let v2 = reg.evolve("user", evo).unwrap();
        assert_eq!(v2.version, 2);
    }

    #[test]
    fn test_evolve_type_change_incompatible() {
        let config = RegistryConfig {
            compatibility_mode: CompatMode::Backward,
            ..default_config()
        };
        let reg = SchemaRegistry::new(config);
        reg.register_schema(make_schema(
            "user",
            1,
            vec![make_field("age", SchemaFieldType::Int64, true)],
        ))
        .unwrap();

        let evo = SchemaEvolution {
            add_fields: Vec::new(),
            remove_fields: Vec::new(),
            rename_fields: Vec::new(),
            change_types: vec![("age".into(), SchemaFieldType::String)],
            description: "change age to string".into(),
        };

        let err = reg.evolve("user", evo).unwrap_err();
        assert!(matches!(err, SchemaError::IncompatibleEvolution(_)));
    }

    #[test]
    fn test_evolve_type_widening_int64_to_float64() {
        let config = RegistryConfig {
            compatibility_mode: CompatMode::Backward,
            ..default_config()
        };
        let reg = SchemaRegistry::new(config);
        reg.register_schema(make_schema(
            "metrics",
            1,
            vec![make_field("value", SchemaFieldType::Int64, true)],
        ))
        .unwrap();

        let evo = SchemaEvolution {
            add_fields: Vec::new(),
            remove_fields: Vec::new(),
            rename_fields: Vec::new(),
            change_types: vec![("value".into(), SchemaFieldType::Float64)],
            description: "widen value to float64".into(),
        };

        let v2 = reg.evolve("metrics", evo).unwrap();
        assert_eq!(v2.version, 2);
        let f = v2.fields.iter().find(|f| f.name == "value").unwrap();
        assert_eq!(f.field_type, SchemaFieldType::Float64);
    }

    #[test]
    fn test_evolve_max_versions_exceeded() {
        let config = RegistryConfig {
            max_versions_per_schema: 2,
            compatibility_mode: CompatMode::None,
            ..default_config()
        };
        let reg = SchemaRegistry::new(config);
        reg.register_schema(make_schema(
            "user",
            1,
            vec![make_field("id", SchemaFieldType::Int64, true)],
        ))
        .unwrap();

        // v1 → v2 ok
        reg.evolve(
            "user",
            SchemaEvolution {
                add_fields: vec![make_field("a", SchemaFieldType::String, false)],
                remove_fields: Vec::new(),
                rename_fields: Vec::new(),
                change_types: Vec::new(),
                description: "v2".into(),
            },
        )
        .unwrap();

        // v2 → v3 should fail
        let err = reg
            .evolve(
                "user",
                SchemaEvolution {
                    add_fields: vec![make_field("b", SchemaFieldType::String, false)],
                    remove_fields: Vec::new(),
                    rename_fields: Vec::new(),
                    change_types: Vec::new(),
                    description: "v3".into(),
                },
            )
            .unwrap_err();
        assert!(matches!(err, SchemaError::MaxVersionsExceeded));
    }

    #[test]
    fn test_evolve_nonexistent_schema() {
        let reg = SchemaRegistry::new(default_config());
        let evo = SchemaEvolution {
            add_fields: Vec::new(),
            remove_fields: Vec::new(),
            rename_fields: Vec::new(),
            change_types: Vec::new(),
            description: "noop".into(),
        };
        let err = reg.evolve("nope", evo).unwrap_err();
        assert!(matches!(err, SchemaError::SchemaNotFound(_)));
    }

    // -- compatibility check ---------------------------------------------

    #[test]
    fn test_check_compatibility_no_existing_schema() {
        let reg = SchemaRegistry::new(default_config());
        let schema = make_schema(
            "new",
            1,
            vec![make_field("id", SchemaFieldType::Int64, true)],
        );
        let result = reg.check_compatibility("new", &schema);
        assert!(result.compatible);
    }

    #[test]
    fn test_check_compatibility_full_mode() {
        let config = RegistryConfig {
            compatibility_mode: CompatMode::Full,
            ..default_config()
        };
        let reg = SchemaRegistry::new(config);
        reg.register_schema(make_schema(
            "user",
            1,
            vec![make_field("id", SchemaFieldType::Int64, true)],
        ))
        .unwrap();

        // Adding required field without default is incompatible in Full mode.
        let candidate = make_schema(
            "user",
            2,
            vec![
                make_field("id", SchemaFieldType::Int64, true),
                make_field("email", SchemaFieldType::String, true),
            ],
        );
        let result = reg.check_compatibility("user", &candidate);
        assert!(!result.compatible);
        assert!(!result.issues.is_empty());
    }

    // -- migration status ------------------------------------------------

    #[test]
    fn test_migration_status_auto_migrate() {
        let config = RegistryConfig {
            auto_migrate: true,
            compatibility_mode: CompatMode::None,
            ..default_config()
        };
        let reg = SchemaRegistry::new(config);
        reg.register_schema(make_schema(
            "user",
            1,
            vec![make_field("id", SchemaFieldType::Int64, true)],
        ))
        .unwrap();

        reg.evolve(
            "user",
            SchemaEvolution {
                add_fields: vec![make_field("name", SchemaFieldType::String, false)],
                remove_fields: Vec::new(),
                rename_fields: Vec::new(),
                change_types: Vec::new(),
                description: "add name".into(),
            },
        )
        .unwrap();

        let status = reg.migrate_status("user").unwrap();
        assert_eq!(status.from_version, 1);
        assert_eq!(status.to_version, 2);
        assert!(matches!(status.progress, MigrationProgress::Pending));
    }

    #[test]
    fn test_migration_status_none_without_auto() {
        let reg = SchemaRegistry::new(default_config());
        assert!(reg.migrate_status("user").is_none());

        reg.register_schema(make_schema(
            "user",
            1,
            vec![make_field("id", SchemaFieldType::Int64, true)],
        ))
        .unwrap();
        assert!(reg.migrate_status("user").is_none());
    }

    // -- rollback --------------------------------------------------------

    #[test]
    fn test_rollback_to_previous_version() {
        let config = RegistryConfig {
            compatibility_mode: CompatMode::None,
            ..default_config()
        };
        let reg = SchemaRegistry::new(config);
        reg.register_schema(make_schema(
            "user",
            1,
            vec![make_field("id", SchemaFieldType::Int64, true)],
        ))
        .unwrap();

        reg.evolve(
            "user",
            SchemaEvolution {
                add_fields: vec![make_field("name", SchemaFieldType::String, false)],
                remove_fields: Vec::new(),
                rename_fields: Vec::new(),
                change_types: Vec::new(),
                description: "add name".into(),
            },
        )
        .unwrap();

        let rolled = reg.rollback("user").unwrap();
        assert_eq!(rolled.version, 1);
        assert_eq!(rolled.fields.len(), 1);
    }

    #[test]
    fn test_rollback_no_previous_version_fails() {
        let reg = SchemaRegistry::new(default_config());
        reg.register_schema(make_schema(
            "user",
            1,
            vec![make_field("id", SchemaFieldType::Int64, true)],
        ))
        .unwrap();

        let err = reg.rollback("user").unwrap_err();
        assert!(matches!(err, SchemaError::RollbackFailed(_)));
    }

    #[test]
    fn test_rollback_nonexistent_schema() {
        let reg = SchemaRegistry::new(default_config());
        let err = reg.rollback("nope").unwrap_err();
        assert!(matches!(err, SchemaError::SchemaNotFound(_)));
    }

    #[test]
    fn test_rollback_increments_stats() {
        let config = RegistryConfig {
            compatibility_mode: CompatMode::None,
            ..default_config()
        };
        let reg = SchemaRegistry::new(config);
        reg.register_schema(make_schema(
            "user",
            1,
            vec![make_field("id", SchemaFieldType::Int64, true)],
        ))
        .unwrap();
        reg.evolve(
            "user",
            SchemaEvolution {
                add_fields: vec![make_field("x", SchemaFieldType::String, false)],
                remove_fields: Vec::new(),
                rename_fields: Vec::new(),
                change_types: Vec::new(),
                description: "v2".into(),
            },
        )
        .unwrap();

        reg.rollback("user").unwrap();
        assert_eq!(reg.get_stats().rollbacks, 1);
    }

    // -- history ---------------------------------------------------------

    #[test]
    fn test_get_history() {
        let config = RegistryConfig {
            compatibility_mode: CompatMode::None,
            ..default_config()
        };
        let reg = SchemaRegistry::new(config);
        reg.register_schema(make_schema(
            "user",
            1,
            vec![make_field("id", SchemaFieldType::Int64, true)],
        ))
        .unwrap();

        reg.evolve(
            "user",
            SchemaEvolution {
                add_fields: vec![make_field("a", SchemaFieldType::String, false)],
                remove_fields: Vec::new(),
                rename_fields: Vec::new(),
                change_types: Vec::new(),
                description: "v2".into(),
            },
        )
        .unwrap();

        reg.evolve(
            "user",
            SchemaEvolution {
                add_fields: vec![make_field("b", SchemaFieldType::String, false)],
                remove_fields: Vec::new(),
                rename_fields: Vec::new(),
                change_types: Vec::new(),
                description: "v3".into(),
            },
        )
        .unwrap();

        let history = reg.get_history("user");
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].version, 1);
        assert_eq!(history[1].version, 2);
        assert_eq!(history[2].version, 3);
    }

    #[test]
    fn test_get_history_nonexistent() {
        let reg = SchemaRegistry::new(default_config());
        assert!(reg.get_history("nope").is_empty());
    }

    // -- field deprecation -----------------------------------------------

    #[test]
    fn test_field_deprecation_on_removal() {
        let config = RegistryConfig {
            compatibility_mode: CompatMode::None,
            ..default_config()
        };
        let reg = SchemaRegistry::new(config);
        reg.register_schema(make_schema(
            "user",
            1,
            vec![
                make_field("id", SchemaFieldType::Int64, true),
                make_field("legacy", SchemaFieldType::String, false),
            ],
        ))
        .unwrap();

        reg.evolve(
            "user",
            SchemaEvolution {
                add_fields: Vec::new(),
                remove_fields: vec!["legacy".into()],
                rename_fields: Vec::new(),
                change_types: Vec::new(),
                description: "deprecate legacy".into(),
            },
        )
        .unwrap();

        let v2 = reg.get_schema("user").unwrap();
        let legacy = v2.fields.iter().find(|f| f.name == "legacy").unwrap();
        assert!(legacy.deprecated);
        assert_eq!(legacy.removed_in_version, Some(2));
    }

    // -- get_version -----------------------------------------------------

    #[test]
    fn test_get_specific_version() {
        let config = RegistryConfig {
            compatibility_mode: CompatMode::None,
            ..default_config()
        };
        let reg = SchemaRegistry::new(config);
        reg.register_schema(make_schema(
            "user",
            1,
            vec![make_field("id", SchemaFieldType::Int64, true)],
        ))
        .unwrap();
        reg.evolve(
            "user",
            SchemaEvolution {
                add_fields: vec![make_field("name", SchemaFieldType::String, false)],
                remove_fields: Vec::new(),
                rename_fields: Vec::new(),
                change_types: Vec::new(),
                description: "v2".into(),
            },
        )
        .unwrap();

        let v1 = reg.get_version("user", 1).unwrap();
        assert_eq!(v1.fields.len(), 1);

        let v2 = reg.get_version("user", 2).unwrap();
        assert_eq!(v2.fields.len(), 2);

        assert!(reg.get_version("user", 99).is_none());
    }

    // -- list_schemas / stats --------------------------------------------

    #[test]
    fn test_list_schemas_and_stats() {
        let config = RegistryConfig {
            compatibility_mode: CompatMode::None,
            ..default_config()
        };
        let reg = SchemaRegistry::new(config);
        reg.register_schema(make_schema(
            "user",
            1,
            vec![make_field("id", SchemaFieldType::Int64, true)],
        ))
        .unwrap();
        reg.register_schema(make_schema(
            "order",
            1,
            vec![make_field("id", SchemaFieldType::Int64, true)],
        ))
        .unwrap();

        reg.evolve(
            "user",
            SchemaEvolution {
                add_fields: vec![make_field("email", SchemaFieldType::String, false)],
                remove_fields: Vec::new(),
                rename_fields: Vec::new(),
                change_types: Vec::new(),
                description: "v2".into(),
            },
        )
        .unwrap();

        let list = reg.list_schemas();
        assert_eq!(list.len(), 2);

        let stats = reg.get_stats();
        assert_eq!(stats.total_schemas, 2);
        assert_eq!(stats.total_versions, 3); // user v1+v2, order v1
    }

    // -- rename ----------------------------------------------------------

    #[test]
    fn test_evolve_rename_field() {
        let config = RegistryConfig {
            compatibility_mode: CompatMode::None,
            ..default_config()
        };
        let reg = SchemaRegistry::new(config);
        reg.register_schema(make_schema(
            "user",
            1,
            vec![
                make_field("id", SchemaFieldType::Int64, true),
                make_field("name", SchemaFieldType::String, true),
            ],
        ))
        .unwrap();

        reg.evolve(
            "user",
            SchemaEvolution {
                add_fields: Vec::new(),
                remove_fields: Vec::new(),
                rename_fields: vec![("name".into(), "full_name".into())],
                change_types: Vec::new(),
                description: "rename name to full_name".into(),
            },
        )
        .unwrap();

        let v2 = reg.get_schema("user").unwrap();
        assert!(v2.fields.iter().any(|f| f.name == "full_name"));
        assert!(!v2.fields.iter().any(|f| f.name == "name"));
    }

    // -- complex field types ---------------------------------------------

    #[test]
    fn test_complex_field_types() {
        let reg = SchemaRegistry::new(default_config());
        let schema = make_schema(
            "document",
            1,
            vec![
                make_field(
                    "tags",
                    SchemaFieldType::Array(Box::new(SchemaFieldType::String)),
                    false,
                ),
                make_field(
                    "attrs",
                    SchemaFieldType::Map(
                        Box::new(SchemaFieldType::String),
                        Box::new(SchemaFieldType::Json),
                    ),
                    false,
                ),
                make_field(
                    "embedding",
                    SchemaFieldType::Vector { dimensions: 128 },
                    false,
                ),
                make_field(
                    "owner",
                    SchemaFieldType::Reference {
                        target_schema: "user".into(),
                    },
                    false,
                ),
                make_field(
                    "status",
                    SchemaFieldType::Enum {
                        variants: vec!["draft".into(), "published".into()],
                    },
                    true,
                ),
            ],
        );

        let id = reg.register_schema(schema).unwrap();
        let fetched = reg.get_schema("document").unwrap();
        assert_eq!(fetched.id, id);
        assert_eq!(fetched.fields.len(), 5);
    }

    // -- string → json widening ------------------------------------------

    #[test]
    fn test_type_widening_string_to_json() {
        let config = RegistryConfig {
            compatibility_mode: CompatMode::Backward,
            ..default_config()
        };
        let reg = SchemaRegistry::new(config);
        reg.register_schema(make_schema(
            "config",
            1,
            vec![make_field("payload", SchemaFieldType::String, true)],
        ))
        .unwrap();

        let evo = SchemaEvolution {
            add_fields: Vec::new(),
            remove_fields: Vec::new(),
            rename_fields: Vec::new(),
            change_types: vec![("payload".into(), SchemaFieldType::Json)],
            description: "widen payload to json".into(),
        };

        let v2 = reg.evolve("config", evo).unwrap();
        let f = v2.fields.iter().find(|f| f.name == "payload").unwrap();
        assert_eq!(f.field_type, SchemaFieldType::Json);
    }
}
