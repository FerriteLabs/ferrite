//! Schema migration for evolving data between schema versions

use super::{EvolutionStrategy, FieldChange, SchemaEvolution};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime};

/// Schema migrator for applying schema evolutions to data
pub struct SchemaMigrator {
    /// Batch size for migrations
    batch_size: usize,
    /// Parallelism level
    parallelism: usize,
    /// Active migrations
    active_migrations: dashmap::DashMap<String, Arc<MigrationState>>,
}

impl SchemaMigrator {
    /// Create a new schema migrator
    pub fn new(batch_size: usize, parallelism: usize) -> Self {
        Self {
            batch_size,
            parallelism,
            active_migrations: dashmap::DashMap::new(),
        }
    }

    /// Create with default settings
    pub fn with_defaults() -> Self {
        Self::new(1000, 4)
    }

    /// Create a migration plan
    pub fn create_plan(
        &self,
        evolution: &SchemaEvolution,
        key_pattern: &str,
        estimated_keys: u64,
    ) -> MigrationPlan {
        let total_batches = (estimated_keys as usize).div_ceil(self.batch_size);

        MigrationPlan {
            id: generate_migration_id(),
            schema_name: evolution.schema_name.clone(),
            from_version: evolution.from_version,
            to_version: evolution.to_version,
            key_pattern: key_pattern.to_string(),
            strategy: EvolutionStrategy::Online,
            changes: evolution.changes.clone(),
            estimated_keys,
            batch_size: self.batch_size,
            total_batches,
            parallelism: self.parallelism,
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            dry_run: false,
        }
    }

    /// Start a migration
    pub fn start_migration(&self, plan: MigrationPlan) -> Result<String, MigrationError> {
        let migration_id = plan.id.clone();

        // Check if migration already exists
        if self.active_migrations.contains_key(&migration_id) {
            return Err(MigrationError::AlreadyRunning(migration_id));
        }

        let state = Arc::new(MigrationState::new(plan));
        self.active_migrations.insert(migration_id.clone(), state);

        Ok(migration_id)
    }

    /// Get migration status
    pub fn get_status(&self, migration_id: &str) -> Option<MigrationStatus> {
        self.active_migrations.get(migration_id).map(|state| {
            let s = state.value();
            MigrationStatus {
                id: migration_id.to_string(),
                schema_name: s.plan.schema_name.clone(),
                from_version: s.plan.from_version,
                to_version: s.plan.to_version,
                status: s.status(),
                progress: s.progress(),
                keys_processed: s.keys_processed.load(Ordering::Relaxed),
                keys_migrated: s.keys_migrated.load(Ordering::Relaxed),
                keys_failed: s.keys_failed.load(Ordering::Relaxed),
                current_batch: s.current_batch.load(Ordering::Relaxed) as usize,
                total_batches: s.plan.total_batches,
                started_at: s.started_at,
                elapsed_secs: s.elapsed_secs(),
                estimated_remaining_secs: s.estimated_remaining_secs(),
                errors: s.get_errors(),
            }
        })
    }

    /// Cancel a migration
    pub fn cancel_migration(&self, migration_id: &str) -> Result<(), MigrationError> {
        if let Some(state) = self.active_migrations.get(migration_id) {
            state.cancel();
            Ok(())
        } else {
            Err(MigrationError::NotFound(migration_id.to_string()))
        }
    }

    /// Pause a migration
    pub fn pause_migration(&self, migration_id: &str) -> Result<(), MigrationError> {
        if let Some(state) = self.active_migrations.get(migration_id) {
            state.pause();
            Ok(())
        } else {
            Err(MigrationError::NotFound(migration_id.to_string()))
        }
    }

    /// Resume a paused migration
    pub fn resume_migration(&self, migration_id: &str) -> Result<(), MigrationError> {
        if let Some(state) = self.active_migrations.get(migration_id) {
            state.resume();
            Ok(())
        } else {
            Err(MigrationError::NotFound(migration_id.to_string()))
        }
    }

    /// Transform a single value according to evolution changes
    pub fn transform_value(
        &self,
        value: &serde_json::Value,
        changes: &[FieldChange],
    ) -> Result<serde_json::Value, MigrationError> {
        let mut result = value.clone();

        if let serde_json::Value::Object(ref mut obj) = result {
            for change in changes {
                apply_change_to_object(obj, change)?;
            }
        }

        Ok(result)
    }

    /// List all migrations
    pub fn list_migrations(&self) -> Vec<MigrationStatus> {
        self.active_migrations
            .iter()
            .map(|entry| {
                let migration_id = entry.key();
                let s = entry.value();
                MigrationStatus {
                    id: migration_id.clone(),
                    schema_name: s.plan.schema_name.clone(),
                    from_version: s.plan.from_version,
                    to_version: s.plan.to_version,
                    status: s.status(),
                    progress: s.progress(),
                    keys_processed: s.keys_processed.load(Ordering::Relaxed),
                    keys_migrated: s.keys_migrated.load(Ordering::Relaxed),
                    keys_failed: s.keys_failed.load(Ordering::Relaxed),
                    current_batch: s.current_batch.load(Ordering::Relaxed) as usize,
                    total_batches: s.plan.total_batches,
                    started_at: s.started_at,
                    elapsed_secs: s.elapsed_secs(),
                    estimated_remaining_secs: s.estimated_remaining_secs(),
                    errors: s.get_errors(),
                }
            })
            .collect()
    }

    /// Remove completed/cancelled migrations
    pub fn cleanup_completed(&self) {
        self.active_migrations.retain(|_, state| {
            let status = state.status();
            !matches!(
                status,
                MigrationStateType::Completed
                    | MigrationStateType::Cancelled
                    | MigrationStateType::Failed
            )
        });
    }
}

/// Apply a single change to a JSON object
fn apply_change_to_object(
    obj: &mut serde_json::Map<String, serde_json::Value>,
    change: &FieldChange,
) -> Result<(), MigrationError> {
    match change {
        FieldChange::Add { name, default, .. } => {
            if !obj.contains_key(name) {
                let value = default
                    .as_ref()
                    .map(|d| d.to_json())
                    .unwrap_or(serde_json::Value::Null);
                obj.insert(name.clone(), value);
            }
        }
        FieldChange::Remove { name, .. } => {
            obj.remove(name);
        }
        FieldChange::Rename { from, to } => {
            if let Some(value) = obj.remove(from) {
                obj.insert(to.clone(), value);
            }
        }
        FieldChange::ChangeType {
            name, transform, ..
        } => {
            if let Some(value) = obj.get(name) {
                if let Some(expr) = transform {
                    let transformed = apply_transform(value, expr)?;
                    obj.insert(name.clone(), transformed);
                }
            }
        }
        FieldChange::MakeOptional { .. } => {
            // No change needed to data
        }
        FieldChange::MakeRequired { name, default } => {
            if !obj.contains_key(name) || obj.get(name) == Some(&serde_json::Value::Null) {
                if let Some(d) = default {
                    obj.insert(name.clone(), d.to_json());
                }
            }
        }
        FieldChange::SetDefault { name, default } => {
            if !obj.contains_key(name) {
                if let Some(d) = default {
                    obj.insert(name.clone(), d.to_json());
                }
            }
        }
        FieldChange::Deprecate { .. } => {
            // No change needed to data
        }
    }

    Ok(())
}

/// Apply a transformation expression to a value
fn apply_transform(
    value: &serde_json::Value,
    expr: &str,
) -> Result<serde_json::Value, MigrationError> {
    // Simple transform expressions
    match expr.to_uppercase().as_str() {
        "TO_STRING" | "STRING" => match value {
            serde_json::Value::String(s) => Ok(serde_json::Value::String(s.clone())),
            serde_json::Value::Number(n) => Ok(serde_json::Value::String(n.to_string())),
            serde_json::Value::Bool(b) => Ok(serde_json::Value::String(b.to_string())),
            serde_json::Value::Null => Ok(serde_json::Value::String("".to_string())),
            _ => Err(MigrationError::TransformFailed(format!(
                "Cannot convert {:?} to string",
                value
            ))),
        },
        "TO_INT" | "INTEGER" => match value {
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(serde_json::Value::Number(i.into()))
                } else {
                    Ok(serde_json::Value::Number(
                        (n.as_f64().unwrap_or(0.0) as i64).into(),
                    ))
                }
            }
            serde_json::Value::String(s) => {
                let i: i64 = s.parse().map_err(|_| {
                    MigrationError::TransformFailed(format!("Cannot parse '{}' as integer", s))
                })?;
                Ok(serde_json::Value::Number(i.into()))
            }
            serde_json::Value::Bool(b) => {
                Ok(serde_json::Value::Number(if *b { 1 } else { 0 }.into()))
            }
            _ => Err(MigrationError::TransformFailed(format!(
                "Cannot convert {:?} to integer",
                value
            ))),
        },
        "TO_FLOAT" | "FLOAT" => match value {
            serde_json::Value::Number(n) => Ok(serde_json::Value::Number(
                serde_json::Number::from_f64(n.as_f64().unwrap_or(0.0))
                    .unwrap_or_else(|| serde_json::Number::from(0)),
            )),
            serde_json::Value::String(s) => {
                let f: f64 = s.parse().map_err(|_| {
                    MigrationError::TransformFailed(format!("Cannot parse '{}' as float", s))
                })?;
                Ok(serde_json::Value::Number(
                    serde_json::Number::from_f64(f).unwrap_or_else(|| serde_json::Number::from(0)),
                ))
            }
            _ => Err(MigrationError::TransformFailed(format!(
                "Cannot convert {:?} to float",
                value
            ))),
        },
        "TO_BOOL" | "BOOLEAN" => match value {
            serde_json::Value::Bool(b) => Ok(serde_json::Value::Bool(*b)),
            serde_json::Value::String(s) => {
                let b = matches!(s.to_lowercase().as_str(), "true" | "1" | "yes" | "on");
                Ok(serde_json::Value::Bool(b))
            }
            serde_json::Value::Number(n) => Ok(serde_json::Value::Bool(
                n.as_i64().is_some_and(|i| i != 0),
            )),
            _ => Err(MigrationError::TransformFailed(format!(
                "Cannot convert {:?} to boolean",
                value
            ))),
        },
        "UPPER" | "UPPERCASE" => match value {
            serde_json::Value::String(s) => Ok(serde_json::Value::String(s.to_uppercase())),
            _ => Err(MigrationError::TransformFailed(
                "UPPER only works on strings".to_string(),
            )),
        },
        "LOWER" | "LOWERCASE" => match value {
            serde_json::Value::String(s) => Ok(serde_json::Value::String(s.to_lowercase())),
            _ => Err(MigrationError::TransformFailed(
                "LOWER only works on strings".to_string(),
            )),
        },
        "TRIM" => match value {
            serde_json::Value::String(s) => Ok(serde_json::Value::String(s.trim().to_string())),
            _ => Ok(value.clone()),
        },
        _ => {
            // Unknown transform, return original
            Ok(value.clone())
        }
    }
}

/// Migration state tracking
struct MigrationState {
    plan: MigrationPlan,
    keys_processed: AtomicU64,
    keys_migrated: AtomicU64,
    keys_failed: AtomicU64,
    current_batch: AtomicU64,
    cancelled: AtomicBool,
    paused: AtomicBool,
    completed: AtomicBool,
    failed: AtomicBool,
    started_at: u64,
    start_instant: Instant,
    errors: parking_lot::RwLock<Vec<String>>,
}

impl MigrationState {
    fn new(plan: MigrationPlan) -> Self {
        Self {
            plan,
            keys_processed: AtomicU64::new(0),
            keys_migrated: AtomicU64::new(0),
            keys_failed: AtomicU64::new(0),
            current_batch: AtomicU64::new(0),
            cancelled: AtomicBool::new(false),
            paused: AtomicBool::new(false),
            completed: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            started_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            start_instant: Instant::now(),
            errors: parking_lot::RwLock::new(Vec::new()),
        }
    }

    fn status(&self) -> MigrationStateType {
        if self.failed.load(Ordering::Relaxed) {
            MigrationStateType::Failed
        } else if self.cancelled.load(Ordering::Relaxed) {
            MigrationStateType::Cancelled
        } else if self.completed.load(Ordering::Relaxed) {
            MigrationStateType::Completed
        } else if self.paused.load(Ordering::Relaxed) {
            MigrationStateType::Paused
        } else {
            MigrationStateType::Running
        }
    }

    fn progress(&self) -> f64 {
        let processed = self.keys_processed.load(Ordering::Relaxed);
        if self.plan.estimated_keys == 0 {
            0.0
        } else {
            processed as f64 / self.plan.estimated_keys as f64
        }
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    fn pause(&self) {
        self.paused.store(true, Ordering::Relaxed);
    }

    fn resume(&self) {
        self.paused.store(false, Ordering::Relaxed);
    }

    fn elapsed_secs(&self) -> u64 {
        self.start_instant.elapsed().as_secs()
    }

    fn estimated_remaining_secs(&self) -> Option<u64> {
        let elapsed = self.elapsed_secs();
        let progress = self.progress();

        if progress <= 0.0 || elapsed == 0 {
            return None;
        }

        let total_estimated = elapsed as f64 / progress;
        let remaining = total_estimated - elapsed as f64;

        Some(remaining.max(0.0) as u64)
    }

    fn get_errors(&self) -> Vec<String> {
        self.errors.read().clone()
    }
}

/// Migration plan
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationPlan {
    /// Unique migration ID
    pub id: String,
    /// Schema name
    pub schema_name: String,
    /// Source version
    pub from_version: u32,
    /// Target version
    pub to_version: u32,
    /// Key pattern to migrate
    pub key_pattern: String,
    /// Migration strategy
    pub strategy: EvolutionStrategy,
    /// Changes to apply
    pub changes: Vec<FieldChange>,
    /// Estimated number of keys
    pub estimated_keys: u64,
    /// Batch size
    pub batch_size: usize,
    /// Total batches
    pub total_batches: usize,
    /// Parallelism
    pub parallelism: usize,
    /// Creation timestamp
    pub created_at: u64,
    /// Dry run mode
    pub dry_run: bool,
}

/// Migration progress information
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationProgress {
    /// Progress percentage (0.0 - 1.0)
    pub progress: f64,
    /// Keys processed
    pub keys_processed: u64,
    /// Keys successfully migrated
    pub keys_migrated: u64,
    /// Keys that failed migration
    pub keys_failed: u64,
    /// Current batch number
    pub current_batch: usize,
    /// Total batches
    pub total_batches: usize,
}

/// Migration status
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationStatus {
    /// Migration ID
    pub id: String,
    /// Schema name
    pub schema_name: String,
    /// Source version
    pub from_version: u32,
    /// Target version
    pub to_version: u32,
    /// Current status
    pub status: MigrationStateType,
    /// Progress (0.0 - 1.0)
    pub progress: f64,
    /// Keys processed
    pub keys_processed: u64,
    /// Keys migrated
    pub keys_migrated: u64,
    /// Keys failed
    pub keys_failed: u64,
    /// Current batch
    pub current_batch: usize,
    /// Total batches
    pub total_batches: usize,
    /// Start timestamp
    pub started_at: u64,
    /// Elapsed seconds
    pub elapsed_secs: u64,
    /// Estimated remaining seconds
    pub estimated_remaining_secs: Option<u64>,
    /// Error messages
    pub errors: Vec<String>,
}

/// Migration state type
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MigrationStateType {
    /// Migration is running
    Running,
    /// Migration is paused
    Paused,
    /// Migration completed successfully
    Completed,
    /// Migration was cancelled
    Cancelled,
    /// Migration failed
    Failed,
}

impl std::fmt::Display for MigrationStateType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationStateType::Running => write!(f, "running"),
            MigrationStateType::Paused => write!(f, "paused"),
            MigrationStateType::Completed => write!(f, "completed"),
            MigrationStateType::Cancelled => write!(f, "cancelled"),
            MigrationStateType::Failed => write!(f, "failed"),
        }
    }
}

/// Migration error
#[derive(Debug)]
pub enum MigrationError {
    /// Migration already running
    AlreadyRunning(String),
    /// Migration not found
    NotFound(String),
    /// Transform failed
    TransformFailed(String),
    /// Schema error
    SchemaError(String),
}

impl std::fmt::Display for MigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationError::AlreadyRunning(id) => write!(f, "Migration {} is already running", id),
            MigrationError::NotFound(id) => write!(f, "Migration {} not found", id),
            MigrationError::TransformFailed(msg) => write!(f, "Transform failed: {}", msg),
            MigrationError::SchemaError(msg) => write!(f, "Schema error: {}", msg),
        }
    }
}

impl std::error::Error for MigrationError {}

/// Generate a unique migration ID
fn generate_migration_id() -> String {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};

    let state = RandomState::new();
    let mut hasher = state.build_hasher();
    hasher.write_u128(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    );

    format!("mig_{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::FieldDefault;
    use crate::schema::FieldType;
    use serde_json::json;

    #[test]
    fn test_transform_to_string() {
        let result = apply_transform(&json!(42), "TO_STRING").unwrap();
        assert_eq!(result, json!("42"));

        let result = apply_transform(&json!(true), "TO_STRING").unwrap();
        assert_eq!(result, json!("true"));
    }

    #[test]
    fn test_transform_to_int() {
        let result = apply_transform(&json!("42"), "TO_INT").unwrap();
        assert_eq!(result, json!(42));

        let result = apply_transform(&json!(3.14), "TO_INT").unwrap();
        assert_eq!(result, json!(3));
    }

    #[test]
    fn test_transform_upper_lower() {
        let result = apply_transform(&json!("Hello"), "UPPER").unwrap();
        assert_eq!(result, json!("HELLO"));

        let result = apply_transform(&json!("Hello"), "LOWER").unwrap();
        assert_eq!(result, json!("hello"));
    }

    #[test]
    fn test_apply_add_field() {
        let mut obj = serde_json::Map::new();
        obj.insert("id".to_string(), json!("1"));

        let change = FieldChange::Add {
            name: "status".to_string(),
            field_type: FieldType::String,
            required: false,
            default: Some(FieldDefault::String("active".to_string())),
        };

        apply_change_to_object(&mut obj, &change).unwrap();
        assert_eq!(obj.get("status"), Some(&json!("active")));
    }

    #[test]
    fn test_apply_rename_field() {
        let mut obj = serde_json::Map::new();
        obj.insert("name".to_string(), json!("Alice"));

        let change = FieldChange::Rename {
            from: "name".to_string(),
            to: "display_name".to_string(),
        };

        apply_change_to_object(&mut obj, &change).unwrap();
        assert!(obj.get("name").is_none());
        assert_eq!(obj.get("display_name"), Some(&json!("Alice")));
    }

    #[test]
    fn test_apply_remove_field() {
        let mut obj = serde_json::Map::new();
        obj.insert("id".to_string(), json!("1"));
        obj.insert("legacy".to_string(), json!("old_value"));

        let change = FieldChange::Remove {
            name: "legacy".to_string(),
            archive: false,
        };

        apply_change_to_object(&mut obj, &change).unwrap();
        assert!(obj.get("legacy").is_none());
        assert_eq!(obj.get("id"), Some(&json!("1")));
    }

    #[test]
    fn test_migration_plan_creation() {
        let evolution = SchemaEvolution::new("user", 1, 2)
            .add_field("email", FieldType::String, None)
            .build();

        let migrator = SchemaMigrator::with_defaults();
        let plan = migrator.create_plan(&evolution, "users:*", 10000);

        assert_eq!(plan.schema_name, "user");
        assert_eq!(plan.from_version, 1);
        assert_eq!(plan.to_version, 2);
        assert_eq!(plan.estimated_keys, 10000);
    }
}
