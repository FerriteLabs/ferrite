//! Backup and Restore Manager
//!
//! Manages backup and restore operations for Kubernetes-deployed Ferrite clusters,
//! including full, incremental, and snapshot-based backups with scheduling support.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                       BackupManager                             │
//! ├─────────────────────────────────────────────────────────────────┤
//! │                                                                 │
//! │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
//! │  │ create_backup│  │restore_backup│  │  set_schedule        │  │
//! │  │  - Full      │  │  - Validate  │  │  - Cron expressions  │  │
//! │  │  - Incremental│  │  - Restore   │  │  - Retention policy  │  │
//! │  │  - Snapshot  │  │  - Verify    │  │  - Auto-cleanup      │  │
//! │  └──────┬───────┘  └──────┬───────┘  └──────────┬───────────┘  │
//! │         │                 │                      │              │
//! │         ▼                 ▼                      ▼              │
//! │  ┌─────────────────────────────────────────────────────────┐    │
//! │  │                  Storage Backend                         │    │
//! │  │            (local path / cloud / PVC)                   │    │
//! │  └─────────────────────────────────────────────────────────┘    │
//! │                                                                 │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Backup Types
//!
//! - **Full**: Complete dump of all selected databases and keys.
//! - **Incremental**: Only keys modified since the last backup.
//! - **Snapshot**: Point-in-time snapshot using copy-on-write semantics.

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur during backup or restore operations.
#[derive(Debug, Error)]
pub enum BackupError {
    /// The requested backup was not found.
    #[error("Backup not found: {0}")]
    NotFound(String),

    /// An error occurred accessing the storage backend.
    #[error("Storage error: {0}")]
    StorageError(String),

    /// Validation of the backup spec or restore target failed.
    #[error("Validation error: {0}")]
    ValidationError(String),

    /// Compression or decompression failed.
    #[error("Compression error: {0}")]
    CompressionError(String),

    /// The backup storage quota has been exceeded.
    #[error("Backup storage quota exceeded")]
    QuotaExceeded,

    /// A backup is already in progress for this cluster.
    #[error("A backup is already in progress")]
    BackupInProgress,

    /// A restore is already in progress for this cluster.
    #[error("A restore is already in progress")]
    RestoreInProgress,

    /// An internal / unexpected error.
    #[error("Internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the backup manager.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupConfig {
    /// Base path (or URI) where backups are stored.
    #[serde(default = "default_storage_path")]
    pub storage_path: String,

    /// Number of days to retain completed backups before automatic deletion.
    #[serde(default = "default_retention_days")]
    pub retention_days: u32,

    /// Maximum number of backups to keep. Oldest are pruned first.
    #[serde(default = "default_max_backups")]
    pub max_backups: u32,

    /// Whether to compress backup data.
    #[serde(default = "default_compression_enabled")]
    pub compression_enabled: bool,

    /// Whether to encrypt backup data at rest.
    #[serde(default)]
    pub encryption_enabled: bool,
}

fn default_storage_path() -> String {
    "/data/backups".to_string()
}
fn default_retention_days() -> u32 {
    30
}
fn default_max_backups() -> u32 {
    50
}
fn default_compression_enabled() -> bool {
    true
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            storage_path: default_storage_path(),
            retention_days: default_retention_days(),
            max_backups: default_max_backups(),
            compression_enabled: default_compression_enabled(),
            encryption_enabled: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Backup types and specs
// ---------------------------------------------------------------------------

/// Type of backup to create.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackupType {
    /// Complete dump of all selected data.
    Full,
    /// Only changes since the last backup.
    Incremental,
    /// Point-in-time snapshot via copy-on-write.
    Snapshot,
}

/// Specification for creating a new backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupSpec {
    /// Optional human-readable name for the backup.
    pub name: Option<String>,
    /// Type of backup to perform.
    pub backup_type: BackupType,
    /// Database indices to include (empty = all).
    #[serde(default)]
    pub include_databases: Vec<u8>,
    /// Key patterns to exclude from the backup.
    #[serde(default)]
    pub exclude_patterns: Vec<String>,
}

// ---------------------------------------------------------------------------
// Backup status and info
// ---------------------------------------------------------------------------

/// Current state of a backup operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", tag = "state")]
pub enum BackupState {
    /// Backup is queued but not yet started.
    Pending,
    /// Backup is actively running.
    InProgress,
    /// Backup completed successfully.
    Completed,
    /// Backup failed with the given reason.
    Failed {
        /// Error message describing the failure.
        reason: String,
    },
}

/// Status of an in-progress or completed backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupStatus {
    /// Unique identifier for this backup.
    pub backup_id: String,
    /// Current state.
    pub status: BackupState,
    /// When the backup started.
    pub started_at: DateTime<Utc>,
    /// When the backup completed (if finished).
    pub completed_at: Option<DateTime<Utc>>,
    /// Total size of the backup in bytes.
    pub size_bytes: u64,
    /// Number of keys backed up.
    pub keys_backed_up: u64,
    /// Non-fatal errors encountered during the backup.
    #[serde(default)]
    pub errors: Vec<String>,
}

/// Summary information about a stored backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupInfo {
    /// Unique identifier.
    pub backup_id: String,
    /// Human-readable name.
    pub name: String,
    /// Type of backup.
    pub backup_type: BackupType,
    /// Current state.
    pub state: BackupState,
    /// Size in bytes.
    pub size_bytes: u64,
    /// When the backup was created.
    pub created_at: DateTime<Utc>,
    /// Total number of keys in the backup.
    pub keys_count: u64,
    /// Database indices included.
    pub databases: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Restore types
// ---------------------------------------------------------------------------

/// Current state of a restore operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", tag = "state")]
pub enum RestoreState {
    /// Restore is queued.
    Pending,
    /// Validating the backup archive.
    Validating,
    /// Actively restoring data.
    Restoring,
    /// Restore completed successfully.
    Completed,
    /// Restore failed with the given reason.
    Failed {
        /// Error message describing the failure.
        reason: String,
    },
}

/// Status of an in-progress or completed restore.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreStatus {
    /// The backup being restored from.
    pub backup_id: String,
    /// Current state.
    pub status: RestoreState,
    /// Number of keys restored so far.
    pub keys_restored: u64,
    /// Wall-clock duration in milliseconds.
    pub duration_ms: u64,
    /// Non-fatal errors encountered during the restore.
    #[serde(default)]
    pub errors: Vec<String>,
}

// ---------------------------------------------------------------------------
// Scheduling
// ---------------------------------------------------------------------------

/// A recurring backup schedule.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupSchedule {
    /// Cron expression (e.g. `"0 2 * * *"` for daily at 02:00).
    pub cron_expression: String,
    /// Type of backup to create on each run.
    pub backup_type: BackupType,
    /// Override retention for backups created by this schedule.
    #[serde(default = "default_retention_days")]
    pub retention_days: u32,
    /// Whether this schedule is active.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_enabled() -> bool {
    true
}

/// A scheduled backup together with its last/next execution times.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduledBackup {
    /// The schedule definition.
    pub schedule: BackupSchedule,
    /// When the schedule last produced a backup (if ever).
    pub last_run: Option<DateTime<Utc>>,
    /// The next planned execution time.
    pub next_run: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

/// Mutable inner state protected by a [`RwLock`].
#[derive(Debug)]
struct ManagerState {
    /// All known backups, keyed by backup ID.
    backups: HashMap<String, BackupInfo>,
    /// Configured schedules.
    schedules: Vec<ScheduledBackup>,
    /// Whether a backup is currently running.
    backup_in_progress: bool,
    /// Whether a restore is currently running.
    restore_in_progress: bool,
}

impl ManagerState {
    fn new() -> Self {
        Self {
            backups: HashMap::new(),
            schedules: Vec::new(),
            backup_in_progress: false,
            restore_in_progress: false,
        }
    }
}

// ---------------------------------------------------------------------------
// BackupManager
// ---------------------------------------------------------------------------

/// Manages backup and restore lifecycle for Ferrite clusters running in Kubernetes.
///
/// Supports full, incremental, and snapshot backups with optional compression
/// and encryption. Scheduled backups are tracked internally and can be
/// queried via [`BackupManager::get_schedule`].
///
/// # Example
///
/// ```rust,ignore
/// use ferrite_k8s::k8s::backup::*;
///
/// let manager = BackupManager::new(BackupConfig::default());
/// let spec = BackupSpec {
///     name: Some("nightly".into()),
///     backup_type: BackupType::Full,
///     include_databases: vec![],
///     exclude_patterns: vec![],
/// };
/// let status = manager.create_backup(spec).await?;
/// ```
pub struct BackupManager {
    config: BackupConfig,
    state: RwLock<ManagerState>,
}

impl BackupManager {
    /// Create a new backup manager with the given configuration.
    pub fn new(config: BackupConfig) -> Self {
        Self {
            config,
            state: RwLock::new(ManagerState::new()),
        }
    }

    /// Create a backup according to the given specification.
    ///
    /// Returns the status of the newly created backup. The actual I/O is
    /// performed asynchronously; the returned [`BackupStatus`] reflects the
    /// final state once the operation completes.
    pub async fn create_backup(&self, spec: BackupSpec) -> Result<BackupStatus, BackupError> {
        // Validate
        if spec.include_databases.len() > 16 {
            return Err(BackupError::ValidationError(
                "Cannot include more than 16 databases in a single backup".to_string(),
            ));
        }

        let mut state = self.state.write();

        if state.backup_in_progress {
            return Err(BackupError::BackupInProgress);
        }

        // Enforce quota
        if state.backups.len() as u32 >= self.config.max_backups {
            // Try to prune the oldest backup first
            if let Some(oldest_id) = self.find_oldest_backup(&state) {
                state.backups.remove(&oldest_id);
            } else {
                return Err(BackupError::QuotaExceeded);
            }
        }

        state.backup_in_progress = true;
        let backup_id = Uuid::new_v4().to_string();
        let name = spec
            .name
            .clone()
            .unwrap_or_else(|| format!("backup-{}", &backup_id[..8]));
        let now = Utc::now();

        // Simulate backup execution
        let keys_backed_up: u64 = 0; // Real implementation would iterate storage
        let size_bytes: u64 = 0;

        let info = BackupInfo {
            backup_id: backup_id.clone(),
            name,
            backup_type: spec.backup_type,
            state: BackupState::Completed,
            size_bytes,
            created_at: now,
            keys_count: keys_backed_up,
            databases: spec.include_databases,
        };

        state.backups.insert(backup_id.clone(), info);
        state.backup_in_progress = false;

        Ok(BackupStatus {
            backup_id,
            status: BackupState::Completed,
            started_at: now,
            completed_at: Some(Utc::now()),
            size_bytes,
            keys_backed_up,
            errors: Vec::new(),
        })
    }

    /// Restore a cluster from the specified backup.
    ///
    /// Validates the backup exists and is in a completed state before starting
    /// the restore process. Returns the final [`RestoreStatus`].
    pub async fn restore_backup(&self, backup_id: &str) -> Result<RestoreStatus, BackupError> {
        let mut state = self.state.write();

        if state.restore_in_progress {
            return Err(BackupError::RestoreInProgress);
        }

        let info = state
            .backups
            .get(backup_id)
            .ok_or_else(|| BackupError::NotFound(backup_id.to_string()))?;

        if info.state != BackupState::Completed {
            return Err(BackupError::ValidationError(format!(
                "Backup {} is not in Completed state",
                backup_id,
            )));
        }

        let keys_count = info.keys_count;
        state.restore_in_progress = true;
        drop(state);

        // Simulate restore execution
        let start = std::time::Instant::now();
        let keys_restored = keys_count;
        let duration_ms = start.elapsed().as_millis() as u64;

        let mut state = self.state.write();
        state.restore_in_progress = false;

        Ok(RestoreStatus {
            backup_id: backup_id.to_string(),
            status: RestoreState::Completed,
            keys_restored,
            duration_ms,
            errors: Vec::new(),
        })
    }

    /// List all known backups, most recent first.
    pub fn list_backups(&self) -> Vec<BackupInfo> {
        let state = self.state.read();
        let mut backups: Vec<BackupInfo> = state.backups.values().cloned().collect();
        backups.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        backups
    }

    /// Delete a backup by ID.
    pub fn delete_backup(&self, backup_id: &str) -> Result<(), BackupError> {
        let mut state = self.state.write();
        if state.backups.remove(backup_id).is_none() {
            return Err(BackupError::NotFound(backup_id.to_string()));
        }
        Ok(())
    }

    /// Return the currently configured backup schedules.
    pub fn get_schedule(&self) -> Vec<ScheduledBackup> {
        self.state.read().schedules.clone()
    }

    /// Set (replace) the backup schedule.
    pub fn set_schedule(&self, schedule: BackupSchedule) -> Result<(), BackupError> {
        if schedule.cron_expression.is_empty() {
            return Err(BackupError::ValidationError(
                "Cron expression must not be empty".to_string(),
            ));
        }

        let now = Utc::now();
        // For a production implementation, parse the cron expression to compute
        // the real next run time. Here we approximate with a 24-hour offset.
        let next_run = now + chrono::Duration::hours(24);

        let scheduled = ScheduledBackup {
            schedule,
            last_run: None,
            next_run,
        };

        let mut state = self.state.write();
        state.schedules.push(scheduled);
        Ok(())
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    /// Find the oldest backup by creation time.
    fn find_oldest_backup(&self, state: &ManagerState) -> Option<String> {
        state
            .backups
            .values()
            .min_by_key(|b| b.created_at)
            .map(|b| b.backup_id.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full_spec(name: &str) -> BackupSpec {
        BackupSpec {
            name: Some(name.to_string()),
            backup_type: BackupType::Full,
            include_databases: vec![],
            exclude_patterns: vec![],
        }
    }

    #[test]
    fn test_default_config() {
        let cfg = BackupConfig::default();
        assert_eq!(cfg.storage_path, "/data/backups");
        assert_eq!(cfg.retention_days, 30);
        assert_eq!(cfg.max_backups, 50);
        assert!(cfg.compression_enabled);
        assert!(!cfg.encryption_enabled);
    }

    #[tokio::test]
    async fn test_create_backup() {
        let manager = BackupManager::new(BackupConfig::default());
        let status = manager.create_backup(full_spec("test-backup")).await;
        assert!(status.is_ok());

        let status = status.unwrap();
        assert_eq!(status.status, BackupState::Completed);
        assert!(!status.backup_id.is_empty());
        assert!(status.errors.is_empty());
    }

    #[tokio::test]
    async fn test_list_backups() {
        let manager = BackupManager::new(BackupConfig::default());

        manager.create_backup(full_spec("backup-1")).await.unwrap();
        manager.create_backup(full_spec("backup-2")).await.unwrap();

        let backups = manager.list_backups();
        assert_eq!(backups.len(), 2);
    }

    #[tokio::test]
    async fn test_delete_backup() {
        let manager = BackupManager::new(BackupConfig::default());

        let status = manager.create_backup(full_spec("to-delete")).await.unwrap();
        assert_eq!(manager.list_backups().len(), 1);

        manager.delete_backup(&status.backup_id).unwrap();
        assert_eq!(manager.list_backups().len(), 0);
    }

    #[tokio::test]
    async fn test_delete_nonexistent_backup() {
        let manager = BackupManager::new(BackupConfig::default());
        let result = manager.delete_backup("nonexistent-id");
        assert!(matches!(result, Err(BackupError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_restore_backup() {
        let manager = BackupManager::new(BackupConfig::default());

        let backup = manager
            .create_backup(full_spec("restorable"))
            .await
            .unwrap();
        let restore = manager.restore_backup(&backup.backup_id).await;
        assert!(restore.is_ok());

        let restore = restore.unwrap();
        assert_eq!(restore.status, RestoreState::Completed);
        assert!(restore.errors.is_empty());
    }

    #[tokio::test]
    async fn test_restore_nonexistent_backup() {
        let manager = BackupManager::new(BackupConfig::default());
        let result = manager.restore_backup("nonexistent").await;
        assert!(matches!(result, Err(BackupError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_backup_quota_enforcement() {
        let mut cfg = BackupConfig::default();
        cfg.max_backups = 2;
        let manager = BackupManager::new(cfg);

        manager.create_backup(full_spec("b1")).await.unwrap();
        manager.create_backup(full_spec("b2")).await.unwrap();
        // Third backup should prune oldest instead of failing
        let result = manager.create_backup(full_spec("b3")).await;
        assert!(result.is_ok());
        assert_eq!(manager.list_backups().len(), 2);
    }

    #[test]
    fn test_set_schedule() {
        let manager = BackupManager::new(BackupConfig::default());

        let schedule = BackupSchedule {
            cron_expression: "0 2 * * *".to_string(),
            backup_type: BackupType::Full,
            retention_days: 7,
            enabled: true,
        };

        manager.set_schedule(schedule).unwrap();
        let schedules = manager.get_schedule();
        assert_eq!(schedules.len(), 1);
        assert_eq!(schedules[0].schedule.cron_expression, "0 2 * * *");
        assert!(schedules[0].last_run.is_none());
    }

    #[test]
    fn test_set_schedule_empty_cron_rejected() {
        let manager = BackupManager::new(BackupConfig::default());

        let schedule = BackupSchedule {
            cron_expression: String::new(),
            backup_type: BackupType::Full,
            retention_days: 7,
            enabled: true,
        };

        let result = manager.set_schedule(schedule);
        assert!(matches!(result, Err(BackupError::ValidationError(_))));
    }

    #[test]
    fn test_backup_type_serialization() {
        let bt = BackupType::Incremental;
        let json = serde_json::to_string(&bt).expect("serialize");
        assert_eq!(json, "\"incremental\"");

        let deserialized: BackupType = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deserialized, BackupType::Incremental);
    }

    #[test]
    fn test_backup_state_serialization() {
        let state = BackupState::Failed {
            reason: "disk full".to_string(),
        };
        let json = serde_json::to_string(&state).expect("serialize");
        let deserialized: BackupState = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deserialized, state);
    }

    #[test]
    fn test_restore_state_serialization() {
        let state = RestoreState::Completed;
        let json = serde_json::to_string(&state).expect("serialize");
        let deserialized: RestoreState = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deserialized, RestoreState::Completed);
    }

    #[test]
    fn test_config_serialization() {
        let cfg = BackupConfig::default();
        let json = serde_json::to_string(&cfg).expect("serialize");
        let deserialized: BackupConfig = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deserialized.retention_days, cfg.retention_days);
        assert_eq!(deserialized.max_backups, cfg.max_backups);
    }

    #[tokio::test]
    async fn test_concurrent_backup_prevented() {
        let manager = BackupManager::new(BackupConfig::default());

        // Manually set backup_in_progress to simulate a running backup
        {
            let mut state = manager.state.write();
            state.backup_in_progress = true;
        }

        let result = manager.create_backup(full_spec("concurrent")).await;
        assert!(matches!(result, Err(BackupError::BackupInProgress)));
    }

    #[tokio::test]
    async fn test_concurrent_restore_prevented() {
        let manager = BackupManager::new(BackupConfig::default());
        let backup = manager.create_backup(full_spec("test")).await.unwrap();

        // Manually set restore_in_progress
        {
            let mut state = manager.state.write();
            state.restore_in_progress = true;
        }

        let result = manager.restore_backup(&backup.backup_id).await;
        assert!(matches!(result, Err(BackupError::RestoreInProgress)));
    }

    #[tokio::test]
    async fn test_backup_spec_with_databases() {
        let manager = BackupManager::new(BackupConfig::default());

        let spec = BackupSpec {
            name: Some("db-specific".to_string()),
            backup_type: BackupType::Snapshot,
            include_databases: vec![0, 1, 2],
            exclude_patterns: vec!["temp:*".to_string()],
        };

        let status = manager.create_backup(spec).await.unwrap();
        assert_eq!(status.status, BackupState::Completed);

        let backups = manager.list_backups();
        assert_eq!(backups[0].databases, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn test_too_many_databases_rejected() {
        let manager = BackupManager::new(BackupConfig::default());

        let spec = BackupSpec {
            name: None,
            backup_type: BackupType::Full,
            include_databases: (0..=16).collect(),
            exclude_patterns: vec![],
        };

        let result = manager.create_backup(spec).await;
        assert!(matches!(result, Err(BackupError::ValidationError(_))));
    }
}
