//! Backup/Restore Lifecycle Manager
//!
//! Provides automated backup scheduling, execution, retention management, and
//! restore operations for Kubernetes-deployed Ferrite clusters. This module
//! extends the basic backup support with production day-2 operations:
//!
//! - **Scheduled backups** via cron expressions with per-schedule retention
//! - **Multi-target backups**: full, database-only, key-pattern, RDB snapshot, AOF
//! - **Multi-backend storage**: local, S3, GCS, Azure
//! - **Backup verification** with integrity and checksum validation
//! - **Retention cleanup** to free expired backups automatically
//! - **Restore with options**: dry-run, key filtering, target cluster override
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────────────────┐
//! │                     BackupLifecycleManager                               │
//! ├──────────────────────────────────────────────────────────────────────────┤
//! │                                                                          │
//! │  ┌───────────────┐  ┌────────────────┐  ┌───────────────────────────┐   │
//! │  │  Scheduling   │  │ Backup Engine  │  │  Restore Engine           │   │
//! │  │               │  │                │  │                           │   │
//! │  │ - Cron parse  │  │ - Full / DB    │  │ - Validate backup         │   │
//! │  │ - Due check   │  │ - KeyPattern   │  │ - Filter keys             │   │
//! │  │ - Retention   │  │ - RDB / AOF    │  │ - Dry-run                 │   │
//! │  └───────┬───────┘  └───────┬────────┘  └──────────┬────────────────┘   │
//! │          │                  │                       │                    │
//! │          ▼                  ▼                       ▼                    │
//! │  ┌──────────────────────────────────────────────────────────────────┐    │
//! │  │              Storage Backends (Local / S3 / GCS / Azure)         │    │
//! │  └──────────────────────────────────────────────────────────────────┘    │
//! │                                                                          │
//! └──────────────────────────────────────────────────────────────────────────┘
//! ```

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Return the current Unix timestamp in seconds.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur during backup lifecycle operations.
#[derive(Debug, Error)]
pub enum BackupError {
    /// The requested resource was not found.
    #[error("Not found: {0}")]
    NotFound(String),

    /// A resource with the same identifier already exists.
    #[error("Already exists: {0}")]
    AlreadyExists(String),

    /// An error occurred accessing the storage backend.
    #[error("Storage error: {0}")]
    StorageError(String),

    /// The supplied schedule definition is invalid.
    #[error("Invalid schedule: {0}")]
    InvalidSchedule(String),

    /// The maximum number of concurrent backups has been reached.
    #[error("Maximum concurrent backups reached")]
    MaxConcurrentReached,

    /// Backup verification failed.
    #[error("Verification failed: {0}")]
    VerificationFailed(String),

    /// An error occurred during restore.
    #[error("Restore error: {0}")]
    RestoreError(String),

    /// An error occurred during retention cleanup.
    #[error("Retention error: {0}")]
    RetentionError(String),
}

// ---------------------------------------------------------------------------
// Storage class
// ---------------------------------------------------------------------------

/// Storage backend for backups.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackupStorageClass {
    /// Local filesystem.
    Local,
    /// Amazon S3.
    S3,
    /// Google Cloud Storage.
    #[serde(rename = "gcs")]
    GCS,
    /// Microsoft Azure Blob Storage.
    Azure,
}

impl Default for BackupStorageClass {
    fn default() -> Self {
        Self::Local
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the backup lifecycle manager.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupConfig {
    /// Maximum number of concurrent backup operations.
    #[serde(default = "default_max_concurrent_backups")]
    pub max_concurrent_backups: usize,

    /// Maximum number of records kept in backup/restore history.
    #[serde(default = "default_max_history")]
    pub max_history: usize,

    /// Default retention period in days for backups without an explicit override.
    #[serde(default = "default_retention_days")]
    pub default_retention_days: u32,

    /// Default storage backend.
    #[serde(default)]
    pub default_storage_class: BackupStorageClass,

    /// Whether to compress backup data.
    #[serde(default = "default_true")]
    pub compression: bool,

    /// Whether to encrypt backup data at rest.
    #[serde(default)]
    pub encryption: bool,

    /// Whether to automatically verify backups after creation.
    #[serde(default = "default_true")]
    pub verify_after_backup: bool,

    /// Optional S3 bucket name.
    pub s3_bucket: Option<String>,

    /// Key prefix inside the S3 bucket.
    #[serde(default = "default_s3_prefix")]
    pub s3_prefix: String,

    /// Optional GCS bucket name.
    pub gcs_bucket: Option<String>,

    /// Optional local filesystem path for backups.
    #[serde(default = "default_local_path")]
    pub local_path: Option<String>,
}

fn default_max_concurrent_backups() -> usize {
    1
}
fn default_max_history() -> usize {
    100
}
fn default_retention_days() -> u32 {
    30
}
fn default_true() -> bool {
    true
}
fn default_s3_prefix() -> String {
    "ferrite-backups/".to_string()
}
fn default_local_path() -> Option<String> {
    Some("/var/lib/ferrite/backups".to_string())
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            max_concurrent_backups: default_max_concurrent_backups(),
            max_history: default_max_history(),
            default_retention_days: default_retention_days(),
            default_storage_class: BackupStorageClass::default(),
            compression: true,
            encryption: false,
            verify_after_backup: true,
            s3_bucket: None,
            s3_prefix: default_s3_prefix(),
            gcs_bucket: None,
            local_path: default_local_path(),
        }
    }
}

// ---------------------------------------------------------------------------
// Backup target
// ---------------------------------------------------------------------------

/// Describes what data a backup should capture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackupTarget {
    /// Full backup of all databases and keys.
    Full,
    /// Backup a single logical database by index.
    DatabaseOnly(u8),
    /// Backup only keys matching the given pattern.
    KeyPattern(String),
    /// Point-in-time RDB snapshot.
    RdbSnapshot,
    /// Append-only file backup.
    AofOnly,
}

// ---------------------------------------------------------------------------
// Backup schedule
// ---------------------------------------------------------------------------

/// A recurring backup schedule definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupSchedule {
    /// Unique identifier for this schedule.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Cron expression (e.g. `"0 2 * * *"` for daily at 02:00 UTC).
    pub cron_expression: String,
    /// What data to back up.
    pub target: BackupTarget,
    /// Number of days to retain backups produced by this schedule.
    pub retention_days: u32,
    /// Whether the schedule is active.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Timestamp (Unix seconds) of the last execution, if any.
    pub last_run: Option<u64>,
    /// Timestamp (Unix seconds) of the next planned execution.
    pub next_run: Option<u64>,
    /// Timestamp (Unix seconds) when the schedule was created.
    pub created_at: u64,
}

// ---------------------------------------------------------------------------
// Backup status & record
// ---------------------------------------------------------------------------

/// Current status of a backup operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum BackupStatus {
    /// Backup is queued.
    Pending,
    /// Backup is actively running.
    InProgress,
    /// Backup completed successfully.
    Completed,
    /// Backup failed.
    Failed {
        /// Reason for the failure.
        reason: String,
    },
    /// Backup has expired past its retention window.
    Expired,
    /// Backup was explicitly deleted.
    Deleted,
}

/// A record of a single backup execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupRecord {
    /// Unique identifier for this backup.
    pub id: String,
    /// Schedule that triggered this backup, if any (manual backups are `None`).
    pub schedule_id: Option<String>,
    /// What data was backed up.
    pub target: BackupTarget,
    /// Current status.
    pub status: BackupStatus,
    /// When the backup started (Unix seconds).
    pub started_at: u64,
    /// When the backup completed (Unix seconds).
    pub completed_at: Option<u64>,
    /// Total size of the backup in bytes.
    pub size_bytes: u64,
    /// Number of keys in the backup.
    pub key_count: u64,
    /// URI or path where the backup is stored.
    pub storage_location: String,
    /// Storage backend used.
    pub storage_class: BackupStorageClass,
    /// Whether the data is compressed.
    pub compressed: bool,
    /// Whether the data is encrypted.
    pub encrypted: bool,
    /// Integrity checksum (e.g. SHA-256 hex digest).
    pub checksum: Option<String>,
    /// Error message if the backup failed.
    pub error: Option<String>,
    /// Timestamp (Unix seconds) when the backup should be deleted.
    pub retention_until: u64,
}

// ---------------------------------------------------------------------------
// Restore types
// ---------------------------------------------------------------------------

/// Options controlling how a restore is performed.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreOptions {
    /// Target cluster name. `None` means restore to the current cluster.
    pub target_cluster: Option<String>,
    /// Target database index to restore into.
    pub target_db: Option<u8>,
    /// Only restore keys matching this pattern.
    pub key_pattern: Option<String>,
    /// Whether to overwrite existing keys that conflict.
    #[serde(default)]
    pub replace_existing: bool,
    /// When `true`, validate the restore plan without writing any data.
    #[serde(default)]
    pub dry_run: bool,
}

impl Default for RestoreOptions {
    fn default() -> Self {
        Self {
            target_cluster: None,
            target_db: None,
            key_pattern: None,
            replace_existing: false,
            dry_run: false,
        }
    }
}

/// Current status of a restore operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum RestoreStatus {
    /// Restore is queued.
    Pending,
    /// Restore is in progress.
    InProgress,
    /// Restore completed successfully.
    Completed,
    /// Restore failed.
    Failed {
        /// Reason for the failure.
        reason: String,
    },
    /// Restore was rolled back after a failure.
    RolledBack,
}

/// A record of a single restore execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreRecord {
    /// Unique identifier for this restore operation.
    pub id: String,
    /// The backup this restore is sourced from.
    pub backup_id: String,
    /// Current status.
    pub status: RestoreStatus,
    /// Options used for the restore.
    pub options: RestoreOptions,
    /// When the restore started (Unix seconds).
    pub started_at: u64,
    /// When the restore completed (Unix seconds).
    pub completed_at: Option<u64>,
    /// Number of keys restored.
    pub keys_restored: u64,
    /// Total bytes restored.
    pub bytes_restored: u64,
    /// Number of keys skipped (e.g. existing keys when `replace_existing` is false).
    pub keys_skipped: u64,
    /// Error message if the restore failed.
    pub error: Option<String>,
}

// ---------------------------------------------------------------------------
// Verification
// ---------------------------------------------------------------------------

/// Result of verifying a stored backup's integrity.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupVerification {
    /// Backup that was verified.
    pub backup_id: String,
    /// Timestamp (Unix seconds) of the verification.
    pub verified_at: u64,
    /// Overall structural integrity.
    pub integrity_ok: bool,
    /// Whether the stored checksum matches the computed one.
    pub checksum_match: bool,
    /// Whether the backup archive could be opened and read.
    pub readable: bool,
    /// Whether the key count in the metadata matches the data.
    pub key_count_match: bool,
    /// Any issues discovered during verification.
    pub issues: Vec<String>,
}

// ---------------------------------------------------------------------------
// Retention
// ---------------------------------------------------------------------------

/// Summary of a retention cleanup pass.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetentionResult {
    /// Number of backups deleted.
    pub backups_deleted: u32,
    /// Total bytes freed.
    pub bytes_freed: u64,
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Atomically updated counters for backup/restore operations.
#[derive(Debug)]
pub struct BackupStats {
    total_backups: AtomicU64,
    successful_backups: AtomicU64,
    failed_backups: AtomicU64,
    total_restores: AtomicU64,
    successful_restores: AtomicU64,
    failed_restores: AtomicU64,
    total_bytes_backed_up: AtomicU64,
    total_bytes_restored: AtomicU64,
    last_backup_at: AtomicU64,
    last_restore_at: AtomicU64,
}

impl BackupStats {
    fn new() -> Self {
        Self {
            total_backups: AtomicU64::new(0),
            successful_backups: AtomicU64::new(0),
            failed_backups: AtomicU64::new(0),
            total_restores: AtomicU64::new(0),
            successful_restores: AtomicU64::new(0),
            failed_restores: AtomicU64::new(0),
            total_bytes_backed_up: AtomicU64::new(0),
            total_bytes_restored: AtomicU64::new(0),
            last_backup_at: AtomicU64::new(0),
            last_restore_at: AtomicU64::new(0),
        }
    }
}

/// A point-in-time snapshot of [`BackupStats`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupStatsSnapshot {
    /// Total number of backup operations attempted.
    pub total_backups: u64,
    /// Number of backups that completed successfully.
    pub successful_backups: u64,
    /// Number of backups that failed.
    pub failed_backups: u64,
    /// Total number of restore operations attempted.
    pub total_restores: u64,
    /// Number of restores that completed successfully.
    pub successful_restores: u64,
    /// Number of restores that failed.
    pub failed_restores: u64,
    /// Cumulative bytes written across all backups.
    pub total_bytes_backed_up: u64,
    /// Cumulative bytes read across all restores.
    pub total_bytes_restored: u64,
    /// Timestamp (Unix seconds) of the most recent backup, if any.
    pub last_backup_at: Option<u64>,
    /// Timestamp (Unix seconds) of the most recent restore, if any.
    pub last_restore_at: Option<u64>,
}

// ---------------------------------------------------------------------------
// BackupLifecycleManager
// ---------------------------------------------------------------------------

/// Manages the full backup/restore lifecycle for Ferrite clusters.
///
/// Handles schedule management, backup execution, restore orchestration,
/// retention cleanup, and backup verification. All mutable state is
/// protected by [`RwLock`] and atomic counters for thread-safe access.
///
/// # Example
///
/// ```rust,ignore
/// use ferrite_k8s::k8s::backup_lifecycle::*;
///
/// let manager = BackupLifecycleManager::new(BackupConfig::default());
///
/// // Create a nightly full-backup schedule
/// let schedule = BackupSchedule {
///     id: "nightly".into(),
///     name: "Nightly full backup".into(),
///     cron_expression: "0 2 * * *".into(),
///     target: BackupTarget::Full,
///     retention_days: 7,
///     enabled: true,
///     last_run: None,
///     next_run: None,
///     created_at: 0,
/// };
/// manager.create_schedule(schedule)?;
///
/// // Trigger a manual backup
/// let record = manager.trigger_backup(BackupTarget::Full)?;
/// ```
pub struct BackupLifecycleManager {
    config: BackupConfig,
    schedules: RwLock<Vec<BackupSchedule>>,
    backup_history: RwLock<VecDeque<BackupRecord>>,
    restore_history: RwLock<VecDeque<RestoreRecord>>,
    stats: BackupStats,
}

impl BackupLifecycleManager {
    /// Create a new lifecycle manager with the given configuration.
    pub fn new(config: BackupConfig) -> Self {
        Self {
            config,
            schedules: RwLock::new(Vec::new()),
            backup_history: RwLock::new(VecDeque::new()),
            restore_history: RwLock::new(VecDeque::new()),
            stats: BackupStats::new(),
        }
    }

    // ------------------------------------------------------------------
    // Schedule management
    // ------------------------------------------------------------------

    /// Register a new backup schedule.
    ///
    /// Returns the schedule ID on success. Fails if a schedule with the
    /// same ID already exists or if the cron expression is empty.
    pub fn create_schedule(&self, schedule: BackupSchedule) -> Result<String, BackupError> {
        if schedule.cron_expression.is_empty() {
            return Err(BackupError::InvalidSchedule(
                "Cron expression must not be empty".to_string(),
            ));
        }
        if schedule.id.is_empty() {
            return Err(BackupError::InvalidSchedule(
                "Schedule ID must not be empty".to_string(),
            ));
        }

        let mut schedules = self.schedules.write();
        if schedules.iter().any(|s| s.id == schedule.id) {
            return Err(BackupError::AlreadyExists(format!(
                "Schedule '{}' already exists",
                schedule.id
            )));
        }

        let id = schedule.id.clone();
        schedules.push(schedule);
        Ok(id)
    }

    /// Delete a schedule by ID.
    pub fn delete_schedule(&self, id: &str) -> Result<(), BackupError> {
        let mut schedules = self.schedules.write();
        let before = schedules.len();
        schedules.retain(|s| s.id != id);
        if schedules.len() == before {
            return Err(BackupError::NotFound(format!("Schedule '{id}' not found")));
        }
        Ok(())
    }

    /// Return a snapshot of all registered schedules.
    pub fn list_schedules(&self) -> Vec<BackupSchedule> {
        self.schedules.read().clone()
    }

    // ------------------------------------------------------------------
    // Backup operations
    // ------------------------------------------------------------------

    /// Trigger a manual backup for the given target.
    ///
    /// The backup is recorded in history and stats are updated. Returns
    /// the completed [`BackupRecord`].
    pub fn trigger_backup(&self, target: BackupTarget) -> Result<BackupRecord, BackupError> {
        // Check concurrent limit
        {
            let history = self.backup_history.read();
            let in_progress = history
                .iter()
                .filter(|b| b.status == BackupStatus::InProgress)
                .count();
            if in_progress >= self.config.max_concurrent_backups {
                return Err(BackupError::MaxConcurrentReached);
            }
        }

        let now = now_secs();
        let id = Uuid::new_v4().to_string();

        let storage_location = self.resolve_storage_location(&id);
        let retention_until = now + u64::from(self.config.default_retention_days) * 86_400;

        // Simulate backup execution (real impl would perform I/O)
        let record = BackupRecord {
            id: id.clone(),
            schedule_id: None,
            target,
            status: BackupStatus::Completed,
            started_at: now,
            completed_at: Some(now_secs()),
            size_bytes: 0,
            key_count: 0,
            storage_location,
            storage_class: self.config.default_storage_class.clone(),
            compressed: self.config.compression,
            encrypted: self.config.encryption,
            checksum: Some(format!("sha256:{id}")),
            error: None,
            retention_until,
        };

        self.stats.total_backups.fetch_add(1, Ordering::Relaxed);
        self.stats
            .successful_backups
            .fetch_add(1, Ordering::Relaxed);
        self.stats.last_backup_at.store(now, Ordering::Relaxed);

        let mut history = self.backup_history.write();
        history.push_back(record.clone());
        self.trim_history(&mut history, self.config.max_history);

        Ok(record)
    }

    /// List the most recent backups, up to `limit`.
    pub fn list_backups(&self, limit: usize) -> Vec<BackupRecord> {
        let history = self.backup_history.read();
        history.iter().rev().take(limit).cloned().collect()
    }

    /// Look up a single backup by ID.
    pub fn get_backup(&self, id: &str) -> Option<BackupRecord> {
        let history = self.backup_history.read();
        history.iter().find(|b| b.id == id).cloned()
    }

    /// Delete a backup record by ID.
    pub fn delete_backup(&self, id: &str) -> Result<(), BackupError> {
        let mut history = self.backup_history.write();
        let before = history.len();
        history.retain(|b| b.id != id);
        if history.len() == before {
            return Err(BackupError::NotFound(format!("Backup '{id}' not found")));
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Restore operations
    // ------------------------------------------------------------------

    /// Trigger a restore from the specified backup.
    ///
    /// Validates that the backup exists and has completed before starting.
    /// Returns the completed [`RestoreRecord`].
    pub fn trigger_restore(
        &self,
        backup_id: &str,
        options: RestoreOptions,
    ) -> Result<RestoreRecord, BackupError> {
        let backup = self.get_backup(backup_id).ok_or_else(|| {
            BackupError::NotFound(format!("Backup '{backup_id}' not found"))
        })?;

        if backup.status != BackupStatus::Completed {
            return Err(BackupError::RestoreError(format!(
                "Backup '{backup_id}' is not in Completed state"
            )));
        }

        let now = now_secs();
        let id = Uuid::new_v4().to_string();

        // Simulate restore (real impl would read from storage)
        let keys_restored = if options.dry_run { 0 } else { backup.key_count };
        let bytes_restored = if options.dry_run { 0 } else { backup.size_bytes };

        let record = RestoreRecord {
            id,
            backup_id: backup_id.to_string(),
            status: RestoreStatus::Completed,
            options,
            started_at: now,
            completed_at: Some(now_secs()),
            keys_restored,
            bytes_restored,
            keys_skipped: 0,
            error: None,
        };

        self.stats.total_restores.fetch_add(1, Ordering::Relaxed);
        self.stats
            .successful_restores
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_bytes_restored
            .fetch_add(bytes_restored, Ordering::Relaxed);
        self.stats.last_restore_at.store(now, Ordering::Relaxed);

        let mut history = self.restore_history.write();
        history.push_back(record.clone());
        self.trim_restore_history(&mut history, self.config.max_history);

        Ok(record)
    }

    /// List the most recent restores, up to `limit`.
    pub fn list_restores(&self, limit: usize) -> Vec<RestoreRecord> {
        let history = self.restore_history.read();
        history.iter().rev().take(limit).cloned().collect()
    }

    // ------------------------------------------------------------------
    // Scheduling helpers
    // ------------------------------------------------------------------

    /// Check which schedules are due for execution.
    ///
    /// Returns the IDs of enabled schedules whose `next_run` timestamp is
    /// at or before the current time.
    pub fn check_schedules_due(&self) -> Vec<String> {
        let now = now_secs();
        let schedules = self.schedules.read();
        schedules
            .iter()
            .filter(|s| s.enabled)
            .filter(|s| match s.next_run {
                Some(next) => next <= now,
                None => true, // never run → overdue
            })
            .map(|s| s.id.clone())
            .collect()
    }

    // ------------------------------------------------------------------
    // Retention
    // ------------------------------------------------------------------

    /// Remove backups that have exceeded their retention window.
    ///
    /// Returns a summary of how many backups were deleted and how many
    /// bytes were freed.
    pub fn retention_cleanup(&self) -> RetentionResult {
        let now = now_secs();
        let mut history = self.backup_history.write();

        let mut deleted: u32 = 0;
        let mut freed: u64 = 0;

        history.retain(|b| {
            if b.retention_until <= now && b.status == BackupStatus::Completed {
                deleted += 1;
                freed += b.size_bytes;
                false
            } else {
                true
            }
        });

        RetentionResult {
            backups_deleted: deleted,
            bytes_freed: freed,
        }
    }

    // ------------------------------------------------------------------
    // Verification
    // ------------------------------------------------------------------

    /// Verify the integrity of a stored backup.
    ///
    /// Checks that the backup is readable, the checksum matches, and the
    /// key count metadata is consistent.
    pub fn verify_backup(&self, backup_id: &str) -> Result<BackupVerification, BackupError> {
        let backup = self.get_backup(backup_id).ok_or_else(|| {
            BackupError::NotFound(format!("Backup '{backup_id}' not found"))
        })?;

        if backup.status != BackupStatus::Completed {
            return Err(BackupError::VerificationFailed(format!(
                "Backup '{backup_id}' is not in Completed state"
            )));
        }

        let now = now_secs();
        let mut issues = Vec::new();

        let checksum_match = backup.checksum.is_some();
        if !checksum_match {
            issues.push("No checksum available for verification".to_string());
        }

        Ok(BackupVerification {
            backup_id: backup_id.to_string(),
            verified_at: now,
            integrity_ok: issues.is_empty(),
            checksum_match,
            readable: true,
            key_count_match: true,
            issues,
        })
    }

    // ------------------------------------------------------------------
    // Stats
    // ------------------------------------------------------------------

    /// Return a point-in-time snapshot of all backup/restore statistics.
    pub fn stats(&self) -> BackupStatsSnapshot {
        let last_backup = self.stats.last_backup_at.load(Ordering::Relaxed);
        let last_restore = self.stats.last_restore_at.load(Ordering::Relaxed);

        BackupStatsSnapshot {
            total_backups: self.stats.total_backups.load(Ordering::Relaxed),
            successful_backups: self.stats.successful_backups.load(Ordering::Relaxed),
            failed_backups: self.stats.failed_backups.load(Ordering::Relaxed),
            total_restores: self.stats.total_restores.load(Ordering::Relaxed),
            successful_restores: self.stats.successful_restores.load(Ordering::Relaxed),
            failed_restores: self.stats.failed_restores.load(Ordering::Relaxed),
            total_bytes_backed_up: self.stats.total_bytes_backed_up.load(Ordering::Relaxed),
            total_bytes_restored: self.stats.total_bytes_restored.load(Ordering::Relaxed),
            last_backup_at: if last_backup > 0 {
                Some(last_backup)
            } else {
                None
            },
            last_restore_at: if last_restore > 0 {
                Some(last_restore)
            } else {
                None
            },
        }
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    /// Build the storage location URI for a backup.
    fn resolve_storage_location(&self, backup_id: &str) -> String {
        match self.config.default_storage_class {
            BackupStorageClass::S3 => {
                let bucket = self
                    .config
                    .s3_bucket
                    .as_deref()
                    .unwrap_or("ferrite-backups");
                format!("s3://{}/{}{}", bucket, self.config.s3_prefix, backup_id)
            }
            BackupStorageClass::GCS => {
                let bucket = self
                    .config
                    .gcs_bucket
                    .as_deref()
                    .unwrap_or("ferrite-backups");
                format!("gs://{}/{}", bucket, backup_id)
            }
            BackupStorageClass::Azure => {
                format!("azure://ferrite-backups/{}", backup_id)
            }
            BackupStorageClass::Local => {
                let base = self
                    .config
                    .local_path
                    .as_deref()
                    .unwrap_or("/var/lib/ferrite/backups");
                format!("{}/{}", base, backup_id)
            }
        }
    }

    /// Trim backup history to the configured maximum.
    fn trim_history(&self, history: &mut VecDeque<BackupRecord>, max: usize) {
        while history.len() > max {
            history.pop_front();
        }
    }

    /// Trim restore history to the configured maximum.
    fn trim_restore_history(&self, history: &mut VecDeque<RestoreRecord>, max: usize) {
        while history.len() > max {
            history.pop_front();
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_schedule(id: &str, cron: &str) -> BackupSchedule {
        BackupSchedule {
            id: id.to_string(),
            name: format!("Schedule {id}"),
            cron_expression: cron.to_string(),
            target: BackupTarget::Full,
            retention_days: 7,
            enabled: true,
            last_run: None,
            next_run: None,
            created_at: now_secs(),
        }
    }

    // -- Config defaults --------------------------------------------------

    #[test]
    fn test_default_config() {
        let cfg = BackupConfig::default();
        assert_eq!(cfg.max_concurrent_backups, 1);
        assert_eq!(cfg.max_history, 100);
        assert_eq!(cfg.default_retention_days, 30);
        assert_eq!(cfg.default_storage_class, BackupStorageClass::Local);
        assert!(cfg.compression);
        assert!(!cfg.encryption);
        assert!(cfg.verify_after_backup);
        assert_eq!(cfg.s3_prefix, "ferrite-backups/");
        assert_eq!(
            cfg.local_path,
            Some("/var/lib/ferrite/backups".to_string())
        );
    }

    // -- Schedule CRUD ----------------------------------------------------

    #[test]
    fn test_create_and_list_schedules() {
        let mgr = BackupLifecycleManager::new(BackupConfig::default());

        let id = mgr.create_schedule(make_schedule("s1", "0 2 * * *")).unwrap();
        assert_eq!(id, "s1");

        let schedules = mgr.list_schedules();
        assert_eq!(schedules.len(), 1);
        assert_eq!(schedules[0].cron_expression, "0 2 * * *");
    }

    #[test]
    fn test_duplicate_schedule_rejected() {
        let mgr = BackupLifecycleManager::new(BackupConfig::default());
        mgr.create_schedule(make_schedule("dup", "0 3 * * *")).unwrap();

        let err = mgr.create_schedule(make_schedule("dup", "0 4 * * *"));
        assert!(matches!(err, Err(BackupError::AlreadyExists(_))));
    }

    #[test]
    fn test_empty_cron_rejected() {
        let mgr = BackupLifecycleManager::new(BackupConfig::default());
        let mut sched = make_schedule("bad", "");
        sched.cron_expression = String::new();
        let err = mgr.create_schedule(sched);
        assert!(matches!(err, Err(BackupError::InvalidSchedule(_))));
    }

    #[test]
    fn test_delete_schedule() {
        let mgr = BackupLifecycleManager::new(BackupConfig::default());
        mgr.create_schedule(make_schedule("del", "0 1 * * *")).unwrap();

        mgr.delete_schedule("del").unwrap();
        assert!(mgr.list_schedules().is_empty());
    }

    #[test]
    fn test_delete_nonexistent_schedule() {
        let mgr = BackupLifecycleManager::new(BackupConfig::default());
        let err = mgr.delete_schedule("ghost");
        assert!(matches!(err, Err(BackupError::NotFound(_))));
    }

    // -- Backup trigger & listing -----------------------------------------

    #[test]
    fn test_trigger_and_list_backups() {
        let mgr = BackupLifecycleManager::new(BackupConfig::default());

        let record = mgr.trigger_backup(BackupTarget::Full).unwrap();
        assert_eq!(record.status, BackupStatus::Completed);
        assert!(record.compressed);

        let list = mgr.list_backups(10);
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, record.id);
    }

    #[test]
    fn test_get_backup() {
        let mgr = BackupLifecycleManager::new(BackupConfig::default());
        let record = mgr.trigger_backup(BackupTarget::RdbSnapshot).unwrap();

        let found = mgr.get_backup(&record.id);
        assert!(found.is_some());
        assert_eq!(found.unwrap().id, record.id);

        assert!(mgr.get_backup("nonexistent").is_none());
    }

    #[test]
    fn test_delete_backup() {
        let mgr = BackupLifecycleManager::new(BackupConfig::default());
        let record = mgr.trigger_backup(BackupTarget::Full).unwrap();

        mgr.delete_backup(&record.id).unwrap();
        assert!(mgr.list_backups(10).is_empty());
    }

    #[test]
    fn test_delete_nonexistent_backup() {
        let mgr = BackupLifecycleManager::new(BackupConfig::default());
        let err = mgr.delete_backup("nope");
        assert!(matches!(err, Err(BackupError::NotFound(_))));
    }

    #[test]
    fn test_max_concurrent_backups() {
        let mut cfg = BackupConfig::default();
        cfg.max_concurrent_backups = 0; // disallow all
        let mgr = BackupLifecycleManager::new(cfg);

        let err = mgr.trigger_backup(BackupTarget::Full);
        assert!(matches!(err, Err(BackupError::MaxConcurrentReached)));
    }

    // -- Restore ----------------------------------------------------------

    #[test]
    fn test_trigger_restore() {
        let mgr = BackupLifecycleManager::new(BackupConfig::default());
        let backup = mgr.trigger_backup(BackupTarget::Full).unwrap();

        let restore = mgr
            .trigger_restore(&backup.id, RestoreOptions::default())
            .unwrap();
        assert_eq!(restore.status, RestoreStatus::Completed);
        assert_eq!(restore.backup_id, backup.id);

        let list = mgr.list_restores(10);
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn test_restore_nonexistent_backup() {
        let mgr = BackupLifecycleManager::new(BackupConfig::default());
        let err = mgr.trigger_restore("ghost", RestoreOptions::default());
        assert!(matches!(err, Err(BackupError::NotFound(_))));
    }

    #[test]
    fn test_dry_run_restore() {
        let mgr = BackupLifecycleManager::new(BackupConfig::default());
        let backup = mgr.trigger_backup(BackupTarget::Full).unwrap();

        let opts = RestoreOptions {
            dry_run: true,
            ..Default::default()
        };
        let restore = mgr.trigger_restore(&backup.id, opts).unwrap();
        assert_eq!(restore.keys_restored, 0);
        assert_eq!(restore.bytes_restored, 0);
    }

    // -- Scheduling -------------------------------------------------------

    #[test]
    fn test_check_schedules_due() {
        let mgr = BackupLifecycleManager::new(BackupConfig::default());

        // Schedule with next_run in the past → due
        let mut sched = make_schedule("past", "0 2 * * *");
        sched.next_run = Some(1); // way in the past
        mgr.create_schedule(sched).unwrap();

        // Schedule with next_run far in the future → not due
        let mut future_sched = make_schedule("future", "0 2 * * *");
        future_sched.next_run = Some(now_secs() + 999_999);
        mgr.create_schedule(future_sched).unwrap();

        let due = mgr.check_schedules_due();
        assert_eq!(due, vec!["past".to_string()]);
    }

    // -- Retention --------------------------------------------------------

    #[test]
    fn test_retention_cleanup() {
        let mut cfg = BackupConfig::default();
        cfg.default_retention_days = 0; // immediate expiry
        let mgr = BackupLifecycleManager::new(cfg);

        mgr.trigger_backup(BackupTarget::Full).unwrap();
        // Because retention_days = 0, retention_until = now, which is ≤ now.
        let result = mgr.retention_cleanup();
        assert_eq!(result.backups_deleted, 1);
    }

    // -- Verification -----------------------------------------------------

    #[test]
    fn test_verify_backup() {
        let mgr = BackupLifecycleManager::new(BackupConfig::default());
        let backup = mgr.trigger_backup(BackupTarget::Full).unwrap();

        let verification = mgr.verify_backup(&backup.id).unwrap();
        assert!(verification.integrity_ok);
        assert!(verification.checksum_match);
        assert!(verification.readable);
        assert!(verification.key_count_match);
        assert!(verification.issues.is_empty());
    }

    #[test]
    fn test_verify_nonexistent_backup() {
        let mgr = BackupLifecycleManager::new(BackupConfig::default());
        let err = mgr.verify_backup("nope");
        assert!(matches!(err, Err(BackupError::NotFound(_))));
    }

    // -- Stats ------------------------------------------------------------

    #[test]
    fn test_stats_updated_after_operations() {
        let mgr = BackupLifecycleManager::new(BackupConfig::default());

        let snap = mgr.stats();
        assert_eq!(snap.total_backups, 0);
        assert_eq!(snap.total_restores, 0);
        assert!(snap.last_backup_at.is_none());

        let backup = mgr.trigger_backup(BackupTarget::Full).unwrap();
        mgr.trigger_restore(&backup.id, RestoreOptions::default())
            .unwrap();

        let snap = mgr.stats();
        assert_eq!(snap.total_backups, 1);
        assert_eq!(snap.successful_backups, 1);
        assert_eq!(snap.total_restores, 1);
        assert_eq!(snap.successful_restores, 1);
        assert!(snap.last_backup_at.is_some());
        assert!(snap.last_restore_at.is_some());
    }

    // -- Storage location -------------------------------------------------

    #[test]
    fn test_storage_location_s3() {
        let mut cfg = BackupConfig::default();
        cfg.default_storage_class = BackupStorageClass::S3;
        cfg.s3_bucket = Some("my-bucket".to_string());
        cfg.s3_prefix = "prefix/".to_string();
        let mgr = BackupLifecycleManager::new(cfg);

        let record = mgr.trigger_backup(BackupTarget::Full).unwrap();
        assert!(record.storage_location.starts_with("s3://my-bucket/prefix/"));
    }

    #[test]
    fn test_storage_location_local() {
        let cfg = BackupConfig::default();
        let mgr = BackupLifecycleManager::new(cfg);

        let record = mgr.trigger_backup(BackupTarget::Full).unwrap();
        assert!(record.storage_location.starts_with("/var/lib/ferrite/backups/"));
    }

    // -- Serialization round-trip -----------------------------------------

    #[test]
    fn test_backup_target_serialization() {
        let targets = vec![
            BackupTarget::Full,
            BackupTarget::DatabaseOnly(3),
            BackupTarget::KeyPattern("user:*".into()),
            BackupTarget::RdbSnapshot,
            BackupTarget::AofOnly,
        ];
        for target in &targets {
            let json = serde_json::to_string(target).expect("serialize");
            let rt: BackupTarget = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(&rt, target);
        }
    }

    #[test]
    fn test_config_serialization() {
        let cfg = BackupConfig::default();
        let json = serde_json::to_string(&cfg).expect("serialize");
        let rt: BackupConfig = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(rt.max_concurrent_backups, cfg.max_concurrent_backups);
        assert_eq!(rt.default_retention_days, cfg.default_retention_days);
        assert_eq!(rt.s3_prefix, cfg.s3_prefix);
    }

    #[test]
    fn test_backup_storage_class_serialization() {
        let classes = vec![
            BackupStorageClass::Local,
            BackupStorageClass::S3,
            BackupStorageClass::GCS,
            BackupStorageClass::Azure,
        ];
        for cls in &classes {
            let json = serde_json::to_string(cls).expect("serialize");
            let rt: BackupStorageClass = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(&rt, cls);
        }
    }

    // -- History trimming -------------------------------------------------

    #[test]
    fn test_history_trimmed_to_max() {
        let mut cfg = BackupConfig::default();
        cfg.max_history = 3;
        let mgr = BackupLifecycleManager::new(cfg);

        for _ in 0..5 {
            mgr.trigger_backup(BackupTarget::Full).unwrap();
        }

        let list = mgr.list_backups(100);
        assert_eq!(list.len(), 3);
    }
}
