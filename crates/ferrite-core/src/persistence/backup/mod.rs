//! Backup and Restore Module
//!
//! This module provides backup and restore capabilities for Ferrite:
//! - Full database backups (RDB-like snapshots)
//! - Incremental backups using AOF
//! - Local and cloud storage backends (S3, GCS, Azure)
//! - Point-in-time recovery
//! - Progress tracking for large backups

mod manager;
mod storage;

pub use manager::{
    BackupChain, BackupConfig, BackupError, BackupInfo, BackupManager, BackupResult, BackupType,
    RestoreOptions, RestoreResult,
};
#[cfg(feature = "cloud")]
pub use storage::{
    create_cloud_backup_storage, create_cloud_backup_storage_with_progress,
    create_s3_backup_storage, CloudBackupStorage, ProgressCloudBackupStorage, S3Config,
};
pub use storage::{
    BackupStorage, BackupStorageError, LocalBackupStorage, LoggingProgressCallback,
    ProgressCallback,
};
