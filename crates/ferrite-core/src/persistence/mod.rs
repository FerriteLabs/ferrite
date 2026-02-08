//! Persistence module for Ferrite
//!
//! This module implements durability features including AOF (Append-Only File),
//! RDB snapshots, checkpointing, and backup/restore capabilities.

mod aof;
/// Backup and restore capabilities with local and cloud storage backends
pub mod backup;
mod checkpoint;
mod rdb;

pub use aof::{replay_aof, AofCommand, AofEntry, AofManager, AofReader, AofWriter};
#[cfg(feature = "cloud")]
pub use backup::CloudBackupStorage;
pub use backup::{
    BackupConfig, BackupInfo, BackupManager, BackupResult, BackupStorage, BackupType,
    LocalBackupStorage, RestoreOptions, RestoreResult,
};
pub use checkpoint::{
    CheckpointConfig, CheckpointManager, CheckpointMetadata, CheckpointRecovery, CheckpointResult,
    CheckpointScheduler,
};
pub use rdb::{generate_rdb, load_rdb, RdbEntry, RdbReader, RdbWriter};
