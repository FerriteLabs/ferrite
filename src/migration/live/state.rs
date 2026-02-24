//! Migration state tracking for zero-downtime live migration.

#![forbid(unsafe_code)]

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Current status of the migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationStatus {
    /// Migration is pending start.
    Pending,
    /// Migration is actively running.
    Running,
    /// Migration is paused.
    Paused,
    /// Migration completed successfully.
    Completed,
    /// Migration failed with the given reason.
    Failed(String),
    /// Migration was rolled back.
    RolledBack,
}

impl std::fmt::Display for MigrationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Running => write!(f, "running"),
            Self::Paused => write!(f, "paused"),
            Self::Completed => write!(f, "completed"),
            Self::Failed(reason) => write!(f, "failed: {}", reason),
            Self::RolledBack => write!(f, "rolled_back"),
        }
    }
}

/// Current phase within the migration pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationPhase {
    /// Bulk-copying all keys from source to target.
    BulkSync,
    /// Streaming changes from source via keyspace notifications.
    ContinuousReplication,
    /// Verifying consistency between source and target.
    Verifying,
    /// Switching traffic from source to target.
    CuttingOver,
    /// Migration is done.
    Done,
}

impl std::fmt::Display for MigrationPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BulkSync => write!(f, "bulk_sync"),
            Self::ContinuousReplication => write!(f, "continuous_replication"),
            Self::Verifying => write!(f, "verifying"),
            Self::CuttingOver => write!(f, "cutting_over"),
            Self::Done => write!(f, "done"),
        }
    }
}

/// Full migration state snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationState {
    /// Unique migration identifier.
    pub id: String,
    /// Source Redis URI.
    pub source_uri: String,
    /// Overall status.
    pub status: MigrationStatus,
    /// Current pipeline phase.
    pub phase: MigrationPhase,
    /// Number of keys synced so far.
    pub keys_synced: u64,
    /// Total keys in source (if known).
    pub keys_total: Option<u64>,
    /// Bytes transferred.
    pub bytes_synced: u64,
    /// Current replication lag in milliseconds.
    pub replication_lag_ms: u64,
    /// Accumulated errors.
    pub errors: Vec<String>,
    /// When the migration was started.
    pub started_at: DateTime<Utc>,
    /// When the migration completed (if finished).
    pub completed_at: Option<DateTime<Utc>>,
}

impl MigrationState {
    /// Create a new pending migration state.
    pub fn new(id: String, source_uri: String) -> Self {
        Self {
            id,
            source_uri,
            status: MigrationStatus::Pending,
            phase: MigrationPhase::BulkSync,
            keys_synced: 0,
            keys_total: None,
            bytes_synced: 0,
            replication_lag_ms: 0,
            errors: Vec::new(),
            started_at: Utc::now(),
            completed_at: None,
        }
    }

    /// Mark the migration as completed.
    pub fn mark_completed(&mut self) {
        self.status = MigrationStatus::Completed;
        self.phase = MigrationPhase::Done;
        self.completed_at = Some(Utc::now());
    }

    /// Mark the migration as failed.
    pub fn mark_failed(&mut self, reason: String) {
        self.status = MigrationStatus::Failed(reason.clone());
        self.errors.push(reason);
        self.completed_at = Some(Utc::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_state_new() {
        let state = MigrationState::new("m1".into(), "redis://localhost:6379".into());
        assert_eq!(state.status, MigrationStatus::Pending);
        assert_eq!(state.phase, MigrationPhase::BulkSync);
        assert_eq!(state.keys_synced, 0);
        assert!(state.completed_at.is_none());
    }

    #[test]
    fn test_mark_completed() {
        let mut state = MigrationState::new("m1".into(), "redis://localhost:6379".into());
        state.mark_completed();
        assert_eq!(state.status, MigrationStatus::Completed);
        assert_eq!(state.phase, MigrationPhase::Done);
        assert!(state.completed_at.is_some());
    }

    #[test]
    fn test_mark_failed() {
        let mut state = MigrationState::new("m1".into(), "redis://localhost:6379".into());
        state.mark_failed("connection lost".into());
        assert!(matches!(state.status, MigrationStatus::Failed(_)));
        assert_eq!(state.errors.len(), 1);
    }

    #[test]
    fn test_status_display() {
        assert_eq!(MigrationStatus::Running.to_string(), "running");
        assert_eq!(
            MigrationStatus::Failed("oops".into()).to_string(),
            "failed: oops"
        );
    }

    #[test]
    fn test_phase_display() {
        assert_eq!(MigrationPhase::BulkSync.to_string(), "bulk_sync");
        assert_eq!(MigrationPhase::Done.to_string(), "done");
    }
}
