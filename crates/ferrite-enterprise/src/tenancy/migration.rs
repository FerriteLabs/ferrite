//! Tenant Migration
//!
//! Zero-downtime tenant migration between clusters.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

use super::TenancyError;

/// Tenant migrator for moving tenants between clusters
pub struct TenantMigrator {
    /// Active migrations
    migrations: RwLock<HashMap<String, Arc<Migration>>>,
    /// Completed migrations
    completed: RwLock<Vec<MigrationRecord>>,
    /// Statistics
    stats: MigratorStats,
}

impl TenantMigrator {
    /// Create a new migrator
    pub fn new() -> Self {
        Self {
            migrations: RwLock::new(HashMap::new()),
            completed: RwLock::new(Vec::new()),
            stats: MigratorStats::default(),
        }
    }

    /// Start a migration
    pub async fn start_migration(&self, plan: MigrationPlan) -> Result<String, TenancyError> {
        let migration_id = format!("migration-{}-{}", plan.tenant_id, current_timestamp_ms());

        let migration = Arc::new(Migration::new(migration_id.clone(), plan));

        let mut migrations = self.migrations.write().await;
        migrations.insert(migration_id.clone(), migration);

        self.stats
            .migrations_started
            .fetch_add(1, Ordering::Relaxed);

        Ok(migration_id)
    }

    /// Get migration status
    pub async fn get_status(&self, migration_id: &str) -> Option<MigrationProgress> {
        let migrations = self.migrations.read().await;
        migrations.get(migration_id).map(|m| m.progress())
    }

    /// Cancel a migration
    pub async fn cancel(&self, migration_id: &str) -> Result<(), TenancyError> {
        let migrations = self.migrations.read().await;
        let migration = migrations
            .get(migration_id)
            .ok_or_else(|| TenancyError::Migration("Migration not found".to_string()))?;

        migration.cancel();
        self.stats
            .migrations_cancelled
            .fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Complete a migration
    pub async fn complete(&self, migration_id: &str) -> Result<(), TenancyError> {
        let mut migrations = self.migrations.write().await;
        let migration = migrations
            .remove(migration_id)
            .ok_or_else(|| TenancyError::Migration("Migration not found".to_string()))?;

        migration.complete();

        // Record completion
        let mut completed = self.completed.write().await;
        completed.push(MigrationRecord {
            migration_id: migration_id.to_string(),
            tenant_id: migration.plan.tenant_id.clone(),
            source: migration.plan.source_cluster.clone(),
            target: migration.plan.target_cluster.clone(),
            started_at: migration.started_at,
            completed_at: current_timestamp_ms(),
            bytes_transferred: migration.bytes_transferred.load(Ordering::Relaxed),
            keys_transferred: migration.keys_transferred.load(Ordering::Relaxed),
            status: MigrationStatus::Completed,
        });

        self.stats
            .migrations_completed
            .fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Get all active migrations
    pub async fn list_active(&self) -> Vec<MigrationProgress> {
        let migrations = self.migrations.read().await;
        migrations.values().map(|m| m.progress()).collect()
    }

    /// Get migration history
    pub async fn history(&self, limit: usize) -> Vec<MigrationRecord> {
        let completed = self.completed.read().await;
        completed.iter().rev().take(limit).cloned().collect()
    }

    /// Get statistics
    pub fn stats(&self) -> MigratorStatsSnapshot {
        MigratorStatsSnapshot {
            migrations_started: self.stats.migrations_started.load(Ordering::Relaxed),
            migrations_completed: self.stats.migrations_completed.load(Ordering::Relaxed),
            migrations_cancelled: self.stats.migrations_cancelled.load(Ordering::Relaxed),
            migrations_failed: self.stats.migrations_failed.load(Ordering::Relaxed),
        }
    }
}

impl Default for TenantMigrator {
    fn default() -> Self {
        Self::new()
    }
}

/// Migration plan
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationPlan {
    /// Tenant to migrate
    pub tenant_id: String,
    /// Source cluster
    pub source_cluster: String,
    /// Target cluster
    pub target_cluster: String,
    /// Migration mode
    pub mode: MigrationMode,
    /// Whether to verify after migration
    pub verify: bool,
    /// Whether to keep source data after migration
    pub keep_source: bool,
    /// Bandwidth limit (bytes per second, 0 = unlimited)
    pub bandwidth_limit: u64,
    /// Batch size for key transfer
    pub batch_size: usize,
}

impl MigrationPlan {
    /// Create a new migration plan
    pub fn new(
        tenant_id: impl Into<String>,
        source: impl Into<String>,
        target: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            source_cluster: source.into(),
            target_cluster: target.into(),
            mode: MigrationMode::LiveSync,
            verify: true,
            keep_source: false,
            bandwidth_limit: 0,
            batch_size: 1000,
        }
    }

    /// Set migration mode
    pub fn with_mode(mut self, mode: MigrationMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set verification
    pub fn with_verify(mut self, verify: bool) -> Self {
        self.verify = verify;
        self
    }
}

/// Migration mode
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationMode {
    /// Offline migration (tenant suspended during migration)
    Offline,
    /// Live sync (sync changes in real-time)
    LiveSync,
    /// Snapshot-based (point-in-time snapshot)
    Snapshot,
}

/// Active migration
pub struct Migration {
    /// Migration ID
    pub migration_id: String,
    /// Migration plan
    pub plan: MigrationPlan,
    /// Started timestamp
    pub started_at: u64,
    /// Current status
    status: RwLock<MigrationStatus>,
    /// Phase
    phase: RwLock<MigrationPhase>,
    /// Keys transferred
    keys_transferred: AtomicU64,
    /// Bytes transferred
    bytes_transferred: AtomicU64,
    /// Total keys to transfer
    total_keys: AtomicU64,
    /// Total bytes to transfer
    total_bytes: AtomicU64,
    /// Errors encountered
    errors: AtomicU64,
    /// Last error message
    last_error: RwLock<Option<String>>,
}

impl Migration {
    /// Create a new migration
    pub fn new(migration_id: String, plan: MigrationPlan) -> Self {
        Self {
            migration_id,
            plan,
            started_at: current_timestamp_ms(),
            status: RwLock::new(MigrationStatus::Pending),
            phase: RwLock::new(MigrationPhase::Preparing),
            keys_transferred: AtomicU64::new(0),
            bytes_transferred: AtomicU64::new(0),
            total_keys: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            last_error: RwLock::new(None),
        }
    }

    /// Get current progress
    pub fn progress(&self) -> MigrationProgress {
        let status = tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async { self.status.read().await.clone() })
        });

        let phase = tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async { self.phase.read().await.clone() })
        });

        let keys_transferred = self.keys_transferred.load(Ordering::Relaxed);
        let total_keys = self.total_keys.load(Ordering::Relaxed);
        let bytes_transferred = self.bytes_transferred.load(Ordering::Relaxed);
        let total_bytes = self.total_bytes.load(Ordering::Relaxed);

        let keys_percent = if total_keys == 0 {
            0.0
        } else {
            (keys_transferred as f64 / total_keys as f64) * 100.0
        };

        let bytes_percent = if total_bytes == 0 {
            0.0
        } else {
            (bytes_transferred as f64 / total_bytes as f64) * 100.0
        };

        let elapsed_ms = current_timestamp_ms() - self.started_at;
        let bytes_per_second = if elapsed_ms == 0 {
            0
        } else {
            (bytes_transferred * 1000) / elapsed_ms
        };

        let eta_ms = if bytes_per_second == 0 {
            None
        } else {
            let remaining_bytes = total_bytes.saturating_sub(bytes_transferred);
            Some((remaining_bytes * 1000) / bytes_per_second)
        };

        MigrationProgress {
            migration_id: self.migration_id.clone(),
            tenant_id: self.plan.tenant_id.clone(),
            status,
            phase,
            keys_transferred,
            total_keys,
            keys_percent,
            bytes_transferred,
            total_bytes,
            bytes_percent,
            bytes_per_second,
            elapsed_ms,
            eta_ms,
            errors: self.errors.load(Ordering::Relaxed),
        }
    }

    /// Cancel the migration
    pub fn cancel(&self) {
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let mut status = self.status.write().await;
                *status = MigrationStatus::Cancelled;
            });
        });
    }

    /// Complete the migration
    pub fn complete(&self) {
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let mut status = self.status.write().await;
                *status = MigrationStatus::Completed;
                let mut phase = self.phase.write().await;
                *phase = MigrationPhase::Complete;
            });
        });
    }

    /// Set phase
    pub async fn set_phase(&self, new_phase: MigrationPhase) {
        let mut phase = self.phase.write().await;
        *phase = new_phase;
    }

    /// Set total keys
    pub fn set_total_keys(&self, total: u64) {
        self.total_keys.store(total, Ordering::Relaxed);
    }

    /// Set total bytes
    pub fn set_total_bytes(&self, total: u64) {
        self.total_bytes.store(total, Ordering::Relaxed);
    }

    /// Record transferred data
    pub fn record_transfer(&self, keys: u64, bytes: u64) {
        self.keys_transferred.fetch_add(keys, Ordering::Relaxed);
        self.bytes_transferred.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record an error
    pub async fn record_error(&self, error: impl Into<String>) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        let mut last_error = self.last_error.write().await;
        *last_error = Some(error.into());
    }
}

/// Migration status
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationStatus {
    /// Pending start
    Pending,
    /// In progress
    InProgress,
    /// Completed successfully
    Completed,
    /// Cancelled
    Cancelled,
    /// Failed
    Failed,
}

/// Migration phase
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationPhase {
    /// Preparing (analyzing source data)
    Preparing,
    /// Initial sync (bulk transfer)
    InitialSync,
    /// Live sync (applying changes)
    LiveSync,
    /// Verifying (checking data integrity)
    Verifying,
    /// Switching (cutover)
    Switching,
    /// Cleanup (removing source data)
    Cleanup,
    /// Complete
    Complete,
}

/// Migration progress
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationProgress {
    /// Migration ID
    pub migration_id: String,
    /// Tenant ID
    pub tenant_id: String,
    /// Current status
    pub status: MigrationStatus,
    /// Current phase
    pub phase: MigrationPhase,
    /// Keys transferred
    pub keys_transferred: u64,
    /// Total keys
    pub total_keys: u64,
    /// Keys percentage
    pub keys_percent: f64,
    /// Bytes transferred
    pub bytes_transferred: u64,
    /// Total bytes
    pub total_bytes: u64,
    /// Bytes percentage
    pub bytes_percent: f64,
    /// Transfer speed (bytes/second)
    pub bytes_per_second: u64,
    /// Elapsed time (milliseconds)
    pub elapsed_ms: u64,
    /// Estimated time remaining (milliseconds)
    pub eta_ms: Option<u64>,
    /// Error count
    pub errors: u64,
}

/// Migration record (for history)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationRecord {
    /// Migration ID
    pub migration_id: String,
    /// Tenant ID
    pub tenant_id: String,
    /// Source cluster
    pub source: String,
    /// Target cluster
    pub target: String,
    /// Started timestamp
    pub started_at: u64,
    /// Completed timestamp
    pub completed_at: u64,
    /// Bytes transferred
    pub bytes_transferred: u64,
    /// Keys transferred
    pub keys_transferred: u64,
    /// Final status
    pub status: MigrationStatus,
}

/// Migrator statistics
#[derive(Default)]
struct MigratorStats {
    migrations_started: AtomicU64,
    migrations_completed: AtomicU64,
    migrations_cancelled: AtomicU64,
    migrations_failed: AtomicU64,
}

/// Statistics snapshot
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigratorStatsSnapshot {
    /// Migrations started
    pub migrations_started: u64,
    /// Migrations completed
    pub migrations_completed: u64,
    /// Migrations cancelled
    pub migrations_cancelled: u64,
    /// Migrations failed
    pub migrations_failed: u64,
}

/// Get current timestamp in milliseconds
fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_plan() {
        let plan = MigrationPlan::new("tenant1", "cluster-a", "cluster-b")
            .with_mode(MigrationMode::LiveSync)
            .with_verify(true);

        assert_eq!(plan.tenant_id, "tenant1");
        assert_eq!(plan.mode, MigrationMode::LiveSync);
        assert!(plan.verify);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_migration_creation() {
        let plan = MigrationPlan::new("tenant1", "source", "target");
        let migration = Migration::new("test-migration".to_string(), plan);

        let progress = migration.progress();
        assert_eq!(progress.status, MigrationStatus::Pending);
        assert_eq!(progress.phase, MigrationPhase::Preparing);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_migration_progress() {
        let plan = MigrationPlan::new("tenant1", "source", "target");
        let migration = Migration::new("test".to_string(), plan);

        migration.set_total_keys(1000);
        migration.set_total_bytes(1_000_000);
        migration.record_transfer(500, 500_000);

        let progress = migration.progress();
        assert_eq!(progress.keys_transferred, 500);
        assert_eq!(progress.keys_percent, 50.0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_migrator() {
        let migrator = TenantMigrator::new();

        let plan = MigrationPlan::new("tenant1", "source", "target");
        let id = migrator.start_migration(plan).await.unwrap();

        let status = migrator.get_status(&id).await;
        assert!(status.is_some());

        let stats = migrator.stats();
        assert_eq!(stats.migrations_started, 1);
    }
}
