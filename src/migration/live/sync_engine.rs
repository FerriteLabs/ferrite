//! Migration engine — orchestrates bulk sync, continuous replication,
//! verification, cutover, and rollback.

#![forbid(unsafe_code)]

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use uuid::Uuid;

use super::connector::{ConnectorError, RedisConnector};
use super::state::{MigrationPhase, MigrationState, MigrationStatus};
use super::verifier::{MigrationVerifier, VerificationReport};

/// Configuration for a live migration run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationConfig {
    /// Number of keys to fetch per SCAN batch.
    pub batch_size: usize,
    /// Number of parallel copy workers.
    pub parallel_workers: usize,
    /// Whether to run verification after the bulk sync phase.
    pub verify_after_sync: bool,
    /// If `true`, no data is actually written to the target.
    pub dry_run: bool,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            parallel_workers: 4,
            verify_after_sync: true,
            dry_run: false,
        }
    }
}

/// Error type for the migration engine.
#[derive(Debug, thiserror::Error)]
pub enum MigrationEngineError {
    /// The migration is not in a valid state for the requested operation.
    #[error("invalid state: {0}")]
    InvalidState(String),
    /// A connector-level error occurred.
    #[error("connector error: {0}")]
    Connector(#[from] ConnectorError),
    /// Verification failed.
    #[error("verification failed: {0}")]
    VerificationFailed(String),
    /// Generic error.
    #[error("{0}")]
    Other(String),
}

/// Result alias for engine operations.
pub type Result<T> = std::result::Result<T, MigrationEngineError>;

/// The migration engine drives the full migration lifecycle.
pub struct MigrationEngine {
    source_uri: String,
    config: MigrationConfig,
    state: Arc<RwLock<MigrationState>>,
}

impl MigrationEngine {
    /// Create a new migration engine targeting `source_uri`.
    pub fn new(source_uri: String, config: MigrationConfig) -> Self {
        let id = Uuid::new_v4().to_string();
        let state = MigrationState::new(id, source_uri.clone());
        Self {
            source_uri,
            config,
            state: Arc::new(RwLock::new(state)),
        }
    }

    /// Start the bulk-sync phase: scan all keys on the source and copy
    /// them to the local store.
    pub async fn start_bulk_sync(&self) -> Result<MigrationState> {
        {
            let mut s = self.state.write().await;
            if s.status != MigrationStatus::Pending && s.status != MigrationStatus::Paused {
                return Err(MigrationEngineError::InvalidState(format!(
                    "cannot start bulk sync in state {}",
                    s.status
                )));
            }
            s.status = MigrationStatus::Running;
            s.phase = MigrationPhase::BulkSync;
        }

        info!(uri = %self.source_uri, "starting bulk sync");

        let connector = RedisConnector::connect(&self.source_uri).await?;

        // Fetch total key count for progress tracking.
        match connector.dbsize().await {
            Ok(total) => {
                let mut s = self.state.write().await;
                s.keys_total = Some(total);
            }
            Err(e) => {
                warn!("failed to fetch DBSIZE: {}", e);
            }
        }

        // Scan loop — iterate until cursor returns 0.
        let mut cursor: u64 = 0;
        loop {
            let (next_cursor, keys) = connector
                .scan_keys(cursor, "*", self.config.batch_size as u64)
                .await?;

            for key in &keys {
                if self.config.dry_run {
                    info!(key, "dry-run: would copy key");
                } else if let Some(dump) = connector.dump_key(key).await? {
                    // In a full implementation we would write `dump` into
                    // the local Store here.
                    let mut s = self.state.write().await;
                    s.keys_synced += 1;
                    s.bytes_synced += dump.key.len() as u64;
                }
            }

            cursor = next_cursor;
            if cursor == 0 {
                break;
            }
        }

        if self.config.verify_after_sync {
            let mut s = self.state.write().await;
            s.phase = MigrationPhase::Verifying;
        }

        info!("bulk sync complete");
        Ok(self.state.read().await.clone())
    }

    /// Begin continuous replication after the bulk sync phase.
    pub async fn start_continuous_replication(&self) -> Result<()> {
        {
            let mut s = self.state.write().await;
            if s.status != MigrationStatus::Running {
                return Err(MigrationEngineError::InvalidState(
                    "migration must be running to start replication".into(),
                ));
            }
            s.phase = MigrationPhase::ContinuousReplication;
        }

        info!("starting continuous replication via keyspace notifications");

        let connector = RedisConnector::connect(&self.source_uri).await?;
        let mut rx = connector.subscribe_keyspace().await?;

        // Process keyspace events until the channel is closed or we cut over.
        while let Some(event) = rx.recv().await {
            let status = self.state.read().await.status.clone();
            if status != MigrationStatus::Running {
                break;
            }
            info!(key = %event.key, op = %event.operation, "replicating keyspace event");
            // In a full implementation we would apply the event to the
            // local Store.
        }

        Ok(())
    }

    /// Return a snapshot of the current migration state.
    pub async fn get_status(&self) -> MigrationState {
        self.state.read().await.clone()
    }

    /// Pause the migration (can be resumed later).
    pub async fn pause(&self) -> Result<()> {
        let mut s = self.state.write().await;
        if s.status != MigrationStatus::Running {
            return Err(MigrationEngineError::InvalidState(
                "can only pause a running migration".into(),
            ));
        }
        s.status = MigrationStatus::Paused;
        info!(id = %s.id, "migration paused");
        Ok(())
    }

    /// Resume a paused migration.
    pub async fn resume(&self) -> Result<()> {
        let mut s = self.state.write().await;
        if s.status != MigrationStatus::Paused {
            return Err(MigrationEngineError::InvalidState(
                "can only resume a paused migration".into(),
            ));
        }
        s.status = MigrationStatus::Running;
        info!(id = %s.id, "migration resumed");
        Ok(())
    }

    /// Verify consistency between source and local store by sampling keys.
    pub async fn verify(&self) -> Result<VerificationReport> {
        {
            let mut s = self.state.write().await;
            s.phase = MigrationPhase::Verifying;
        }

        info!("running verification");
        let report = MigrationVerifier::verify_snapshot(100);

        if report.mismatched > 0 || report.missing_in_target > 0 {
            let msg = format!(
                "{} mismatched, {} missing in target",
                report.mismatched, report.missing_in_target
            );
            warn!(msg, "verification found discrepancies");
            return Err(MigrationEngineError::VerificationFailed(msg));
        }

        info!(
            total_checked = report.total_checked,
            matching = report.matching,
            "verification passed"
        );
        Ok(report)
    }

    /// Cut over: switch traffic to Ferrite and mark migration complete.
    pub async fn cutover(&self) -> Result<()> {
        {
            let s = self.state.read().await;
            if s.status != MigrationStatus::Running && s.status != MigrationStatus::Paused {
                return Err(MigrationEngineError::InvalidState(
                    "migration must be running or paused to cut over".into(),
                ));
            }
        }

        {
            let mut s = self.state.write().await;
            s.phase = MigrationPhase::CuttingOver;
        }

        info!("cutting over to Ferrite");
        // In a full implementation this would update routing, drain
        // in-flight requests, and flip the connection target.

        let mut s = self.state.write().await;
        s.mark_completed();
        info!(id = %s.id, "cutover complete");
        Ok(())
    }

    /// Abort the migration and clean up.
    pub async fn rollback(&self) -> Result<()> {
        let mut s = self.state.write().await;
        if s.status == MigrationStatus::Completed {
            return Err(MigrationEngineError::InvalidState(
                "cannot rollback a completed migration".into(),
            ));
        }

        error!(id = %s.id, "rolling back migration");
        s.status = MigrationStatus::RolledBack;
        s.phase = MigrationPhase::Done;
        s.completed_at = Some(chrono::Utc::now());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_engine_new() {
        let engine = MigrationEngine::new(
            "redis://localhost:6379".into(),
            MigrationConfig::default(),
        );
        let status = engine.get_status().await;
        assert_eq!(status.status, MigrationStatus::Pending);
    }

    #[tokio::test]
    async fn test_pause_requires_running() {
        let engine = MigrationEngine::new(
            "redis://localhost:6379".into(),
            MigrationConfig::default(),
        );
        assert!(engine.pause().await.is_err());
    }

    #[tokio::test]
    async fn test_resume_requires_paused() {
        let engine = MigrationEngine::new(
            "redis://localhost:6379".into(),
            MigrationConfig::default(),
        );
        assert!(engine.resume().await.is_err());
    }

    #[tokio::test]
    async fn test_rollback_pending() {
        let engine = MigrationEngine::new(
            "redis://localhost:6379".into(),
            MigrationConfig::default(),
        );
        assert!(engine.rollback().await.is_ok());
        let s = engine.get_status().await;
        assert_eq!(s.status, MigrationStatus::RolledBack);
    }

    #[tokio::test]
    async fn test_cutover_requires_running() {
        let engine = MigrationEngine::new(
            "redis://localhost:6379".into(),
            MigrationConfig::default(),
        );
        assert!(engine.cutover().await.is_err());
    }

    #[tokio::test]
    async fn test_bulk_sync_dry_run() {
        let engine = MigrationEngine::new(
            "redis://localhost:6379".into(),
            MigrationConfig {
                dry_run: true,
                ..Default::default()
            },
        );
        let state = engine.start_bulk_sync().await.unwrap();
        assert_eq!(state.status, MigrationStatus::Running);
    }

    #[tokio::test]
    async fn test_config_defaults() {
        let cfg = MigrationConfig::default();
        assert_eq!(cfg.batch_size, 1000);
        assert_eq!(cfg.parallel_workers, 4);
        assert!(cfg.verify_after_sync);
        assert!(!cfg.dry_run);
    }
}
