//! Replication offset persistence
//!
//! Persists the replication offset (and replication ID) to a small file
//! alongside the AOF / checkpoint data so that a replica can attempt
//! partial resync after a restart instead of a full resync.
//!
//! File format (plain text, easy to debug):
//! ```text
//! replid:<hex-id>
//! offset:<u64>
//! ```

use std::path::{Path, PathBuf};

use tokio::fs;
use tracing::{debug, error};

use super::{ReplicationId, ReplicationOffset, SharedReplicationState};

/// Default filename for the persisted offset.
const OFFSET_FILENAME: &str = "repl_offset.meta";

/// Manages persistence of replication offset to disk.
pub struct ReplicationOffsetPersistence {
    path: PathBuf,
}

/// Errors during offset persistence.
#[derive(Debug, thiserror::Error)]
pub enum OffsetPersistenceError {
    /// I/O error reading or writing the offset file.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The file content could not be parsed.
    #[error("corrupt offset file: {0}")]
    Corrupt(String),
}

/// Persisted replication metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedReplicationMeta {
    /// Replication ID of the primary.
    pub replid: ReplicationId,
    /// Last known replication offset.
    pub offset: ReplicationOffset,
}

impl ReplicationOffsetPersistence {
    /// Create a new persistence handle writing to the given data directory.
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self {
            path: data_dir.as_ref().join(OFFSET_FILENAME),
        }
    }

    /// Create with an explicit file path.
    pub fn with_path(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Persist the current replication state to disk.
    pub async fn save(&self, state: &SharedReplicationState) -> Result<(), OffsetPersistenceError> {
        let replid = state.master_replid().await;
        let offset = state.repl_offset();
        self.save_raw(&replid, offset).await
    }

    /// Persist a specific replid and offset.
    pub async fn save_raw(
        &self,
        replid: &ReplicationId,
        offset: ReplicationOffset,
    ) -> Result<(), OffsetPersistenceError> {
        let content = format!("replid:{}\noffset:{}\n", replid.as_str(), offset);

        // Write atomically via a temp file + rename
        let tmp = self.path.with_extension("tmp");
        fs::write(&tmp, content.as_bytes()).await?;
        fs::rename(&tmp, &self.path).await?;

        debug!(offset, replid = replid.as_str(), "persisted replication offset");
        Ok(())
    }

    /// Load persisted replication metadata, if available.
    pub async fn load(&self) -> Result<Option<PersistedReplicationMeta>, OffsetPersistenceError> {
        match fs::read_to_string(&self.path).await {
            Ok(content) => {
                let meta = Self::parse(&content)?;
                debug!(
                    offset = meta.offset,
                    replid = meta.replid.as_str(),
                    "loaded persisted replication offset"
                );
                Ok(Some(meta))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => {
                error!(error = %e, "failed to read replication offset file");
                Err(OffsetPersistenceError::Io(e))
            }
        }
    }

    /// Apply the persisted metadata to the replication state.
    ///
    /// Returns `true` if metadata was loaded and applied.
    pub async fn restore(
        &self,
        state: &SharedReplicationState,
    ) -> Result<bool, OffsetPersistenceError> {
        match self.load().await? {
            Some(meta) => {
                state.set_master_replid(meta.replid).await;
                state.set_offset(meta.offset);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Remove the persisted offset file.
    pub async fn remove(&self) -> Result<(), OffsetPersistenceError> {
        match fs::remove_file(&self.path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(OffsetPersistenceError::Io(e)),
        }
    }

    fn parse(content: &str) -> Result<PersistedReplicationMeta, OffsetPersistenceError> {
        let mut replid: Option<String> = None;
        let mut offset: Option<u64> = None;

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if let Some(val) = line.strip_prefix("replid:") {
                replid = Some(val.to_string());
            } else if let Some(val) = line.strip_prefix("offset:") {
                offset = Some(val.parse::<u64>().map_err(|_| {
                    OffsetPersistenceError::Corrupt(format!("invalid offset: {}", val))
                })?);
            }
        }

        match (replid, offset) {
            (Some(id), Some(off)) => Ok(PersistedReplicationMeta {
                replid: ReplicationId::from_string(id),
                offset: off,
            }),
            _ => Err(OffsetPersistenceError::Corrupt(
                "missing replid or offset field".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::ReplicationState;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_save_and_load() {
        let dir = tempfile::tempdir().expect("tmp dir");
        let persist = ReplicationOffsetPersistence::new(dir.path());

        let replid = ReplicationId::from_string("abc123");
        persist.save_raw(&replid, 42).await.expect("save");

        let meta = persist.load().await.expect("load").expect("meta present");
        assert_eq!(meta.replid, replid);
        assert_eq!(meta.offset, 42);
    }

    #[tokio::test]
    async fn test_load_missing() {
        let dir = tempfile::tempdir().expect("tmp dir");
        let persist = ReplicationOffsetPersistence::new(dir.path());
        let meta = persist.load().await.expect("load");
        assert!(meta.is_none());
    }

    #[tokio::test]
    async fn test_restore_applies_state() {
        let dir = tempfile::tempdir().expect("tmp dir");
        let persist = ReplicationOffsetPersistence::new(dir.path());

        let replid = ReplicationId::from_string("myid");
        persist.save_raw(&replid, 999).await.expect("save");

        let state = Arc::new(ReplicationState::new());
        let restored = persist.restore(&state).await.expect("restore");
        assert!(restored);
        assert_eq!(state.repl_offset(), 999);
        assert_eq!(state.master_replid().await, replid);
    }

    #[tokio::test]
    async fn test_restore_missing_returns_false() {
        let dir = tempfile::tempdir().expect("tmp dir");
        let persist = ReplicationOffsetPersistence::new(dir.path());
        let state = Arc::new(ReplicationState::new());
        let restored = persist.restore(&state).await.expect("restore");
        assert!(!restored);
    }

    #[tokio::test]
    async fn test_save_via_shared_state() {
        let dir = tempfile::tempdir().expect("tmp dir");
        let persist = ReplicationOffsetPersistence::new(dir.path());

        let state = Arc::new(ReplicationState::new());
        state.set_offset(777);
        persist.save(&state).await.expect("save");

        let meta = persist.load().await.expect("load").expect("meta present");
        assert_eq!(meta.offset, 777);
    }

    #[tokio::test]
    async fn test_remove() {
        let dir = tempfile::tempdir().expect("tmp dir");
        let persist = ReplicationOffsetPersistence::new(dir.path());

        let replid = ReplicationId::from_string("rm-test");
        persist.save_raw(&replid, 1).await.expect("save");
        assert!(persist.load().await.expect("load").is_some());

        persist.remove().await.expect("remove");
        assert!(persist.load().await.expect("load").is_none());
    }

    #[tokio::test]
    async fn test_remove_idempotent() {
        let dir = tempfile::tempdir().expect("tmp dir");
        let persist = ReplicationOffsetPersistence::new(dir.path());
        // Removing a file that doesn't exist should not error
        persist.remove().await.expect("remove");
    }

    #[test]
    fn test_parse_corrupt() {
        let result = ReplicationOffsetPersistence::parse("garbage");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_partial() {
        let result = ReplicationOffsetPersistence::parse("replid:abc\n");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_valid() {
        let meta =
            ReplicationOffsetPersistence::parse("replid:abc\noffset:100\n").expect("parse");
        assert_eq!(meta.replid, ReplicationId::from_string("abc"));
        assert_eq!(meta.offset, 100);
    }
}
