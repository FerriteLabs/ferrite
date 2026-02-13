//! Checkpoint management for pipeline state persistence.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A checkpoint of pipeline processing state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Pipeline name.
    pub pipeline_name: String,
    /// Checkpoint ID (monotonically increasing).
    pub checkpoint_id: u64,
    /// Timestamp of the checkpoint.
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Last processed event ID per source.
    pub offsets: HashMap<String, u64>,
    /// Window state snapshots.
    pub window_states: HashMap<String, Vec<u8>>,
    /// Operator state snapshots.
    pub operator_states: HashMap<String, Vec<u8>>,
}

/// Manages checkpoint creation and recovery.
pub struct CheckpointManager {
    pipeline_name: String,
    next_id: u64,
    checkpoints: Vec<Checkpoint>,
    max_retained: usize,
}

impl CheckpointManager {
    pub fn new(pipeline_name: &str, max_retained: usize) -> Self {
        Self {
            pipeline_name: pipeline_name.to_string(),
            next_id: 0,
            checkpoints: Vec::new(),
            max_retained,
        }
    }

    /// Creates a new checkpoint.
    pub fn create_checkpoint(&mut self, offsets: HashMap<String, u64>) -> Checkpoint {
        let checkpoint = Checkpoint {
            pipeline_name: self.pipeline_name.clone(),
            checkpoint_id: self.next_id,
            timestamp: chrono::Utc::now(),
            offsets,
            window_states: HashMap::new(),
            operator_states: HashMap::new(),
        };
        self.next_id += 1;

        self.checkpoints.push(checkpoint.clone());

        // Retain only the most recent checkpoints
        while self.checkpoints.len() > self.max_retained {
            self.checkpoints.remove(0);
        }

        checkpoint
    }

    /// Returns the latest checkpoint.
    pub fn latest(&self) -> Option<&Checkpoint> {
        self.checkpoints.last()
    }

    /// Returns checkpoint by ID.
    pub fn get(&self, id: u64) -> Option<&Checkpoint> {
        self.checkpoints.iter().find(|c| c.checkpoint_id == id)
    }

    /// Returns the number of stored checkpoints.
    pub fn count(&self) -> usize {
        self.checkpoints.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_checkpoint() {
        let mut mgr = CheckpointManager::new("test-pipeline", 3);
        let offsets: HashMap<String, u64> = [("source1".to_string(), 100)].into();

        let cp = mgr.create_checkpoint(offsets);
        assert_eq!(cp.checkpoint_id, 0);
        assert_eq!(cp.pipeline_name, "test-pipeline");
    }

    #[test]
    fn test_latest_checkpoint() {
        let mut mgr = CheckpointManager::new("test", 5);
        mgr.create_checkpoint(HashMap::new());
        mgr.create_checkpoint(HashMap::new());

        let latest = mgr.latest().unwrap();
        assert_eq!(latest.checkpoint_id, 1);
    }

    #[test]
    fn test_max_retained() {
        let mut mgr = CheckpointManager::new("test", 2);
        mgr.create_checkpoint(HashMap::new());
        mgr.create_checkpoint(HashMap::new());
        mgr.create_checkpoint(HashMap::new());

        assert_eq!(mgr.count(), 2);
        // Oldest should have been removed
        assert!(mgr.get(0).is_none());
        assert!(mgr.get(1).is_some());
        assert!(mgr.get(2).is_some());
    }
}
