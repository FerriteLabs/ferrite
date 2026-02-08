//! Savepoint support for nested transaction rollback
//!
//! Savepoints allow partial rollback within a transaction without
//! aborting the entire transaction. This is useful for:
//! - Retry loops within a transaction
//! - Conditional operations with fallback
//! - Nested business logic with independent error handling
//!
//! # Example
//!
//! ```ignore
//! let txn = manager.begin(None).await?;
//!
//! txn.set("key1", "value1").await?;
//!
//! // Create a savepoint
//! let sp = txn.savepoint("before_risky_op").await?;
//!
//! // Try a risky operation
//! if let Err(_) = do_risky_thing(&txn).await {
//!     // Rollback to savepoint (keeps key1 = value1)
//!     txn.rollback_to_savepoint("before_risky_op").await?;
//! }
//!
//! // Continue with transaction
//! txn.set("key2", "value2").await?;
//! txn.commit().await?;
//! ```

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::storage::Value as StorageValue;

/// A savepoint within a transaction
#[derive(Clone, Debug)]
pub struct Savepoint {
    /// Savepoint name
    pub name: String,
    /// Savepoint ID (monotonically increasing)
    pub id: u64,
    /// Write set snapshot at savepoint creation
    pub write_set_snapshot: HashMap<(u8, Bytes), Option<StorageValue>>,
    /// Keys locked after this savepoint
    pub locks_after: Vec<(u8, Bytes)>,
    /// Creation timestamp
    pub created_at: Instant,
}

impl Savepoint {
    /// Create a new savepoint
    pub fn new(
        name: String,
        id: u64,
        write_set_snapshot: HashMap<(u8, Bytes), Option<StorageValue>>,
    ) -> Self {
        Self {
            name,
            id,
            write_set_snapshot,
            locks_after: Vec::new(),
            created_at: Instant::now(),
        }
    }

    /// Record a lock acquired after this savepoint
    pub fn record_lock(&mut self, db: u8, key: Bytes) {
        self.locks_after.push((db, key));
    }

    /// Get locks to release on rollback
    pub fn locks_to_release(&self) -> &[(u8, Bytes)] {
        &self.locks_after
    }

    /// Check if a key was modified after this savepoint
    pub fn key_modified_after(
        &self,
        db: u8,
        key: &Bytes,
        current_write_set: &HashMap<(u8, Bytes), Option<StorageValue>>,
    ) -> bool {
        let k = (db, key.clone());
        match (self.write_set_snapshot.get(&k), current_write_set.get(&k)) {
            (None, Some(_)) => true,              // New key
            (Some(old), Some(new)) => old != new, // Modified
            (Some(_), None) => true,              // Removed (shouldn't happen)
            (None, None) => false,                // Not in either
        }
    }
}

/// Savepoint manager for a transaction
#[derive(Debug)]
pub struct SavepointManager {
    /// Savepoints indexed by name
    savepoints: HashMap<String, Savepoint>,
    /// Savepoints in creation order
    savepoint_stack: Vec<String>,
    /// Next savepoint ID
    next_id: AtomicU64,
}

impl SavepointManager {
    /// Create a new savepoint manager
    pub fn new() -> Self {
        Self {
            savepoints: HashMap::new(),
            savepoint_stack: Vec::new(),
            next_id: AtomicU64::new(1),
        }
    }

    /// Create a savepoint
    pub fn create(
        &mut self,
        name: &str,
        write_set: &HashMap<(u8, Bytes), Option<StorageValue>>,
    ) -> Result<&Savepoint, SavepointError> {
        if self.savepoints.contains_key(name) {
            return Err(SavepointError::AlreadyExists(name.to_string()));
        }

        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let savepoint = Savepoint::new(name.to_string(), id, write_set.clone());

        self.savepoints.insert(name.to_string(), savepoint);
        self.savepoint_stack.push(name.to_string());

        self.savepoints
            .get(name)
            .ok_or_else(|| SavepointError::NotFound(name.to_string()))
    }

    /// Get a savepoint by name
    pub fn get(&self, name: &str) -> Option<&Savepoint> {
        self.savepoints.get(name)
    }

    /// Get a mutable savepoint by name
    pub fn get_mut(&mut self, name: &str) -> Option<&mut Savepoint> {
        self.savepoints.get_mut(name)
    }

    /// Release a savepoint (and all savepoints created after it)
    pub fn release(&mut self, name: &str) -> Result<(), SavepointError> {
        if !self.savepoints.contains_key(name) {
            return Err(SavepointError::NotFound(name.to_string()));
        }

        // Find the savepoint in the stack
        let pos = self
            .savepoint_stack
            .iter()
            .position(|n| n == name)
            .ok_or_else(|| SavepointError::NotFound(name.to_string()))?;

        // Remove all savepoints from this one to the end
        let to_remove: Vec<String> = self.savepoint_stack.drain(pos..).collect();
        for sp_name in to_remove {
            self.savepoints.remove(&sp_name);
        }

        Ok(())
    }

    /// Rollback to a savepoint (returns writes to undo)
    pub fn rollback_to(&mut self, name: &str) -> Result<SavepointRollback, SavepointError> {
        let savepoint = self
            .savepoints
            .get(name)
            .ok_or_else(|| SavepointError::NotFound(name.to_string()))?;

        let rollback = SavepointRollback {
            write_set_to_restore: savepoint.write_set_snapshot.clone(),
            locks_to_release: savepoint.locks_after.clone(),
        };

        // Find the savepoint in the stack
        let pos = self
            .savepoint_stack
            .iter()
            .position(|n| n == name)
            .ok_or_else(|| SavepointError::NotFound(name.to_string()))?;

        // Remove all savepoints after this one (not including this one)
        let to_remove: Vec<String> = self.savepoint_stack.drain(pos + 1..).collect();
        for sp_name in to_remove {
            self.savepoints.remove(&sp_name);
        }

        // Clear the locks_after since we're rolling back to this point
        if let Some(sp) = self.savepoints.get_mut(name) {
            sp.locks_after.clear();
        }

        Ok(rollback)
    }

    /// Record a lock acquired (track in all active savepoints)
    pub fn record_lock(&mut self, db: u8, key: Bytes) {
        for sp in self.savepoints.values_mut() {
            sp.record_lock(db, key.clone());
        }
    }

    /// Get all savepoint names
    pub fn names(&self) -> Vec<&str> {
        self.savepoint_stack.iter().map(|s| s.as_str()).collect()
    }

    /// Get the number of savepoints
    pub fn count(&self) -> usize {
        self.savepoints.len()
    }

    /// Check if a savepoint exists
    pub fn exists(&self, name: &str) -> bool {
        self.savepoints.contains_key(name)
    }

    /// Clear all savepoints
    pub fn clear(&mut self) {
        self.savepoints.clear();
        self.savepoint_stack.clear();
    }
}

impl Default for SavepointManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Information needed to rollback to a savepoint
#[derive(Clone, Debug)]
pub struct SavepointRollback {
    /// Write set to restore
    pub write_set_to_restore: HashMap<(u8, Bytes), Option<StorageValue>>,
    /// Locks to release
    pub locks_to_release: Vec<(u8, Bytes)>,
}

/// Savepoint errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum SavepointError {
    /// The specified savepoint was not found.
    #[error("Savepoint not found: {0}")]
    NotFound(String),

    /// A savepoint with the same name already exists.
    #[error("Savepoint already exists: {0}")]
    AlreadyExists(String),

    /// The savepoint cannot be released.
    #[error("Cannot release savepoint: {0}")]
    CannotRelease(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_savepoint_creation() {
        let mut manager = SavepointManager::new();
        let write_set = HashMap::new();

        manager.create("sp1", &write_set).unwrap();
        assert!(manager.exists("sp1"));
        assert_eq!(manager.count(), 1);
    }

    #[test]
    fn test_savepoint_duplicate() {
        let mut manager = SavepointManager::new();
        let write_set = HashMap::new();

        manager.create("sp1", &write_set).unwrap();
        let result = manager.create("sp1", &write_set);
        assert!(matches!(result, Err(SavepointError::AlreadyExists(_))));
    }

    #[test]
    fn test_savepoint_release() {
        let mut manager = SavepointManager::new();
        let write_set = HashMap::new();

        manager.create("sp1", &write_set).unwrap();
        manager.create("sp2", &write_set).unwrap();
        manager.create("sp3", &write_set).unwrap();

        // Release sp2 should also remove sp3
        manager.release("sp2").unwrap();

        assert!(manager.exists("sp1"));
        assert!(!manager.exists("sp2"));
        assert!(!manager.exists("sp3"));
    }

    #[test]
    fn test_savepoint_rollback() {
        let mut manager = SavepointManager::new();

        let mut write_set = HashMap::new();
        write_set.insert(
            (0, Bytes::from("key1")),
            Some(StorageValue::String(Bytes::from("value1"))),
        );

        manager.create("sp1", &write_set).unwrap();

        // Add more writes
        write_set.insert(
            (0, Bytes::from("key2")),
            Some(StorageValue::String(Bytes::from("value2"))),
        );

        manager.create("sp2", &write_set).unwrap();

        // Rollback to sp1
        let rollback = manager.rollback_to("sp1").unwrap();

        // Should restore write_set to only key1
        assert_eq!(rollback.write_set_to_restore.len(), 1);
        assert!(rollback
            .write_set_to_restore
            .contains_key(&(0, Bytes::from("key1"))));

        // sp2 should be gone, sp1 should remain
        assert!(manager.exists("sp1"));
        assert!(!manager.exists("sp2"));
    }

    #[test]
    fn test_lock_tracking() {
        let mut manager = SavepointManager::new();
        let write_set = HashMap::new();

        manager.create("sp1", &write_set).unwrap();

        // Record locks after savepoint
        manager.record_lock(0, Bytes::from("lock1"));
        manager.record_lock(0, Bytes::from("lock2"));

        let sp = manager.get("sp1").unwrap();
        assert_eq!(sp.locks_after.len(), 2);
    }

    #[test]
    fn test_key_modified_after() {
        let mut initial_write_set = HashMap::new();
        initial_write_set.insert(
            (0, Bytes::from("key1")),
            Some(StorageValue::String(Bytes::from("v1"))),
        );

        let sp = Savepoint::new("test".to_string(), 1, initial_write_set);

        let mut current = HashMap::new();
        current.insert(
            (0, Bytes::from("key1")),
            Some(StorageValue::String(Bytes::from("v1"))),
        );
        current.insert(
            (0, Bytes::from("key2")),
            Some(StorageValue::String(Bytes::from("v2"))),
        );

        // key1 not modified
        assert!(!sp.key_modified_after(0, &Bytes::from("key1"), &current));

        // key2 is new
        assert!(sp.key_modified_after(0, &Bytes::from("key2"), &current));

        // key3 doesn't exist in either
        assert!(!sp.key_modified_after(0, &Bytes::from("key3"), &current));
    }
}
