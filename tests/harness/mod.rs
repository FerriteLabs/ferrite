//! Multi-node test harness for Ferrite integration tests.
//!
//! Provides [`TestCluster`] for spinning up N in-memory `Store` instances
//! (primary + replicas) at the handler/executor level — no TCP servers needed.
//!
//! # Usage
//!
//! ```rust,ignore
//! mod harness;
//! use harness::TestCluster;
//!
//! let cluster = TestCluster::new(2); // 1 primary + 2 replicas
//! let primary = cluster.primary_store();
//! let replica = cluster.replica_store(0);
//! ```

pub mod assertions;

use std::sync::Arc;

use bytes::Bytes;
use ferrite::storage::{Store, Value};

// Re-export assertion helpers at the harness level for convenience.
#[allow(unused_imports)]
pub use assertions::*;

/// A lightweight multi-node cluster for handler-level integration testing.
///
/// Each node owns an independent [`Store`] (16 databases). No TCP or
/// networking is involved — tests call handler functions directly,
/// passing the appropriate `Arc<Store>`.
pub struct TestCluster {
    pub primary: Arc<Store>,
    pub replicas: Vec<Arc<Store>>,
}

impl TestCluster {
    /// Create a cluster with one primary and `replica_count` replicas.
    pub fn new(replica_count: usize) -> Self {
        let primary = Arc::new(Store::new(16));
        let replicas = (0..replica_count)
            .map(|_| Arc::new(Store::new(16)))
            .collect();
        Self { primary, replicas }
    }

    /// Reference to the primary store.
    pub fn primary_store(&self) -> &Arc<Store> {
        &self.primary
    }

    /// Reference to replica `idx` (0-based). Panics if out of range.
    pub fn replica_store(&self, idx: usize) -> &Arc<Store> {
        &self.replicas[idx]
    }

    /// Simulate replication of a single key from the primary to a replica.
    ///
    /// Reads the value at `(db, key)` from the primary and writes it into
    /// the specified replica store, mimicking what a real replication stream
    /// would do for simple key-value data.
    #[allow(dead_code)]
    pub fn replicate_key(&self, db: u8, key: &str, replica_idx: usize) {
        let k = Bytes::from(key.to_string());
        match self.primary.get(db, &k) {
            Some(value) => {
                self.replicas[replica_idx].set(db, k, value);
            }
            None => {
                self.replicas[replica_idx].del(db, &[k]);
            }
        }
    }

    /// Replicate all `__ferrite:*` metadata keys used by handler SAVE/LOAD.
    ///
    /// This copies every matching key from db 0 of the primary into
    /// db 0 of the target replica.
    #[allow(dead_code)]
    pub fn replicate_ferrite_state(&self, replica_idx: usize) {
        let ferrite_keys: &[&str] = &[
            "__ferrite:chronicle:data",
            "__ferrite:concord:data",
            "__ferrite:lucidity:data",
            "__ferrite:pangea:data",
            "__ferrite:mnemo:data",
            "__ferrite:forge:data",
        ];

        for &key_str in ferrite_keys {
            let key = Bytes::from(key_str);
            if let Some(value) = self.primary.get(0, &key) {
                self.replicas[replica_idx].set(0, key, value);
            }
        }
    }

    /// Set a string value on the primary store (convenience wrapper).
    #[allow(dead_code)]
    pub fn primary_set(&self, db: u8, key: &str, value: &str) {
        self.primary.set(
            db,
            Bytes::from(key.to_string()),
            Value::String(Bytes::from(value.to_string())),
        );
    }

    /// Get a string value from the primary store (convenience wrapper).
    #[allow(dead_code)]
    pub fn primary_get(&self, db: u8, key: &str) -> Option<String> {
        match self.primary.get(db, &Bytes::from(key.to_string())) {
            Some(Value::String(data)) => Some(String::from_utf8_lossy(&data).into_owned()),
            _ => None,
        }
    }
}
