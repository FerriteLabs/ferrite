//! Global WATCH Registry
//!
//! Provides cross-connection WATCH functionality for optimistic locking.
//! When a key is modified, all connections watching that key are notified.
//!
//! # Architecture
//!
//! ```text
//! Connection 1: WATCH key1, key2
//!     │
//!     ▼
//! ┌──────────────────────────────────────┐
//! │           WatchRegistry              │
//! ├──────────────────────────────────────┤
//! │ key1 -> [conn1, conn3]               │
//! │ key2 -> [conn1]                      │
//! │ key3 -> [conn2, conn3]               │
//! └──────────────────────────────────────┘
//!     │
//!     ▼ (on key modification)
//! Notify conn1: watch_dirty = true
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use tokio::sync::watch;

/// Connection identifier for WATCH tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub u64);

impl ConnectionId {
    /// Generate a new unique connection ID
    pub fn new() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        Self(NEXT_ID.fetch_add(1, Ordering::SeqCst))
    }
}

impl Default for ConnectionId {
    fn default() -> Self {
        Self::new()
    }
}

/// Watch notification sent to connections
#[derive(Debug, Clone)]
pub struct WatchNotification {
    /// Key that was modified
    pub key: Bytes,
    /// Database the key is in
    pub db: u8,
}

/// Handle for a connection to receive watch notifications
pub struct WatchHandle {
    /// Connection ID
    id: ConnectionId,
    /// Receiver for dirty flag updates
    receiver: watch::Receiver<bool>,
}

impl WatchHandle {
    /// Check if any watched keys have been modified
    pub fn is_dirty(&self) -> bool {
        *self.receiver.borrow()
    }

    /// Wait for a change notification
    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.receiver.changed().await
    }

    /// Get the connection ID
    pub fn id(&self) -> ConnectionId {
        self.id
    }
}

/// Entry for a watched key
#[derive(Default)]
struct WatchEntry {
    /// Set of connection IDs watching this key
    watchers: HashSet<ConnectionId>,
}

/// Global registry for WATCH functionality
pub struct WatchRegistry {
    /// Map of (db, key) -> watchers
    watches: RwLock<HashMap<(u8, Bytes), WatchEntry>>,
    /// Map of connection_id -> (sender, watched_keys)
    connections: RwLock<HashMap<ConnectionId, ConnectionState>>,
}

struct ConnectionState {
    /// Sender to notify connection of dirty state
    sender: watch::Sender<bool>,
    /// Keys this connection is watching
    watched_keys: HashSet<(u8, Bytes)>,
}

impl WatchRegistry {
    /// Create a new watch registry
    pub fn new() -> Self {
        Self {
            watches: RwLock::new(HashMap::new()),
            connections: RwLock::new(HashMap::new()),
        }
    }

    /// Register a connection and get a handle for notifications
    pub fn register(&self) -> WatchHandle {
        let id = ConnectionId::new();
        let (sender, receiver) = watch::channel(false);

        self.connections.write().insert(
            id,
            ConnectionState {
                sender,
                watched_keys: HashSet::new(),
            },
        );

        WatchHandle { id, receiver }
    }

    /// Unregister a connection (called on disconnect)
    pub fn unregister(&self, id: ConnectionId) {
        // Remove from connection state
        let keys_to_clean = {
            let mut connections = self.connections.write();
            connections.remove(&id).map(|s| s.watched_keys)
        };

        // Clean up watch entries
        if let Some(keys) = keys_to_clean {
            let mut watches = self.watches.write();
            for key in keys {
                if let Some(entry) = watches.get_mut(&key) {
                    entry.watchers.remove(&id);
                    if entry.watchers.is_empty() {
                        watches.remove(&key);
                    }
                }
            }
        }
    }

    /// Watch a key
    pub fn watch(&self, id: ConnectionId, db: u8, key: Bytes) {
        let cache_key = (db, key.clone());

        // Add to connection's watched keys
        {
            let mut connections = self.connections.write();
            if let Some(state) = connections.get_mut(&id) {
                state.watched_keys.insert(cache_key.clone());
            }
        }

        // Add to watch entry
        {
            let mut watches = self.watches.write();
            watches.entry(cache_key).or_default().watchers.insert(id);
        }
    }

    /// Watch multiple keys
    pub fn watch_many(&self, id: ConnectionId, db: u8, keys: Vec<Bytes>) {
        for key in keys {
            self.watch(id, db, key);
        }
    }

    /// Unwatch all keys for a connection
    pub fn unwatch_all(&self, id: ConnectionId) {
        let keys_to_clean = {
            let mut connections = self.connections.write();
            if let Some(state) = connections.get_mut(&id) {
                // Reset dirty flag
                let _ = state.sender.send(false);
                // Take watched keys
                std::mem::take(&mut state.watched_keys)
            } else {
                HashSet::new()
            }
        };

        // Clean up watch entries
        let mut watches = self.watches.write();
        for key in keys_to_clean {
            if let Some(entry) = watches.get_mut(&key) {
                entry.watchers.remove(&id);
                if entry.watchers.is_empty() {
                    watches.remove(&key);
                }
            }
        }
    }

    /// Notify that a key was modified - invalidate all watchers
    pub fn notify_modified(&self, db: u8, key: &Bytes) {
        let cache_key = (db, key.clone());

        // Get watchers for this key
        let watcher_ids: Vec<ConnectionId> = {
            let watches = self.watches.read();
            watches
                .get(&cache_key)
                .map(|e| e.watchers.iter().copied().collect())
                .unwrap_or_default()
        };

        // Mark each watcher as dirty
        if !watcher_ids.is_empty() {
            let connections = self.connections.read();
            for id in watcher_ids {
                if let Some(state) = connections.get(&id) {
                    // Set dirty flag to true
                    let _ = state.sender.send(true);
                }
            }
        }
    }

    /// Notify that multiple keys were modified
    pub fn notify_modified_many(&self, db: u8, keys: &[Bytes]) {
        for key in keys {
            self.notify_modified(db, key);
        }
    }

    /// Check if a connection's watches are dirty
    pub fn is_dirty(&self, id: ConnectionId) -> bool {
        self.connections
            .read()
            .get(&id)
            .map(|s| *s.sender.borrow())
            .unwrap_or(false)
    }

    /// Reset dirty flag for a connection (after EXEC or DISCARD)
    pub fn reset_dirty(&self, id: ConnectionId) {
        if let Some(state) = self.connections.read().get(&id) {
            let _ = state.sender.send(false);
        }
    }

    /// Get the number of connections watching a key
    pub fn watcher_count(&self, db: u8, key: &Bytes) -> usize {
        self.watches
            .read()
            .get(&(db, key.clone()))
            .map(|e| e.watchers.len())
            .unwrap_or(0)
    }

    /// Get the number of keys being watched
    pub fn watched_key_count(&self) -> usize {
        self.watches.read().len()
    }

    /// Get the number of registered connections
    pub fn connection_count(&self) -> usize {
        self.connections.read().len()
    }
}

impl Default for WatchRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared watch registry handle
pub type SharedWatchRegistry = Arc<WatchRegistry>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_id_uniqueness() {
        let id1 = ConnectionId::new();
        let id2 = ConnectionId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_registry_creation() {
        let registry = WatchRegistry::new();
        assert_eq!(registry.watched_key_count(), 0);
        assert_eq!(registry.connection_count(), 0);
    }

    #[test]
    fn test_register_unregister() {
        let registry = WatchRegistry::new();

        let handle = registry.register();
        assert_eq!(registry.connection_count(), 1);
        assert!(!handle.is_dirty());

        registry.unregister(handle.id());
        assert_eq!(registry.connection_count(), 0);
    }

    #[test]
    fn test_watch_key() {
        let registry = WatchRegistry::new();
        let handle = registry.register();

        let key = Bytes::from("test_key");
        registry.watch(handle.id(), 0, key.clone());

        assert_eq!(registry.watcher_count(0, &key), 1);
        assert_eq!(registry.watched_key_count(), 1);
    }

    #[test]
    fn test_watch_many() {
        let registry = WatchRegistry::new();
        let handle = registry.register();

        registry.watch_many(
            handle.id(),
            0,
            vec![
                Bytes::from("key1"),
                Bytes::from("key2"),
                Bytes::from("key3"),
            ],
        );

        assert_eq!(registry.watched_key_count(), 3);
    }

    #[test]
    fn test_notify_modified() {
        let registry = WatchRegistry::new();
        let handle = registry.register();

        let key = Bytes::from("test_key");
        registry.watch(handle.id(), 0, key.clone());

        assert!(!handle.is_dirty());

        registry.notify_modified(0, &key);

        assert!(handle.is_dirty());
    }

    #[test]
    fn test_unwatch_all() {
        let registry = WatchRegistry::new();
        let handle = registry.register();

        registry.watch_many(
            handle.id(),
            0,
            vec![Bytes::from("key1"), Bytes::from("key2")],
        );

        assert_eq!(registry.watched_key_count(), 2);

        registry.unwatch_all(handle.id());

        assert_eq!(registry.watched_key_count(), 0);
    }

    #[test]
    fn test_multiple_watchers() {
        let registry = WatchRegistry::new();
        let handle1 = registry.register();
        let handle2 = registry.register();

        let key = Bytes::from("shared_key");
        registry.watch(handle1.id(), 0, key.clone());
        registry.watch(handle2.id(), 0, key.clone());

        assert_eq!(registry.watcher_count(0, &key), 2);

        // Modify triggers both
        registry.notify_modified(0, &key);

        assert!(handle1.is_dirty());
        assert!(handle2.is_dirty());
    }

    #[test]
    fn test_reset_dirty() {
        let registry = WatchRegistry::new();
        let handle = registry.register();

        let key = Bytes::from("test_key");
        registry.watch(handle.id(), 0, key.clone());

        registry.notify_modified(0, &key);
        assert!(handle.is_dirty());

        registry.reset_dirty(handle.id());
        assert!(!handle.is_dirty());
    }

    #[test]
    fn test_different_databases() {
        let registry = WatchRegistry::new();
        let handle = registry.register();

        let key = Bytes::from("same_key");
        registry.watch(handle.id(), 0, key.clone());

        // Modifying in different DB shouldn't trigger
        registry.notify_modified(1, &key);
        assert!(!handle.is_dirty());

        // Modifying in same DB should trigger
        registry.notify_modified(0, &key);
        assert!(handle.is_dirty());
    }

    #[test]
    fn test_cleanup_on_unregister() {
        let registry = WatchRegistry::new();
        let handle = registry.register();
        let id = handle.id();

        registry.watch_many(id, 0, vec![Bytes::from("key1"), Bytes::from("key2")]);

        assert_eq!(registry.watched_key_count(), 2);

        registry.unregister(id);

        assert_eq!(registry.watched_key_count(), 0);
        assert_eq!(registry.connection_count(), 0);
    }
}
