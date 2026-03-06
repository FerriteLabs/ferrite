//! Client-side caching support via server-assisted key invalidation.
//!
//! Implements the Redis CLIENT TRACKING protocol:
//! - Clients opt-in to tracking via `CLIENT TRACKING ON`
//! - Server records which keys each client has accessed
//! - When a tracked key is modified, the server sends an invalidation
//!   message to all clients tracking that key
//! - Supports both default (per-key) and broadcasting (prefix-based) modes
//! - Supports REDIRECT to send invalidations to a different client (for RESP2)

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use bytes::Bytes;

/// Maximum number of keys tracked per client (to prevent memory exhaustion)
const DEFAULT_MAX_TRACKED_KEYS_PER_CLIENT: usize = 1_000_000;

/// Maximum total tracking entries (across all clients)
const DEFAULT_MAX_TRACKING_TABLE_SIZE: usize = 10_000_000;

/// Tracking mode for a client
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackingMode {
    /// Default mode: track specific keys the client reads
    Default,
    /// Broadcasting mode: subscribe to key prefix notifications
    Broadcast,
    /// Opt-in mode: only track keys after CLIENT CACHING YES
    OptIn,
}

/// Per-client tracking configuration
#[derive(Debug, Clone)]
pub struct ClientTrackingState {
    /// Whether tracking is enabled for this client
    pub enabled: bool,
    /// Tracking mode
    pub mode: TrackingMode,
    /// Redirect invalidation messages to this client ID (-1 = no redirect)
    pub redirect: i64,
    /// Prefixes to track in BCAST mode (empty = all keys)
    pub prefixes: Vec<Bytes>,
    /// Whether the next read should be tracked (for OPTIN mode with CLIENT CACHING YES)
    pub caching_next: bool,
    /// Number of keys currently tracked by this client
    pub tracked_key_count: usize,
}

impl Default for ClientTrackingState {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: TrackingMode::Default,
            redirect: -1,
            prefixes: Vec::new(),
            caching_next: false,
            tracked_key_count: 0,
        }
    }
}

/// Server-side tracking table for client-side caching invalidation.
///
/// Maps keys to the set of client IDs that have read (and are tracking) them.
/// When a key is modified, all tracking clients receive an invalidation message.
pub struct TrackingTable {
    /// key → set of client IDs tracking this key
    key_to_clients: RwLock<HashMap<Bytes, HashSet<u64>>>,
    /// client ID → tracking state
    client_states: RwLock<HashMap<u64, ClientTrackingState>>,
    /// Total number of tracking entries (for memory limiting)
    total_entries: std::sync::atomic::AtomicUsize,
    /// Maximum tracked keys per client
    max_keys_per_client: usize,
    /// Maximum total tracking table entries
    max_table_size: usize,
}

impl TrackingTable {
    /// Create a new tracking table with default limits
    pub fn new() -> Self {
        Self {
            key_to_clients: RwLock::new(HashMap::new()),
            client_states: RwLock::new(HashMap::new()),
            total_entries: std::sync::atomic::AtomicUsize::new(0),
            max_keys_per_client: DEFAULT_MAX_TRACKED_KEYS_PER_CLIENT,
            max_table_size: DEFAULT_MAX_TRACKING_TABLE_SIZE,
        }
    }

    /// Enable tracking for a client
    pub fn enable_tracking(
        &self,
        client_id: u64,
        mode: TrackingMode,
        redirect: i64,
        prefixes: Vec<Bytes>,
    ) {
        let mut states = self.client_states.write().unwrap_or_else(|e| e.into_inner());
        states.insert(
            client_id,
            ClientTrackingState {
                enabled: true,
                mode,
                redirect,
                prefixes,
                caching_next: false,
                tracked_key_count: 0,
            },
        );
    }

    /// Disable tracking for a client and clean up all its entries
    pub fn disable_tracking(&self, client_id: u64) {
        // Remove client state
        {
            let mut states = self.client_states.write().unwrap_or_else(|e| e.into_inner());
            states.remove(&client_id);
        }

        // Remove client from all tracking entries
        let mut table = self
            .key_to_clients
            .write()
            .unwrap_or_else(|e| e.into_inner());
        let mut removed = 0usize;
        table.retain(|_key, clients| {
            if clients.remove(&client_id) {
                removed += 1;
            }
            !clients.is_empty()
        });
        self.total_entries
            .fetch_sub(removed, std::sync::atomic::Ordering::Relaxed);
    }

    /// Check if a client has tracking enabled
    pub fn is_tracking(&self, client_id: u64) -> bool {
        self.client_states
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&client_id)
            .is_some_and(|s| s.enabled)
    }

    /// Get the tracking state for a client
    pub fn get_state(&self, client_id: u64) -> Option<ClientTrackingState> {
        self.client_states
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&client_id)
            .cloned()
    }

    /// Record that a client has read a key (default tracking mode).
    ///
    /// In default mode, every key read by a tracked client is registered
    /// so that mutations trigger invalidation.
    pub fn track_key(&self, client_id: u64, key: &Bytes) {
        let states = self.client_states.read().unwrap_or_else(|e| e.into_inner());
        let state = match states.get(&client_id) {
            Some(s) if s.enabled => s,
            _ => return,
        };

        // In broadcast mode, we don't track individual keys
        if state.mode == TrackingMode::Broadcast {
            return;
        }

        // In opt-in mode, only track if CLIENT CACHING YES was sent
        if state.mode == TrackingMode::OptIn && !state.caching_next {
            return;
        }

        // Check per-client limit
        if state.tracked_key_count >= self.max_keys_per_client {
            return;
        }

        // Check global limit
        let current_total = self
            .total_entries
            .load(std::sync::atomic::Ordering::Relaxed);
        if current_total >= self.max_table_size {
            return;
        }

        drop(states);

        // Add to tracking table
        let mut table = self
            .key_to_clients
            .write()
            .unwrap_or_else(|e| e.into_inner());
        let clients = table.entry(key.clone()).or_default();
        if clients.insert(client_id) {
            self.total_entries
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            // Increment per-client count
            let mut states = self.client_states.write().unwrap_or_else(|e| e.into_inner());
            if let Some(state) = states.get_mut(&client_id) {
                state.tracked_key_count += 1;
                // Reset caching_next flag for opt-in mode
                if state.mode == TrackingMode::OptIn {
                    state.caching_next = false;
                }
            }
        }
    }

    /// Set the CLIENT CACHING YES flag for opt-in tracking mode
    pub fn set_caching(&self, client_id: u64, yes: bool) {
        let mut states = self.client_states.write().unwrap_or_else(|e| e.into_inner());
        if let Some(state) = states.get_mut(&client_id) {
            state.caching_next = yes;
        }
    }

    /// Get the list of client IDs that need invalidation for a modified key.
    ///
    /// This removes the key from the tracking table (one-shot invalidation)
    /// and also checks broadcast-mode clients whose prefixes match.
    pub fn get_invalidations(&self, key: &Bytes) -> Vec<InvalidationTarget> {
        let mut targets = Vec::new();

        // Default/OptIn mode: remove the key's tracking entries (one-shot)
        {
            let mut table = self
                .key_to_clients
                .write()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(client_ids) = table.remove(key) {
                let count = client_ids.len();
                self.total_entries
                    .fetch_sub(count, std::sync::atomic::Ordering::Relaxed);

                let states = self.client_states.read().unwrap_or_else(|e| e.into_inner());
                for client_id in client_ids {
                    if let Some(state) = states.get(&client_id) {
                        let target_id = if state.redirect >= 0 {
                            state.redirect as u64
                        } else {
                            client_id
                        };
                        targets.push(InvalidationTarget {
                            client_id: target_id,
                            key: key.clone(),
                        });
                    }
                }

                // Decrement per-client counts
                drop(states);
                let mut states = self.client_states.write().unwrap_or_else(|e| e.into_inner());
                for target in &targets {
                    if let Some(state) = states.get_mut(&target.client_id) {
                        state.tracked_key_count = state.tracked_key_count.saturating_sub(1);
                    }
                }
            }
        }

        // Broadcast mode: check all broadcast clients for matching prefixes
        {
            let states = self.client_states.read().unwrap_or_else(|e| e.into_inner());
            for (client_id, state) in states.iter() {
                if state.mode != TrackingMode::Broadcast || !state.enabled {
                    continue;
                }

                let should_notify = if state.prefixes.is_empty() {
                    true // Empty prefixes = all keys
                } else {
                    state
                        .prefixes
                        .iter()
                        .any(|prefix| key.starts_with(prefix.as_ref()))
                };

                if should_notify {
                    let target_id = if state.redirect >= 0 {
                        state.redirect as u64
                    } else {
                        *client_id
                    };
                    // Avoid duplicates if client was already in per-key tracking
                    if !targets.iter().any(|t| t.client_id == target_id) {
                        targets.push(InvalidationTarget {
                            client_id: target_id,
                            key: key.clone(),
                        });
                    }
                }
            }
        }

        targets
    }

    /// Get statistics about the tracking table
    pub fn stats(&self) -> TrackingStats {
        let table = self
            .key_to_clients
            .read()
            .unwrap_or_else(|e| e.into_inner());
        let states = self.client_states.read().unwrap_or_else(|e| e.into_inner());

        TrackingStats {
            tracking_clients: states.values().filter(|s| s.enabled).count(),
            tracked_keys: table.len(),
            total_entries: self
                .total_entries
                .load(std::sync::atomic::Ordering::Relaxed),
            broadcast_clients: states
                .values()
                .filter(|s| s.enabled && s.mode == TrackingMode::Broadcast)
                .count(),
        }
    }
}

impl Default for TrackingTable {
    fn default() -> Self {
        Self::new()
    }
}

/// Target for an invalidation message
#[derive(Debug, Clone)]
pub struct InvalidationTarget {
    /// Client ID to send the invalidation to
    pub client_id: u64,
    /// Key that was invalidated
    pub key: Bytes,
}

/// Statistics about the tracking table
#[derive(Debug, Clone)]
pub struct TrackingStats {
    /// Number of clients with tracking enabled
    pub tracking_clients: usize,
    /// Number of unique keys being tracked
    pub tracked_keys: usize,
    /// Total tracking entries (sum of all clients × keys)
    pub total_entries: usize,
    /// Number of clients in broadcast mode
    pub broadcast_clients: usize,
}

/// Shared tracking table type
pub type SharedTrackingTable = std::sync::Arc<TrackingTable>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enable_disable_tracking() {
        let table = TrackingTable::new();

        assert!(!table.is_tracking(1));

        table.enable_tracking(1, TrackingMode::Default, -1, vec![]);
        assert!(table.is_tracking(1));

        table.disable_tracking(1);
        assert!(!table.is_tracking(1));
    }

    #[test]
    fn test_track_and_invalidate() {
        let table = TrackingTable::new();
        let key = Bytes::from("mykey");

        // Enable tracking for client 1
        table.enable_tracking(1, TrackingMode::Default, -1, vec![]);

        // Track a key
        table.track_key(1, &key);

        // Modify the key — should get invalidation
        let targets = table.get_invalidations(&key);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].client_id, 1);

        // Second invalidation for same key — should be empty (one-shot)
        let targets = table.get_invalidations(&key);
        assert!(targets.is_empty());
    }

    #[test]
    fn test_tracking_redirect() {
        let table = TrackingTable::new();
        let key = Bytes::from("mykey");

        // Client 1 tracks with redirect to client 2
        table.enable_tracking(1, TrackingMode::Default, 2, vec![]);
        table.track_key(1, &key);

        let targets = table.get_invalidations(&key);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].client_id, 2);
    }

    #[test]
    fn test_broadcast_mode() {
        let table = TrackingTable::new();
        let key = Bytes::from("user:123");

        // Client 1 in broadcast mode with prefix "user:"
        table.enable_tracking(
            1,
            TrackingMode::Broadcast,
            -1,
            vec![Bytes::from("user:")],
        );

        // Should get invalidation for matching key
        let targets = table.get_invalidations(&key);
        assert_eq!(targets.len(), 1);

        // Should NOT get invalidation for non-matching key
        let targets = table.get_invalidations(&Bytes::from("order:456"));
        assert!(targets.is_empty());
    }

    #[test]
    fn test_multiple_clients_tracking_same_key() {
        let table = TrackingTable::new();
        let key = Bytes::from("shared");

        table.enable_tracking(1, TrackingMode::Default, -1, vec![]);
        table.enable_tracking(2, TrackingMode::Default, -1, vec![]);

        table.track_key(1, &key);
        table.track_key(2, &key);

        let targets = table.get_invalidations(&key);
        assert_eq!(targets.len(), 2);
    }

    #[test]
    fn test_disable_cleans_up() {
        let table = TrackingTable::new();
        let key = Bytes::from("mykey");

        table.enable_tracking(1, TrackingMode::Default, -1, vec![]);
        table.track_key(1, &key);

        // Disable removes all entries
        table.disable_tracking(1);

        let targets = table.get_invalidations(&key);
        assert!(targets.is_empty());
    }

    #[test]
    fn test_stats() {
        let table = TrackingTable::new();

        table.enable_tracking(1, TrackingMode::Default, -1, vec![]);
        table.enable_tracking(2, TrackingMode::Broadcast, -1, vec![]);
        table.track_key(1, &Bytes::from("a"));
        table.track_key(1, &Bytes::from("b"));

        let stats = table.stats();
        assert_eq!(stats.tracking_clients, 2);
        assert_eq!(stats.tracked_keys, 2);
        assert_eq!(stats.total_entries, 2);
        assert_eq!(stats.broadcast_clients, 1);
    }
}
