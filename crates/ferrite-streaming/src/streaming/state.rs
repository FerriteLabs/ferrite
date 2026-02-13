//! State management for stream processing

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::StreamError;

/// State store for stateful stream processing
pub struct StateStore {
    /// Store name
    name: String,
    /// Key-value state
    state: Arc<RwLock<HashMap<String, StateValue>>>,
    /// Changelog for recovery
    changelog: Arc<RwLock<Vec<StateChange>>>,
    /// Configuration
    config: StateStoreConfig,
}

/// State store configuration
#[derive(Debug, Clone)]
pub struct StateStoreConfig {
    /// Enable changelog for recovery
    pub changelog_enabled: bool,
    /// Enable caching
    pub caching_enabled: bool,
    /// Cache size
    pub cache_size: usize,
    /// Time-to-live for state entries
    pub ttl_ms: Option<u64>,
}

impl Default for StateStoreConfig {
    fn default() -> Self {
        Self {
            changelog_enabled: true,
            caching_enabled: true,
            cache_size: 10000,
            ttl_ms: None,
        }
    }
}

/// State value with metadata
#[derive(Debug, Clone)]
pub struct StateValue {
    /// The value
    pub value: serde_json::Value,
    /// Last update timestamp
    pub timestamp: u64,
    /// Version for optimistic locking
    pub version: u64,
}

impl StateValue {
    /// Create a new state value
    pub fn new(value: serde_json::Value) -> Self {
        Self {
            value,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            version: 1,
        }
    }

    /// Update the value
    pub fn update(&mut self, value: serde_json::Value) {
        self.value = value;
        self.timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.version += 1;
    }
}

/// State change for changelog
#[derive(Debug, Clone)]
pub struct StateChange {
    /// Key that changed
    pub key: String,
    /// Old value (None for inserts)
    pub old_value: Option<serde_json::Value>,
    /// New value (None for deletes)
    pub new_value: Option<serde_json::Value>,
    /// Change timestamp
    pub timestamp: u64,
}

impl StateStore {
    /// Create a new state store
    pub fn new(name: String) -> Self {
        Self {
            name,
            state: Arc::new(RwLock::new(HashMap::new())),
            changelog: Arc::new(RwLock::new(Vec::new())),
            config: StateStoreConfig::default(),
        }
    }

    /// Create with configuration
    pub fn with_config(name: String, config: StateStoreConfig) -> Self {
        Self {
            name,
            state: Arc::new(RwLock::new(HashMap::new())),
            changelog: Arc::new(RwLock::new(Vec::new())),
            config,
        }
    }

    /// Get store name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get a value by key
    pub async fn get(&self, key: &str) -> Option<serde_json::Value> {
        let state = self.state.read().await;
        state.get(key).map(|v| v.value.clone())
    }

    /// Get value with metadata
    pub async fn get_with_metadata(&self, key: &str) -> Option<StateValue> {
        let state = self.state.read().await;
        state.get(key).cloned()
    }

    /// Put a value
    pub async fn put(&self, key: &str, value: serde_json::Value) -> Result<(), StreamError> {
        let mut state = self.state.write().await;

        let old_value = state.get(key).map(|v| v.value.clone());

        if let Some(existing) = state.get_mut(key) {
            existing.update(value.clone());
        } else {
            state.insert(key.to_string(), StateValue::new(value.clone()));
        }

        // Record changelog
        if self.config.changelog_enabled {
            let mut changelog = self.changelog.write().await;
            changelog.push(StateChange {
                key: key.to_string(),
                old_value,
                new_value: Some(value),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            });
        }

        Ok(())
    }

    /// Put if absent (returns true if inserted)
    pub async fn put_if_absent(
        &self,
        key: &str,
        value: serde_json::Value,
    ) -> Result<bool, StreamError> {
        let mut state = self.state.write().await;

        if state.contains_key(key) {
            return Ok(false);
        }

        state.insert(key.to_string(), StateValue::new(value.clone()));

        if self.config.changelog_enabled {
            let mut changelog = self.changelog.write().await;
            changelog.push(StateChange {
                key: key.to_string(),
                old_value: None,
                new_value: Some(value),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            });
        }

        Ok(true)
    }

    /// Delete a value
    pub async fn delete(&self, key: &str) -> Result<Option<serde_json::Value>, StreamError> {
        let mut state = self.state.write().await;

        let old = state.remove(key);
        let old_value = old.map(|v| v.value);

        if self.config.changelog_enabled && old_value.is_some() {
            let mut changelog = self.changelog.write().await;
            changelog.push(StateChange {
                key: key.to_string(),
                old_value: old_value.clone(),
                new_value: None,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            });
        }

        Ok(old_value)
    }

    /// Get all keys
    pub async fn keys(&self) -> Vec<String> {
        let state = self.state.read().await;
        state.keys().cloned().collect()
    }

    /// Get all entries
    pub async fn all(&self) -> HashMap<String, serde_json::Value> {
        let state = self.state.read().await;
        state
            .iter()
            .map(|(k, v)| (k.clone(), v.value.clone()))
            .collect()
    }

    /// Get entries by prefix
    pub async fn prefix_scan(&self, prefix: &str) -> HashMap<String, serde_json::Value> {
        let state = self.state.read().await;
        state
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.value.clone()))
            .collect()
    }

    /// Get entries in range
    pub async fn range(&self, start: &str, end: &str) -> HashMap<String, serde_json::Value> {
        let state = self.state.read().await;
        state
            .iter()
            .filter(|(k, _)| k.as_str() >= start && k.as_str() < end)
            .map(|(k, v)| (k.clone(), v.value.clone()))
            .collect()
    }

    /// Count entries
    pub async fn count(&self) -> usize {
        let state = self.state.read().await;
        state.len()
    }

    /// Clear all state
    pub async fn clear(&self) {
        let mut state = self.state.write().await;
        state.clear();
    }

    /// Get changelog
    pub async fn get_changelog(&self) -> Vec<StateChange> {
        let changelog = self.changelog.read().await;
        changelog.clone()
    }

    /// Clear changelog
    pub async fn clear_changelog(&self) {
        let mut changelog = self.changelog.write().await;
        changelog.clear();
    }

    /// Apply changelog for recovery
    pub async fn apply_changelog(&self, changes: Vec<StateChange>) -> Result<(), StreamError> {
        let mut state = self.state.write().await;

        for change in changes {
            match change.new_value {
                Some(value) => {
                    state.insert(change.key, StateValue::new(value));
                }
                None => {
                    state.remove(&change.key);
                }
            }
        }

        Ok(())
    }
}

/// Keyed state store for partitioned state
pub struct KeyedStateStore {
    /// Base name
    name: String,
    /// Stores by key
    stores: Arc<RwLock<HashMap<String, Arc<StateStore>>>>,
    /// Configuration
    config: StateStoreConfig,
}

impl KeyedStateStore {
    /// Create a new keyed state store
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            stores: Arc::new(RwLock::new(HashMap::new())),
            config: StateStoreConfig::default(),
        }
    }

    /// Get or create store for key
    pub async fn get_store(&self, key: &str) -> Arc<StateStore> {
        let stores = self.stores.read().await;
        if let Some(store) = stores.get(key) {
            return store.clone();
        }
        drop(stores);

        let mut stores = self.stores.write().await;
        if let Some(store) = stores.get(key) {
            return store.clone();
        }

        let store = Arc::new(StateStore::with_config(
            format!("{}-{}", self.name, key),
            self.config.clone(),
        ));
        stores.insert(key.to_string(), store.clone());
        store
    }

    /// Get value for keyed state
    pub async fn get(&self, partition_key: &str, state_key: &str) -> Option<serde_json::Value> {
        let store = self.get_store(partition_key).await;
        store.get(state_key).await
    }

    /// Put value for keyed state
    pub async fn put(
        &self,
        partition_key: &str,
        state_key: &str,
        value: serde_json::Value,
    ) -> Result<(), StreamError> {
        let store = self.get_store(partition_key).await;
        store.put(state_key, value).await
    }

    /// Delete value for keyed state
    pub async fn delete(
        &self,
        partition_key: &str,
        state_key: &str,
    ) -> Result<Option<serde_json::Value>, StreamError> {
        let store = self.get_store(partition_key).await;
        store.delete(state_key).await
    }

    /// Get all partition keys
    pub async fn partition_keys(&self) -> Vec<String> {
        let stores = self.stores.read().await;
        stores.keys().cloned().collect()
    }
}

/// Window state store for windowed aggregations
pub struct WindowStateStore {
    /// Base name
    name: String,
    /// State by window key
    windows: Arc<RwLock<HashMap<WindowKey, serde_json::Value>>>,
}

/// Key for window state
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WindowKey {
    /// Partition key
    pub key: String,
    /// Window start
    pub window_start: u64,
    /// Window end
    pub window_end: u64,
}

impl WindowKey {
    /// Create a new window key
    pub fn new(key: &str, window_start: u64, window_end: u64) -> Self {
        Self {
            key: key.to_string(),
            window_start,
            window_end,
        }
    }
}

impl WindowStateStore {
    /// Create a new window state store
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            windows: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get window state
    pub async fn get(&self, key: &WindowKey) -> Option<serde_json::Value> {
        let windows = self.windows.read().await;
        windows.get(key).cloned()
    }

    /// Put window state
    pub async fn put(&self, key: WindowKey, value: serde_json::Value) {
        let mut windows = self.windows.write().await;
        windows.insert(key, value);
    }

    /// Delete window state
    pub async fn delete(&self, key: &WindowKey) -> Option<serde_json::Value> {
        let mut windows = self.windows.write().await;
        windows.remove(key)
    }

    /// Get all windows for a key
    pub async fn get_windows(&self, key: &str) -> Vec<(WindowKey, serde_json::Value)> {
        let windows = self.windows.read().await;
        windows
            .iter()
            .filter(|(k, _)| k.key == key)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Get windows in time range
    pub async fn get_windows_in_range(
        &self,
        key: &str,
        start: u64,
        end: u64,
    ) -> Vec<(WindowKey, serde_json::Value)> {
        let windows = self.windows.read().await;
        windows
            .iter()
            .filter(|(k, _)| k.key == key && k.window_start >= start && k.window_end <= end)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Delete expired windows
    pub async fn delete_expired(&self, watermark: u64) -> usize {
        let mut windows = self.windows.write().await;
        let before = windows.len();
        windows.retain(|k, _| k.window_end > watermark);
        before - windows.len()
    }

    /// Count windows
    pub async fn count(&self) -> usize {
        let windows = self.windows.read().await;
        windows.len()
    }

    /// Clear all windows
    pub async fn clear(&self) {
        let mut windows = self.windows.write().await;
        windows.clear();
    }
}

/// Session state store for session windows
pub struct SessionStateStore {
    /// Base name
    name: String,
    /// Sessions by key
    sessions: Arc<RwLock<HashMap<String, Vec<Session>>>>,
    /// Session gap
    gap_ms: u64,
}

/// Session data
#[derive(Debug, Clone)]
pub struct Session {
    /// Session start
    pub start: u64,
    /// Session end
    pub end: u64,
    /// Session state
    pub state: serde_json::Value,
}

impl Session {
    /// Create a new session
    pub fn new(start: u64, end: u64, state: serde_json::Value) -> Self {
        Self { start, end, state }
    }

    /// Check if timestamp is in session
    pub fn contains(&self, timestamp: u64) -> bool {
        timestamp >= self.start && timestamp < self.end
    }

    /// Check if sessions overlap or are adjacent
    pub fn overlaps_or_adjacent(&self, other: &Session, gap_ms: u64) -> bool {
        self.start <= other.end + gap_ms && other.start <= self.end + gap_ms
    }

    /// Merge with another session
    pub fn merge(&mut self, other: &Session) {
        self.start = self.start.min(other.start);
        self.end = self.end.max(other.end);
    }
}

impl SessionStateStore {
    /// Create a new session state store
    pub fn new(name: &str, gap: std::time::Duration) -> Self {
        Self {
            name: name.to_string(),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            gap_ms: gap.as_millis() as u64,
        }
    }

    /// Add event to session
    pub async fn add_event(&self, key: &str, timestamp: u64, state: serde_json::Value) -> Session {
        let mut sessions = self.sessions.write().await;
        let key_sessions = sessions.entry(key.to_string()).or_insert_with(Vec::new);

        // Create new session for this event
        let new_session = Session::new(timestamp, timestamp + self.gap_ms, state);

        // Find overlapping sessions
        let mut to_merge: Vec<usize> = key_sessions
            .iter()
            .enumerate()
            .filter(|(_, s)| s.overlaps_or_adjacent(&new_session, self.gap_ms))
            .map(|(i, _)| i)
            .collect();

        if to_merge.is_empty() {
            // No overlapping sessions, add new one
            key_sessions.push(new_session.clone());
            return new_session;
        }

        // Merge all overlapping sessions
        to_merge.sort_by(|a, b| b.cmp(a)); // Sort descending for removal

        let mut merged = new_session;
        for idx in to_merge {
            let session = key_sessions.remove(idx);
            merged.merge(&session);
        }

        key_sessions.push(merged.clone());
        merged
    }

    /// Get session for timestamp
    pub async fn get_session(&self, key: &str, timestamp: u64) -> Option<Session> {
        let sessions = self.sessions.read().await;
        sessions.get(key).and_then(|s| {
            s.iter()
                .find(|session| session.contains(timestamp))
                .cloned()
        })
    }

    /// Get all sessions for key
    pub async fn get_sessions(&self, key: &str) -> Vec<Session> {
        let sessions = self.sessions.read().await;
        sessions.get(key).cloned().unwrap_or_default()
    }

    /// Delete expired sessions
    pub async fn delete_expired(&self, watermark: u64) -> usize {
        let mut sessions = self.sessions.write().await;
        let mut deleted = 0;

        for (_, key_sessions) in sessions.iter_mut() {
            let before = key_sessions.len();
            key_sessions.retain(|s| s.end > watermark);
            deleted += before - key_sessions.len();
        }

        // Remove empty keys
        sessions.retain(|_, v| !v.is_empty());

        deleted
    }
}

/// State backend trait for pluggable storage
#[async_trait::async_trait]
pub trait StateBackend: Send + Sync {
    /// Get value
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StreamError>;

    /// Put value
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StreamError>;

    /// Delete value
    async fn delete(&self, key: &[u8]) -> Result<(), StreamError>;

    /// Scan by prefix
    async fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StreamError>;

    /// Flush to storage
    async fn flush(&self) -> Result<(), StreamError>;

    /// Create checkpoint
    async fn checkpoint(&self) -> Result<u64, StreamError>;

    /// Restore from checkpoint
    async fn restore(&self, checkpoint_id: u64) -> Result<(), StreamError>;
}

/// In-memory state backend
pub struct MemoryStateBackend {
    data: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
    #[allow(clippy::type_complexity)]
    checkpoints: Arc<RwLock<HashMap<u64, HashMap<Vec<u8>, Vec<u8>>>>>,
    next_checkpoint: Arc<RwLock<u64>>,
}

impl MemoryStateBackend {
    /// Create a new memory state backend
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            checkpoints: Arc::new(RwLock::new(HashMap::new())),
            next_checkpoint: Arc::new(RwLock::new(0)),
        }
    }
}

impl Default for MemoryStateBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl StateBackend for MemoryStateBackend {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StreamError> {
        let data = self.data.read().await;
        Ok(data.get(key).cloned())
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StreamError> {
        let mut data = self.data.write().await;
        data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<(), StreamError> {
        let mut data = self.data.write().await;
        data.remove(key);
        Ok(())
    }

    async fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StreamError> {
        let data = self.data.read().await;
        Ok(data
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    async fn flush(&self) -> Result<(), StreamError> {
        Ok(())
    }

    async fn checkpoint(&self) -> Result<u64, StreamError> {
        let data = self.data.read().await;
        let mut checkpoints = self.checkpoints.write().await;
        let mut next = self.next_checkpoint.write().await;

        let id = *next;
        *next += 1;
        checkpoints.insert(id, data.clone());

        Ok(id)
    }

    async fn restore(&self, checkpoint_id: u64) -> Result<(), StreamError> {
        let checkpoints = self.checkpoints.read().await;

        if let Some(checkpoint) = checkpoints.get(&checkpoint_id) {
            let mut data = self.data.write().await;
            *data = checkpoint.clone();
            Ok(())
        } else {
            Err(StreamError::StateError(format!(
                "Checkpoint {} not found",
                checkpoint_id
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_state_store() {
        let store = StateStore::new("test".to_string());

        store.put("key1", json!({"value": 1})).await.unwrap();
        store.put("key2", json!({"value": 2})).await.unwrap();

        let value = store.get("key1").await.unwrap();
        assert_eq!(value.get("value").unwrap().as_i64(), Some(1));

        store.delete("key1").await.unwrap();
        assert!(store.get("key1").await.is_none());
    }

    #[tokio::test]
    async fn test_keyed_state_store() {
        let store = KeyedStateStore::new("test");

        store.put("partition1", "key1", json!(1)).await.unwrap();
        store.put("partition1", "key2", json!(2)).await.unwrap();
        store.put("partition2", "key1", json!(3)).await.unwrap();

        let value = store.get("partition1", "key1").await.unwrap();
        assert_eq!(value.as_i64(), Some(1));

        let value = store.get("partition2", "key1").await.unwrap();
        assert_eq!(value.as_i64(), Some(3));
    }

    #[tokio::test]
    async fn test_window_state_store() {
        let store = WindowStateStore::new("test");

        let key1 = WindowKey::new("key1", 0, 1000);
        let key2 = WindowKey::new("key1", 1000, 2000);

        store.put(key1.clone(), json!({"count": 5})).await;
        store.put(key2.clone(), json!({"count": 10})).await;

        let value = store.get(&key1).await.unwrap();
        assert_eq!(value.get("count").unwrap().as_i64(), Some(5));

        let windows = store.get_windows("key1").await;
        assert_eq!(windows.len(), 2);
    }

    #[tokio::test]
    async fn test_session_state_store() {
        let store = SessionStateStore::new("test", std::time::Duration::from_millis(100));

        // First event creates a session
        let session1 = store.add_event("user1", 0, json!(1)).await;
        assert_eq!(session1.start, 0);
        assert_eq!(session1.end, 100);

        // Event within gap extends session
        let session2 = store.add_event("user1", 50, json!(2)).await;
        assert_eq!(session2.start, 0);
        assert_eq!(session2.end, 150);

        // Event after gap creates new session
        let session3 = store.add_event("user1", 500, json!(3)).await;
        assert_eq!(session3.start, 500);
        assert_eq!(session3.end, 600);

        let sessions = store.get_sessions("user1").await;
        assert_eq!(sessions.len(), 2);
    }

    #[tokio::test]
    async fn test_memory_backend_checkpoint() {
        let backend = MemoryStateBackend::new();

        backend.put(b"key1", b"value1").await.unwrap();
        backend.put(b"key2", b"value2").await.unwrap();

        let checkpoint_id = backend.checkpoint().await.unwrap();

        backend.put(b"key3", b"value3").await.unwrap();
        backend.delete(b"key1").await.unwrap();

        // Verify current state
        assert!(backend.get(b"key1").await.unwrap().is_none());
        assert!(backend.get(b"key3").await.unwrap().is_some());

        // Restore checkpoint
        backend.restore(checkpoint_id).await.unwrap();

        // Verify restored state
        assert!(backend.get(b"key1").await.unwrap().is_some());
        assert!(backend.get(b"key3").await.unwrap().is_none());
    }
}
