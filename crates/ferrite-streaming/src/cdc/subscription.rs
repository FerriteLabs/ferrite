//! CDC subscription management
//!
//! Pattern-based subscriptions with offset tracking and delivery modes.

use super::event::{ChangeEvent, Operation};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::broadcast;

/// Subscription identifier
pub type SubscriptionId = u64;

/// A CDC subscription
#[derive(Debug)]
pub struct Subscription {
    /// Subscription ID
    pub id: SubscriptionId,
    /// Name (for consumer groups)
    pub name: String,
    /// Key patterns to subscribe to
    pub patterns: Vec<String>,
    /// Operations to capture (empty = all)
    pub operations: HashSet<String>,
    /// Databases to monitor (empty = all)
    pub databases: HashSet<u8>,
    /// Current position in change log
    position: AtomicU64,
    /// Output format
    pub format: OutputFormat,
    /// Delivery mode
    pub delivery: DeliveryMode,
    /// Whether to capture old values
    pub with_old_values: bool,
    /// Subscription state
    pub state: SubscriptionState,
    /// Created timestamp
    pub created_at: SystemTime,
    /// Last active timestamp
    pub last_active: AtomicU64,
}

impl Subscription {
    /// Create a new subscription
    pub fn new(name: impl Into<String>, patterns: Vec<String>) -> Self {
        Self {
            id: 0,
            name: name.into(),
            patterns,
            operations: HashSet::new(),
            databases: HashSet::new(),
            position: AtomicU64::new(0),
            format: OutputFormat::Json,
            delivery: DeliveryMode::Poll,
            with_old_values: false,
            state: SubscriptionState::Active,
            created_at: SystemTime::now(),
            last_active: AtomicU64::new(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
            ),
        }
    }

    /// Set subscription ID
    pub fn with_id(mut self, id: SubscriptionId) -> Self {
        self.id = id;
        self
    }

    /// Set operations to capture
    pub fn with_operations(mut self, ops: HashSet<String>) -> Self {
        self.operations = ops;
        self
    }

    /// Set databases to monitor
    pub fn with_databases(mut self, dbs: HashSet<u8>) -> Self {
        self.databases = dbs;
        self
    }

    /// Set output format
    pub fn with_format(mut self, format: OutputFormat) -> Self {
        self.format = format;
        self
    }

    /// Set delivery mode
    pub fn with_delivery(mut self, delivery: DeliveryMode) -> Self {
        self.delivery = delivery;
        self
    }

    /// Set starting position
    pub fn from_position(self, position: u64) -> Self {
        self.position.store(position, Ordering::SeqCst);
        self
    }

    /// Enable old value capture
    pub fn with_old_values(mut self) -> Self {
        self.with_old_values = true;
        self
    }

    /// Check if subscription matches a key
    pub fn matches_key(&self, key: &[u8]) -> bool {
        if self.patterns.is_empty() || self.patterns.iter().any(|p| p == "*") {
            return true;
        }

        let key_str = String::from_utf8_lossy(key);
        self.patterns
            .iter()
            .any(|pattern| glob_match(pattern, &key_str))
    }

    /// Check if subscription matches an operation
    pub fn matches_operation(&self, op: &Operation) -> bool {
        if self.operations.is_empty() {
            return true;
        }
        self.operations.contains(op.name())
    }

    /// Check if subscription matches a database
    pub fn matches_database(&self, db: u8) -> bool {
        if self.databases.is_empty() {
            return true;
        }
        self.databases.contains(&db)
    }

    /// Check if subscription matches an event
    pub fn matches(&self, event: &ChangeEvent) -> bool {
        self.state == SubscriptionState::Active
            && self.matches_key(&event.key)
            && self.matches_operation(&event.operation)
            && self.matches_database(event.db)
    }

    /// Get current position
    pub fn position(&self) -> u64 {
        self.position.load(Ordering::SeqCst)
    }

    /// Update position
    pub fn update_position(&self, new_position: u64) {
        self.position.store(new_position, Ordering::SeqCst);
        self.last_active.store(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            Ordering::SeqCst,
        );
    }

    /// Acknowledge events up to and including the given ID
    pub fn acknowledge(&self, event_id: u64) {
        let current = self.position.load(Ordering::SeqCst);
        if event_id >= current {
            self.position.store(event_id + 1, Ordering::SeqCst);
        }
    }

    /// Seek to a position
    pub fn seek(&self, position: u64) {
        self.position.store(position, Ordering::SeqCst);
    }

    /// Pause the subscription
    pub fn pause(&mut self) {
        self.state = SubscriptionState::Paused;
    }

    /// Resume the subscription
    pub fn resume(&mut self) {
        self.state = SubscriptionState::Active;
    }

    /// Get subscription info as a map
    pub fn info(&self) -> SubscriptionInfo {
        SubscriptionInfo {
            id: self.id,
            name: self.name.clone(),
            patterns: self.patterns.clone(),
            operations: self.operations.iter().cloned().collect(),
            databases: self.databases.iter().copied().collect(),
            position: self.position(),
            format: self.format.clone(),
            delivery: self.delivery.clone(),
            with_old_values: self.with_old_values,
            state: self.state.clone(),
            created_at: self.created_at,
        }
    }
}

/// Subscription state
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubscriptionState {
    /// Subscription is active
    #[default]
    Active,
    /// Subscription is paused
    Paused,
    /// Subscription is being deleted
    Deleting,
}

impl SubscriptionState {
    /// Get state name
    pub fn name(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Deleting => "deleting",
        }
    }
}

/// Output format for events
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum OutputFormat {
    /// JSON format
    #[default]
    Json,
    /// Avro with schema registry
    Avro {
        /// Schema registry URL
        schema_registry_url: String,
    },
    /// Protocol Buffers
    Protobuf,
    /// RESP3 format (for Redis clients)
    Resp3,
}

impl OutputFormat {
    /// Get format name
    pub fn name(&self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Avro { .. } => "avro",
            Self::Protobuf => "protobuf",
            Self::Resp3 => "resp3",
        }
    }

    /// Parse format from string
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "json" => Some(Self::Json),
            "avro" => Some(Self::Avro {
                schema_registry_url: String::new(),
            }),
            "protobuf" | "proto" => Some(Self::Protobuf),
            "resp3" | "resp" => Some(Self::Resp3),
            _ => None,
        }
    }

    /// Format an event
    pub fn format_event(&self, event: &ChangeEvent) -> String {
        match self {
            Self::Json => event.to_json(),
            Self::Avro { .. } => {
                // Would use avro serialization
                event.to_json()
            }
            Self::Protobuf => {
                // Would use protobuf serialization
                event.to_json()
            }
            Self::Resp3 => {
                // Format as RESP3 map
                format!(
                    "*6\r\n$2\r\nid\r\n:{}\r\n$3\r\nkey\r\n${}\r\n{}\r\n$2\r\nop\r\n${}\r\n{}\r\n",
                    event.id,
                    event.key.len(),
                    String::from_utf8_lossy(&event.key),
                    event.operation.name().len(),
                    event.operation.name()
                )
            }
        }
    }
}

/// Delivery mode for subscriptions
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum DeliveryMode {
    /// Push to connected client
    Push,
    /// Client polls for changes
    #[default]
    Poll,
    /// Forward to external sink
    Sink {
        /// Sink name
        sink_name: String,
    },
}

impl DeliveryMode {
    /// Get delivery mode name
    pub fn name(&self) -> &'static str {
        match self {
            Self::Push => "push",
            Self::Poll => "poll",
            Self::Sink { .. } => "sink",
        }
    }
}

/// Subscription information (serializable)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubscriptionInfo {
    /// Subscription ID
    pub id: SubscriptionId,
    /// Name
    pub name: String,
    /// Key patterns
    pub patterns: Vec<String>,
    /// Operations
    pub operations: Vec<String>,
    /// Databases
    pub databases: Vec<u8>,
    /// Current position
    pub position: u64,
    /// Output format
    pub format: OutputFormat,
    /// Delivery mode
    pub delivery: DeliveryMode,
    /// Whether to capture old values
    pub with_old_values: bool,
    /// State
    pub state: SubscriptionState,
    /// Created timestamp
    #[serde(with = "system_time_serde")]
    pub created_at: SystemTime,
}

/// Manager for CDC subscriptions
pub struct SubscriptionManager {
    /// Subscriptions by ID
    subscriptions: DashMap<SubscriptionId, Arc<Subscription>>,
    /// Subscriptions by name
    by_name: DashMap<String, SubscriptionId>,
    /// Next subscription ID
    next_id: AtomicU64,
    /// Broadcast channel for push notifications
    broadcast: broadcast::Sender<ChangeEvent>,
}

impl SubscriptionManager {
    /// Create a new subscription manager
    pub fn new() -> Self {
        let (broadcast, _) = broadcast::channel(10000);
        Self {
            subscriptions: DashMap::new(),
            by_name: DashMap::new(),
            next_id: AtomicU64::new(1),
            broadcast,
        }
    }

    /// Create a subscription
    pub fn create(&self, subscription: Subscription) -> Result<SubscriptionId, String> {
        // Check if name already exists
        if self.by_name.contains_key(&subscription.name) {
            return Err(format!(
                "Subscription '{}' already exists",
                subscription.name
            ));
        }

        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let subscription = subscription.with_id(id);
        let name = subscription.name.clone();

        self.subscriptions.insert(id, Arc::new(subscription));
        self.by_name.insert(name, id);

        Ok(id)
    }

    /// Get a subscription by ID
    pub fn get(&self, id: SubscriptionId) -> Option<Arc<Subscription>> {
        self.subscriptions.get(&id).map(|s| Arc::clone(&s))
    }

    /// Get a subscription by name
    pub fn get_by_name(&self, name: &str) -> Option<Arc<Subscription>> {
        self.by_name
            .get(name)
            .and_then(|id| self.subscriptions.get(&id).map(|s| Arc::clone(&s)))
    }

    /// Delete a subscription
    pub fn delete(&self, name: &str) -> bool {
        if let Some((_, id)) = self.by_name.remove(name) {
            self.subscriptions.remove(&id);
            true
        } else {
            false
        }
    }

    /// List all subscriptions
    pub fn list(&self) -> Vec<SubscriptionInfo> {
        self.subscriptions
            .iter()
            .map(|s| s.value().info())
            .collect()
    }

    /// Check if any subscription matches an event
    pub fn matches(&self, event: &ChangeEvent) -> bool {
        self.subscriptions.iter().any(|s| s.value().matches(event))
    }

    /// Get all subscriptions that match an event
    pub fn matching(&self, event: &ChangeEvent) -> Vec<Arc<Subscription>> {
        self.subscriptions
            .iter()
            .filter(|s| s.value().matches(event))
            .map(|s| Arc::clone(s.value()))
            .collect()
    }

    /// Notify all matching subscriptions of an event
    pub async fn notify(&self, event: ChangeEvent) {
        // Send to broadcast channel for push subscribers
        let _ = self.broadcast.send(event);
    }

    /// Subscribe to push notifications
    pub fn subscribe_push(&self) -> broadcast::Receiver<ChangeEvent> {
        self.broadcast.subscribe()
    }

    /// Get number of subscriptions
    pub fn len(&self) -> usize {
        self.subscriptions.len()
    }

    /// Check if there are no subscriptions
    pub fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple glob pattern matching
fn glob_match(pattern: &str, text: &str) -> bool {
    let mut p = pattern.chars().peekable();
    let mut t = text.chars().peekable();

    fn match_impl(
        pattern: &mut std::iter::Peekable<std::str::Chars<'_>>,
        text: &mut std::iter::Peekable<std::str::Chars<'_>>,
    ) -> bool {
        while let Some(&pc) = pattern.peek() {
            match pc {
                '*' => {
                    pattern.next();
                    while pattern.peek() == Some(&'*') {
                        pattern.next();
                    }
                    if pattern.peek().is_none() {
                        return true;
                    }
                    while text.peek().is_some() {
                        let mut p_clone = pattern.clone();
                        let mut t_clone = text.clone();
                        if match_impl(&mut p_clone, &mut t_clone) {
                            return true;
                        }
                        text.next();
                    }
                    return false;
                }
                '?' => {
                    pattern.next();
                    if text.next().is_none() {
                        return false;
                    }
                }
                c => {
                    pattern.next();
                    if text.next() != Some(c) {
                        return false;
                    }
                }
            }
        }
        text.peek().is_none()
    }

    match_impl(&mut p, &mut t)
}

// Serde helper for SystemTime
mod system_time_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    pub fn serialize<S>(time: &SystemTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let millis = time
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;
        millis.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<SystemTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = u64::deserialize(deserializer)?;
        Ok(UNIX_EPOCH + Duration::from_millis(millis))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("user:*", "user:123"));
        assert!(glob_match("user:*", "user:"));
        assert!(!glob_match("user:*", "order:123"));
        assert!(glob_match("user:*:name", "user:123:name"));
        assert!(!glob_match("user:*:name", "user:123:email"));
        assert!(glob_match("user:?", "user:1"));
        assert!(!glob_match("user:?", "user:12"));
    }

    #[test]
    fn test_subscription_matches_key() {
        let sub = Subscription::new("test", vec!["user:*".to_string()]);
        assert!(sub.matches_key(b"user:123"));
        assert!(sub.matches_key(b"user:abc"));
        assert!(!sub.matches_key(b"order:123"));
    }

    #[test]
    fn test_subscription_matches_operation() {
        let mut sub = Subscription::new("test", vec!["*".to_string()]);
        sub.operations.insert("SET".to_string());
        sub.operations.insert("DEL".to_string());

        assert!(sub.matches_operation(&Operation::Set));
        assert!(sub.matches_operation(&Operation::Del));
        assert!(!sub.matches_operation(&Operation::Append));
    }

    #[test]
    fn test_subscription_matches_database() {
        let mut sub = Subscription::new("test", vec!["*".to_string()]);
        sub.databases.insert(0);
        sub.databases.insert(1);

        assert!(sub.matches_database(0));
        assert!(sub.matches_database(1));
        assert!(!sub.matches_database(2));
    }

    #[test]
    fn test_subscription_matches_event() {
        let sub = Subscription::new("test", vec!["user:*".to_string()]);

        let event = ChangeEvent::new(Operation::Set, Bytes::from_static(b"user:123"), 0);
        assert!(sub.matches(&event));

        let event = ChangeEvent::new(Operation::Set, Bytes::from_static(b"order:123"), 0);
        assert!(!sub.matches(&event));
    }

    #[test]
    fn test_subscription_position() {
        let sub = Subscription::new("test", vec!["*".to_string()]).from_position(100);

        assert_eq!(sub.position(), 100);

        sub.update_position(150);
        assert_eq!(sub.position(), 150);

        sub.acknowledge(160);
        assert_eq!(sub.position(), 161);
    }

    #[test]
    fn test_subscription_manager() {
        let manager = SubscriptionManager::new();

        let sub = Subscription::new("test", vec!["user:*".to_string()]);
        let id = manager.create(sub).unwrap();

        assert_eq!(manager.len(), 1);

        let retrieved = manager.get(id).unwrap();
        assert_eq!(retrieved.name, "test");

        let by_name = manager.get_by_name("test").unwrap();
        assert_eq!(by_name.id, id);

        // Duplicate name should fail
        let sub2 = Subscription::new("test", vec!["*".to_string()]);
        assert!(manager.create(sub2).is_err());

        // Delete
        assert!(manager.delete("test"));
        assert_eq!(manager.len(), 0);
    }

    #[test]
    fn test_output_format() {
        assert_eq!(OutputFormat::Json.name(), "json");
        assert!(matches!(
            OutputFormat::from_str("json"),
            Some(OutputFormat::Json)
        ));
        assert!(matches!(
            OutputFormat::from_str("RESP3"),
            Some(OutputFormat::Resp3)
        ));
    }

    #[test]
    fn test_subscription_state() {
        let mut sub = Subscription::new("test", vec!["*".to_string()]);
        assert_eq!(sub.state, SubscriptionState::Active);

        sub.pause();
        assert_eq!(sub.state, SubscriptionState::Paused);

        sub.resume();
        assert_eq!(sub.state, SubscriptionState::Active);
    }
}
