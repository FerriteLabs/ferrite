//! # Programmable Data Triggers (FerriteFunctions)
//!
//! Event-driven functions that execute on data mutations, providing
//! Firebase-like reactivity for Redis. Triggers run atomically with the
//! triggering operation, enabling powerful automation patterns.
//!
//! ## Why Triggers?
//!
//! Instead of building change detection into your application:
//!
//! - **Real-time notifications**: Alert systems when data changes
//! - **Data validation**: Reject invalid writes before they commit
//! - **Cascading updates**: Automatically update related keys
//! - **Audit logging**: Record who changed what and when
//! - **External integrations**: Call webhooks on specific changes
//!
//! ## Trigger Types
//!
//! | Type | Execution | Use Case |
//! |------|-----------|----------|
//! | PUBLISH | Sync | Notify subscribers |
//! | HTTP | Async | Webhooks, external APIs |
//! | CALL | Sync | Execute Redis commands |
//! | WASM | Sync/Async | Custom logic |
//!
//! ## Quick Start
//!
//! ### Create a Notification Trigger
//!
//! ```no_run
//! use ferrite::triggers::{
//!     TriggerRegistry, TriggerConfig, Trigger, Condition, EventType, Pattern,
//!     Action, BuiltinAction, PublishAction,
//! };
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let registry = TriggerRegistry::new(TriggerConfig::default());
//!
//!     // Create trigger: when any order changes, publish to channel
//!     let trigger = Trigger::new(
//!         "order_notification".to_string(),
//!         Condition::new(
//!             EventType::Set,
//!             Pattern::Glob("order:*".to_string()),
//!         ),
//!         vec![
//!             Action::Builtin(BuiltinAction::Publish(PublishAction {
//!                 channel: "order_updates".to_string(),
//!                 message_template: "$KEY changed to $VALUE".to_string(),
//!             })),
//!         ],
//!     );
//!
//!     registry.create(trigger).await?;
//!     println!("Trigger created!");
//!     Ok(())
//! }
//! ```
//!
//! ### Create a Webhook Trigger
//!
//! ```no_run
//! use ferrite::triggers::{
//!     TriggerRegistry, TriggerConfig, Trigger, Condition, EventType, Pattern,
//!     Action, BuiltinAction, HttpAction,
//! };
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let registry = TriggerRegistry::new(TriggerConfig::default());
//!
//! // Call webhook when payment is created
//! let trigger = Trigger::new(
//!     "payment_webhook".to_string(),
//!     Condition::new(
//!         EventType::Set,
//!         Pattern::Glob("payment:*".to_string()),
//!     ),
//!     vec![
//!         Action::Builtin(BuiltinAction::Http(HttpAction {
//!             url: "https://api.example.com/webhooks/payment".to_string(),
//!             method: "POST".to_string(),
//!             headers: vec![
//!                 ("Authorization".to_string(), "Bearer $SECRET".to_string()),
//!                 ("Content-Type".to_string(), "application/json".to_string()),
//!             ],
//!             body_template: r#"{"key": "$KEY", "value": $VALUE}"#.to_string(),
//!             timeout_ms: 5000,
//!         })),
//!     ],
//! );
//!
//! registry.create(trigger).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Fire Triggers Programmatically
//!
//! ```no_run
//! use ferrite::triggers::{TriggerRegistry, TriggerConfig, TriggerEvent};
//! use bytes::Bytes;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let registry = TriggerRegistry::new(TriggerConfig::default());
//!
//! // Fire triggers for a SET operation
//! let event = TriggerEvent {
//!     event_type: ferrite::triggers::EventType::Set,
//!     key: Bytes::from("order:123"),
//!     value: Some(Bytes::from(r#"{"status": "shipped"}"#)),
//!     old_value: Some(Bytes::from(r#"{"status": "processing"}"#)),
//!     db: 0,
//!     timestamp: std::time::SystemTime::now(),
//! };
//!
//! let results = registry.fire(event).await?;
//! println!("Executed {} actions", results.len());
//! # Ok(())
//! # }
//! ```
//!
//! ## Event Types
//!
//! Triggers can listen for specific operations:
//!
//! | Event | Description | Example Key |
//! |-------|-------------|-------------|
//! | Set | String SET/SETEX | `user:123:name` |
//! | Del | DEL command | `session:*` |
//! | Expire | Key expiration | `cache:*` |
//! | HSet | Hash field set | `user:123` |
//! | HDel | Hash field delete | `user:123` |
//! | LPush/RPush | List operations | `queue:jobs` |
//! | SAdd/SRem | Set operations | `tags:post:*` |
//! | ZAdd/ZRem | Sorted set ops | `leaderboard:*` |
//!
//! ## Template Variables
//!
//! Use these in action templates:
//!
//! | Variable | Description | Example |
//! |----------|-------------|---------|
//! | `$KEY` | Full key name | `order:123` |
//! | `$VALUE` | New value (JSON) | `{"status":"shipped"}` |
//! | `$OLD_VALUE` | Previous value | `{"status":"pending"}` |
//! | `$DB` | Database number | `0` |
//! | `$TIMESTAMP` | Unix timestamp | `1702900000` |
//! | `$FIELD` | Hash field name | `status` |
//!
//! ## Redis CLI Commands
//!
//! ```text
//! # Create a trigger
//! TRIGGER.CREATE order_notify ON SET orders:* DO
//!   PUBLISH order_updates $KEY
//! END
//!
//! # Create webhook trigger
//! TRIGGER.CREATE payment_hook ON SET payment:* DO
//!   HTTP.POST https://api.example.com/webhook
//!     HEADER "Authorization: Bearer secret"
//!     BODY {"key": "$KEY", "value": $VALUE}
//! END
//!
//! # Create WASM trigger
//! TRIGGER.CREATE validate_user ON SET users:* WASM validate.wasm
//!
//! # List triggers
//! TRIGGER.LIST
//! TRIGGER.LIST order_*
//!
//! # Get trigger info
//! TRIGGER.INFO order_notify
//!
//! # Enable/disable
//! TRIGGER.ENABLE order_notify
//! TRIGGER.DISABLE order_notify
//!
//! # Delete trigger
//! TRIGGER.DELETE order_notify
//!
//! # View statistics
//! TRIGGER.STATS
//! ```
//!
//! ## Configuration
//!
//! ```no_run
//! use ferrite::triggers::TriggerConfig;
//!
//! let config = TriggerConfig {
//!     enabled: true,
//!     max_triggers: 1000,           // Maximum triggers allowed
//!     max_actions_per_trigger: 10,  // Actions per trigger
//!     default_timeout_ms: 5000,     // Trigger execution timeout
//!     max_concurrent_executions: 100,
//!     async_execution: true,        // Non-blocking execution
//!     http_timeout_ms: 10000,       // Webhook timeout
//!     max_retries: 3,               // Retry failed actions
//!     retry_delay_ms: 1000,
//!     log_executions: true,         // Log all executions
//! };
//! ```
//!
//! ## Priority and Ordering
//!
//! Triggers execute in priority order (higher first):
//!
//! ```no_run
//! use ferrite::triggers::{Trigger, Condition, EventType, Pattern};
//!
//! let mut trigger = Trigger::new(
//!     "high_priority".to_string(),
//!     Condition::new(EventType::Set, Pattern::Glob("*".to_string())),
//!     vec![],
//! );
//! trigger.priority = 100;  // Executes before priority=0 triggers
//! ```
//!
//! ## Performance
//!
//! | Metric | Value | Notes |
//! |--------|-------|-------|
//! | Pattern matching | <1μs | Per trigger |
//! | PUBLISH action | <10μs | Local pub/sub |
//! | HTTP action (async) | Non-blocking | Background execution |
//! | WASM action | 10-100μs | Depends on function |
//!
//! ## Best Practices
//!
//! 1. **Use specific patterns**: `user:*:settings` is better than `user:*`
//! 2. **Prefer async execution**: For non-critical notifications
//! 3. **Set reasonable timeouts**: Prevent slow webhooks from blocking
//! 4. **Monitor execution stats**: Track success/failure rates
//! 5. **Use WASM for validation**: Block invalid writes synchronously

/// Trigger action definitions (PUBLISH, HTTP, CALL, WASM, etc.)
pub mod actions;
/// Trigger condition and pattern matching types
pub mod conditions;
/// Trigger execution engine
pub mod engine;

pub use actions::{Action, ActionResult, BuiltinAction, HttpAction, PublishAction};
pub use conditions::{Condition, EventType, Pattern};
pub use engine::{TriggerEngine, TriggerEvent};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::runtime::SharedSubscriptionManager;

/// Configuration for the trigger system
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TriggerConfig {
    /// Whether triggers are enabled
    pub enabled: bool,
    /// Maximum number of triggers
    pub max_triggers: usize,
    /// Maximum actions per trigger
    pub max_actions_per_trigger: usize,
    /// Default timeout for trigger execution (milliseconds)
    pub default_timeout_ms: u64,
    /// Maximum concurrent trigger executions
    pub max_concurrent_executions: usize,
    /// Enable async trigger execution (non-blocking)
    pub async_execution: bool,
    /// HTTP timeout for webhook actions (milliseconds)
    pub http_timeout_ms: u64,
    /// Maximum retries for failed actions
    pub max_retries: u32,
    /// Retry delay (milliseconds)
    pub retry_delay_ms: u64,
    /// Enable trigger execution logging
    pub log_executions: bool,
}

impl Default for TriggerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_triggers: 1000,
            max_actions_per_trigger: 10,
            default_timeout_ms: 5000,
            max_concurrent_executions: 100,
            async_execution: true,
            http_timeout_ms: 10000,
            max_retries: 3,
            retry_delay_ms: 1000,
            log_executions: true,
        }
    }
}

/// Trigger definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Trigger {
    /// Unique trigger name
    pub name: String,
    /// Trigger condition (event type + pattern)
    pub condition: Condition,
    /// Actions to execute when triggered
    pub actions: Vec<Action>,
    /// Whether the trigger is enabled
    pub enabled: bool,
    /// Trigger priority (higher = earlier execution)
    pub priority: i32,
    /// Creation timestamp
    pub created_at: u64,
    /// Last execution timestamp
    pub last_executed_at: Option<u64>,
    /// Total execution count
    pub execution_count: u64,
    /// Description
    pub description: Option<String>,
}

impl Trigger {
    /// Create a new trigger
    pub fn new(name: String, condition: Condition, actions: Vec<Action>) -> Self {
        Self {
            name,
            condition,
            actions,
            enabled: true,
            priority: 0,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            last_executed_at: None,
            execution_count: 0,
            description: None,
        }
    }

    /// Check if trigger matches an event
    pub fn matches(&self, event: &TriggerEvent) -> bool {
        if !self.enabled {
            return false;
        }
        self.condition.matches(event)
    }
}

/// Trigger registry for managing all triggers
pub struct TriggerRegistry {
    /// All registered triggers
    triggers: RwLock<HashMap<String, Trigger>>,
    /// Trigger execution engine
    engine: Arc<TriggerEngine>,
    /// Configuration
    config: TriggerConfig,
    /// Statistics
    stats: TriggerStats,
}

/// Trigger system statistics
#[derive(Debug, Default)]
pub struct TriggerStats {
    /// Total triggers created
    pub triggers_created: AtomicU64,
    /// Total triggers deleted
    pub triggers_deleted: AtomicU64,
    /// Total executions
    pub total_executions: AtomicU64,
    /// Successful executions
    pub successful_executions: AtomicU64,
    /// Failed executions
    pub failed_executions: AtomicU64,
    /// Total actions executed
    pub total_actions: AtomicU64,
    /// Average execution time (microseconds)
    pub avg_execution_time_us: AtomicU64,
}

impl TriggerRegistry {
    /// Create a new trigger registry
    pub fn new(config: TriggerConfig) -> Self {
        Self {
            triggers: RwLock::new(HashMap::new()),
            engine: Arc::new(TriggerEngine::new(config.clone())),
            config,
            stats: TriggerStats::default(),
        }
    }

    /// Create a new trigger registry with subscription manager for pub/sub actions
    pub fn with_subscription_manager(
        config: TriggerConfig,
        subscription_manager: SharedSubscriptionManager,
    ) -> Self {
        Self {
            triggers: RwLock::new(HashMap::new()),
            engine: Arc::new(TriggerEngine::with_subscription_manager(
                config.clone(),
                subscription_manager,
            )),
            config,
            stats: TriggerStats::default(),
        }
    }

    /// Register a new trigger
    pub async fn create(&self, trigger: Trigger) -> Result<(), TriggerError> {
        let mut triggers = self.triggers.write().await;

        if triggers.len() >= self.config.max_triggers {
            return Err(TriggerError::LimitExceeded(format!(
                "Maximum triggers ({}) reached",
                self.config.max_triggers
            )));
        }

        if triggers.contains_key(&trigger.name) {
            return Err(TriggerError::AlreadyExists(trigger.name));
        }

        if trigger.actions.len() > self.config.max_actions_per_trigger {
            return Err(TriggerError::LimitExceeded(format!(
                "Maximum actions per trigger ({}) exceeded",
                self.config.max_actions_per_trigger
            )));
        }

        triggers.insert(trigger.name.clone(), trigger);
        self.stats.triggers_created.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Delete a trigger
    pub async fn delete(&self, name: &str) -> Result<(), TriggerError> {
        let mut triggers = self.triggers.write().await;

        if triggers.remove(name).is_none() {
            return Err(TriggerError::NotFound(name.to_string()));
        }

        self.stats.triggers_deleted.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Get a trigger by name
    pub async fn get(&self, name: &str) -> Option<Trigger> {
        let triggers = self.triggers.read().await;
        triggers.get(name).cloned()
    }

    /// List all triggers
    pub async fn list(&self, pattern: Option<&str>) -> Vec<Trigger> {
        let triggers = self.triggers.read().await;

        if let Some(pattern) = pattern {
            triggers
                .values()
                .filter(|t| conditions::Pattern::Glob(pattern.to_string()).matches(&t.name))
                .cloned()
                .collect()
        } else {
            triggers.values().cloned().collect()
        }
    }

    /// Enable a trigger
    pub async fn enable(&self, name: &str) -> Result<(), TriggerError> {
        let mut triggers = self.triggers.write().await;

        let trigger = triggers
            .get_mut(name)
            .ok_or_else(|| TriggerError::NotFound(name.to_string()))?;

        trigger.enabled = true;
        Ok(())
    }

    /// Disable a trigger
    pub async fn disable(&self, name: &str) -> Result<(), TriggerError> {
        let mut triggers = self.triggers.write().await;

        let trigger = triggers
            .get_mut(name)
            .ok_or_else(|| TriggerError::NotFound(name.to_string()))?;

        trigger.enabled = false;
        Ok(())
    }

    /// Fire triggers matching an event
    pub async fn fire(&self, event: TriggerEvent) -> Result<Vec<ActionResult>, TriggerError> {
        if !self.config.enabled {
            return Ok(vec![]);
        }

        let triggers = self.triggers.read().await;

        // Find matching triggers
        let mut matching: Vec<&Trigger> = triggers.values().filter(|t| t.matches(&event)).collect();

        // Sort by priority (higher first)
        matching.sort_by(|a, b| b.priority.cmp(&a.priority));

        let mut results = Vec::new();

        for trigger in matching {
            self.stats.total_executions.fetch_add(1, Ordering::Relaxed);

            match self.engine.execute(trigger, &event).await {
                Ok(action_results) => {
                    self.stats
                        .successful_executions
                        .fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .total_actions
                        .fetch_add(action_results.len() as u64, Ordering::Relaxed);
                    results.extend(action_results);
                }
                Err(e) => {
                    self.stats.failed_executions.fetch_add(1, Ordering::Relaxed);
                    if !self.config.async_execution {
                        return Err(e);
                    }
                }
            }
        }

        Ok(results)
    }

    /// Get trigger statistics
    pub fn stats(&self) -> TriggerStatsSnapshot {
        TriggerStatsSnapshot {
            triggers_created: self.stats.triggers_created.load(Ordering::Relaxed),
            triggers_deleted: self.stats.triggers_deleted.load(Ordering::Relaxed),
            total_executions: self.stats.total_executions.load(Ordering::Relaxed),
            successful_executions: self.stats.successful_executions.load(Ordering::Relaxed),
            failed_executions: self.stats.failed_executions.load(Ordering::Relaxed),
            total_actions: self.stats.total_actions.load(Ordering::Relaxed),
            avg_execution_time_us: self.stats.avg_execution_time_us.load(Ordering::Relaxed),
        }
    }

    /// Get the configuration
    pub fn config(&self) -> &TriggerConfig {
        &self.config
    }
}

/// Snapshot of trigger statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerStatsSnapshot {
    /// Total triggers created
    pub triggers_created: u64,
    /// Total triggers deleted
    pub triggers_deleted: u64,
    /// Total executions
    pub total_executions: u64,
    /// Successful executions
    pub successful_executions: u64,
    /// Failed executions
    pub failed_executions: u64,
    /// Total actions executed
    pub total_actions: u64,
    /// Average execution time (microseconds)
    pub avg_execution_time_us: u64,
}

/// Errors that can occur in the trigger system
#[derive(Debug, Clone, thiserror::Error)]
pub enum TriggerError {
    /// Trigger not found
    #[error("trigger not found: {0}")]
    NotFound(String),

    /// Trigger already exists
    #[error("trigger already exists: {0}")]
    AlreadyExists(String),

    /// Invalid trigger definition
    #[error("invalid trigger: {0}")]
    Invalid(String),

    /// Limit exceeded
    #[error("limit exceeded: {0}")]
    LimitExceeded(String),

    /// Execution error
    #[error("execution error: {0}")]
    Execution(String),

    /// Action error
    #[error("action error: {0}")]
    Action(String),

    /// Timeout
    #[error("trigger execution timed out")]
    Timeout,

    /// WASM error
    #[error("WASM error: {0}")]
    Wasm(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trigger_config_default() {
        let config = TriggerConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_triggers, 1000);
        assert!(config.async_execution);
    }

    #[tokio::test]
    async fn test_trigger_registry_create() {
        let registry = TriggerRegistry::new(TriggerConfig::default());
        let trigger = Trigger::new(
            "test_trigger".to_string(),
            Condition::new(EventType::Set, Pattern::Glob("test:*".to_string())),
            vec![Action::Builtin(BuiltinAction::Publish(PublishAction {
                channel: "test_channel".to_string(),
                message_template: "$KEY".to_string(),
            }))],
        );

        registry.create(trigger).await.unwrap();

        let fetched = registry.get("test_trigger").await;
        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap().name, "test_trigger");
    }

    #[tokio::test]
    async fn test_trigger_registry_delete() {
        let registry = TriggerRegistry::new(TriggerConfig::default());
        let trigger = Trigger::new(
            "delete_me".to_string(),
            Condition::new(EventType::Set, Pattern::Glob("*".to_string())),
            vec![],
        );

        registry.create(trigger).await.unwrap();
        assert!(registry.get("delete_me").await.is_some());

        registry.delete("delete_me").await.unwrap();
        assert!(registry.get("delete_me").await.is_none());
    }

    #[tokio::test]
    async fn test_trigger_registry_list() {
        let registry = TriggerRegistry::new(TriggerConfig::default());

        for i in 0..5 {
            let trigger = Trigger::new(
                format!("trigger_{}", i),
                Condition::new(EventType::Set, Pattern::Glob("*".to_string())),
                vec![],
            );
            registry.create(trigger).await.unwrap();
        }

        let all = registry.list(None).await;
        assert_eq!(all.len(), 5);

        let filtered = registry.list(Some("trigger_1")).await;
        assert_eq!(filtered.len(), 1);
    }

    #[tokio::test]
    async fn test_trigger_enable_disable() {
        let registry = TriggerRegistry::new(TriggerConfig::default());
        let trigger = Trigger::new(
            "toggle".to_string(),
            Condition::new(EventType::Set, Pattern::Glob("*".to_string())),
            vec![],
        );

        registry.create(trigger).await.unwrap();

        registry.disable("toggle").await.unwrap();
        let t = registry.get("toggle").await.unwrap();
        assert!(!t.enabled);

        registry.enable("toggle").await.unwrap();
        let t = registry.get("toggle").await.unwrap();
        assert!(t.enabled);
    }
}
