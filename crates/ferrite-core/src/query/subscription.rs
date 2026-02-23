//! Real-time query subscription system for FerriteQL.
//!
//! Allows clients to subscribe to query results and receive updates when
//! underlying data changes. Subscriptions are tracked by key patterns
//! extracted from query FROM/WHERE clauses, enabling efficient change
//! detection without re-evaluating every active subscription on every write.
//!
//! # RESP Commands
//!
//! - `QUERY.SUBSCRIBE "<sql>"` — subscribe to live query results
//! - `QUERY.UNSUBSCRIBE <id>` — cancel a subscription
//! - `QUERY.SUBSCRIPTIONS` — list active subscriptions
//!
//! # Example
//!
//! ```ignore
//! let config = SubscriptionConfig::default();
//! let manager = SubscriptionManager::new(config);
//! let (tx, mut rx) = tokio::sync::mpsc::channel(64);
//!
//! let id = manager.subscribe("SELECT * FROM users:*", tx).await?;
//! // When a key matching `users:*` changes, the query is re-evaluated
//! // and the new ResultSet is sent through `rx`.
//! manager.notify_change("users:42").await;
//! manager.unsubscribe(id).await;
//! ```

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use dashmap::DashMap;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::query::{QueryError, ResultSet};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors specific to the subscription subsystem.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SubscriptionError {
    /// Subscription not found for the given id.
    #[error("subscription not found: {0}")]
    NotFound(SubscriptionId),

    /// Maximum number of concurrent subscriptions reached.
    #[error("subscription limit reached (max {max})")]
    LimitReached {
        /// The configured maximum.
        max: usize,
    },

    /// The underlying query failed to parse or execute.
    #[error("query error: {0}")]
    Query(#[from] QueryError),

    /// The subscriber channel has been closed.
    #[error("subscriber channel closed")]
    ChannelClosed,
}

// ---------------------------------------------------------------------------
// Identifiers & metadata
// ---------------------------------------------------------------------------

/// Unique identifier for a subscription, backed by a UUID v4.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionId(Uuid);

impl SubscriptionId {
    /// Create a new random subscription id.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Return the inner UUID.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for SubscriptionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SubscriptionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Metadata about a single active subscription.
#[derive(Debug, Clone)]
pub struct SubscriptionInfo {
    /// Unique subscription id.
    pub id: SubscriptionId,
    /// The original FerriteQL query string.
    pub query: String,
    /// Key patterns this subscription depends on.
    pub key_patterns: Vec<String>,
    /// When the subscription was created.
    pub created_at: Instant,
    /// When the last result was pushed to the subscriber.
    pub last_update: Option<Instant>,
    /// Total number of result updates delivered.
    pub update_count: u64,
}

/// Aggregated statistics across all subscriptions.
#[derive(Debug, Clone, Default)]
pub struct SubscriptionStats {
    /// Number of currently active subscriptions.
    pub active_count: u64,
    /// Total result updates delivered across all subscriptions.
    pub total_updates: u64,
    /// Total query re-evaluations triggered by change notifications.
    pub total_evaluations: u64,
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration knobs for the subscription subsystem.
#[derive(Debug, Clone)]
pub struct SubscriptionConfig {
    /// Maximum number of concurrent subscriptions (0 = unlimited).
    pub max_subscriptions: usize,
    /// Debounce window in milliseconds — changes arriving within this
    /// window after the first notification are coalesced into a single
    /// re-evaluation.
    pub evaluation_debounce_ms: u64,
    /// Maximum number of rows returned per subscription update.
    pub max_results_per_update: usize,
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            max_subscriptions: 10_000,
            evaluation_debounce_ms: 50,
            max_results_per_update: 1_000,
        }
    }
}

impl SubscriptionConfig {
    /// Validate configuration values, returning an error on invalid input.
    pub fn validate(&self) -> Result<(), SubscriptionError> {
        if self.max_results_per_update == 0 {
            return Err(SubscriptionError::Query(QueryError::Parse(
                "max_results_per_update must be > 0".into(),
            )));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Change tracker — key-pattern dependency extraction
// ---------------------------------------------------------------------------

/// Tracks which key patterns a subscription depends on, enabling efficient
/// change-notification routing.
#[derive(Debug, Clone)]
pub struct ChangeTracker {
    /// Glob-style key patterns extracted from FROM/WHERE clauses.
    patterns: Vec<String>,
}

impl ChangeTracker {
    /// Build a `ChangeTracker` by extracting key patterns from a query string.
    ///
    /// Patterns are derived from:
    /// - `FROM <pattern>` clauses (e.g. `FROM users:*`)
    /// - String literals in `WHERE` equality checks on `__key`
    ///
    /// The extraction is intentionally lightweight — a full AST walk is
    /// deferred to the query planner integration.
    pub fn from_query(query: &str) -> Self {
        let mut patterns = Vec::new();
        let lower = query.to_ascii_lowercase();
        let tokens: Vec<&str> = lower.split_whitespace().collect();

        for (i, token) in tokens.iter().enumerate() {
            // FROM <pattern>
            if *token == "from" {
                if let Some(pattern) = tokens.get(i + 1) {
                    let cleaned = pattern.trim_matches(|c: char| c == '\'' || c == '"' || c == ',');
                    if !cleaned.is_empty() && !is_sql_keyword(cleaned) {
                        patterns.push(cleaned.to_string());
                    }
                }
            }

            // WHERE __key = '<literal>' / WHERE __key LIKE '<pattern>'
            if *token == "__key" || *token == "key" {
                if let Some(op) = tokens.get(i + 1) {
                    if *op == "=" || *op == "like" {
                        if let Some(val) = tokens.get(i + 2) {
                            let cleaned =
                                val.trim_matches(|c: char| c == '\'' || c == '"' || c == ';');
                            if !cleaned.is_empty() {
                                patterns.push(cleaned.to_string());
                            }
                        }
                    }
                }
            }
        }

        // Fallback: if we couldn't extract any patterns, match everything.
        if patterns.is_empty() {
            patterns.push("*".to_string());
        }

        Self { patterns }
    }

    /// Return the extracted key patterns.
    pub fn patterns(&self) -> &[String] {
        &self.patterns
    }

    /// Check whether a concrete key matches any tracked pattern.
    pub fn matches_key(&self, key: &str) -> bool {
        self.patterns.iter().any(|p| glob_match(p, key))
    }
}

/// Minimal glob matching supporting `*` (any chars) and `?` (single char).
fn glob_match(pattern: &str, value: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let val: Vec<char> = value.chars().collect();
    glob_match_inner(&pat, &val)
}

fn glob_match_inner(pat: &[char], val: &[char]) -> bool {
    match (pat.first(), val.first()) {
        (None, None) => true,
        (Some('*'), _) => {
            // '*' matches zero chars or consumes one and keeps matching.
            glob_match_inner(&pat[1..], val)
                || (!val.is_empty() && glob_match_inner(pat, &val[1..]))
        }
        (Some('?'), Some(_)) => glob_match_inner(&pat[1..], &val[1..]),
        (Some(a), Some(b)) if a == b => glob_match_inner(&pat[1..], &val[1..]),
        _ => false,
    }
}

/// Quick check to skip SQL keywords when extracting FROM targets.
fn is_sql_keyword(s: &str) -> bool {
    matches!(
        s,
        "select" | "where" | "group" | "order" | "limit" | "having" | "join" | "on" | "as"
    )
}

// ---------------------------------------------------------------------------
// Internal subscription entry
// ---------------------------------------------------------------------------

/// Internal bookkeeping for a single subscription.
struct SubscriptionEntry {
    id: SubscriptionId,
    query: String,
    tracker: ChangeTracker,
    sender: mpsc::Sender<ResultSet>,
    created_at: Instant,
    last_update: Option<Instant>,
    update_count: u64,
}

impl SubscriptionEntry {
    fn info(&self) -> SubscriptionInfo {
        SubscriptionInfo {
            id: self.id,
            query: self.query.clone(),
            key_patterns: self.tracker.patterns().to_vec(),
            created_at: self.created_at,
            last_update: self.last_update,
            update_count: self.update_count,
        }
    }
}

// ---------------------------------------------------------------------------
// SubscriptionManager
// ---------------------------------------------------------------------------

/// Manages active FerriteQL query subscriptions.
///
/// Thread-safe — all public methods may be called concurrently from any
/// task or thread.
pub struct SubscriptionManager {
    /// Active subscriptions keyed by id.
    subscriptions: DashMap<SubscriptionId, SubscriptionEntry>,
    /// Reverse index: key pattern → set of subscription ids.
    pattern_index: DashMap<String, HashSet<SubscriptionId>>,
    /// Configuration.
    config: SubscriptionConfig,
    /// Global counters.
    total_updates: AtomicU64,
    total_evaluations: AtomicU64,
}

impl SubscriptionManager {
    /// Create a new `SubscriptionManager` with the given configuration.
    pub fn new(config: SubscriptionConfig) -> Self {
        Self {
            subscriptions: DashMap::new(),
            pattern_index: DashMap::new(),
            config,
            total_updates: AtomicU64::new(0),
            total_evaluations: AtomicU64::new(0),
        }
    }

    /// Subscribe to live results for the given FerriteQL query.
    ///
    /// Returns a [`SubscriptionId`] that can later be used to unsubscribe.
    /// Query results (and subsequent updates) are delivered through
    /// `callback_channel`.
    pub async fn subscribe(
        &self,
        query: &str,
        callback_channel: mpsc::Sender<ResultSet>,
    ) -> Result<SubscriptionId, SubscriptionError> {
        // Enforce subscription limit.
        if self.config.max_subscriptions > 0
            && self.subscriptions.len() >= self.config.max_subscriptions
        {
            return Err(SubscriptionError::LimitReached {
                max: self.config.max_subscriptions,
            });
        }

        let id = SubscriptionId::new();
        let tracker = ChangeTracker::from_query(query);

        // Update reverse index.
        for pattern in tracker.patterns() {
            self.pattern_index
                .entry(pattern.clone())
                .or_default()
                .insert(id);
        }

        let entry = SubscriptionEntry {
            id,
            query: query.to_string(),
            tracker,
            sender: callback_channel,
            created_at: Instant::now(),
            last_update: None,
            update_count: 0,
        };

        self.subscriptions.insert(id, entry);
        Ok(id)
    }

    /// Remove a subscription.
    pub async fn unsubscribe(&self, id: SubscriptionId) -> Result<(), SubscriptionError> {
        let (_, entry) = self
            .subscriptions
            .remove(&id)
            .ok_or(SubscriptionError::NotFound(id))?;

        // Clean up reverse index.
        for pattern in entry.tracker.patterns() {
            if let Some(mut set) = self.pattern_index.get_mut(pattern) {
                set.remove(&id);
                if set.is_empty() {
                    drop(set);
                    self.pattern_index.remove(pattern);
                }
            }
        }

        Ok(())
    }

    /// Notify the manager that a key has changed.
    ///
    /// All subscriptions whose tracked patterns match the key will be
    /// marked for re-evaluation. In a full integration the manager would
    /// re-execute the query through the [`QueryExecutor`] and push the
    /// new [`ResultSet`] to subscribers. Here we construct a lightweight
    /// notification result.
    pub async fn notify_change(&self, key: &str) {
        // Collect matching subscription ids.
        let mut affected: HashSet<SubscriptionId> = HashSet::new();

        for entry in self.subscriptions.iter() {
            if entry.tracker.matches_key(key) {
                affected.insert(entry.id);
            }
        }

        self.total_evaluations
            .fetch_add(affected.len() as u64, Ordering::Relaxed);

        for id in &affected {
            if let Some(mut entry) = self.subscriptions.get_mut(id) {
                // Build a lightweight notification result.
                // In production this would re-execute the query against the store.
                let result = ResultSet::new(
                    vec!["__key".to_string(), "__event".to_string()],
                    vec![crate::query::Row::new(vec![
                        crate::query::Value::String(key.to_string()),
                        crate::query::Value::String("change".to_string()),
                    ])],
                );

                match entry.sender.try_send(result) {
                    Ok(()) => {
                        entry.update_count += 1;
                        entry.last_update = Some(Instant::now());
                        self.total_updates.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        // Back-pressure: skip this update rather than block.
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        // Subscriber dropped — will be cleaned up on next
                        // unsubscribe or GC sweep.
                    }
                }
            }
        }
    }

    /// List all active subscriptions.
    pub fn list_subscriptions(&self) -> Vec<SubscriptionInfo> {
        self.subscriptions.iter().map(|e| e.info()).collect()
    }

    /// Return aggregated statistics.
    pub fn get_stats(&self) -> SubscriptionStats {
        SubscriptionStats {
            active_count: self.subscriptions.len() as u64,
            total_updates: self.total_updates.load(Ordering::Relaxed),
            total_evaluations: self.total_evaluations.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// RESP command integration types
// ---------------------------------------------------------------------------

/// Parsed RESP commands for the subscription subsystem.
#[derive(Debug, Clone)]
pub enum SubscriptionCommand {
    /// `QUERY.SUBSCRIBE "<sql>"`
    Subscribe {
        /// The FerriteQL query to subscribe to.
        query: String,
    },
    /// `QUERY.UNSUBSCRIBE <id>`
    Unsubscribe {
        /// The subscription id to cancel.
        id: String,
    },
    /// `QUERY.SUBSCRIPTIONS`
    List,
}

impl SubscriptionCommand {
    /// Try to parse a RESP command from its constituent parts.
    ///
    /// Returns `None` if the command is not a subscription command.
    pub fn parse(parts: &[&str]) -> Option<Self> {
        if parts.is_empty() {
            return None;
        }

        let cmd = parts[0].to_ascii_uppercase();
        match cmd.as_str() {
            "QUERY.SUBSCRIBE" => {
                let query = parts.get(1)?;
                Some(Self::Subscribe {
                    query: query.trim_matches('"').to_string(),
                })
            }
            "QUERY.UNSUBSCRIBE" => {
                let id = parts.get(1)?;
                Some(Self::Unsubscribe { id: id.to_string() })
            }
            "QUERY.SUBSCRIPTIONS" => Some(Self::List),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- ChangeTracker / pattern matching ------------------------------------

    #[test]
    fn test_change_tracker_from_simple_query() {
        let tracker = ChangeTracker::from_query("SELECT * FROM users:*");
        assert_eq!(tracker.patterns(), &["users:*"]);
    }

    #[test]
    fn test_change_tracker_multiple_froms() {
        let tracker =
            ChangeTracker::from_query("SELECT * FROM orders:* WHERE __key = 'orders:123'");
        assert!(tracker.patterns().contains(&"orders:*".to_string()));
        assert!(tracker.patterns().contains(&"orders:123".to_string()));
    }

    #[test]
    fn test_change_tracker_fallback_wildcard() {
        let tracker = ChangeTracker::from_query("SELECT 1");
        assert_eq!(tracker.patterns(), &["*"]);
    }

    #[test]
    fn test_glob_match_star() {
        assert!(glob_match("users:*", "users:42"));
        assert!(glob_match("users:*", "users:"));
        assert!(!glob_match("users:*", "orders:1"));
    }

    #[test]
    fn test_glob_match_question() {
        assert!(glob_match("key?", "key1"));
        assert!(!glob_match("key?", "key12"));
    }

    #[test]
    fn test_glob_match_exact() {
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "exactx"));
    }

    #[test]
    fn test_tracker_matches_key() {
        let tracker = ChangeTracker::from_query("SELECT * FROM users:*");
        assert!(tracker.matches_key("users:99"));
        assert!(!tracker.matches_key("orders:1"));
    }

    // -- SubscriptionManager ------------------------------------------------

    #[tokio::test]
    async fn test_subscribe_and_unsubscribe() {
        let manager = SubscriptionManager::new(SubscriptionConfig::default());
        let (tx, _rx) = mpsc::channel(16);

        let id = manager
            .subscribe("SELECT * FROM users:*", tx)
            .await
            .unwrap();

        assert_eq!(manager.list_subscriptions().len(), 1);

        manager.unsubscribe(id).await.unwrap();
        assert_eq!(manager.list_subscriptions().len(), 0);
    }

    #[tokio::test]
    async fn test_unsubscribe_not_found() {
        let manager = SubscriptionManager::new(SubscriptionConfig::default());
        let result = manager.unsubscribe(SubscriptionId::new()).await;
        assert!(matches!(result, Err(SubscriptionError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_notify_change_delivers_result() {
        let manager = SubscriptionManager::new(SubscriptionConfig::default());
        let (tx, mut rx) = mpsc::channel(16);

        manager
            .subscribe("SELECT * FROM users:*", tx)
            .await
            .unwrap();

        manager.notify_change("users:42").await;

        let result = rx.try_recv().unwrap();
        assert_eq!(result.columns, vec!["__key", "__event"]);
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_notify_no_match() {
        let manager = SubscriptionManager::new(SubscriptionConfig::default());
        let (tx, mut rx) = mpsc::channel(16);

        manager
            .subscribe("SELECT * FROM users:*", tx)
            .await
            .unwrap();

        manager.notify_change("orders:1").await;

        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_subscription_limit() {
        let config = SubscriptionConfig {
            max_subscriptions: 2,
            ..Default::default()
        };
        let manager = SubscriptionManager::new(config);

        let (tx1, _r1) = mpsc::channel(1);
        let (tx2, _r2) = mpsc::channel(1);
        let (tx3, _r3) = mpsc::channel(1);

        manager.subscribe("SELECT 1", tx1).await.unwrap();
        manager.subscribe("SELECT 2", tx2).await.unwrap();

        let result = manager.subscribe("SELECT 3", tx3).await;
        assert!(matches!(
            result,
            Err(SubscriptionError::LimitReached { max: 2 })
        ));
    }

    // -- Stats --------------------------------------------------------------

    #[tokio::test]
    async fn test_stats_tracking() {
        let manager = SubscriptionManager::new(SubscriptionConfig::default());
        let (tx, mut rx) = mpsc::channel(64);

        manager.subscribe("SELECT * FROM k:*", tx).await.unwrap();

        manager.notify_change("k:1").await;
        manager.notify_change("k:2").await;

        // Drain the channel so sends succeed.
        let _ = rx.try_recv();
        let _ = rx.try_recv();

        let stats = manager.get_stats();
        assert_eq!(stats.active_count, 1);
        assert_eq!(stats.total_updates, 2);
        assert_eq!(stats.total_evaluations, 2);
    }

    // -- Config validation --------------------------------------------------

    #[test]
    fn test_config_default_is_valid() {
        SubscriptionConfig::default().validate().unwrap();
    }

    #[test]
    fn test_config_invalid_max_results() {
        let config = SubscriptionConfig {
            max_results_per_update: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    // -- SubscriptionCommand parsing ----------------------------------------

    #[test]
    fn test_parse_subscribe_command() {
        let cmd = SubscriptionCommand::parse(&["QUERY.SUBSCRIBE", "\"SELECT * FROM t\""]);
        assert!(matches!(cmd, Some(SubscriptionCommand::Subscribe { .. })));
    }

    #[test]
    fn test_parse_unsubscribe_command() {
        let cmd = SubscriptionCommand::parse(&["QUERY.UNSUBSCRIBE", "abc-123"]);
        assert!(matches!(cmd, Some(SubscriptionCommand::Unsubscribe { .. })));
    }

    #[test]
    fn test_parse_list_command() {
        let cmd = SubscriptionCommand::parse(&["QUERY.SUBSCRIPTIONS"]);
        assert!(matches!(cmd, Some(SubscriptionCommand::List)));
    }

    #[test]
    fn test_parse_unknown_command() {
        let cmd = SubscriptionCommand::parse(&["UNKNOWN"]);
        assert!(cmd.is_none());
    }

    // -- SubscriptionInfo ---------------------------------------------------

    #[tokio::test]
    async fn test_subscription_info_fields() {
        let manager = SubscriptionManager::new(SubscriptionConfig::default());
        let (tx, _rx) = mpsc::channel(16);

        let id = manager
            .subscribe("SELECT * FROM events:*", tx)
            .await
            .unwrap();

        let subs = manager.list_subscriptions();
        assert_eq!(subs.len(), 1);
        let info = &subs[0];
        assert_eq!(info.id, id);
        assert_eq!(info.query, "SELECT * FROM events:*");
        assert!(info.key_patterns.contains(&"events:*".to_string()));
        assert!(info.last_update.is_none());
        assert_eq!(info.update_count, 0);
    }
}
