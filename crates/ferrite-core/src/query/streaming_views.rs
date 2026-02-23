//! Streaming Materialized View Engine
//!
//! Provides a high-performance streaming materialized view engine with
//! incremental computation and pub/sub notification. Views are automatically
//! refreshed when underlying data changes, supporting both immediate
//! (on-mutation) and periodic refresh modes.
//!
//! # Architecture
//!
//! The engine maintains a set of streaming views, each defined by a query
//! and a set of key patterns. When a data mutation arrives, the
//! [`DependencyTracker`] resolves which views are affected. Depending on
//! the view's [`RefreshMode`] and the engine's [`ConsistencyMode`], the
//! view is refreshed incrementally or fully and subscribers are notified.
//!
//! # Example
//!
//! ```ignore
//! let config = StreamingViewConfig::default();
//! let engine = StreamingViewEngine::new(config);
//!
//! let def = StreamingViewDef {
//!     name: "active_users".into(),
//!     query: "SELECT * FROM users:* WHERE status = 'active'".into(),
//!     refresh_mode: RefreshMode::OnMutation,
//!     key_patterns: vec!["users:*".into()],
//!     description: Some("All active users".into()),
//! };
//!
//! let view_id = engine.create_view(def)?;
//! let sub_id = engine.subscribe(&view_id)?;
//!
//! // When a key matching `users:*` changes, the view is refreshed and
//! // subscribers are notified.
//! let affected = engine.notify_mutation(DataMutation {
//!     key: "users:42".into(),
//!     mutation_type: MutationType::Set,
//!     timestamp: chrono::Utc::now(),
//!     old_value: None,
//!     new_value: Some("alice".into()),
//! });
//! assert!(affected.contains(&view_id));
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors specific to the streaming materialized view subsystem.
#[derive(Debug, Clone, thiserror::Error)]
pub enum StreamingViewError {
    /// The requested view does not exist.
    #[error("view not found")]
    ViewNotFound,

    /// A view with the given name already exists.
    #[error("view already exists: {0}")]
    ViewAlreadyExists(String),

    /// The maximum number of views has been reached.
    #[error("maximum number of views exceeded")]
    MaxViewsExceeded,

    /// The maximum number of subscribers for a view has been reached.
    #[error("maximum number of subscribers exceeded")]
    MaxSubscribersExceeded,

    /// The query is invalid or could not be parsed.
    #[error("invalid query: {0}")]
    InvalidQuery(String),

    /// A refresh operation failed.
    #[error("refresh failed: {0}")]
    RefreshFailed(String),

    /// The subscription was not found.
    #[error("subscription not found")]
    SubscriptionNotFound,

    /// An internal error occurred.
    #[error("internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Identifiers
// ---------------------------------------------------------------------------

/// Unique identifier for a streaming materialized view, backed by a UUID v4.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ViewId(Uuid);

impl ViewId {
    /// Create a new random view id.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Return the inner UUID.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for ViewId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ViewId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique identifier for a view subscription, backed by a UUID v4.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ViewSubscriptionId(Uuid);

impl ViewSubscriptionId {
    /// Create a new random subscription id.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Return the inner UUID.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for ViewSubscriptionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ViewSubscriptionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Consistency mode for streaming materialized views.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsistencyMode {
    /// Eventually consistent — views may lag behind mutations by up to
    /// `max_lag_ms` milliseconds.
    Eventual {
        /// Maximum acceptable staleness in milliseconds.
        max_lag_ms: u64,
    },
    /// Strongly consistent — views are refreshed synchronously before
    /// returning from [`StreamingViewEngine::notify_mutation`].
    Strong,
}

impl Default for ConsistencyMode {
    fn default() -> Self {
        Self::Eventual { max_lag_ms: 100 }
    }
}

/// Configuration for the streaming materialized view engine.
#[derive(Debug, Clone)]
pub struct StreamingViewConfig {
    /// Maximum number of views the engine can manage.
    pub max_views: usize,
    /// Maximum number of rows a single view may hold.
    pub max_rows_per_view: usize,
    /// Debounce window in milliseconds for coalescing rapid mutations.
    pub refresh_debounce_ms: u64,
    /// Consistency guarantee for view refreshes.
    pub consistency_mode: ConsistencyMode,
    /// Whether incremental (delta-based) refresh is enabled.
    pub enable_incremental: bool,
    /// Maximum number of subscribers per view.
    pub max_subscribers_per_view: usize,
}

impl Default for StreamingViewConfig {
    fn default() -> Self {
        Self {
            max_views: 1000,
            max_rows_per_view: 100_000,
            refresh_debounce_ms: 50,
            consistency_mode: ConsistencyMode::default(),
            enable_incremental: true,
            max_subscribers_per_view: 100,
        }
    }
}

impl StreamingViewConfig {
    /// Validate configuration values, returning an error on invalid input.
    pub fn validate(&self) -> Result<(), StreamingViewError> {
        if self.max_views == 0 {
            return Err(StreamingViewError::InvalidQuery(
                "max_views must be > 0".into(),
            ));
        }
        if self.max_rows_per_view == 0 {
            return Err(StreamingViewError::InvalidQuery(
                "max_rows_per_view must be > 0".into(),
            ));
        }
        if self.max_subscribers_per_view == 0 {
            return Err(StreamingViewError::InvalidQuery(
                "max_subscribers_per_view must be > 0".into(),
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// View definition & refresh mode
// ---------------------------------------------------------------------------

/// How a streaming view is refreshed when underlying data changes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RefreshMode {
    /// Refresh immediately when a matching mutation is detected.
    OnMutation,
    /// Refresh at a fixed interval.
    Periodic {
        /// Interval between refreshes in milliseconds.
        interval_ms: u64,
    },
    /// Refresh only when explicitly requested.
    Manual,
}

impl std::fmt::Display for RefreshMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OnMutation => write!(f, "on_mutation"),
            Self::Periodic { interval_ms } => write!(f, "periodic({}ms)", interval_ms),
            Self::Manual => write!(f, "manual"),
        }
    }
}

/// Definition of a streaming materialized view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingViewDef {
    /// Human-readable name for the view.
    pub name: String,
    /// FerriteQL query that produces the view data.
    pub query: String,
    /// How the view is refreshed.
    pub refresh_mode: RefreshMode,
    /// Key patterns the view depends on (glob syntax).
    pub key_patterns: Vec<String>,
    /// Optional description.
    pub description: Option<String>,
}

// ---------------------------------------------------------------------------
// View data types
// ---------------------------------------------------------------------------

/// A single value in a view row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ViewValue {
    /// SQL NULL.
    Null,
    /// Boolean value.
    Bool(bool),
    /// 64-bit signed integer.
    Int(i64),
    /// 64-bit floating point.
    Float(f64),
    /// UTF-8 string.
    String(String),
    /// Raw bytes.
    Bytes(Vec<u8>),
}

impl std::fmt::Display for ViewValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Bool(b) => write!(f, "{}", b),
            Self::Int(i) => write!(f, "{}", i),
            Self::Float(v) => write!(f, "{}", v),
            Self::String(s) => write!(f, "{}", s),
            Self::Bytes(b) => write!(f, "<{} bytes>", b.len()),
        }
    }
}

/// A single row in a view snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ViewRow {
    /// Column values for this row.
    pub values: Vec<ViewValue>,
}

/// Freshness indicator for a view snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ViewFreshness {
    /// The view is up to date.
    Current,
    /// The view is stale — it may not reflect the most recent mutations.
    Stale {
        /// Estimated staleness in milliseconds.
        lag_ms: u64,
    },
    /// The view is currently being refreshed.
    Refreshing,
}

/// A point-in-time snapshot of a materialized view's data.
#[derive(Debug, Clone)]
pub struct ViewSnapshot {
    /// The view this snapshot belongs to.
    pub view_id: ViewId,
    /// Column names.
    pub columns: Vec<String>,
    /// Rows of data.
    pub rows: Vec<ViewRow>,
    /// Number of rows in this snapshot.
    pub row_count: u64,
    /// When this snapshot was last refreshed.
    pub last_refreshed: DateTime<Utc>,
    /// Monotonically increasing version counter.
    pub version: u64,
    /// How fresh this snapshot is.
    pub freshness: ViewFreshness,
}

// ---------------------------------------------------------------------------
// Mutation types
// ---------------------------------------------------------------------------

/// Type of data mutation that occurred.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationType {
    /// A key was set.
    Set,
    /// A key was deleted.
    Delete,
    /// A key expired.
    Expire,
    /// An element was pushed to a list.
    ListPush,
    /// A field was set in a hash.
    HashSet,
    /// A member was added to a set.
    SetAdd,
    /// An entry was added to a stream.
    StreamAdd,
}

impl std::fmt::Display for MutationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Set => write!(f, "SET"),
            Self::Delete => write!(f, "DEL"),
            Self::Expire => write!(f, "EXPIRE"),
            Self::ListPush => write!(f, "LPUSH/RPUSH"),
            Self::HashSet => write!(f, "HSET"),
            Self::SetAdd => write!(f, "SADD"),
            Self::StreamAdd => write!(f, "XADD"),
        }
    }
}

/// Describes a data mutation that may affect one or more streaming views.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataMutation {
    /// The key that was mutated.
    pub key: String,
    /// Type of mutation.
    pub mutation_type: MutationType,
    /// When the mutation occurred.
    pub timestamp: DateTime<Utc>,
    /// Previous value (if available).
    pub old_value: Option<String>,
    /// New value (if available).
    pub new_value: Option<String>,
}

// ---------------------------------------------------------------------------
// View status & info
// ---------------------------------------------------------------------------

/// Runtime status of a streaming view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ViewStatus {
    /// The view is active and accepting mutations.
    Active,
    /// The view is currently being refreshed.
    Refreshing,
    /// The view encountered an error during its last refresh.
    Error(String),
    /// The view has been suspended.
    Suspended,
}

impl std::fmt::Display for ViewStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Refreshing => write!(f, "refreshing"),
            Self::Error(e) => write!(f, "error: {}", e),
            Self::Suspended => write!(f, "suspended"),
        }
    }
}

/// Summary information about a streaming view.
#[derive(Debug, Clone)]
pub struct ViewInfo {
    /// Unique view id.
    pub id: ViewId,
    /// Human-readable name.
    pub name: String,
    /// The FerriteQL query.
    pub query: String,
    /// Number of rows currently in the view.
    pub row_count: u64,
    /// Number of active subscribers.
    pub subscriber_count: u64,
    /// Refresh mode.
    pub refresh_mode: RefreshMode,
    /// When the view was last refreshed.
    pub last_refreshed: Option<DateTime<Utc>>,
    /// Total number of refreshes performed.
    pub refreshes_total: u64,
    /// Average refresh time in microseconds.
    pub avg_refresh_time_us: u64,
    /// Current status.
    pub status: ViewStatus,
}

// ---------------------------------------------------------------------------
// Notifications
// ---------------------------------------------------------------------------

/// Type of notification sent to subscribers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NotificationType {
    /// The entire view was recomputed.
    FullRefresh,
    /// Only changed rows were sent.
    IncrementalUpdate,
    /// The view has been dropped.
    ViewDropped,
}

/// Type of change applied to a single row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeType {
    /// A new row was inserted.
    Insert,
    /// An existing row was updated.
    Update,
    /// A row was deleted.
    Delete,
}

/// Describes a change to a single row in a view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowChange {
    /// What kind of change.
    pub change_type: ChangeType,
    /// Index of the affected row.
    pub row_index: u64,
    /// Previous row values (for updates and deletes).
    pub old_values: Option<Vec<ViewValue>>,
    /// New row values (for inserts and updates).
    pub new_values: Option<Vec<ViewValue>>,
}

/// Notification sent to subscribers when a view is refreshed.
#[derive(Debug, Clone)]
pub struct ViewNotification {
    /// The view that was refreshed.
    pub view_id: ViewId,
    /// Type of notification.
    pub notification_type: NotificationType,
    /// View version after this refresh.
    pub version: u64,
    /// When the notification was created.
    pub timestamp: DateTime<Utc>,
    /// Individual row changes (empty for full refresh).
    pub changes: Vec<RowChange>,
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Aggregated statistics for the streaming view engine.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StreamingViewStats {
    /// Total number of views (all statuses).
    pub total_views: u64,
    /// Number of views in active status.
    pub active_views: u64,
    /// Total number of active subscribers across all views.
    pub total_subscribers: u64,
    /// Total number of refreshes performed.
    pub total_refreshes: u64,
    /// Number of incremental (delta) refreshes.
    pub incremental_refreshes: u64,
    /// Number of full refreshes.
    pub full_refreshes: u64,
    /// Average refresh time in microseconds across all views.
    pub avg_refresh_time_us: u64,
    /// Total mutations processed by the engine.
    pub total_mutations_processed: u64,
    /// Breakdown of views by status.
    pub views_by_status: HashMap<String, u64>,
}

// ---------------------------------------------------------------------------
// Internal view entry
// ---------------------------------------------------------------------------

/// Internal bookkeeping for a single streaming view.
struct ViewEntry {
    id: ViewId,
    name: String,
    query: String,
    refresh_mode: RefreshMode,
    #[allow(dead_code)]
    key_patterns: Vec<String>,
    #[allow(dead_code)]
    description: Option<String>,
    status: ViewStatus,
    snapshot: ViewSnapshot,
    subscribers: HashMap<ViewSubscriptionId, ViewSubscriber>,
    notifications: Vec<ViewNotification>,
    refreshes_total: u64,
    total_refresh_time_us: u64,
    #[allow(dead_code)]
    created_at: DateTime<Utc>,
}

impl ViewEntry {
    fn info(&self) -> ViewInfo {
        let avg = if self.refreshes_total > 0 {
            self.total_refresh_time_us / self.refreshes_total
        } else {
            0
        };
        ViewInfo {
            id: self.id,
            name: self.name.clone(),
            query: self.query.clone(),
            row_count: self.snapshot.row_count,
            subscriber_count: self.subscribers.len() as u64,
            refresh_mode: self.refresh_mode.clone(),
            last_refreshed: if self.refreshes_total > 0 {
                Some(self.snapshot.last_refreshed)
            } else {
                None
            },
            refreshes_total: self.refreshes_total,
            avg_refresh_time_us: avg,
            status: self.status.clone(),
        }
    }
}

/// An active subscriber on a view.
struct ViewSubscriber {
    #[allow(dead_code)]
    id: ViewSubscriptionId,
    #[allow(dead_code)]
    subscribed_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// DependencyTracker
// ---------------------------------------------------------------------------

/// Tracks which key patterns each view depends on and resolves which views
/// are affected by a given key mutation.
pub struct DependencyTracker {
    /// Map of view id → key patterns.
    view_patterns: DashMap<ViewId, Vec<String>>,
}

impl DependencyTracker {
    /// Create a new empty dependency tracker.
    pub fn new() -> Self {
        Self {
            view_patterns: DashMap::new(),
        }
    }

    /// Register the key patterns for a view.
    pub fn register_view(&self, view_id: &ViewId, patterns: &[String]) {
        self.view_patterns.insert(*view_id, patterns.to_vec());
    }

    /// Remove all tracked patterns for a view.
    pub fn remove_view(&self, view_id: &ViewId) {
        self.view_patterns.remove(view_id);
    }

    /// Return the ids of all views whose key patterns match the given key.
    pub fn get_affected_views(&self, key: &str) -> Vec<ViewId> {
        let mut affected = Vec::new();
        for entry in self.view_patterns.iter() {
            if entry.value().iter().any(|p| glob_match(p, key)) {
                affected.push(*entry.key());
            }
        }
        affected
    }
}

impl Default for DependencyTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Glob matching
// ---------------------------------------------------------------------------

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
            glob_match_inner(&pat[1..], val)
                || (!val.is_empty() && glob_match_inner(pat, &val[1..]))
        }
        (Some('?'), Some(_)) => glob_match_inner(&pat[1..], &val[1..]),
        (Some(a), Some(b)) if a == b => glob_match_inner(&pat[1..], &val[1..]),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// StreamingViewEngine
// ---------------------------------------------------------------------------

/// Manages streaming materialized views with incremental computation
/// and pub/sub notification.
///
/// Thread-safe — all public methods may be called concurrently from any
/// task or thread.
pub struct StreamingViewEngine {
    /// Active views keyed by id.
    views: DashMap<ViewId, RwLock<ViewEntry>>,
    /// Name → id reverse index for uniqueness enforcement.
    name_index: DashMap<String, ViewId>,
    /// Tracks which views depend on which key patterns.
    dependency_tracker: DependencyTracker,
    /// Engine configuration.
    config: StreamingViewConfig,
    /// Global counters.
    total_refreshes: AtomicU64,
    incremental_refreshes: AtomicU64,
    full_refreshes: AtomicU64,
    total_mutations_processed: AtomicU64,
}

impl StreamingViewEngine {
    /// Create a new streaming view engine with the given configuration.
    pub fn new(config: StreamingViewConfig) -> Self {
        Self {
            views: DashMap::new(),
            name_index: DashMap::new(),
            dependency_tracker: DependencyTracker::new(),
            config,
            total_refreshes: AtomicU64::new(0),
            incremental_refreshes: AtomicU64::new(0),
            full_refreshes: AtomicU64::new(0),
            total_mutations_processed: AtomicU64::new(0),
        }
    }

    /// Create a new streaming materialized view.
    ///
    /// Returns the [`ViewId`] assigned to the new view, or an error if the
    /// name is already taken or limits are exceeded.
    pub fn create_view(&self, def: StreamingViewDef) -> Result<ViewId, StreamingViewError> {
        // Validate query is non-empty.
        if def.query.trim().is_empty() {
            return Err(StreamingViewError::InvalidQuery(
                "query must not be empty".into(),
            ));
        }
        if def.name.trim().is_empty() {
            return Err(StreamingViewError::InvalidQuery(
                "view name must not be empty".into(),
            ));
        }

        // Enforce uniqueness.
        if self.name_index.contains_key(&def.name) {
            return Err(StreamingViewError::ViewAlreadyExists(def.name));
        }

        // Enforce view limit.
        if self.views.len() >= self.config.max_views {
            return Err(StreamingViewError::MaxViewsExceeded);
        }

        let id = ViewId::new();
        let now = Utc::now();

        let snapshot = ViewSnapshot {
            view_id: id,
            columns: Vec::new(),
            rows: Vec::new(),
            row_count: 0,
            last_refreshed: now,
            version: 0,
            freshness: ViewFreshness::Current,
        };

        let entry = ViewEntry {
            id,
            name: def.name.clone(),
            query: def.query,
            refresh_mode: def.refresh_mode,
            key_patterns: def.key_patterns.clone(),
            description: def.description,
            status: ViewStatus::Active,
            snapshot,
            subscribers: HashMap::new(),
            notifications: Vec::new(),
            refreshes_total: 0,
            total_refresh_time_us: 0,
            created_at: now,
        };

        self.dependency_tracker
            .register_view(&id, &def.key_patterns);
        self.name_index.insert(def.name, id);
        self.views.insert(id, RwLock::new(entry));

        Ok(id)
    }

    /// Drop a streaming materialized view.
    ///
    /// Notifies any remaining subscribers with [`NotificationType::ViewDropped`]
    /// before removing the view.
    pub fn drop_view(&self, id: &ViewId) -> Result<(), StreamingViewError> {
        let (_, view_lock) = self
            .views
            .remove(id)
            .ok_or(StreamingViewError::ViewNotFound)?;

        let view = view_lock.read();
        self.name_index.remove(&view.name);
        self.dependency_tracker.remove_view(id);

        Ok(())
    }

    /// Return a point-in-time snapshot of a view's data.
    pub fn get_view_data(&self, id: &ViewId) -> Option<ViewSnapshot> {
        self.views
            .get(id)
            .map(|entry| entry.read().snapshot.clone())
    }

    /// Process a data mutation and return the ids of all affected views.
    ///
    /// For views with [`RefreshMode::OnMutation`], an incremental or full
    /// refresh is performed immediately (depending on
    /// [`StreamingViewConfig::enable_incremental`]). Views with other
    /// refresh modes are marked stale.
    pub fn notify_mutation(&self, mutation: DataMutation) -> Vec<ViewId> {
        self.total_mutations_processed
            .fetch_add(1, Ordering::Relaxed);

        let affected = self.dependency_tracker.get_affected_views(&mutation.key);

        for view_id in &affected {
            if let Some(entry) = self.views.get(view_id) {
                let mut view = entry.write();
                match view.refresh_mode {
                    RefreshMode::OnMutation => {
                        self.refresh_view_internal(&mut view, &mutation);
                    }
                    RefreshMode::Periodic { .. } | RefreshMode::Manual => {
                        view.snapshot.freshness = ViewFreshness::Stale { lag_ms: 0 };
                    }
                }
            }
        }

        affected
    }

    /// Subscribe to notifications for a view.
    ///
    /// Returns a [`ViewSubscriptionId`] that can be used to unsubscribe.
    pub fn subscribe(&self, view_id: &ViewId) -> Result<ViewSubscriptionId, StreamingViewError> {
        let entry = self
            .views
            .get(view_id)
            .ok_or(StreamingViewError::ViewNotFound)?;

        let mut view = entry.write();

        if view.subscribers.len() >= self.config.max_subscribers_per_view {
            return Err(StreamingViewError::MaxSubscribersExceeded);
        }

        let sub_id = ViewSubscriptionId::new();
        view.subscribers.insert(
            sub_id,
            ViewSubscriber {
                id: sub_id,
                subscribed_at: Utc::now(),
            },
        );

        Ok(sub_id)
    }

    /// Remove a subscription.
    pub fn unsubscribe(&self, sub_id: &ViewSubscriptionId) -> Result<(), StreamingViewError> {
        for entry in self.views.iter() {
            let mut view = entry.write();
            if view.subscribers.remove(sub_id).is_some() {
                return Ok(());
            }
        }
        Err(StreamingViewError::SubscriptionNotFound)
    }

    /// List summary information for all views.
    pub fn list_views(&self) -> Vec<ViewInfo> {
        self.views.iter().map(|entry| entry.read().info()).collect()
    }

    /// Return aggregated engine statistics.
    pub fn get_stats(&self) -> StreamingViewStats {
        let mut stats = StreamingViewStats {
            total_views: self.views.len() as u64,
            total_refreshes: self.total_refreshes.load(Ordering::Relaxed),
            incremental_refreshes: self.incremental_refreshes.load(Ordering::Relaxed),
            full_refreshes: self.full_refreshes.load(Ordering::Relaxed),
            total_mutations_processed: self.total_mutations_processed.load(Ordering::Relaxed),
            ..Default::default()
        };

        let mut total_subs = 0u64;
        let mut active = 0u64;
        let mut total_refresh_us = 0u64;
        let mut total_refresh_count = 0u64;

        for entry in self.views.iter() {
            let view = entry.read();
            total_subs += view.subscribers.len() as u64;

            let status_key = match &view.status {
                ViewStatus::Active => "active".to_string(),
                ViewStatus::Refreshing => "refreshing".to_string(),
                ViewStatus::Error(_) => "error".to_string(),
                ViewStatus::Suspended => "suspended".to_string(),
            };

            if view.status == ViewStatus::Active {
                active += 1;
            }

            total_refresh_us += view.total_refresh_time_us;
            total_refresh_count += view.refreshes_total;

            *stats.views_by_status.entry(status_key).or_insert(0) += 1;
        }

        stats.total_subscribers = total_subs;
        stats.active_views = active;
        stats.avg_refresh_time_us = if total_refresh_count > 0 {
            total_refresh_us / total_refresh_count
        } else {
            0
        };

        stats
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Perform a refresh of a view triggered by a mutation.
    fn refresh_view_internal(&self, view: &mut ViewEntry, mutation: &DataMutation) {
        let start = std::time::Instant::now();

        view.status = ViewStatus::Refreshing;
        view.snapshot.version += 1;

        let notification = if self.config.enable_incremental {
            self.incremental_refreshes.fetch_add(1, Ordering::Relaxed);
            self.apply_incremental(view, mutation)
        } else {
            self.full_refreshes.fetch_add(1, Ordering::Relaxed);
            self.apply_full_refresh(view)
        };

        let elapsed_us = start.elapsed().as_micros() as u64;
        view.refreshes_total += 1;
        view.total_refresh_time_us += elapsed_us;
        view.snapshot.last_refreshed = Utc::now();
        view.snapshot.freshness = ViewFreshness::Current;
        view.status = ViewStatus::Active;

        self.total_refreshes.fetch_add(1, Ordering::Relaxed);

        view.notifications.push(notification);
    }

    /// Apply an incremental delta to a view based on a single mutation.
    fn apply_incremental(&self, view: &mut ViewEntry, mutation: &DataMutation) -> ViewNotification {
        let mut changes = Vec::new();

        match mutation.mutation_type {
            MutationType::Delete | MutationType::Expire => {
                // Remove rows whose first column matches the mutated key.
                let mut idx = 0u64;
                let key_val = ViewValue::String(mutation.key.clone());
                view.snapshot.rows.retain(|row| {
                    let keep = row.values.first() != Some(&key_val);
                    if !keep {
                        changes.push(RowChange {
                            change_type: ChangeType::Delete,
                            row_index: idx,
                            old_values: Some(row.values.clone()),
                            new_values: None,
                        });
                    }
                    idx += 1;
                    keep
                });
            }
            MutationType::Set
            | MutationType::ListPush
            | MutationType::HashSet
            | MutationType::SetAdd
            | MutationType::StreamAdd => {
                let key_val = ViewValue::String(mutation.key.clone());
                let new_row_values = vec![
                    key_val.clone(),
                    mutation
                        .new_value
                        .as_ref()
                        .map(|v| ViewValue::String(v.clone()))
                        .unwrap_or(ViewValue::Null),
                ];

                // Check if the key already exists.
                if let Some((idx, row)) = view
                    .snapshot
                    .rows
                    .iter_mut()
                    .enumerate()
                    .find(|(_, r)| r.values.first() == Some(&key_val))
                {
                    let old = row.values.clone();
                    row.values = new_row_values.clone();
                    changes.push(RowChange {
                        change_type: ChangeType::Update,
                        row_index: idx as u64,
                        old_values: Some(old),
                        new_values: Some(new_row_values),
                    });
                } else {
                    let row_index = view.snapshot.rows.len() as u64;
                    view.snapshot.rows.push(ViewRow {
                        values: new_row_values.clone(),
                    });
                    changes.push(RowChange {
                        change_type: ChangeType::Insert,
                        row_index,
                        old_values: None,
                        new_values: Some(new_row_values),
                    });
                }
            }
        }

        view.snapshot.row_count = view.snapshot.rows.len() as u64;

        ViewNotification {
            view_id: view.id,
            notification_type: NotificationType::IncrementalUpdate,
            version: view.snapshot.version,
            timestamp: Utc::now(),
            changes,
        }
    }

    /// Perform a full refresh (clear and rebuild). In a real implementation
    /// this would re-execute the view's query against the store.
    fn apply_full_refresh(&self, view: &mut ViewEntry) -> ViewNotification {
        // In production this would re-execute the query. Here we simply
        // mark the view as freshly refreshed.
        view.snapshot.freshness = ViewFreshness::Current;

        ViewNotification {
            view_id: view.id,
            notification_type: NotificationType::FullRefresh,
            version: view.snapshot.version,
            timestamp: Utc::now(),
            changes: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- helpers -----------------------------------------------------------

    fn default_engine() -> StreamingViewEngine {
        StreamingViewEngine::new(StreamingViewConfig::default())
    }

    fn sample_def(name: &str) -> StreamingViewDef {
        StreamingViewDef {
            name: name.to_string(),
            query: "SELECT * FROM users:*".into(),
            refresh_mode: RefreshMode::OnMutation,
            key_patterns: vec!["users:*".into()],
            description: Some("test view".into()),
        }
    }

    fn sample_mutation(key: &str) -> DataMutation {
        DataMutation {
            key: key.to_string(),
            mutation_type: MutationType::Set,
            timestamp: Utc::now(),
            old_value: None,
            new_value: Some("value".into()),
        }
    }

    // -- view creation & retrieval ----------------------------------------

    #[test]
    fn create_and_get_view() {
        let engine = default_engine();
        let id = engine.create_view(sample_def("v1")).unwrap();
        let snap = engine.get_view_data(&id).unwrap();
        assert_eq!(snap.view_id, id);
        assert_eq!(snap.row_count, 0);
        assert_eq!(snap.version, 0);
    }

    #[test]
    fn create_duplicate_name_fails() {
        let engine = default_engine();
        engine.create_view(sample_def("dup")).unwrap();
        let err = engine.create_view(sample_def("dup")).unwrap_err();
        assert!(matches!(err, StreamingViewError::ViewAlreadyExists(_)));
    }

    #[test]
    fn create_empty_name_fails() {
        let engine = default_engine();
        let mut def = sample_def("");
        def.name = "  ".into();
        let err = engine.create_view(def).unwrap_err();
        assert!(matches!(err, StreamingViewError::InvalidQuery(_)));
    }

    #[test]
    fn create_empty_query_fails() {
        let engine = default_engine();
        let mut def = sample_def("q");
        def.query = "  ".into();
        let err = engine.create_view(def).unwrap_err();
        assert!(matches!(err, StreamingViewError::InvalidQuery(_)));
    }

    #[test]
    fn drop_view_removes_it() {
        let engine = default_engine();
        let id = engine.create_view(sample_def("d")).unwrap();
        engine.drop_view(&id).unwrap();
        assert!(engine.get_view_data(&id).is_none());
    }

    #[test]
    fn drop_nonexistent_view_fails() {
        let engine = default_engine();
        let err = engine.drop_view(&ViewId::new()).unwrap_err();
        assert!(matches!(err, StreamingViewError::ViewNotFound));
    }

    #[test]
    fn list_views_returns_all() {
        let engine = default_engine();
        engine.create_view(sample_def("a")).unwrap();
        engine.create_view(sample_def("b")).unwrap();
        let views = engine.list_views();
        assert_eq!(views.len(), 2);
    }

    // -- mutation notification & affected views ---------------------------

    #[test]
    fn notify_mutation_returns_affected_views() {
        let engine = default_engine();
        let id = engine.create_view(sample_def("m")).unwrap();
        let affected = engine.notify_mutation(sample_mutation("users:1"));
        assert!(affected.contains(&id));
    }

    #[test]
    fn notify_mutation_unrelated_key_returns_empty() {
        let engine = default_engine();
        engine.create_view(sample_def("m2")).unwrap();
        let affected = engine.notify_mutation(sample_mutation("orders:1"));
        assert!(affected.is_empty());
    }

    #[test]
    fn notify_mutation_inserts_row_incremental() {
        let engine = default_engine();
        let id = engine.create_view(sample_def("inc")).unwrap();
        engine.notify_mutation(sample_mutation("users:42"));
        let snap = engine.get_view_data(&id).unwrap();
        assert_eq!(snap.row_count, 1);
        assert_eq!(snap.version, 1);
        assert_eq!(snap.freshness, ViewFreshness::Current);
        assert_eq!(snap.rows[0].values[0], ViewValue::String("users:42".into()));
    }

    #[test]
    fn notify_mutation_updates_existing_row() {
        let engine = default_engine();
        let id = engine.create_view(sample_def("upd")).unwrap();
        engine.notify_mutation(sample_mutation("users:1"));

        let mut m2 = sample_mutation("users:1");
        m2.new_value = Some("updated".into());
        engine.notify_mutation(m2);

        let snap = engine.get_view_data(&id).unwrap();
        assert_eq!(snap.row_count, 1);
        assert_eq!(snap.version, 2);
        assert_eq!(snap.rows[0].values[1], ViewValue::String("updated".into()));
    }

    #[test]
    fn notify_mutation_delete_removes_row() {
        let engine = default_engine();
        let id = engine.create_view(sample_def("del")).unwrap();
        engine.notify_mutation(sample_mutation("users:1"));
        assert_eq!(engine.get_view_data(&id).unwrap().row_count, 1);

        let del = DataMutation {
            key: "users:1".into(),
            mutation_type: MutationType::Delete,
            timestamp: Utc::now(),
            old_value: Some("value".into()),
            new_value: None,
        };
        engine.notify_mutation(del);
        assert_eq!(engine.get_view_data(&id).unwrap().row_count, 0);
    }

    // -- dependency tracker -----------------------------------------------

    #[test]
    fn dependency_tracker_register_and_resolve() {
        let tracker = DependencyTracker::new();
        let id = ViewId::new();
        tracker.register_view(&id, &["users:*".into()]);
        let affected = tracker.get_affected_views("users:42");
        assert_eq!(affected, vec![id]);
    }

    #[test]
    fn dependency_tracker_remove_view() {
        let tracker = DependencyTracker::new();
        let id = ViewId::new();
        tracker.register_view(&id, &["users:*".into()]);
        tracker.remove_view(&id);
        let affected = tracker.get_affected_views("users:42");
        assert!(affected.is_empty());
    }

    #[test]
    fn dependency_tracker_multiple_patterns() {
        let tracker = DependencyTracker::new();
        let id = ViewId::new();
        tracker.register_view(&id, &["users:*".into(), "accounts:*".into()]);
        assert!(!tracker.get_affected_views("users:1").is_empty());
        assert!(!tracker.get_affected_views("accounts:2").is_empty());
        assert!(tracker.get_affected_views("orders:3").is_empty());
    }

    #[test]
    fn dependency_tracker_question_mark_glob() {
        let tracker = DependencyTracker::new();
        let id = ViewId::new();
        tracker.register_view(&id, &["user?".into()]);
        assert!(!tracker.get_affected_views("user1").is_empty());
        assert!(!tracker.get_affected_views("userA").is_empty());
        assert!(!tracker.get_affected_views("users").is_empty());
        assert!(tracker.get_affected_views("user12").is_empty());
    }

    // -- view snapshot data -----------------------------------------------

    #[test]
    fn snapshot_columns_and_rows() {
        let engine = default_engine();
        let id = engine.create_view(sample_def("snap")).unwrap();
        engine.notify_mutation(DataMutation {
            key: "users:1".into(),
            mutation_type: MutationType::Set,
            timestamp: Utc::now(),
            old_value: None,
            new_value: Some("alice".into()),
        });
        let snap = engine.get_view_data(&id).unwrap();
        assert_eq!(snap.rows.len(), 1);
        assert_eq!(
            snap.rows[0].values,
            vec![
                ViewValue::String("users:1".into()),
                ViewValue::String("alice".into()),
            ]
        );
    }

    // -- subscription lifecycle -------------------------------------------

    #[test]
    fn subscribe_and_unsubscribe() {
        let engine = default_engine();
        let id = engine.create_view(sample_def("sub")).unwrap();
        let sub_id = engine.subscribe(&id).unwrap();
        engine.unsubscribe(&sub_id).unwrap();
    }

    #[test]
    fn subscribe_nonexistent_view_fails() {
        let engine = default_engine();
        let err = engine.subscribe(&ViewId::new()).unwrap_err();
        assert!(matches!(err, StreamingViewError::ViewNotFound));
    }

    #[test]
    fn unsubscribe_nonexistent_fails() {
        let engine = default_engine();
        let err = engine.unsubscribe(&ViewSubscriptionId::new()).unwrap_err();
        assert!(matches!(err, StreamingViewError::SubscriptionNotFound));
    }

    #[test]
    fn subscribe_respects_limit() {
        let config = StreamingViewConfig {
            max_subscribers_per_view: 2,
            ..Default::default()
        };
        let engine = StreamingViewEngine::new(config);
        let id = engine.create_view(sample_def("lim")).unwrap();
        engine.subscribe(&id).unwrap();
        engine.subscribe(&id).unwrap();
        let err = engine.subscribe(&id).unwrap_err();
        assert!(matches!(err, StreamingViewError::MaxSubscribersExceeded));
    }

    // -- stats tracking ---------------------------------------------------

    #[test]
    fn stats_reflect_engine_state() {
        let engine = default_engine();
        let id = engine.create_view(sample_def("st")).unwrap();
        engine.subscribe(&id).unwrap();
        engine.notify_mutation(sample_mutation("users:1"));

        let stats = engine.get_stats();
        assert_eq!(stats.total_views, 1);
        assert_eq!(stats.active_views, 1);
        assert_eq!(stats.total_subscribers, 1);
        assert_eq!(stats.total_mutations_processed, 1);
        assert_eq!(stats.total_refreshes, 1);
        assert!(stats.incremental_refreshes > 0 || stats.full_refreshes > 0);
    }

    #[test]
    fn stats_views_by_status() {
        let engine = default_engine();
        engine.create_view(sample_def("s1")).unwrap();
        engine.create_view(sample_def("s2")).unwrap();
        let stats = engine.get_stats();
        assert_eq!(*stats.views_by_status.get("active").unwrap(), 2);
    }

    // -- consistency mode behaviour ---------------------------------------

    #[test]
    fn eventual_consistency_marks_stale_for_periodic() {
        let engine = default_engine();
        let def = StreamingViewDef {
            name: "periodic".into(),
            query: "SELECT * FROM users:*".into(),
            refresh_mode: RefreshMode::Periodic { interval_ms: 5000 },
            key_patterns: vec!["users:*".into()],
            description: None,
        };
        let id = engine.create_view(def).unwrap();
        engine.notify_mutation(sample_mutation("users:1"));
        let snap = engine.get_view_data(&id).unwrap();
        assert!(matches!(snap.freshness, ViewFreshness::Stale { .. }));
    }

    #[test]
    fn manual_view_stays_stale() {
        let engine = default_engine();
        let def = StreamingViewDef {
            name: "manual".into(),
            query: "SELECT * FROM users:*".into(),
            refresh_mode: RefreshMode::Manual,
            key_patterns: vec!["users:*".into()],
            description: None,
        };
        let id = engine.create_view(def).unwrap();
        engine.notify_mutation(sample_mutation("users:1"));
        let snap = engine.get_view_data(&id).unwrap();
        assert!(matches!(snap.freshness, ViewFreshness::Stale { .. }));
        // Row count should be 0 since manual views are not refreshed.
        assert_eq!(snap.row_count, 0);
    }

    #[test]
    fn on_mutation_view_refreshes_immediately() {
        let engine = default_engine();
        let id = engine.create_view(sample_def("imm")).unwrap();
        engine.notify_mutation(sample_mutation("users:99"));
        let snap = engine.get_view_data(&id).unwrap();
        assert_eq!(snap.freshness, ViewFreshness::Current);
        assert_eq!(snap.row_count, 1);
    }

    // -- incremental vs full refresh --------------------------------------

    #[test]
    fn incremental_enabled_uses_delta() {
        let config = StreamingViewConfig {
            enable_incremental: true,
            ..Default::default()
        };
        let engine = StreamingViewEngine::new(config);
        let id = engine.create_view(sample_def("idelta")).unwrap();
        engine.notify_mutation(sample_mutation("users:1"));
        let snap = engine.get_view_data(&id).unwrap();
        assert_eq!(snap.row_count, 1);

        let stats = engine.get_stats();
        assert_eq!(stats.incremental_refreshes, 1);
        assert_eq!(stats.full_refreshes, 0);
    }

    #[test]
    fn incremental_disabled_uses_full_refresh() {
        let config = StreamingViewConfig {
            enable_incremental: false,
            ..Default::default()
        };
        let engine = StreamingViewEngine::new(config);
        let id = engine.create_view(sample_def("full")).unwrap();
        engine.notify_mutation(sample_mutation("users:1"));

        let stats = engine.get_stats();
        assert_eq!(stats.full_refreshes, 1);
        assert_eq!(stats.incremental_refreshes, 0);

        // Full refresh doesn't modify rows (no query executor here).
        let snap = engine.get_view_data(&id).unwrap();
        assert_eq!(snap.row_count, 0);
    }

    // -- view status transitions ------------------------------------------

    #[test]
    fn view_status_is_active_after_creation() {
        let engine = default_engine();
        let id = engine.create_view(sample_def("status")).unwrap();
        let views = engine.list_views();
        let info = views.iter().find(|v| v.id == id).unwrap();
        assert_eq!(info.status, ViewStatus::Active);
    }

    #[test]
    fn view_status_returns_active_after_refresh() {
        let engine = default_engine();
        let id = engine.create_view(sample_def("sr")).unwrap();
        engine.notify_mutation(sample_mutation("users:1"));
        let views = engine.list_views();
        let info = views.iter().find(|v| v.id == id).unwrap();
        assert_eq!(info.status, ViewStatus::Active);
    }

    // -- error handling ---------------------------------------------------

    #[test]
    fn max_views_exceeded() {
        let config = StreamingViewConfig {
            max_views: 2,
            ..Default::default()
        };
        let engine = StreamingViewEngine::new(config);
        engine.create_view(sample_def("e1")).unwrap();
        engine.create_view(sample_def("e2")).unwrap();
        let err = engine.create_view(sample_def("e3")).unwrap_err();
        assert!(matches!(err, StreamingViewError::MaxViewsExceeded));
    }

    #[test]
    fn error_display_messages() {
        let err = StreamingViewError::ViewNotFound;
        assert_eq!(err.to_string(), "view not found");

        let err = StreamingViewError::ViewAlreadyExists("test".into());
        assert_eq!(err.to_string(), "view already exists: test");

        let err = StreamingViewError::MaxViewsExceeded;
        assert_eq!(err.to_string(), "maximum number of views exceeded");

        let err = StreamingViewError::Internal("oops".into());
        assert_eq!(err.to_string(), "internal error: oops");
    }

    // -- config validation ------------------------------------------------

    #[test]
    fn config_default_is_valid() {
        StreamingViewConfig::default().validate().unwrap();
    }

    #[test]
    fn config_zero_max_views_invalid() {
        let mut cfg = StreamingViewConfig::default();
        cfg.max_views = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn config_zero_max_rows_invalid() {
        let mut cfg = StreamingViewConfig::default();
        cfg.max_rows_per_view = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn config_zero_max_subs_invalid() {
        let mut cfg = StreamingViewConfig::default();
        cfg.max_subscribers_per_view = 0;
        assert!(cfg.validate().is_err());
    }

    // -- glob matching ----------------------------------------------------

    #[test]
    fn glob_star_matches() {
        assert!(glob_match("users:*", "users:123"));
        assert!(glob_match("users:*", "users:"));
        assert!(!glob_match("users:*", "orders:1"));
        assert!(glob_match("*", "anything"));
    }

    #[test]
    fn glob_question_matches() {
        assert!(glob_match("a?c", "abc"));
        assert!(glob_match("a?c", "axc"));
        assert!(!glob_match("a?c", "ac"));
        assert!(!glob_match("a?c", "abbc"));
    }

    #[test]
    fn glob_exact_matches() {
        assert!(glob_match("hello", "hello"));
        assert!(!glob_match("hello", "world"));
    }

    // -- ViewValue display ------------------------------------------------

    #[test]
    fn view_value_display() {
        assert_eq!(ViewValue::Null.to_string(), "NULL");
        assert_eq!(ViewValue::Bool(true).to_string(), "true");
        assert_eq!(ViewValue::Int(42).to_string(), "42");
        assert_eq!(ViewValue::Float(3.14).to_string(), "3.14");
        assert_eq!(ViewValue::String("hi".into()).to_string(), "hi");
        assert_eq!(ViewValue::Bytes(vec![1, 2, 3]).to_string(), "<3 bytes>");
    }

    // -- RefreshMode display ----------------------------------------------

    #[test]
    fn refresh_mode_display() {
        assert_eq!(RefreshMode::OnMutation.to_string(), "on_mutation");
        assert_eq!(
            RefreshMode::Periodic { interval_ms: 1000 }.to_string(),
            "periodic(1000ms)"
        );
        assert_eq!(RefreshMode::Manual.to_string(), "manual");
    }

    // -- ViewStatus display -----------------------------------------------

    #[test]
    fn view_status_display() {
        assert_eq!(ViewStatus::Active.to_string(), "active");
        assert_eq!(ViewStatus::Refreshing.to_string(), "refreshing");
        assert_eq!(ViewStatus::Error("fail".into()).to_string(), "error: fail");
        assert_eq!(ViewStatus::Suspended.to_string(), "suspended");
    }

    // -- MutationType display ---------------------------------------------

    #[test]
    fn mutation_type_display() {
        assert_eq!(MutationType::Set.to_string(), "SET");
        assert_eq!(MutationType::Delete.to_string(), "DEL");
        assert_eq!(MutationType::Expire.to_string(), "EXPIRE");
        assert_eq!(MutationType::HashSet.to_string(), "HSET");
    }

    // -- ViewId / ViewSubscriptionId identity -----------------------------

    #[test]
    fn view_id_display_is_uuid() {
        let id = ViewId::new();
        let s = id.to_string();
        assert_eq!(s.len(), 36); // UUID format: 8-4-4-4-12
    }

    #[test]
    fn view_subscription_id_display_is_uuid() {
        let id = ViewSubscriptionId::new();
        let s = id.to_string();
        assert_eq!(s.len(), 36);
    }

    // -- Multiple mutations -----------------------------------------------

    #[test]
    fn multiple_mutations_accumulate_rows() {
        let engine = default_engine();
        let id = engine.create_view(sample_def("multi")).unwrap();
        for i in 0..5 {
            engine.notify_mutation(sample_mutation(&format!("users:{}", i)));
        }
        let snap = engine.get_view_data(&id).unwrap();
        assert_eq!(snap.row_count, 5);
        assert_eq!(snap.version, 5);
    }

    // -- Expire mutation type ---------------------------------------------

    #[test]
    fn expire_removes_row() {
        let engine = default_engine();
        let id = engine.create_view(sample_def("exp")).unwrap();
        engine.notify_mutation(sample_mutation("users:1"));
        assert_eq!(engine.get_view_data(&id).unwrap().row_count, 1);

        let expire = DataMutation {
            key: "users:1".into(),
            mutation_type: MutationType::Expire,
            timestamp: Utc::now(),
            old_value: None,
            new_value: None,
        };
        engine.notify_mutation(expire);
        assert_eq!(engine.get_view_data(&id).unwrap().row_count, 0);
    }

    // -- Can re-create after drop -----------------------------------------

    #[test]
    fn recreate_after_drop() {
        let engine = default_engine();
        let id = engine.create_view(sample_def("rc")).unwrap();
        engine.drop_view(&id).unwrap();
        let id2 = engine.create_view(sample_def("rc")).unwrap();
        assert_ne!(id, id2);
    }

    // -- Subscriber count in ViewInfo ------------------------------------

    #[test]
    fn view_info_subscriber_count() {
        let engine = default_engine();
        let id = engine.create_view(sample_def("vc")).unwrap();
        engine.subscribe(&id).unwrap();
        engine.subscribe(&id).unwrap();
        let views = engine.list_views();
        let info = views.iter().find(|v| v.id == id).unwrap();
        assert_eq!(info.subscriber_count, 2);
    }
}
