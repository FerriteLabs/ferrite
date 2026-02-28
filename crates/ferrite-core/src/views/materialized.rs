//! FerriteQL Materialized Views
//!
//! Persistent query results that auto-update when source data changes.
//! Uses CDC (change data capture) for incremental maintenance.
//!
//! # Example
//! ```ignore
//! VIEW.CREATE top_users FROM users:* WHERE $.active = true ORDER BY $.score DESC LIMIT 100
//! VIEW.GET top_users
//! VIEW.REFRESH top_users
//! VIEW.DROP top_users
//! VIEW.LIST
//! ```
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// View definition types
// ---------------------------------------------------------------------------

/// Defines a materialized view with source pattern, filter, ordering, and refresh policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewDefinition {
    /// Unique name for this view
    pub name: String,
    /// Source key pattern (e.g., "users:*")
    pub source_pattern: String,
    /// Optional filter condition
    pub filter: Option<ViewFilter>,
    /// Optional ordering
    pub order_by: Option<ViewOrderBy>,
    /// Maximum number of rows to materialize
    pub limit: Option<usize>,
    /// When this view was created (epoch millis for serde compat)
    #[serde(skip)]
    pub created_at: Option<Instant>,
    /// When this view was last refreshed
    #[serde(skip)]
    pub last_refreshed: Option<Instant>,
    /// Auto-refresh interval
    pub refresh_interval: Option<Duration>,
    /// Whether automatic refresh is enabled
    pub auto_refresh: bool,
}

/// Filter condition for a materialized view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewFilter {
    /// JSON path expression (e.g., "$.active")
    pub field: String,
    /// Comparison operator
    pub op: FilterOp,
    /// Value to compare against
    pub value: serde_json::Value,
}

/// Comparison operators for view filters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FilterOp {
    /// Equal
    Eq,
    /// Not equal
    Ne,
    /// Greater than
    Gt,
    /// Less than
    Lt,
    /// Greater than or equal
    Gte,
    /// Less than or equal
    Lte,
    /// String contains
    Contains,
    /// Field exists
    Exists,
}

/// Ordering specification for a materialized view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewOrderBy {
    /// Field to order by (JSON path)
    pub field: String,
    /// Whether to sort in descending order
    pub descending: bool,
}

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

/// Cached result of a materialized view computation.
#[derive(Debug, Clone)]
pub struct ViewResult {
    /// View name
    pub name: String,
    /// Materialized rows (JSON values)
    pub rows: Vec<serde_json::Value>,
    /// Total rows before any limit
    pub total_count: usize,
    /// When this result was computed
    pub computed_at: Instant,
    /// Time spent computing in milliseconds
    pub computation_ms: u64,
    /// Whether source data changed since last refresh
    pub stale: bool,
}

/// Summary information about a view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewInfo {
    /// View name
    pub name: String,
    /// Source key pattern
    pub source_pattern: String,
    /// Number of materialized rows
    pub row_count: usize,
    /// When last refreshed (ms since creation, for serialization)
    pub last_refreshed_ms: Option<u64>,
    /// Whether auto-refresh is enabled
    pub auto_refresh: bool,
    /// Whether the view data is stale
    pub stale: bool,
}

/// Errors that can occur in materialized view operations.
#[derive(Debug, thiserror::Error)]
pub enum ViewError {
    /// A view with this name already exists.
    #[error("view '{0}' already exists")]
    AlreadyExists(String),

    /// No view found with this name.
    #[error("view '{0}' not found")]
    NotFound(String),

    /// The filter expression is invalid.
    #[error("invalid filter: {0}")]
    InvalidFilter(String),

    /// The source pattern is invalid.
    #[error("invalid source pattern: {0}")]
    InvalidSource(String),

    /// View refresh failed.
    #[error("refresh failed: {0}")]
    RefreshFailed(String),
}

// ---------------------------------------------------------------------------
// Manager
// ---------------------------------------------------------------------------

/// Internal state for a single managed view.
struct ManagedView {
    definition: ViewDefinition,
    result: Option<ViewResult>,
    dirty: bool,
}

/// Manages all materialized views, providing CRUD and CDC-based incremental updates.
pub struct MaterializedViewManager {
    views: Arc<RwLock<HashMap<String, ManagedView>>>,
}

impl MaterializedViewManager {
    /// Create a new empty view manager.
    pub fn new() -> Self {
        Self {
            views: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a new materialized view.
    pub fn create_view(&self, def: ViewDefinition) -> Result<(), ViewError> {
        let mut views = self.views.write();
        if views.contains_key(&def.name) {
            return Err(ViewError::AlreadyExists(def.name.clone()));
        }
        if def.source_pattern.is_empty() {
            return Err(ViewError::InvalidSource(
                "source pattern cannot be empty".to_string(),
            ));
        }
        let name = def.name.clone();
        views.insert(
            name,
            ManagedView {
                definition: def,
                result: None,
                dirty: true,
            },
        );
        Ok(())
    }

    /// Return the cached result for a view, if available.
    pub fn get_view(&self, name: &str) -> Option<ViewResult> {
        let views = self.views.read();
        views.get(name).and_then(|mv| mv.result.clone())
    }

    /// Force a full recomputation of the view from source data.
    pub fn refresh_view(&self, name: &str) -> Result<ViewResult, ViewError> {
        let mut views = self.views.write();
        let mv = views
            .get_mut(name)
            .ok_or_else(|| ViewError::NotFound(name.to_string()))?;

        let start = Instant::now();

        // Stub: in production, scan keys matching source_pattern,
        // apply filter, sort by order_by, apply limit
        let rows: Vec<serde_json::Value> = Vec::new();
        let total_count = rows.len();

        let result = ViewResult {
            name: name.to_string(),
            rows,
            total_count,
            computed_at: Instant::now(),
            computation_ms: start.elapsed().as_millis() as u64,
            stale: false,
        };

        mv.result = Some(result.clone());
        mv.dirty = false;
        mv.definition.last_refreshed = Some(Instant::now());

        Ok(result)
    }

    /// Drop (delete) a materialized view.
    pub fn drop_view(&self, name: &str) -> Result<(), ViewError> {
        let mut views = self.views.write();
        if views.remove(name).is_none() {
            return Err(ViewError::NotFound(name.to_string()));
        }
        Ok(())
    }

    /// List all registered views with summary information.
    pub fn list_views(&self) -> Vec<ViewInfo> {
        let views = self.views.read();
        views
            .values()
            .map(|mv| {
                let row_count = mv.result.as_ref().map_or(0, |r| r.rows.len());
                let last_refreshed_ms = mv
                    .definition
                    .last_refreshed
                    .map(|t| t.elapsed().as_millis() as u64);
                ViewInfo {
                    name: mv.definition.name.clone(),
                    source_pattern: mv.definition.source_pattern.clone(),
                    row_count,
                    last_refreshed_ms,
                    auto_refresh: mv.definition.auto_refresh,
                    stale: mv.dirty,
                }
            })
            .collect()
    }

    /// CDC callback: invoked when a key changes to mark affected views as stale.
    pub fn on_key_change(&self, key: &str, _old_value: Option<&str>, _new_value: Option<&str>) {
        let mut views = self.views.write();
        for mv in views.values_mut() {
            if key_matches_pattern(key, &mv.definition.source_pattern) {
                mv.dirty = true;
                if let Some(ref mut result) = mv.result {
                    result.stale = true;
                }
            }
        }
    }

    /// Return names of views whose auto-refresh interval has elapsed.
    pub fn auto_refresh_due(&self) -> Vec<String> {
        let views = self.views.read();
        views
            .values()
            .filter(|mv| {
                if !mv.definition.auto_refresh {
                    return false;
                }
                let interval = match mv.definition.refresh_interval {
                    Some(d) => d,
                    None => return false,
                };
                match mv.definition.last_refreshed {
                    Some(t) => t.elapsed() >= interval,
                    None => true, // never refreshed
                }
            })
            .map(|mv| mv.definition.name.clone())
            .collect()
    }
}

impl Default for MaterializedViewManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple glob-style pattern matching for key patterns (supports trailing `*`).
fn key_matches_pattern(key: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        key.starts_with(prefix)
    } else {
        key == pattern
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_def(name: &str, pattern: &str) -> ViewDefinition {
        ViewDefinition {
            name: name.to_string(),
            source_pattern: pattern.to_string(),
            filter: None,
            order_by: None,
            limit: None,
            created_at: Some(Instant::now()),
            last_refreshed: None,
            refresh_interval: None,
            auto_refresh: false,
        }
    }

    #[test]
    fn test_create_and_list() {
        let mgr = MaterializedViewManager::new();
        mgr.create_view(test_def("v1", "users:*")).unwrap();
        let views = mgr.list_views();
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].name, "v1");
    }

    #[test]
    fn test_create_duplicate() {
        let mgr = MaterializedViewManager::new();
        mgr.create_view(test_def("v1", "users:*")).unwrap();
        assert!(mgr.create_view(test_def("v1", "users:*")).is_err());
    }

    #[test]
    fn test_drop_view() {
        let mgr = MaterializedViewManager::new();
        mgr.create_view(test_def("v1", "users:*")).unwrap();
        mgr.drop_view("v1").unwrap();
        assert!(mgr.list_views().is_empty());
    }

    #[test]
    fn test_drop_nonexistent() {
        let mgr = MaterializedViewManager::new();
        assert!(mgr.drop_view("nope").is_err());
    }

    #[test]
    fn test_refresh_and_get() {
        let mgr = MaterializedViewManager::new();
        mgr.create_view(test_def("v1", "users:*")).unwrap();
        let result = mgr.refresh_view("v1").unwrap();
        assert_eq!(result.name, "v1");
        assert!(!result.stale);

        let cached = mgr.get_view("v1");
        assert!(cached.is_some());
    }

    #[test]
    fn test_cdc_marks_stale() {
        let mgr = MaterializedViewManager::new();
        mgr.create_view(test_def("v1", "users:*")).unwrap();
        mgr.refresh_view("v1").unwrap();

        mgr.on_key_change("users:123", None, Some("{}"));

        let result = mgr.get_view("v1").unwrap();
        assert!(result.stale);
    }

    #[test]
    fn test_key_matches_pattern() {
        assert!(key_matches_pattern("users:123", "users:*"));
        assert!(!key_matches_pattern("orders:1", "users:*"));
        assert!(key_matches_pattern("anything", "*"));
        assert!(key_matches_pattern("exact", "exact"));
        assert!(!key_matches_pattern("exact2", "exact"));
    }

    #[test]
    fn test_auto_refresh_due() {
        let mgr = MaterializedViewManager::new();
        let mut def = test_def("v1", "users:*");
        def.auto_refresh = true;
        def.refresh_interval = Some(Duration::from_millis(0));
        mgr.create_view(def).unwrap();

        let due = mgr.auto_refresh_due();
        assert!(due.contains(&"v1".to_string()));
    }

    #[test]
    fn test_empty_source_pattern() {
        let mgr = MaterializedViewManager::new();
        assert!(mgr.create_view(test_def("v1", "")).is_err());
    }
}
