#![forbid(unsafe_code)]

//! View definition types for materialized views.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Defines a materialized view over key patterns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewDefinition {
    /// Unique name for this view.
    pub name: String,
    /// FerriteQL query string.
    pub query: String,
    /// Key patterns this view depends on (e.g., "users:*").
    pub source_patterns: Vec<String>,
    /// How and when the view is refreshed.
    pub refresh_strategy: RefreshStrategy,
    /// When this view was created.
    pub created_at: DateTime<Utc>,
    /// When this view was last refreshed.
    pub last_refreshed: Option<DateTime<Utc>>,
    /// Current status of the view.
    pub status: ViewStatus,
}

/// Controls how a materialized view is refreshed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RefreshStrategy {
    /// Refresh immediately when source data changes.
    Eager,
    /// Refresh on next read if stale.
    Lazy,
    /// Refresh on a fixed interval.
    Periodic {
        /// Interval in seconds between refreshes.
        interval_secs: u64,
    },
}

/// Runtime status of a materialized view.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ViewStatus {
    /// View is active and up-to-date.
    Active,
    /// Source data changed; view not yet refreshed.
    Stale,
    /// View is currently being refreshed.
    Refreshing,
    /// An error occurred during the last refresh.
    Error(String),
    /// View is disabled and will not be refreshed.
    Disabled,
}

/// A single row in a materialized view result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewRow {
    /// Key associated with this row.
    pub key: String,
    /// Value associated with this row.
    pub value: String,
}

/// Result of a view refresh operation.
#[derive(Debug, Clone)]
pub struct ViewRefreshResult {
    /// Number of rows computed.
    pub rows_computed: usize,
    /// Duration in milliseconds.
    pub duration_ms: u64,
    /// Whether the view was stale before refresh.
    pub was_stale: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_refresh_strategy_equality() {
        assert_eq!(RefreshStrategy::Eager, RefreshStrategy::Eager);
        assert_eq!(RefreshStrategy::Lazy, RefreshStrategy::Lazy);
        assert_eq!(
            RefreshStrategy::Periodic { interval_secs: 60 },
            RefreshStrategy::Periodic { interval_secs: 60 }
        );
        assert_ne!(RefreshStrategy::Eager, RefreshStrategy::Lazy);
    }

    #[test]
    fn test_view_status_equality() {
        assert_eq!(ViewStatus::Active, ViewStatus::Active);
        assert_eq!(ViewStatus::Stale, ViewStatus::Stale);
        assert_ne!(ViewStatus::Active, ViewStatus::Stale);
    }

    #[test]
    fn test_view_definition_clone() {
        let def = ViewDefinition {
            name: "test_view".to_string(),
            query: "SELECT * FROM users:*".to_string(),
            source_patterns: vec!["users:*".to_string()],
            refresh_strategy: RefreshStrategy::Lazy,
            created_at: Utc::now(),
            last_refreshed: None,
            status: ViewStatus::Active,
        };
        let cloned = def.clone();
        assert_eq!(cloned.name, "test_view");
        assert_eq!(cloned.source_patterns.len(), 1);
    }
}
