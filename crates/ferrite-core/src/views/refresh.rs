#![forbid(unsafe_code)]

//! Refresh logic for materialized views.

use std::time::Instant;

use super::definition::{RefreshStrategy, ViewDefinition, ViewRefreshResult, ViewRow, ViewStatus};

/// Handles view refresh operations.
#[derive(Debug)]
pub struct ViewRefresher;

impl ViewRefresher {
    /// Create a new refresher.
    pub fn new() -> Self {
        Self
    }

    /// Refresh a view, producing updated rows.
    ///
    /// In a full implementation this would execute the FerriteQL query against
    /// the underlying data store. For now it returns an empty result set.
    pub fn refresh(&self, view: &ViewDefinition) -> Result<ViewRefreshResult, String> {
        let start = Instant::now();
        let was_stale = view.status == ViewStatus::Stale;

        // In a full implementation, we would:
        // 1. Parse the FerriteQL query from view.query
        // 2. Execute it against the store matching view.source_patterns
        // 3. Collect and cache the results
        let _rows: Vec<ViewRow> = Vec::new();

        let duration_ms = start.elapsed().as_millis() as u64;

        Ok(ViewRefreshResult {
            rows_computed: 0,
            duration_ms,
            was_stale,
        })
    }

    /// Check if a periodic view needs refresh based on its interval.
    pub fn needs_periodic_refresh(view: &ViewDefinition) -> bool {
        if let RefreshStrategy::Periodic { interval_secs } = &view.refresh_strategy {
            match view.last_refreshed {
                Some(last) => {
                    let elapsed = chrono::Utc::now().signed_duration_since(last).num_seconds();
                    elapsed >= (*interval_secs as i64)
                }
                None => true,
            }
        } else {
            false
        }
    }
}

impl Default for ViewRefresher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::views::definition::{RefreshStrategy, ViewDefinition, ViewStatus};
    use chrono::Utc;

    fn make_view(strategy: RefreshStrategy, status: ViewStatus) -> ViewDefinition {
        ViewDefinition {
            name: "test".to_string(),
            query: "SELECT * FROM users:*".to_string(),
            source_patterns: vec!["users:*".to_string()],
            refresh_strategy: strategy,
            created_at: Utc::now(),
            last_refreshed: None,
            status,
        }
    }

    #[test]
    fn test_refresh_returns_result() {
        let refresher = ViewRefresher::new();
        let view = make_view(RefreshStrategy::Eager, ViewStatus::Stale);
        let result = refresher.refresh(&view).expect("refresh should succeed");
        assert!(result.was_stale);
    }

    #[test]
    fn test_refresh_active_view() {
        let refresher = ViewRefresher::new();
        let view = make_view(RefreshStrategy::Lazy, ViewStatus::Active);
        let result = refresher.refresh(&view).expect("refresh should succeed");
        assert!(!result.was_stale);
    }

    #[test]
    fn test_needs_periodic_refresh_no_last_refresh() {
        let view = make_view(
            RefreshStrategy::Periodic { interval_secs: 60 },
            ViewStatus::Active,
        );
        assert!(ViewRefresher::needs_periodic_refresh(&view));
    }

    #[test]
    fn test_needs_periodic_refresh_not_periodic() {
        let view = make_view(RefreshStrategy::Eager, ViewStatus::Active);
        assert!(!ViewRefresher::needs_periodic_refresh(&view));
    }
}
