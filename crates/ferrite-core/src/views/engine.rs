#![forbid(unsafe_code)]

//! View engine — manages lifecycle and state for materialized views.

use chrono::Utc;
use dashmap::DashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

use super::definition::{RefreshStrategy, ViewDefinition, ViewRefreshResult, ViewRow, ViewStatus};
use super::dependency::DependencyGraph;
use super::refresh::ViewRefresher;

/// Cached state for a single view: definition + last query results.
#[derive(Debug, Clone)]
struct ViewState {
    definition: ViewDefinition,
    cached_rows: Vec<ViewRow>,
}

/// Thread-safe engine that manages all materialized views.
#[derive(Debug, Clone)]
pub struct ViewEngine {
    views: Arc<DashMap<String, ViewState>>,
    deps: DependencyGraph,
    refresher: Arc<ViewRefresher>,
}

impl ViewEngine {
    /// Create a new view engine.
    pub fn new() -> Self {
        Self {
            views: Arc::new(DashMap::new()),
            deps: DependencyGraph::new(),
            refresher: Arc::new(ViewRefresher::new()),
        }
    }

    /// Register a new materialized view.
    pub fn create_view(&self, def: ViewDefinition) -> Result<(), String> {
        if self.views.contains_key(&def.name) {
            return Err(format!("view '{}' already exists", def.name));
        }

        info!(view = %def.name, query = %def.query, "creating materialized view");

        self.deps.register(&def.name, &def.source_patterns);

        let state = ViewState {
            definition: def.clone(),
            cached_rows: Vec::new(),
        };
        self.views.insert(def.name.clone(), state);

        Ok(())
    }

    /// Remove a materialized view.
    pub fn drop_view(&self, name: &str) -> Result<(), String> {
        if self.views.remove(name).is_none() {
            return Err(format!("view '{}' not found", name));
        }

        self.deps.unregister(name);
        info!(view = %name, "dropped materialized view");

        Ok(())
    }

    /// Get a view's definition.
    pub fn get_view(&self, name: &str) -> Option<ViewDefinition> {
        self.views.get(name).map(|s| s.definition.clone())
    }

    /// List all registered views.
    pub fn list_views(&self) -> Vec<ViewDefinition> {
        self.views
            .iter()
            .map(|entry| entry.value().definition.clone())
            .collect()
    }

    /// Force-refresh a view's cached data.
    pub fn refresh_view(&self, name: &str) -> Result<ViewRefreshResult, String> {
        let mut state = self
            .views
            .get_mut(name)
            .ok_or_else(|| format!("view '{}' not found", name))?;

        state.definition.status = ViewStatus::Refreshing;
        debug!(view = %name, "refreshing view");

        match self.refresher.refresh(&state.definition) {
            Ok(result) => {
                state.definition.status = ViewStatus::Active;
                state.definition.last_refreshed = Some(Utc::now());
                // In full implementation, cached_rows would be populated by refresh
                Ok(result)
            }
            Err(e) => {
                state.definition.status = ViewStatus::Error(e.clone());
                warn!(view = %name, error = %e, "view refresh failed");
                Err(e)
            }
        }
    }

    /// Notify the engine that a key has changed — marks dependent views as stale.
    pub fn notify_change(&self, key: &str) {
        let affected = self.deps.affected_views(key);
        for view_name in &affected {
            if let Some(mut state) = self.views.get_mut(view_name) {
                if state.definition.status == ViewStatus::Active {
                    state.definition.status = ViewStatus::Stale;
                    debug!(view = %view_name, key = %key, "view marked stale");
                }
            }
        }
    }

    /// Query a view's cached data, refreshing if lazy and stale.
    pub fn query_view(&self, name: &str) -> Result<Vec<ViewRow>, String> {
        // Check if lazy refresh needed
        let needs_refresh = {
            let state = self
                .views
                .get(name)
                .ok_or_else(|| format!("view '{}' not found", name))?;

            let is_stale = state.definition.status == ViewStatus::Stale;
            let is_lazy = state.definition.refresh_strategy == RefreshStrategy::Lazy;
            let is_periodic = ViewRefresher::needs_periodic_refresh(&state.definition);

            (is_stale && is_lazy) || is_periodic
        };

        if needs_refresh {
            let _ = self.refresh_view(name);
        }

        let state = self
            .views
            .get(name)
            .ok_or_else(|| format!("view '{}' not found", name))?;

        Ok(state.cached_rows.clone())
    }
}

impl Default for ViewEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::views::definition::{RefreshStrategy, ViewDefinition, ViewStatus};
    use chrono::Utc;

    fn make_def(name: &str, patterns: Vec<&str>, strategy: RefreshStrategy) -> ViewDefinition {
        ViewDefinition {
            name: name.to_string(),
            query: "SELECT * FROM test".to_string(),
            source_patterns: patterns.into_iter().map(String::from).collect(),
            refresh_strategy: strategy,
            created_at: Utc::now(),
            last_refreshed: None,
            status: ViewStatus::Active,
        }
    }

    #[test]
    fn test_create_and_get_view() {
        let engine = ViewEngine::new();
        let def = make_def("v1", vec!["users:*"], RefreshStrategy::Lazy);
        engine.create_view(def).expect("create should succeed");

        let view = engine.get_view("v1");
        assert!(view.is_some());
        assert_eq!(view.expect("view should exist").name, "v1");
    }

    #[test]
    fn test_create_duplicate_view() {
        let engine = ViewEngine::new();
        let def = make_def("v1", vec!["users:*"], RefreshStrategy::Lazy);
        engine.create_view(def.clone()).expect("first create ok");
        assert!(engine.create_view(def).is_err());
    }

    #[test]
    fn test_drop_view() {
        let engine = ViewEngine::new();
        let def = make_def("v1", vec!["users:*"], RefreshStrategy::Lazy);
        engine.create_view(def).expect("create ok");
        engine.drop_view("v1").expect("drop ok");
        assert!(engine.get_view("v1").is_none());
    }

    #[test]
    fn test_drop_nonexistent_view() {
        let engine = ViewEngine::new();
        assert!(engine.drop_view("nope").is_err());
    }

    #[test]
    fn test_list_views() {
        let engine = ViewEngine::new();
        engine
            .create_view(make_def("a", vec!["a:*"], RefreshStrategy::Eager))
            .expect("create ok");
        engine
            .create_view(make_def("b", vec!["b:*"], RefreshStrategy::Lazy))
            .expect("create ok");

        let views = engine.list_views();
        assert_eq!(views.len(), 2);
    }

    #[test]
    fn test_notify_change_marks_stale() {
        let engine = ViewEngine::new();
        engine
            .create_view(make_def("v1", vec!["users:*"], RefreshStrategy::Eager))
            .expect("create ok");

        engine.notify_change("users:42");

        let view = engine.get_view("v1").expect("view should exist");
        assert_eq!(view.status, ViewStatus::Stale);
    }

    #[test]
    fn test_notify_change_no_match() {
        let engine = ViewEngine::new();
        engine
            .create_view(make_def("v1", vec!["users:*"], RefreshStrategy::Eager))
            .expect("create ok");

        engine.notify_change("orders:1");

        let view = engine.get_view("v1").expect("view should exist");
        assert_eq!(view.status, ViewStatus::Active);
    }

    #[test]
    fn test_refresh_view() {
        let engine = ViewEngine::new();
        engine
            .create_view(make_def("v1", vec!["users:*"], RefreshStrategy::Lazy))
            .expect("create ok");

        let result = engine.refresh_view("v1").expect("refresh ok");
        assert!(!result.was_stale);

        let view = engine.get_view("v1").expect("view should exist");
        assert!(view.last_refreshed.is_some());
    }

    #[test]
    fn test_query_lazy_view_refreshes_when_stale() {
        let engine = ViewEngine::new();
        engine
            .create_view(make_def("v1", vec!["users:*"], RefreshStrategy::Lazy))
            .expect("create ok");

        // Mark stale via change notification
        engine.notify_change("users:1");

        // Query should trigger lazy refresh
        let rows = engine.query_view("v1").expect("query ok");
        assert!(rows.is_empty()); // no real data yet

        let view = engine.get_view("v1").expect("view should exist");
        assert_eq!(view.status, ViewStatus::Active);
    }
}
