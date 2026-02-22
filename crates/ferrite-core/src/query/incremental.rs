//! Incremental View Refresh Engine
//!
//! Processes change events (from CDC or direct mutation tracking) and
//! applies deltas to materialized views without a full re-computation.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, RwLock};

use crate::query::{ResultSet, Row, Value as QValue};

/// A data change event that may affect a materialized view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    /// The key that changed.
    pub key: String,
    /// Type of change.
    pub change_type: ChangeType,
    /// Old value (if available).
    pub old_value: Option<String>,
    /// New value (if available).
    pub new_value: Option<String>,
    /// Timestamp of the change.
    pub timestamp_ms: u64,
}

/// Type of data change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeType {
    /// A new key was inserted.
    Insert,
    /// An existing key was updated.
    Update,
    /// A key was deleted.
    Delete,
}

/// Tracks which views depend on which key patterns.
#[derive(Debug, Clone)]
pub struct ViewDependency {
    /// Name of the view.
    pub view_name: String,
    /// Key patterns this view sources data from (glob).
    pub source_patterns: Vec<String>,
}

/// Manages incremental refresh state for all views.
pub struct IncrementalEngine {
    /// Map of view name → pending change events.
    pending: RwLock<HashMap<String, Vec<ChangeEvent>>>,
    /// View dependencies.
    dependencies: RwLock<Vec<ViewDependency>>,
    /// Channel to receive change events from storage.
    #[allow(dead_code)] // Planned for v0.2 — stored for incremental view refresh
    event_rx: Option<mpsc::Receiver<ChangeEvent>>,
    /// Statistics.
    stats: IncrementalStats,
}

/// Statistics for the incremental refresh engine.
#[derive(Debug, Default)]
pub struct IncrementalStats {
    /// Total number of change events processed.
    pub events_processed: std::sync::atomic::AtomicU64,
    /// Number of incremental (delta) refreshes performed.
    pub incremental_refreshes: std::sync::atomic::AtomicU64,
    /// Number of full refreshes performed.
    pub full_refreshes: std::sync::atomic::AtomicU64,
    /// Number of change events dropped (e.g., buffer overflow).
    pub events_dropped: std::sync::atomic::AtomicU64,
}

impl IncrementalEngine {
    /// Create a new engine with an event receiver channel.
    pub fn new(event_rx: mpsc::Receiver<ChangeEvent>) -> Self {
        Self {
            pending: RwLock::new(HashMap::new()),
            dependencies: RwLock::new(Vec::new()),
            event_rx: Some(event_rx),
            stats: IncrementalStats::default(),
        }
    }

    /// Create an engine without a channel (events are pushed manually).
    pub fn standalone() -> Self {
        Self {
            pending: RwLock::new(HashMap::new()),
            dependencies: RwLock::new(Vec::new()),
            event_rx: None,
            stats: IncrementalStats::default(),
        }
    }

    /// Register a view dependency so the engine knows which views to refresh
    /// when keys matching certain patterns change.
    pub async fn register_dependency(&self, dep: ViewDependency) {
        self.dependencies.write().await.push(dep);
    }

    /// Remove all dependencies for a view.
    pub async fn remove_dependencies(&self, view_name: &str) {
        self.dependencies
            .write()
            .await
            .retain(|d| d.view_name != view_name);
        self.pending.write().await.remove(view_name);
    }

    /// Push a change event into the engine. It will be routed to views whose
    /// source patterns match the event key.
    pub async fn push_event(&self, event: ChangeEvent) {
        use std::sync::atomic::Ordering;
        self.stats.events_processed.fetch_add(1, Ordering::Relaxed);

        let deps = self.dependencies.read().await;
        let mut pending = self.pending.write().await;

        for dep in deps.iter() {
            if dep
                .source_patterns
                .iter()
                .any(|p| glob_matches(p, &event.key))
            {
                pending
                    .entry(dep.view_name.clone())
                    .or_insert_with(Vec::new)
                    .push(event.clone());
            }
        }
    }

    /// Drain pending events for a view. Returns the events that have been
    /// buffered since the last drain. The caller is responsible for applying
    /// the delta to the materialized result set.
    pub async fn drain_pending(&self, view_name: &str) -> Vec<ChangeEvent> {
        self.pending
            .write()
            .await
            .remove(view_name)
            .unwrap_or_default()
    }

    /// Returns the number of pending events per view.
    pub async fn pending_counts(&self) -> HashMap<String, usize> {
        self.pending
            .read()
            .await
            .iter()
            .map(|(k, v)| (k.clone(), v.len()))
            .collect()
    }

    /// Apply a batch of change events to an existing result set.
    ///
    /// This is a simplified delta application: inserts are appended, deletes
    /// remove matching rows, and updates replace matching rows. The `key_column`
    /// indicates which column holds the key for matching.
    pub fn apply_delta(
        result: &mut ResultSet,
        events: &[ChangeEvent],
        key_column: usize,
    ) -> DeltaApplyResult {
        let mut inserted = 0usize;
        let mut updated = 0usize;
        let mut deleted = 0usize;

        for event in events {
            match event.change_type {
                ChangeType::Insert => {
                    if let Some(ref val) = event.new_value {
                        result.rows.push(Row::new(vec![
                            QValue::String(event.key.clone()),
                            QValue::String(val.clone()),
                        ]));
                        inserted += 1;
                    }
                }
                ChangeType::Update => {
                    let key_val = QValue::String(event.key.clone());
                    if let Some(row) = result
                        .rows
                        .iter_mut()
                        .find(|r| r.get(key_column) == Some(&key_val))
                    {
                        if let Some(ref val) = event.new_value {
                            if row.values.len() > key_column + 1 {
                                row.values[key_column + 1] = QValue::String(val.clone());
                            }
                        }
                        updated += 1;
                    }
                }
                ChangeType::Delete => {
                    let key_val = QValue::String(event.key.clone());
                    let before = result.rows.len();
                    result.rows.retain(|r| r.get(key_column) != Some(&key_val));
                    deleted += before - result.rows.len();
                }
            }
        }

        DeltaApplyResult {
            inserted,
            updated,
            deleted,
        }
    }
}

/// Outcome of applying a delta to a result set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaApplyResult {
    /// Number of rows inserted.
    pub inserted: usize,
    /// Number of rows updated.
    pub updated: usize,
    /// Number of rows deleted.
    pub deleted: usize,
}

/// Simple glob matcher supporting `*` and `?`.
fn glob_matches(pattern: &str, input: &str) -> bool {
    glob_rec(pattern.as_bytes(), input.as_bytes())
}

fn glob_rec(p: &[u8], s: &[u8]) -> bool {
    match (p.first(), s.first()) {
        (None, None) => true,
        (Some(b'*'), _) => glob_rec(&p[1..], s) || (!s.is_empty() && glob_rec(p, &s[1..])),
        (Some(b'?'), Some(_)) => glob_rec(&p[1..], &s[1..]),
        (Some(a), Some(b)) if a == b => glob_rec(&p[1..], &s[1..]),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_result_set(rows: Vec<Vec<String>>) -> ResultSet {
        let q_rows = rows
            .into_iter()
            .map(|r| Row::new(r.into_iter().map(QValue::String).collect()))
            .collect();
        ResultSet::new(vec!["key".to_string(), "value".to_string()], q_rows)
    }

    #[test]
    fn delta_insert() {
        let mut rs = make_result_set(vec![vec!["k1".into(), "v1".into()]]);
        let events = vec![ChangeEvent {
            key: "k2".into(),
            change_type: ChangeType::Insert,
            old_value: None,
            new_value: Some("v2".into()),
            timestamp_ms: 0,
        }];
        let result = IncrementalEngine::apply_delta(&mut rs, &events, 0);
        assert_eq!(result.inserted, 1);
        assert_eq!(rs.rows.len(), 2);
    }

    #[test]
    fn delta_delete() {
        let mut rs = make_result_set(vec![
            vec!["k1".into(), "v1".into()],
            vec!["k2".into(), "v2".into()],
        ]);
        let events = vec![ChangeEvent {
            key: "k1".into(),
            change_type: ChangeType::Delete,
            old_value: None,
            new_value: None,
            timestamp_ms: 0,
        }];
        let result = IncrementalEngine::apply_delta(&mut rs, &events, 0);
        assert_eq!(result.deleted, 1);
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0].get(0), Some(&QValue::String("k2".into())));
    }

    #[test]
    fn delta_update() {
        let mut rs = make_result_set(vec![vec!["k1".into(), "old".into()]]);
        let events = vec![ChangeEvent {
            key: "k1".into(),
            change_type: ChangeType::Update,
            old_value: Some("old".into()),
            new_value: Some("new".into()),
            timestamp_ms: 0,
        }];
        let result = IncrementalEngine::apply_delta(&mut rs, &events, 0);
        assert_eq!(result.updated, 1);
        assert_eq!(rs.rows[0].get(1), Some(&QValue::String("new".into())));
    }

    #[tokio::test]
    async fn push_event_routes_to_matching_view() {
        let engine = IncrementalEngine::standalone();
        engine
            .register_dependency(ViewDependency {
                view_name: "user_view".into(),
                source_patterns: vec!["users:*".into()],
            })
            .await;

        engine
            .push_event(ChangeEvent {
                key: "users:123".into(),
                change_type: ChangeType::Insert,
                old_value: None,
                new_value: Some("alice".into()),
                timestamp_ms: 1000,
            })
            .await;

        // Should not match orders
        engine
            .push_event(ChangeEvent {
                key: "orders:456".into(),
                change_type: ChangeType::Insert,
                old_value: None,
                new_value: Some("order".into()),
                timestamp_ms: 1001,
            })
            .await;

        let pending = engine.drain_pending("user_view").await;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].key, "users:123");
    }

    #[test]
    fn glob_matching() {
        assert!(glob_matches("users:*", "users:123"));
        assert!(glob_matches("users:*", "users:"));
        assert!(!glob_matches("users:*", "orders:1"));
        assert!(glob_matches("*", "anything"));
        assert!(glob_matches("a?c", "abc"));
        assert!(!glob_matches("a?c", "abbc"));
    }
}
