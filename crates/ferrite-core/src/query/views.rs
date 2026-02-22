//! FerriteQL Materialized Views
//!
//! Manages materialized views with automatic refresh.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::query::ast::{RefreshType, Statement};
use crate::query::{QueryError, ResultSet};

/// View manager
pub struct ViewManager {
    views: HashMap<String, MaterializedView>,
    #[allow(dead_code)] // Planned for v0.2 — stored for view query materialization
    store: Arc<crate::storage::Store>,
}

impl ViewManager {
    /// Create a new view manager
    pub fn new(store: Arc<crate::storage::Store>) -> Self {
        Self {
            views: HashMap::new(),
            store,
        }
    }

    /// Create a new materialized view
    pub async fn create(
        &mut self,
        name: &str,
        query: Statement,
        refresh_interval_ms: Option<u64>,
    ) -> Result<(), QueryError> {
        if self.views.contains_key(name) {
            return Err(QueryError::View(format!("View '{}' already exists", name)));
        }

        let view = MaterializedView {
            name: name.to_string(),
            query,
            refresh_type: if refresh_interval_ms.is_some() {
                RefreshType::Incremental
            } else {
                RefreshType::OnDemand
            },
            refresh_interval_ms,
            last_refresh: None,
            data: Arc::new(RwLock::new(None)),
            version: 0,
        };

        self.views.insert(name.to_string(), view);
        Ok(())
    }

    /// Drop a materialized view
    pub async fn drop_view(&mut self, name: &str) -> Result<(), QueryError> {
        if self.views.remove(name).is_none() {
            return Err(QueryError::View(format!("View '{}' not found", name)));
        }
        Ok(())
    }

    /// Get a view by name
    pub fn get(&self, name: &str) -> Option<&MaterializedView> {
        self.views.get(name)
    }

    /// List all views
    pub fn list(&self) -> Vec<&MaterializedView> {
        self.views.values().collect()
    }

    /// Refresh a view
    pub async fn refresh(&mut self, name: &str, result: ResultSet) -> Result<(), QueryError> {
        let view = self
            .views
            .get_mut(name)
            .ok_or_else(|| QueryError::View(format!("View '{}' not found", name)))?;

        let mut data = view.data.write().await;
        *data = Some(result);
        view.last_refresh = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        );
        view.version += 1;

        Ok(())
    }

    /// Get cached data for a view
    pub async fn get_data(&self, name: &str) -> Option<ResultSet> {
        let view = self.views.get(name)?;
        let data = view.data.read().await;
        data.clone()
    }

    /// Check if a view needs refresh
    pub fn needs_refresh(&self, name: &str) -> bool {
        if let Some(view) = self.views.get(name) {
            if let Some(interval) = view.refresh_interval_ms {
                if let Some(last_refresh) = view.last_refresh {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    return now - last_refresh > interval;
                }
                return true; // Never refreshed
            }
        }
        false
    }
}

/// Materialized view definition
pub struct MaterializedView {
    /// View name
    pub name: String,
    /// Underlying query
    pub query: Statement,
    /// Refresh type
    pub refresh_type: RefreshType,
    /// Refresh interval in milliseconds (for automatic refresh)
    pub refresh_interval_ms: Option<u64>,
    /// Last refresh timestamp
    pub last_refresh: Option<u64>,
    /// Cached data
    pub data: Arc<RwLock<Option<ResultSet>>>,
    /// Version number (incremented on each refresh)
    pub version: u64,
}

impl MaterializedView {
    /// Get view metadata
    pub fn metadata(&self) -> ViewMetadata {
        ViewMetadata {
            name: self.name.clone(),
            refresh_type: self.refresh_type.clone(),
            refresh_interval_ms: self.refresh_interval_ms,
            last_refresh: self.last_refresh,
            version: self.version,
        }
    }
}

/// View metadata (for INFO commands)
#[derive(Clone, Debug)]
pub struct ViewMetadata {
    /// View name.
    pub name: String,
    /// Refresh strategy for this view.
    pub refresh_type: RefreshType,
    /// Refresh interval in milliseconds, if periodic.
    pub refresh_interval_ms: Option<u64>,
    /// Timestamp of the last refresh in milliseconds, if any.
    pub last_refresh: Option<u64>,
    /// Monotonically increasing version counter.
    pub version: u64,
}

/// View subscription for real-time updates
pub struct ViewSubscription {
    /// View name
    pub view_name: String,
    /// Subscriber ID
    pub subscriber_id: String,
    /// Filter condition (optional)
    pub filter: Option<String>,
    /// Callback channel
    pub sender: tokio::sync::mpsc::Sender<ViewUpdate>,
}

/// View update notification
#[derive(Clone, Debug)]
pub struct ViewUpdate {
    /// View name
    pub view_name: String,
    /// Update type
    pub update_type: ViewUpdateType,
    /// Affected rows
    pub rows: Option<ResultSet>,
    /// Timestamp
    pub timestamp: u64,
}

/// Type of view update
#[derive(Clone, Debug)]
pub enum ViewUpdateType {
    /// Full refresh
    FullRefresh,
    /// Incremental insert
    Insert,
    /// Incremental update
    Update,
    /// Incremental delete
    Delete,
}

/// Subscription manager for views
pub struct SubscriptionManager {
    subscriptions: HashMap<String, Vec<ViewSubscription>>,
}

impl SubscriptionManager {
    /// Create a new subscription manager
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
        }
    }

    /// Subscribe to a view
    pub fn subscribe(&mut self, subscription: ViewSubscription) {
        self.subscriptions
            .entry(subscription.view_name.clone())
            .or_default()
            .push(subscription);
    }

    /// Unsubscribe from a view
    pub fn unsubscribe(&mut self, view_name: &str, subscriber_id: &str) {
        if let Some(subs) = self.subscriptions.get_mut(view_name) {
            subs.retain(|s| s.subscriber_id != subscriber_id);
        }
    }

    /// Notify subscribers of a view update
    pub async fn notify(&self, view_name: &str, update: ViewUpdate) {
        if let Some(subs) = self.subscriptions.get(view_name) {
            for sub in subs {
                let _ = sub.sender.send(update.clone()).await;
            }
        }
    }

    /// Get subscriber count for a view
    pub fn subscriber_count(&self, view_name: &str) -> usize {
        self.subscriptions
            .get(view_name)
            .map(|s| s.len())
            .unwrap_or(0)
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::SelectStatement;
    use crate::storage::Store;

    fn create_test_store() -> Arc<Store> {
        Arc::new(Store::new(16))
    }

    #[tokio::test]
    async fn test_view_create_drop() {
        let store = create_test_store();
        let mut manager = ViewManager::new(store);

        let query = Statement::Select(SelectStatement::default());
        manager.create("test_view", query, None).await.unwrap();

        assert!(manager.get("test_view").is_some());

        manager.drop_view("test_view").await.unwrap();
        assert!(manager.get("test_view").is_none());
    }

    #[tokio::test]
    async fn test_view_refresh() {
        let store = create_test_store();
        let mut manager = ViewManager::new(store);

        let query = Statement::Select(SelectStatement::default());
        manager
            .create("test_view", query, Some(1000))
            .await
            .unwrap();

        let result = ResultSet::empty();
        manager.refresh("test_view", result).await.unwrap();

        let view = manager.get("test_view").unwrap();
        assert!(view.last_refresh.is_some());
        assert_eq!(view.version, 1);
    }

    #[tokio::test]
    async fn test_subscription_manager() {
        let mut manager = SubscriptionManager::new();
        let (tx, _rx) = tokio::sync::mpsc::channel(10);

        let sub = ViewSubscription {
            view_name: "test_view".to_string(),
            subscriber_id: "sub1".to_string(),
            filter: None,
            sender: tx,
        };

        manager.subscribe(sub);
        assert_eq!(manager.subscriber_count("test_view"), 1);

        manager.unsubscribe("test_view", "sub1");
        assert_eq!(manager.subscriber_count("test_view"), 0);
    }
}
