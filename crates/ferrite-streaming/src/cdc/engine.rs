//! CDC Engine
//!
//! The main engine that coordinates change capture, subscriptions, and sinks.

use super::event::{ChangeEvent, ChangeMetadata, ChangeSource, Operation};
use super::log::{ChangeLog, ChangeLogConfig, ChangeLogStats};
use super::sink::{SinkConfig, SinkManager, SinkStatus};
use super::subscription::{
    DeliveryMode, OutputFormat, Subscription, SubscriptionId, SubscriptionInfo,
    SubscriptionManager, SubscriptionState,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

/// CDC Engine configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdcConfig {
    /// Whether CDC is enabled
    pub enabled: bool,
    /// Whether to capture old values before writes
    pub capture_old_values: bool,
    /// Change log configuration
    pub log: ChangeLogConfig,
    /// Default output format
    pub default_format: OutputFormat,
    /// Default batch size
    pub default_batch_size: usize,
    /// Maximum batch delay in milliseconds
    pub max_batch_delay_ms: u64,
}

impl Default for CdcConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            capture_old_values: false,
            log: ChangeLogConfig::default(),
            default_format: OutputFormat::Json,
            default_batch_size: 100,
            max_batch_delay_ms: 100,
        }
    }
}

/// CDC Engine
pub struct CdcEngine {
    /// Configuration
    config: CdcConfig,
    /// Change log
    log: Arc<ChangeLog>,
    /// Subscription manager
    subscriptions: Arc<SubscriptionManager>,
    /// Sink manager
    sinks: Arc<SinkManager>,
    /// Total events captured
    events_captured: AtomicU64,
    /// Total events delivered
    events_delivered: AtomicU64,
    /// Whether engine is running
    running: AtomicBool,
}

impl CdcEngine {
    /// Create a new CDC engine
    pub fn new(config: CdcConfig) -> std::io::Result<Self> {
        let log = Arc::new(ChangeLog::new(config.log.clone())?);

        Ok(Self {
            config,
            log,
            subscriptions: Arc::new(SubscriptionManager::new()),
            sinks: Arc::new(SinkManager::new()),
            events_captured: AtomicU64::new(0),
            events_delivered: AtomicU64::new(0),
            running: AtomicBool::new(true),
        })
    }

    /// Capture a change event
    pub async fn capture(&self, event: ChangeEvent) {
        if !self.config.enabled || !self.running.load(Ordering::Relaxed) {
            return;
        }

        // Check if any subscription matches
        if !self.subscriptions.matches(&event) {
            return;
        }

        // Append to log
        if let Ok(_id) = self.log.append(event.clone()).await {
            self.events_captured.fetch_add(1, Ordering::Relaxed);

            // Get matching subscriptions and notify
            let matching = self.subscriptions.matching(&event);
            for sub in matching {
                // For sink delivery, send to attached sinks
                if let DeliveryMode::Sink { ref sink_name } = sub.delivery {
                    if let Some(sink) = self.sinks.get_by_name(sink_name) {
                        let _ = sink.send(event.clone()).await;
                        self.events_delivered.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            // Notify push subscribers
            self.subscriptions.notify(event).await;
        }
    }

    /// Capture a SET operation
    pub async fn capture_set(&self, db: u8, key: &Bytes, value: &Bytes, old_value: Option<&Bytes>) {
        let mut event = ChangeEvent::new(Operation::Set, key.clone(), db)
            .with_value(value.clone())
            .with_metadata(ChangeMetadata::local("SET"));

        if let Some(old) = old_value {
            event = event.with_old_value(old.clone());
        }

        self.capture(event).await;
    }

    /// Capture a DEL operation
    pub async fn capture_del(&self, db: u8, key: &Bytes, old_value: Option<&Bytes>) {
        let mut event = ChangeEvent::new(Operation::Del, key.clone(), db)
            .with_metadata(ChangeMetadata::local("DEL"));

        if let Some(old) = old_value {
            event = event.with_old_value(old.clone());
        }

        self.capture(event).await;
    }

    /// Capture an EXPIRE operation
    pub async fn capture_expire(&self, db: u8, key: &Bytes, ttl_ms: u64) {
        let event = ChangeEvent::new(Operation::Expire { ttl_ms }, key.clone(), db)
            .with_metadata(ChangeMetadata::local("EXPIRE"));

        self.capture(event).await;
    }

    /// Capture an expiration event (internal)
    pub async fn capture_expired(&self, db: u8, key: &Bytes) {
        let mut metadata = ChangeMetadata::local("EXPIRED");
        metadata.source = ChangeSource::Internal;

        let event = ChangeEvent::new(Operation::Expired, key.clone(), db).with_metadata(metadata);

        self.capture(event).await;
    }

    /// Subscribe to changes
    pub async fn subscribe(
        &self,
        name: impl Into<String>,
        patterns: Vec<String>,
        options: SubscribeOptions,
    ) -> Result<SubscriptionId, String> {
        let mut sub = Subscription::new(name, patterns);

        if !options.operations.is_empty() {
            sub = sub.with_operations(options.operations);
        }

        if !options.databases.is_empty() {
            sub = sub.with_databases(options.databases);
        }

        if let Some(format) = options.format {
            sub = sub.with_format(format);
        }

        if let Some(delivery) = options.delivery {
            sub = sub.with_delivery(delivery);
        }

        if let Some(position) = options.from_position {
            sub = sub.from_position(position);
        } else if options.from_earliest {
            sub = sub.from_position(self.log.oldest_position().await);
        } else {
            sub = sub.from_position(self.log.current_position());
        }

        if options.with_old_values {
            sub = sub.with_old_values();
        }

        self.subscriptions.create(sub)
    }

    /// Unsubscribe
    pub fn unsubscribe(&self, name: &str) -> bool {
        self.subscriptions.delete(name)
    }

    /// Get subscription info
    pub fn subscription_info(&self, name: &str) -> Option<SubscriptionInfo> {
        self.subscriptions.get_by_name(name).map(|s| s.info())
    }

    /// List subscriptions
    pub fn list_subscriptions(&self) -> Vec<SubscriptionInfo> {
        self.subscriptions.list()
    }

    /// Read events from a subscription
    pub async fn read(&self, name: &str, count: usize) -> Result<Vec<ChangeEvent>, String> {
        let sub = self
            .subscriptions
            .get_by_name(name)
            .ok_or_else(|| format!("Subscription '{}' not found", name))?;

        if sub.state != SubscriptionState::Active {
            return Err(format!("Subscription is {}", sub.state.name()));
        }

        let from_id = sub.position();
        let events = self
            .log
            .read_from(from_id, count * 2) // Read extra to account for filtering
            .await
            .map_err(|e| e.to_string())?;

        // Filter events that match subscription
        let matching: Vec<_> = events
            .into_iter()
            .filter(|e| sub.matches(e))
            .take(count)
            .collect();

        // Update position
        if let Some(last) = matching.last() {
            sub.update_position(last.id + 1);
        }

        Ok(matching)
    }

    /// Acknowledge processed events
    pub fn acknowledge(&self, name: &str, event_id: u64) -> Result<(), String> {
        let sub = self
            .subscriptions
            .get_by_name(name)
            .ok_or_else(|| format!("Subscription '{}' not found", name))?;

        sub.acknowledge(event_id);
        Ok(())
    }

    /// Seek subscription to position
    pub fn seek(&self, name: &str, position: u64) -> Result<(), String> {
        let sub = self
            .subscriptions
            .get_by_name(name)
            .ok_or_else(|| format!("Subscription '{}' not found", name))?;

        sub.seek(position);
        Ok(())
    }

    /// Get current position for subscription
    pub fn position(&self, name: &str) -> Result<u64, String> {
        let sub = self
            .subscriptions
            .get_by_name(name)
            .ok_or_else(|| format!("Subscription '{}' not found", name))?;

        Ok(sub.position())
    }

    /// Create a sink
    pub fn create_sink(&self, name: impl Into<String>, config: SinkConfig) -> Result<u64, String> {
        self.sinks.create(name, config)
    }

    /// Delete a sink
    pub async fn delete_sink(&self, name: &str) -> bool {
        self.sinks.delete(name).await
    }

    /// Attach subscription to sink
    pub fn attach_sink(&self, subscription: &str, sink: &str) -> Result<(), String> {
        self.sinks.attach(subscription, sink)
    }

    /// Detach subscription from sink
    pub fn detach_sink(&self, subscription: &str, sink: &str) -> Result<(), String> {
        self.sinks.detach(subscription, sink)
    }

    /// List sinks
    pub async fn list_sinks(&self) -> Vec<SinkStatus> {
        self.sinks.list().await
    }

    /// Get sink status
    pub async fn sink_status(&self, name: &str) -> Option<SinkStatus> {
        self.sinks.get_by_name(name).map(|s| {
            // Can't await in map, so we use a workaround
            tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(s.status()))
        })
    }

    /// Get change log statistics
    pub async fn log_stats(&self) -> ChangeLogStats {
        self.log.stats().await
    }

    /// Read events by ID range
    pub async fn log_range(&self, from: u64, to: u64, count: usize) -> Vec<ChangeEvent> {
        let limit = std::cmp::min(count, (to - from) as usize);
        self.log.read_from(from, limit).await.unwrap_or_default()
    }

    /// Read events by time range
    pub async fn log_time_range(
        &self,
        from: SystemTime,
        to: SystemTime,
        count: usize,
    ) -> Vec<ChangeEvent> {
        self.log
            .read_time_range(from, to, count)
            .await
            .unwrap_or_default()
    }

    /// Compact the change log
    pub async fn compact(&self) -> Result<(), String> {
        self.log.compact().await.map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Get engine info
    pub async fn info(&self) -> CdcInfo {
        let log_stats = self.log.stats().await;

        CdcInfo {
            enabled: self.config.enabled,
            capture_old_values: self.config.capture_old_values,
            subscriptions: self.subscriptions.len(),
            sinks: self.sinks.len(),
            events_captured: self.events_captured.load(Ordering::Relaxed),
            events_delivered: self.events_delivered.load(Ordering::Relaxed),
            log_segments: log_stats.total_segments,
            log_size: log_stats.total_size,
            log_events: log_stats.total_events,
            oldest_event: log_stats.oldest_event,
            newest_event: log_stats.newest_event,
        }
    }

    /// Get engine statistics
    pub fn stats(&self) -> CdcStats {
        CdcStats {
            events_captured: self.events_captured.load(Ordering::Relaxed),
            events_delivered: self.events_delivered.load(Ordering::Relaxed),
            subscriptions: self.subscriptions.len(),
            sinks: self.sinks.len(),
        }
    }

    /// Check if engine is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Enable the engine
    pub fn enable(&self) {
        self.running.store(true, Ordering::Relaxed);
    }

    /// Disable the engine
    pub fn disable(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// Check if capture_old_values is enabled
    pub fn captures_old_values(&self) -> bool {
        self.config.capture_old_values
    }
}

impl Default for CdcEngine {
    fn default() -> Self {
        Self::new(CdcConfig::default()).expect("Failed to create default CDC engine")
    }
}

/// Options for creating a subscription
#[derive(Clone, Debug, Default)]
pub struct SubscribeOptions {
    /// Operations to capture
    pub operations: HashSet<String>,
    /// Databases to monitor
    pub databases: HashSet<u8>,
    /// Output format
    pub format: Option<OutputFormat>,
    /// Delivery mode
    pub delivery: Option<DeliveryMode>,
    /// Start from specific position
    pub from_position: Option<u64>,
    /// Start from earliest event
    pub from_earliest: bool,
    /// Capture old values
    pub with_old_values: bool,
}

/// CDC engine information
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdcInfo {
    /// Whether CDC is enabled
    pub enabled: bool,
    /// Whether old value capture is enabled
    pub capture_old_values: bool,
    /// Number of subscriptions
    pub subscriptions: usize,
    /// Number of sinks
    pub sinks: usize,
    /// Total events captured
    pub events_captured: u64,
    /// Total events delivered
    pub events_delivered: u64,
    /// Number of log segments
    pub log_segments: usize,
    /// Total log size in bytes
    pub log_size: u64,
    /// Total events in log
    pub log_events: usize,
    /// Oldest event ID
    pub oldest_event: u64,
    /// Newest event ID
    pub newest_event: u64,
}

/// CDC statistics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdcStats {
    /// Total events captured
    pub events_captured: u64,
    /// Total events delivered
    pub events_delivered: u64,
    /// Number of subscriptions
    pub subscriptions: usize,
    /// Number of sinks
    pub sinks: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cdc_engine_creation() {
        let engine = CdcEngine::default();
        assert!(engine.is_enabled());

        let info = engine.info().await;
        assert!(info.enabled);
        assert_eq!(info.subscriptions, 0);
        assert_eq!(info.sinks, 0);
    }

    #[tokio::test]
    async fn test_subscription_lifecycle() {
        let engine = CdcEngine::default();

        // Create subscription
        let id = engine
            .subscribe("test", vec!["user:*".to_string()], Default::default())
            .await
            .unwrap();

        assert!(id > 0);

        // Get info
        let info = engine.subscription_info("test").unwrap();
        assert_eq!(info.name, "test");
        assert_eq!(info.patterns, vec!["user:*"]);

        // List subscriptions
        let subs = engine.list_subscriptions();
        assert_eq!(subs.len(), 1);

        // Unsubscribe
        assert!(engine.unsubscribe("test"));
        assert!(engine.subscription_info("test").is_none());
    }

    #[tokio::test]
    async fn test_capture_and_read() {
        let engine = CdcEngine::default();

        // Create subscription for all events
        engine
            .subscribe("all", vec!["*".to_string()], Default::default())
            .await
            .unwrap();

        // Capture some events
        engine
            .capture_set(
                0,
                &Bytes::from_static(b"key1"),
                &Bytes::from_static(b"value1"),
                None,
            )
            .await;
        engine
            .capture_set(
                0,
                &Bytes::from_static(b"key2"),
                &Bytes::from_static(b"value2"),
                None,
            )
            .await;
        engine
            .capture_del(0, &Bytes::from_static(b"key1"), None)
            .await;

        // Read events
        let events = engine.read("all", 10).await.unwrap();
        assert_eq!(events.len(), 3);

        // Check event contents
        assert_eq!(events[0].key, Bytes::from_static(b"key1"));
        assert!(matches!(events[0].operation, Operation::Set));

        assert_eq!(events[2].key, Bytes::from_static(b"key1"));
        assert!(matches!(events[2].operation, Operation::Del));
    }

    #[tokio::test]
    async fn test_filtered_subscription() {
        let engine = CdcEngine::default();

        // Subscribe only to user:* keys
        engine
            .subscribe("users", vec!["user:*".to_string()], Default::default())
            .await
            .unwrap();

        // Capture events
        engine
            .capture_set(
                0,
                &Bytes::from_static(b"user:1"),
                &Bytes::from_static(b"alice"),
                None,
            )
            .await;
        engine
            .capture_set(
                0,
                &Bytes::from_static(b"order:1"),
                &Bytes::from_static(b"pending"),
                None,
            )
            .await;
        engine
            .capture_set(
                0,
                &Bytes::from_static(b"user:2"),
                &Bytes::from_static(b"bob"),
                None,
            )
            .await;

        // Read events - should only get user events
        let events = engine.read("users", 10).await.unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|e| e.key.starts_with(b"user:")));
    }

    #[tokio::test]
    async fn test_position_tracking() {
        let engine = CdcEngine::default();

        engine
            .subscribe("test", vec!["*".to_string()], Default::default())
            .await
            .unwrap();

        // Capture events
        for i in 0..10 {
            engine
                .capture_set(
                    0,
                    &Bytes::from(format!("key{}", i)),
                    &Bytes::from(format!("value{}", i)),
                    None,
                )
                .await;
        }

        // Read first batch
        let events1 = engine.read("test", 3).await.unwrap();
        assert_eq!(events1.len(), 3);

        // Read next batch
        let events2 = engine.read("test", 3).await.unwrap();
        assert_eq!(events2.len(), 3);

        // Events should be different
        assert_ne!(events1[0].id, events2[0].id);

        // Check position
        let pos = engine.position("test").unwrap();
        assert!(pos > 0);
    }

    #[tokio::test]
    async fn test_sink_lifecycle() {
        let engine = CdcEngine::default();

        // Create sink
        let config = SinkConfig::Http(super::super::sink::HttpSinkConfig::default());
        let id = engine.create_sink("http-sink", config).unwrap();
        assert!(id > 0);

        // List sinks
        let sinks = engine.list_sinks().await;
        assert_eq!(sinks.len(), 1);
        assert_eq!(sinks[0].name, "http-sink");

        // Delete sink
        assert!(engine.delete_sink("http-sink").await);
        assert!(engine.list_sinks().await.is_empty());
    }

    #[tokio::test]
    async fn test_log_stats() {
        let engine = CdcEngine::default();

        // Capture some events
        for i in 0..5 {
            let event = ChangeEvent::new(Operation::Set, Bytes::from(format!("key{}", i)), 0);
            engine.capture(event).await;
        }

        let stats = engine.log_stats().await;
        // Stats might be 0 if no subscription matches
        assert!(stats.total_segments >= 1);
    }

    #[test]
    fn test_cdc_stats() {
        let engine = CdcEngine::default();

        let stats = engine.stats();
        assert_eq!(stats.events_captured, 0);
        assert_eq!(stats.events_delivered, 0);
        assert_eq!(stats.subscriptions, 0);
        assert_eq!(stats.sinks, 0);
    }
}
