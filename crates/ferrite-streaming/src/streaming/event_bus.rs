//! Unified Event Bus for Streaming Functions
//!
//! Connects streaming, serverless, triggers, and CDC subsystems into a single
//! coherent programming model with function chaining, backpressure, and a
//! unified event schema.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// EventType
// ---------------------------------------------------------------------------

/// Categorizes the kind of data mutation or action that produced an event.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum EventType {
    KeySet,
    KeyDelete,
    KeyExpire,
    ListPush,
    ListPop,
    HashSet,
    HashDelete,
    SetAdd,
    SetRemove,
    SortedSetAdd,
    SortedSetRemove,
    StreamAppend,
    Custom(String),
}

// ---------------------------------------------------------------------------
// DataEvent
// ---------------------------------------------------------------------------

/// Unified event schema shared across streaming, serverless, triggers, and CDC.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataEvent {
    /// Unique event identifier (UUID v4).
    pub id: String,
    /// When the event was created.
    pub timestamp: SystemTime,
    /// The kind of mutation that produced this event.
    pub event_type: EventType,
    /// The key that was affected.
    pub key: String,
    /// The logical database number.
    pub database: u8,
    /// Value before the mutation, if available.
    pub old_value: Option<Vec<u8>>,
    /// Value after the mutation, if available.
    pub new_value: Option<Vec<u8>>,
    /// Arbitrary metadata (tenant_id, source_node, etc.).
    pub metadata: HashMap<String, String>,
    /// Optional TTL associated with the key.
    pub ttl: Option<Duration>,
}

impl DataEvent {
    /// Create a new `DataEvent` with a generated UUID and current timestamp.
    pub fn new(event_type: EventType, key: String, database: u8) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            timestamp: SystemTime::now(),
            event_type,
            key,
            database,
            old_value: None,
            new_value: None,
            metadata: HashMap::new(),
            ttl: None,
        }
    }
}

// ---------------------------------------------------------------------------
// BackpressureStrategy / BackpressureConfig
// ---------------------------------------------------------------------------

/// Strategy applied when a subscriber's queue is full.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum BackpressureStrategy {
    DropOldest,
    DropNewest,
    Block,
    ErrorOnFull,
}

impl Default for BackpressureStrategy {
    fn default() -> Self {
        Self::DropNewest
    }
}

/// Configuration for backpressure behaviour.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BackpressureConfig {
    /// Maximum number of queued events before backpressure kicks in.
    pub max_queue_size: usize,
    /// Strategy to apply when the queue is full.
    pub strategy: BackpressureStrategy,
    /// How long to wait for a slow subscriber before dropping.
    pub slow_subscriber_timeout: Duration,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            max_queue_size: 10_000,
            strategy: BackpressureStrategy::default(),
            slow_subscriber_timeout: Duration::from_secs(5),
        }
    }
}

// ---------------------------------------------------------------------------
// EventBusConfig
// ---------------------------------------------------------------------------

/// Top-level configuration for the [`EventBus`].
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EventBusConfig {
    /// Whether the event bus is enabled.
    pub enabled: bool,
    /// Maximum number of concurrent subscribers.
    pub max_subscribers: usize,
    /// Backpressure settings.
    pub backpressure: BackpressureConfig,
    /// Default channel buffer size per subscriber.
    pub event_buffer_size: usize,
}

impl Default for EventBusConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_subscribers: 1_000,
            backpressure: BackpressureConfig::default(),
            event_buffer_size: 10_000,
        }
    }
}

// ---------------------------------------------------------------------------
// EventFilter
// ---------------------------------------------------------------------------

/// Describes which events a subscriber is interested in.
#[derive(Debug, Clone, Default)]
pub struct EventFilter {
    /// If set, only events with one of these types are delivered.
    pub event_types: Option<Vec<EventType>>,
    /// If set, only events whose key matches one of these glob patterns.
    pub key_patterns: Option<Vec<String>>,
    /// If set, only events from one of these databases.
    pub databases: Option<Vec<u8>>,
}

impl EventFilter {
    /// Returns `true` if the given `event` matches this filter.
    pub fn matches(&self, event: &DataEvent) -> bool {
        if let Some(ref types) = self.event_types {
            if !types.contains(&event.event_type) {
                return false;
            }
        }
        if let Some(ref patterns) = self.key_patterns {
            if !patterns.iter().any(|p| glob_match(p, &event.key)) {
                return false;
            }
        }
        if let Some(ref dbs) = self.databases {
            if !dbs.contains(&event.database) {
                return false;
            }
        }
        true
    }
}

/// Simple glob matching supporting `*` (any chars) and `?` (single char).
fn glob_match(pattern: &str, value: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let val: Vec<char> = value.chars().collect();
    glob_match_inner(&pat, &val)
}

fn glob_match_inner(pat: &[char], val: &[char]) -> bool {
    match (pat.first(), val.first()) {
        (None, None) => true,
        (Some('*'), _) => {
            // '*' matches zero or more characters
            glob_match_inner(&pat[1..], val)
                || (!val.is_empty() && glob_match_inner(pat, &val[1..]))
        }
        (Some('?'), Some(_)) => glob_match_inner(&pat[1..], &val[1..]),
        (Some(a), Some(b)) if a == b => glob_match_inner(&pat[1..], &val[1..]),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// EventHandler
// ---------------------------------------------------------------------------

/// Describes how an event should be delivered to a subscriber.
pub enum EventHandler {
    /// Send the event into a tokio mpsc channel.
    Channel(tokio::sync::mpsc::Sender<DataEvent>),
    /// Invoke a callback function.
    Callback(Arc<dyn Fn(DataEvent) + Send + Sync>),
    /// Fan-out to multiple handlers in order.
    Chain(Vec<EventHandler>),
}

impl std::fmt::Debug for EventHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Channel(_) => write!(f, "Channel(<sender>)"),
            Self::Callback(_) => write!(f, "Callback(<fn>)"),
            Self::Chain(v) => write!(f, "Chain(len={})", v.len()),
        }
    }
}

// ---------------------------------------------------------------------------
// SubscriptionId / EventSubscription
// ---------------------------------------------------------------------------

/// Opaque handle returned by [`EventBus::subscribe`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionId(pub u64);

/// Internal representation of a subscription.
struct EventSubscription {
    id: SubscriptionId,
    filter: EventFilter,
    handler: EventHandler,
}

// ---------------------------------------------------------------------------
// EventBusStats
// ---------------------------------------------------------------------------

/// Runtime statistics for the event bus.
#[derive(Debug, Serialize, Clone)]
pub struct EventBusStats {
    pub total_events: u64,
    pub events_per_sec: f64,
    pub active_subscribers: usize,
    pub dropped_events: u64,
    pub backpressure_events: u64,
}

// ---------------------------------------------------------------------------
// EventBus
// ---------------------------------------------------------------------------

/// Central event routing hub that connects streaming subsystems.
pub struct EventBus {
    subscribers: RwLock<Vec<EventSubscription>>,
    event_count: AtomicU64,
    dropped_count: AtomicU64,
    next_id: AtomicU64,
    // Reserved for backpressure policy enforcement
    #[allow(dead_code)]
    backpressure_config: BackpressureConfig,
    // Retained for runtime reconfiguration support
    #[allow(dead_code)]
    config: EventBusConfig,
}

impl EventBus {
    /// Create a new `EventBus` with the given configuration.
    pub fn new(config: EventBusConfig) -> Self {
        Self {
            subscribers: RwLock::new(Vec::new()),
            event_count: AtomicU64::new(0),
            dropped_count: AtomicU64::new(0),
            next_id: AtomicU64::new(1),
            backpressure_config: config.backpressure.clone(),
            config,
        }
    }

    /// Publish an event to all matching subscribers.
    pub fn publish(&self, event: DataEvent) {
        self.event_count.fetch_add(1, Ordering::Relaxed);

        let subs = self.subscribers.read();
        for sub in subs.iter() {
            if sub.filter.matches(&event) {
                Self::deliver(&sub.handler, &event, &self.dropped_count);
            }
        }
    }

    /// Register a new subscriber. Returns a [`SubscriptionId`] that can be
    /// used later with [`unsubscribe`](Self::unsubscribe).
    pub fn subscribe(&self, filter: EventFilter, handler: EventHandler) -> SubscriptionId {
        let id = SubscriptionId(self.next_id.fetch_add(1, Ordering::Relaxed));
        let sub = EventSubscription {
            id,
            filter,
            handler,
        };
        self.subscribers.write().push(sub);
        id
    }

    /// Remove a previously registered subscription.
    pub fn unsubscribe(&self, id: SubscriptionId) {
        self.subscribers.write().retain(|s| s.id != id);
    }

    /// Return current runtime statistics.
    pub fn stats(&self) -> EventBusStats {
        let subs = self.subscribers.read();
        EventBusStats {
            total_events: self.event_count.load(Ordering::Relaxed),
            events_per_sec: 0.0, // requires a time window to compute
            active_subscribers: subs.len(),
            dropped_events: self.dropped_count.load(Ordering::Relaxed),
            backpressure_events: 0,
        }
    }

    /// Attempt to deliver an event to a single handler.
    fn deliver(handler: &EventHandler, event: &DataEvent, dropped: &AtomicU64) {
        match handler {
            EventHandler::Channel(tx) => {
                if tx.try_send(event.clone()).is_err() {
                    dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
            EventHandler::Callback(f) => {
                f(event.clone());
            }
            EventHandler::Chain(handlers) => {
                for h in handlers {
                    Self::deliver(h, event, dropped);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// FunctionChain
// ---------------------------------------------------------------------------

/// Error handling strategy for [`FunctionChain`] processing.
#[derive(Debug, Clone)]
pub enum ErrorHandling {
    /// Skip the failing event.
    Skip,
    /// Retry up to `max` times.
    Retry { max: u32 },
    /// Send the failing event to a dead-letter queue.
    DeadLetterQueue,
}

impl Default for ErrorHandling {
    fn default() -> Self {
        Self::Skip
    }
}

/// A single step inside a [`FunctionChain`].
pub enum ChainStep {
    /// Drop events that don't match the filter.
    Filter(EventFilter),
    /// Transform (or drop) an event.
    Transform(Arc<dyn Fn(DataEvent) -> Option<DataEvent> + Send + Sync>),
    /// Deliver the event to a handler.
    Sink(EventHandler),
}

impl std::fmt::Debug for ChainStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Filter(ef) => write!(f, "Filter({ef:?})"),
            Self::Transform(_) => write!(f, "Transform(<fn>)"),
            Self::Sink(h) => write!(f, "Sink({h:?})"),
        }
    }
}

/// A composable chain of filter → transform → sink processing steps.
pub struct FunctionChain {
    steps: Vec<ChainStep>,
    // Reserved for chain-level error handling
    #[allow(dead_code)]
    error_handler: ErrorHandling,
}

impl FunctionChain {
    /// Create an empty chain with the default error handling strategy.
    pub fn new() -> Self {
        Self {
            steps: Vec::new(),
            error_handler: ErrorHandling::default(),
        }
    }

    /// Append a filter step (builder pattern).
    pub fn filter(mut self, filter: EventFilter) -> Self {
        self.steps.push(ChainStep::Filter(filter));
        self
    }

    /// Append a transform step (builder pattern).
    pub fn transform(
        mut self,
        f: impl Fn(DataEvent) -> Option<DataEvent> + Send + Sync + 'static,
    ) -> Self {
        self.steps.push(ChainStep::Transform(Arc::new(f)));
        self
    }

    /// Append a sink step (builder pattern).
    pub fn sink(mut self, handler: EventHandler) -> Self {
        self.steps.push(ChainStep::Sink(handler));
        self
    }

    /// Set the error handling strategy (builder pattern).
    pub fn with_error_handler(mut self, handler: ErrorHandling) -> Self {
        self.error_handler = handler;
        self
    }

    /// Run `event` through every step in order. Returns `None` if the event
    /// was filtered out or consumed by a sink.
    pub fn process(&self, event: DataEvent) -> Option<DataEvent> {
        let mut current = Some(event);

        for step in &self.steps {
            let ev = current.take()?;

            match step {
                ChainStep::Filter(filter) => {
                    if filter.matches(&ev) {
                        current = Some(ev);
                    }
                }
                ChainStep::Transform(f) => {
                    current = f(ev);
                }
                ChainStep::Sink(handler) => {
                    let dropped = AtomicU64::new(0);
                    EventBus::deliver(handler, &ev, &dropped);
                    current = Some(ev);
                }
            }
        }

        current
    }
}

impl Default for FunctionChain {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    #[test]
    fn test_publish_and_subscribe() {
        let bus = EventBus::new(EventBusConfig::default());
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);

        let filter = EventFilter {
            event_types: Some(vec![EventType::KeySet]),
            ..Default::default()
        };
        let _id = bus.subscribe(filter, EventHandler::Channel(tx));

        let event = DataEvent::new(EventType::KeySet, "foo".into(), 0);
        bus.publish(event.clone());

        let received = rx.try_recv().expect("should receive event");
        assert_eq!(received.key, "foo");

        let stats = bus.stats();
        assert_eq!(stats.total_events, 1);
        assert_eq!(stats.active_subscribers, 1);
    }

    #[test]
    fn test_unsubscribe() {
        let bus = EventBus::new(EventBusConfig::default());
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);

        let id = bus.subscribe(EventFilter::default(), EventHandler::Channel(tx));
        bus.unsubscribe(id);

        bus.publish(DataEvent::new(EventType::KeySet, "bar".into(), 0));
        assert!(rx.try_recv().is_err());
        assert_eq!(bus.stats().active_subscribers, 0);
    }

    #[test]
    fn test_filter_event_types() {
        let filter = EventFilter {
            event_types: Some(vec![EventType::KeyDelete]),
            ..Default::default()
        };

        let set_event = DataEvent::new(EventType::KeySet, "k".into(), 0);
        let del_event = DataEvent::new(EventType::KeyDelete, "k".into(), 0);

        assert!(!filter.matches(&set_event));
        assert!(filter.matches(&del_event));
    }

    #[test]
    fn test_filter_key_patterns() {
        let filter = EventFilter {
            key_patterns: Some(vec!["user:*".into()]),
            ..Default::default()
        };

        let match_event = DataEvent::new(EventType::KeySet, "user:123".into(), 0);
        let no_match = DataEvent::new(EventType::KeySet, "order:1".into(), 0);

        assert!(filter.matches(&match_event));
        assert!(!filter.matches(&no_match));
    }

    #[test]
    fn test_filter_databases() {
        let filter = EventFilter {
            databases: Some(vec![0, 1]),
            ..Default::default()
        };

        let db0 = DataEvent::new(EventType::KeySet, "k".into(), 0);
        let db5 = DataEvent::new(EventType::KeySet, "k".into(), 5);

        assert!(filter.matches(&db0));
        assert!(!filter.matches(&db5));
    }

    #[test]
    fn test_filter_combined() {
        let filter = EventFilter {
            event_types: Some(vec![EventType::KeySet]),
            key_patterns: Some(vec!["cache:*".into()]),
            databases: Some(vec![0]),
        };

        let good = DataEvent::new(EventType::KeySet, "cache:abc".into(), 0);
        let wrong_type = DataEvent::new(EventType::KeyDelete, "cache:abc".into(), 0);
        let wrong_key = DataEvent::new(EventType::KeySet, "session:abc".into(), 0);
        let wrong_db = DataEvent::new(EventType::KeySet, "cache:abc".into(), 2);

        assert!(filter.matches(&good));
        assert!(!filter.matches(&wrong_type));
        assert!(!filter.matches(&wrong_key));
        assert!(!filter.matches(&wrong_db));
    }

    #[test]
    fn test_function_chain_filter() {
        let chain = FunctionChain::new().filter(EventFilter {
            event_types: Some(vec![EventType::KeySet]),
            ..Default::default()
        });

        let pass = DataEvent::new(EventType::KeySet, "a".into(), 0);
        let fail = DataEvent::new(EventType::KeyDelete, "a".into(), 0);

        assert!(chain.process(pass).is_some());
        assert!(chain.process(fail).is_none());
    }

    #[test]
    fn test_function_chain_transform() {
        let chain = FunctionChain::new().transform(|mut ev| {
            ev.metadata.insert("processed".into(), "true".into());
            Some(ev)
        });

        let event = DataEvent::new(EventType::KeySet, "x".into(), 0);
        let result = chain.process(event).expect("should pass through");
        assert_eq!(result.metadata.get("processed"), Some(&"true".to_string()));
    }

    #[test]
    fn test_function_chain_sink() {
        let counter = Arc::new(AtomicU64::new(0));
        let c = counter.clone();

        let chain = FunctionChain::new().sink(EventHandler::Callback(Arc::new(move |_ev| {
            c.fetch_add(1, Ordering::Relaxed);
        })));

        let event = DataEvent::new(EventType::StreamAppend, "s".into(), 0);
        chain.process(event);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_function_chain_full_pipeline() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);

        let chain = FunctionChain::new()
            .filter(EventFilter {
                event_types: Some(vec![EventType::KeySet]),
                ..Default::default()
            })
            .transform(|mut ev| {
                ev.metadata.insert("enriched".into(), "yes".into());
                Some(ev)
            })
            .sink(EventHandler::Channel(tx));

        let event = DataEvent::new(EventType::KeySet, "pipeline_key".into(), 0);
        let out = chain.process(event).expect("should pass through");
        assert_eq!(out.metadata.get("enriched"), Some(&"yes".to_string()));

        let received = rx.try_recv().expect("sink should have sent event");
        assert_eq!(received.key, "pipeline_key");
    }

    #[test]
    fn test_backpressure_drop() {
        let bus = EventBus::new(EventBusConfig::default());
        // Channel with capacity 1 to force drops
        let (tx, _rx) = tokio::sync::mpsc::channel(1);

        bus.subscribe(EventFilter::default(), EventHandler::Channel(tx));

        // Publish more events than the channel can hold
        for i in 0..10 {
            bus.publish(DataEvent::new(EventType::KeySet, format!("key:{i}"), 0));
        }

        assert!(bus.stats().dropped_events > 0);
    }

    #[test]
    fn test_glob_matching() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("user:*", "user:123"));
        assert!(!glob_match("user:*", "order:1"));
        assert!(glob_match("?oo", "foo"));
        assert!(!glob_match("?oo", "fooo"));
        assert!(glob_match("a*b", "aXYZb"));
        assert!(glob_match("cache:*:count", "cache:page:count"));
    }

    #[test]
    fn test_callback_handler() {
        let bus = EventBus::new(EventBusConfig::default());
        let seen = Arc::new(AtomicU64::new(0));
        let s = seen.clone();

        bus.subscribe(
            EventFilter::default(),
            EventHandler::Callback(Arc::new(move |_| {
                s.fetch_add(1, Ordering::Relaxed);
            })),
        );

        bus.publish(DataEvent::new(EventType::KeyExpire, "ttl".into(), 0));
        assert_eq!(seen.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_chain_handler() {
        let c1 = Arc::new(AtomicU64::new(0));
        let c2 = Arc::new(AtomicU64::new(0));
        let c1c = c1.clone();
        let c2c = c2.clone();

        let handler = EventHandler::Chain(vec![
            EventHandler::Callback(Arc::new(move |_| {
                c1c.fetch_add(1, Ordering::Relaxed);
            })),
            EventHandler::Callback(Arc::new(move |_| {
                c2c.fetch_add(1, Ordering::Relaxed);
            })),
        ]);

        let bus = EventBus::new(EventBusConfig::default());
        bus.subscribe(EventFilter::default(), handler);
        bus.publish(DataEvent::new(EventType::SetAdd, "s".into(), 0));

        assert_eq!(c1.load(Ordering::Relaxed), 1);
        assert_eq!(c2.load(Ordering::Relaxed), 1);
    }
}
