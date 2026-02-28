//! Change Tracking Layer for Materialized View Incremental Updates
//!
//! Provides a high-throughput change tracking system that captures data mutations
//! and feeds them to materialized views for incremental refresh. Changes are
//! recorded in a bounded ring buffer, matched against subscriber glob patterns,
//! and delivered via async channels.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use super::Value;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the [`ChangeTracker`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangeTrackerConfig {
    /// Maximum number of events retained in the ring buffer.
    pub max_buffer_size: usize,
    /// Number of events to process per batch.
    pub batch_size: usize,
    /// Interval in milliseconds between automatic flushes.
    pub flush_interval_ms: u64,
}

impl Default for ChangeTrackerConfig {
    fn default() -> Self {
        Self {
            max_buffer_size: 100_000,
            batch_size: 1000,
            flush_interval_ms: 100,
        }
    }
}

// ---------------------------------------------------------------------------
// Change events
// ---------------------------------------------------------------------------

/// The type of mutation that occurred on a key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeOperation {
    /// A new key was created.
    Insert,
    /// An existing key was modified.
    Update,
    /// A key was removed.
    Delete,
}

/// A single data-change event.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangeEvent {
    /// The key that changed.
    pub key: String,
    /// The type of change.
    pub operation: ChangeOperation,
    /// Previous value (None for inserts).
    pub old_value: Option<Vec<u8>>,
    /// New value (None for deletes).
    pub new_value: Option<Vec<u8>>,
    /// Unix timestamp in milliseconds.
    pub timestamp: u64,
    /// Database number.
    pub db: u8,
    /// Monotonically increasing sequence number assigned by the tracker.
    #[serde(default)]
    pub sequence: u64,
}

// ---------------------------------------------------------------------------
// Subscriptions
// ---------------------------------------------------------------------------

/// A subscription that receives change events matching a glob pattern.
pub struct ChangeSubscription {
    /// Unique subscription identifier.
    pub id: String,
    /// Name of the materialized view this subscription feeds.
    pub view_name: String,
    /// Glob pattern for matching keys.
    pub pattern: String,
    /// Async channel sender for delivering matched events.
    pub sender: mpsc::Sender<ChangeEvent>,
}

// ---------------------------------------------------------------------------
// Glob matching
// ---------------------------------------------------------------------------

/// Matches a string against a simple glob pattern.
///
/// Supports `*` (matches zero or more characters) and `?` (matches exactly
/// one character). All other characters are matched literally.
fn glob_match(pattern: &str, text: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let txt: Vec<char> = text.chars().collect();
    glob_match_inner(&pat, &txt)
}

fn glob_match_inner(pat: &[char], txt: &[char]) -> bool {
    if pat.is_empty() {
        return txt.is_empty();
    }
    match pat[0] {
        '*' => {
            // Try consuming 0..=remaining chars from txt
            for i in 0..=txt.len() {
                if glob_match_inner(&pat[1..], &txt[i..]) {
                    return true;
                }
            }
            false
        }
        '?' => {
            if txt.is_empty() {
                false
            } else {
                glob_match_inner(&pat[1..], &txt[1..])
            }
        }
        c => {
            if txt.is_empty() || txt[0] != c {
                false
            } else {
                glob_match_inner(&pat[1..], &txt[1..])
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ChangeTracker
// ---------------------------------------------------------------------------

/// Central change tracker that records data mutations and broadcasts them
/// to registered materialized-view subscribers.
pub struct ChangeTracker {
    /// Configuration.
    config: ChangeTrackerConfig,
    /// Ring buffer of recent change events.
    buffer: RwLock<VecDeque<ChangeEvent>>,
    /// Monotonically increasing sequence counter.
    next_sequence: AtomicU64,
    /// Active subscriptions keyed by subscription id.
    subscriptions: RwLock<HashMap<String, Arc<ChangeSubscription>>>,
}

impl ChangeTracker {
    /// Creates a new [`ChangeTracker`] with the given configuration.
    pub fn new(config: ChangeTrackerConfig) -> Self {
        Self {
            config,
            buffer: RwLock::new(VecDeque::new()),
            next_sequence: AtomicU64::new(1),
            subscriptions: RwLock::new(HashMap::new()),
        }
    }

    /// Registers a new subscription for the given view and key pattern.
    ///
    /// Returns a [`ChangeSubscription`] and its matching receiver channel.
    pub fn subscribe(
        &self,
        view_name: &str,
        pattern: &str,
    ) -> (Arc<ChangeSubscription>, mpsc::Receiver<ChangeEvent>) {
        let (tx, rx) = mpsc::channel(self.config.batch_size.max(64));
        let sub = Arc::new(ChangeSubscription {
            id: format!("sub-{}-{}", view_name, self.next_sequence.load(Ordering::Relaxed)),
            view_name: view_name.to_string(),
            pattern: pattern.to_string(),
            sender: tx,
        });
        self.subscriptions
            .write()
            .insert(sub.id.clone(), Arc::clone(&sub));
        (sub, rx)
    }

    /// Removes a subscription by its id.
    pub fn unsubscribe(&self, id: &str) {
        self.subscriptions.write().remove(id);
    }

    /// Records a change event, assigns a sequence number, stores it in the
    /// ring buffer, and broadcasts it to all matching subscribers.
    pub fn record_change(&self, mut event: ChangeEvent) {
        let seq = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        event.sequence = seq;

        // Store in ring buffer
        {
            let mut buf = self.buffer.write();
            if buf.len() >= self.config.max_buffer_size {
                buf.pop_front();
            }
            buf.push_back(event.clone());
        }

        // Broadcast to matching subscribers
        let subs = self.subscriptions.read();
        for sub in subs.values() {
            if glob_match(&sub.pattern, &event.key) {
                // Best-effort send; drop if subscriber is slow
                let _ = sub.sender.try_send(event.clone());
            }
        }
    }

    /// Returns all events with a sequence number greater than or equal to
    /// the given value, useful for catch-up after a reconnect.
    pub fn get_changes_since(&self, sequence: u64) -> Vec<ChangeEvent> {
        let buf = self.buffer.read();
        buf.iter()
            .filter(|e| e.sequence >= sequence)
            .cloned()
            .collect()
    }

    /// Returns the current sequence number (next to be assigned).
    pub fn current_sequence(&self) -> u64 {
        self.next_sequence.load(Ordering::Relaxed)
    }

    /// Returns the number of events currently in the ring buffer.
    pub fn buffer_len(&self) -> usize {
        self.buffer.read().len()
    }
}

// ---------------------------------------------------------------------------
// Aggregate operations
// ---------------------------------------------------------------------------

/// Supported aggregate operations for incremental computation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggregateOperation {
    /// Count of values.
    Count,
    /// Sum of numeric values.
    Sum,
    /// Average of numeric values (sum / count).
    Avg,
    /// Minimum value.
    Min,
    /// Maximum value.
    Max,
}

/// Internal state for a single aggregate column.
#[derive(Clone, Debug)]
struct AggregateState {
    op: AggregateOperation,
    count: i64,
    sum: f64,
    /// Sorted multiset for Min/Max recalculation on deletes.
    sorted_values: BTreeSet<OrderedF64>,
    /// Counts for duplicate float values in the BTreeSet.
    value_counts: HashMap<OrderedF64, usize>,
}

/// An `f64` wrapper that implements `Ord` for use in `BTreeSet`.
#[derive(Clone, Debug)]
struct OrderedF64(f64);

impl PartialEq for OrderedF64 {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0) == std::cmp::Ordering::Equal
    }
}

impl Eq for OrderedF64 {}

impl PartialOrd for OrderedF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl std::hash::Hash for OrderedF64 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

impl AggregateState {
    fn new(op: AggregateOperation) -> Self {
        Self {
            op,
            count: 0,
            sum: 0.0,
            sorted_values: BTreeSet::new(),
            value_counts: HashMap::new(),
        }
    }

    fn add_value(&mut self, v: f64) {
        self.count += 1;
        self.sum += v;
        let key = OrderedF64(v);
        *self.value_counts.entry(key.clone()).or_insert(0) += 1;
        self.sorted_values.insert(key);
    }

    fn remove_value(&mut self, v: f64) {
        self.count -= 1;
        self.sum -= v;
        let key = OrderedF64(v);
        if let Some(cnt) = self.value_counts.get_mut(&key) {
            *cnt -= 1;
            if *cnt == 0 {
                self.value_counts.remove(&key);
                self.sorted_values.remove(&key);
            }
        }
    }

    fn current(&self) -> Value {
        match self.op {
            AggregateOperation::Count => Value::Int(self.count),
            AggregateOperation::Sum => Value::Float(self.sum),
            AggregateOperation::Avg => {
                if self.count == 0 {
                    Value::Null
                } else {
                    Value::Float(self.sum / self.count as f64)
                }
            }
            AggregateOperation::Min => self
                .sorted_values
                .iter()
                .next()
                .map(|v| Value::Float(v.0))
                .unwrap_or(Value::Null),
            AggregateOperation::Max => self
                .sorted_values
                .iter()
                .next_back()
                .map(|v| Value::Float(v.0))
                .unwrap_or(Value::Null),
        }
    }
}

/// Maintains running aggregation state that can be incrementally updated
/// as rows are inserted, updated, or deleted.
pub struct IncrementalAggregator {
    /// Per-column aggregate state, keyed by `"column:operation"`.
    states: HashMap<String, AggregateState>,
}

fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        _ => None,
    }
}

impl IncrementalAggregator {
    /// Creates a new empty aggregator.
    pub fn new() -> Self {
        Self {
            states: HashMap::new(),
        }
    }

    /// Registers a column and aggregate operation to track.
    pub fn add_aggregate(&mut self, column: &str, op: AggregateOperation) {
        let key = format!("{}:{:?}", column, op);
        self.states.entry(key).or_insert_with(|| AggregateState::new(op));
    }

    /// Applies an insert by incorporating `value` into all aggregates
    /// registered for the given column.
    pub fn apply_insert(&mut self, column: &str, value: &Value) {
        let numeric = value_to_f64(value);
        for (key, state) in self.states.iter_mut() {
            if key.starts_with(&format!("{}:", column)) {
                if let Some(v) = numeric {
                    state.add_value(v);
                } else if state.op == AggregateOperation::Count {
                    state.count += 1;
                }
            }
        }
    }

    /// Reverses an insert (i.e., applies a delete) by removing `value`
    /// from all aggregates registered for the given column.
    pub fn apply_delete(&mut self, column: &str, value: &Value) {
        let numeric = value_to_f64(value);
        for (key, state) in self.states.iter_mut() {
            if key.starts_with(&format!("{}:", column)) {
                if let Some(v) = numeric {
                    state.remove_value(v);
                } else if state.op == AggregateOperation::Count {
                    state.count -= 1;
                }
            }
        }
    }

    /// Adjusts aggregation state for an in-place update (old → new).
    pub fn apply_update(&mut self, column: &str, old: &Value, new: &Value) {
        self.apply_delete(column, old);
        self.apply_insert(column, new);
    }

    /// Returns the current aggregate results keyed by `"column:Operation"`.
    pub fn current_result(&self) -> HashMap<String, Value> {
        self.states
            .iter()
            .map(|(k, s)| (k.clone(), s.current()))
            .collect()
    }
}

impl Default for IncrementalAggregator {
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

    fn make_event(key: &str, op: ChangeOperation) -> ChangeEvent {
        ChangeEvent {
            key: key.to_string(),
            operation: op,
            old_value: None,
            new_value: Some(vec![1, 2, 3]),
            timestamp: 1_000_000,
            db: 0,
            sequence: 0,
        }
    }

    // -- ChangeTracker tests ------------------------------------------------

    #[test]
    fn test_record_and_retrieve_changes() {
        let tracker = ChangeTracker::new(ChangeTrackerConfig::default());
        tracker.record_change(make_event("users:1", ChangeOperation::Insert));
        tracker.record_change(make_event("users:2", ChangeOperation::Update));

        let changes = tracker.get_changes_since(1);
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].key, "users:1");
        assert_eq!(changes[1].key, "users:2");
    }

    #[test]
    fn test_glob_pattern_matching() {
        assert!(glob_match("users:*", "users:42"));
        assert!(glob_match("users:*", "users:"));
        assert!(!glob_match("users:*", "orders:1"));
        assert!(glob_match("user?:1", "users:1"));
        assert!(!glob_match("user?:1", "userss:1"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("*:*", "a:b"));
        assert!(!glob_match("a:*", "b:x"));
    }

    #[tokio::test]
    async fn test_subscription_delivery() {
        let tracker = ChangeTracker::new(ChangeTrackerConfig::default());
        let (_sub, mut rx) = tracker.subscribe("active_users", "users:*");

        tracker.record_change(make_event("users:1", ChangeOperation::Insert));
        tracker.record_change(make_event("orders:1", ChangeOperation::Insert));

        let received = rx.recv().await.unwrap();
        assert_eq!(received.key, "users:1");

        // orders:1 should not be delivered
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_sequence_numbering() {
        let tracker = ChangeTracker::new(ChangeTrackerConfig::default());
        tracker.record_change(make_event("k1", ChangeOperation::Insert));
        tracker.record_change(make_event("k2", ChangeOperation::Insert));
        tracker.record_change(make_event("k3", ChangeOperation::Insert));

        let changes = tracker.get_changes_since(1);
        assert_eq!(changes[0].sequence, 1);
        assert_eq!(changes[1].sequence, 2);
        assert_eq!(changes[2].sequence, 3);
    }

    #[test]
    fn test_buffer_overflow_eviction() {
        let config = ChangeTrackerConfig {
            max_buffer_size: 3,
            ..Default::default()
        };
        let tracker = ChangeTracker::new(config);

        for i in 0..5 {
            tracker.record_change(make_event(&format!("k{}", i), ChangeOperation::Insert));
        }

        assert_eq!(tracker.buffer_len(), 3);
        let changes = tracker.get_changes_since(1);
        // Only the last 3 events should remain (sequences 3, 4, 5)
        assert_eq!(changes[0].sequence, 3);
        assert_eq!(changes[1].sequence, 4);
        assert_eq!(changes[2].sequence, 5);
    }

    // -- IncrementalAggregator tests ----------------------------------------

    #[test]
    fn test_aggregator_count_sum_avg() {
        let mut agg = IncrementalAggregator::new();
        agg.add_aggregate("price", AggregateOperation::Count);
        agg.add_aggregate("price", AggregateOperation::Sum);
        agg.add_aggregate("price", AggregateOperation::Avg);

        agg.apply_insert("price", &Value::Float(10.0));
        agg.apply_insert("price", &Value::Float(20.0));
        agg.apply_insert("price", &Value::Float(30.0));

        let result = agg.current_result();
        assert_eq!(result["price:Count"], Value::Int(3));
        assert_eq!(result["price:Sum"], Value::Float(60.0));
        assert_eq!(result["price:Avg"], Value::Float(20.0));

        // Delete one value and verify recalculation
        agg.apply_delete("price", &Value::Float(10.0));
        let result = agg.current_result();
        assert_eq!(result["price:Count"], Value::Int(2));
        assert_eq!(result["price:Sum"], Value::Float(50.0));
        assert_eq!(result["price:Avg"], Value::Float(25.0));
    }

    #[test]
    fn test_aggregator_update() {
        let mut agg = IncrementalAggregator::new();
        agg.add_aggregate("qty", AggregateOperation::Sum);

        agg.apply_insert("qty", &Value::Int(5));
        agg.apply_update("qty", &Value::Int(5), &Value::Int(8));

        let result = agg.current_result();
        assert_eq!(result["qty:Sum"], Value::Float(8.0));
    }

    #[test]
    fn test_aggregator_min_max_with_deletes() {
        let mut agg = IncrementalAggregator::new();
        agg.add_aggregate("score", AggregateOperation::Min);
        agg.add_aggregate("score", AggregateOperation::Max);

        agg.apply_insert("score", &Value::Int(5));
        agg.apply_insert("score", &Value::Int(1));
        agg.apply_insert("score", &Value::Int(10));

        let result = agg.current_result();
        assert_eq!(result["score:Min"], Value::Float(1.0));
        assert_eq!(result["score:Max"], Value::Float(10.0));

        // Delete the min value — min should update to 5
        agg.apply_delete("score", &Value::Int(1));
        let result = agg.current_result();
        assert_eq!(result["score:Min"], Value::Float(5.0));
        assert_eq!(result["score:Max"], Value::Float(10.0));

        // Delete the max value — max should update to 5
        agg.apply_delete("score", &Value::Int(10));
        let result = agg.current_result();
        assert_eq!(result["score:Min"], Value::Float(5.0));
        assert_eq!(result["score:Max"], Value::Float(5.0));

        // Delete last value — should return Null
        agg.apply_delete("score", &Value::Int(5));
        let result = agg.current_result();
        assert_eq!(result["score:Min"], Value::Null);
        assert_eq!(result["score:Max"], Value::Null);
    }
}
