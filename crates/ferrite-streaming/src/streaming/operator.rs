//! Stream operators for transforming events

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::{StreamError, StreamEvent};

/// Base trait for stream operators
#[async_trait::async_trait]
pub trait Operator<K, V>: Send + Sync {
    /// Process an event
    async fn process(&self, event: StreamEvent) -> Result<Vec<StreamEvent>, StreamError>;

    /// Get operator name
    fn name(&self) -> &str;

    /// Get operator type
    fn operator_type(&self) -> OperatorType;
}

/// Operator types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorType {
    /// Filter events
    Filter,
    /// Map events
    Map,
    /// Flat map events
    FlatMap,
    /// Aggregate events
    Aggregate,
    /// Join streams
    Join,
    /// Window events
    Window,
    /// Group by key
    GroupBy,
    /// Reduce events
    Reduce,
    /// Branch events
    Branch,
    /// Merge streams
    Merge,
    /// Peek (for debugging)
    Peek,
}

/// Filter operator
pub struct FilterOperator<F>
where
    F: Fn(&StreamEvent) -> bool + Send + Sync,
{
    name: String,
    predicate: F,
}

impl<F> FilterOperator<F>
where
    F: Fn(&StreamEvent) -> bool + Send + Sync,
{
    /// Create a new filter operator
    pub fn new(predicate: F) -> Self {
        Self {
            name: "filter".to_string(),
            predicate,
        }
    }

    /// Create with custom name
    pub fn with_name(name: &str, predicate: F) -> Self {
        Self {
            name: name.to_string(),
            predicate,
        }
    }
}

#[async_trait::async_trait]
impl<F> Operator<String, serde_json::Value> for FilterOperator<F>
where
    F: Fn(&StreamEvent) -> bool + Send + Sync,
{
    async fn process(&self, event: StreamEvent) -> Result<Vec<StreamEvent>, StreamError> {
        if (self.predicate)(&event) {
            Ok(vec![event])
        } else {
            Ok(vec![])
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::Filter
    }
}

/// Map operator
pub struct MapOperator<F>
where
    F: Fn(StreamEvent) -> StreamEvent + Send + Sync,
{
    name: String,
    mapper: F,
}

impl<F> MapOperator<F>
where
    F: Fn(StreamEvent) -> StreamEvent + Send + Sync,
{
    /// Create a new map operator
    pub fn new(mapper: F) -> Self {
        Self {
            name: "map".to_string(),
            mapper,
        }
    }

    /// Create with custom name
    pub fn with_name(name: &str, mapper: F) -> Self {
        Self {
            name: name.to_string(),
            mapper,
        }
    }
}

#[async_trait::async_trait]
impl<F> Operator<String, serde_json::Value> for MapOperator<F>
where
    F: Fn(StreamEvent) -> StreamEvent + Send + Sync,
{
    async fn process(&self, event: StreamEvent) -> Result<Vec<StreamEvent>, StreamError> {
        Ok(vec![(self.mapper)(event)])
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::Map
    }
}

/// Flat map operator
pub struct FlatMapOperator<F>
where
    F: Fn(StreamEvent) -> Vec<StreamEvent> + Send + Sync,
{
    name: String,
    mapper: F,
}

impl<F> FlatMapOperator<F>
where
    F: Fn(StreamEvent) -> Vec<StreamEvent> + Send + Sync,
{
    /// Create a new flat map operator
    pub fn new(mapper: F) -> Self {
        Self {
            name: "flat_map".to_string(),
            mapper,
        }
    }

    /// Create with custom name
    pub fn with_name(name: &str, mapper: F) -> Self {
        Self {
            name: name.to_string(),
            mapper,
        }
    }
}

#[async_trait::async_trait]
impl<F> Operator<String, serde_json::Value> for FlatMapOperator<F>
where
    F: Fn(StreamEvent) -> Vec<StreamEvent> + Send + Sync,
{
    async fn process(&self, event: StreamEvent) -> Result<Vec<StreamEvent>, StreamError> {
        Ok((self.mapper)(event))
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::FlatMap
    }
}

/// Aggregate operator
pub struct AggregateOperator<S, A, O>
where
    S: Default + Clone + Send + Sync,
    A: Fn(&S, &StreamEvent) -> S + Send + Sync,
    O: Fn(&str, &S) -> StreamEvent + Send + Sync,
{
    name: String,
    state: Arc<RwLock<HashMap<String, S>>>,
    aggregator: A,
    output_fn: O,
    _marker: std::marker::PhantomData<S>,
}

impl<S, A, O> AggregateOperator<S, A, O>
where
    S: Default + Clone + Send + Sync,
    A: Fn(&S, &StreamEvent) -> S + Send + Sync,
    O: Fn(&str, &S) -> StreamEvent + Send + Sync,
{
    /// Create a new aggregate operator
    pub fn new(aggregator: A, output_fn: O) -> Self {
        Self {
            name: "aggregate".to_string(),
            state: Arc::new(RwLock::new(HashMap::new())),
            aggregator,
            output_fn,
            _marker: std::marker::PhantomData,
        }
    }

    /// Get aggregated value for a key
    pub async fn get(&self, key: &str) -> Option<S> {
        let state = self.state.read().await;
        state.get(key).cloned()
    }
}

#[async_trait::async_trait]
impl<S, A, O> Operator<String, serde_json::Value> for AggregateOperator<S, A, O>
where
    S: Default + Clone + Send + Sync + 'static,
    A: Fn(&S, &StreamEvent) -> S + Send + Sync + 'static,
    O: Fn(&str, &S) -> StreamEvent + Send + Sync + 'static,
{
    async fn process(&self, event: StreamEvent) -> Result<Vec<StreamEvent>, StreamError> {
        let key = event.key.clone().unwrap_or_else(|| "default".to_string());

        let mut state = self.state.write().await;
        let current = state.get(&key).cloned().unwrap_or_default();
        let new_state = (self.aggregator)(&current, &event);
        state.insert(key.clone(), new_state.clone());

        let output = (self.output_fn)(&key, &new_state);
        Ok(vec![output])
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::Aggregate
    }
}

/// Branch operator for splitting streams
pub struct BranchOperator {
    name: String,
    #[allow(clippy::type_complexity)]
    predicates: Vec<Box<dyn Fn(&StreamEvent) -> bool + Send + Sync>>,
}

impl BranchOperator {
    /// Create a new branch operator
    pub fn new() -> Self {
        Self {
            name: "branch".to_string(),
            predicates: Vec::new(),
        }
    }

    /// Add a branch
    pub fn branch<F>(mut self, predicate: F) -> Self
    where
        F: Fn(&StreamEvent) -> bool + Send + Sync + 'static,
    {
        self.predicates.push(Box::new(predicate));
        self
    }
}

impl Default for BranchOperator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Operator<String, serde_json::Value> for BranchOperator {
    async fn process(&self, event: StreamEvent) -> Result<Vec<StreamEvent>, StreamError> {
        // Find the first matching branch
        for (i, predicate) in self.predicates.iter().enumerate() {
            if predicate(&event) {
                let mut branched = event.clone();
                branched.headers.insert("branch".to_string(), i.to_string());
                return Ok(vec![branched]);
            }
        }

        // No branch matched - emit to default (last) branch
        let mut branched = event;
        branched
            .headers
            .insert("branch".to_string(), "default".to_string());
        Ok(vec![branched])
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::Branch
    }
}

/// Peek operator for debugging
pub struct PeekOperator<F>
where
    F: Fn(&StreamEvent) + Send + Sync,
{
    name: String,
    callback: F,
}

impl<F> PeekOperator<F>
where
    F: Fn(&StreamEvent) + Send + Sync,
{
    /// Create a new peek operator
    pub fn new(callback: F) -> Self {
        Self {
            name: "peek".to_string(),
            callback,
        }
    }
}

#[async_trait::async_trait]
impl<F> Operator<String, serde_json::Value> for PeekOperator<F>
where
    F: Fn(&StreamEvent) + Send + Sync,
{
    async fn process(&self, event: StreamEvent) -> Result<Vec<StreamEvent>, StreamError> {
        (self.callback)(&event);
        Ok(vec![event])
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::Peek
    }
}

/// Group by key operator
pub struct GroupByOperator<K, F>
where
    K: Clone + Send + Sync,
    F: Fn(&StreamEvent) -> K + Send + Sync,
{
    name: String,
    key_extractor: F,
    _marker: std::marker::PhantomData<K>,
}

impl<K, F> GroupByOperator<K, F>
where
    K: Clone + Send + Sync + ToString,
    F: Fn(&StreamEvent) -> K + Send + Sync,
{
    /// Create a new group by operator
    pub fn new(key_extractor: F) -> Self {
        Self {
            name: "group_by".to_string(),
            key_extractor,
            _marker: std::marker::PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<K, F> Operator<String, serde_json::Value> for GroupByOperator<K, F>
where
    K: Clone + Send + Sync + ToString + 'static,
    F: Fn(&StreamEvent) -> K + Send + Sync + 'static,
{
    async fn process(&self, mut event: StreamEvent) -> Result<Vec<StreamEvent>, StreamError> {
        let key = (self.key_extractor)(&event);
        event.key = Some(key.to_string());
        Ok(vec![event])
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::GroupBy
    }
}

/// Reduce operator
pub struct ReduceOperator<F>
where
    F: Fn(&StreamEvent, &StreamEvent) -> StreamEvent + Send + Sync,
{
    name: String,
    reducer: F,
    state: Arc<RwLock<HashMap<String, StreamEvent>>>,
}

impl<F> ReduceOperator<F>
where
    F: Fn(&StreamEvent, &StreamEvent) -> StreamEvent + Send + Sync,
{
    /// Create a new reduce operator
    pub fn new(reducer: F) -> Self {
        Self {
            name: "reduce".to_string(),
            reducer,
            state: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait::async_trait]
impl<F> Operator<String, serde_json::Value> for ReduceOperator<F>
where
    F: Fn(&StreamEvent, &StreamEvent) -> StreamEvent + Send + Sync + 'static,
{
    async fn process(&self, event: StreamEvent) -> Result<Vec<StreamEvent>, StreamError> {
        let key = event.key.clone().unwrap_or_else(|| "default".to_string());

        let mut state = self.state.write().await;

        let result = if let Some(current) = state.get(&key) {
            (self.reducer)(current, &event)
        } else {
            event.clone()
        };

        state.insert(key, result.clone());
        Ok(vec![result])
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::Reduce
    }
}

/// Join operator for stream-stream joins
pub struct JoinOperator {
    name: String,
    join_type: JoinType,
    left_key: String,
    right_key: String,
    window_ms: u64,
    left_buffer: Arc<RwLock<Vec<StreamEvent>>>,
    right_buffer: Arc<RwLock<Vec<StreamEvent>>>,
}

/// Join types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// Inner join
    Inner,
    /// Left outer join
    LeftOuter,
    /// Right outer join
    RightOuter,
    /// Full outer join
    FullOuter,
}

impl JoinOperator {
    /// Create a new join operator
    pub fn new(left_key: &str, right_key: &str, join_type: JoinType, window_ms: u64) -> Self {
        Self {
            name: "join".to_string(),
            join_type,
            left_key: left_key.to_string(),
            right_key: right_key.to_string(),
            window_ms,
            left_buffer: Arc::new(RwLock::new(Vec::new())),
            right_buffer: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Add event to left stream
    pub async fn add_left(&self, event: StreamEvent) -> Vec<StreamEvent> {
        let mut left = self.left_buffer.write().await;
        left.push(event.clone());

        // Clean old events
        let cutoff = event.timestamp.saturating_sub(self.window_ms);
        left.retain(|e| e.timestamp >= cutoff);

        // Try to join with right
        let right = self.right_buffer.read().await;
        self.join_events(&event, &right, true)
    }

    /// Add event to right stream
    pub async fn add_right(&self, event: StreamEvent) -> Vec<StreamEvent> {
        let mut right = self.right_buffer.write().await;
        right.push(event.clone());

        // Clean old events
        let cutoff = event.timestamp.saturating_sub(self.window_ms);
        right.retain(|e| e.timestamp >= cutoff);

        // Try to join with left
        let left = self.left_buffer.read().await;
        self.join_events(&event, &left, false)
    }

    fn join_events(
        &self,
        event: &StreamEvent,
        buffer: &[StreamEvent],
        is_left: bool,
    ) -> Vec<StreamEvent> {
        let mut results = Vec::new();

        let key_field = if is_left {
            &self.left_key
        } else {
            &self.right_key
        };
        let other_key_field = if is_left {
            &self.right_key
        } else {
            &self.left_key
        };

        let key_value = event.value.get(key_field);

        for other in buffer {
            let other_key_value = other.value.get(other_key_field);

            if key_value == other_key_value {
                // Create joined event
                let mut joined_value = serde_json::Map::new();

                // Add left values
                if let serde_json::Value::Object(obj) =
                    if is_left { &event.value } else { &other.value }
                {
                    for (k, v) in obj {
                        joined_value.insert(format!("left_{}", k), v.clone());
                    }
                }

                // Add right values
                if let serde_json::Value::Object(obj) =
                    if is_left { &other.value } else { &event.value }
                {
                    for (k, v) in obj {
                        joined_value.insert(format!("right_{}", k), v.clone());
                    }
                }

                let joined =
                    StreamEvent::new(event.key.clone(), serde_json::Value::Object(joined_value));
                results.push(joined);
            }
        }

        results
    }
}

#[async_trait::async_trait]
impl Operator<String, serde_json::Value> for JoinOperator {
    async fn process(&self, event: StreamEvent) -> Result<Vec<StreamEvent>, StreamError> {
        // Determine which stream this event belongs to based on header
        let is_left = event
            .headers
            .get("stream")
            .map(|s| s == "left")
            .unwrap_or(true);

        let results = if is_left {
            self.add_left(event).await
        } else {
            self.add_right(event).await
        };

        Ok(results)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::Join
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_filter_operator() {
        let filter = FilterOperator::new(|e| e.get_i64("count").unwrap_or(0) > 10);

        let event1 = StreamEvent::new(None, json!({ "count": 15 }));
        let event2 = StreamEvent::new(None, json!({ "count": 5 }));

        let result1 = filter.process(event1).await.unwrap();
        let result2 = filter.process(event2).await.unwrap();

        assert_eq!(result1.len(), 1);
        assert_eq!(result2.len(), 0);
    }

    #[tokio::test]
    async fn test_map_operator() {
        let map = MapOperator::new(|mut e| {
            if let Some(count) = e.value.get("count").and_then(|v| v.as_i64()) {
                e.value = json!({ "count": count * 2 });
            }
            e
        });

        let event = StreamEvent::new(None, json!({ "count": 10 }));
        let result = map.process(event).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value.get("count").unwrap().as_i64(), Some(20));
    }

    #[tokio::test]
    async fn test_flat_map_operator() {
        let flat_map = FlatMapOperator::new(|e| {
            if let Some(items) = e.value.get("items").and_then(|v| v.as_array()) {
                items
                    .iter()
                    .map(|item| StreamEvent::new(None, item.clone()))
                    .collect()
            } else {
                vec![]
            }
        });

        let event = StreamEvent::new(None, json!({ "items": [1, 2, 3] }));
        let result = flat_map.process(event).await.unwrap();

        assert_eq!(result.len(), 3);
    }

    #[tokio::test]
    async fn test_aggregate_operator() {
        let aggregate = AggregateOperator::new(
            |state: &i64, event: &StreamEvent| {
                state
                    + event
                        .value
                        .get("count")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0)
            },
            |key, state| StreamEvent::new(Some(key.to_string()), json!({ "total": state })),
        );

        let event1 = StreamEvent::new(Some("key1".to_string()), json!({ "count": 5 }));
        let event2 = StreamEvent::new(Some("key1".to_string()), json!({ "count": 3 }));

        aggregate.process(event1).await.unwrap();
        let result = aggregate.process(event2).await.unwrap();

        assert_eq!(result[0].value.get("total").unwrap().as_i64(), Some(8));
    }
}
