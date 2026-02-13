//! Stream sources for ingesting data

use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

use super::{StreamError, StreamEvent};

/// Base trait for stream sources
#[async_trait::async_trait]
pub trait Source: Send + Sync {
    /// Start the source
    async fn start(&self) -> Result<(), StreamError>;

    /// Stop the source
    async fn stop(&self) -> Result<(), StreamError>;

    /// Get the next event
    async fn poll(&self) -> Option<StreamEvent>;

    /// Get source name
    fn name(&self) -> &str;

    /// Get source type
    fn source_type(&self) -> SourceType;

    /// Commit offset (for exactly-once)
    async fn commit(&self, offset: u64) -> Result<(), StreamError>;

    /// Seek to offset
    async fn seek(&self, offset: u64) -> Result<(), StreamError>;
}

/// Source types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceType {
    /// Redis Streams
    Redis,
    /// Kafka
    Kafka,
    /// File
    File,
    /// HTTP/Webhook
    Http,
    /// Generator (for testing)
    Generator,
    /// Memory (in-process)
    Memory,
    /// Timer
    Timer,
    /// Custom
    Custom(String),
}

/// Redis Streams source
///
/// This source can be connected to Ferrite's internal stream implementation
/// or used with an external message queue via the push_event method.
pub struct RedisSource {
    name: String,
    stream_key: String,
    consumer_group: String,
    consumer_name: String,
    running: Arc<RwLock<bool>>,
    last_id: Arc<RwLock<String>>,
    /// Internal event queue for push-based delivery
    event_queue: Arc<RwLock<std::collections::VecDeque<StreamEvent>>>,
    /// Pending message IDs for acknowledgment tracking
    pending_ids: Arc<RwLock<Vec<String>>>,
    /// Block timeout in milliseconds (0 = non-blocking)
    block_timeout_ms: u64,
}

impl RedisSource {
    /// Create a new Redis source
    pub fn new(stream_key: &str) -> Self {
        Self {
            name: format!("redis-{}", stream_key),
            stream_key: stream_key.to_string(),
            consumer_group: "ferrite-stream".to_string(),
            consumer_name: format!("consumer-{}", std::process::id()),
            running: Arc::new(RwLock::new(false)),
            last_id: Arc::new(RwLock::new("0".to_string())),
            event_queue: Arc::new(RwLock::new(std::collections::VecDeque::new())),
            pending_ids: Arc::new(RwLock::new(Vec::new())),
            block_timeout_ms: 1000,
        }
    }

    /// Set consumer group
    pub fn consumer_group(mut self, group: &str) -> Self {
        self.consumer_group = group.to_string();
        self
    }

    /// Set consumer name
    pub fn consumer_name(mut self, name: &str) -> Self {
        self.consumer_name = name.to_string();
        self
    }

    /// Set block timeout
    pub fn block_timeout(mut self, timeout_ms: u64) -> Self {
        self.block_timeout_ms = timeout_ms;
        self
    }

    /// Push events into the source (for integration with Ferrite's XREADGROUP)
    /// This method is called by the command executor when reading from streams
    pub async fn push_event(&self, event: StreamEvent, message_id: String) {
        let mut queue = self.event_queue.write().await;
        queue.push_back(event);
        let mut pending = self.pending_ids.write().await;
        pending.push(message_id);
    }

    /// Push multiple events
    pub async fn push_events(&self, events: Vec<(StreamEvent, String)>) {
        let mut queue = self.event_queue.write().await;
        let mut pending = self.pending_ids.write().await;
        for (event, id) in events {
            queue.push_back(event);
            pending.push(id);
        }
    }

    /// Get stream key
    pub fn stream_key(&self) -> &str {
        &self.stream_key
    }

    /// Get consumer group
    pub fn get_consumer_group(&self) -> &str {
        &self.consumer_group
    }

    /// Get consumer name
    pub fn get_consumer_name(&self) -> &str {
        &self.consumer_name
    }

    /// Get pending message count
    pub async fn pending_count(&self) -> usize {
        self.pending_ids.read().await.len()
    }
}

#[async_trait::async_trait]
impl Source for RedisSource {
    async fn start(&self) -> Result<(), StreamError> {
        *self.running.write().await = true;
        Ok(())
    }

    async fn stop(&self) -> Result<(), StreamError> {
        *self.running.write().await = false;
        Ok(())
    }

    async fn poll(&self) -> Option<StreamEvent> {
        if !*self.running.read().await {
            return None;
        }

        // Try to get from internal queue
        let mut queue = self.event_queue.write().await;
        if let Some(event) = queue.pop_front() {
            return Some(event);
        }
        drop(queue);

        // If no events in queue, wait briefly then return None
        // In production, this would be integrated with XREADGROUP blocking
        if self.block_timeout_ms > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        None
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn source_type(&self) -> SourceType {
        SourceType::Redis
    }

    async fn commit(&self, offset: u64) -> Result<(), StreamError> {
        // Remove acknowledged messages from pending list
        let mut pending = self.pending_ids.write().await;
        if offset as usize <= pending.len() {
            pending.drain(..offset as usize);
        }
        Ok(())
    }

    async fn seek(&self, _offset: u64) -> Result<(), StreamError> {
        // Redis Streams use message IDs, not numeric offsets
        // For seek, we would need to clear the queue and update last_id
        let mut queue = self.event_queue.write().await;
        queue.clear();
        Ok(())
    }
}

/// Memory source for testing
pub struct MemorySource {
    name: String,
    events: Arc<RwLock<Vec<StreamEvent>>>,
    position: Arc<RwLock<usize>>,
    running: Arc<RwLock<bool>>,
}

impl MemorySource {
    /// Create a new memory source
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            events: Arc::new(RwLock::new(Vec::new())),
            position: Arc::new(RwLock::new(0)),
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Add events to the source
    pub async fn add_events(&self, events: Vec<StreamEvent>) {
        let mut source_events = self.events.write().await;
        source_events.extend(events);
    }

    /// Add a single event
    pub async fn add_event(&self, event: StreamEvent) {
        let mut source_events = self.events.write().await;
        source_events.push(event);
    }
}

#[async_trait::async_trait]
impl Source for MemorySource {
    async fn start(&self) -> Result<(), StreamError> {
        *self.running.write().await = true;
        Ok(())
    }

    async fn stop(&self) -> Result<(), StreamError> {
        *self.running.write().await = false;
        Ok(())
    }

    async fn poll(&self) -> Option<StreamEvent> {
        if !*self.running.read().await {
            return None;
        }

        let events = self.events.read().await;
        let mut pos = self.position.write().await;

        if *pos < events.len() {
            let event = events[*pos].clone();
            *pos += 1;
            Some(event)
        } else {
            None
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn source_type(&self) -> SourceType {
        SourceType::Memory
    }

    async fn commit(&self, _offset: u64) -> Result<(), StreamError> {
        // Memory source doesn't need commits
        Ok(())
    }

    async fn seek(&self, offset: u64) -> Result<(), StreamError> {
        let mut pos = self.position.write().await;
        *pos = offset as usize;
        Ok(())
    }
}

/// Generator source for testing
pub struct GeneratorSource<F>
where
    F: Fn(u64) -> StreamEvent + Send + Sync,
{
    name: String,
    generator: F,
    rate_per_second: f64,
    max_events: Option<u64>,
    counter: Arc<RwLock<u64>>,
    running: Arc<RwLock<bool>>,
}

impl<F> GeneratorSource<F>
where
    F: Fn(u64) -> StreamEvent + Send + Sync,
{
    /// Create a new generator source
    pub fn new(name: &str, generator: F) -> Self {
        Self {
            name: name.to_string(),
            generator,
            rate_per_second: 100.0,
            max_events: None,
            counter: Arc::new(RwLock::new(0)),
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Set generation rate
    pub fn rate(mut self, per_second: f64) -> Self {
        self.rate_per_second = per_second;
        self
    }

    /// Set maximum events
    pub fn max_events(mut self, max: u64) -> Self {
        self.max_events = Some(max);
        self
    }
}

#[async_trait::async_trait]
impl<F> Source for GeneratorSource<F>
where
    F: Fn(u64) -> StreamEvent + Send + Sync,
{
    async fn start(&self) -> Result<(), StreamError> {
        *self.running.write().await = true;
        Ok(())
    }

    async fn stop(&self) -> Result<(), StreamError> {
        *self.running.write().await = false;
        Ok(())
    }

    async fn poll(&self) -> Option<StreamEvent> {
        if !*self.running.read().await {
            return None;
        }

        let mut counter = self.counter.write().await;

        // Check max events
        if let Some(max) = self.max_events {
            if *counter >= max {
                return None;
            }
        }

        // Generate event
        let event = (self.generator)(*counter);
        *counter += 1;

        // Rate limiting delay
        let delay_ms = (1000.0 / self.rate_per_second) as u64;
        if delay_ms > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
        }

        Some(event)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn source_type(&self) -> SourceType {
        SourceType::Generator
    }

    async fn commit(&self, _offset: u64) -> Result<(), StreamError> {
        Ok(())
    }

    async fn seek(&self, offset: u64) -> Result<(), StreamError> {
        let mut counter = self.counter.write().await;
        *counter = offset;
        Ok(())
    }
}

/// Timer source for periodic events
pub struct TimerSource {
    name: String,
    interval_ms: u64,
    running: Arc<RwLock<bool>>,
    counter: Arc<RwLock<u64>>,
}

impl TimerSource {
    /// Create a new timer source
    pub fn new(name: &str, interval: std::time::Duration) -> Self {
        Self {
            name: name.to_string(),
            interval_ms: interval.as_millis() as u64,
            running: Arc::new(RwLock::new(false)),
            counter: Arc::new(RwLock::new(0)),
        }
    }
}

#[async_trait::async_trait]
impl Source for TimerSource {
    async fn start(&self) -> Result<(), StreamError> {
        *self.running.write().await = true;
        Ok(())
    }

    async fn stop(&self) -> Result<(), StreamError> {
        *self.running.write().await = false;
        Ok(())
    }

    async fn poll(&self) -> Option<StreamEvent> {
        if !*self.running.read().await {
            return None;
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(self.interval_ms)).await;

        let mut counter = self.counter.write().await;
        *counter += 1;

        let event = StreamEvent::new(
            None,
            serde_json::json!({
                "tick": *counter,
                "interval_ms": self.interval_ms
            }),
        );

        Some(event)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn source_type(&self) -> SourceType {
        SourceType::Timer
    }

    async fn commit(&self, _offset: u64) -> Result<(), StreamError> {
        Ok(())
    }

    async fn seek(&self, _offset: u64) -> Result<(), StreamError> {
        Ok(())
    }
}

/// Channel-based source for connecting components
pub struct ChannelSource {
    name: String,
    receiver: Arc<RwLock<Option<mpsc::Receiver<StreamEvent>>>>,
    running: Arc<RwLock<bool>>,
}

impl ChannelSource {
    /// Create a new channel source
    pub fn new(name: &str, receiver: mpsc::Receiver<StreamEvent>) -> Self {
        Self {
            name: name.to_string(),
            receiver: Arc::new(RwLock::new(Some(receiver))),
            running: Arc::new(RwLock::new(false)),
        }
    }
}

#[async_trait::async_trait]
impl Source for ChannelSource {
    async fn start(&self) -> Result<(), StreamError> {
        *self.running.write().await = true;
        Ok(())
    }

    async fn stop(&self) -> Result<(), StreamError> {
        *self.running.write().await = false;
        Ok(())
    }

    async fn poll(&self) -> Option<StreamEvent> {
        if !*self.running.read().await {
            return None;
        }

        let mut receiver_guard = self.receiver.write().await;
        if let Some(ref mut rx) = *receiver_guard {
            rx.recv().await
        } else {
            None
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn source_type(&self) -> SourceType {
        SourceType::Memory
    }

    async fn commit(&self, _offset: u64) -> Result<(), StreamError> {
        Ok(())
    }

    async fn seek(&self, _offset: u64) -> Result<(), StreamError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_memory_source() {
        let source = MemorySource::new("test");
        source
            .add_event(StreamEvent::new(None, json!({ "value": 1 })))
            .await;
        source
            .add_event(StreamEvent::new(None, json!({ "value": 2 })))
            .await;

        source.start().await.unwrap();

        let event1 = source.poll().await.unwrap();
        assert_eq!(event1.value.get("value").unwrap().as_i64(), Some(1));

        let event2 = source.poll().await.unwrap();
        assert_eq!(event2.value.get("value").unwrap().as_i64(), Some(2));

        let event3 = source.poll().await;
        assert!(event3.is_none());
    }

    #[tokio::test]
    async fn test_generator_source() {
        let source = GeneratorSource::new("test", |n| StreamEvent::new(None, json!({ "n": n })))
            .max_events(3)
            .rate(1000.0);

        source.start().await.unwrap();

        let mut count = 0;
        while let Some(_) = source.poll().await {
            count += 1;
        }

        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_channel_source() {
        let (tx, rx) = mpsc::channel(10);
        let source = ChannelSource::new("test", rx);

        source.start().await.unwrap();

        tx.send(StreamEvent::new(None, json!({ "value": 1 })))
            .await
            .unwrap();

        let event = source.poll().await.unwrap();
        assert_eq!(event.value.get("value").unwrap().as_i64(), Some(1));
    }
}
