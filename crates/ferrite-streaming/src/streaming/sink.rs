//! Stream sinks for outputting processed data

use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

use super::{StreamError, StreamEvent};

/// Base trait for stream sinks
#[async_trait::async_trait]
pub trait Sink: Send + Sync {
    /// Start the sink
    async fn start(&self) -> Result<(), StreamError>;

    /// Stop the sink
    async fn stop(&self) -> Result<(), StreamError>;

    /// Write an event
    async fn write(&self, event: StreamEvent) -> Result<(), StreamError>;

    /// Write multiple events
    async fn write_batch(&self, events: Vec<StreamEvent>) -> Result<(), StreamError> {
        for event in events {
            self.write(event).await?;
        }
        Ok(())
    }

    /// Flush pending writes
    async fn flush(&self) -> Result<(), StreamError>;

    /// Get sink name
    fn name(&self) -> &str;

    /// Get sink type
    fn sink_type(&self) -> SinkType;
}

/// Sink types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkType {
    /// Redis
    Redis,
    /// Kafka
    Kafka,
    /// File
    File,
    /// HTTP/Webhook
    Http,
    /// Memory (in-process)
    Memory,
    /// Console (stdout)
    Console,
    /// Null (discard)
    Null,
    /// Custom
    Custom(String),
}

/// Redis sink for writing to Ferrite streams
///
/// This sink buffers events and provides methods to retrieve them for
/// integration with Ferrite's XADD command.
pub struct RedisSink {
    name: String,
    stream_key: String,
    max_len: Option<usize>,
    running: Arc<RwLock<bool>>,
    /// Pending events buffer (not yet written to stream)
    buffer: Arc<RwLock<Vec<StreamEvent>>>,
    /// Written events (available for retrieval by XADD integration)
    written: Arc<RwLock<Vec<StreamEvent>>>,
    /// Batch size for automatic flushing
    batch_size: usize,
    /// Total events written
    events_written: Arc<RwLock<u64>>,
}

impl RedisSink {
    /// Create a new Redis sink
    pub fn new(stream_key: &str) -> Self {
        Self {
            name: format!("redis-{}", stream_key),
            stream_key: stream_key.to_string(),
            max_len: None,
            running: Arc::new(RwLock::new(false)),
            buffer: Arc::new(RwLock::new(Vec::new())),
            written: Arc::new(RwLock::new(Vec::new())),
            batch_size: 100,
            events_written: Arc::new(RwLock::new(0)),
        }
    }

    /// Set max stream length (for MAXLEN trimming)
    pub fn max_len(mut self, max: usize) -> Self {
        self.max_len = Some(max);
        self
    }

    /// Set batch size for auto-flushing
    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Get stream key
    pub fn stream_key(&self) -> &str {
        &self.stream_key
    }

    /// Get max length setting
    pub fn get_max_len(&self) -> Option<usize> {
        self.max_len
    }

    /// Get total events written
    pub async fn events_written(&self) -> u64 {
        *self.events_written.read().await
    }

    /// Drain written events (for XADD integration)
    /// This returns and clears events that have been flushed
    pub async fn drain_written(&self) -> Vec<StreamEvent> {
        let mut written = self.written.write().await;
        std::mem::take(&mut *written)
    }

    /// Peek at written events without draining
    pub async fn peek_written(&self) -> Vec<StreamEvent> {
        self.written.read().await.clone()
    }

    /// Get pending buffer size
    pub async fn pending_count(&self) -> usize {
        self.buffer.read().await.len()
    }
}

#[async_trait::async_trait]
impl Sink for RedisSink {
    async fn start(&self) -> Result<(), StreamError> {
        *self.running.write().await = true;
        Ok(())
    }

    async fn stop(&self) -> Result<(), StreamError> {
        self.flush().await?;
        *self.running.write().await = false;
        Ok(())
    }

    async fn write(&self, event: StreamEvent) -> Result<(), StreamError> {
        if !*self.running.read().await {
            return Err(StreamError::SinkError("Sink not running".into()));
        }

        let mut buffer = self.buffer.write().await;
        buffer.push(event);

        // Auto-flush when batch size is reached
        if buffer.len() >= self.batch_size {
            drop(buffer);
            self.flush().await?;
        }

        Ok(())
    }

    async fn flush(&self) -> Result<(), StreamError> {
        let mut buffer = self.buffer.write().await;
        let mut written = self.written.write().await;
        let mut count = self.events_written.write().await;

        // Move buffered events to written
        let event_count = buffer.len();
        written.extend(buffer.drain(..));
        *count += event_count as u64;

        // Apply max_len trimming if configured
        if let Some(max) = self.max_len {
            if written.len() > max {
                let trim_count = written.len() - max;
                written.drain(..trim_count);
            }
        }

        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn sink_type(&self) -> SinkType {
        SinkType::Redis
    }
}

/// Memory sink for testing
pub struct MemorySink {
    name: String,
    events: Arc<RwLock<Vec<StreamEvent>>>,
    running: Arc<RwLock<bool>>,
}

impl MemorySink {
    /// Create a new memory sink
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            events: Arc::new(RwLock::new(Vec::new())),
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Get all events written to the sink
    pub async fn get_events(&self) -> Vec<StreamEvent> {
        self.events.read().await.clone()
    }

    /// Get event count
    pub async fn count(&self) -> usize {
        self.events.read().await.len()
    }

    /// Clear events
    pub async fn clear(&self) {
        self.events.write().await.clear();
    }
}

#[async_trait::async_trait]
impl Sink for MemorySink {
    async fn start(&self) -> Result<(), StreamError> {
        *self.running.write().await = true;
        Ok(())
    }

    async fn stop(&self) -> Result<(), StreamError> {
        *self.running.write().await = false;
        Ok(())
    }

    async fn write(&self, event: StreamEvent) -> Result<(), StreamError> {
        if !*self.running.read().await {
            return Err(StreamError::SinkError("Sink not running".into()));
        }

        let mut events = self.events.write().await;
        events.push(event);
        Ok(())
    }

    async fn flush(&self) -> Result<(), StreamError> {
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn sink_type(&self) -> SinkType {
        SinkType::Memory
    }
}

/// Console sink for debugging
pub struct ConsoleSink {
    name: String,
    format: OutputFormat,
    running: Arc<RwLock<bool>>,
}

/// Output format for console sink
#[derive(Clone)]
pub enum OutputFormat {
    /// JSON format
    Json,
    /// Pretty JSON
    JsonPretty,
    /// Key=Value format
    KeyValue,
    /// Custom format function
    Custom(Arc<dyn Fn(&StreamEvent) -> String + Send + Sync>),
}

impl std::fmt::Debug for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::Json => write!(f, "Json"),
            OutputFormat::JsonPretty => write!(f, "JsonPretty"),
            OutputFormat::KeyValue => write!(f, "KeyValue"),
            OutputFormat::Custom(_) => write!(f, "Custom(<fn>)"),
        }
    }
}

impl ConsoleSink {
    /// Create a new console sink
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            format: OutputFormat::Json,
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Set output format
    pub fn format(mut self, format: OutputFormat) -> Self {
        self.format = format;
        self
    }
}

#[async_trait::async_trait]
impl Sink for ConsoleSink {
    async fn start(&self) -> Result<(), StreamError> {
        *self.running.write().await = true;
        Ok(())
    }

    async fn stop(&self) -> Result<(), StreamError> {
        *self.running.write().await = false;
        Ok(())
    }

    async fn write(&self, event: StreamEvent) -> Result<(), StreamError> {
        if !*self.running.read().await {
            return Err(StreamError::SinkError("Sink not running".into()));
        }

        let output = match &self.format {
            OutputFormat::Json => serde_json::to_string(&event.value).unwrap_or_default(),
            OutputFormat::JsonPretty => {
                serde_json::to_string_pretty(&event.value).unwrap_or_default()
            }
            OutputFormat::KeyValue => {
                format!(
                    "key={} timestamp={} value={}",
                    event.key.as_deref().unwrap_or("null"),
                    event.timestamp,
                    serde_json::to_string(&event.value).unwrap_or_default()
                )
            }
            OutputFormat::Custom(formatter) => formatter(&event),
        };

        println!("[{}] {}", self.name, output);
        Ok(())
    }

    async fn flush(&self) -> Result<(), StreamError> {
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn sink_type(&self) -> SinkType {
        SinkType::Console
    }
}

/// Null sink (discards all events)
pub struct NullSink {
    name: String,
    count: Arc<RwLock<u64>>,
}

impl NullSink {
    /// Create a new null sink
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            count: Arc::new(RwLock::new(0)),
        }
    }

    /// Get number of events discarded
    pub async fn count(&self) -> u64 {
        *self.count.read().await
    }
}

#[async_trait::async_trait]
impl Sink for NullSink {
    async fn start(&self) -> Result<(), StreamError> {
        Ok(())
    }

    async fn stop(&self) -> Result<(), StreamError> {
        Ok(())
    }

    async fn write(&self, _event: StreamEvent) -> Result<(), StreamError> {
        let mut count = self.count.write().await;
        *count += 1;
        Ok(())
    }

    async fn flush(&self) -> Result<(), StreamError> {
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn sink_type(&self) -> SinkType {
        SinkType::Null
    }
}

/// Channel sink for connecting components
pub struct ChannelSink {
    name: String,
    sender: mpsc::Sender<StreamEvent>,
    running: Arc<RwLock<bool>>,
}

impl ChannelSink {
    /// Create a new channel sink
    pub fn new(name: &str, sender: mpsc::Sender<StreamEvent>) -> Self {
        Self {
            name: name.to_string(),
            sender,
            running: Arc::new(RwLock::new(false)),
        }
    }
}

#[async_trait::async_trait]
impl Sink for ChannelSink {
    async fn start(&self) -> Result<(), StreamError> {
        *self.running.write().await = true;
        Ok(())
    }

    async fn stop(&self) -> Result<(), StreamError> {
        *self.running.write().await = false;
        Ok(())
    }

    async fn write(&self, event: StreamEvent) -> Result<(), StreamError> {
        if !*self.running.read().await {
            return Err(StreamError::SinkError("Sink not running".into()));
        }

        self.sender
            .send(event)
            .await
            .map_err(|_| StreamError::ChannelClosed)
    }

    async fn flush(&self) -> Result<(), StreamError> {
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn sink_type(&self) -> SinkType {
        SinkType::Memory
    }
}

/// Partitioned sink for parallel writing
pub struct PartitionedSink<S: Sink> {
    name: String,
    sinks: Vec<Arc<S>>,
    partition_fn: Arc<dyn Fn(&StreamEvent) -> usize + Send + Sync>,
}

impl<S: Sink + 'static> PartitionedSink<S> {
    /// Create a new partitioned sink
    pub fn new<F>(name: &str, sinks: Vec<S>, partition_fn: F) -> Self
    where
        F: Fn(&StreamEvent) -> usize + Send + Sync + 'static,
    {
        Self {
            name: name.to_string(),
            sinks: sinks.into_iter().map(Arc::new).collect(),
            partition_fn: Arc::new(partition_fn),
        }
    }
}

#[async_trait::async_trait]
impl<S: Sink + 'static> Sink for PartitionedSink<S> {
    async fn start(&self) -> Result<(), StreamError> {
        for sink in &self.sinks {
            sink.start().await?;
        }
        Ok(())
    }

    async fn stop(&self) -> Result<(), StreamError> {
        for sink in &self.sinks {
            sink.stop().await?;
        }
        Ok(())
    }

    async fn write(&self, event: StreamEvent) -> Result<(), StreamError> {
        let partition = (self.partition_fn)(&event) % self.sinks.len();
        self.sinks[partition].write(event).await
    }

    async fn flush(&self) -> Result<(), StreamError> {
        for sink in &self.sinks {
            sink.flush().await?;
        }
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn sink_type(&self) -> SinkType {
        SinkType::Custom("partitioned".to_string())
    }
}

/// Fan-out sink that writes to multiple sinks
pub struct FanOutSink {
    name: String,
    sinks: Vec<Arc<dyn Sink>>,
}

impl FanOutSink {
    /// Create a new fan-out sink
    pub fn new(name: &str, sinks: Vec<Arc<dyn Sink>>) -> Self {
        Self {
            name: name.to_string(),
            sinks,
        }
    }
}

#[async_trait::async_trait]
impl Sink for FanOutSink {
    async fn start(&self) -> Result<(), StreamError> {
        for sink in &self.sinks {
            sink.start().await?;
        }
        Ok(())
    }

    async fn stop(&self) -> Result<(), StreamError> {
        for sink in &self.sinks {
            sink.stop().await?;
        }
        Ok(())
    }

    async fn write(&self, event: StreamEvent) -> Result<(), StreamError> {
        for sink in &self.sinks {
            sink.write(event.clone()).await?;
        }
        Ok(())
    }

    async fn flush(&self) -> Result<(), StreamError> {
        for sink in &self.sinks {
            sink.flush().await?;
        }
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn sink_type(&self) -> SinkType {
        SinkType::Custom("fan-out".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_memory_sink() {
        let sink = MemorySink::new("test");
        sink.start().await.unwrap();

        sink.write(StreamEvent::new(None, json!({ "value": 1 })))
            .await
            .unwrap();
        sink.write(StreamEvent::new(None, json!({ "value": 2 })))
            .await
            .unwrap();

        let events = sink.get_events().await;
        assert_eq!(events.len(), 2);
    }

    #[tokio::test]
    async fn test_null_sink() {
        let sink = NullSink::new("test");

        for i in 0..100 {
            sink.write(StreamEvent::new(None, json!({ "n": i })))
                .await
                .unwrap();
        }

        assert_eq!(sink.count().await, 100);
    }

    #[tokio::test]
    async fn test_channel_sink() {
        let (tx, mut rx) = mpsc::channel(10);
        let sink = ChannelSink::new("test", tx);

        sink.start().await.unwrap();
        sink.write(StreamEvent::new(None, json!({ "value": 1 })))
            .await
            .unwrap();

        let event = rx.recv().await.unwrap();
        assert_eq!(event.value.get("value").unwrap().as_i64(), Some(1));
    }

    #[tokio::test]
    async fn test_fan_out_sink() {
        let sink1 = Arc::new(MemorySink::new("sink1"));
        let sink2 = Arc::new(MemorySink::new("sink2"));

        let fan_out = FanOutSink::new("fan-out", vec![sink1.clone(), sink2.clone()]);
        fan_out.start().await.unwrap();

        fan_out
            .write(StreamEvent::new(None, json!({ "value": 1 })))
            .await
            .unwrap();

        assert_eq!(sink1.count().await, 1);
        assert_eq!(sink2.count().await, 1);
    }
}
