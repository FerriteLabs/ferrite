//! AMQP protocol adapter
//!
//! Maps AMQP queue semantics to Ferrite Lists and Streams,
//! enabling RabbitMQ-compatible message consumption.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// AMQP adapter configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmqpConfig {
    /// Whether the AMQP adapter is enabled.
    pub enable: bool,
    /// TCP port to listen on.
    pub port: u16,
    /// Maximum number of channels per connection.
    pub max_channels: u32,
    /// Maximum frame size in bytes.
    pub max_frame_size: u32,
    /// Heartbeat interval.
    pub heartbeat_interval: Duration,
}

impl Default for AmqpConfig {
    fn default() -> Self {
        Self {
            enable: false,
            port: 5672,
            max_channels: 256,
            max_frame_size: 131072,
            heartbeat_interval: Duration::from_secs(60),
        }
    }
}

// ---------------------------------------------------------------------------
// Exchange and queue types
// ---------------------------------------------------------------------------

/// Type of AMQP exchange.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExchangeType {
    /// Direct routing (exact match on routing key).
    Direct,
    /// Fanout to all bound queues.
    Fanout,
    /// Topic-based routing with wildcards.
    Topic,
    /// Header-based routing.
    Headers,
}

impl std::fmt::Display for ExchangeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Direct => write!(f, "direct"),
            Self::Fanout => write!(f, "fanout"),
            Self::Topic => write!(f, "topic"),
            Self::Headers => write!(f, "headers"),
        }
    }
}

/// A binding between a queue and an exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Binding {
    /// Name of the bound queue.
    pub queue: String,
    /// Routing key for the binding.
    pub routing_key: String,
}

/// An AMQP exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmqpExchange {
    /// Exchange name.
    pub name: String,
    /// Exchange type.
    pub exchange_type: ExchangeType,
    /// Whether the exchange survives broker restart.
    pub durable: bool,
    /// Queue bindings.
    pub bindings: Vec<Binding>,
}

/// An AMQP queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmqpQueue {
    /// Queue name.
    pub name: String,
    /// Whether the queue survives broker restart.
    pub durable: bool,
    /// Whether the queue is exclusive to the declaring connection.
    pub exclusive: bool,
    /// Whether the queue is auto-deleted when last consumer disconnects.
    pub auto_delete: bool,
    /// Number of messages currently in the queue.
    pub messages: u64,
    /// Number of active consumers.
    pub consumers: u32,
}

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

/// Properties attached to an AMQP message.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MessageProperties {
    /// MIME content type.
    pub content_type: Option<String>,
    /// Delivery mode (1 = non-persistent, 2 = persistent).
    pub delivery_mode: u8,
    /// Message priority (0-9).
    pub priority: u8,
    /// Correlation ID for RPC-style patterns.
    pub correlation_id: Option<String>,
    /// Reply-to queue for RPC-style patterns.
    pub reply_to: Option<String>,
    /// Per-message TTL.
    pub expiration: Option<String>,
    /// Application-generated message ID.
    pub message_id: Option<String>,
    /// Message timestamp (UNIX epoch seconds).
    pub timestamp: Option<u64>,
}

/// A delivered AMQP message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmqpMessage {
    /// Unique delivery tag for ack/nack.
    pub delivery_tag: u64,
    /// Exchange the message was published to.
    pub exchange: String,
    /// Routing key used for delivery.
    pub routing_key: String,
    /// Message body.
    pub body: Vec<u8>,
    /// Message properties.
    pub properties: MessageProperties,
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

/// AMQP adapter statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmqpStats {
    /// Number of declared exchanges.
    pub exchanges: u32,
    /// Number of declared queues.
    pub queues: u32,
    /// Total messages published.
    pub messages_published: u64,
    /// Total messages delivered to consumers.
    pub messages_delivered: u64,
    /// Total messages acknowledged.
    pub messages_acked: u64,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// AMQP adapter errors.
#[derive(Debug, thiserror::Error)]
pub enum AmqpError {
    /// Referenced exchange does not exist.
    #[error("exchange not found: {0}")]
    ExchangeNotFound(String),

    /// Referenced queue does not exist.
    #[error("queue not found: {0}")]
    QueueNotFound(String),

    /// The channel has been closed.
    #[error("channel closed")]
    ChannelClosed,

    /// The frame exceeds the maximum allowed size.
    #[error("frame too large: {size} bytes (max {max})")]
    FrameTooLarge { size: u32, max: u32 },
}

// ---------------------------------------------------------------------------
// Adapter
// ---------------------------------------------------------------------------

/// The AMQP protocol adapter.
pub struct AmqpAdapter {
    config: AmqpConfig,
    exchanges: RwLock<HashMap<String, AmqpExchange>>,
    queues: RwLock<HashMap<String, AmqpQueue>>,
    messages: RwLock<HashMap<String, Vec<AmqpMessage>>>,
    next_delivery_tag: AtomicU64,
    messages_published: AtomicU64,
    messages_delivered: AtomicU64,
    messages_acked: AtomicU64,
}

impl AmqpAdapter {
    /// Create a new AMQP adapter with the given configuration.
    pub fn new(config: AmqpConfig) -> Self {
        Self {
            config,
            exchanges: RwLock::new(HashMap::new()),
            queues: RwLock::new(HashMap::new()),
            messages: RwLock::new(HashMap::new()),
            next_delivery_tag: AtomicU64::new(1),
            messages_published: AtomicU64::new(0),
            messages_delivered: AtomicU64::new(0),
            messages_acked: AtomicU64::new(0),
        }
    }

    /// Declare an exchange.
    pub fn declare_exchange(&self, exchange: AmqpExchange) -> Result<(), AmqpError> {
        self.exchanges
            .write()
            .insert(exchange.name.clone(), exchange);
        Ok(())
    }

    /// Declare a queue.
    pub fn declare_queue(&self, queue: AmqpQueue) -> Result<(), AmqpError> {
        let name = queue.name.clone();
        self.queues.write().insert(name.clone(), queue);
        // Initialize message buffer for this queue
        self.messages.write().entry(name).or_insert_with(Vec::new);
        Ok(())
    }

    /// Bind a queue to an exchange with a routing key.
    pub fn bind_queue(
        &self,
        queue: &str,
        exchange: &str,
        routing_key: &str,
    ) -> Result<(), AmqpError> {
        // Verify queue exists
        if !self.queues.read().contains_key(queue) {
            return Err(AmqpError::QueueNotFound(queue.to_string()));
        }

        let mut exchanges = self.exchanges.write();
        let ex = exchanges
            .get_mut(exchange)
            .ok_or_else(|| AmqpError::ExchangeNotFound(exchange.to_string()))?;

        ex.bindings.push(Binding {
            queue: queue.to_string(),
            routing_key: routing_key.to_string(),
        });

        Ok(())
    }

    /// Publish a message to an exchange.
    pub fn publish(
        &self,
        exchange: &str,
        routing_key: &str,
        body: &[u8],
        properties: MessageProperties,
    ) -> Result<(), AmqpError> {
        let exchanges = self.exchanges.read();
        let ex = exchanges
            .get(exchange)
            .ok_or_else(|| AmqpError::ExchangeNotFound(exchange.to_string()))?;

        // Find matching queues based on exchange type and routing key
        let target_queues: Vec<String> = match ex.exchange_type {
            ExchangeType::Fanout => ex.bindings.iter().map(|b| b.queue.clone()).collect(),
            ExchangeType::Direct => ex
                .bindings
                .iter()
                .filter(|b| b.routing_key == routing_key)
                .map(|b| b.queue.clone())
                .collect(),
            ExchangeType::Topic => ex
                .bindings
                .iter()
                .filter(|b| topic_matches(&b.routing_key, routing_key))
                .map(|b| b.queue.clone())
                .collect(),
            ExchangeType::Headers => ex.bindings.iter().map(|b| b.queue.clone()).collect(),
        };

        // Drop the exchanges read lock before acquiring messages write lock
        drop(exchanges);

        let mut messages = self.messages.write();
        for queue_name in &target_queues {
            let tag = self.next_delivery_tag.fetch_add(1, Ordering::Relaxed);
            let msg = AmqpMessage {
                delivery_tag: tag,
                exchange: exchange.to_string(),
                routing_key: routing_key.to_string(),
                body: body.to_vec(),
                properties: properties.clone(),
            };
            messages.entry(queue_name.clone()).or_default().push(msg);
        }

        self.messages_published.fetch_add(1, Ordering::Relaxed);

        // Update queue message counts
        let mut queues = self.queues.write();
        for queue_name in &target_queues {
            if let Some(q) = queues.get_mut(queue_name) {
                q.messages = messages
                    .get(queue_name)
                    .map(|v| v.len() as u64)
                    .unwrap_or(0);
            }
        }

        Ok(())
    }

    /// Consume the next message from a queue.
    pub fn consume(&self, queue: &str) -> Result<Option<AmqpMessage>, AmqpError> {
        if !self.queues.read().contains_key(queue) {
            return Err(AmqpError::QueueNotFound(queue.to_string()));
        }

        let mut messages = self.messages.write();
        let msg = messages.get_mut(queue).and_then(|msgs| {
            if msgs.is_empty() {
                None
            } else {
                Some(msgs.remove(0))
            }
        });

        if msg.is_some() {
            self.messages_delivered.fetch_add(1, Ordering::Relaxed);
            // Update queue message count
            if let Some(q) = self.queues.write().get_mut(queue) {
                q.messages = messages.get(queue).map(|v| v.len() as u64).unwrap_or(0);
            }
        }

        Ok(msg)
    }

    /// Acknowledge a delivered message.
    pub fn ack(&self, _delivery_tag: u64) -> Result<(), AmqpError> {
        self.messages_acked.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// List all declared exchanges.
    pub fn list_exchanges(&self) -> Vec<AmqpExchange> {
        self.exchanges.read().values().cloned().collect()
    }

    /// List all declared queues.
    pub fn list_queues(&self) -> Vec<AmqpQueue> {
        self.queues.read().values().cloned().collect()
    }

    /// Get adapter statistics.
    pub fn stats(&self) -> AmqpStats {
        AmqpStats {
            exchanges: self.exchanges.read().len() as u32,
            queues: self.queues.read().len() as u32,
            messages_published: self.messages_published.load(Ordering::Relaxed),
            messages_delivered: self.messages_delivered.load(Ordering::Relaxed),
            messages_acked: self.messages_acked.load(Ordering::Relaxed),
        }
    }
}

/// Simple AMQP topic matching (supports `*` and `#` wildcards).
fn topic_matches(pattern: &str, routing_key: &str) -> bool {
    let pattern_parts: Vec<&str> = pattern.split('.').collect();
    let key_parts: Vec<&str> = routing_key.split('.').collect();

    let mut pi = 0;
    let mut ki = 0;

    while pi < pattern_parts.len() && ki < key_parts.len() {
        match pattern_parts[pi] {
            "#" => return true, // matches everything remaining
            "*" => {
                pi += 1;
                ki += 1;
            }
            word => {
                if word != key_parts[ki] {
                    return false;
                }
                pi += 1;
                ki += 1;
            }
        }
    }

    pi == pattern_parts.len() && ki == key_parts.len()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_adapter() -> AmqpAdapter {
        AmqpAdapter::new(AmqpConfig::default())
    }

    #[test]
    fn test_declare_exchange() {
        let adapter = make_adapter();
        let ex = AmqpExchange {
            name: "test_ex".to_string(),
            exchange_type: ExchangeType::Direct,
            durable: false,
            bindings: vec![],
        };
        adapter.declare_exchange(ex).expect("should succeed");
        assert_eq!(adapter.list_exchanges().len(), 1);
    }

    #[test]
    fn test_declare_queue() {
        let adapter = make_adapter();
        let q = AmqpQueue {
            name: "test_q".to_string(),
            durable: false,
            exclusive: false,
            auto_delete: false,
            messages: 0,
            consumers: 0,
        };
        adapter.declare_queue(q).expect("should succeed");
        assert_eq!(adapter.list_queues().len(), 1);
    }

    #[test]
    fn test_bind_queue() {
        let adapter = make_adapter();
        adapter
            .declare_exchange(AmqpExchange {
                name: "ex1".to_string(),
                exchange_type: ExchangeType::Direct,
                durable: false,
                bindings: vec![],
            })
            .expect("declare exchange");
        adapter
            .declare_queue(AmqpQueue {
                name: "q1".to_string(),
                durable: false,
                exclusive: false,
                auto_delete: false,
                messages: 0,
                consumers: 0,
            })
            .expect("declare queue");

        adapter.bind_queue("q1", "ex1", "key1").expect("bind");

        let exchanges = adapter.list_exchanges();
        let ex = exchanges.iter().find(|e| e.name == "ex1").expect("find");
        assert_eq!(ex.bindings.len(), 1);
    }

    #[test]
    fn test_publish_and_consume() {
        let adapter = make_adapter();
        adapter
            .declare_exchange(AmqpExchange {
                name: "ex".to_string(),
                exchange_type: ExchangeType::Direct,
                durable: false,
                bindings: vec![],
            })
            .expect("declare exchange");
        adapter
            .declare_queue(AmqpQueue {
                name: "q".to_string(),
                durable: false,
                exclusive: false,
                auto_delete: false,
                messages: 0,
                consumers: 0,
            })
            .expect("declare queue");
        adapter.bind_queue("q", "ex", "rk").expect("bind");

        adapter
            .publish("ex", "rk", b"hello", MessageProperties::default())
            .expect("publish");

        let msg = adapter.consume("q").expect("consume").expect("has message");
        assert_eq!(msg.body, b"hello");
        assert_eq!(msg.routing_key, "rk");
    }

    #[test]
    fn test_consume_empty_queue() {
        let adapter = make_adapter();
        adapter
            .declare_queue(AmqpQueue {
                name: "empty_q".to_string(),
                durable: false,
                exclusive: false,
                auto_delete: false,
                messages: 0,
                consumers: 0,
            })
            .expect("declare queue");

        let msg = adapter.consume("empty_q").expect("consume");
        assert!(msg.is_none());
    }

    #[test]
    fn test_consume_nonexistent_queue() {
        let adapter = make_adapter();
        let result = adapter.consume("no_such_queue");
        assert!(matches!(result, Err(AmqpError::QueueNotFound(_))));
    }

    #[test]
    fn test_ack() {
        let adapter = make_adapter();
        adapter.ack(1).expect("ack should succeed");
        let stats = adapter.stats();
        assert_eq!(stats.messages_acked, 1);
    }

    #[test]
    fn test_fanout_exchange() {
        let adapter = make_adapter();
        adapter
            .declare_exchange(AmqpExchange {
                name: "fanout_ex".to_string(),
                exchange_type: ExchangeType::Fanout,
                durable: false,
                bindings: vec![],
            })
            .expect("declare");
        adapter
            .declare_queue(AmqpQueue {
                name: "q1".to_string(),
                durable: false,
                exclusive: false,
                auto_delete: false,
                messages: 0,
                consumers: 0,
            })
            .expect("declare q1");
        adapter
            .declare_queue(AmqpQueue {
                name: "q2".to_string(),
                durable: false,
                exclusive: false,
                auto_delete: false,
                messages: 0,
                consumers: 0,
            })
            .expect("declare q2");
        adapter.bind_queue("q1", "fanout_ex", "").expect("bind q1");
        adapter.bind_queue("q2", "fanout_ex", "").expect("bind q2");

        adapter
            .publish("fanout_ex", "", b"broadcast", MessageProperties::default())
            .expect("publish");

        let m1 = adapter.consume("q1").expect("consume q1");
        let m2 = adapter.consume("q2").expect("consume q2");
        assert!(m1.is_some());
        assert!(m2.is_some());
    }

    #[test]
    fn test_stats() {
        let adapter = make_adapter();
        let stats = adapter.stats();
        assert_eq!(stats.exchanges, 0);
        assert_eq!(stats.queues, 0);
        assert_eq!(stats.messages_published, 0);
    }

    #[test]
    fn test_topic_matching() {
        assert!(topic_matches("a.b.c", "a.b.c"));
        assert!(topic_matches("a.*.c", "a.b.c"));
        assert!(topic_matches("a.#", "a.b.c"));
        assert!(!topic_matches("a.b", "a.b.c"));
        assert!(!topic_matches("x.y.z", "a.b.c"));
    }

    #[test]
    fn test_exchange_type_display() {
        assert_eq!(ExchangeType::Direct.to_string(), "direct");
        assert_eq!(ExchangeType::Fanout.to_string(), "fanout");
        assert_eq!(ExchangeType::Topic.to_string(), "topic");
        assert_eq!(ExchangeType::Headers.to_string(), "headers");
    }
}
