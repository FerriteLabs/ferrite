//! Function Triggers
//!
//! Define triggers that invoke serverless functions based on events.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Trigger type
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TriggerType {
    /// HTTP trigger (REST API)
    Http(HttpTrigger),
    /// Key pattern trigger (data change)
    Event(EventTrigger),
    /// Schedule trigger (cron/interval)
    Schedule(ScheduleTrigger),
    /// Queue trigger
    Queue(QueueTrigger),
    /// Pub/Sub trigger
    PubSub(PubSubTrigger),
}

/// Trigger configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TriggerConfig {
    /// Trigger name
    pub name: String,
    /// Trigger type
    pub trigger_type: TriggerType,
    /// Enabled
    pub enabled: bool,
    /// Batching configuration
    pub batching: Option<BatchConfig>,
    /// Filter expression
    pub filter: Option<String>,
    /// Max retries
    pub max_retries: usize,
}

impl TriggerConfig {
    /// Create an HTTP trigger
    pub fn http(path: &str) -> Self {
        Self {
            name: format!("http_{}", path.replace('/', "_")),
            trigger_type: TriggerType::Http(HttpTrigger {
                path: path.to_string(),
                methods: vec!["GET".to_string(), "POST".to_string()],
                auth: HttpAuth::None,
                cors: None,
                rate_limit: None,
            }),
            enabled: true,
            batching: None,
            filter: None,
            max_retries: 3,
        }
    }

    /// Create a key pattern trigger
    pub fn on_key_change(pattern: &str) -> Self {
        Self {
            name: format!("event_{}", pattern.replace('*', "star")),
            trigger_type: TriggerType::Event(EventTrigger {
                pattern: pattern.to_string(),
                operations: vec![EventOperation::Set, EventOperation::Delete],
            }),
            enabled: true,
            batching: None,
            filter: None,
            max_retries: 3,
        }
    }

    /// Create a schedule trigger
    pub fn scheduled(cron: &str) -> Self {
        Self {
            name: format!("schedule_{}", cron.replace(' ', "_")),
            trigger_type: TriggerType::Schedule(ScheduleTrigger {
                cron: cron.to_string(),
                timezone: "UTC".to_string(),
            }),
            enabled: true,
            batching: None,
            filter: None,
            max_retries: 3,
        }
    }

    /// Create a queue trigger
    pub fn queue(queue_name: &str) -> Self {
        Self {
            name: format!("queue_{}", queue_name),
            trigger_type: TriggerType::Queue(QueueTrigger {
                queue_name: queue_name.to_string(),
                batch_size: 10,
                visibility_timeout_secs: 30,
            }),
            enabled: true,
            batching: Some(BatchConfig {
                max_batch_size: 10,
                max_wait_ms: 1000,
            }),
            filter: None,
            max_retries: 3,
        }
    }

    /// Create a pub/sub trigger
    pub fn pubsub(channel: &str) -> Self {
        Self {
            name: format!("pubsub_{}", channel),
            trigger_type: TriggerType::PubSub(PubSubTrigger {
                channel: channel.to_string(),
                subscription_type: SubscriptionType::Shared,
            }),
            enabled: true,
            batching: None,
            filter: None,
            max_retries: 3,
        }
    }

    /// Set filter expression
    pub fn with_filter(mut self, filter: &str) -> Self {
        self.filter = Some(filter.to_string());
        self
    }

    /// Set batching
    pub fn with_batching(mut self, max_size: usize, max_wait_ms: u64) -> Self {
        self.batching = Some(BatchConfig {
            max_batch_size: max_size,
            max_wait_ms,
        });
        self
    }

    /// Set max retries
    pub fn with_retries(mut self, retries: usize) -> Self {
        self.max_retries = retries;
        self
    }
}

/// HTTP trigger configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HttpTrigger {
    /// URL path
    pub path: String,
    /// Allowed HTTP methods
    pub methods: Vec<String>,
    /// Authentication
    pub auth: HttpAuth,
    /// CORS configuration
    pub cors: Option<CorsConfig>,
    /// Rate limiting
    pub rate_limit: Option<RateLimitConfig>,
}

/// HTTP authentication
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HttpAuth {
    /// No authentication
    None,
    /// API key authentication
    ApiKey { header: String },
    /// JWT authentication
    Jwt {
        issuer: String,
        audience: Option<String>,
    },
    /// Basic authentication
    Basic,
    /// Custom authentication
    Custom { validator: String },
}

/// CORS configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CorsConfig {
    /// Allowed origins
    pub allowed_origins: Vec<String>,
    /// Allowed methods
    pub allowed_methods: Vec<String>,
    /// Allowed headers
    pub allowed_headers: Vec<String>,
    /// Exposed headers
    pub exposed_headers: Vec<String>,
    /// Max age (seconds)
    pub max_age: u64,
    /// Allow credentials
    pub allow_credentials: bool,
}

/// Rate limit configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Requests per window
    pub requests: usize,
    /// Window duration (seconds)
    pub window_secs: u64,
    /// By IP
    pub by_ip: bool,
    /// By API key
    pub by_api_key: bool,
}

/// Event trigger configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventTrigger {
    /// Key pattern
    pub pattern: String,
    /// Operations to trigger on
    pub operations: Vec<EventOperation>,
}

/// Event operations
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventOperation {
    /// Key set
    Set,
    /// Key deleted
    Delete,
    /// Key expired
    Expire,
    /// Hash field changed
    HSet,
    /// List push
    LPush,
    /// List pop
    RPop,
    /// Set add
    SAdd,
    /// Set remove
    SRem,
    /// Sorted set add
    ZAdd,
    /// Any operation
    Any,
}

/// Schedule trigger configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScheduleTrigger {
    /// Cron expression
    pub cron: String,
    /// Timezone
    pub timezone: String,
}

/// Queue trigger configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueueTrigger {
    /// Queue name
    pub queue_name: String,
    /// Batch size
    pub batch_size: usize,
    /// Visibility timeout (seconds)
    pub visibility_timeout_secs: u64,
}

/// Pub/Sub trigger configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PubSubTrigger {
    /// Channel/topic
    pub channel: String,
    /// Subscription type
    pub subscription_type: SubscriptionType,
}

/// Subscription type
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SubscriptionType {
    /// Exclusive (one consumer)
    Exclusive,
    /// Shared (multiple consumers)
    Shared,
    /// Failover (standby consumers)
    Failover,
}

/// Batch configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BatchConfig {
    /// Maximum batch size
    pub max_batch_size: usize,
    /// Maximum wait time (ms)
    pub max_wait_ms: u64,
}

/// Trigger event (incoming)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TriggerEvent {
    /// Event ID
    pub id: String,
    /// Trigger name
    pub trigger_name: String,
    /// Event type
    pub event_type: String,
    /// Event source
    pub source: EventSource,
    /// Event data
    pub data: EventData,
    /// Timestamp
    pub timestamp: u64,
    /// Metadata
    pub metadata: HashMap<String, String>,
}

/// Event source
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EventSource {
    /// HTTP request
    Http {
        method: String,
        path: String,
        headers: HashMap<String, String>,
    },
    /// Data change
    Data {
        key: String,
        operation: EventOperation,
    },
    /// Schedule
    Schedule { schedule_id: String },
    /// Queue
    Queue {
        queue_name: String,
        message_id: String,
    },
    /// Pub/Sub
    PubSub { channel: String },
}

/// Event data
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EventData {
    /// Raw bytes
    Bytes(Vec<u8>),
    /// JSON
    Json(serde_json::Value),
    /// String
    String(String),
    /// Key-value change
    KeyChange {
        key: String,
        old_value: Option<String>,
        new_value: Option<String>,
    },
    /// Batch of events
    Batch(Vec<EventData>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_trigger() {
        let trigger = TriggerConfig::http("/api/process");

        assert!(trigger.enabled);
        if let TriggerType::Http(http) = trigger.trigger_type {
            assert_eq!(http.path, "/api/process");
            assert!(http.methods.contains(&"GET".to_string()));
        } else {
            panic!("Expected HTTP trigger");
        }
    }

    #[test]
    fn test_event_trigger() {
        let trigger = TriggerConfig::on_key_change("user:*");

        if let TriggerType::Event(event) = trigger.trigger_type {
            assert_eq!(event.pattern, "user:*");
            assert!(event.operations.contains(&EventOperation::Set));
        } else {
            panic!("Expected Event trigger");
        }
    }

    #[test]
    fn test_schedule_trigger() {
        let trigger = TriggerConfig::scheduled("0 * * * *");

        if let TriggerType::Schedule(schedule) = trigger.trigger_type {
            assert_eq!(schedule.cron, "0 * * * *");
            assert_eq!(schedule.timezone, "UTC");
        } else {
            panic!("Expected Schedule trigger");
        }
    }

    #[test]
    fn test_queue_trigger() {
        let trigger = TriggerConfig::queue("my-queue");

        if let TriggerType::Queue(queue) = trigger.trigger_type {
            assert_eq!(queue.queue_name, "my-queue");
            assert_eq!(queue.batch_size, 10);
        } else {
            panic!("Expected Queue trigger");
        }
    }

    #[test]
    fn test_trigger_with_filter() {
        let trigger = TriggerConfig::on_key_change("user:*").with_filter("$.status == 'active'");

        assert_eq!(trigger.filter, Some("$.status == 'active'".to_string()));
    }

    #[test]
    fn test_trigger_with_batching() {
        let trigger = TriggerConfig::queue("batch-queue").with_batching(100, 5000);

        let batch = trigger.batching.unwrap();
        assert_eq!(batch.max_batch_size, 100);
        assert_eq!(batch.max_wait_ms, 5000);
    }

    #[test]
    fn test_event_operations() {
        let ops = vec![EventOperation::Set, EventOperation::Delete];
        assert!(ops.contains(&EventOperation::Set));
        assert!(!ops.contains(&EventOperation::HSet));
    }

    #[test]
    fn test_cors_config() {
        let cors = CorsConfig {
            allowed_origins: vec!["*".to_string()],
            allowed_methods: vec!["GET".to_string(), "POST".to_string()],
            allowed_headers: vec!["Content-Type".to_string()],
            exposed_headers: vec![],
            max_age: 3600,
            allow_credentials: false,
        };

        assert!(cors.allowed_origins.contains(&"*".to_string()));
    }
}
