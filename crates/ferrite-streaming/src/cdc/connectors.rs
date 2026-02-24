//! CDC connector management framework
//!
//! Pre-built connectors for forwarding change events to external systems
//! (Kafka, NATS, PostgreSQL, S3, Webhooks) with lifecycle management,
//! dead letter queues, and delivery guarantees.

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Connector errors
#[derive(Debug, Error)]
pub enum ConnectorError {
    /// Connector not found
    #[error("connector not found")]
    ConnectorNotFound,
    /// Connector with this name already exists
    #[error("connector already exists: {0}")]
    AlreadyExists(String),
    /// Invalid configuration
    #[error("invalid configuration: {0}")]
    ConfigInvalid(String),
    /// Connection to external system failed
    #[error("connection failed: {0}")]
    ConnectionFailed(String),
    /// Event delivery failed
    #[error("delivery failed: {0}")]
    DeliveryFailed(String),
    /// Maximum number of connectors exceeded
    #[error("maximum number of connectors exceeded")]
    MaxConnectorsExceeded,
    /// Invalid state transition
    #[error("invalid state: {0}")]
    InvalidState(String),
    /// Internal error
    #[error("internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Identifiers
// ---------------------------------------------------------------------------

/// Unique connector identifier (UUID wrapper)
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConnectorId(pub String);

impl ConnectorId {
    /// Create a new random connector ID
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

impl Default for ConnectorId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ConnectorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ---------------------------------------------------------------------------
// Connector types and configuration
// ---------------------------------------------------------------------------

/// Type of CDC connector
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConnectorType {
    /// Apache Kafka
    Kafka,
    /// NATS / JetStream
    Nats,
    /// PostgreSQL
    PostgreSQL,
    /// AWS S3
    S3,
    /// HTTP webhook
    Webhook,
    /// Custom connector
    Custom(String),
}

impl ConnectorType {
    /// Get connector type name
    pub fn name(&self) -> &str {
        match self {
            Self::Kafka => "kafka",
            Self::Nats => "nats",
            Self::PostgreSQL => "postgresql",
            Self::S3 => "s3",
            Self::Webhook => "webhook",
            Self::Custom(name) => name,
        }
    }
}

/// Kafka compression algorithm
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum KafkaCompression {
    /// No compression
    #[default]
    None,
    /// Gzip compression
    Gzip,
    /// Snappy compression
    Snappy,
    /// LZ4 compression
    Lz4,
    /// Zstandard compression
    Zstd,
}

/// Kafka acknowledgement level
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum KafkaAcks {
    /// No acknowledgement (fire and forget)
    Zero,
    /// Leader acknowledgement
    #[default]
    One,
    /// All replicas acknowledgement
    All,
}

/// PostgreSQL write mode
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum PgMode {
    /// Simple INSERT
    #[default]
    Insert,
    /// INSERT ... ON CONFLICT UPDATE
    Upsert {
        /// Conflict key columns
        conflict_keys: Vec<String>,
    },
}

/// S3 output format
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum S3Format {
    /// JSON (one object per line)
    #[default]
    Json,
    /// CSV format
    Csv,
    /// Apache Parquet
    Parquet,
    /// Apache Avro
    Avro,
}

impl S3Format {
    /// Get file extension
    pub fn extension(&self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Csv => "csv",
            Self::Parquet => "parquet",
            Self::Avro => "avro",
        }
    }
}

/// S3 partitioning strategy
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum S3Partition {
    /// Partition by hour
    Hourly,
    /// Partition by day
    #[default]
    Daily,
    /// Partition by month
    Monthly,
    /// No partitioning
    None,
}

/// Webhook authentication
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum WebhookAuth {
    /// No authentication
    None,
    /// Bearer token
    Bearer(String),
    /// HTTP basic authentication
    Basic {
        /// Username
        username: String,
        /// Password
        password: String,
    },
    /// API key in header
    ApiKey {
        /// Header name
        header: String,
        /// API key value
        value: String,
    },
}

impl Default for WebhookAuth {
    fn default() -> Self {
        Self::None
    }
}

/// Connector-specific configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ConnectorConfig {
    /// Kafka connector configuration
    Kafka {
        /// Broker addresses
        brokers: Vec<String>,
        /// Target topic
        topic: String,
        /// Client identifier
        client_id: String,
        /// Compression algorithm
        compression: KafkaCompression,
        /// Acknowledgement level
        acks: KafkaAcks,
    },
    /// NATS connector configuration
    Nats {
        /// Server addresses
        servers: Vec<String>,
        /// Subject to publish to
        subject: String,
        /// JetStream stream name
        stream: Option<String>,
        /// Credentials file path
        credentials: Option<String>,
    },
    /// PostgreSQL connector configuration
    PostgreSQL {
        /// Connection string
        connection_string: String,
        /// Target table
        table: String,
        /// Schema name
        schema: String,
        /// Write mode
        mode: PgMode,
    },
    /// S3 connector configuration
    S3 {
        /// Bucket name
        bucket: String,
        /// Key prefix
        prefix: String,
        /// AWS region
        region: String,
        /// Output format
        format: S3Format,
        /// Partitioning strategy
        partition: S3Partition,
    },
    /// Webhook connector configuration
    Webhook {
        /// Endpoint URL
        url: String,
        /// HTTP method
        method: String,
        /// Additional headers
        headers: HashMap<String, String>,
        /// Authentication
        auth: Option<WebhookAuth>,
    },
}

impl ConnectorConfig {
    /// Get the connector type for this configuration
    pub fn connector_type(&self) -> ConnectorType {
        match self {
            Self::Kafka { .. } => ConnectorType::Kafka,
            Self::Nats { .. } => ConnectorType::Nats,
            Self::PostgreSQL { .. } => ConnectorType::PostgreSQL,
            Self::S3 { .. } => ConnectorType::S3,
            Self::Webhook { .. } => ConnectorType::Webhook,
        }
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), ConnectorError> {
        match self {
            Self::Kafka {
                brokers,
                topic,
                client_id,
                ..
            } => {
                if brokers.is_empty() {
                    return Err(ConnectorError::ConfigInvalid(
                        "kafka: at least one broker required".to_string(),
                    ));
                }
                if topic.is_empty() {
                    return Err(ConnectorError::ConfigInvalid(
                        "kafka: topic is required".to_string(),
                    ));
                }
                if client_id.is_empty() {
                    return Err(ConnectorError::ConfigInvalid(
                        "kafka: client_id is required".to_string(),
                    ));
                }
                Ok(())
            }
            Self::Nats {
                servers, subject, ..
            } => {
                if servers.is_empty() {
                    return Err(ConnectorError::ConfigInvalid(
                        "nats: at least one server required".to_string(),
                    ));
                }
                if subject.is_empty() {
                    return Err(ConnectorError::ConfigInvalid(
                        "nats: subject is required".to_string(),
                    ));
                }
                Ok(())
            }
            Self::PostgreSQL {
                connection_string,
                table,
                schema,
                ..
            } => {
                if connection_string.is_empty() {
                    return Err(ConnectorError::ConfigInvalid(
                        "postgresql: connection_string is required".to_string(),
                    ));
                }
                if table.is_empty() {
                    return Err(ConnectorError::ConfigInvalid(
                        "postgresql: table is required".to_string(),
                    ));
                }
                if schema.is_empty() {
                    return Err(ConnectorError::ConfigInvalid(
                        "postgresql: schema is required".to_string(),
                    ));
                }
                Ok(())
            }
            Self::S3 { bucket, region, .. } => {
                if bucket.is_empty() {
                    return Err(ConnectorError::ConfigInvalid(
                        "s3: bucket is required".to_string(),
                    ));
                }
                if region.is_empty() {
                    return Err(ConnectorError::ConfigInvalid(
                        "s3: region is required".to_string(),
                    ));
                }
                Ok(())
            }
            Self::Webhook { url, method, .. } => {
                if url.is_empty() {
                    return Err(ConnectorError::ConfigInvalid(
                        "webhook: url is required".to_string(),
                    ));
                }
                if method.is_empty() {
                    return Err(ConnectorError::ConfigInvalid(
                        "webhook: method is required".to_string(),
                    ));
                }
                Ok(())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Delivery guarantee
// ---------------------------------------------------------------------------

/// Delivery guarantee level
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum DeliveryGuarantee {
    /// Events may be lost (fastest)
    AtMostOnce,
    /// Events may be delivered more than once (default)
    #[default]
    AtLeastOnce,
    /// Events are delivered exactly once (slowest, highest integrity)
    ExactlyOnce,
}

// ---------------------------------------------------------------------------
// Event filter
// ---------------------------------------------------------------------------

/// Filter for selecting which CDC events a connector receives
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EventFilter {
    /// Key patterns to include (glob-style, e.g. "user:*")
    pub key_patterns: Vec<String>,
    /// Event types to include (e.g. "SET", "DEL")
    pub event_types: Vec<String>,
    /// Key patterns to exclude
    pub exclude_patterns: Vec<String>,
}

impl EventFilter {
    /// Check if a key matches this filter
    pub fn matches_key(&self, key: &str) -> bool {
        // If no include patterns, everything matches
        let included =
            self.key_patterns.is_empty() || self.key_patterns.iter().any(|p| glob_match(p, key));

        if !included {
            return false;
        }

        // Check exclude patterns
        !self.exclude_patterns.iter().any(|p| glob_match(p, key))
    }

    /// Check if an event type matches this filter
    pub fn matches_event_type(&self, event_type: &str) -> bool {
        self.event_types.is_empty()
            || self
                .event_types
                .iter()
                .any(|t| t.eq_ignore_ascii_case(event_type))
    }

    /// Check if a key and event type both match
    pub fn matches(&self, key: &str, event_type: &str) -> bool {
        self.matches_key(key) && self.matches_event_type(event_type)
    }
}

/// Simple glob matching supporting `*` and `?` wildcards
fn glob_match(pattern: &str, value: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let value: Vec<char> = value.chars().collect();
    glob_match_inner(&pattern, &value)
}

fn glob_match_inner(pattern: &[char], value: &[char]) -> bool {
    match (pattern.first(), value.first()) {
        (None, None) => true,
        (Some('*'), _) => {
            // '*' matches zero or more characters
            glob_match_inner(&pattern[1..], value)
                || (!value.is_empty() && glob_match_inner(pattern, &value[1..]))
        }
        (Some('?'), Some(_)) => glob_match_inner(&pattern[1..], &value[1..]),
        (Some(a), Some(b)) if a == b => glob_match_inner(&pattern[1..], &value[1..]),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Connector spec
// ---------------------------------------------------------------------------

/// Specification for creating a new connector
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectorSpec {
    /// Human-readable name
    pub name: String,
    /// Type of connector
    pub connector_type: ConnectorType,
    /// Connector-specific configuration
    pub config: ConnectorConfig,
    /// Event filters
    pub filters: Vec<EventFilter>,
    /// Delivery guarantee level
    pub delivery_guarantee: DeliveryGuarantee,
}

// ---------------------------------------------------------------------------
// Connector state and status
// ---------------------------------------------------------------------------

/// Connector lifecycle state
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConnectorState {
    /// Connector is running and delivering events
    #[default]
    Running,
    /// Connector is paused (events are buffered)
    Paused,
    /// Connector has failed
    Failed(String),
    /// Connector is starting up
    Starting,
    /// Connector is shutting down
    Stopping,
}

impl ConnectorState {
    /// Get state name
    pub fn name(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Failed(_) => "failed",
            Self::Starting => "starting",
            Self::Stopping => "stopping",
        }
    }
}

/// Detailed connector status
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectorStatus {
    /// Connector ID
    pub id: ConnectorId,
    /// Connector name
    pub name: String,
    /// Connector type
    pub connector_type: ConnectorType,
    /// Current state
    pub state: ConnectorState,
    /// Total events delivered successfully
    pub events_delivered: u64,
    /// Total events that failed delivery
    pub events_failed: u64,
    /// Timestamp of last delivered event
    pub last_event_at: Option<DateTime<Utc>>,
    /// Last error message
    pub last_error: Option<String>,
    /// Event lag (pending events)
    pub lag: u64,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
}

/// Summary info for listing connectors
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectorInfo {
    /// Connector ID
    pub id: ConnectorId,
    /// Connector name
    pub name: String,
    /// Connector type
    pub connector_type: ConnectorType,
    /// Current state
    pub state: ConnectorState,
    /// Delivery guarantee
    pub delivery_guarantee: DeliveryGuarantee,
    /// Total events delivered
    pub events_delivered: u64,
    /// Error rate (failed / total attempted)
    pub error_rate: f64,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Dead letter queue
// ---------------------------------------------------------------------------

/// Entry in the dead letter queue for events that could not be delivered
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeadLetterEntry {
    /// Original event ID
    pub original_event_id: String,
    /// Connector that failed delivery
    pub connector_id: ConnectorId,
    /// Error message
    pub error: String,
    /// Number of delivery attempts
    pub attempts: u32,
    /// First failure timestamp
    pub first_failed_at: DateTime<Utc>,
    /// Most recent failure timestamp
    pub last_failed_at: DateTime<Utc>,
    /// Serialized event data
    pub event_data: String,
}

// ---------------------------------------------------------------------------
// Manager stats
// ---------------------------------------------------------------------------

/// Aggregate statistics for the connector manager
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ConnectorManagerStats {
    /// Total registered connectors
    pub total_connectors: usize,
    /// Connectors in running state
    pub running_connectors: usize,
    /// Connectors in paused state
    pub paused_connectors: usize,
    /// Connectors in failed state
    pub failed_connectors: usize,
    /// Total events delivered across all connectors
    pub total_events_delivered: u64,
    /// Total events that failed delivery
    pub total_events_failed: u64,
    /// Total dead letter entries
    pub total_dead_letters: u64,
    /// Average delivery latency in milliseconds
    pub avg_delivery_latency_ms: f64,
}

// ---------------------------------------------------------------------------
// Manager config
// ---------------------------------------------------------------------------

/// Configuration for the connector manager
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectorManagerConfig {
    /// Maximum number of connectors allowed
    pub max_connectors: usize,
    /// Health check interval in milliseconds
    pub health_check_interval_ms: u64,
    /// Default batch size for connectors
    pub default_batch_size: usize,
    /// Default flush interval in milliseconds
    pub default_flush_interval_ms: u64,
    /// Enable dead letter queue
    pub enable_dead_letter: bool,
    /// Maximum delivery retries before dead-lettering
    pub max_retries: u32,
}

impl Default for ConnectorManagerConfig {
    fn default() -> Self {
        Self {
            max_connectors: 100,
            health_check_interval_ms: 10_000,
            default_batch_size: 1000,
            default_flush_interval_ms: 5000,
            enable_dead_letter: true,
            max_retries: 3,
        }
    }
}

// ---------------------------------------------------------------------------
// Internal connector handle
// ---------------------------------------------------------------------------

/// Internal connector instance managed by the ConnectorManager
struct ConnectorHandle {
    /// Connector ID
    id: ConnectorId,
    /// Connector name
    name: String,
    /// Connector specification
    spec: ConnectorSpec,
    /// Current state
    state: RwLock<ConnectorState>,
    /// Events delivered counter
    events_delivered: AtomicU64,
    /// Events failed counter
    events_failed: AtomicU64,
    /// Timestamp of last delivered event
    last_event_at: RwLock<Option<DateTime<Utc>>>,
    /// Last error message
    last_error: RwLock<Option<String>>,
    /// Event lag
    lag: AtomicU64,
    /// Creation timestamp
    created_at: DateTime<Utc>,
    /// Cumulative delivery latency in microseconds
    total_latency_us: AtomicU64,
    /// Number of latency samples
    latency_samples: AtomicU64,
}

impl ConnectorHandle {
    fn new(id: ConnectorId, spec: ConnectorSpec) -> Self {
        Self {
            id,
            name: spec.name.clone(),
            spec,
            state: RwLock::new(ConnectorState::Running),
            events_delivered: AtomicU64::new(0),
            events_failed: AtomicU64::new(0),
            last_event_at: RwLock::new(None),
            last_error: RwLock::new(None),
            lag: AtomicU64::new(0),
            created_at: Utc::now(),
            total_latency_us: AtomicU64::new(0),
            latency_samples: AtomicU64::new(0),
        }
    }

    async fn status(&self) -> ConnectorStatus {
        ConnectorStatus {
            id: self.id.clone(),
            name: self.name.clone(),
            connector_type: self.spec.connector_type.clone(),
            state: self.state.read().await.clone(),
            events_delivered: self.events_delivered.load(Ordering::Relaxed),
            events_failed: self.events_failed.load(Ordering::Relaxed),
            last_event_at: *self.last_event_at.read().await,
            last_error: self.last_error.read().await.clone(),
            lag: self.lag.load(Ordering::Relaxed),
            created_at: self.created_at,
        }
    }

    async fn info(&self) -> ConnectorInfo {
        let delivered = self.events_delivered.load(Ordering::Relaxed);
        let failed = self.events_failed.load(Ordering::Relaxed);
        let total = delivered + failed;
        let error_rate = if total > 0 {
            failed as f64 / total as f64
        } else {
            0.0
        };

        ConnectorInfo {
            id: self.id.clone(),
            name: self.name.clone(),
            connector_type: self.spec.connector_type.clone(),
            state: self.state.read().await.clone(),
            delivery_guarantee: self.spec.delivery_guarantee.clone(),
            events_delivered: delivered,
            error_rate,
            created_at: self.created_at,
        }
    }

    fn avg_latency_ms(&self) -> f64 {
        let samples = self.latency_samples.load(Ordering::Relaxed);
        if samples == 0 {
            return 0.0;
        }
        let total_us = self.total_latency_us.load(Ordering::Relaxed);
        (total_us as f64 / samples as f64) / 1000.0
    }
}

// ---------------------------------------------------------------------------
// ConnectorManager
// ---------------------------------------------------------------------------

/// Manages CDC connectors — creation, lifecycle, delivery tracking, and dead letter queue
pub struct ConnectorManager {
    /// Configuration
    config: ConnectorManagerConfig,
    /// Connectors by ID
    connectors: DashMap<ConnectorId, Arc<ConnectorHandle>>,
    /// Connectors by name (for uniqueness)
    by_name: DashMap<String, ConnectorId>,
    /// Dead letter queue
    dead_letters: DashMap<String, DeadLetterEntry>,
    /// Global dead letter counter
    dead_letter_count: AtomicU64,
}

impl ConnectorManager {
    /// Create a new connector manager
    pub fn new(config: ConnectorManagerConfig) -> Self {
        Self {
            config,
            connectors: DashMap::new(),
            by_name: DashMap::new(),
            dead_letters: DashMap::new(),
            dead_letter_count: AtomicU64::new(0),
        }
    }

    /// Create a new connector from a specification
    pub fn create_connector(&self, spec: ConnectorSpec) -> Result<ConnectorId, ConnectorError> {
        // Check capacity
        if self.connectors.len() >= self.config.max_connectors {
            return Err(ConnectorError::MaxConnectorsExceeded);
        }

        // Check name uniqueness
        if self.by_name.contains_key(&spec.name) {
            return Err(ConnectorError::AlreadyExists(spec.name.clone()));
        }

        // Validate config
        spec.config.validate()?;

        let id = ConnectorId::new();
        let handle = ConnectorHandle::new(id.clone(), spec.clone());

        self.by_name.insert(spec.name.clone(), id.clone());
        self.connectors.insert(id.clone(), Arc::new(handle));

        Ok(id)
    }

    /// Delete a connector
    pub async fn delete_connector(&self, id: &ConnectorId) -> Result<(), ConnectorError> {
        let (_, handle) = self
            .connectors
            .remove(id)
            .ok_or(ConnectorError::ConnectorNotFound)?;

        self.by_name.remove(&handle.name);

        // Transition to stopping
        *handle.state.write().await = ConnectorState::Stopping;

        Ok(())
    }

    /// Pause a connector (events will be buffered)
    pub async fn pause_connector(&self, id: &ConnectorId) -> Result<(), ConnectorError> {
        let handle = self
            .connectors
            .get(id)
            .ok_or(ConnectorError::ConnectorNotFound)?;

        let mut state = handle.state.write().await;
        match &*state {
            ConnectorState::Running => {
                *state = ConnectorState::Paused;
                Ok(())
            }
            other => Err(ConnectorError::InvalidState(format!(
                "cannot pause connector in {} state",
                other.name()
            ))),
        }
    }

    /// Resume a paused connector
    pub async fn resume_connector(&self, id: &ConnectorId) -> Result<(), ConnectorError> {
        let handle = self
            .connectors
            .get(id)
            .ok_or(ConnectorError::ConnectorNotFound)?;

        let mut state = handle.state.write().await;
        match &*state {
            ConnectorState::Paused => {
                *state = ConnectorState::Running;
                Ok(())
            }
            other => Err(ConnectorError::InvalidState(format!(
                "cannot resume connector in {} state",
                other.name()
            ))),
        }
    }

    /// Get detailed status for a connector
    pub async fn get_status(&self, id: &ConnectorId) -> Option<ConnectorStatus> {
        let handle = self.connectors.get(id)?;
        Some(handle.status().await)
    }

    /// List all connectors
    pub async fn list_connectors(&self) -> Vec<ConnectorInfo> {
        let mut infos = Vec::with_capacity(self.connectors.len());
        for entry in self.connectors.iter() {
            infos.push(entry.value().info().await);
        }
        infos
    }

    /// Get aggregate manager statistics
    pub async fn get_stats(&self) -> ConnectorManagerStats {
        let mut stats = ConnectorManagerStats::default();
        let mut total_latency = 0.0;
        let mut latency_count = 0u64;

        for entry in self.connectors.iter() {
            let handle = entry.value();
            stats.total_connectors += 1;

            let state = handle.state.read().await;
            match &*state {
                ConnectorState::Running => stats.running_connectors += 1,
                ConnectorState::Paused => stats.paused_connectors += 1,
                ConnectorState::Failed(_) => stats.failed_connectors += 1,
                _ => {}
            }
            drop(state);

            let delivered = handle.events_delivered.load(Ordering::Relaxed);
            let failed = handle.events_failed.load(Ordering::Relaxed);
            stats.total_events_delivered += delivered;
            stats.total_events_failed += failed;

            let samples = handle.latency_samples.load(Ordering::Relaxed);
            if samples > 0 {
                total_latency += handle.avg_latency_ms() * samples as f64;
                latency_count += samples;
            }
        }

        stats.total_dead_letters = self.dead_letter_count.load(Ordering::Relaxed);
        stats.avg_delivery_latency_ms = if latency_count > 0 {
            total_latency / latency_count as f64
        } else {
            0.0
        };

        stats
    }

    /// Record a successful event delivery for a connector
    pub async fn record_delivery(
        &self,
        id: &ConnectorId,
        count: u64,
        latency_us: u64,
    ) -> Result<(), ConnectorError> {
        let handle = self
            .connectors
            .get(id)
            .ok_or(ConnectorError::ConnectorNotFound)?;

        handle.events_delivered.fetch_add(count, Ordering::Relaxed);
        handle
            .total_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);
        handle.latency_samples.fetch_add(count, Ordering::Relaxed);
        *handle.last_event_at.write().await = Some(Utc::now());

        Ok(())
    }

    /// Record a failed event delivery
    pub async fn record_failure(
        &self,
        id: &ConnectorId,
        error: String,
    ) -> Result<(), ConnectorError> {
        let handle = self
            .connectors
            .get(id)
            .ok_or(ConnectorError::ConnectorNotFound)?;

        handle.events_failed.fetch_add(1, Ordering::Relaxed);
        *handle.last_error.write().await = Some(error.clone());

        // Transition to failed state
        *handle.state.write().await = ConnectorState::Failed(error);

        Ok(())
    }

    /// Add an entry to the dead letter queue
    pub fn add_dead_letter(&self, entry: DeadLetterEntry) {
        if self.config.enable_dead_letter {
            let key = format!("{}:{}", entry.connector_id, entry.original_event_id);
            self.dead_letters.insert(key, entry);
            self.dead_letter_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get dead letter entries for a connector
    pub fn get_dead_letters(&self, connector_id: &ConnectorId) -> Vec<DeadLetterEntry> {
        self.dead_letters
            .iter()
            .filter(|e| e.value().connector_id == *connector_id)
            .map(|e| e.value().clone())
            .collect()
    }

    /// Get the manager configuration
    pub fn config(&self) -> &ConnectorManagerConfig {
        &self.config
    }

    /// Get number of connectors
    pub fn len(&self) -> usize {
        self.connectors.len()
    }

    /// Check if there are no connectors
    pub fn is_empty(&self) -> bool {
        self.connectors.is_empty()
    }
}

impl Default for ConnectorManager {
    fn default() -> Self {
        Self::new(ConnectorManagerConfig::default())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Helpers --

    fn kafka_spec(name: &str) -> ConnectorSpec {
        ConnectorSpec {
            name: name.to_string(),
            connector_type: ConnectorType::Kafka,
            config: ConnectorConfig::Kafka {
                brokers: vec!["localhost:9092".to_string()],
                topic: "ferrite-cdc".to_string(),
                client_id: "ferrite".to_string(),
                compression: KafkaCompression::None,
                acks: KafkaAcks::One,
            },
            filters: vec![],
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
        }
    }

    fn nats_spec(name: &str) -> ConnectorSpec {
        ConnectorSpec {
            name: name.to_string(),
            connector_type: ConnectorType::Nats,
            config: ConnectorConfig::Nats {
                servers: vec!["nats://localhost:4222".to_string()],
                subject: "ferrite.events".to_string(),
                stream: Some("FERRITE".to_string()),
                credentials: None,
            },
            filters: vec![],
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
        }
    }

    fn pg_spec(name: &str) -> ConnectorSpec {
        ConnectorSpec {
            name: name.to_string(),
            connector_type: ConnectorType::PostgreSQL,
            config: ConnectorConfig::PostgreSQL {
                connection_string: "postgres://localhost/ferrite".to_string(),
                table: "cdc_events".to_string(),
                schema: "public".to_string(),
                mode: PgMode::Insert,
            },
            filters: vec![],
            delivery_guarantee: DeliveryGuarantee::ExactlyOnce,
        }
    }

    fn s3_spec(name: &str) -> ConnectorSpec {
        ConnectorSpec {
            name: name.to_string(),
            connector_type: ConnectorType::S3,
            config: ConnectorConfig::S3 {
                bucket: "ferrite-cdc".to_string(),
                prefix: "events/".to_string(),
                region: "us-east-1".to_string(),
                format: S3Format::Json,
                partition: S3Partition::Daily,
            },
            filters: vec![],
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
        }
    }

    fn webhook_spec(name: &str) -> ConnectorSpec {
        ConnectorSpec {
            name: name.to_string(),
            connector_type: ConnectorType::Webhook,
            config: ConnectorConfig::Webhook {
                url: "https://example.com/webhook".to_string(),
                method: "POST".to_string(),
                headers: HashMap::new(),
                auth: Some(WebhookAuth::Bearer("token123".to_string())),
            },
            filters: vec![],
            delivery_guarantee: DeliveryGuarantee::AtMostOnce,
        }
    }

    // -- CRUD tests --

    #[test]
    fn test_create_connector() {
        let mgr = ConnectorManager::default();
        let id = mgr.create_connector(kafka_spec("kafka-1")).unwrap();
        assert_eq!(mgr.len(), 1);
        assert!(!id.0.is_empty());
    }

    #[test]
    fn test_create_all_connector_types() {
        let mgr = ConnectorManager::default();
        mgr.create_connector(kafka_spec("kafka")).unwrap();
        mgr.create_connector(nats_spec("nats")).unwrap();
        mgr.create_connector(pg_spec("pg")).unwrap();
        mgr.create_connector(s3_spec("s3")).unwrap();
        mgr.create_connector(webhook_spec("webhook")).unwrap();
        assert_eq!(mgr.len(), 5);
    }

    #[test]
    fn test_create_duplicate_name_fails() {
        let mgr = ConnectorManager::default();
        mgr.create_connector(kafka_spec("kafka-1")).unwrap();
        let err = mgr.create_connector(kafka_spec("kafka-1")).unwrap_err();
        assert!(matches!(err, ConnectorError::AlreadyExists(_)));
    }

    #[test]
    fn test_max_connectors_exceeded() {
        let config = ConnectorManagerConfig {
            max_connectors: 2,
            ..Default::default()
        };
        let mgr = ConnectorManager::new(config);
        mgr.create_connector(kafka_spec("c1")).unwrap();
        mgr.create_connector(nats_spec("c2")).unwrap();
        let err = mgr.create_connector(pg_spec("c3")).unwrap_err();
        assert!(matches!(err, ConnectorError::MaxConnectorsExceeded));
    }

    #[tokio::test]
    async fn test_delete_connector() {
        let mgr = ConnectorManager::default();
        let id = mgr.create_connector(kafka_spec("kafka-1")).unwrap();
        assert_eq!(mgr.len(), 1);

        mgr.delete_connector(&id).await.unwrap();
        assert_eq!(mgr.len(), 0);
    }

    #[tokio::test]
    async fn test_delete_nonexistent_connector() {
        let mgr = ConnectorManager::default();
        let err = mgr.delete_connector(&ConnectorId::new()).await.unwrap_err();
        assert!(matches!(err, ConnectorError::ConnectorNotFound));
    }

    #[tokio::test]
    async fn test_delete_allows_name_reuse() {
        let mgr = ConnectorManager::default();
        let id = mgr.create_connector(kafka_spec("kafka-1")).unwrap();
        mgr.delete_connector(&id).await.unwrap();

        // Should be able to create with same name
        let id2 = mgr.create_connector(kafka_spec("kafka-1")).unwrap();
        assert_ne!(id, id2);
        assert_eq!(mgr.len(), 1);
    }

    // -- Pause / Resume lifecycle --

    #[tokio::test]
    async fn test_pause_connector() {
        let mgr = ConnectorManager::default();
        let id = mgr.create_connector(kafka_spec("kafka-1")).unwrap();

        mgr.pause_connector(&id).await.unwrap();

        let status = mgr.get_status(&id).await.unwrap();
        assert_eq!(status.state, ConnectorState::Paused);
    }

    #[tokio::test]
    async fn test_resume_connector() {
        let mgr = ConnectorManager::default();
        let id = mgr.create_connector(kafka_spec("kafka-1")).unwrap();

        mgr.pause_connector(&id).await.unwrap();
        mgr.resume_connector(&id).await.unwrap();

        let status = mgr.get_status(&id).await.unwrap();
        assert_eq!(status.state, ConnectorState::Running);
    }

    #[tokio::test]
    async fn test_pause_already_paused_fails() {
        let mgr = ConnectorManager::default();
        let id = mgr.create_connector(kafka_spec("kafka-1")).unwrap();

        mgr.pause_connector(&id).await.unwrap();
        let err = mgr.pause_connector(&id).await.unwrap_err();
        assert!(matches!(err, ConnectorError::InvalidState(_)));
    }

    #[tokio::test]
    async fn test_resume_running_fails() {
        let mgr = ConnectorManager::default();
        let id = mgr.create_connector(kafka_spec("kafka-1")).unwrap();

        let err = mgr.resume_connector(&id).await.unwrap_err();
        assert!(matches!(err, ConnectorError::InvalidState(_)));
    }

    #[tokio::test]
    async fn test_pause_nonexistent_fails() {
        let mgr = ConnectorManager::default();
        let err = mgr.pause_connector(&ConnectorId::new()).await.unwrap_err();
        assert!(matches!(err, ConnectorError::ConnectorNotFound));
    }

    // -- Status tracking --

    #[tokio::test]
    async fn test_get_status_initial() {
        let mgr = ConnectorManager::default();
        let id = mgr.create_connector(kafka_spec("kafka-1")).unwrap();

        let status = mgr.get_status(&id).await.unwrap();
        assert_eq!(status.name, "kafka-1");
        assert_eq!(status.connector_type, ConnectorType::Kafka);
        assert_eq!(status.state, ConnectorState::Running);
        assert_eq!(status.events_delivered, 0);
        assert_eq!(status.events_failed, 0);
        assert!(status.last_event_at.is_none());
        assert!(status.last_error.is_none());
    }

    #[tokio::test]
    async fn test_get_status_nonexistent_returns_none() {
        let mgr = ConnectorManager::default();
        assert!(mgr.get_status(&ConnectorId::new()).await.is_none());
    }

    #[tokio::test]
    async fn test_record_delivery_updates_status() {
        let mgr = ConnectorManager::default();
        let id = mgr.create_connector(kafka_spec("kafka-1")).unwrap();

        mgr.record_delivery(&id, 10, 5000).await.unwrap();

        let status = mgr.get_status(&id).await.unwrap();
        assert_eq!(status.events_delivered, 10);
        assert!(status.last_event_at.is_some());
    }

    #[tokio::test]
    async fn test_record_failure_updates_status() {
        let mgr = ConnectorManager::default();
        let id = mgr.create_connector(kafka_spec("kafka-1")).unwrap();

        mgr.record_failure(&id, "connection timeout".to_string())
            .await
            .unwrap();

        let status = mgr.get_status(&id).await.unwrap();
        assert_eq!(status.events_failed, 1);
        assert_eq!(
            status.state,
            ConnectorState::Failed("connection timeout".to_string())
        );
        assert_eq!(status.last_error.as_deref(), Some("connection timeout"));
    }

    // -- List connectors --

    #[tokio::test]
    async fn test_list_connectors() {
        let mgr = ConnectorManager::default();
        mgr.create_connector(kafka_spec("kafka")).unwrap();
        mgr.create_connector(nats_spec("nats")).unwrap();

        let list = mgr.list_connectors().await;
        assert_eq!(list.len(), 2);

        let names: Vec<&str> = list.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"kafka"));
        assert!(names.contains(&"nats"));
    }

    #[tokio::test]
    async fn test_list_connectors_empty() {
        let mgr = ConnectorManager::default();
        let list = mgr.list_connectors().await;
        assert!(list.is_empty());
    }

    #[tokio::test]
    async fn test_connector_info_error_rate() {
        let mgr = ConnectorManager::default();
        let id = mgr.create_connector(kafka_spec("kafka")).unwrap();

        // 8 delivered, 2 failed = 20% error rate
        mgr.record_delivery(&id, 8, 1000).await.unwrap();
        // Record 2 failures — we need to reset state back to running after each
        for _ in 0..2 {
            mgr.record_failure(&id, "err".to_string()).await.unwrap();
        }

        let list = mgr.list_connectors().await;
        let info = list.iter().find(|c| c.name == "kafka").unwrap();
        assert!((info.error_rate - 0.2).abs() < 0.01);
    }

    // -- Event filter matching --

    #[test]
    fn test_filter_matches_key_pattern() {
        let filter = EventFilter {
            key_patterns: vec!["user:*".to_string()],
            event_types: vec![],
            exclude_patterns: vec![],
        };
        assert!(filter.matches_key("user:123"));
        assert!(filter.matches_key("user:abc"));
        assert!(!filter.matches_key("order:456"));
    }

    #[test]
    fn test_filter_empty_patterns_match_all() {
        let filter = EventFilter::default();
        assert!(filter.matches_key("anything"));
        assert!(filter.matches_event_type("SET"));
    }

    #[test]
    fn test_filter_exclude_patterns() {
        let filter = EventFilter {
            key_patterns: vec!["user:*".to_string()],
            event_types: vec![],
            exclude_patterns: vec!["user:internal:*".to_string()],
        };
        assert!(filter.matches_key("user:123"));
        assert!(!filter.matches_key("user:internal:system"));
    }

    #[test]
    fn test_filter_event_type_matching() {
        let filter = EventFilter {
            key_patterns: vec![],
            event_types: vec!["SET".to_string(), "DEL".to_string()],
            exclude_patterns: vec![],
        };
        assert!(filter.matches_event_type("SET"));
        assert!(filter.matches_event_type("set")); // case-insensitive
        assert!(filter.matches_event_type("DEL"));
        assert!(!filter.matches_event_type("HSET"));
    }

    #[test]
    fn test_filter_combined_match() {
        let filter = EventFilter {
            key_patterns: vec!["user:*".to_string()],
            event_types: vec!["SET".to_string()],
            exclude_patterns: vec![],
        };
        assert!(filter.matches("user:123", "SET"));
        assert!(!filter.matches("user:123", "DEL"));
        assert!(!filter.matches("order:1", "SET"));
    }

    #[test]
    fn test_glob_match_question_mark() {
        let filter = EventFilter {
            key_patterns: vec!["user:?".to_string()],
            event_types: vec![],
            exclude_patterns: vec![],
        };
        assert!(filter.matches_key("user:1"));
        assert!(!filter.matches_key("user:12"));
    }

    #[test]
    fn test_glob_match_exact() {
        let filter = EventFilter {
            key_patterns: vec!["exact-key".to_string()],
            event_types: vec![],
            exclude_patterns: vec![],
        };
        assert!(filter.matches_key("exact-key"));
        assert!(!filter.matches_key("exact-keys"));
        assert!(!filter.matches_key("exact-ke"));
    }

    // -- Dead letter queue --

    #[test]
    fn test_dead_letter_add_and_retrieve() {
        let mgr = ConnectorManager::default();
        let id = mgr.create_connector(kafka_spec("kafka")).unwrap();

        let entry = DeadLetterEntry {
            original_event_id: "evt-1".to_string(),
            connector_id: id.clone(),
            error: "timeout".to_string(),
            attempts: 3,
            first_failed_at: Utc::now(),
            last_failed_at: Utc::now(),
            event_data: r#"{"key":"test"}"#.to_string(),
        };
        mgr.add_dead_letter(entry);

        let entries = mgr.get_dead_letters(&id);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].original_event_id, "evt-1");
        assert_eq!(entries[0].attempts, 3);
    }

    #[test]
    fn test_dead_letter_disabled() {
        let config = ConnectorManagerConfig {
            enable_dead_letter: false,
            ..Default::default()
        };
        let mgr = ConnectorManager::new(config);
        let id = ConnectorId::new();

        let entry = DeadLetterEntry {
            original_event_id: "evt-1".to_string(),
            connector_id: id.clone(),
            error: "timeout".to_string(),
            attempts: 1,
            first_failed_at: Utc::now(),
            last_failed_at: Utc::now(),
            event_data: "{}".to_string(),
        };
        mgr.add_dead_letter(entry);

        let entries = mgr.get_dead_letters(&id);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_dead_letter_per_connector() {
        let mgr = ConnectorManager::default();
        let id1 = mgr.create_connector(kafka_spec("k1")).unwrap();
        let id2 = mgr.create_connector(nats_spec("n1")).unwrap();

        for i in 0..3 {
            mgr.add_dead_letter(DeadLetterEntry {
                original_event_id: format!("evt-{}", i),
                connector_id: id1.clone(),
                error: "err".to_string(),
                attempts: 1,
                first_failed_at: Utc::now(),
                last_failed_at: Utc::now(),
                event_data: "{}".to_string(),
            });
        }
        mgr.add_dead_letter(DeadLetterEntry {
            original_event_id: "evt-99".to_string(),
            connector_id: id2.clone(),
            error: "err".to_string(),
            attempts: 1,
            first_failed_at: Utc::now(),
            last_failed_at: Utc::now(),
            event_data: "{}".to_string(),
        });

        assert_eq!(mgr.get_dead_letters(&id1).len(), 3);
        assert_eq!(mgr.get_dead_letters(&id2).len(), 1);
    }

    // -- Stats --

    #[tokio::test]
    async fn test_stats_empty() {
        let mgr = ConnectorManager::default();
        let stats = mgr.get_stats().await;
        assert_eq!(stats.total_connectors, 0);
        assert_eq!(stats.running_connectors, 0);
        assert_eq!(stats.total_events_delivered, 0);
    }

    #[tokio::test]
    async fn test_stats_aggregation() {
        let mgr = ConnectorManager::default();
        let id1 = mgr.create_connector(kafka_spec("kafka")).unwrap();
        let id2 = mgr.create_connector(nats_spec("nats")).unwrap();

        mgr.record_delivery(&id1, 100, 5000).await.unwrap();
        mgr.record_delivery(&id2, 50, 3000).await.unwrap();
        mgr.record_failure(&id2, "timeout".to_string())
            .await
            .unwrap();

        mgr.add_dead_letter(DeadLetterEntry {
            original_event_id: "evt-1".to_string(),
            connector_id: id2.clone(),
            error: "timeout".to_string(),
            attempts: 3,
            first_failed_at: Utc::now(),
            last_failed_at: Utc::now(),
            event_data: "{}".to_string(),
        });

        let stats = mgr.get_stats().await;
        assert_eq!(stats.total_connectors, 2);
        assert_eq!(stats.running_connectors, 1); // id2 is Failed
        assert_eq!(stats.failed_connectors, 1);
        assert_eq!(stats.total_events_delivered, 150);
        assert_eq!(stats.total_events_failed, 1);
        assert_eq!(stats.total_dead_letters, 1);
    }

    #[tokio::test]
    async fn test_stats_paused_connectors() {
        let mgr = ConnectorManager::default();
        let id1 = mgr.create_connector(kafka_spec("k1")).unwrap();
        let id2 = mgr.create_connector(nats_spec("n1")).unwrap();
        let _id3 = mgr.create_connector(pg_spec("p1")).unwrap();

        mgr.pause_connector(&id1).await.unwrap();
        mgr.pause_connector(&id2).await.unwrap();

        let stats = mgr.get_stats().await;
        assert_eq!(stats.total_connectors, 3);
        assert_eq!(stats.running_connectors, 1);
        assert_eq!(stats.paused_connectors, 2);
    }

    // -- Delivery guarantee modes --

    #[test]
    fn test_delivery_guarantee_default() {
        let dg = DeliveryGuarantee::default();
        assert_eq!(dg, DeliveryGuarantee::AtLeastOnce);
    }

    #[test]
    fn test_delivery_guarantee_serialization() {
        let guarantees = vec![
            DeliveryGuarantee::AtMostOnce,
            DeliveryGuarantee::AtLeastOnce,
            DeliveryGuarantee::ExactlyOnce,
        ];
        for g in &guarantees {
            let json = serde_json::to_string(g).unwrap();
            let parsed: DeliveryGuarantee = serde_json::from_str(&json).unwrap();
            assert_eq!(&parsed, g);
        }
    }

    #[tokio::test]
    async fn test_connector_preserves_delivery_guarantee() {
        let mgr = ConnectorManager::default();

        let mut spec = kafka_spec("kafka-eo");
        spec.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
        let id = mgr.create_connector(spec).unwrap();

        let list = mgr.list_connectors().await;
        let info = list.iter().find(|c| c.id == id).unwrap();
        assert_eq!(info.delivery_guarantee, DeliveryGuarantee::ExactlyOnce);
    }

    // -- Config validation --

    #[test]
    fn test_kafka_config_validation() {
        let empty_brokers = ConnectorConfig::Kafka {
            brokers: vec![],
            topic: "t".to_string(),
            client_id: "c".to_string(),
            compression: KafkaCompression::None,
            acks: KafkaAcks::One,
        };
        assert!(matches!(
            empty_brokers.validate(),
            Err(ConnectorError::ConfigInvalid(_))
        ));

        let empty_topic = ConnectorConfig::Kafka {
            brokers: vec!["b:9092".to_string()],
            topic: "".to_string(),
            client_id: "c".to_string(),
            compression: KafkaCompression::None,
            acks: KafkaAcks::One,
        };
        assert!(empty_topic.validate().is_err());

        let empty_client = ConnectorConfig::Kafka {
            brokers: vec!["b:9092".to_string()],
            topic: "t".to_string(),
            client_id: "".to_string(),
            compression: KafkaCompression::None,
            acks: KafkaAcks::One,
        };
        assert!(empty_client.validate().is_err());
    }

    #[test]
    fn test_nats_config_validation() {
        let empty_servers = ConnectorConfig::Nats {
            servers: vec![],
            subject: "s".to_string(),
            stream: None,
            credentials: None,
        };
        assert!(empty_servers.validate().is_err());

        let empty_subject = ConnectorConfig::Nats {
            servers: vec!["nats://localhost:4222".to_string()],
            subject: "".to_string(),
            stream: None,
            credentials: None,
        };
        assert!(empty_subject.validate().is_err());
    }

    #[test]
    fn test_pg_config_validation() {
        let empty_conn = ConnectorConfig::PostgreSQL {
            connection_string: "".to_string(),
            table: "t".to_string(),
            schema: "public".to_string(),
            mode: PgMode::Insert,
        };
        assert!(empty_conn.validate().is_err());

        let empty_table = ConnectorConfig::PostgreSQL {
            connection_string: "postgres://localhost/db".to_string(),
            table: "".to_string(),
            schema: "public".to_string(),
            mode: PgMode::Insert,
        };
        assert!(empty_table.validate().is_err());

        let empty_schema = ConnectorConfig::PostgreSQL {
            connection_string: "postgres://localhost/db".to_string(),
            table: "t".to_string(),
            schema: "".to_string(),
            mode: PgMode::Insert,
        };
        assert!(empty_schema.validate().is_err());
    }

    #[test]
    fn test_s3_config_validation() {
        let empty_bucket = ConnectorConfig::S3 {
            bucket: "".to_string(),
            prefix: "p/".to_string(),
            region: "us-east-1".to_string(),
            format: S3Format::Json,
            partition: S3Partition::Daily,
        };
        assert!(empty_bucket.validate().is_err());

        let empty_region = ConnectorConfig::S3 {
            bucket: "b".to_string(),
            prefix: "".to_string(),
            region: "".to_string(),
            format: S3Format::Json,
            partition: S3Partition::Daily,
        };
        assert!(empty_region.validate().is_err());
    }

    #[test]
    fn test_webhook_config_validation() {
        let empty_url = ConnectorConfig::Webhook {
            url: "".to_string(),
            method: "POST".to_string(),
            headers: HashMap::new(),
            auth: None,
        };
        assert!(empty_url.validate().is_err());

        let empty_method = ConnectorConfig::Webhook {
            url: "https://example.com".to_string(),
            method: "".to_string(),
            headers: HashMap::new(),
            auth: None,
        };
        assert!(empty_method.validate().is_err());
    }

    #[test]
    fn test_valid_configs_pass_validation() {
        let configs = vec![
            ConnectorConfig::Kafka {
                brokers: vec!["localhost:9092".to_string()],
                topic: "topic".to_string(),
                client_id: "client".to_string(),
                compression: KafkaCompression::Zstd,
                acks: KafkaAcks::All,
            },
            ConnectorConfig::Nats {
                servers: vec!["nats://localhost:4222".to_string()],
                subject: "events".to_string(),
                stream: Some("STREAM".to_string()),
                credentials: None,
            },
            ConnectorConfig::PostgreSQL {
                connection_string: "postgres://localhost/db".to_string(),
                table: "events".to_string(),
                schema: "public".to_string(),
                mode: PgMode::Upsert {
                    conflict_keys: vec!["id".to_string()],
                },
            },
            ConnectorConfig::S3 {
                bucket: "bucket".to_string(),
                prefix: "prefix/".to_string(),
                region: "eu-west-1".to_string(),
                format: S3Format::Parquet,
                partition: S3Partition::Hourly,
            },
            ConnectorConfig::Webhook {
                url: "https://example.com/hook".to_string(),
                method: "POST".to_string(),
                headers: HashMap::from([("X-Custom".to_string(), "val".to_string())]),
                auth: Some(WebhookAuth::Basic {
                    username: "user".to_string(),
                    password: "pass".to_string(),
                }),
            },
        ];
        for config in &configs {
            assert!(
                config.validate().is_ok(),
                "Config should be valid: {:?}",
                config
            );
        }
    }

    // -- Connector type name --

    #[test]
    fn test_connector_type_names() {
        assert_eq!(ConnectorType::Kafka.name(), "kafka");
        assert_eq!(ConnectorType::Nats.name(), "nats");
        assert_eq!(ConnectorType::PostgreSQL.name(), "postgresql");
        assert_eq!(ConnectorType::S3.name(), "s3");
        assert_eq!(ConnectorType::Webhook.name(), "webhook");
        assert_eq!(ConnectorType::Custom("redis".to_string()).name(), "redis");
    }

    // -- ConnectorState name --

    #[test]
    fn test_connector_state_names() {
        assert_eq!(ConnectorState::Running.name(), "running");
        assert_eq!(ConnectorState::Paused.name(), "paused");
        assert_eq!(ConnectorState::Failed("err".to_string()).name(), "failed");
        assert_eq!(ConnectorState::Starting.name(), "starting");
        assert_eq!(ConnectorState::Stopping.name(), "stopping");
    }

    // -- S3Format extension --

    #[test]
    fn test_s3_format_extensions() {
        assert_eq!(S3Format::Json.extension(), "json");
        assert_eq!(S3Format::Csv.extension(), "csv");
        assert_eq!(S3Format::Parquet.extension(), "parquet");
        assert_eq!(S3Format::Avro.extension(), "avro");
    }

    // -- Serialization round-trips --

    #[test]
    fn test_connector_config_serialization() {
        let config = ConnectorConfig::Kafka {
            brokers: vec!["localhost:9092".to_string()],
            topic: "test".to_string(),
            client_id: "ferrite".to_string(),
            compression: KafkaCompression::Lz4,
            acks: KafkaAcks::All,
        };
        let json = serde_json::to_string(&config).unwrap();
        let parsed: ConnectorConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.connector_type(), ConnectorType::Kafka);
    }

    #[test]
    fn test_connector_spec_serialization() {
        let spec = kafka_spec("test-kafka");
        let json = serde_json::to_string(&spec).unwrap();
        let parsed: ConnectorSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.name, "test-kafka");
        assert_eq!(parsed.connector_type, ConnectorType::Kafka);
    }

    #[test]
    fn test_webhook_auth_serialization() {
        let auths = vec![
            WebhookAuth::None,
            WebhookAuth::Bearer("token".to_string()),
            WebhookAuth::Basic {
                username: "u".to_string(),
                password: "p".to_string(),
            },
            WebhookAuth::ApiKey {
                header: "X-Key".to_string(),
                value: "secret".to_string(),
            },
        ];
        for auth in &auths {
            let json = serde_json::to_string(auth).unwrap();
            let parsed: WebhookAuth = serde_json::from_str(&json).unwrap();
            assert_eq!(&parsed, auth);
        }
    }

    // -- ConnectorId --

    #[test]
    fn test_connector_id_uniqueness() {
        let id1 = ConnectorId::new();
        let id2 = ConnectorId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_connector_id_display() {
        let id = ConnectorId::new();
        let display = format!("{}", id);
        assert!(!display.is_empty());
        // UUID format: 8-4-4-4-12 = 36 chars
        assert_eq!(display.len(), 36);
    }

    // -- Default config --

    #[test]
    fn test_connector_manager_config_defaults() {
        let config = ConnectorManagerConfig::default();
        assert_eq!(config.max_connectors, 100);
        assert_eq!(config.default_batch_size, 1000);
        assert_eq!(config.default_flush_interval_ms, 5000);
        assert!(config.enable_dead_letter);
        assert_eq!(config.max_retries, 3);
    }

    // -- Full lifecycle --

    #[tokio::test]
    async fn test_full_connector_lifecycle() {
        let mgr = ConnectorManager::default();

        // Create
        let id = mgr.create_connector(kafka_spec("lifecycle-test")).unwrap();
        let status = mgr.get_status(&id).await.unwrap();
        assert_eq!(status.state, ConnectorState::Running);

        // Deliver events
        mgr.record_delivery(&id, 5, 1000).await.unwrap();
        let status = mgr.get_status(&id).await.unwrap();
        assert_eq!(status.events_delivered, 5);

        // Pause
        mgr.pause_connector(&id).await.unwrap();
        let status = mgr.get_status(&id).await.unwrap();
        assert_eq!(status.state, ConnectorState::Paused);

        // Resume
        mgr.resume_connector(&id).await.unwrap();
        let status = mgr.get_status(&id).await.unwrap();
        assert_eq!(status.state, ConnectorState::Running);

        // Fail
        mgr.record_failure(&id, "network error".to_string())
            .await
            .unwrap();
        let status = mgr.get_status(&id).await.unwrap();
        assert!(matches!(status.state, ConnectorState::Failed(_)));
        assert_eq!(status.events_failed, 1);

        // Delete
        mgr.delete_connector(&id).await.unwrap();
        assert!(mgr.get_status(&id).await.is_none());
    }

    // -- Manager default --

    #[test]
    fn test_manager_default() {
        let mgr = ConnectorManager::default();
        assert!(mgr.is_empty());
        assert_eq!(mgr.len(), 0);
        assert_eq!(mgr.config().max_connectors, 100);
    }

    // -- Latency tracking --

    #[tokio::test]
    async fn test_avg_delivery_latency() {
        let mgr = ConnectorManager::default();
        let id = mgr.create_connector(kafka_spec("lat-test")).unwrap();

        // 10 events at 2000us each, 10 events at 4000us each
        mgr.record_delivery(&id, 10, 20_000).await.unwrap();
        mgr.record_delivery(&id, 10, 40_000).await.unwrap();

        let stats = mgr.get_stats().await;
        // (20000 + 40000) us / 20 samples / 1000 = 3.0 ms
        assert!((stats.avg_delivery_latency_ms - 3.0).abs() < 0.01);
    }
}
