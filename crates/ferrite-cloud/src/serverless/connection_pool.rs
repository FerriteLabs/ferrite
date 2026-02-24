//! Connection Pooling Proxy for Serverless Environments
//!
//! Solves the connection exhaustion problem common in serverless platforms
//! (AWS Lambda, Cloudflare Workers, Vercel Edge Functions) where each
//! invocation may open a new connection to the database.
//!
//! # Features
//!
//! - **Connection Pooling**: Reuse connections across serverless invocations
//! - **Idle Eviction**: Automatically reclaim idle connections
//! - **Multiplexing**: Share connections across concurrent requests
//! - **Health Checks**: Monitor pool health and degrade gracefully
//! - **Event Bridge**: Route data mutation events to serverless targets
//!
//! # Example
//!
//! ```ignore
//! use ferrite_cloud::serverless::connection_pool::{ServerlessConnectionPool, PoolConfig};
//!
//! let pool = ServerlessConnectionPool::new(PoolConfig::default());
//! let conn = pool.acquire("lambda-abc123").await?;
//! // ... use connection ...
//! pool.release(conn);
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Serde helpers for `Uuid` (the workspace `uuid` crate has no `serde` feature).
mod uuid_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use uuid::Uuid;

    pub fn serialize<S: Serializer>(val: &Uuid, s: S) -> Result<S::Ok, S::Error> {
        val.to_string().serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Uuid, D::Error> {
        let s = String::deserialize(d)?;
        Uuid::parse_str(&s).map_err(serde::de::Error::custom)
    }
}

/// Serde helpers for `Option<Vec<u8>>` (kept as base64 in JSON for ergonomics).
mod opt_bytes_serde {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(val: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
        val.as_ref().map(|b| STANDARD.encode(b)).serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<u8>>, D::Error> {
        let opt: Option<String> = Option::deserialize(d)?;
        match opt {
            Some(s) => STANDARD
                .decode(&s)
                .map(Some)
                .map_err(serde::de::Error::custom),
            None => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors returned by the connection pool and event bridge.
#[derive(Debug, Clone, thiserror::Error)]
pub enum PoolError {
    /// Timed out waiting for a connection
    #[error("connection acquisition timed out")]
    ConnectionTimeout,

    /// Pool has no available connections
    #[error("connection pool exhausted")]
    PoolExhausted,

    /// Attempted to use a closed connection
    #[error("connection is closed")]
    ConnectionClosed,

    /// Event target not found
    #[error("event target not found")]
    TargetNotFound,

    /// Event dispatch failed
    #[error("event dispatch failed: {0}")]
    DispatchFailed(String),

    /// Invalid configuration
    #[error("invalid configuration: {0}")]
    ConfigInvalid(String),

    /// Internal error
    #[error("internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Connection pool types
// ---------------------------------------------------------------------------

/// Configuration for the serverless connection pool.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PoolConfig {
    /// Maximum number of connections in the pool
    pub max_connections: usize,
    /// Minimum number of idle connections to maintain
    pub min_idle: usize,
    /// Maximum time a connection can remain idle before eviction (ms)
    pub max_idle_time_ms: u64,
    /// Timeout for acquiring a connection (ms)
    pub connection_timeout_ms: u64,
    /// Maximum lifetime of a connection (ms)
    pub max_lifetime_ms: u64,
    /// Interval between health checks (ms)
    pub health_check_interval_ms: u64,
    /// Enable connection multiplexing across concurrent requests
    pub enable_multiplexing: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 1000,
            min_idle: 10,
            max_idle_time_ms: 30_000,
            connection_timeout_ms: 5_000,
            max_lifetime_ms: 300_000,
            health_check_interval_ms: 10_000,
            enable_multiplexing: true,
        }
    }
}

impl PoolConfig {
    /// Validate the configuration, returning an error if invalid.
    pub fn validate(&self) -> Result<(), PoolError> {
        if self.max_connections == 0 {
            return Err(PoolError::ConfigInvalid(
                "max_connections must be > 0".to_string(),
            ));
        }
        if self.min_idle > self.max_connections {
            return Err(PoolError::ConfigInvalid(
                "min_idle must be <= max_connections".to_string(),
            ));
        }
        if self.connection_timeout_ms == 0 {
            return Err(PoolError::ConfigInvalid(
                "connection_timeout_ms must be > 0".to_string(),
            ));
        }
        if self.max_lifetime_ms == 0 {
            return Err(PoolError::ConfigInvalid(
                "max_lifetime_ms must be > 0".to_string(),
            ));
        }
        Ok(())
    }
}

/// State of a pooled connection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionState {
    /// Connection is idle and available for use
    Idle,
    /// Connection is actively serving a request
    Active,
    /// Connection is draining (finishing current work before close)
    Draining,
    /// Connection is closed
    Closed,
}

/// A connection managed by the pool.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PooledConnection {
    /// Unique connection identifier
    #[serde(with = "uuid_serde")]
    pub id: Uuid,
    /// Client that owns this connection
    pub client_id: String,
    /// When the connection was created
    pub created_at: DateTime<Utc>,
    /// When the connection was last used
    pub last_used: DateTime<Utc>,
    /// Current state
    pub state: ConnectionState,
    /// Number of requests this connection has served
    pub requests_served: u64,
}

/// Health status of the pool.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PoolHealth {
    /// Total connections (all states)
    pub total_connections: usize,
    /// Active connections
    pub active_connections: usize,
    /// Idle connections
    pub idle_connections: usize,
    /// Pending acquire requests
    pub pending_requests: usize,
    /// Overall health status
    pub health_status: HealthStatus,
}

/// Overall health classification.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HealthStatus {
    /// Pool is operating normally
    Healthy,
    /// Pool is under pressure but functional
    Degraded { reason: String },
    /// Pool is unable to serve requests
    Unhealthy { reason: String },
}

/// Cumulative statistics for the pool.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PoolStats {
    /// Total connections acquired
    pub total_acquired: u64,
    /// Total connections released
    pub total_released: u64,
    /// Total connections created
    pub total_created: u64,
    /// Total connections destroyed
    pub total_destroyed: u64,
    /// Total acquisition timeouts
    pub total_timeouts: u64,
    /// Total idle evictions
    pub total_evictions: u64,
    /// Average wait time to acquire a connection (µs)
    pub avg_wait_time_us: u64,
    /// Average connection lifetime (ms)
    pub avg_lifetime_ms: u64,
    /// Peak concurrent connections observed
    pub peak_connections: usize,
    /// Current number of connections
    pub current_connections: usize,
}

// ---------------------------------------------------------------------------
// ServerlessConnectionPool
// ---------------------------------------------------------------------------

/// Atomic counters for pool statistics.
#[derive(Debug, Default)]
struct AtomicPoolStats {
    total_acquired: AtomicU64,
    total_released: AtomicU64,
    total_created: AtomicU64,
    total_destroyed: AtomicU64,
    total_timeouts: AtomicU64,
    total_evictions: AtomicU64,
    total_wait_time_us: AtomicU64,
    total_lifetime_ms: AtomicU64,
    peak_connections: AtomicU64,
}

/// Connection pool designed for serverless environments.
///
/// Maintains a set of reusable connections to prevent exhaustion when
/// many short-lived serverless function invocations connect to Ferrite.
pub struct ServerlessConnectionPool {
    /// Pool configuration
    config: PoolConfig,
    /// Active connections keyed by connection id
    connections: DashMap<Uuid, PooledConnection>,
    /// Pending acquire count
    pending_requests: AtomicU64,
    /// Atomic statistics
    stats: Arc<AtomicPoolStats>,
}

impl ServerlessConnectionPool {
    /// Create a new connection pool with the given configuration.
    pub fn new(config: PoolConfig) -> Self {
        Self {
            config,
            connections: DashMap::new(),
            pending_requests: AtomicU64::new(0),
            stats: Arc::new(AtomicPoolStats::default()),
        }
    }

    /// Acquire a connection for the given client.
    ///
    /// If an idle connection exists it will be reused; otherwise a new
    /// connection is created provided the pool has not reached its limit.
    pub async fn acquire(&self, client_id: &str) -> Result<PooledConnection, PoolError> {
        self.pending_requests.fetch_add(1, Ordering::Relaxed);
        let start = Utc::now();

        let result = self.try_acquire(client_id);

        self.pending_requests.fetch_sub(1, Ordering::Relaxed);

        let wait_us = (Utc::now() - start).num_microseconds().unwrap_or(0) as u64;
        self.stats
            .total_wait_time_us
            .fetch_add(wait_us, Ordering::Relaxed);

        match result {
            Ok(conn) => {
                self.stats.total_acquired.fetch_add(1, Ordering::Relaxed);
                debug!(client_id, conn_id = %conn.id, "connection acquired");
                Ok(conn)
            }
            Err(e) => {
                if matches!(e, PoolError::ConnectionTimeout | PoolError::PoolExhausted) {
                    self.stats.total_timeouts.fetch_add(1, Ordering::Relaxed);
                }
                Err(e)
            }
        }
    }

    /// Try to acquire or create a connection synchronously.
    fn try_acquire(&self, client_id: &str) -> Result<PooledConnection, PoolError> {
        let now = Utc::now();

        // If multiplexing is enabled, look for an idle connection from the
        // same client first, then any idle connection.
        if self.config.enable_multiplexing {
            // Prefer same-client idle connection
            let reused = self.find_idle_connection(Some(client_id));
            if let Some(mut conn) = reused {
                conn.state = ConnectionState::Active;
                conn.last_used = now;
                conn.requests_served += 1;
                self.connections.insert(conn.id, conn.clone());
                return Ok(conn);
            }
        }

        // Try any idle connection
        let reused = self.find_idle_connection(None);
        if let Some(mut conn) = reused {
            conn.state = ConnectionState::Active;
            conn.client_id = client_id.to_string();
            conn.last_used = now;
            conn.requests_served += 1;
            self.connections.insert(conn.id, conn.clone());
            return Ok(conn);
        }

        // Create a new connection if below limit
        if self.connections.len() < self.config.max_connections {
            let conn = PooledConnection {
                id: Uuid::new_v4(),
                client_id: client_id.to_string(),
                created_at: now,
                last_used: now,
                state: ConnectionState::Active,
                requests_served: 1,
            };
            self.connections.insert(conn.id, conn.clone());
            self.stats.total_created.fetch_add(1, Ordering::Relaxed);

            let current = self.connections.len() as u64;
            let mut peak = self.stats.peak_connections.load(Ordering::Relaxed);
            while current > peak {
                match self.stats.peak_connections.compare_exchange_weak(
                    peak,
                    current,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => peak = actual,
                }
            }

            return Ok(conn);
        }

        Err(PoolError::PoolExhausted)
    }

    /// Find an idle connection, optionally preferring a specific client.
    fn find_idle_connection(&self, preferred_client: Option<&str>) -> Option<PooledConnection> {
        let now = Utc::now();
        let max_idle = chrono::Duration::milliseconds(self.config.max_idle_time_ms as i64);
        let max_lifetime = chrono::Duration::milliseconds(self.config.max_lifetime_ms as i64);

        let mut best: Option<PooledConnection> = None;

        for entry in self.connections.iter() {
            let conn = entry.value();
            if conn.state != ConnectionState::Idle {
                continue;
            }
            // Skip expired connections
            if now - conn.last_used >= max_idle || now - conn.created_at >= max_lifetime {
                continue;
            }
            if let Some(cid) = preferred_client {
                if conn.client_id == cid {
                    return Some(conn.clone());
                }
            }
            if best.is_none() {
                best = Some(conn.clone());
            }
        }

        best
    }

    /// Release a connection back to the pool.
    pub fn release(&self, conn: PooledConnection) {
        if let Some(mut entry) = self.connections.get_mut(&conn.id) {
            entry.state = ConnectionState::Idle;
            entry.last_used = Utc::now();
            self.stats.total_released.fetch_add(1, Ordering::Relaxed);
            debug!(conn_id = %conn.id, "connection released");
        }
    }

    /// Perform a health check on the pool.
    pub async fn health_check(&self) -> PoolHealth {
        let mut total = 0usize;
        let mut active = 0usize;
        let mut idle = 0usize;

        for entry in self.connections.iter() {
            total += 1;
            match entry.value().state {
                ConnectionState::Active => active += 1,
                ConnectionState::Idle => idle += 1,
                _ => {}
            }
        }

        let pending = self.pending_requests.load(Ordering::Relaxed) as usize;

        let health_status = if total == 0 && pending == 0 {
            HealthStatus::Healthy
        } else if active as f64 / self.config.max_connections as f64 > 0.95 {
            HealthStatus::Unhealthy {
                reason: format!(
                    "pool near capacity: {}/{} connections active",
                    active, self.config.max_connections
                ),
            }
        } else if active as f64 / self.config.max_connections as f64 > 0.75 {
            HealthStatus::Degraded {
                reason: format!(
                    "pool under pressure: {}/{} connections active",
                    active, self.config.max_connections
                ),
            }
        } else {
            HealthStatus::Healthy
        };

        PoolHealth {
            total_connections: total,
            active_connections: active,
            idle_connections: idle,
            pending_requests: pending,
            health_status,
        }
    }

    /// Evict idle connections that have exceeded `max_idle_time_ms`.
    ///
    /// Returns the number of connections evicted.
    pub fn evict_idle(&self) -> usize {
        let now = Utc::now();
        let max_idle = chrono::Duration::milliseconds(self.config.max_idle_time_ms as i64);
        let max_lifetime = chrono::Duration::milliseconds(self.config.max_lifetime_ms as i64);

        let mut evicted: Vec<Uuid> = Vec::new();

        for entry in self.connections.iter() {
            let conn = entry.value();
            if conn.state != ConnectionState::Idle {
                continue;
            }
            let idle_expired = now - conn.last_used >= max_idle;
            let lifetime_expired = now - conn.created_at >= max_lifetime;
            // Keep at least min_idle connections
            let above_min =
                self.connections.len().saturating_sub(evicted.len()) > self.config.min_idle;

            if (idle_expired || lifetime_expired) && above_min {
                evicted.push(*entry.key());
            }
        }

        let count = evicted.len();
        for id in &evicted {
            if let Some((_, conn)) = self.connections.remove(id) {
                let lifetime_ms = (now - conn.created_at).num_milliseconds().max(0) as u64;
                self.stats
                    .total_lifetime_ms
                    .fetch_add(lifetime_ms, Ordering::Relaxed);
                self.stats.total_destroyed.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.stats
            .total_evictions
            .fetch_add(count as u64, Ordering::Relaxed);

        if count > 0 {
            info!(count, "evicted idle connections");
        }

        count
    }

    /// Return a snapshot of pool statistics.
    pub fn get_stats(&self) -> PoolStats {
        let total_acquired = self.stats.total_acquired.load(Ordering::Relaxed);
        let total_created = self.stats.total_created.load(Ordering::Relaxed);
        let total_destroyed = self.stats.total_destroyed.load(Ordering::Relaxed);
        let total_lifetime = self.stats.total_lifetime_ms.load(Ordering::Relaxed);
        let total_wait = self.stats.total_wait_time_us.load(Ordering::Relaxed);

        PoolStats {
            total_acquired,
            total_released: self.stats.total_released.load(Ordering::Relaxed),
            total_created,
            total_destroyed,
            total_timeouts: self.stats.total_timeouts.load(Ordering::Relaxed),
            total_evictions: self.stats.total_evictions.load(Ordering::Relaxed),
            avg_wait_time_us: if total_acquired > 0 {
                total_wait / total_acquired
            } else {
                0
            },
            avg_lifetime_ms: if total_destroyed > 0 {
                total_lifetime / total_destroyed
            } else {
                0
            },
            peak_connections: self.stats.peak_connections.load(Ordering::Relaxed) as usize,
            current_connections: self.connections.len(),
        }
    }
}

// ---------------------------------------------------------------------------
// Event bridge types
// ---------------------------------------------------------------------------

/// Unique identifier for an event target.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TargetId(#[serde(with = "uuid_serde")] pub Uuid);

impl TargetId {
    /// Create a new random target id.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for TargetId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for TargetId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Type of event that occurred on a key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventType {
    /// A key was set
    KeySet,
    /// A key was deleted
    KeyDelete,
    /// A key expired
    KeyExpire,
    /// An entry was added to a stream
    StreamAdd,
    /// A message was published to a pub/sub channel
    PubSubPublish,
}

/// A data mutation event.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataEvent {
    /// Unique event identifier
    #[serde(with = "uuid_serde")]
    pub event_id: Uuid,
    /// Type of event
    pub event_type: EventType,
    /// Key that was affected
    pub key: String,
    /// Optional value payload
    #[serde(with = "opt_bytes_serde")]
    pub value: Option<Vec<u8>>,
    /// When the event occurred
    pub timestamp: DateTime<Utc>,
    /// Arbitrary metadata
    pub metadata: HashMap<String, String>,
}

/// Filter for selecting which events a target receives.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventFilter {
    /// Key patterns to match (glob-style)
    pub key_patterns: Vec<String>,
    /// Event types to match
    pub event_types: Vec<EventType>,
    /// Minimum batch size before dispatching
    pub min_batch_size: Option<usize>,
}

impl EventFilter {
    /// Check whether an event matches this filter.
    pub fn matches(&self, event: &DataEvent) -> bool {
        let type_match =
            self.event_types.is_empty() || self.event_types.contains(&event.event_type);

        let key_match = self.key_patterns.is_empty()
            || self.key_patterns.iter().any(|p| {
                if p.ends_with('*') {
                    let prefix = &p[..p.len() - 1];
                    event.key.starts_with(prefix)
                } else {
                    *p == event.key
                }
            });

        type_match && key_match
    }
}

/// Type of serverless target.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TargetType {
    /// AWS Lambda function
    AwsLambda,
    /// Cloudflare Worker
    CloudflareWorker,
    /// Vercel Edge Function
    VercelEdge,
    /// Generic webhook
    Webhook,
    /// Custom target type
    Custom(String),
}

/// Authentication for an event target.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TargetAuth {
    /// No authentication
    None,
    /// Bearer token
    Bearer(String),
    /// AWS IAM role
    AwsIam { role_arn: String },
    /// API key
    ApiKey(String),
}

/// Registration payload for an event target.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventTarget {
    /// Human-readable name
    pub name: String,
    /// Target type
    pub target_type: TargetType,
    /// Endpoint URL
    pub endpoint: String,
    /// Optional event filter
    pub filter: Option<EventFilter>,
    /// Optional authentication
    pub auth: Option<TargetAuth>,
}

/// Current status of a registered target.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TargetStatus {
    /// Target is active and receiving events
    Active,
    /// Target is paused
    Paused,
    /// Target is in an error state
    Error(String),
}

/// Information about a registered event target.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventTargetInfo {
    /// Target identifier
    pub id: TargetId,
    /// Human-readable name
    pub name: String,
    /// Target type
    pub target_type: TargetType,
    /// Total events dispatched to this target
    pub events_dispatched: u64,
    /// Last dispatch timestamp
    pub last_dispatch: Option<DateTime<Utc>>,
    /// Current status
    pub status: TargetStatus,
}

/// Result of dispatching an event.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DispatchResult {
    /// The event that was dispatched
    #[serde(with = "uuid_serde")]
    pub event_id: Uuid,
    /// Number of targets that were successfully notified
    pub targets_notified: usize,
    /// Number of targets that failed
    pub targets_failed: usize,
    /// Total dispatch latency (ms)
    pub latency_ms: u64,
}

/// Configuration for the event bridge.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventBridgeConfig {
    /// Maximum number of targets
    pub max_targets: usize,
    /// Batch size for event delivery
    pub batch_size: usize,
    /// Flush interval (ms)
    pub flush_interval_ms: u64,
    /// Number of retry attempts for failed dispatches
    pub retry_attempts: u32,
    /// Enable dead-letter queue for failed events
    pub dead_letter_enabled: bool,
}

impl Default for EventBridgeConfig {
    fn default() -> Self {
        Self {
            max_targets: 100,
            batch_size: 10,
            flush_interval_ms: 1000,
            retry_attempts: 3,
            dead_letter_enabled: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Internal target record
// ---------------------------------------------------------------------------

/// Internal state for a registered target.
struct RegisteredTarget {
    id: TargetId,
    target: EventTarget,
    events_dispatched: AtomicU64,
    last_dispatch: RwLock<Option<DateTime<Utc>>>,
    status: RwLock<TargetStatus>,
}

// ---------------------------------------------------------------------------
// EventBridge
// ---------------------------------------------------------------------------

/// Routes data mutation events to registered serverless function targets.
pub struct EventBridge {
    /// Configuration
    config: EventBridgeConfig,
    /// Registered targets keyed by id
    targets: DashMap<TargetId, Arc<RegisteredTarget>>,
}

impl EventBridge {
    /// Create a new event bridge.
    pub fn new(config: EventBridgeConfig) -> Self {
        Self {
            config,
            targets: DashMap::new(),
        }
    }

    /// Register a new event target.
    ///
    /// Returns the assigned [`TargetId`] on success.
    pub fn register_target(&self, target: EventTarget) -> Result<TargetId, PoolError> {
        if self.targets.len() >= self.config.max_targets {
            return Err(PoolError::ConfigInvalid(format!(
                "maximum targets ({}) reached",
                self.config.max_targets
            )));
        }

        if target.name.is_empty() {
            return Err(PoolError::ConfigInvalid(
                "target name must not be empty".to_string(),
            ));
        }

        if target.endpoint.is_empty() {
            return Err(PoolError::ConfigInvalid(
                "target endpoint must not be empty".to_string(),
            ));
        }

        let id = TargetId::new();
        let record = Arc::new(RegisteredTarget {
            id: id.clone(),
            target,
            events_dispatched: AtomicU64::new(0),
            last_dispatch: RwLock::new(None),
            status: RwLock::new(TargetStatus::Active),
        });

        self.targets.insert(id.clone(), record);
        info!(%id, "registered event target");
        Ok(id)
    }

    /// Dispatch a data event to all matching targets.
    pub async fn dispatch(&self, event: DataEvent) -> Result<DispatchResult, PoolError> {
        let start = Utc::now();
        let mut notified = 0usize;
        let mut failed = 0usize;

        for entry in self.targets.iter() {
            let record = entry.value();

            // Check filter
            if let Some(ref filter) = record.target.filter {
                if !filter.matches(&event) {
                    continue;
                }
            }

            // Simulate dispatch (in production this would make an HTTP call
            // or invoke the target via the appropriate SDK)
            let success = self.dispatch_to_target(record, &event).await;

            if success {
                notified += 1;
                record.events_dispatched.fetch_add(1, Ordering::Relaxed);
                *record.last_dispatch.write() = Some(Utc::now());
            } else {
                failed += 1;
                *record.status.write() = TargetStatus::Error("dispatch failed".to_string());
                warn!(target = %record.id, "event dispatch failed");
            }
        }

        let latency_ms = (Utc::now() - start).num_milliseconds().max(0) as u64;

        Ok(DispatchResult {
            event_id: event.event_id,
            targets_notified: notified,
            targets_failed: failed,
            latency_ms,
        })
    }

    /// Simulate dispatching to a single target.
    async fn dispatch_to_target(&self, _target: &RegisteredTarget, _event: &DataEvent) -> bool {
        // Placeholder — real implementation would use reqwest / AWS SDK / etc.
        true
    }

    /// List all registered targets with their current info.
    pub fn list_targets(&self) -> Vec<EventTargetInfo> {
        self.targets
            .iter()
            .map(|entry| {
                let r = entry.value();
                EventTargetInfo {
                    id: r.id.clone(),
                    name: r.target.name.clone(),
                    target_type: r.target.target_type.clone(),
                    events_dispatched: r.events_dispatched.load(Ordering::Relaxed),
                    last_dispatch: *r.last_dispatch.read(),
                    status: r.status.read().clone(),
                }
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Pool config validation ----------------------------------------

    #[test]
    fn test_pool_config_default_is_valid() {
        let config = PoolConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_pool_config_invalid_max_connections() {
        let config = PoolConfig {
            max_connections: 0,
            ..Default::default()
        };
        assert!(matches!(
            config.validate(),
            Err(PoolError::ConfigInvalid(_))
        ));
    }

    #[test]
    fn test_pool_config_invalid_min_idle() {
        let config = PoolConfig {
            min_idle: 2000,
            max_connections: 100,
            ..Default::default()
        };
        assert!(matches!(
            config.validate(),
            Err(PoolError::ConfigInvalid(_))
        ));
    }

    #[test]
    fn test_pool_config_invalid_timeout() {
        let config = PoolConfig {
            connection_timeout_ms: 0,
            ..Default::default()
        };
        assert!(matches!(
            config.validate(),
            Err(PoolError::ConfigInvalid(_))
        ));
    }

    #[test]
    fn test_pool_config_invalid_lifetime() {
        let config = PoolConfig {
            max_lifetime_ms: 0,
            ..Default::default()
        };
        assert!(matches!(
            config.validate(),
            Err(PoolError::ConfigInvalid(_))
        ));
    }

    // -- Connection acquire / release ----------------------------------

    #[tokio::test]
    async fn test_acquire_creates_connection() {
        let pool = ServerlessConnectionPool::new(PoolConfig::default());

        let conn = pool.acquire("client-1").await.unwrap();
        assert_eq!(conn.client_id, "client-1");
        assert_eq!(conn.state, ConnectionState::Active);
        assert_eq!(conn.requests_served, 1);
    }

    #[tokio::test]
    async fn test_acquire_release_reuse() {
        let pool = ServerlessConnectionPool::new(PoolConfig::default());

        let conn = pool.acquire("client-1").await.unwrap();
        let id = conn.id;
        pool.release(conn);

        let conn2 = pool.acquire("client-1").await.unwrap();
        // Should reuse the same connection
        assert_eq!(conn2.id, id);
        assert_eq!(conn2.requests_served, 2);
    }

    #[tokio::test]
    async fn test_acquire_pool_exhausted() {
        let config = PoolConfig {
            max_connections: 1,
            ..Default::default()
        };
        let pool = ServerlessConnectionPool::new(config);

        let _conn = pool.acquire("client-1").await.unwrap();
        let result = pool.acquire("client-2").await;
        assert!(matches!(result, Err(PoolError::PoolExhausted)));
    }

    // -- Idle eviction -------------------------------------------------

    #[tokio::test]
    async fn test_evict_idle_connections() {
        let config = PoolConfig {
            max_idle_time_ms: 0, // Immediately idle-expire
            min_idle: 0,
            ..Default::default()
        };
        let pool = ServerlessConnectionPool::new(config);

        let conn = pool.acquire("client-1").await.unwrap();
        pool.release(conn);

        let evicted = pool.evict_idle();
        assert_eq!(evicted, 1);
        assert_eq!(pool.get_stats().current_connections, 0);
    }

    #[tokio::test]
    async fn test_evict_respects_min_idle() {
        let config = PoolConfig {
            max_idle_time_ms: 0,
            min_idle: 1,
            ..Default::default()
        };
        let pool = ServerlessConnectionPool::new(config);

        let conn = pool.acquire("client-1").await.unwrap();
        pool.release(conn);

        let evicted = pool.evict_idle();
        // min_idle = 1, so the single idle connection is kept
        assert_eq!(evicted, 0);
    }

    // -- Pool stats ----------------------------------------------------

    #[tokio::test]
    async fn test_pool_stats_tracking() {
        let pool = ServerlessConnectionPool::new(PoolConfig::default());

        let conn1 = pool.acquire("c1").await.unwrap();
        let conn2 = pool.acquire("c2").await.unwrap();
        pool.release(conn1);
        pool.release(conn2);

        let stats = pool.get_stats();
        assert_eq!(stats.total_acquired, 2);
        assert_eq!(stats.total_released, 2);
        assert_eq!(stats.total_created, 2);
        assert_eq!(stats.current_connections, 2);
        assert!(stats.peak_connections >= 2);
    }

    #[tokio::test]
    async fn test_pool_stats_timeout_tracking() {
        let config = PoolConfig {
            max_connections: 1,
            ..Default::default()
        };
        let pool = ServerlessConnectionPool::new(config);

        let _conn = pool.acquire("c1").await.unwrap();
        let _ = pool.acquire("c2").await; // will fail

        let stats = pool.get_stats();
        assert_eq!(stats.total_timeouts, 1);
    }

    // -- Health check --------------------------------------------------

    #[tokio::test]
    async fn test_health_check_healthy() {
        let pool = ServerlessConnectionPool::new(PoolConfig::default());
        let health = pool.health_check().await;

        assert_eq!(health.total_connections, 0);
        assert!(matches!(health.health_status, HealthStatus::Healthy));
    }

    #[tokio::test]
    async fn test_health_check_with_connections() {
        let pool = ServerlessConnectionPool::new(PoolConfig::default());

        let conn = pool.acquire("c1").await.unwrap();
        let health = pool.health_check().await;

        assert_eq!(health.total_connections, 1);
        assert_eq!(health.active_connections, 1);
        assert_eq!(health.idle_connections, 0);

        pool.release(conn);
        let health = pool.health_check().await;
        assert_eq!(health.idle_connections, 1);
        assert_eq!(health.active_connections, 0);
    }

    // -- Event target registration -------------------------------------

    #[test]
    fn test_register_target() {
        let bridge = EventBridge::new(EventBridgeConfig::default());

        let target = EventTarget {
            name: "my-lambda".to_string(),
            target_type: TargetType::AwsLambda,
            endpoint: "arn:aws:lambda:us-east-1:123456789:function:my-func".to_string(),
            filter: None,
            auth: Some(TargetAuth::AwsIam {
                role_arn: "arn:aws:iam::123456789:role/my-role".to_string(),
            }),
        };

        let id = bridge.register_target(target).unwrap();
        let targets = bridge.list_targets();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].id, id);
        assert_eq!(targets[0].name, "my-lambda");
    }

    #[test]
    fn test_register_target_empty_name() {
        let bridge = EventBridge::new(EventBridgeConfig::default());

        let target = EventTarget {
            name: String::new(),
            target_type: TargetType::Webhook,
            endpoint: "https://example.com/hook".to_string(),
            filter: None,
            auth: None,
        };

        assert!(matches!(
            bridge.register_target(target),
            Err(PoolError::ConfigInvalid(_))
        ));
    }

    #[test]
    fn test_register_target_empty_endpoint() {
        let bridge = EventBridge::new(EventBridgeConfig::default());

        let target = EventTarget {
            name: "bad-target".to_string(),
            target_type: TargetType::Webhook,
            endpoint: String::new(),
            filter: None,
            auth: None,
        };

        assert!(matches!(
            bridge.register_target(target),
            Err(PoolError::ConfigInvalid(_))
        ));
    }

    #[test]
    fn test_register_target_max_limit() {
        let config = EventBridgeConfig {
            max_targets: 1,
            ..Default::default()
        };
        let bridge = EventBridge::new(config);

        let target1 = EventTarget {
            name: "t1".to_string(),
            target_type: TargetType::Webhook,
            endpoint: "https://example.com/1".to_string(),
            filter: None,
            auth: None,
        };
        let target2 = EventTarget {
            name: "t2".to_string(),
            target_type: TargetType::Webhook,
            endpoint: "https://example.com/2".to_string(),
            filter: None,
            auth: None,
        };

        bridge.register_target(target1).unwrap();
        assert!(matches!(
            bridge.register_target(target2),
            Err(PoolError::ConfigInvalid(_))
        ));
    }

    // -- Event dispatch ------------------------------------------------

    #[tokio::test]
    async fn test_dispatch_event() {
        let bridge = EventBridge::new(EventBridgeConfig::default());

        let target = EventTarget {
            name: "worker".to_string(),
            target_type: TargetType::CloudflareWorker,
            endpoint: "https://my-worker.workers.dev".to_string(),
            filter: None,
            auth: Some(TargetAuth::Bearer("tok_123".to_string())),
        };
        bridge.register_target(target).unwrap();

        let event = DataEvent {
            event_id: Uuid::new_v4(),
            event_type: EventType::KeySet,
            key: "user:42".to_string(),
            value: Some(b"data".to_vec()),
            timestamp: Utc::now(),
            metadata: HashMap::new(),
        };

        let result = bridge.dispatch(event).await.unwrap();
        assert_eq!(result.targets_notified, 1);
        assert_eq!(result.targets_failed, 0);
    }

    #[tokio::test]
    async fn test_dispatch_with_filter_match() {
        let bridge = EventBridge::new(EventBridgeConfig::default());

        let target = EventTarget {
            name: "filtered".to_string(),
            target_type: TargetType::VercelEdge,
            endpoint: "https://my-edge.vercel.app/api/hook".to_string(),
            filter: Some(EventFilter {
                key_patterns: vec!["orders:*".to_string()],
                event_types: vec![EventType::KeySet],
                min_batch_size: None,
            }),
            auth: None,
        };
        bridge.register_target(target).unwrap();

        // Matching event
        let event = DataEvent {
            event_id: Uuid::new_v4(),
            event_type: EventType::KeySet,
            key: "orders:123".to_string(),
            value: None,
            timestamp: Utc::now(),
            metadata: HashMap::new(),
        };
        let result = bridge.dispatch(event).await.unwrap();
        assert_eq!(result.targets_notified, 1);

        // Non-matching event
        let event = DataEvent {
            event_id: Uuid::new_v4(),
            event_type: EventType::KeyDelete,
            key: "users:456".to_string(),
            value: None,
            timestamp: Utc::now(),
            metadata: HashMap::new(),
        };
        let result = bridge.dispatch(event).await.unwrap();
        assert_eq!(result.targets_notified, 0);
    }

    #[tokio::test]
    async fn test_dispatch_updates_target_info() {
        let bridge = EventBridge::new(EventBridgeConfig::default());

        let target = EventTarget {
            name: "counter".to_string(),
            target_type: TargetType::Webhook,
            endpoint: "https://example.com/hook".to_string(),
            filter: None,
            auth: None,
        };
        bridge.register_target(target).unwrap();

        for _ in 0..3 {
            let event = DataEvent {
                event_id: Uuid::new_v4(),
                event_type: EventType::StreamAdd,
                key: "stream:logs".to_string(),
                value: None,
                timestamp: Utc::now(),
                metadata: HashMap::new(),
            };
            bridge.dispatch(event).await.unwrap();
        }

        let targets = bridge.list_targets();
        assert_eq!(targets[0].events_dispatched, 3);
        assert!(targets[0].last_dispatch.is_some());
    }

    // -- Event filter --------------------------------------------------

    #[test]
    fn test_event_filter_matches_type() {
        let filter = EventFilter {
            key_patterns: vec![],
            event_types: vec![EventType::KeySet, EventType::KeyDelete],
            min_batch_size: None,
        };

        let event = DataEvent {
            event_id: Uuid::new_v4(),
            event_type: EventType::KeySet,
            key: "any".to_string(),
            value: None,
            timestamp: Utc::now(),
            metadata: HashMap::new(),
        };
        assert!(filter.matches(&event));

        let event2 = DataEvent {
            event_type: EventType::PubSubPublish,
            ..event.clone()
        };
        assert!(!filter.matches(&event2));
    }

    #[test]
    fn test_event_filter_matches_key_pattern() {
        let filter = EventFilter {
            key_patterns: vec!["user:*".to_string(), "session:abc".to_string()],
            event_types: vec![],
            min_batch_size: None,
        };

        let event = DataEvent {
            event_id: Uuid::new_v4(),
            event_type: EventType::KeySet,
            key: "user:42".to_string(),
            value: None,
            timestamp: Utc::now(),
            metadata: HashMap::new(),
        };
        assert!(filter.matches(&event));

        let event2 = DataEvent {
            key: "session:abc".to_string(),
            ..event.clone()
        };
        assert!(filter.matches(&event2));

        let event3 = DataEvent {
            key: "other:key".to_string(),
            ..event.clone()
        };
        assert!(!filter.matches(&event3));
    }

    // -- Target types --------------------------------------------------

    #[test]
    fn test_target_type_variants() {
        let types = vec![
            TargetType::AwsLambda,
            TargetType::CloudflareWorker,
            TargetType::VercelEdge,
            TargetType::Webhook,
            TargetType::Custom("my-platform".to_string()),
        ];
        assert_eq!(types.len(), 5);
    }

    #[test]
    fn test_target_id_display() {
        let id = TargetId::new();
        let s = id.to_string();
        assert!(!s.is_empty());
    }
}
