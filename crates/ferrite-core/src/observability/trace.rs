//! Distributed Tracing Support
//!
//! Session-based tracing for debugging and performance analysis.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use uuid::Uuid;

/// Tracer for managing trace sessions
pub struct Tracer {
    sessions: RwLock<HashMap<String, Arc<TraceSession>>>,
    max_events_per_session: usize,
    max_sessions: usize,
    session_counter: AtomicU64,
}

impl Tracer {
    /// Create a new tracer
    pub fn new(max_events_per_session: usize, max_sessions: usize) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            max_events_per_session,
            max_sessions,
            session_counter: AtomicU64::new(0),
        }
    }

    /// Start a new trace session
    pub fn start_session(&self, connection_id: u64) -> String {
        let session_id = Uuid::new_v4().to_string();
        let session = Arc::new(TraceSession::new(
            session_id.clone(),
            connection_id,
            self.max_events_per_session,
        ));

        // Use blocking write for synchronous context
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let mut sessions = self.sessions.write().await;

                // Cleanup old sessions if at capacity
                if sessions.len() >= self.max_sessions {
                    // Remove oldest sessions
                    let mut oldest: Vec<(String, u64)> = sessions
                        .iter()
                        .map(|(k, v)| (k.clone(), v.started_at))
                        .collect();
                    oldest.sort_by_key(|(_, ts)| *ts);

                    for (key, _) in oldest.iter().take(self.max_sessions / 10) {
                        sessions.remove(key);
                    }
                }

                sessions.insert(session_id.clone(), session);
            });
        });

        self.session_counter.fetch_add(1, Ordering::Relaxed);
        session_id
    }

    /// Stop a trace session
    pub fn stop_session(&self, session_id: &str) {
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let sessions = self.sessions.read().await;
                if let Some(session) = sessions.get(session_id) {
                    session.stop();
                }
            });
        });
    }

    /// Record an event in a session
    pub fn record_event(&self, session_id: &str, event: TraceEvent) {
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let sessions = self.sessions.read().await;
                if let Some(session) = sessions.get(session_id) {
                    session.record(event).await;
                }
            });
        });
    }

    /// Get a trace session
    pub fn get_session(&self, session_id: &str) -> Option<Arc<TraceSession>> {
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let sessions = self.sessions.read().await;
                sessions.get(session_id).cloned()
            })
        })
    }

    /// Get number of active sessions
    pub fn active_sessions(&self) -> usize {
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let sessions = self.sessions.read().await;
                sessions.values().filter(|s| s.is_active()).count()
            })
        })
    }

    /// Clean up old sessions
    pub async fn cleanup(&self, max_age_secs: u64) {
        let mut sessions = self.sessions.write().await;
        let now = current_timestamp_ms();
        let max_age_ms = max_age_secs * 1000;

        sessions.retain(|_, session| {
            let age = now.saturating_sub(session.started_at);
            age < max_age_ms || session.is_active()
        });
    }
}

/// A trace session for a single connection
pub struct TraceSession {
    /// Session identifier
    pub session_id: String,
    /// Connection that owns this session
    pub connection_id: u64,
    /// When the session started (Unix timestamp ms)
    pub started_at: u64,
    /// When the session stopped (Unix timestamp ms)
    stopped_at: RwLock<Option<u64>>,
    /// Recorded events
    pub events: RwLock<Vec<TraceEvent>>,
    /// Maximum events to store
    max_events: usize,
    /// Event counter
    event_counter: AtomicU64,
}

impl TraceSession {
    /// Create a new trace session
    pub fn new(session_id: String, connection_id: u64, max_events: usize) -> Self {
        Self {
            session_id,
            connection_id,
            started_at: current_timestamp_ms(),
            stopped_at: RwLock::new(None),
            events: RwLock::new(Vec::with_capacity(max_events.min(1000))),
            max_events,
            event_counter: AtomicU64::new(0),
        }
    }

    /// Record an event
    pub async fn record(&self, mut event: TraceEvent) {
        let mut events = self.events.write().await;

        // Assign sequence number
        event.sequence = self.event_counter.fetch_add(1, Ordering::Relaxed);

        // Maintain max events limit (circular buffer behavior)
        if events.len() >= self.max_events {
            events.remove(0);
        }

        events.push(event);
    }

    /// Stop the session
    pub fn stop(&self) {
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let mut stopped_at = self.stopped_at.write().await;
                if stopped_at.is_none() {
                    *stopped_at = Some(current_timestamp_ms());
                }
            });
        });
    }

    /// Check if session is active
    pub fn is_active(&self) -> bool {
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let stopped_at = self.stopped_at.read().await;
                stopped_at.is_none()
            })
        })
    }

    /// Get session duration in microseconds
    pub fn duration_us(&self) -> u64 {
        let end = tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let stopped_at = self.stopped_at.read().await;
                stopped_at.unwrap_or_else(current_timestamp_ms)
            })
        });
        (end.saturating_sub(self.started_at)) * 1000 // Convert ms to us
    }
}

/// A single trace event
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraceEvent {
    /// Event sequence number within session
    pub sequence: u64,
    /// Timestamp (Unix timestamp microseconds)
    pub timestamp_us: u64,
    /// Event type
    pub event_type: EventType,
    /// Command name (if applicable)
    pub command: Option<String>,
    /// Key pattern (if applicable)
    pub key_pattern: Option<String>,
    /// Duration in microseconds
    pub duration_us: u64,
    /// Result status
    pub status: EventStatus,
    /// Additional attributes
    pub attributes: HashMap<String, String>,
    /// Span context for distributed tracing
    pub span_context: Option<SpanContext>,
}

impl TraceEvent {
    /// Create a new trace event
    pub fn new(event_type: EventType) -> Self {
        Self {
            sequence: 0,
            timestamp_us: current_timestamp_us(),
            event_type,
            command: None,
            key_pattern: None,
            duration_us: 0,
            status: EventStatus::Ok,
            attributes: HashMap::new(),
            span_context: None,
        }
    }

    /// Set the command
    pub fn with_command(mut self, command: impl Into<String>) -> Self {
        self.command = Some(command.into());
        self
    }

    /// Set the key pattern
    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key_pattern = Some(key.into());
        self
    }

    /// Set the duration
    pub fn with_duration(mut self, duration_us: u64) -> Self {
        self.duration_us = duration_us;
        self
    }

    /// Set the status
    pub fn with_status(mut self, status: EventStatus) -> Self {
        self.status = status;
        self
    }

    /// Add an attribute
    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    /// Set span context
    pub fn with_span_context(mut self, ctx: SpanContext) -> Self {
        self.span_context = Some(ctx);
        self
    }
}

/// Event types
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventType {
    /// Command execution
    Command,
    /// Storage operation
    Storage,
    /// Network I/O
    Network,
    /// Replication event
    Replication,
    /// Internal operation
    Internal,
    /// Custom event
    Custom(String),
}

/// Event status
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventStatus {
    /// Successful
    Ok,
    /// Error occurred
    Error(String),
    /// Timeout
    Timeout,
    /// Cancelled
    Cancelled,
}

/// Span context for distributed tracing (W3C Trace Context compatible)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpanContext {
    /// Trace ID (128-bit as hex string)
    pub trace_id: String,
    /// Span ID (64-bit as hex string)
    pub span_id: String,
    /// Parent span ID
    pub parent_span_id: Option<String>,
    /// Trace flags
    pub trace_flags: u8,
    /// Trace state
    pub trace_state: Option<String>,
}

impl SpanContext {
    /// Create a new span context
    pub fn new() -> Self {
        Self {
            trace_id: generate_trace_id(),
            span_id: generate_span_id(),
            parent_span_id: None,
            trace_flags: 0x01, // Sampled
            trace_state: None,
        }
    }

    /// Create a child span context
    pub fn child(&self) -> Self {
        Self {
            trace_id: self.trace_id.clone(),
            span_id: generate_span_id(),
            parent_span_id: Some(self.span_id.clone()),
            trace_flags: self.trace_flags,
            trace_state: self.trace_state.clone(),
        }
    }

    /// Parse from W3C traceparent header
    pub fn from_traceparent(header: &str) -> Option<Self> {
        let parts: Vec<&str> = header.split('-').collect();
        if parts.len() != 4 {
            return None;
        }

        Some(Self {
            trace_id: parts[1].to_string(),
            span_id: parts[2].to_string(),
            parent_span_id: None,
            trace_flags: u8::from_str_radix(parts[3], 16).ok()?,
            trace_state: None,
        })
    }

    /// Format as W3C traceparent header
    pub fn to_traceparent(&self) -> String {
        format!(
            "00-{}-{}-{:02x}",
            self.trace_id, self.span_id, self.trace_flags
        )
    }
}

impl Default for SpanContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Generate a random trace ID (128-bit hex)
fn generate_trace_id() -> String {
    format!("{:032x}", rand::random::<u128>())
}

/// Generate a random span ID (64-bit hex)
fn generate_span_id() -> String {
    format!("{:016x}", rand::random::<u64>())
}

/// Get current timestamp in milliseconds
fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Get current timestamp in microseconds
fn current_timestamp_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_event_creation() {
        let event = TraceEvent::new(EventType::Command)
            .with_command("GET")
            .with_key("foo")
            .with_duration(100);

        assert_eq!(event.command, Some("GET".to_string()));
        assert_eq!(event.key_pattern, Some("foo".to_string()));
        assert_eq!(event.duration_us, 100);
    }

    #[test]
    fn test_span_context() {
        let ctx = SpanContext::new();
        assert_eq!(ctx.trace_id.len(), 32);
        assert_eq!(ctx.span_id.len(), 16);

        let child = ctx.child();
        assert_eq!(child.trace_id, ctx.trace_id);
        assert_eq!(child.parent_span_id, Some(ctx.span_id));
    }

    #[test]
    fn test_traceparent_parsing() {
        let header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let ctx = SpanContext::from_traceparent(header).unwrap();

        assert_eq!(ctx.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(ctx.span_id, "00f067aa0ba902b7");
        assert_eq!(ctx.trace_flags, 0x01);
    }

    #[test]
    fn test_traceparent_formatting() {
        let ctx = SpanContext {
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".to_string(),
            span_id: "00f067aa0ba902b7".to_string(),
            parent_span_id: None,
            trace_flags: 0x01,
            trace_state: None,
        };

        let header = ctx.to_traceparent();
        assert_eq!(
            header,
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        );
    }
}
