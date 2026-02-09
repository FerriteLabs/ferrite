//! Distributed Tracing Context Propagation
//!
//! Implements W3C Trace Context propagation for distributed tracing across
//! service boundaries. Supports the `traceparent` header format as defined in
//! <https://www.w3.org/TR/trace-context/>.
//!
//! # Example
//!
//! ```ignore
//! use ferrite::observability::distributed_trace::{
//!     DistributedTraceContext, TraceContextPropagator, SpanBuilder,
//! };
//!
//! // Extract from incoming request
//! let header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
//! if let Some(ctx) = TraceContextPropagator::extract(header) {
//!     // Create a child span
//!     let span = SpanBuilder::new("redis.GET")
//!         .with_parent(&ctx)
//!         .with_tag("db.system", "ferrite")
//!         .start();
//! }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// W3C Trace Context version supported by this implementation.
const TRACE_CONTEXT_VERSION: &str = "00";

/// Distributed tracing context following the W3C Trace Context specification.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DistributedTraceContext {
    /// 128-bit trace identifier encoded as a 32-character lowercase hex string.
    pub trace_id: String,
    /// 64-bit span identifier encoded as a 16-character lowercase hex string.
    pub span_id: String,
    /// Optional parent span identifier (16-character lowercase hex string).
    pub parent_span_id: Option<String>,
    /// W3C trace flags (bit field; 0x01 = sampled).
    pub trace_flags: u8,
    /// Arbitrary baggage items propagated across service boundaries.
    pub baggage: HashMap<String, String>,
}

impl DistributedTraceContext {
    /// Create a new root trace context with random identifiers.
    pub fn new() -> Self {
        Self {
            trace_id: generate_trace_id(),
            span_id: generate_span_id(),
            parent_span_id: None,
            trace_flags: 0x01, // sampled by default
            baggage: HashMap::new(),
        }
    }

    /// Create a child context inheriting the trace ID and baggage from this
    /// context. The current span becomes the parent of the new span.
    pub fn child(&self) -> Self {
        Self {
            trace_id: self.trace_id.clone(),
            span_id: generate_span_id(),
            parent_span_id: Some(self.span_id.clone()),
            trace_flags: self.trace_flags,
            baggage: self.baggage.clone(),
        }
    }

    /// Returns `true` if the sampled flag is set.
    pub fn is_sampled(&self) -> bool {
        self.trace_flags & 0x01 != 0
    }

    /// Set the sampled flag.
    pub fn set_sampled(&mut self, sampled: bool) {
        if sampled {
            self.trace_flags |= 0x01;
        } else {
            self.trace_flags &= !0x01;
        }
    }

    /// Insert a baggage item.
    pub fn set_baggage(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.baggage.insert(key.into(), value.into());
    }

    /// Retrieve a baggage item by key.
    pub fn get_baggage(&self, key: &str) -> Option<&str> {
        self.baggage.get(key).map(|v| v.as_str())
    }
}

impl Default for DistributedTraceContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Propagator for the W3C Trace Context `traceparent` header format.
///
/// Header format: `{version}-{trace_id}-{span_id}-{trace_flags}`
///
/// Example: `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`
pub struct TraceContextPropagator;

impl TraceContextPropagator {
    /// Serialize a [`DistributedTraceContext`] into a W3C `traceparent` header
    /// value.
    pub fn inject(ctx: &DistributedTraceContext) -> String {
        format!(
            "{}-{}-{}-{:02x}",
            TRACE_CONTEXT_VERSION, ctx.trace_id, ctx.span_id, ctx.trace_flags
        )
    }

    /// Deserialize a W3C `traceparent` header value into a
    /// [`DistributedTraceContext`].
    ///
    /// Returns `None` if the header is malformed or contains invalid values.
    pub fn extract(header: &str) -> Option<DistributedTraceContext> {
        let parts: Vec<&str> = header.split('-').collect();
        if parts.len() != 4 {
            return None;
        }

        let version = parts[0];
        let trace_id = parts[1];
        let span_id = parts[2];
        let flags_str = parts[3];

        // Validate version
        if version != TRACE_CONTEXT_VERSION {
            return None;
        }

        // Validate trace_id: 32 hex chars, not all zeros
        if trace_id.len() != 32 || trace_id.chars().all(|c| c == '0') {
            return None;
        }
        if !trace_id.chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }

        // Validate span_id: 16 hex chars, not all zeros
        if span_id.len() != 16 || span_id.chars().all(|c| c == '0') {
            return None;
        }
        if !span_id.chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }

        let trace_flags = u8::from_str_radix(flags_str, 16).ok()?;

        Some(DistributedTraceContext {
            trace_id: trace_id.to_string(),
            span_id: span_id.to_string(),
            parent_span_id: None,
            trace_flags,
            baggage: HashMap::new(),
        })
    }
}

/// Builder for creating spans with operation names, tags, and timing.
#[derive(Clone, Debug)]
pub struct SpanBuilder {
    /// The operation name for this span (e.g., "redis.GET").
    operation_name: String,
    /// Optional parent trace context.
    parent: Option<DistributedTraceContext>,
    /// Key-value tags attached to this span.
    tags: HashMap<String, String>,
}

impl SpanBuilder {
    /// Create a new span builder with the given operation name.
    pub fn new(operation_name: impl Into<String>) -> Self {
        Self {
            operation_name: operation_name.into(),
            parent: None,
            tags: HashMap::new(),
        }
    }

    /// Set a parent trace context. The resulting span will be a child of this
    /// context.
    pub fn with_parent(mut self, parent: &DistributedTraceContext) -> Self {
        self.parent = Some(parent.clone());
        self
    }

    /// Attach a tag to the span.
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    /// Finalize and start the span, recording its start time.
    pub fn start(self) -> Span {
        let context = match self.parent {
            Some(ref parent) => parent.child(),
            None => DistributedTraceContext::new(),
        };

        Span {
            context,
            operation_name: self.operation_name,
            tags: self.tags,
            start_time_us: current_timestamp_us(),
            end_time_us: None,
        }
    }
}

/// A span representing a unit of work within a distributed trace.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Span {
    /// The distributed trace context for this span.
    pub context: DistributedTraceContext,
    /// The operation name (e.g., "redis.GET", "storage.read").
    pub operation_name: String,
    /// Tags attached to this span.
    pub tags: HashMap<String, String>,
    /// Start time as a Unix timestamp in microseconds.
    pub start_time_us: u64,
    /// End time as a Unix timestamp in microseconds (`None` while in-flight).
    pub end_time_us: Option<u64>,
}

impl Span {
    /// Finish the span, recording its end time.
    pub fn finish(&mut self) {
        if self.end_time_us.is_none() {
            self.end_time_us = Some(current_timestamp_us());
        }
    }

    /// Duration in microseconds. Returns `None` if the span has not finished.
    pub fn duration_us(&self) -> Option<u64> {
        self.end_time_us
            .map(|end| end.saturating_sub(self.start_time_us))
    }

    /// Returns `true` if the span has been finished.
    pub fn is_finished(&self) -> bool {
        self.end_time_us.is_some()
    }
}

/// Generate a random 128-bit trace ID as a 32-character lowercase hex string.
fn generate_trace_id() -> String {
    format!("{:032x}", rand::random::<u128>())
}

/// Generate a random 64-bit span ID as a 16-character lowercase hex string.
fn generate_span_id() -> String {
    format!("{:016x}", rand::random::<u64>())
}

/// Get the current Unix timestamp in microseconds.
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
    fn test_context_new() {
        let ctx = DistributedTraceContext::new();
        assert_eq!(ctx.trace_id.len(), 32);
        assert_eq!(ctx.span_id.len(), 16);
        assert!(ctx.parent_span_id.is_none());
        assert!(ctx.is_sampled());
        assert!(ctx.baggage.is_empty());
    }

    #[test]
    fn test_context_child() {
        let parent = DistributedTraceContext::new();
        let child = parent.child();

        assert_eq!(child.trace_id, parent.trace_id);
        assert_ne!(child.span_id, parent.span_id);
        assert_eq!(
            child.parent_span_id.as_deref(),
            Some(parent.span_id.as_str())
        );
        assert_eq!(child.trace_flags, parent.trace_flags);
    }

    #[test]
    fn test_sampled_flag() {
        let mut ctx = DistributedTraceContext::new();
        assert!(ctx.is_sampled());

        ctx.set_sampled(false);
        assert!(!ctx.is_sampled());

        ctx.set_sampled(true);
        assert!(ctx.is_sampled());
    }

    #[test]
    fn test_baggage() {
        let mut ctx = DistributedTraceContext::new();
        ctx.set_baggage("user_id", "42");
        ctx.set_baggage("request_id", "abc-123");

        assert_eq!(ctx.get_baggage("user_id"), Some("42"));
        assert_eq!(ctx.get_baggage("request_id"), Some("abc-123"));
        assert_eq!(ctx.get_baggage("missing"), None);
    }

    #[test]
    fn test_baggage_propagated_to_child() {
        let mut parent = DistributedTraceContext::new();
        parent.set_baggage("tenant", "acme");

        let child = parent.child();
        assert_eq!(child.get_baggage("tenant"), Some("acme"));
    }

    #[test]
    fn test_inject_extract_roundtrip() {
        let original = DistributedTraceContext {
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".to_string(),
            span_id: "00f067aa0ba902b7".to_string(),
            parent_span_id: None,
            trace_flags: 0x01,
            baggage: HashMap::new(),
        };

        let header = TraceContextPropagator::inject(&original);
        assert_eq!(
            header,
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        );

        let extracted = TraceContextPropagator::extract(&header).unwrap();
        assert_eq!(extracted.trace_id, original.trace_id);
        assert_eq!(extracted.span_id, original.span_id);
        assert_eq!(extracted.trace_flags, original.trace_flags);
    }

    #[test]
    fn test_extract_valid_header() {
        let header = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        let ctx = TraceContextPropagator::extract(header).unwrap();

        assert_eq!(ctx.trace_id, "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(ctx.span_id, "b7ad6b7169203331");
        assert_eq!(ctx.trace_flags, 0x01);
    }

    #[test]
    fn test_extract_not_sampled() {
        let header = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00";
        let ctx = TraceContextPropagator::extract(header).unwrap();
        assert!(!ctx.is_sampled());
    }

    #[test]
    fn test_extract_invalid_version() {
        let header = "ff-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        assert!(TraceContextPropagator::extract(header).is_none());
    }

    #[test]
    fn test_extract_invalid_trace_id_all_zeros() {
        let header = "00-00000000000000000000000000000000-b7ad6b7169203331-01";
        assert!(TraceContextPropagator::extract(header).is_none());
    }

    #[test]
    fn test_extract_invalid_span_id_all_zeros() {
        let header = "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01";
        assert!(TraceContextPropagator::extract(header).is_none());
    }

    #[test]
    fn test_extract_invalid_format() {
        assert!(TraceContextPropagator::extract("invalid").is_none());
        assert!(TraceContextPropagator::extract("00-abc-def-01").is_none());
        assert!(TraceContextPropagator::extract("").is_none());
    }

    #[test]
    fn test_extract_invalid_hex_chars() {
        let header = "00-ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ-b7ad6b7169203331-01";
        assert!(TraceContextPropagator::extract(header).is_none());
    }

    #[test]
    fn test_inject_unsampled() {
        let ctx = DistributedTraceContext {
            trace_id: "0af7651916cd43dd8448eb211c80319c".to_string(),
            span_id: "b7ad6b7169203331".to_string(),
            parent_span_id: None,
            trace_flags: 0x00,
            baggage: HashMap::new(),
        };

        let header = TraceContextPropagator::inject(&ctx);
        assert!(header.ends_with("-00"));
    }

    #[test]
    fn test_span_builder_basic() {
        let span = SpanBuilder::new("redis.GET").start();

        assert_eq!(span.operation_name, "redis.GET");
        assert!(span.start_time_us > 0);
        assert!(span.end_time_us.is_none());
        assert!(!span.is_finished());
    }

    #[test]
    fn test_span_builder_with_parent() {
        let parent_ctx = DistributedTraceContext::new();
        let span = SpanBuilder::new("redis.SET")
            .with_parent(&parent_ctx)
            .start();

        assert_eq!(span.context.trace_id, parent_ctx.trace_id);
        assert_eq!(
            span.context.parent_span_id.as_deref(),
            Some(parent_ctx.span_id.as_str())
        );
    }

    #[test]
    fn test_span_builder_with_tags() {
        let span = SpanBuilder::new("redis.GET")
            .with_tag("db.system", "ferrite")
            .with_tag("db.operation", "GET")
            .start();

        assert_eq!(span.tags.get("db.system").unwrap(), "ferrite");
        assert_eq!(span.tags.get("db.operation").unwrap(), "GET");
    }

    #[test]
    fn test_span_finish() {
        let mut span = SpanBuilder::new("redis.GET").start();
        assert!(!span.is_finished());
        assert!(span.duration_us().is_none());

        span.finish();
        assert!(span.is_finished());
        assert!(span.duration_us().is_some());
    }

    #[test]
    fn test_span_finish_idempotent() {
        let mut span = SpanBuilder::new("redis.GET").start();
        span.finish();
        let first_end = span.end_time_us;

        span.finish();
        assert_eq!(span.end_time_us, first_end);
    }

    #[test]
    fn test_new_context_roundtrip_inject_extract() {
        let ctx = DistributedTraceContext::new();
        let header = TraceContextPropagator::inject(&ctx);
        let extracted = TraceContextPropagator::extract(&header).unwrap();

        assert_eq!(extracted.trace_id, ctx.trace_id);
        assert_eq!(extracted.span_id, ctx.span_id);
        assert_eq!(extracted.trace_flags, ctx.trace_flags);
    }
}
