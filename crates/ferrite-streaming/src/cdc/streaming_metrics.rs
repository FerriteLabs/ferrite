//! Streaming and CDC Metrics
//!
//! Prometheus-compatible metrics using the `metrics` crate.
//! Provides counters for produce/consume rates, gauges for consumer lag
//! and buffer utilization, and a failure counter.

use std::sync::atomic::{AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// StreamingMetrics
// ---------------------------------------------------------------------------

/// Central metrics registry for the streaming / CDC subsystem.
///
/// All public methods are cheap (atomic increments / loads) and safe
/// to call from hot paths.
pub struct StreamingMetrics {
    // Counters
    events_produced: AtomicU64,
    events_consumed: AtomicU64,
    delivery_failures: AtomicU64,

    // Gauges (stored as atomic u64 for simplicity)
    consumer_lag: AtomicU64,
    buffer_size: AtomicU64,
    buffer_capacity: AtomicU64,
}

impl StreamingMetrics {
    /// Create a new metrics instance and register with the `metrics` crate.
    pub fn new() -> Self {
        Self::register_descriptors();

        Self {
            events_produced: AtomicU64::new(0),
            events_consumed: AtomicU64::new(0),
            delivery_failures: AtomicU64::new(0),
            consumer_lag: AtomicU64::new(0),
            buffer_size: AtomicU64::new(0),
            buffer_capacity: AtomicU64::new(0),
        }
    }

    /// Register metric descriptors (descriptions) with the `metrics` crate.
    fn register_descriptors() {
        metrics::describe_counter!(
            "ferrite_streaming_events_produced_total",
            "Total events produced"
        );
        metrics::describe_counter!(
            "ferrite_streaming_events_consumed_total",
            "Total events consumed"
        );
        metrics::describe_counter!(
            "ferrite_streaming_delivery_failures_total",
            "Total delivery failures"
        );
        metrics::describe_gauge!(
            "ferrite_streaming_consumer_lag",
            "Number of unconsumed events across all consumer groups"
        );
        metrics::describe_gauge!(
            "ferrite_streaming_buffer_utilization",
            "Buffer utilization ratio (0.0 – 1.0)"
        );
    }

    // -- Counters -----------------------------------------------------------

    /// Record that `n` events were produced.
    pub fn record_produced(&self, n: u64) {
        self.events_produced.fetch_add(n, Ordering::Relaxed);
        metrics::counter!("ferrite_streaming_events_produced_total").increment(n);
    }

    /// Record that `n` events were consumed.
    pub fn record_consumed(&self, n: u64) {
        self.events_consumed.fetch_add(n, Ordering::Relaxed);
        metrics::counter!("ferrite_streaming_events_consumed_total").increment(n);
    }

    /// Record a delivery failure.
    pub fn record_failure(&self) {
        self.delivery_failures.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("ferrite_streaming_delivery_failures_total").increment(1);
    }

    // -- Gauges -------------------------------------------------------------

    /// Update the consumer lag gauge.
    pub fn set_consumer_lag(&self, lag: u64) {
        self.consumer_lag.store(lag, Ordering::Relaxed);
        metrics::gauge!("ferrite_streaming_consumer_lag").set(lag as f64);
    }

    /// Update the buffer size and push utilization gauge.
    pub fn set_buffer_usage(&self, size: u64, capacity: u64) {
        self.buffer_size.store(size, Ordering::Relaxed);
        self.buffer_capacity.store(capacity, Ordering::Relaxed);
        let utilization = if capacity > 0 {
            size as f64 / capacity as f64
        } else {
            0.0
        };
        metrics::gauge!("ferrite_streaming_buffer_utilization").set(utilization);
    }

    // -- Snapshot -----------------------------------------------------------

    /// Return a snapshot of all metric values.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let cap = self.buffer_capacity.load(Ordering::Relaxed);
        let size = self.buffer_size.load(Ordering::Relaxed);
        MetricsSnapshot {
            events_produced: self.events_produced.load(Ordering::Relaxed),
            events_consumed: self.events_consumed.load(Ordering::Relaxed),
            delivery_failures: self.delivery_failures.load(Ordering::Relaxed),
            consumer_lag: self.consumer_lag.load(Ordering::Relaxed),
            buffer_size: size,
            buffer_capacity: cap,
            buffer_utilization: if cap > 0 {
                size as f64 / cap as f64
            } else {
                0.0
            },
        }
    }
}

impl Default for StreamingMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// A point-in-time snapshot of streaming metrics.
#[derive(Clone, Debug)]
pub struct MetricsSnapshot {
    pub events_produced: u64,
    pub events_consumed: u64,
    pub delivery_failures: u64,
    pub consumer_lag: u64,
    pub buffer_size: u64,
    pub buffer_capacity: u64,
    pub buffer_utilization: f64,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_produced_consumed() {
        let m = StreamingMetrics::new();
        m.record_produced(10);
        m.record_consumed(5);
        m.record_failure();

        let snap = m.snapshot();
        assert_eq!(snap.events_produced, 10);
        assert_eq!(snap.events_consumed, 5);
        assert_eq!(snap.delivery_failures, 1);
    }

    #[test]
    fn test_consumer_lag() {
        let m = StreamingMetrics::new();
        m.set_consumer_lag(42);

        let snap = m.snapshot();
        assert_eq!(snap.consumer_lag, 42);
    }

    #[test]
    fn test_buffer_utilization() {
        let m = StreamingMetrics::new();
        m.set_buffer_usage(500, 1000);

        let snap = m.snapshot();
        assert_eq!(snap.buffer_size, 500);
        assert_eq!(snap.buffer_capacity, 1000);
        assert!((snap.buffer_utilization - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_buffer_utilization_zero_capacity() {
        let m = StreamingMetrics::new();
        m.set_buffer_usage(0, 0);

        let snap = m.snapshot();
        assert_eq!(snap.buffer_utilization, 0.0);
    }

    #[test]
    fn test_snapshot_initial() {
        let m = StreamingMetrics::new();
        let snap = m.snapshot();
        assert_eq!(snap.events_produced, 0);
        assert_eq!(snap.events_consumed, 0);
        assert_eq!(snap.delivery_failures, 0);
        assert_eq!(snap.consumer_lag, 0);
    }
}
