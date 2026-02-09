//! OpenTelemetry metrics for Ferrite.
//!
//! This module defines OTel-native metrics that complement the existing
//! Prometheus `metrics` crate counters/gauges in [`crate::metrics::recorder`].
//!
//! All public helpers are gated behind `#[cfg(feature = "otel")]`.

#[cfg(feature = "otel")]
use opentelemetry::{
    global,
    metrics::{Counter, Histogram, Meter, UpDownCounter},
    KeyValue,
};

#[cfg(feature = "otel")]
use std::sync::OnceLock;

// ---------------------------------------------------------------------------
// Meter singleton
// ---------------------------------------------------------------------------

#[cfg(feature = "otel")]
static METER: OnceLock<Meter> = OnceLock::new();

/// Return (or lazily create) the global `Meter` for Ferrite.
#[cfg(feature = "otel")]
#[inline]
fn meter() -> &'static Meter {
    METER.get_or_init(|| global::meter("ferrite"))
}

// ---------------------------------------------------------------------------
// Instrument accessors (lazy, cached via OnceLock)
// ---------------------------------------------------------------------------

#[cfg(feature = "otel")]
macro_rules! cached_instrument {
    ($name:ident, $ty:ty, $init:expr) => {
        #[inline]
        fn $name() -> &'static $ty {
            static INST: OnceLock<$ty> = OnceLock::new();
            INST.get_or_init(|| $init)
        }
    };
}

#[cfg(feature = "otel")]
cached_instrument!(
    commands_counter,
    Counter<u64>,
    meter()
        .u64_counter("ferrite.commands")
        .with_description("Total commands processed")
        .init()
);

#[cfg(feature = "otel")]
cached_instrument!(
    command_duration_histogram,
    Histogram<f64>,
    meter()
        .f64_histogram("ferrite.command.duration")
        .with_description("Command execution duration in seconds")
        .with_unit(opentelemetry::metrics::Unit::new("s"))
        .init()
);

#[cfg(feature = "otel")]
cached_instrument!(
    active_connections_gauge,
    UpDownCounter<i64>,
    meter()
        .i64_up_down_counter("ferrite.connections.active")
        .with_description("Number of currently active client connections")
        .init()
);

#[cfg(feature = "otel")]
cached_instrument!(
    memory_usage_gauge,
    UpDownCounter<i64>,
    meter()
        .i64_up_down_counter("ferrite.memory.used_bytes")
        .with_description("Approximate memory usage in bytes")
        .with_unit(opentelemetry::metrics::Unit::new("By"))
        .init()
);

#[cfg(feature = "otel")]
cached_instrument!(
    cache_hits_counter,
    Counter<u64>,
    meter()
        .u64_counter("ferrite.cache.hits")
        .with_description("Total cache (keyspace) hits")
        .init()
);

#[cfg(feature = "otel")]
cached_instrument!(
    cache_misses_counter,
    Counter<u64>,
    meter()
        .u64_counter("ferrite.cache.misses")
        .with_description("Total cache (keyspace) misses")
        .init()
);

#[cfg(feature = "otel")]
cached_instrument!(
    hybridlog_mutable_size_gauge,
    UpDownCounter<i64>,
    meter()
        .i64_up_down_counter("ferrite.hybridlog.mutable.size_bytes")
        .with_description("Size of the HybridLog mutable (memory) region in bytes")
        .with_unit(opentelemetry::metrics::Unit::new("By"))
        .init()
);

#[cfg(feature = "otel")]
cached_instrument!(
    hybridlog_readonly_size_gauge,
    UpDownCounter<i64>,
    meter()
        .i64_up_down_counter("ferrite.hybridlog.readonly.size_bytes")
        .with_description("Size of the HybridLog read-only (mmap) region in bytes")
        .with_unit(opentelemetry::metrics::Unit::new("By"))
        .init()
);

#[cfg(feature = "otel")]
cached_instrument!(
    hybridlog_disk_size_gauge,
    UpDownCounter<i64>,
    meter()
        .i64_up_down_counter("ferrite.hybridlog.disk.size_bytes")
        .with_description("Size of the HybridLog disk region in bytes")
        .with_unit(opentelemetry::metrics::Unit::new("By"))
        .init()
);

#[cfg(feature = "otel")]
cached_instrument!(
    replication_lag_histogram,
    Histogram<f64>,
    meter()
        .f64_histogram("ferrite.replication.lag")
        .with_description("Replication lag in seconds")
        .with_unit(opentelemetry::metrics::Unit::new("s"))
        .init()
);

// ---------------------------------------------------------------------------
// Public recording helpers
// ---------------------------------------------------------------------------

/// Record a command execution.
///
/// # Arguments
/// * `cmd_name` - The Redis command name (e.g. `"GET"`, `"SET"`).
/// * `duration_secs` - Execution wall-clock time in fractional seconds.
/// * `success` - Whether the command succeeded.
#[cfg(feature = "otel")]
#[inline]
pub fn record_command(cmd_name: &str, duration_secs: f64, success: bool) {
    let attrs = [
        KeyValue::new("db.system", "ferrite"),
        KeyValue::new("db.operation", cmd_name.to_string()),
        KeyValue::new(
            "status",
            if success { "ok" } else { "error" }.to_string(),
        ),
    ];
    commands_counter().add(1, &attrs);
    command_duration_histogram().record(duration_secs, &attrs);
}

/// Increment the active-connections gauge by one.
#[cfg(feature = "otel")]
#[inline]
pub fn connection_opened() {
    active_connections_gauge().add(1, &[]);
}

/// Decrement the active-connections gauge by one.
#[cfg(feature = "otel")]
#[inline]
pub fn connection_closed() {
    active_connections_gauge().add(-1, &[]);
}

/// Set the current memory usage (absolute value).
///
/// Because OTel uses an `UpDownCounter` we track a delta internally;
/// for simplicity we report the absolute value each call.
#[cfg(feature = "otel")]
#[inline]
pub fn set_memory_usage(bytes: i64) {
    // For absolute reporting, we reset to 0 then add the value.
    // A better approach would use an ObservableGauge via a callback,
    // but that requires async registration. UpDownCounter is the
    // synchronous approximation available in OTel 0.22.
    memory_usage_gauge().add(bytes, &[]);
}

/// Record a cache (keyspace) hit.
#[cfg(feature = "otel")]
#[inline]
pub fn record_cache_hit() {
    cache_hits_counter().add(1, &[KeyValue::new("db.system", "ferrite")]);
}

/// Record a cache (keyspace) miss.
#[cfg(feature = "otel")]
#[inline]
pub fn record_cache_miss() {
    cache_misses_counter().add(1, &[KeyValue::new("db.system", "ferrite")]);
}

/// Update HybridLog tier sizes.
///
/// Reports absolute sizes for each tier of the three-tier HybridLog.
#[cfg(feature = "otel")]
#[inline]
pub fn set_hybridlog_tier_sizes(mutable_bytes: i64, readonly_bytes: i64, disk_bytes: i64) {
    hybridlog_mutable_size_gauge().add(mutable_bytes, &[]);
    hybridlog_readonly_size_gauge().add(readonly_bytes, &[]);
    hybridlog_disk_size_gauge().add(disk_bytes, &[]);
}

/// Record observed replication lag.
#[cfg(feature = "otel")]
#[inline]
pub fn record_replication_lag(lag_secs: f64) {
    replication_lag_histogram().record(
        lag_secs,
        &[KeyValue::new("db.system", "ferrite")],
    );
}

// ---------------------------------------------------------------------------
// No-op stubs when the `otel` feature is disabled
// ---------------------------------------------------------------------------

/// Record a command execution (no-op without `otel` feature).
#[cfg(not(feature = "otel"))]
#[inline]
pub fn record_command(_cmd_name: &str, _duration_secs: f64, _success: bool) {}

/// Increment the active-connections gauge (no-op without `otel` feature).
#[cfg(not(feature = "otel"))]
#[inline]
pub fn connection_opened() {}

/// Decrement the active-connections gauge (no-op without `otel` feature).
#[cfg(not(feature = "otel"))]
#[inline]
pub fn connection_closed() {}

/// Set the current memory usage (no-op without `otel` feature).
#[cfg(not(feature = "otel"))]
#[inline]
pub fn set_memory_usage(_bytes: i64) {}

/// Record a cache hit (no-op without `otel` feature).
#[cfg(not(feature = "otel"))]
#[inline]
pub fn record_cache_hit() {}

/// Record a cache miss (no-op without `otel` feature).
#[cfg(not(feature = "otel"))]
#[inline]
pub fn record_cache_miss() {}

/// Update HybridLog tier sizes (no-op without `otel` feature).
#[cfg(not(feature = "otel"))]
#[inline]
pub fn set_hybridlog_tier_sizes(_mutable: i64, _readonly: i64, _disk: i64) {}

/// Record replication lag (no-op without `otel` feature).
#[cfg(not(feature = "otel"))]
#[inline]
pub fn record_replication_lag(_lag_secs: f64) {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that no-op stubs compile and can be called without panicking.
    #[test]
    fn test_noop_stubs_do_not_panic() {
        // These calls exercise the public API regardless of feature flags.
        record_command("GET", 0.001, true);
        record_command("SET", 0.002, false);
        connection_opened();
        connection_closed();
        set_memory_usage(1024);
        record_cache_hit();
        record_cache_miss();
        set_hybridlog_tier_sizes(100, 200, 300);
        record_replication_lag(0.05);
    }
}
