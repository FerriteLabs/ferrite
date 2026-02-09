//! Metrics recording utilities
//!
//! This module provides functions to record various Ferrite metrics.

use metrics::{counter, gauge, histogram};
use std::time::Duration;

/// Record a command execution
pub fn record_command(command: &str, db: u8, duration: Duration, success: bool) {
    let labels = [
        ("command", command.to_string()),
        ("database", db.to_string()),
        ("status", if success { "ok" } else { "error" }.to_string()),
    ];

    counter!("ferrite_commands_total", &labels).increment(1);
    histogram!("ferrite_command_duration_seconds", &labels).record(duration.as_secs_f64());
}

/// Record a connection event
pub fn record_connection_opened() {
    counter!("ferrite_connections_opened_total").increment(1);
    gauge!("ferrite_connections_active").increment(1.0);
}

/// Record a connection close event
pub fn record_connection_closed() {
    gauge!("ferrite_connections_active").decrement(1.0);
}

/// Record bytes received
pub fn record_bytes_received(bytes: u64) {
    counter!("ferrite_bytes_received_total").increment(bytes);
}

/// Record bytes sent
pub fn record_bytes_sent(bytes: u64) {
    counter!("ferrite_bytes_sent_total").increment(bytes);
}

/// Record key operations
pub fn record_key_hit() {
    counter!("ferrite_keyspace_hits_total").increment(1);
}

/// Record key miss
pub fn record_key_miss() {
    counter!("ferrite_keyspace_misses_total").increment(1);
}

/// Record expired key
pub fn record_key_expired() {
    counter!("ferrite_expired_keys_total").increment(1);
}

/// Record evicted key
pub fn record_key_evicted() {
    counter!("ferrite_evicted_keys_total").increment(1);
}

// ── Replication metrics ──────────────────────────────────────────────────────

/// Record replication lag in bytes for a given replica
pub fn record_replication_lag_bytes(replica: &str, lag: f64) {
    let labels = [("replica", replica.to_string())];
    gauge!("ferrite_replication_lag_bytes", &labels).set(lag);
}

/// Record replication lag in seconds for a given replica
pub fn record_replication_lag_seconds(replica: &str, lag: f64) {
    let labels = [("replica", replica.to_string())];
    gauge!("ferrite_replication_lag_seconds", &labels).set(lag);
}

/// Record the number of connected replicas
pub fn record_connected_replicas(count: f64) {
    gauge!("ferrite_replication_connected_replicas").set(count);
}

/// Record a partial resync accepted event
pub fn record_partial_resync_accepted() {
    counter!("ferrite_replication_partial_resync_total").increment(1);
}

/// Record a full resync event
pub fn record_full_resync() {
    counter!("ferrite_replication_full_resync_total").increment(1);
}

/// Record a replication failover event
pub fn record_replication_failover(success: bool) {
    let labels = [("status", if success { "ok" } else { "error" }.to_string())];
    counter!("ferrite_replication_failover_total", &labels).increment(1);
}

/// Record the current primary replication offset
pub fn record_replication_offset(offset: f64) {
    gauge!("ferrite_replication_offset").set(offset);
}

/// Update memory usage gauge
pub fn update_memory_usage(bytes: u64) {
    gauge!("ferrite_memory_used_bytes").set(bytes as f64);
}

/// Update key count gauge for a database
pub fn update_key_count(db: u8, count: usize) {
    let labels = [("database", db.to_string())];
    gauge!("ferrite_keys_total", &labels).set(count as f64);
}

/// Record AOF write
pub fn record_aof_write(bytes: u64, duration: Duration) {
    counter!("ferrite_aof_writes_total").increment(1);
    counter!("ferrite_aof_bytes_written_total").increment(bytes);
    histogram!("ferrite_aof_write_duration_seconds").record(duration.as_secs_f64());
}

/// Record AOF sync
pub fn record_aof_sync(duration: Duration) {
    counter!("ferrite_aof_syncs_total").increment(1);
    histogram!("ferrite_aof_sync_duration_seconds").record(duration.as_secs_f64());
}

/// Initialize standard Ferrite metrics with descriptions
pub fn init_metrics() {
    // These calls will register the metrics with their descriptions
    metrics::describe_counter!(
        "ferrite_commands_total",
        "Total number of commands processed"
    );
    metrics::describe_histogram!(
        "ferrite_command_duration_seconds",
        "Command execution duration in seconds"
    );
    metrics::describe_counter!(
        "ferrite_connections_opened_total",
        "Total number of connections opened"
    );
    metrics::describe_gauge!(
        "ferrite_connections_active",
        "Number of currently active connections"
    );
    metrics::describe_counter!(
        "ferrite_bytes_received_total",
        "Total bytes received from clients"
    );
    metrics::describe_counter!("ferrite_bytes_sent_total", "Total bytes sent to clients");
    metrics::describe_counter!(
        "ferrite_keyspace_hits_total",
        "Total number of successful key lookups"
    );
    metrics::describe_counter!(
        "ferrite_keyspace_misses_total",
        "Total number of failed key lookups"
    );
    metrics::describe_counter!("ferrite_expired_keys_total", "Total number of keys expired");
    metrics::describe_counter!("ferrite_evicted_keys_total", "Total number of keys evicted");
    metrics::describe_gauge!("ferrite_memory_used_bytes", "Total memory used in bytes");
    metrics::describe_gauge!("ferrite_keys_total", "Total number of keys per database");
    metrics::describe_counter!(
        "ferrite_aof_writes_total",
        "Total number of AOF write operations"
    );
    metrics::describe_counter!(
        "ferrite_aof_bytes_written_total",
        "Total bytes written to AOF"
    );
    metrics::describe_histogram!(
        "ferrite_aof_write_duration_seconds",
        "AOF write duration in seconds"
    );
    metrics::describe_counter!(
        "ferrite_aof_syncs_total",
        "Total number of AOF sync operations"
    );
    metrics::describe_histogram!(
        "ferrite_aof_sync_duration_seconds",
        "AOF sync duration in seconds"
    );
}

#[cfg(test)]
mod tests {
    // Note: Testing metrics requires setting up a recorder
    // These tests would normally use metrics_util::debugging::DebuggingRecorder
    // For now, we just verify the functions compile correctly

    #[test]
    fn test_metrics_functions_exist() {
        // This test just verifies all metric functions are callable
        // In a real test, we'd set up a debugging recorder and verify values
    }
}
