//! Tracing span helpers for OpenTelemetry.
//!
//! Provides convenience functions that create pre-attributed [`tracing::Span`]
//! instances for the hot paths in Ferrite (command execution, storage I/O,
//! and connection handling). The spans carry semantic attributes following
//! [OpenTelemetry semantic conventions](https://opentelemetry.io/docs/specs/semconv/).
//!
//! When the `otel` feature is **disabled**, the functions still return valid
//! `tracing` spans (they just won't be exported to an OTLP collector).

use tracing::Span;

// ---------------------------------------------------------------------------
// Constants — Semantic convention attribute values
// ---------------------------------------------------------------------------

/// The `db.system` attribute value for all Ferrite spans.
const DB_SYSTEM: &str = "ferrite";

// ---------------------------------------------------------------------------
// Public span constructors
// ---------------------------------------------------------------------------

/// Create a span for a Redis-compatible command execution.
///
/// # Attributes
///
/// | Attribute           | Value                       |
/// |---------------------|-----------------------------|
/// | `db.system`         | `"ferrite"`                 |
/// | `db.operation`      | The command name (e.g. GET) |
/// | `db.statement`      | The key, if available       |
/// | `otel.kind`         | `"SERVER"`                  |
///
/// # Arguments
///
/// * `cmd_name` - Uppercase command name (e.g. `"GET"`, `"HSET"`).
/// * `key` - Optional key bytes; included as `db.statement` when present.
#[inline]
pub fn command_span(cmd_name: &str, key: Option<&[u8]>) -> Span {
    let key_str = key.map(|k| String::from_utf8_lossy(k).into_owned());

    match key_str {
        Some(k) => tracing::info_span!(
            "ferrite.command",
            db.system = DB_SYSTEM,
            db.operation = cmd_name,
            db.statement = %k,
            otel.kind = "SERVER",
        ),
        None => tracing::info_span!(
            "ferrite.command",
            db.system = DB_SYSTEM,
            db.operation = cmd_name,
            otel.kind = "SERVER",
        ),
    }
}

/// Create a span for a storage-layer operation.
///
/// # Attributes
///
/// | Attribute           | Value                                     |
/// |---------------------|-------------------------------------------|
/// | `db.system`         | `"ferrite"`                               |
/// | `db.operation`      | The storage operation (e.g. `"read"`)     |
/// | `ferrite.tier`      | HybridLog tier (`"mutable"` / `"readonly"` / `"disk"`) |
/// | `otel.kind`         | `"INTERNAL"`                              |
///
/// # Arguments
///
/// * `operation` - A short verb describing the I/O operation.
/// * `tier` - The HybridLog tier name.
#[inline]
pub fn storage_span(operation: &str, tier: &str) -> Span {
    tracing::info_span!(
        "ferrite.storage",
        db.system = DB_SYSTEM,
        db.operation = operation,
        ferrite.tier = tier,
        otel.kind = "INTERNAL",
    )
}

/// Create a span for an inbound client connection.
///
/// # Attributes
///
/// | Attribute           | Value                        |
/// |---------------------|------------------------------|
/// | `db.system`         | `"ferrite"`                  |
/// | `net.peer.name`     | Remote address string        |
/// | `otel.kind`         | `"SERVER"`                   |
///
/// # Arguments
///
/// * `addr` - The remote peer address (e.g. `"127.0.0.1:51234"`).
#[inline]
pub fn connection_span(addr: &str) -> Span {
    tracing::info_span!(
        "ferrite.connection",
        db.system = DB_SYSTEM,
        net.peer.name = addr,
        otel.kind = "SERVER",
    )
}

/// Create a span for a replication operation.
///
/// # Attributes
///
/// | Attribute           | Value                                       |
/// |---------------------|---------------------------------------------|
/// | `db.system`         | `"ferrite"`                                 |
/// | `db.operation`      | `"replication"`                             |
/// | `ferrite.role`      | `"primary"` or `"replica"`                  |
/// | `net.peer.name`     | Remote peer address                         |
/// | `otel.kind`         | `"CLIENT"` (outbound to peer)               |
#[inline]
pub fn replication_span(role: &str, peer_addr: &str) -> Span {
    tracing::info_span!(
        "ferrite.replication",
        db.system = DB_SYSTEM,
        db.operation = "replication",
        ferrite.role = role,
        net.peer.name = peer_addr,
        otel.kind = "CLIENT",
    )
}

/// Create a span for a persistence operation (AOF write, RDB snapshot, etc.).
///
/// # Attributes
///
/// | Attribute           | Value                                       |
/// |---------------------|---------------------------------------------|
/// | `db.system`         | `"ferrite"`                                 |
/// | `db.operation`      | `"persistence"`                             |
/// | `ferrite.persist`   | Persistence type (`"aof"`, `"rdb"`, etc.)  |
/// | `otel.kind`         | `"INTERNAL"`                                |
#[inline]
pub fn persistence_span(persist_type: &str) -> Span {
    tracing::info_span!(
        "ferrite.persistence",
        db.system = DB_SYSTEM,
        db.operation = "persistence",
        ferrite.persist = persist_type,
        otel.kind = "INTERNAL",
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::Instrument;

    #[test]
    fn test_command_span_with_key() {
        let span = command_span("GET", Some(b"mykey"));
        // Verify the span is valid and can be entered.
        let _guard = span.enter();
    }

    #[test]
    fn test_command_span_without_key() {
        let span = command_span("PING", None);
        let _guard = span.enter();
    }

    #[test]
    fn test_storage_span() {
        let span = storage_span("read", "mutable");
        let _guard = span.enter();
    }

    #[test]
    fn test_storage_span_disk_tier() {
        let span = storage_span("write", "disk");
        let _guard = span.enter();
    }

    #[test]
    fn test_connection_span() {
        let span = connection_span("127.0.0.1:6379");
        let _guard = span.enter();
    }

    #[test]
    fn test_replication_span() {
        let span = replication_span("primary", "10.0.0.2:6379");
        let _guard = span.enter();
    }

    #[test]
    fn test_persistence_span() {
        let span = persistence_span("aof");
        let _guard = span.enter();
    }

    #[test]
    fn test_span_key_with_non_utf8() {
        // Verify non-UTF-8 keys don't panic (lossy conversion).
        let key: &[u8] = &[0xff, 0xfe, 0x00, 0x01];
        let span = command_span("SET", Some(key));
        let _guard = span.enter();
    }

    #[tokio::test]
    async fn test_span_instrument_async() {
        let span = command_span("GET", Some(b"async-key"));
        async {
            // Body is instrumented with the span.
        }
        .instrument(span)
        .await;
    }
}
