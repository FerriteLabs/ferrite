//! Redis connector for live migration.
//!
//! Provides a logical interface to a source Redis instance for reading
//! metadata, scanning keys, dumping values, and subscribing to keyspace
//! notifications.

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::fmt;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::protocol::Frame;

/// Information about a source Redis instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisInfo {
    /// Redis server version string.
    pub version: String,
    /// Role of this node (master / slave).
    pub role: String,
    /// Number of connected replicas.
    pub connected_slaves: u64,
    /// Total memory used by the instance in bytes.
    pub used_memory: u64,
}

impl Default for RedisInfo {
    fn default() -> Self {
        Self {
            version: "unknown".into(),
            role: "master".into(),
            connected_slaves: 0,
            used_memory: 0,
        }
    }
}

/// A single key's dump: type, serialised value, and optional TTL.
#[derive(Debug, Clone)]
pub struct KeyDump {
    /// The key name.
    pub key: String,
    /// Redis data type (string, list, hash, set, zset, stream, …).
    pub key_type: String,
    /// Serialised RESP value.
    pub value: Frame,
    /// Remaining TTL in milliseconds, or `None` for persistent keys.
    pub ttl_ms: Option<i64>,
}

/// A keyspace notification event.
#[derive(Debug, Clone)]
pub struct KeyspaceEvent {
    /// The affected key.
    pub key: String,
    /// The operation (set, del, expire, …).
    pub operation: String,
}

/// Receiver end for keyspace notification events.
pub type KeyspaceReceiver = mpsc::Receiver<KeyspaceEvent>;

/// Error type for connector operations.
#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    /// Failed to connect to the source Redis.
    #[error("connection error: {0}")]
    Connection(String),
    /// A command returned an unexpected response.
    #[error("protocol error: {0}")]
    Protocol(String),
    /// The operation timed out.
    #[error("timeout: {0}")]
    Timeout(String),
    /// Generic I/O error.
    #[error("io error: {0}")]
    Io(String),
}

/// Result alias for connector operations.
pub type Result<T> = std::result::Result<T, ConnectorError>;

/// A logical connector to a source Redis instance.
///
/// In production this would hold a TCP connection using RESP framing.
/// The current implementation provides the public API surface and
/// returns placeholder data so the rest of the migration pipeline
/// can compile and be tested end-to-end.
pub struct RedisConnector {
    uri: String,
    connected: bool,
}

impl fmt::Debug for RedisConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisConnector")
            .field("uri", &self.uri)
            .field("connected", &self.connected)
            .finish()
    }
}

impl RedisConnector {
    /// Connect to a Redis instance at the given URI.
    pub async fn connect(uri: &str) -> Result<Self> {
        debug!(uri, "connecting to source Redis");
        // In a full implementation this would establish a TCP connection
        // and perform RESP handshake. For now we validate the URI format
        // and return a logical connector.
        if uri.is_empty() {
            return Err(ConnectorError::Connection(
                "empty URI provided".into(),
            ));
        }
        Ok(Self {
            uri: uri.to_string(),
            connected: true,
        })
    }

    /// Return the URI this connector targets.
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Retrieve high-level INFO from the source Redis.
    pub async fn info(&self) -> Result<RedisInfo> {
        self.ensure_connected()?;
        debug!("fetching INFO from source");
        // Placeholder — a real implementation sends `INFO` over RESP.
        Ok(RedisInfo::default())
    }

    /// Return the number of keys in the source database (`DBSIZE`).
    pub async fn dbsize(&self) -> Result<u64> {
        self.ensure_connected()?;
        debug!("fetching DBSIZE from source");
        Ok(0)
    }

    /// Incrementally scan keys matching `pattern` using `SCAN`.
    ///
    /// Returns `(next_cursor, keys)`. A `next_cursor` of `0` means the
    /// full keyspace has been traversed.
    pub async fn scan_keys(
        &self,
        cursor: u64,
        pattern: &str,
        count: u64,
    ) -> Result<(u64, Vec<String>)> {
        self.ensure_connected()?;
        debug!(cursor, pattern, count, "SCAN keys");
        // Placeholder — real implementation sends SCAN command.
        Ok((0, Vec::new()))
    }

    /// Dump a single key's type, value, and TTL.
    pub async fn dump_key(&self, key: &str) -> Result<Option<KeyDump>> {
        self.ensure_connected()?;
        debug!(key, "dumping key from source");
        // Placeholder — real implementation sends TYPE, GET/DUMP, PTTL.
        Ok(None)
    }

    /// Subscribe to keyspace notifications on the source and return a
    /// receiver channel that yields [`KeyspaceEvent`]s.
    pub async fn subscribe_keyspace(&self) -> Result<KeyspaceReceiver> {
        self.ensure_connected()?;
        debug!("subscribing to keyspace notifications");
        let (_tx, rx) = mpsc::channel(256);
        // Placeholder — real implementation sends
        // `CONFIG SET notify-keyspace-events KEA` followed by
        // `PSUBSCRIBE __keyevent@*__:*`.
        warn!("keyspace subscription is not yet wired to a live connection");
        Ok(rx)
    }

    /// Close the connection (idempotent).
    pub async fn disconnect(&mut self) {
        self.connected = false;
        debug!(uri = %self.uri, "disconnected from source Redis");
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    fn ensure_connected(&self) -> Result<()> {
        if !self.connected {
            return Err(ConnectorError::Connection(
                "not connected to source Redis".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_connect_ok() {
        let conn = RedisConnector::connect("redis://localhost:6379").await;
        assert!(conn.is_ok());
    }

    #[tokio::test]
    async fn test_connect_empty_uri() {
        let conn = RedisConnector::connect("").await;
        assert!(conn.is_err());
    }

    #[tokio::test]
    async fn test_info() {
        let conn = RedisConnector::connect("redis://localhost:6379")
            .await
            .unwrap();
        let info = conn.info().await.unwrap();
        assert_eq!(info.role, "master");
    }

    #[tokio::test]
    async fn test_scan_keys_returns_zero_cursor() {
        let conn = RedisConnector::connect("redis://localhost:6379")
            .await
            .unwrap();
        let (cursor, keys) = conn.scan_keys(0, "*", 100).await.unwrap();
        assert_eq!(cursor, 0);
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn test_disconnect() {
        let mut conn = RedisConnector::connect("redis://localhost:6379")
            .await
            .unwrap();
        conn.disconnect().await;
        assert!(conn.info().await.is_err());
    }
}
