//! WebSocket server for real-time dashboard updates
#![allow(dead_code)]

use std::time::Duration;

use super::data_source::DashboardSnapshot;

/// Errors that can occur in the dashboard WebSocket server.
#[derive(Debug, thiserror::Error)]
pub enum DashboardError {
    /// Failed to bind the WebSocket server to the specified port.
    #[error("failed to bind WebSocket server: {0}")]
    BindFailed(String),

    /// Failed to send a message to a connected client.
    #[error("failed to send message: {0}")]
    SendFailed(String),

    /// The WebSocket connection was closed.
    #[error("WebSocket connection closed")]
    Closed,
}

/// WebSocket server that broadcasts dashboard snapshots to connected clients.
pub struct DashboardWebSocket {
    /// Port to bind the WebSocket server on
    port: u16,
    /// Interval between broadcast updates
    interval: Duration,
    /// Number of currently connected clients
    connected_clients: std::sync::atomic::AtomicU32,
}

impl DashboardWebSocket {
    /// Create a new WebSocket server configuration.
    pub fn new(port: u16, interval: Duration) -> Self {
        Self {
            port,
            interval,
            connected_clients: std::sync::atomic::AtomicU32::new(0),
        }
    }

    /// Start the WebSocket server (stub — would use tokio-tungstenite).
    ///
    /// In a full implementation this would:
    /// 1. Bind a TCP listener on `self.port`
    /// 2. Accept WebSocket upgrade requests
    /// 3. Spawn a broadcast task that sends snapshots at `self.interval`
    pub fn start(&self) -> Result<(), DashboardError> {
        // Stub: validate port is available
        if self.port == 0 {
            return Err(DashboardError::BindFailed(
                "port 0 is not allowed".to_string(),
            ));
        }
        // In production, this would spawn a tokio task with tungstenite
        Ok(())
    }

    /// Broadcast a snapshot to all connected WebSocket clients.
    pub fn broadcast_snapshot(&self, snapshot: &DashboardSnapshot) {
        // Serialize the snapshot to JSON for transmission
        let _json = match serde_json::to_string(snapshot) {
            Ok(j) => j,
            Err(_) => return,
        };

        // Stub: in production, iterate over connected clients and send
        let _clients = self
            .connected_clients
            .load(std::sync::atomic::Ordering::Relaxed);
    }

    /// Return the configured port.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Return the configured broadcast interval.
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Return the number of currently connected clients.
    pub fn client_count(&self) -> u32 {
        self.connected_clients
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_websocket_new() {
        let ws = DashboardWebSocket::new(9090, Duration::from_secs(1));
        assert_eq!(ws.port(), 9090);
        assert_eq!(ws.interval(), Duration::from_secs(1));
        assert_eq!(ws.client_count(), 0);
    }

    #[test]
    fn test_websocket_start_stub() {
        let ws = DashboardWebSocket::new(9090, Duration::from_secs(1));
        assert!(ws.start().is_ok());
    }

    #[test]
    fn test_websocket_port_zero_fails() {
        let ws = DashboardWebSocket::new(0, Duration::from_secs(1));
        assert!(ws.start().is_err());
    }

    #[test]
    fn test_broadcast_snapshot() {
        let ws = DashboardWebSocket::new(9090, Duration::from_secs(1));
        let ds = crate::dashboard::data_source::DashboardDataSource::new();
        let snap = ds.collect_snapshot();
        // Should not panic even with no clients
        ws.broadcast_snapshot(&snap);
    }
}
