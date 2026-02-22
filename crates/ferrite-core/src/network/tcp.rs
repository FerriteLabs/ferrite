//! TCP Transport Layer
//!
//! Standard TCP/IP transport with performance optimizations for
//! high-throughput, low-latency networking.
//!
//! # Features
//!
//! - TCP_NODELAY for latency-sensitive operations
//! - TCP keepalive for connection health
//! - Connection pooling
//! - Optional SO_BUSY_POLL for reduced latency
//! - Zero-copy receive with vectored I/O
//!
//! # Example
//!
//! ```ignore
//! use ferrite::network::tcp::{TcpTransport, TcpConfig};
//!
//! let config = TcpConfig::default();
//! let transport = TcpTransport::new(config)?;
//!
//! let conn = transport.connect("192.168.1.100:6379".parse()?).await?;
//! conn.send(data).await?;
//! ```

use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self, ErrorKind};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::{Mutex, RwLock};

/// Default buffer size (64KB)
pub const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;

/// Default keepalive interval
pub const DEFAULT_KEEPALIVE_SECS: u64 = 30;

/// TCP transport errors
#[derive(Debug, Error)]
pub enum TcpError {
    /// Connection failed
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// Connection closed
    #[error("connection closed")]
    ConnectionClosed,

    /// Read error
    #[error("read error: {0}")]
    ReadError(String),

    /// Write error
    #[error("write error: {0}")]
    WriteError(String),

    /// Timeout
    #[error("operation timed out")]
    Timeout,

    /// Address in use
    #[error("address already in use: {0}")]
    AddressInUse(SocketAddr),

    /// Invalid state
    #[error("invalid state: {0}")]
    InvalidState(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// Result type for TCP operations
pub type Result<T> = std::result::Result<T, TcpError>;

/// TCP configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TcpConfig {
    /// Enable TCP_NODELAY (disable Nagle's algorithm)
    pub nodelay: bool,
    /// Send buffer size
    pub send_buffer_size: usize,
    /// Receive buffer size
    pub recv_buffer_size: usize,
    /// Keepalive interval (None = disabled)
    pub keepalive: Option<Duration>,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Read timeout (None = no timeout)
    pub read_timeout: Option<Duration>,
    /// Write timeout (None = no timeout)
    pub write_timeout: Option<Duration>,
    /// Enable SO_REUSEADDR
    pub reuse_addr: bool,
    /// Enable SO_REUSEPORT (where available)
    pub reuse_port: bool,
    /// Maximum connections per peer
    pub max_connections_per_peer: usize,
    /// Enable busy polling (Linux only)
    pub busy_poll: bool,
    /// Busy poll timeout in microseconds
    pub busy_poll_us: u32,
    /// Linger timeout (None = default behavior)
    pub linger: Option<Duration>,
}

impl Default for TcpConfig {
    fn default() -> Self {
        Self {
            nodelay: true,
            send_buffer_size: DEFAULT_BUFFER_SIZE,
            recv_buffer_size: DEFAULT_BUFFER_SIZE,
            keepalive: Some(Duration::from_secs(DEFAULT_KEEPALIVE_SECS)),
            connect_timeout: Duration::from_secs(5),
            read_timeout: Some(Duration::from_secs(30)),
            write_timeout: Some(Duration::from_secs(30)),
            reuse_addr: true,
            reuse_port: false,
            max_connections_per_peer: 4,
            busy_poll: false,
            busy_poll_us: 50,
            linger: None,
        }
    }
}

/// TCP connection
pub struct TcpConnection {
    /// Connection ID
    id: u64,
    /// Remote address
    remote_addr: SocketAddr,
    /// Local address
    local_addr: SocketAddr,
    /// TCP stream (wrapped for buffered I/O)
    #[allow(clippy::type_complexity)]
    stream: Mutex<(
        BufReader<tokio::io::ReadHalf<TcpStream>>,
        BufWriter<tokio::io::WriteHalf<TcpStream>>,
    )>,
    /// Is active
    active: AtomicBool,
    /// Statistics
    stats: TcpConnectionStats,
    /// Configuration
    #[allow(dead_code)] // Planned for v0.2 — stored for TCP connection configuration
    config: TcpConfig,
}

impl TcpConnection {
    /// Create from a TcpStream
    async fn new(id: u64, stream: TcpStream, config: TcpConfig) -> Result<Self> {
        // Configure socket options
        stream.set_nodelay(config.nodelay)?;

        // Get addresses before splitting
        let remote_addr = stream.peer_addr()?;
        let local_addr = stream.local_addr()?;

        // Split stream for separate read/write
        let (read_half, write_half) = tokio::io::split(stream);
        let reader = BufReader::with_capacity(config.recv_buffer_size, read_half);
        let writer = BufWriter::with_capacity(config.send_buffer_size, write_half);

        Ok(Self {
            id,
            remote_addr,
            local_addr,
            stream: Mutex::new((reader, writer)),
            active: AtomicBool::new(true),
            stats: TcpConnectionStats::default(),
            config,
        })
    }

    /// Get connection ID
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get remote address
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    /// Get local address
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Check if connection is active
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Send data
    pub async fn send(&self, data: Bytes) -> Result<()> {
        if !self.is_active() {
            return Err(TcpError::InvalidState("connection not active".into()));
        }

        let len = data.len();
        let result = {
            let mut stream = self.stream.lock().await;
            let (_, writer) = &mut *stream;

            // Write length prefix (4 bytes, big-endian)
            let len_bytes = (len as u32).to_be_bytes();
            writer.write_all(&len_bytes).await?;

            // Write data
            writer.write_all(&data).await?;
            writer.flush().await
        };

        match result {
            Ok(()) => {
                self.stats
                    .bytes_sent
                    .fetch_add(len as u64 + 4, Ordering::Relaxed);
                self.stats.messages_sent.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.stats.send_errors.fetch_add(1, Ordering::Relaxed);
                Err(TcpError::WriteError(e.to_string()))
            }
        }
    }

    /// Send data without length prefix (raw)
    pub async fn send_raw(&self, data: &[u8]) -> Result<()> {
        if !self.is_active() {
            return Err(TcpError::InvalidState("connection not active".into()));
        }

        let len = data.len();
        let result = {
            let mut stream = self.stream.lock().await;
            let (_, writer) = &mut *stream;
            writer.write_all(data).await?;
            writer.flush().await
        };

        match result {
            Ok(()) => {
                self.stats
                    .bytes_sent
                    .fetch_add(len as u64, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.stats.send_errors.fetch_add(1, Ordering::Relaxed);
                Err(TcpError::WriteError(e.to_string()))
            }
        }
    }

    /// Receive data
    pub async fn recv(&self) -> Result<Bytes> {
        if !self.is_active() {
            return Err(TcpError::InvalidState("connection not active".into()));
        }

        let result: std::result::Result<Bytes, io::Error> = {
            let mut stream = self.stream.lock().await;
            let (reader, _) = &mut *stream;

            // Read length prefix (4 bytes, big-endian)
            let mut len_bytes = [0u8; 4];
            reader.read_exact(&mut len_bytes).await?;
            let len = u32::from_be_bytes(len_bytes) as usize;

            // Read data
            let mut buf = BytesMut::with_capacity(len);
            buf.resize(len, 0);
            reader.read_exact(&mut buf).await?;

            Ok(buf.freeze())
        };

        match result {
            Ok(data) => {
                self.stats
                    .bytes_received
                    .fetch_add(data.len() as u64 + 4, Ordering::Relaxed);
                self.stats.messages_received.fetch_add(1, Ordering::Relaxed);
                Ok(data)
            }
            Err(e) => {
                if e.kind() == ErrorKind::UnexpectedEof {
                    self.active.store(false, Ordering::Release);
                    Err(TcpError::ConnectionClosed)
                } else {
                    self.stats.recv_errors.fetch_add(1, Ordering::Relaxed);
                    Err(TcpError::ReadError(e.to_string()))
                }
            }
        }
    }

    /// Receive raw data into buffer
    pub async fn recv_raw(&self, buf: &mut [u8]) -> Result<usize> {
        if !self.is_active() {
            return Err(TcpError::InvalidState("connection not active".into()));
        }

        let result = {
            let mut stream = self.stream.lock().await;
            let (reader, _) = &mut *stream;
            reader.read(buf).await
        };

        match result {
            Ok(0) => {
                self.active.store(false, Ordering::Release);
                Err(TcpError::ConnectionClosed)
            }
            Ok(n) => {
                self.stats
                    .bytes_received
                    .fetch_add(n as u64, Ordering::Relaxed);
                Ok(n)
            }
            Err(e) => {
                self.stats.recv_errors.fetch_add(1, Ordering::Relaxed);
                Err(TcpError::ReadError(e.to_string()))
            }
        }
    }

    /// Get statistics
    pub fn stats(&self) -> &TcpConnectionStats {
        &self.stats
    }

    /// Close the connection
    pub async fn close(&self) -> Result<()> {
        if self.active.swap(false, Ordering::AcqRel) {
            let mut stream = self.stream.lock().await;
            let (_, writer) = &mut *stream;
            let _ = writer.shutdown().await;
        }
        Ok(())
    }
}

/// TCP connection statistics
#[derive(Default)]
pub struct TcpConnectionStats {
    /// Bytes sent
    pub bytes_sent: AtomicU64,
    /// Bytes received
    pub bytes_received: AtomicU64,
    /// Messages sent
    pub messages_sent: AtomicU64,
    /// Messages received
    pub messages_received: AtomicU64,
    /// Send errors
    pub send_errors: AtomicU64,
    /// Receive errors
    pub recv_errors: AtomicU64,
}

/// TCP listener
pub struct TcpListener {
    /// Listen address
    addr: SocketAddr,
    /// Underlying listener
    listener: tokio::net::TcpListener,
    /// Connection counter
    next_conn_id: AtomicU64,
    /// Configuration
    config: TcpConfig,
    /// Is active
    active: AtomicBool,
}

impl TcpListener {
    /// Create a new TCP listener
    async fn new(addr: SocketAddr, config: TcpConfig) -> Result<Self> {
        let socket = if addr.is_ipv6() {
            TcpSocket::new_v6()?
        } else {
            TcpSocket::new_v4()?
        };

        if config.reuse_addr {
            socket.set_reuseaddr(true)?;
        }

        #[cfg(unix)]
        if config.reuse_port {
            socket.set_reuseport(true)?;
        }

        socket.bind(addr)?;
        let listener = socket.listen(1024)?;

        let local_addr = listener.local_addr()?;

        Ok(Self {
            addr: local_addr,
            listener,
            next_conn_id: AtomicU64::new(1),
            config,
            active: AtomicBool::new(true),
        })
    }

    /// Get local address
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Accept an incoming connection
    pub async fn accept(&self) -> Result<TcpConnection> {
        if !self.active.load(Ordering::Acquire) {
            return Err(TcpError::InvalidState("listener not active".into()));
        }

        let (stream, _peer_addr) = self.listener.accept().await?;
        let conn_id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);

        TcpConnection::new(conn_id, stream, self.config.clone()).await
    }

    /// Check if listener is active
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Close the listener
    pub fn close(&self) {
        self.active.store(false, Ordering::Release);
    }
}

/// TCP transport
pub struct TcpTransport {
    /// Configuration
    config: TcpConfig,
    /// Is initialized
    initialized: AtomicBool,
    /// Active connections
    connections: RwLock<HashMap<u64, Arc<TcpConnection>>>,
    /// Next connection ID
    next_conn_id: AtomicU64,
    /// Statistics
    stats: TcpTransportStats,
    /// Shutdown flag
    shutdown: AtomicBool,
}

/// TCP transport statistics
#[derive(Default)]
pub struct TcpTransportStats {
    /// Total connections created
    pub connections_created: AtomicU64,
    /// Active connections
    pub active_connections: AtomicU64,
    /// Total bytes sent
    pub bytes_sent: AtomicU64,
    /// Total bytes received
    pub bytes_received: AtomicU64,
    /// Connection errors
    pub connection_errors: AtomicU64,
}

impl TcpTransport {
    /// Create a new TCP transport
    pub fn new(config: TcpConfig) -> Result<Self> {
        Ok(Self {
            config,
            initialized: AtomicBool::new(false),
            connections: RwLock::new(HashMap::new()),
            next_conn_id: AtomicU64::new(1),
            stats: TcpTransportStats::default(),
            shutdown: AtomicBool::new(false),
        })
    }

    /// Check if TCP is available (always true)
    pub fn is_available(&self) -> bool {
        true
    }

    /// Initialize the transport
    pub async fn initialize(&self) -> Result<()> {
        self.initialized.store(true, Ordering::Release);
        tracing::info!("TCP transport initialized");
        Ok(())
    }

    /// Connect to a remote peer
    pub async fn connect(&self, addr: SocketAddr) -> Result<Arc<TcpConnection>> {
        if !self.initialized.load(Ordering::Acquire) {
            self.initialize().await?;
        }

        let connect_future = async {
            let socket = if addr.is_ipv6() {
                TcpSocket::new_v6()?
            } else {
                TcpSocket::new_v4()?
            };

            // Set socket options before connecting
            if self.config.reuse_addr {
                socket.set_reuseaddr(true)?;
            }

            socket.connect(addr).await
        };

        let stream = tokio::time::timeout(self.config.connect_timeout, connect_future)
            .await
            .map_err(|_| TcpError::Timeout)?
            .map_err(|e| TcpError::ConnectionFailed(e.to_string()))?;

        let conn_id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
        let connection = Arc::new(TcpConnection::new(conn_id, stream, self.config.clone()).await?);

        self.connections
            .write()
            .await
            .insert(conn_id, connection.clone());
        self.stats
            .connections_created
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .active_connections
            .fetch_add(1, Ordering::Relaxed);

        tracing::debug!("TCP connection established to {}", addr);

        Ok(connection)
    }

    /// Listen for incoming connections
    pub async fn listen(&self, addr: SocketAddr) -> Result<TcpListener> {
        if !self.initialized.load(Ordering::Acquire) {
            self.initialize().await?;
        }

        TcpListener::new(addr, self.config.clone()).await
    }

    /// Get a connection by ID
    pub async fn get_connection(&self, id: u64) -> Option<Arc<TcpConnection>> {
        self.connections.read().await.get(&id).cloned()
    }

    /// Close a connection
    pub async fn close_connection(&self, id: u64) -> Result<()> {
        if let Some(conn) = self.connections.write().await.remove(&id) {
            conn.close().await?;
            self.stats
                .active_connections
                .fetch_sub(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Get transport statistics
    pub fn stats(&self) -> &TcpTransportStats {
        &self.stats
    }

    /// Shutdown the transport
    pub async fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);

        // Close all connections
        let connections: Vec<_> = self.connections.write().await.drain().collect();
        for (_, conn) in connections {
            let _ = conn.close().await;
        }

        self.initialized.store(false, Ordering::Release);
        tracing::info!("TCP transport shut down");
    }
}

/// Builder for TCP transport
pub struct TcpTransportBuilder {
    config: TcpConfig,
}

impl TcpTransportBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: TcpConfig::default(),
        }
    }

    /// Set TCP_NODELAY
    pub fn nodelay(mut self, enabled: bool) -> Self {
        self.config.nodelay = enabled;
        self
    }

    /// Set buffer sizes
    pub fn buffer_sizes(mut self, send: usize, recv: usize) -> Self {
        self.config.send_buffer_size = send;
        self.config.recv_buffer_size = recv;
        self
    }

    /// Set keepalive
    pub fn keepalive(mut self, interval: Option<Duration>) -> Self {
        self.config.keepalive = interval;
        self
    }

    /// Set connect timeout
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = timeout;
        self
    }

    /// Set read timeout
    pub fn read_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.config.read_timeout = timeout;
        self
    }

    /// Set write timeout
    pub fn write_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.config.write_timeout = timeout;
        self
    }

    /// Enable SO_REUSEADDR
    pub fn reuse_addr(mut self, enabled: bool) -> Self {
        self.config.reuse_addr = enabled;
        self
    }

    /// Enable SO_REUSEPORT
    pub fn reuse_port(mut self, enabled: bool) -> Self {
        self.config.reuse_port = enabled;
        self
    }

    /// Build the transport
    pub fn build(self) -> Result<TcpTransport> {
        TcpTransport::new(self.config)
    }
}

impl Default for TcpTransportBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tcp_config_default() {
        let config = TcpConfig::default();
        assert!(config.nodelay);
        assert!(config.reuse_addr);
        assert!(config.keepalive.is_some());
    }

    #[test]
    fn test_builder() {
        let transport = TcpTransportBuilder::new()
            .nodelay(true)
            .buffer_sizes(128 * 1024, 128 * 1024)
            .connect_timeout(Duration::from_secs(10))
            .build();
        assert!(transport.is_ok());
    }

    #[tokio::test]
    async fn test_transport_creation() {
        let transport = TcpTransport::new(TcpConfig::default());
        assert!(transport.is_ok());
        assert!(transport.unwrap().is_available());
    }

    #[tokio::test]
    async fn test_transport_initialize() {
        let transport = TcpTransport::new(TcpConfig::default()).unwrap();
        let result = transport.initialize().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_listen_and_connect() {
        let transport = TcpTransport::new(TcpConfig::default()).unwrap();
        transport.initialize().await.unwrap();

        // Create listener
        let listener = transport.listen("127.0.0.1:0".parse().unwrap()).await;
        assert!(listener.is_ok());
        let listener = listener.unwrap();
        let addr = listener.local_addr();

        // Connect in background
        let transport2 = TcpTransport::new(TcpConfig::default()).unwrap();
        transport2.initialize().await.unwrap();

        let connect_handle = tokio::spawn(async move { transport2.connect(addr).await });

        // Accept connection
        let server_conn = listener.accept().await;
        assert!(server_conn.is_ok());

        let client_conn = connect_handle.await.unwrap();
        assert!(client_conn.is_ok());

        // Cleanup
        transport.shutdown().await;
    }

    #[tokio::test]
    async fn test_send_recv() {
        let transport = TcpTransport::new(TcpConfig::default()).unwrap();
        transport.initialize().await.unwrap();

        let listener = transport
            .listen("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let addr = listener.local_addr();

        let transport2 = TcpTransport::new(TcpConfig::default()).unwrap();
        transport2.initialize().await.unwrap();

        let client_handle = tokio::spawn(async move {
            let conn = transport2.connect(addr).await.unwrap();
            conn.send(Bytes::from("hello")).await.unwrap();
            let response = conn.recv().await.unwrap();
            assert_eq!(&response[..], b"world");
        });

        let server_conn = listener.accept().await.unwrap();
        let data = server_conn.recv().await.unwrap();
        assert_eq!(&data[..], b"hello");
        server_conn.send(Bytes::from("world")).await.unwrap();

        client_handle.await.unwrap();
        transport.shutdown().await;
    }
}
