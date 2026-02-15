//! Connection handling
//!
//! This module implements buffered I/O for client connections.

use std::net::SocketAddr;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::error::{FerriteError, Result};
use crate::protocol::{encode_frame, parse_frame, Frame, ParseError};

/// Default buffer size (4KB)
const DEFAULT_CONNECTION_BUFFER_SIZE: usize = 4 * 1024;

/// RESP protocol version
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProtocolVersion {
    /// RESP2 (default for backward compatibility)
    #[default]
    Resp2,
    /// RESP3 (modern protocol with richer types)
    Resp3,
}

impl ProtocolVersion {
    /// Create from version number
    pub fn from_version(v: u8) -> Option<Self> {
        match v {
            2 => Some(Self::Resp2),
            3 => Some(Self::Resp3),
            _ => None,
        }
    }

    /// Get version number
    pub fn version_number(&self) -> u8 {
        match self {
            Self::Resp2 => 2,
            Self::Resp3 => 3,
        }
    }

    /// Check if this is RESP3
    pub fn is_resp3(&self) -> bool {
        matches!(self, Self::Resp3)
    }
}

/// A connection to a client
pub struct Connection {
    /// The TCP stream
    stream: TcpStream,

    /// Read buffer
    read_buf: BytesMut,

    /// Write buffer
    write_buf: BytesMut,

    /// Selected database index (0-15)
    pub database: u8,

    /// Protocol version (RESP2 or RESP3)
    pub protocol_version: ProtocolVersion,

    /// Client name (set via CLIENT SETNAME or HELLO SETNAME)
    pub client_name: Option<String>,

    /// Remote peer address
    pub peer_addr: Option<SocketAddr>,
}

impl Connection {
    /// Create a new connection
    pub fn new(stream: TcpStream) -> Self {
        let peer_addr = stream.peer_addr().ok();
        Self {
            stream,
            read_buf: BytesMut::with_capacity(DEFAULT_CONNECTION_BUFFER_SIZE),
            write_buf: BytesMut::with_capacity(DEFAULT_CONNECTION_BUFFER_SIZE),
            database: 0,
            protocol_version: ProtocolVersion::default(),
            client_name: None,
            peer_addr,
        }
    }

    /// Set the protocol version
    pub fn set_protocol_version(&mut self, version: ProtocolVersion) {
        self.protocol_version = version;
    }

    /// Set the client name
    pub fn set_client_name(&mut self, name: Option<String>) {
        self.client_name = name;
    }

    /// Read a frame from the connection
    ///
    /// Returns `Ok(Some(frame))` if a frame was successfully read,
    /// `Ok(None)` if the connection was closed cleanly,
    /// or `Err` if an error occurred.
    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            // Try to parse a frame from the buffer
            match parse_frame(&mut self.read_buf) {
                Ok(Some(frame)) => return Ok(Some(frame)),
                Ok(None) => {
                    // Need more data
                }
                Err(ParseError::Incomplete) => {
                    // Need more data
                }
                Err(e) => {
                    return Err(FerriteError::Protocol(e.to_string()));
                }
            }

            // Read more data from the socket
            let n = self.stream.read_buf(&mut self.read_buf).await?;

            if n == 0 {
                // Connection closed
                if self.read_buf.is_empty() {
                    return Ok(None);
                } else {
                    return Err(FerriteError::ConnectionClosed);
                }
            }
        }
    }

    /// Write a frame to the connection (immediate write)
    ///
    /// This encodes the frame and writes it immediately to the socket.
    /// For pipelined commands, consider using `buffer_frame()` and `flush_buffered()`
    /// for better throughput.
    pub async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        encode_frame(frame, &mut self.write_buf);
        self.stream.write_all(&self.write_buf).await?;
        self.write_buf.clear();
        Ok(())
    }

    /// Buffer a frame for later writing (batch write support)
    ///
    /// This encodes the frame into the write buffer without flushing to the socket.
    /// Call `flush_buffered()` to send all buffered frames at once.
    /// This is more efficient for pipelined commands as it reduces syscalls.
    #[inline]
    pub fn buffer_frame(&mut self, frame: &Frame) {
        encode_frame(frame, &mut self.write_buf);
    }

    /// Flush all buffered frames to the connection
    ///
    /// This writes all buffered frame data to the socket and clears the buffer.
    /// Used in conjunction with `buffer_frame()` for batch writes.
    pub async fn flush_buffered(&mut self) -> Result<()> {
        if !self.write_buf.is_empty() {
            self.stream.write_all(&self.write_buf).await?;
            self.write_buf.clear();
        }
        Ok(())
    }

    /// Check if there is pending data in the read buffer
    ///
    /// Returns true if there is unprocessed data in the read buffer,
    /// indicating more pipelined commands may be available.
    #[inline]
    pub fn has_pending_data(&self) -> bool {
        !self.read_buf.is_empty()
    }

    /// Try to parse a frame from the existing buffer without reading from socket
    ///
    /// Returns `Ok(Some(frame))` if a complete frame is available,
    /// `Ok(None)` if more data is needed (buffer exhausted),
    /// or `Err` if the data is malformed.
    pub fn try_parse_buffered(&mut self) -> Result<Option<Frame>> {
        match parse_frame(&mut self.read_buf) {
            Ok(Some(frame)) => Ok(Some(frame)),
            Ok(None) | Err(ParseError::Incomplete) => Ok(None),
            Err(e) => Err(FerriteError::Protocol(e.to_string())),
        }
    }

    /// Flush the write buffer
    pub async fn flush(&mut self) -> Result<()> {
        self.stream.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn test_protocol_version_from_version() {
        assert_eq!(
            ProtocolVersion::from_version(2),
            Some(ProtocolVersion::Resp2)
        );
        assert_eq!(
            ProtocolVersion::from_version(3),
            Some(ProtocolVersion::Resp3)
        );
        assert_eq!(ProtocolVersion::from_version(1), None);
        assert_eq!(ProtocolVersion::from_version(4), None);
    }

    #[test]
    fn test_protocol_version_number() {
        assert_eq!(ProtocolVersion::Resp2.version_number(), 2);
        assert_eq!(ProtocolVersion::Resp3.version_number(), 3);
    }

    #[test]
    fn test_protocol_version_is_resp3() {
        assert!(!ProtocolVersion::Resp2.is_resp3());
        assert!(ProtocolVersion::Resp3.is_resp3());
    }

    #[test]
    fn test_protocol_version_default() {
        let version = ProtocolVersion::default();
        assert_eq!(version, ProtocolVersion::Resp2);
    }

    #[tokio::test]
    async fn test_connection_database_default() {
        // We can't easily test the full connection without a real socket,
        // but we can test the default state
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_handle =
            tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });

        let (stream, _) = listener.accept().await.unwrap();
        let conn = Connection::new(stream);
        assert_eq!(conn.database, 0);

        // Clean up
        let _ = client_handle.await;
    }

    #[tokio::test]
    async fn test_connection_protocol_version_default() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_handle =
            tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });

        let (stream, _) = listener.accept().await.unwrap();
        let conn = Connection::new(stream);
        assert_eq!(conn.protocol_version, ProtocolVersion::Resp2);
        assert!(conn.client_name.is_none());

        let _ = client_handle.await;
    }

    #[tokio::test]
    async fn test_connection_set_protocol_version() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_handle =
            tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });

        let (stream, _) = listener.accept().await.unwrap();
        let mut conn = Connection::new(stream);

        conn.set_protocol_version(ProtocolVersion::Resp3);
        assert_eq!(conn.protocol_version, ProtocolVersion::Resp3);
        assert!(conn.protocol_version.is_resp3());

        let _ = client_handle.await;
    }

    #[tokio::test]
    async fn test_connection_set_client_name() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_handle =
            tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });

        let (stream, _) = listener.accept().await.unwrap();
        let mut conn = Connection::new(stream);

        conn.set_client_name(Some("my-client".to_string()));
        assert_eq!(conn.client_name, Some("my-client".to_string()));

        conn.set_client_name(None);
        assert!(conn.client_name.is_none());

        let _ = client_handle.await;
    }

    #[tokio::test]
    async fn test_buffer_frame_accumulates() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_handle =
            tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });

        let (stream, _) = listener.accept().await.unwrap();
        let mut conn = Connection::new(stream);

        // Buffer multiple frames
        conn.buffer_frame(&Frame::simple("OK"));
        let first_len = conn.write_buf.len();
        assert!(first_len > 0);

        conn.buffer_frame(&Frame::simple("PONG"));
        let second_len = conn.write_buf.len();
        assert!(second_len > first_len);

        conn.buffer_frame(&Frame::Integer(42));
        let third_len = conn.write_buf.len();
        assert!(third_len > second_len);

        let _ = client_handle.await;
    }

    #[tokio::test]
    async fn test_flush_buffered_clears_buffer() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_handle = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            // Read the data sent by flush_buffered
            let mut buf = [0u8; 128];
            use tokio::io::AsyncReadExt;
            let _ = stream.read(&mut buf).await;
        });

        let (stream, _) = listener.accept().await.unwrap();
        let mut conn = Connection::new(stream);

        // Buffer frames
        conn.buffer_frame(&Frame::simple("OK"));
        conn.buffer_frame(&Frame::simple("PONG"));
        assert!(!conn.write_buf.is_empty());

        // Flush should clear the buffer
        conn.flush_buffered().await.unwrap();
        assert!(conn.write_buf.is_empty());

        let _ = client_handle.await;
    }

    #[tokio::test]
    async fn test_flush_buffered_empty_is_noop() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_handle =
            tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });

        let (stream, _) = listener.accept().await.unwrap();
        let mut conn = Connection::new(stream);

        // Flush empty buffer should succeed
        assert!(conn.write_buf.is_empty());
        conn.flush_buffered().await.unwrap();
        assert!(conn.write_buf.is_empty());

        let _ = client_handle.await;
    }

    #[tokio::test]
    async fn test_has_pending_data() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_handle =
            tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });

        let (stream, _) = listener.accept().await.unwrap();
        let conn = Connection::new(stream);

        // Initially no pending data
        assert!(!conn.has_pending_data());

        let _ = client_handle.await;
    }

    #[tokio::test]
    async fn test_batch_write_integration() {
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_handle = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            let mut buf = vec![0u8; 256];
            let n = stream.read(&mut buf).await.unwrap();
            buf.truncate(n);
            buf
        });

        let (stream, _) = listener.accept().await.unwrap();
        let mut conn = Connection::new(stream);

        // Buffer multiple frames and flush together
        conn.buffer_frame(&Frame::simple("OK"));
        conn.buffer_frame(&Frame::Integer(100));
        conn.buffer_frame(&Frame::Bulk(Some(bytes::Bytes::from("hello"))));
        conn.flush_buffered().await.unwrap();

        // Verify client received data
        let received = client_handle.await.unwrap();
        // Should contain "+OK\r\n:100\r\n$5\r\nhello\r\n"
        let received_str = String::from_utf8_lossy(&received);
        assert!(received_str.contains("+OK\r\n"));
        assert!(received_str.contains(":100\r\n"));
        assert!(received_str.contains("$5\r\nhello\r\n"));
    }
}
