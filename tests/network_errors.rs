#![allow(clippy::unwrap_used)]
//! Network Error Scenario Integration Tests
//!
//! Tests for network error handling including:
//! - Connection timeouts
//! - Connection drops mid-command
//! - Partial data handling
//! - Graceful error recovery

use bytes::{Bytes, BytesMut};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

use ferrite::protocol::{encode_frame, parse_frame, Frame};

/// Helper to encode a frame to bytes
fn encode_to_bytes(frame: &Frame) -> BytesMut {
    let mut buf = BytesMut::new();
    encode_frame(frame, &mut buf);
    buf
}

// ============================================================================
// Protocol Parsing Error Tests
// ============================================================================

#[test]
fn test_parse_incomplete_frame() {
    // Incomplete simple string (missing \r\n)
    let mut buf = BytesMut::from("+OK");
    let result = parse_frame(&mut buf);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none()); // Should return None for incomplete

    // Incomplete bulk string (missing data)
    let mut buf = BytesMut::from("$6\r\nfoo");
    let result = parse_frame(&mut buf);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    // Incomplete array
    let mut buf = BytesMut::from("*2\r\n$3\r\nfoo");
    let result = parse_frame(&mut buf);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn test_parse_invalid_frame() {
    // Invalid frame type
    let mut buf = BytesMut::from("?invalid\r\n");
    let result = parse_frame(&mut buf);
    assert!(result.is_err());

    // Invalid bulk string length
    let mut buf = BytesMut::from("$-2\r\n");
    let result = parse_frame(&mut buf);
    // Should handle gracefully
    let _ = result;

    // Invalid integer
    let mut buf = BytesMut::from(":notanumber\r\n");
    let result = parse_frame(&mut buf);
    assert!(result.is_err());
}

#[test]
fn test_parse_empty_buffer() {
    let mut buf = BytesMut::new();
    let result = parse_frame(&mut buf);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn test_parse_multiple_frames() {
    let mut buf = BytesMut::from("+OK\r\n+PONG\r\n:42\r\n");

    // Parse first frame
    let frame1 = parse_frame(&mut buf).unwrap();
    assert!(frame1.is_some());

    // Parse second frame
    let frame2 = parse_frame(&mut buf).unwrap();
    assert!(frame2.is_some());

    // Parse third frame
    let frame3 = parse_frame(&mut buf).unwrap();
    assert!(frame3.is_some());

    // No more frames
    let frame4 = parse_frame(&mut buf).unwrap();
    assert!(frame4.is_none());
}

// ============================================================================
// Connection Timeout Tests
// ============================================================================

#[tokio::test]
async fn test_connection_timeout() {
    // Start a listener that doesn't accept connections
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Drop the listener immediately - port now refuses connections
    drop(listener);

    // Attempt to connect with a short timeout
    let result = timeout(Duration::from_millis(100), TcpStream::connect(addr)).await;

    // Should timeout or fail to connect
    assert!(result.is_err() || result.unwrap().is_err());
}

#[tokio::test]
async fn test_read_timeout() {
    // Start a server that accepts but never responds
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        // Accept connection but never respond
        // Just hold the socket open
        tokio::time::sleep(Duration::from_secs(10)).await;
        drop(socket);
    });

    // Connect and send a command
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();

    // Try to read with timeout
    let mut buf = [0u8; 1024];
    let result = timeout(Duration::from_millis(100), stream.read(&mut buf)).await;

    // Should timeout
    assert!(result.is_err());
}

// ============================================================================
// Connection Drop Tests
// ============================================================================

#[tokio::test]
async fn test_server_drops_connection() {
    // Server that closes connection immediately after accept
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        // Immediately drop the connection
        drop(socket);
    });

    // Connect
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Give server time to close
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Try to read - should get EOF or error
    let mut buf = [0u8; 1024];
    let result = stream.read(&mut buf).await;

    // Should get 0 bytes (EOF) or error
    match result {
        Ok(0) => {} // EOF - connection closed
        Ok(_) => panic!("Expected EOF or error"),
        Err(_) => {} // Connection error - also acceptable
    }
}

#[tokio::test]
async fn test_connection_reset_during_write() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        // Read a bit then close
        let mut buf = [0u8; 10];
        let _ = socket.read(&mut buf).await;
        drop(socket);
    });

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Give server time to close after reading
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Try to write a large amount of data
    let large_data = vec![0u8; 1024 * 1024]; // 1MB
    let write_result = stream.write_all(&large_data).await;

    // On some platforms (macOS), write may succeed if data fits in socket buffer
    // but a subsequent read will definitely fail with EOF or error
    if write_result.is_ok() {
        // If write succeeded, verify the connection is actually dead
        let mut buf = [0u8; 1024];
        let read_result = stream.read(&mut buf).await;
        // Should get EOF (0 bytes) or error
        match read_result {
            Ok(0) => {} // EOF - connection closed
            Ok(_) => panic!("Expected EOF or error after writing to closed connection"),
            Err(_) => {} // Connection error - also acceptable
        }
    }
    // If write failed, test passed
}

// ============================================================================
// Partial Data Handling Tests
// ============================================================================

#[tokio::test]
async fn test_partial_command_received() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();

        // Read data
        let mut buf = BytesMut::with_capacity(1024);
        let mut read_buf = [0u8; 1024];

        loop {
            match socket.read(&mut read_buf).await {
                Ok(0) => break, // Connection closed
                Ok(n) => {
                    buf.extend_from_slice(&read_buf[..n]);

                    // Try to parse
                    if let Ok(Some(_frame)) = parse_frame(&mut buf) {
                        // Successfully parsed - send response
                        socket.write_all(b"+OK\r\n").await.unwrap();
                    }
                }
                Err(_) => break,
            }
        }
    });

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Send command in parts
    stream.write_all(b"*1\r\n").await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    stream.write_all(b"$4\r\n").await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    stream.write_all(b"PING\r\n").await.unwrap();

    // Read response
    let mut response = [0u8; 1024];
    let result = timeout(Duration::from_millis(500), stream.read(&mut response)).await;

    assert!(result.is_ok());
    let n = result.unwrap().unwrap();
    assert_eq!(&response[..n], b"+OK\r\n");

    drop(stream);
    let _ = server_handle.await;
}

#[tokio::test]
async fn test_very_large_command() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();

        // Read some data then close
        let mut buf = [0u8; 4096];
        let _ = socket.read(&mut buf).await;

        // Send error response for too-large command
        let _ = socket.write_all(b"-ERR command too large\r\n").await;
    });

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Send a very large bulk string command
    let large_value = vec![b'x'; 100 * 1024]; // 100KB
    let header = format!(
        "*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n${}\r\n",
        large_value.len()
    );

    stream.write_all(header.as_bytes()).await.unwrap();
    let result = stream.write_all(&large_value).await;

    // Writing might succeed or fail depending on buffer sizes
    let _ = result;

    // Try to read response
    let mut response = [0u8; 1024];
    let _ = timeout(Duration::from_millis(500), stream.read(&mut response)).await;
}

// ============================================================================
// Error Recovery Tests
// ============================================================================

#[tokio::test]
async fn test_reconnect_after_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Counter for connections
    let connections = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let conn_clone = connections.clone();

    tokio::spawn(async move {
        loop {
            if let Ok((mut socket, _)) = listener.accept().await {
                let count = conn_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                if count == 0 {
                    // First connection: close immediately
                    drop(socket);
                } else {
                    // Subsequent connections: respond normally
                    let mut buf = [0u8; 1024];
                    if socket.read(&mut buf).await.is_ok() {
                        let _ = socket.write_all(b"+PONG\r\n").await;
                    }
                }
            }
        }
    });

    // First connection attempt - should fail
    let mut stream1 = TcpStream::connect(addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut buf = [0u8; 1024];
    let result = stream1.read(&mut buf).await;
    assert!(matches!(result, Ok(0))); // EOF

    // Reconnect
    let mut stream2 = TcpStream::connect(addr).await.unwrap();
    stream2.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();

    let mut response = [0u8; 1024];
    let result = timeout(Duration::from_millis(500), stream2.read(&mut response)).await;

    assert!(result.is_ok());
    let n = result.unwrap().unwrap();
    assert_eq!(&response[..n], b"+PONG\r\n");
}

// ============================================================================
// Frame Encoding Tests
// ============================================================================

#[test]
fn test_frame_encoding_roundtrip() {
    // Simple string
    let frame = Frame::Simple(Bytes::from("OK"));
    let mut buf = encode_to_bytes(&frame);
    let decoded = parse_frame(&mut buf).unwrap().unwrap();
    assert!(matches!(decoded, Frame::Simple(s) if s == "OK"));

    // Integer
    let frame = Frame::Integer(42);
    let mut buf = encode_to_bytes(&frame);
    let decoded = parse_frame(&mut buf).unwrap().unwrap();
    assert!(matches!(decoded, Frame::Integer(42)));

    // Bulk string
    let frame = Frame::Bulk(Some(Bytes::from("hello")));
    let mut buf = encode_to_bytes(&frame);
    let decoded = parse_frame(&mut buf).unwrap().unwrap();
    assert!(matches!(decoded, Frame::Bulk(Some(s)) if s == "hello"));

    // Null bulk
    let frame = Frame::Bulk(None);
    let mut buf = encode_to_bytes(&frame);
    let decoded = parse_frame(&mut buf).unwrap().unwrap();
    assert!(matches!(decoded, Frame::Bulk(None)));

    // Error
    let frame = Frame::Error(Bytes::from("ERR something wrong"));
    let mut buf = encode_to_bytes(&frame);
    let decoded = parse_frame(&mut buf).unwrap().unwrap();
    assert!(matches!(decoded, Frame::Error(_)));
}

#[test]
fn test_nested_array_encoding() {
    // Array with nested elements
    let inner = vec![Frame::Simple(Bytes::from("OK")), Frame::Integer(42)];
    let outer = vec![
        Frame::Bulk(Some(Bytes::from("key"))),
        Frame::Array(Some(inner)),
    ];
    let frame = Frame::Array(Some(outer));

    let mut buf = encode_to_bytes(&frame);
    let decoded = parse_frame(&mut buf).unwrap().unwrap();

    if let Frame::Array(Some(arr)) = decoded {
        assert_eq!(arr.len(), 2);
        assert!(matches!(&arr[0], Frame::Bulk(Some(k)) if k == "key"));
        if let Frame::Array(Some(inner)) = &arr[1] {
            assert_eq!(inner.len(), 2);
        } else {
            panic!("Expected inner array");
        }
    } else {
        panic!("Expected outer array");
    }
}

// ============================================================================
// Binary Data Tests
// ============================================================================

#[test]
fn test_binary_data_in_frames() {
    // Binary data with null bytes and special characters
    let binary = vec![0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD, b'\r', b'\n'];
    let frame = Frame::Bulk(Some(Bytes::from(binary.clone())));

    let mut buf = encode_to_bytes(&frame);
    let decoded = parse_frame(&mut buf).unwrap().unwrap();

    if let Frame::Bulk(Some(data)) = decoded {
        assert_eq!(data.as_ref(), &binary[..]);
    } else {
        panic!("Expected bulk string");
    }
}

// ============================================================================
// Stress Tests
// ============================================================================

#[tokio::test]
async fn test_rapid_connect_disconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        for _ in 0..100 {
            if let Ok((socket, _)) = listener.accept().await {
                drop(socket);
            }
        }
    });

    // Rapidly connect and disconnect
    for _ in 0..100 {
        if let Ok(stream) = TcpStream::connect(addr).await {
            drop(stream);
        }
    }

    let _ = server_handle.await;
}
