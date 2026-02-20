//! Shared test utilities for Ferrite integration tests.
//!
//! Import via `mod common;` in integration test files:
//! ```rust,ignore
//! mod common;
//! use common::*;
//! ```

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use ferrite::config::Config;
use ferrite::protocol::Frame;
use ferrite::server::Server;
use ferrite::storage::Store;

// ============================================================================
// Store helpers
// ============================================================================

/// Create a new `Store` with 16 databases (the Redis default).
#[allow(dead_code)]
pub fn new_store() -> Store {
    Store::new(16)
}

/// Create a new `Store` wrapped in `Arc` for concurrent tests.
#[allow(dead_code)]
pub fn new_shared_store() -> std::sync::Arc<Store> {
    std::sync::Arc::new(Store::new(16))
}

// ============================================================================
// Server helpers
// ============================================================================

/// Create a test `Config` with persistence/metrics disabled and a random port.
#[allow(dead_code)]
pub fn default_test_config(port: u16) -> Config {
    let mut config = Config::default();
    config.server.port = port;
    config.server.bind = "127.0.0.1".to_string();
    config.persistence.aof_enabled = false;
    config.persistence.checkpoint_enabled = false;
    config.metrics.enabled = false;
    config
}

/// Start a Ferrite server on a random port and return the port number.
/// The server runs in a background tokio task.
#[allow(dead_code)]
pub async fn start_test_server() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("should bind to random port");
    let port = listener.local_addr().expect("should have local addr").port();
    drop(listener);

    let config = default_test_config(port);
    let server = Server::new(config).await.expect("server should start");
    tokio::spawn(async move {
        let _ = server.run().await;
    });

    wait_for_server(port).await;
    port
}

/// Poll until the server at `port` is accepting TCP connections.
/// Panics if the server does not start within ~2.5 seconds.
#[allow(dead_code)]
pub async fn wait_for_server(port: u16) {
    for _ in 0..50 {
        if TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .is_ok()
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("Server did not start within 2.5 seconds on port {port}");
}

// ============================================================================
// TCP / RESP helpers
// ============================================================================

/// Connect to a test server and return a buffered stream.
#[allow(dead_code)]
pub async fn connect(port: u16) -> BufReader<TcpStream> {
    let stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .expect("should connect to test server");
    BufReader::new(stream)
}

/// Send a RESP array command and read the first response.
#[allow(dead_code)]
pub async fn send_command(stream: &mut BufReader<TcpStream>, args: &[&str]) -> String {
    let inner = stream.get_mut();

    let mut cmd = format!("*{}\r\n", args.len());
    for arg in args {
        cmd.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }
    inner
        .write_all(cmd.as_bytes())
        .await
        .expect("should write command");
    inner.flush().await.expect("should flush");

    read_resp_value(stream).await
}

/// Read a single RESP value from the stream.
#[allow(dead_code)]
pub async fn read_resp_value(stream: &mut BufReader<TcpStream>) -> String {
    let mut line = String::new();
    let read = tokio::time::timeout(Duration::from_secs(5), stream.read_line(&mut line))
        .await
        .expect("read should not timeout")
        .expect("read should succeed");
    if read == 0 {
        return String::new();
    }

    match line.chars().next() {
        Some('+') | Some('-') | Some(':') => line,
        Some('$') => {
            let len: i64 = line[1..].trim().parse().expect("should parse bulk length");
            if len < 0 {
                return line; // null bulk string
            }
            let mut buf = vec![0u8; (len as usize) + 2]; // +2 for \r\n
            stream
                .read_exact(&mut buf)
                .await
                .expect("should read bulk data");
            let value = String::from_utf8_lossy(&buf[..len as usize]).to_string();
            format!("${}\r\n{}\r\n", len, value)
        }
        Some('*') => {
            let count: i64 = line[1..].trim().parse().expect("should parse array count");
            if count < 0 {
                return line; // null array
            }
            let mut result = line.clone();
            for _ in 0..count {
                let part = Box::pin(read_resp_value(stream)).await;
                result.push_str(&part);
            }
            result
        }
        _ => line,
    }
}

// ============================================================================
// Frame encoding helper
// ============================================================================

/// Encode a RESP `Frame` into a `BytesMut` buffer for network tests.
#[allow(dead_code)]
pub fn encode_frame(frame: &Frame) -> BytesMut {
    let mut buf = BytesMut::new();
    match frame {
        Frame::Simple(s) => {
            buf.extend_from_slice(b"+");
            buf.extend_from_slice(s.as_ref());
            buf.extend_from_slice(b"\r\n");
        }
        Frame::Error(s) => {
            buf.extend_from_slice(b"-");
            buf.extend_from_slice(s.as_ref());
            buf.extend_from_slice(b"\r\n");
        }
        Frame::Integer(n) => {
            buf.extend_from_slice(format!(":{}\r\n", n).as_bytes());
        }
        Frame::Bulk(Some(data)) => {
            buf.extend_from_slice(format!("${}\r\n", data.len()).as_bytes());
            buf.extend_from_slice(data);
            buf.extend_from_slice(b"\r\n");
        }
        Frame::Bulk(None) => {
            buf.extend_from_slice(b"$-1\r\n");
        }
        Frame::Array(Some(items)) => {
            buf.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
            for item in items {
                buf.extend_from_slice(&encode_frame(item));
            }
        }
        Frame::Array(None) => {
            buf.extend_from_slice(b"*-1\r\n");
        }
        _ => {
            // Other RESP3 types not handled in test helper
            buf.extend_from_slice(b"-ERR unsupported frame\r\n");
        }
    }
    buf
}

// ============================================================================
// Assertion helpers
// ============================================================================

/// Assert a RESP response starts with the given prefix (e.g., "+OK", "-ERR").
#[allow(dead_code)]
pub fn assert_resp_prefix(response: &str, prefix: &str) {
    assert!(
        response.starts_with(prefix),
        "Expected RESP response starting with '{}', got: '{}'",
        prefix,
        response.trim()
    );
}

/// Assert a RESP response is "+OK\r\n".
#[allow(dead_code)]
pub fn assert_ok(response: &str) {
    assert_resp_prefix(response, "+OK");
}

/// Assert a RESP response is a bulk string containing the expected value.
#[allow(dead_code)]
pub fn assert_bulk_string(response: &str, expected: &str) {
    let expected_resp = format!("${}\r\n{}\r\n", expected.len(), expected);
    assert_eq!(
        response, expected_resp,
        "Expected bulk string '{}', got: '{}'",
        expected, response
    );
}

/// Assert a RESP response is a null bulk string.
#[allow(dead_code)]
pub fn assert_null(response: &str) {
    assert_resp_prefix(response, "$-1");
}

/// Create a `Bytes` value from a string literal.
#[allow(dead_code)]
pub fn b(s: &str) -> Bytes {
    Bytes::from(s.to_string())
}
