#![allow(clippy::unwrap_used)]
//! End-to-end server integration tests
//!
//! These tests boot a real Ferrite server on a random port and exercise it
//! over TCP using the RESP protocol, verifying the full stack works from
//! network to storage and back.

use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use ferrite::config::Config;
use ferrite::server::Server;

/// Start a Ferrite server on a random port and return the port number.
/// The server runs in a background tokio task.
async fn start_test_server() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let mut config = Config::default();
    config.server.port = port;
    config.server.bind = "127.0.0.1".to_string();
    config.persistence.aof_enabled = false;
    config.persistence.checkpoint_enabled = false;
    config.metrics.enabled = false;

    let server = Server::new(config).await.expect("server should start");
    tokio::spawn(async move {
        let _ = server.run().await;
    });

    // Wait for the server to accept connections
    for _ in 0..50 {
        if TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .is_ok()
        {
            // Small delay to let the server fully accept
            tokio::time::sleep(Duration::from_millis(10)).await;
            return port;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("Server did not start within 2.5 seconds on port {port}");
}

/// Connect to the test server and return a buffered stream.
async fn connect(port: u16) -> BufReader<TcpStream> {
    let stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .expect("should connect");
    BufReader::new(stream)
}

/// Send a RESP array command and read the first response line.
async fn send_command(stream: &mut BufReader<TcpStream>, args: &[&str]) -> String {
    let inner = stream.get_mut();

    let mut cmd = format!("*{}\r\n", args.len());
    for arg in args {
        cmd.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }
    inner.write_all(cmd.as_bytes()).await.unwrap();
    inner.flush().await.unwrap();

    read_resp_value(stream).await
}

/// Read a single RESP value from the stream.
async fn read_resp_value(stream: &mut BufReader<TcpStream>) -> String {
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
            let len: i64 = line[1..].trim().parse().unwrap();
            if len < 0 {
                return line; // null bulk string
            }
            let mut buf = vec![0u8; (len as usize) + 2]; // +2 for \r\n
            stream.read_exact(&mut buf).await.unwrap();
            let value = String::from_utf8_lossy(&buf[..len as usize]).to_string();
            format!("${}\r\n{}\r\n", len, value)
        }
        Some('*') => {
            let count: i64 = line[1..].trim().parse().unwrap();
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

#[tokio::test]
async fn test_ping_pong() {
    let port = start_test_server().await;
    let mut stream = connect(port).await;

    let resp = send_command(&mut stream, &["PING"]).await;
    assert_eq!(resp.trim(), "+PONG");
}

#[tokio::test]
async fn test_set_get_del() {
    let port = start_test_server().await;
    let mut stream = connect(port).await;

    // SET
    let resp = send_command(&mut stream, &["SET", "e2e_key", "hello_ferrite"]).await;
    assert_eq!(resp.trim(), "+OK");

    // GET
    let resp = send_command(&mut stream, &["GET", "e2e_key"]).await;
    assert!(
        resp.contains("hello_ferrite"),
        "GET should return the value"
    );

    // DEL
    let resp = send_command(&mut stream, &["DEL", "e2e_key"]).await;
    assert!(resp.contains(":1"), "DEL should return 1");

    // GET after DEL
    let resp = send_command(&mut stream, &["GET", "e2e_key"]).await;
    assert!(resp.contains("$-1"), "GET after DEL should return null");
}

#[tokio::test]
async fn test_incr_decr() {
    let port = start_test_server().await;
    let mut stream = connect(port).await;

    let resp = send_command(&mut stream, &["SET", "counter", "10"]).await;
    assert_eq!(resp.trim(), "+OK");

    let resp = send_command(&mut stream, &["INCR", "counter"]).await;
    assert!(resp.contains(":11"), "INCR should return 11");

    let resp = send_command(&mut stream, &["DECR", "counter"]).await;
    assert!(resp.contains(":10"), "DECR should return 10");
}

#[tokio::test]
async fn test_hash_operations() {
    let port = start_test_server().await;
    let mut stream = connect(port).await;

    // HSET
    let resp = send_command(&mut stream, &["HSET", "myhash", "field1", "value1"]).await;
    assert!(resp.contains(":1"), "HSET should return 1 for new field");

    // HGET
    let resp = send_command(&mut stream, &["HGET", "myhash", "field1"]).await;
    assert!(resp.contains("value1"), "HGET should return the value");

    // HGET nonexistent field
    let resp = send_command(&mut stream, &["HGET", "myhash", "nosuchfield"]).await;
    assert!(
        resp.contains("$-1"),
        "HGET missing field should return null"
    );
}

#[tokio::test]
async fn test_list_operations() {
    let port = start_test_server().await;
    let mut stream = connect(port).await;

    let resp = send_command(&mut stream, &["RPUSH", "mylist", "a", "b", "c"]).await;
    assert!(resp.contains(":3"), "RPUSH should return list length 3");

    let resp = send_command(&mut stream, &["LLEN", "mylist"]).await;
    assert!(resp.contains(":3"), "LLEN should return 3");

    let resp = send_command(&mut stream, &["LPOP", "mylist"]).await;
    assert!(resp.contains("a"), "LPOP should return first element");
}

#[tokio::test]
async fn test_multiple_databases() {
    let port = start_test_server().await;
    let mut stream = connect(port).await;

    // Set in db 0
    send_command(&mut stream, &["SET", "dbkey", "db0_value"]).await;

    // Switch to db 1
    let resp = send_command(&mut stream, &["SELECT", "1"]).await;
    assert_eq!(resp.trim(), "+OK");

    // Key should not exist in db 1
    let resp = send_command(&mut stream, &["GET", "dbkey"]).await;
    assert!(resp.contains("$-1"), "Key should not exist in different db");

    // Switch back to db 0
    send_command(&mut stream, &["SELECT", "0"]).await;
    let resp = send_command(&mut stream, &["GET", "dbkey"]).await;
    assert!(
        resp.contains("db0_value"),
        "Key should still exist in original db"
    );
}
