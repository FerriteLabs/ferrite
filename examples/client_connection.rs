#![allow(clippy::unwrap_used)]
//! Client Connection Example
//!
//! Demonstrates connecting to a Ferrite server over TCP using the RESP
//! protocol — the same way redis-cli, redis-py, redis-rs, and every other
//! Redis client connects.
//!
//! This example:
//! 1. Starts a Ferrite server on a random port (no config file needed)
//! 2. Connects as a TCP client
//! 3. Sends RESP commands (PING, SET, GET, INCR, DEL)
//! 4. Prints the raw RESP responses
//!
//! Run with: cargo run --example client_connection
//!
//! In production you would connect with any Redis client library:
//!
//!   # redis-cli
//!   redis-cli -p 6379 SET hello world
//!
//!   # Python (redis-py)
//!   import redis; r = redis.Redis(); r.set("hello", "world")
//!
//!   # Rust (redis-rs)
//!   let client = redis::Client::open("redis://127.0.0.1/")?;

use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use ferrite::config::Config;
use ferrite::server::Server;

/// Build a RESP array command from arguments.
fn resp_command(args: &[&str]) -> String {
    let mut cmd = format!("*{}\r\n", args.len());
    for arg in args {
        cmd.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }
    cmd
}

/// Send a RESP command and print the response.
async fn send_and_print(stream: &mut BufReader<TcpStream>, label: &str, args: &[&str]) {
    let inner = stream.get_mut();
    let cmd = resp_command(args);
    inner.write_all(cmd.as_bytes()).await.unwrap();
    inner.flush().await.unwrap();

    let response = read_resp_value(stream).await;
    println!("  {label}: {}", response.trim());
}

/// Read one RESP value (simple string, error, integer, bulk string, or array).
async fn read_resp_value(stream: &mut BufReader<TcpStream>) -> String {
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_line(&mut line))
        .await
        .expect("timeout")
        .expect("read");

    match line.chars().next() {
        Some('+') | Some('-') | Some(':') => line,
        Some('$') => {
            let len: i64 = line[1..].trim().parse().unwrap();
            if len < 0 {
                return "$-1 (nil)\r\n".to_string();
            }
            let mut buf = vec![0u8; (len as usize) + 2];
            stream.read_exact(&mut buf).await.unwrap();
            String::from_utf8_lossy(&buf[..len as usize]).to_string() + "\r\n"
        }
        Some('*') => {
            let count: i64 = line[1..].trim().parse().unwrap();
            if count < 0 {
                return "*-1 (nil)\r\n".to_string();
            }
            let mut parts = Vec::new();
            for _ in 0..count {
                parts.push(Box::pin(read_resp_value(stream)).await.trim().to_string());
            }
            format!("[{}]\r\n", parts.join(", "))
        }
        _ => line,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Start a Ferrite server on a random port with default in-memory config
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    drop(listener);

    let mut config = Config::default();
    config.server.port = port;
    config.server.bind = "127.0.0.1".to_string();
    config.persistence.aof_enabled = false;
    config.persistence.checkpoint_enabled = false;
    config.metrics.enabled = false;

    println!("=== Ferrite Client Connection Example ===\n");
    println!("Starting Ferrite server on 127.0.0.1:{port}...");

    let server = Server::new(config).await?;
    tokio::spawn(async move {
        let _ = server.run().await;
    });

    // Wait for server to be ready
    for _ in 0..50 {
        if TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Connect as a client — same as redis-cli or any Redis client library
    let stream = TcpStream::connect(format!("127.0.0.1:{port}")).await?;
    let mut reader = BufReader::new(stream);

    println!("Connected!\n");

    // --- PING ---
    println!("--- PING ---");
    send_and_print(&mut reader, "PING", &["PING"]).await;
    println!();

    // --- String operations ---
    println!("--- String Operations ---");
    send_and_print(&mut reader, "SET mykey hello", &["SET", "mykey", "hello"]).await;
    send_and_print(&mut reader, "GET mykey", &["GET", "mykey"]).await;
    send_and_print(&mut reader, "SET counter 0", &["SET", "counter", "0"]).await;
    send_and_print(&mut reader, "INCR counter", &["INCR", "counter"]).await;
    send_and_print(&mut reader, "INCR counter", &["INCR", "counter"]).await;
    send_and_print(&mut reader, "GET counter", &["GET", "counter"]).await;
    println!();

    // --- Hash operations ---
    println!("--- Hash Operations ---");
    send_and_print(
        &mut reader,
        "HSET user name Alice",
        &["HSET", "user:1", "name", "Alice"],
    )
    .await;
    send_and_print(
        &mut reader,
        "HSET user email",
        &["HSET", "user:1", "email", "alice@example.com"],
    )
    .await;
    send_and_print(&mut reader, "HGET user name", &["HGET", "user:1", "name"]).await;
    println!();

    // --- List operations ---
    println!("--- List Operations ---");
    send_and_print(
        &mut reader,
        "RPUSH queue a b c",
        &["RPUSH", "queue", "a", "b", "c"],
    )
    .await;
    send_and_print(&mut reader, "LLEN queue", &["LLEN", "queue"]).await;
    send_and_print(&mut reader, "LPOP queue", &["LPOP", "queue"]).await;
    println!();

    // --- Cleanup ---
    println!("--- Cleanup ---");
    send_and_print(
        &mut reader,
        "DEL mykey counter",
        &["DEL", "mykey", "counter", "user:1", "queue"],
    )
    .await;

    println!("\nDone! In production, connect with any Redis client:");
    println!("  redis-cli -p 6379");
    println!("  redis-py:  redis.Redis(host='localhost', port=6379)");
    println!("  redis-rs:  redis::Client::open(\"redis://127.0.0.1/\")?");

    Ok(())
}
