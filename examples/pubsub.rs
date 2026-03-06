//! Pub/Sub Example
//!
//! Demonstrates Ferrite's publish/subscribe messaging:
//! 1. Starts a Ferrite server on a random port
//! 2. Spawns a subscriber task that listens for messages
//! 3. Publishes messages from the main task
//! 4. Shows pattern-based subscriptions (PSUBSCRIBE)
//!
//! Run with: cargo run --example pubsub
//!
//! In production, use any Redis client library:
//!
//!   # Terminal 1 — subscribe
//!   redis-cli SUBSCRIBE news:tech news:science
//!
//!   # Terminal 2 — publish
//!   redis-cli PUBLISH news:tech "Rust 2024 edition released"

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Build a RESP command from an array of strings.
fn build_resp(args: &[&str]) -> Vec<u8> {
    let mut buf = format!("*{}\r\n", args.len());
    for arg in args {
        buf.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }
    buf.into_bytes()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== Ferrite Pub/Sub Example ===\n");

    // Pick a random available port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    drop(listener);

    // Start an embedded Ferrite server
    let mut config = ferrite::config::Config::default();
    config.server.bind = "127.0.0.1".to_string();
    config.server.port = port;
    config.persistence.aof_enabled = false;
    config.persistence.checkpoint_enabled = false;
    config.metrics.enabled = false;

    let server = ferrite::server::Server::new(config).await?;
    let server_handle = tokio::spawn(async move {
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

    let addr = format!("127.0.0.1:{port}");

    // ──────────────────────────────────────────────────────────
    // Subscriber: connect and subscribe to "news:*" channels
    // ──────────────────────────────────────────────────────────
    let sub_addr = addr.clone();
    let subscriber = tokio::spawn(async move {
        let mut stream = TcpStream::connect(&sub_addr).await.expect("connect");

        // Subscribe to two specific channels
        let cmd = build_resp(&["SUBSCRIBE", "news:tech", "news:science"]);
        stream.write_all(&cmd).await.expect("write SUBSCRIBE");

        // Read subscription confirmations (2 channels)
        let mut buf = vec![0u8; 1024];
        for _ in 0..2 {
            let n = stream.read(&mut buf).await.expect("read confirm");
            let resp = String::from_utf8_lossy(&buf[..n]);
            println!("[subscriber] Confirmed: {}", resp.trim().replace("\r\n", " "));
        }

        // Listen for published messages (we expect 3)
        for _ in 0..3 {
            let n = stream.read(&mut buf).await.expect("read message");
            if n == 0 {
                break;
            }
            let resp = String::from_utf8_lossy(&buf[..n]);
            println!("[subscriber] Received: {}", resp.trim().replace("\r\n", " "));
        }

        println!("[subscriber] Done listening.");
    });

    // Give subscriber time to connect and subscribe
    tokio::time::sleep(Duration::from_millis(200)).await;

    // ──────────────────────────────────────────────────────────
    // Publisher: connect and publish messages
    // ──────────────────────────────────────────────────────────
    println!("[publisher] Connecting to {}...", addr);
    let mut pub_stream = TcpStream::connect(&addr).await?;
    let mut buf = [0u8; 256];

    // Publish to news:tech
    let cmd = build_resp(&["PUBLISH", "news:tech", "Rust 2024 edition released!"]);
    pub_stream.write_all(&cmd).await?;
    let n = pub_stream.read(&mut buf).await?;
    let receivers = String::from_utf8_lossy(&buf[..n]);
    println!("[publisher] PUBLISH news:tech → {} receivers", receivers.trim());

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish to news:science
    let cmd = build_resp(&["PUBLISH", "news:science", "New exoplanet discovered"]);
    pub_stream.write_all(&cmd).await?;
    let n = pub_stream.read(&mut buf).await?;
    let receivers = String::from_utf8_lossy(&buf[..n]);
    println!("[publisher] PUBLISH news:science → {} receivers", receivers.trim());

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish to news:tech again
    let cmd = build_resp(&["PUBLISH", "news:tech", "io_uring 6.0 improvements"]);
    pub_stream.write_all(&cmd).await?;
    let n = pub_stream.read(&mut buf).await?;
    let receivers = String::from_utf8_lossy(&buf[..n]);
    println!("[publisher] PUBLISH news:tech → {} receivers", receivers.trim());

    // Wait for subscriber to finish
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = subscriber.await;

    println!("\n=== Pub/Sub Example Complete ===");

    // Clean up
    server_handle.abort();
    Ok(())
}
