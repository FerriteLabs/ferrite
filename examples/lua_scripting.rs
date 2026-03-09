#![allow(clippy::print_stdout)]
//! Lua Scripting Example
//!
//! Demonstrates Ferrite's Lua scripting support via EVAL/EVALSHA:
//! 1. Starts a Ferrite server with scripting enabled
//! 2. Executes inline Lua scripts with EVAL
//! 3. Shows key access from Lua (KEYS/ARGV parameters)
//! 4. Demonstrates atomic multi-key operations via scripting
//!
//! Run with: cargo run --example lua_scripting --features scripting
//!
//! In production, use any Redis client library:
//!
//!   redis-cli EVAL "return redis.call('SET', KEYS[1], ARGV[1])" 1 mykey myvalue
//!   redis-cli EVAL "return redis.call('GET', KEYS[1])" 1 mykey

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use ferrite::config::Config;
use ferrite::server::Server;

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
    println!("=== Ferrite Lua Scripting Example ===\n");

    // Pick a random available port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    drop(listener);

    let mut config = Config::default();
    config.server.bind = "127.0.0.1".to_string();
    config.server.port = port;
    config.persistence.aof_enabled = false;
    config.persistence.checkpoint_enabled = false;
    config.metrics.enabled = false;

    let server = Server::new(config).await?;
    tokio::spawn(async move {
        let _ = server.run().await;
    });

    // Wait for server to be ready
    let addr = format!("127.0.0.1:{port}");
    for _ in 0..50 {
        if TcpStream::connect(&addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    println!("Server ready on {addr}\n");

    let mut stream = TcpStream::connect(&addr).await?;
    let mut buf = [0u8; 4096];

    // ──────────────────────────────────────────────────────────
    // 1. Simple EVAL — return a value from Lua
    // ──────────────────────────────────────────────────────────
    println!("--- 1. Simple EVAL ---");
    let cmd = build_resp(&["EVAL", "return 'Hello from Lua!'", "0"]);
    stream.write_all(&cmd).await?;
    let n = stream.read(&mut buf).await?;
    println!("  EVAL \"return 'Hello from Lua!'\" 0");
    println!("  → {}\n", String::from_utf8_lossy(&buf[..n]).trim());

    // ──────────────────────────────────────────────────────────
    // 2. EVAL with KEYS and ARGV — set a key atomically
    // ──────────────────────────────────────────────────────────
    println!("--- 2. EVAL with KEYS/ARGV ---");
    let script = "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])";
    let cmd = build_resp(&["EVAL", script, "1", "greeting", "Hello, Ferrite!"]);
    stream.write_all(&cmd).await?;
    let n = stream.read(&mut buf).await?;
    println!("  EVAL \"SET KEYS[1] ARGV[1]; GET KEYS[1]\" 1 greeting \"Hello, Ferrite!\"");
    println!("  → {}\n", String::from_utf8_lossy(&buf[..n]).trim());

    // ──────────────────────────────────────────────────────────
    // 3. Atomic counter with conditional logic
    // ──────────────────────────────────────────────────────────
    println!("--- 3. Atomic rate limiter script ---");
    let rate_limit_script = r#"
        local current = redis.call('INCR', KEYS[1])
        if current == 1 then
            redis.call('EXPIRE', KEYS[1], ARGV[1])
        end
        if current > tonumber(ARGV[2]) then
            return 0
        end
        return 1
    "#;

    // First 3 calls should succeed (limit=3), 4th should be rate-limited
    for i in 1..=4 {
        let cmd = build_resp(&["EVAL", rate_limit_script, "1", "ratelimit:api", "60", "3"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        let resp = String::from_utf8_lossy(&buf[..n]);
        let allowed = resp.contains(":1");
        println!(
            "  Request {i}: {}",
            if allowed {
                "✅ allowed"
            } else {
                "❌ rate-limited"
            }
        );
    }

    // ──────────────────────────────────────────────────────────
    // 4. Atomic balance transfer between two keys
    // ──────────────────────────────────────────────────────────
    println!("\n--- 4. Atomic balance transfer ---");

    // Set up initial balances
    let cmd = build_resp(&["SET", "account:alice", "1000"]);
    stream.write_all(&cmd).await?;
    let _ = stream.read(&mut buf).await?;

    let cmd = build_resp(&["SET", "account:bob", "500"]);
    stream.write_all(&cmd).await?;
    let _ = stream.read(&mut buf).await?;

    let transfer_script = r#"
        local from = tonumber(redis.call('GET', KEYS[1]))
        local amount = tonumber(ARGV[1])
        if from < amount then
            return redis.error_reply('insufficient funds')
        end
        redis.call('DECRBY', KEYS[1], amount)
        redis.call('INCRBY', KEYS[2], amount)
        return 'OK: transferred ' .. amount
    "#;

    let cmd = build_resp(&[
        "EVAL",
        transfer_script,
        "2",
        "account:alice",
        "account:bob",
        "250",
    ]);
    stream.write_all(&cmd).await?;
    let n = stream.read(&mut buf).await?;
    println!(
        "  Transfer 250 from Alice to Bob: {}",
        String::from_utf8_lossy(&buf[..n]).trim()
    );

    // Check final balances
    let cmd = build_resp(&["MGET", "account:alice", "account:bob"]);
    stream.write_all(&cmd).await?;
    let n = stream.read(&mut buf).await?;
    println!(
        "  Final balances: {}\n",
        String::from_utf8_lossy(&buf[..n]).trim()
    );

    println!("=== Lua Scripting Example Complete ===");
    Ok(())
}
