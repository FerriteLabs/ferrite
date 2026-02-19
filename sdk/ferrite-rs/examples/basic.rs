//! Basic usage example for the Ferrite Rust SDK.
//!
//! Start a Ferrite (or Redis) server on 127.0.0.1:6379, then run:
//!
//! ```sh
//! cargo run --example basic
//! ```

use ferrite_rs::{AsyncClient, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Connect to the server.
    let client = AsyncClient::connect("127.0.0.1", 6379).await?;

    // PING
    let pong = client.ping().await?;
    println!("PING -> {}", pong);

    // SET / GET
    client.set("sdk:hello", "world").await?.execute().await?;
    let val = client.get("sdk:hello").await?;
    println!("GET sdk:hello -> {}", val);

    // SET with options (builder pattern)
    client
        .set("sdk:temp", "expires-soon")
        .await?
        .ex(30)
        .nx()
        .execute()
        .await?;

    // INCR
    client.set("sdk:counter", "0").await?.execute().await?;
    let n = client.incr("sdk:counter").await?;
    println!("INCR sdk:counter -> {}", n);

    // List operations
    client.rpush("sdk:list", &["a", "b", "c"]).await?;
    let items = client.lrange("sdk:list", 0, -1).await?;
    println!("LRANGE sdk:list 0 -1 -> {:?}", items);

    // Hash operations
    client
        .hset("sdk:user:1", &[("name", "Alice"), ("age", "30")])
        .await?;
    let name = client.hget("sdk:user:1", "name").await?;
    println!("HGET sdk:user:1 name -> {}", name);

    // Set operations
    client.sadd("sdk:tags", &["rust", "ferrite", "fast"]).await?;
    let members = client.smembers("sdk:tags").await?;
    println!("SMEMBERS sdk:tags -> {:?}", members);

    // Sorted set operations
    client
        .zadd("sdk:scores", &[(100.0, "alice"), (200.0, "bob")])
        .await?;
    let top = client.zrange("sdk:scores", 0, -1, true).await?;
    println!("ZRANGE sdk:scores 0 -1 WITHSCORES -> {:?}", top);

    // Server info
    let db_size = client.dbsize().await?;
    println!("DBSIZE -> {}", db_size);

    // Clean up example keys
    client
        .del(&[
            "sdk:hello",
            "sdk:temp",
            "sdk:counter",
            "sdk:list",
            "sdk:user:1",
            "sdk:tags",
            "sdk:scores",
        ])
        .await?;

    println!("Done!");
    Ok(())
}
