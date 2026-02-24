//! Basic embedded Ferrite usage
//!
//! Demonstrates the high-level [`Ferrite`] embedded API for strings, counters,
//! hashes, lists, and sets — all without starting a network server.
//!
//! Ferrite's embedded mode lets you use it as an in-process library (like
//! SQLite) rather than a client-server system. The builder pattern configures
//! memory limits, persistence, and eviction policies.
//!
//! # What this example covers
//!
//! - Creating an in-memory database via the builder API
//! - String operations: SET, GET, DEL, INCR, INCRBY
//! - Hash operations: HSET, HGET, HGETALL (user profiles)
//! - List operations: RPUSH, LPOP, LRANGE (task queues)
//! - Set operations: SADD, SMEMBERS, SISMEMBER (tag sets)
//! - Utility: DBSIZE, KEYS, INFO
//!
//! Run with: `cargo run --example embedded_basic`

use ferrite::embedded::Ferrite;

fn main() -> anyhow::Result<()> {
    println!("=== Ferrite Embedded -- Basic Example ===\n");

    // ── Create an in-memory database ────────────────────────────────────
    let db = Ferrite::builder()
        .max_memory("128mb")
        .persistence(false)
        .build()?;

    // ── String operations ───────────────────────────────────────────────
    println!("--- String Operations ---");
    db.set("greeting", "Hello, Ferrite!")?;
    println!("GET greeting = {:?}", db.get("greeting")?);

    // Overwrite
    db.set("greeting", "Ahoy, embedded world!")?;
    println!("GET greeting (updated) = {:?}", db.get("greeting")?);

    // Delete
    let was_deleted = db.del("greeting")?;
    println!("DEL greeting = {} (should be true)", was_deleted);
    println!("GET greeting (after delete) = {:?}", db.get("greeting")?);

    // ── Counter ─────────────────────────────────────────────────────────
    println!("\n--- Counter ---");
    db.set("counter", "0")?;
    for _ in 0..100 {
        db.incr("counter")?;
    }
    println!("Counter after 100 INCRs = {:?}", db.get("counter")?);

    // Decrement by 50
    db.incr_by("counter", -50)?;
    println!("Counter after INCRBY -50 = {:?}", db.get("counter")?);

    // ── Hash (user profile) ─────────────────────────────────────────────
    println!("\n--- Hash Operations ---");
    db.hset("user:1", "name", "Alice")?;
    db.hset("user:1", "email", "alice@example.com")?;
    db.hset("user:1", "role", "admin")?;

    println!("HGET user:1 name = {:?}", db.hget("user:1", "name")?);
    println!("HGETALL user:1:");
    for (field, value) in db.hgetall("user:1")? {
        println!(
            "  {} = {}",
            String::from_utf8_lossy(&field),
            String::from_utf8_lossy(&value),
        );
    }

    // ── List (task queue) ───────────────────────────────────────────────
    println!("\n--- List Operations ---");
    db.rpush(
        "tasks",
        &[
            bytes::Bytes::from("compile"),
            bytes::Bytes::from("test"),
            bytes::Bytes::from("deploy"),
        ],
    )?;
    println!("LRANGE tasks 0 -1 = {:?}", db.lrange("tasks", 0, -1)?);

    if let Some(task) = db.lpop("tasks")? {
        println!("LPOP tasks = {}", String::from_utf8_lossy(&task));
    }
    println!(
        "LRANGE tasks 0 -1 (after pop) = {:?}",
        db.lrange("tasks", 0, -1)?
    );

    // ── Set (tags) ──────────────────────────────────────────────────────
    println!("\n--- Set Operations ---");
    db.sadd(
        "tags",
        &[
            bytes::Bytes::from("rust"),
            bytes::Bytes::from("database"),
            bytes::Bytes::from("cache"),
            bytes::Bytes::from("rust"), // duplicate -- ignored
        ],
    )?;
    println!("SMEMBERS tags = {:?}", db.smembers("tags")?);
    println!("SISMEMBER tags rust  = {}", db.sismember("tags", "rust"));
    println!("SISMEMBER tags java  = {}", db.sismember("tags", "java"));

    // ── Utility ─────────────────────────────────────────────────────────
    println!("\n--- Utility ---");
    println!("DBSIZE = {}", db.dbsize());
    println!("KEYS * = {:?}", db.keys("*")?);

    // INFO
    println!("\n--- INFO (excerpt) ---");
    let info = db.info();
    for line in info.lines().take(10) {
        println!("  {}", line);
    }

    println!("\n=== Done ===");
    Ok(())
}
