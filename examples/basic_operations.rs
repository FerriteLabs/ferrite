//! Basic Operations Example
//!
//! Demonstrates core Ferrite embedded database operations:
//! - GET/SET string operations
//! - INCR/DECR numeric operations
//! - Hash operations (HSET/HGET)
//! - List operations (LPUSH/RPOP)
//! - Set operations (SADD/SMEMBERS)
//! - Multi-database support
//! - Database statistics
//!
//! Run with: cargo run --example basic_operations

use ferrite::embedded::core::{Database, EmbeddedConfig, SyncMode};

fn main() -> ferrite::embedded::core::Result<()> {
    println!("=== Ferrite Basic Operations Example ===\n");

    // Create an in-memory database for this example
    // For persistence, use Database::open("./path/to/data")
    let db = Database::memory()?;

    // ==================== String Operations ====================
    println!("--- String Operations ---");

    // SET and GET basic strings
    db.set("user:1:name", "Alice")?;
    db.set("user:1:email", "alice@example.com")?;

    let name = db.get_str("user:1:name")?;
    println!("User name: {:?}", name);

    // SET if not exists (SETNX)
    let was_set = db.set_nx("user:1:name", "Bob")?; // Returns false - key exists
    println!("SET NX (should be false): {}", was_set);

    let was_set = db.set_nx("user:2:name", "Bob")?; // Returns true - new key
    println!("SET NX new key (should be true): {}", was_set);

    // ==================== Numeric Operations ====================
    println!("\n--- Numeric Operations ---");

    // INCR automatically creates key if it doesn't exist
    let count = db.incr("page_views")?;
    println!("Page views after INCR: {}", count);

    let count = db.incr_by("page_views", 10)?;
    println!("Page views after INCRBY 10: {}", count);

    let count = db.decr("page_views")?;
    println!("Page views after DECR: {}", count);

    // ==================== Hash Operations ====================
    println!("\n--- Hash Operations ---");

    // Store user profile as a hash
    db.hset("user:100:profile", "name", "Charlie")?;
    db.hset("user:100:profile", "age", "30")?;
    db.hset("user:100:profile", "city", "New York")?;

    let user_name = db.hget("user:100:profile", "name")?;
    println!(
        "User 100 name: {:?}",
        String::from_utf8_lossy(&user_name.unwrap_or_default())
    );

    // Get all hash fields
    let profile = db.hgetall("user:100:profile")?;
    println!("User 100 profile fields:");
    for (field, value) in &profile {
        println!(
            "  {}: {}",
            String::from_utf8_lossy(field),
            String::from_utf8_lossy(value)
        );
    }

    // Increment a hash field
    let new_age = db.hincrby("user:100:profile", "age", 1)?;
    println!("User 100 age after birthday: {}", new_age);

    // ==================== List Operations ====================
    println!("\n--- List Operations ---");

    // Use lists as a queue (FIFO with LPUSH + RPOP)
    db.lpush("task_queue", &["task1", "task2", "task3"])?;
    println!("Pushed 3 tasks to queue");

    let queue_len = db.llen("task_queue")?;
    println!("Queue length: {}", queue_len);

    // Process tasks from the queue
    while let Some(task) = db.rpop("task_queue")? {
        println!("Processing: {}", String::from_utf8_lossy(&task));
    }

    // Use lists as a stack (LIFO with LPUSH + LPOP)
    db.lpush("undo_stack", &["action1", "action2", "action3"])?;
    let last_action = db.lpop("undo_stack")?;
    println!(
        "Last action (undo): {:?}",
        last_action.map(|v| String::from_utf8_lossy(&v).to_string())
    );

    // ==================== Set Operations ====================
    println!("\n--- Set Operations ---");

    // Store unique tags
    db.sadd("article:1:tags", &["rust", "database", "redis", "rust"])?; // "rust" only added once

    let tag_count = db.scard("article:1:tags")?;
    println!("Unique tag count: {}", tag_count);

    let is_member = db.sismember("article:1:tags", "rust")?;
    println!("Has 'rust' tag: {}", is_member);

    let tags = db.smembers("article:1:tags")?;
    println!(
        "All tags: {:?}",
        tags.iter()
            .map(|t| String::from_utf8_lossy(t).to_string())
            .collect::<Vec<_>>()
    );

    // ==================== Multi-Database Support ====================
    println!("\n--- Multi-Database Support ---");

    // Create a new database instance with multiple logical databases
    let mut multi_db = Database::open_with_config(EmbeddedConfig {
        path: None, // In-memory
        num_databases: 4,
        sync_mode: SyncMode::None,
        ..Default::default()
    })?;

    // Use database 0 (default)
    multi_db.set("key", "value in db 0")?;
    println!("DB 0: {:?}", multi_db.get_str("key")?);

    // Switch to database 1
    multi_db.select(1)?;
    multi_db.set("key", "value in db 1")?;
    println!("DB 1: {:?}", multi_db.get_str("key")?);

    // Switch back to database 0
    multi_db.select(0)?;
    println!("Back to DB 0: {:?}", multi_db.get_str("key")?);

    // ==================== Database Statistics ====================
    println!("\n--- Database Statistics ---");

    let stats = db.stats();
    println!("Total keys: {}", stats.total_keys);
    println!("Uptime: {} seconds", stats.uptime_secs);

    let db_size = db.dbsize()?;
    println!("Keys in current database: {}", db_size);

    // ==================== Key Management ====================
    println!("\n--- Key Management ---");

    // Check if keys exist
    let exists_count = db.exists(&["user:1:name", "user:1:email", "nonexistent"])?;
    println!("Existing keys (out of 3): {}", exists_count);

    // Get key type
    let key_type = db.key_type("user:100:profile")?;
    println!("Type of 'user:100:profile': {:?}", key_type);

    // Delete keys
    let deleted = db.del(&["user:1:name", "user:1:email"])?;
    println!("Deleted {} keys", deleted);

    // Verify deletion
    let name_after_delete = db.get_str("user:1:name")?;
    println!("User name after delete: {:?}", name_after_delete);

    println!("\n=== Example Complete ===");
    Ok(())
}
