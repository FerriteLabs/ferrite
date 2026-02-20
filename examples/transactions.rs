//! Transactions Example
//!
//! Demonstrates Ferrite's transaction support for atomic operations:
//! - Creating transactions with closure API
//! - Reading within transactions
//! - Atomic multi-key updates
//! - Transaction discard (rollback)
//! - Common patterns: balance transfers, counters
//!
//! Run with: cargo run --example transactions

use ferrite::embedded::core::Database;

fn main() -> ferrite::embedded::core::Result<()> {
    println!("=== Ferrite Transactions Example ===\n");

    let db = Database::memory()?;

    // ==================== Basic Transaction ====================
    println!("--- Basic Transaction ---");

    // Use the closure-based transaction API for simple cases
    db.transaction(|tx| {
        tx.set("config:version", "1.0.0")?;
        tx.set("config:env", "production")?;
        tx.set("config:debug", "false")?;
        Ok(())
    })?;

    println!("Config values set atomically:");
    println!("  version: {:?}", db.get_str("config:version")?);
    println!("  env: {:?}", db.get_str("config:env")?);
    println!("  debug: {:?}", db.get_str("config:debug")?);

    // ==================== Transaction with Reads ====================
    println!("\n--- Transaction with Reads ---");

    // Set up initial account balances
    db.set("account:alice:balance", "1000")?;
    db.set("account:bob:balance", "500")?;

    println!("Initial balances:");
    println!("  Alice: {:?}", db.get_str("account:alice:balance")?);
    println!("  Bob: {:?}", db.get_str("account:bob:balance")?);

    // Transfer money atomically with validation
    let transfer_result = db.transaction(|tx| {
        // Read current balance (reads are immediate, not queued)
        let alice_balance_bytes = tx.get("account:alice:balance")?.unwrap_or_default();
        let alice_balance: i64 = String::from_utf8_lossy(&alice_balance_bytes)
            .parse()
            .unwrap_or(0);

        let transfer_amount = 300;

        // Validate sufficient funds
        if alice_balance < transfer_amount {
            return Ok(false); // Insufficient funds
        }

        // Queue the transfer operations
        tx.incr_by("account:alice:balance", -transfer_amount)?;
        tx.incr_by("account:bob:balance", transfer_amount)?;

        Ok(true)
    })?;

    if transfer_result {
        println!("\nTransfer of 300 successful!");
    } else {
        println!("\nTransfer failed: insufficient funds");
    }

    println!("Final balances:");
    println!("  Alice: {:?}", db.get_str("account:alice:balance")?);
    println!("  Bob: {:?}", db.get_str("account:bob:balance")?);

    // ==================== Manual Transaction Control ====================
    println!("\n--- Manual Transaction Control ---");

    // For more control, use begin_transaction and commit_transaction
    let mut tx = db.begin_transaction();

    tx.set("order:1:status", "pending")?;
    tx.set("order:1:items", "3")?;
    tx.hset("order:1:details", "customer", "alice")?;
    tx.hset("order:1:details", "total", "99.99")?;

    // Check queue length before commit
    println!("Operations queued: {}", tx.queue_len());

    // Commit all operations atomically
    let ops_executed = db.commit_transaction(tx)?;
    println!("Operations executed: {}", ops_executed);

    // Verify the data
    println!("Order status: {:?}", db.get_str("order:1:status")?);

    // ==================== Transaction Discard ====================
    println!("\n--- Transaction Discard (Rollback) ---");

    // Set initial value
    db.set("important_data", "original_value")?;
    println!("Before transaction: {:?}", db.get_str("important_data")?);

    // Start a transaction but discard it
    let mut tx = db.begin_transaction();
    tx.set("important_data", "modified_value")?;
    tx.set("new_key", "new_value")?;
    println!("Operations queued: {}", tx.queue_len());

    // Discard the transaction - changes are NOT applied
    tx.discard();
    println!("Transaction discarded!");

    // Verify original value is unchanged
    println!("After discard: {:?}", db.get_str("important_data")?);
    println!("New key exists: {}", db.exists(&["new_key"])? > 0);

    // ==================== Atomic Counter Pattern ====================
    println!("\n--- Atomic Counter Pattern ---");

    // Initialize counters
    db.set("stats:page_views", "0")?;
    db.set("stats:unique_visitors", "0")?;
    db.set("stats:clicks", "0")?;

    // Update multiple counters atomically
    db.transaction(|tx| {
        tx.incr("stats:page_views")?;
        tx.incr("stats:unique_visitors")?;
        tx.incr_by("stats:clicks", 5)?;
        Ok(())
    })?;

    println!("Stats after update:");
    println!("  Page views: {:?}", db.get_str("stats:page_views")?);
    println!(
        "  Unique visitors: {:?}",
        db.get_str("stats:unique_visitors")?
    );
    println!("  Clicks: {:?}", db.get_str("stats:clicks")?);

    // ==================== List Operations in Transaction ====================
    println!("\n--- List Operations in Transaction ---");

    db.transaction(|tx| {
        tx.rpush("events", &["login", "view_page", "click_button"])?;
        tx.lpush("notifications", &["Welcome!", "New message"])?;
        Ok(())
    })?;

    println!("Events list length: {}", db.llen("events")?);
    println!("Notifications list length: {}", db.llen("notifications")?);

    // ==================== Set Operations in Transaction ====================
    println!("\n--- Set Operations in Transaction ---");

    db.transaction(|tx| {
        tx.sadd("user:1:roles", &["admin", "editor", "viewer"])?;
        tx.sadd("user:1:permissions", &["read", "write", "delete"])?;
        Ok(())
    })?;

    println!("User 1 roles: {}", db.scard("user:1:roles")?);
    println!("User 1 permissions: {}", db.scard("user:1:permissions")?);

    // ==================== Real-World Pattern: Order Processing ====================
    println!("\n--- Real-World Pattern: Order Processing ---");

    // Simulate creating an order with inventory check
    db.set("inventory:widget", "100")?;

    let order_quantity = 5;
    let order_id = "order:12345";

    let order_success = db.transaction(|tx| {
        // Check inventory
        let inventory_bytes = tx.get("inventory:widget")?.unwrap_or_default();
        let inventory: i64 = String::from_utf8_lossy(&inventory_bytes)
            .parse()
            .unwrap_or(0);

        if inventory < order_quantity {
            return Ok(false);
        }

        // Decrement inventory
        tx.incr_by("inventory:widget", -order_quantity)?;

        // Create order record
        tx.set(order_id, "confirmed")?;
        tx.hset(
            format!("{}:details", order_id),
            "quantity",
            order_quantity.to_string(),
        )?;
        tx.hset(format!("{}:details", order_id), "product", "widget")?;

        // Add to order list
        tx.lpush("orders:pending", &[order_id])?;

        Ok(true)
    })?;

    if order_success {
        println!("Order {} created successfully!", order_id);
        println!("Remaining inventory: {:?}", db.get_str("inventory:widget")?);
    } else {
        println!("Order failed: insufficient inventory");
    }

    println!("\n=== Example Complete ===");
    Ok(())
}
