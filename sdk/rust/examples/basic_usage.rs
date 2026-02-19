//! Basic usage example for the Ferrite embedded Rust SDK
//!
//! ```bash
//! cargo run --example basic_usage
//! ```

fn main() {
    println!("=== Ferrite Embedded SDK — Basic Usage ===\n");

    // Example 1: In-memory database (no persistence)
    println!("--- Example 1: In-Memory Database ---");
    println!("  let db = Ferrite::builder()");
    println!("      .memory_limit(\"256mb\")");
    println!("      .build()");
    println!("      .unwrap();");
    println!("  db.set(\"key\", \"value\");");
    println!("  let val = db.get(\"key\"); // Some(\"value\")");
    println!();

    // Example 2: Persistent database
    println!("--- Example 2: Persistent Database ---");
    println!("  let db = Ferrite::builder()");
    println!("      .data_dir(\"/var/lib/ferrite\")");
    println!("      .persistence(true)");
    println!("      .build()");
    println!("      .unwrap();");
    println!();

    // Example 3: With eviction policy
    println!("--- Example 3: LRU Eviction ---");
    println!("  let db = Ferrite::builder()");
    println!("      .memory_limit(\"128mb\")");
    println!("      .eviction_policy(EvictionPolicy::AllKeysLru)");
    println!("      .build()");
    println!("      .unwrap();");
    println!();

    // Example 4: TTL support
    println!("--- Example 4: TTL Support ---");
    println!("  db.set_with_ttl(\"session\", \"data\", Duration::from_secs(3600));");
    println!("  let ttl = db.ttl(\"session\"); // remaining seconds");
    println!();

    // Example 5: Transactions
    println!("--- Example 5: Transactions ---");
    println!("  db.transaction(|tx| {{");
    println!("      tx.set(\"account:1\", \"1000\");");
    println!("      tx.set(\"account:2\", \"500\");");
    println!("      Ok(())");
    println!("  }});");
    println!();

    println!("For full API reference, see: https://docs.rs/ferrite");
}
