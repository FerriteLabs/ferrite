//! Persistence Configuration Example
//!
//! Demonstrates Ferrite's embedded database configuration options:
//! - In-memory vs persistent databases
//! - EmbeddedConfig options
//! - Sync modes (None, Normal, Full)
//! - WAL (Write-Ahead Logging) settings
//! - Memory limits
//! - Multiple logical databases
//!
//! Run with: cargo run --example persistence_config

use ferrite::embedded::core::{Database, DatabaseBuilder, EmbeddedConfig, SyncMode};
use std::path::PathBuf;

fn main() -> ferrite::embedded::core::Result<()> {
    println!("=== Ferrite Persistence Configuration Example ===\n");

    // ==================== In-Memory Database ====================
    println!("--- In-Memory Database ---");

    // Simplest option: pure in-memory, no persistence
    let memory_db = Database::memory()?;
    memory_db.set("key", "This data will be lost when the program exits")?;
    println!("Created in-memory database");
    println!("Data: {:?}", memory_db.get_str("key")?);

    // ==================== Persistent Database ====================
    println!("\n--- Persistent Database ---");

    // Create a temporary directory for this example
    let temp_dir = std::env::temp_dir().join("ferrite_example");
    std::fs::create_dir_all(&temp_dir).ok();
    let db_path = temp_dir.join("myapp.ferrite");

    // Open or create a persistent database
    // Data survives program restarts
    {
        let persistent_db = Database::open(&db_path)?;
        persistent_db.set("persistent_key", "This data survives restarts")?;
        println!("Created persistent database at: {:?}", db_path);
        println!(
            "Data written: {:?}",
            persistent_db.get_str("persistent_key")?
        );
        // Database is closed when it goes out of scope
    }

    // Reopen and verify data persists
    {
        let reopened_db = Database::open(&db_path)?;
        println!(
            "Reopened database, data: {:?}",
            reopened_db.get_str("persistent_key")?
        );
    }

    // ==================== EmbeddedConfig Options ====================
    println!("\n--- EmbeddedConfig Options ---");

    // Full control with EmbeddedConfig
    let config = EmbeddedConfig {
        // Data directory (None for in-memory)
        path: Some(temp_dir.join("configured.ferrite")),

        // Memory limit for hot data (256MB)
        memory_limit: 256 * 1024 * 1024,

        // Enable Write-Ahead Logging for durability
        wal_enabled: true,

        // Sync mode determines durability vs performance trade-off
        sync_mode: SyncMode::Normal,

        // Number of logical databases (like Redis's SELECT command)
        num_databases: 8,
    };

    let configured_db = Database::open_with_config(config)?;
    println!("Created database with custom config:");
    println!(
        "  Memory limit: {} MB",
        configured_db.config().memory_limit / 1024 / 1024
    );
    println!("  WAL enabled: {}", configured_db.config().wal_enabled);
    println!("  Num databases: {}", configured_db.config().num_databases);

    // ==================== Sync Modes ====================
    println!("\n--- Sync Modes ---");

    // SyncMode::None - Fastest, no durability guarantees
    println!("SyncMode::None:");
    println!("  - Fastest performance");
    println!("  - Data may be lost on crash");
    println!("  - Good for: caches, temporary data, tests");

    let fast_config = EmbeddedConfig {
        path: None,
        sync_mode: SyncMode::None,
        wal_enabled: false,
        ..Default::default()
    };
    let _fast_db = Database::open_with_config(fast_config)?;

    // SyncMode::Normal - Balanced (default)
    println!("\nSyncMode::Normal (default):");
    println!("  - Syncs on transaction commit");
    println!("  - Good balance of speed and durability");
    println!("  - Good for: most applications");

    let balanced_config = EmbeddedConfig {
        path: None,
        sync_mode: SyncMode::Normal,
        wal_enabled: true,
        ..Default::default()
    };
    let _balanced_db = Database::open_with_config(balanced_config)?;

    // SyncMode::Full - Safest, every write synced
    println!("\nSyncMode::Full:");
    println!("  - Syncs after every write operation");
    println!("  - Slowest but most durable");
    println!("  - Good for: financial data, critical records");

    let safe_config = EmbeddedConfig {
        path: None,
        sync_mode: SyncMode::Full,
        wal_enabled: true,
        ..Default::default()
    };
    let _safe_db = Database::open_with_config(safe_config)?;

    // ==================== DatabaseBuilder API ====================
    println!("\n--- DatabaseBuilder API ---");

    // Fluent builder pattern for configuration
    let builder_db = DatabaseBuilder::new()
        .memory_only() // No persistence
        .memory_limit(128 * 1024 * 1024) // 128MB
        .wal_enabled(false) // No WAL for in-memory
        .sync_mode(SyncMode::None)
        .num_databases(4)
        .open()?;

    println!("Created database with builder:");
    println!("  Path: {:?}", builder_db.config().path);
    println!(
        "  Memory limit: {} MB",
        builder_db.config().memory_limit / 1024 / 1024
    );
    println!("  Num databases: {}", builder_db.config().num_databases);

    // Builder with persistence
    let persistent_builder_db = DatabaseBuilder::new()
        .path(temp_dir.join("builder.ferrite"))
        .memory_limit(512 * 1024 * 1024) // 512MB
        .wal_enabled(true)
        .sync_mode(SyncMode::Normal)
        .num_databases(16)
        .open()?;

    println!("\nCreated persistent database with builder:");
    println!("  Path: {:?}", persistent_builder_db.config().path);

    // ==================== Multiple Databases ====================
    println!("\n--- Multiple Logical Databases ---");

    let mut multi_db = DatabaseBuilder::new()
        .memory_only()
        .num_databases(4)
        .open()?;

    // Database 0 for user data
    multi_db.select(0)?;
    multi_db.set("purpose", "user_data")?;
    multi_db.set("user:1", "Alice")?;
    println!("Database 0: User data");

    // Database 1 for cache
    multi_db.select(1)?;
    multi_db.set("purpose", "cache")?;
    multi_db.set("cached:result", "computed_value")?;
    println!("Database 1: Cache");

    // Database 2 for sessions
    multi_db.select(2)?;
    multi_db.set("purpose", "sessions")?;
    multi_db.set("session:abc123", "user:1")?;
    println!("Database 2: Sessions");

    // Database 3 for analytics
    multi_db.select(3)?;
    multi_db.set("purpose", "analytics")?;
    multi_db.set("pageviews", "1000")?;
    println!("Database 3: Analytics");

    // Show data is isolated
    println!("\nData isolation demonstration:");
    for i in 0..4 {
        multi_db.select(i)?;
        let purpose = multi_db.get_str("purpose")?;
        let size = multi_db.dbsize()?;
        println!("  DB {}: purpose={:?}, keys={}", i, purpose, size);
    }

    // ==================== Configuration for Different Use Cases ====================
    println!("\n--- Configuration Recommendations ---");

    println!("Unit Testing:");
    println!("  Database::memory() - Fast, isolated, no cleanup needed");

    println!("\nDevelopment:");
    let _dev_config = EmbeddedConfig {
        path: Some(PathBuf::from("./dev_data")),
        sync_mode: SyncMode::Normal,
        wal_enabled: true,
        memory_limit: 256 * 1024 * 1024,
        num_databases: 16,
    };
    println!("  Persistent, normal sync, WAL enabled");

    println!("\nProduction (High Durability):");
    let _prod_config = EmbeddedConfig {
        path: Some(PathBuf::from("/var/lib/myapp/data")),
        sync_mode: SyncMode::Full,
        wal_enabled: true,
        memory_limit: 1024 * 1024 * 1024, // 1GB
        num_databases: 16,
    };
    println!("  Full sync mode, WAL enabled, larger memory");

    println!("\nEdge/IoT (Resource Constrained):");
    let _edge_config = EmbeddedConfig {
        path: Some(PathBuf::from("./edge_data")),
        sync_mode: SyncMode::Normal,
        wal_enabled: true,
        memory_limit: 32 * 1024 * 1024, // 32MB
        num_databases: 4,
    };
    println!("  Smaller memory limit, fewer databases");

    println!("\nCaching (Speed Priority):");
    let _cache_config = EmbeddedConfig {
        path: None, // In-memory
        sync_mode: SyncMode::None,
        wal_enabled: false,
        memory_limit: 512 * 1024 * 1024, // 512MB
        num_databases: 1,
    };
    println!("  In-memory, no sync, no WAL");

    // ==================== Cleanup ====================
    println!("\n--- Cleanup ---");

    // Clean up the temporary directory
    std::fs::remove_dir_all(&temp_dir).ok();
    println!("Cleaned up temporary files");

    println!("\n=== Example Complete ===");
    Ok(())
}
