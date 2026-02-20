//! Edge caching with periodic cloud sync
//!
//! Demonstrates the edge-to-cloud sync pattern commonly used in IoT and edge
//! computing deployments:
//!
//! - Local `LiteDatabase` acts as an edge cache with bounded memory.
//! - A `SyncEngine` tracks all local mutations as a change log.
//! - Periodically, a delta is generated and "uploaded" to the cloud.
//! - Incoming remote deltas are applied with conflict resolution.
//!
//! This pattern ensures data is always available locally (offline-first) while
//! eventually propagating to the central cloud when connectivity allows.
//!
//! Run with: `cargo run --example embedded_edge_cache`

use bytes::Bytes;
use ferrite_core::embedded::lite::{LiteConfig, LiteDatabase};
use ferrite_core::embedded::sync::{
    ChangeType, ConflictResolution, SyncConfig, SyncEngine,
};

fn main() -> anyhow::Result<()> {
    println!("=== Ferrite Embedded -- Edge Cache + Cloud Sync ===\n");

    // ── 1. Configure the local edge database ────────────────────────────
    //
    // On a resource-constrained device (e.g. Raspberry Pi, ARM gateway) we
    // keep a small memory budget and enable compression to maximise the
    // amount of data we can hold locally.
    println!("--- Setting up edge database ---");
    let db_config = LiteConfig {
        max_memory_mb: 32,
        max_keys: 10_000,
        enable_persistence: false, // in-memory for this demo
        data_dir: "/tmp/ferrite-edge-demo".to_string(),
        compression_enabled: true,
        enable_sync: false, // we drive sync manually via SyncEngine
        ..Default::default()
    };

    let db = LiteDatabase::new(db_config)?;
    println!("  Edge database ready (32 MB budget, 10K key limit)");

    // ── 2. Configure the sync engine ────────────────────────────────────
    //
    // The sync engine maintains a change log with vector clocks so we can
    // compute compact deltas and handle conflicts deterministically.
    let sync_config = SyncConfig {
        node_id: "edge-gateway-01".to_string(),
        conflict_resolution: ConflictResolution::LastWriteWins,
        compression: true,
        batch_size: 500,
        ..Default::default()
    };

    let sync = SyncEngine::new(sync_config);
    println!("  Sync engine ready (node: edge-gateway-01)\n");

    // ── 3. Collect sensor data locally ──────────────────────────────────
    //
    // Simulate an edge gateway receiving readings from multiple sensors.
    // Each reading is stored in the local database AND recorded in the
    // sync engine's change log.
    println!("--- Collecting sensor data ---");
    let sensors = ["temp-north", "temp-south", "humidity-east", "pressure-west"];
    let mut ts: u64 = 1_700_000_000;

    for round in 0..25 {
        for sensor in &sensors {
            let key = format!("sensor:{}:{}", sensor, ts);
            let value = format!(
                "{{\"v\":{:.1},\"ts\":{},\"round\":{}}}",
                20.0 + (round as f64 * 0.3) + (ts % 5) as f64 * 0.1,
                ts,
                round,
            );

            // Store locally
            db.set(&key, &value)?;

            // Track for sync
            sync.record_change(&key, ChangeType::Set, Some(Bytes::from(value)));

            ts += 1;
        }
    }

    let stats = db.stats();
    println!(
        "  Collected {} keys, {} bytes used / {} bytes limit",
        stats.keys_count, stats.memory_used_bytes, stats.memory_limit_bytes,
    );
    println!("  Sync sequence: {}\n", sync.sequence());

    // ── 4. Generate a delta for cloud upload ────────────────────────────
    //
    // In production this delta would be serialised and sent over HTTPS,
    // MQTT, or another transport. Here we just inspect it.
    println!("--- Generating sync delta ---");
    let delta = sync.get_delta(0);
    println!(
        "  Delta: {} changes (seq {}-{})",
        delta.changes.len(),
        delta.from_sequence,
        delta.to_sequence,
    );

    let delta_bytes = delta.to_bytes()?;
    println!(
        "  Serialised size: {} bytes (compressed: {})",
        delta_bytes.len(),
        delta.compressed,
    );

    // Verify checksum round-trip
    assert!(delta.verify_checksum(), "checksum verification failed");
    println!("  Checksum: OK\n");

    // ── 5. Simulate receiving a remote delta ────────────────────────────
    //
    // In a real system another edge node or the cloud would send us a
    // delta with its own changes. We apply it locally, resolving any
    // conflicts with the configured strategy (LastWriteWins).
    println!("--- Simulating remote delta application ---");
    let remote_sync_config = SyncConfig {
        node_id: "cloud-primary".to_string(),
        conflict_resolution: ConflictResolution::LastWriteWins,
        compression: true,
        ..Default::default()
    };
    let remote_engine = SyncEngine::new(remote_sync_config);

    // Remote node writes overlapping and non-overlapping keys
    remote_engine.record_change(
        "sensor:temp-north:1700000000",
        ChangeType::Set,
        Some(Bytes::from("{\"v\":99.9,\"ts\":1700000000,\"source\":\"cloud\"}")),
    );
    remote_engine.record_change(
        "config:sample_rate",
        ChangeType::Set,
        Some(Bytes::from("2000")),
    );
    remote_engine.record_change(
        "config:sync_interval",
        ChangeType::Set,
        Some(Bytes::from("30")),
    );

    let remote_delta = remote_engine.get_delta(0);
    let result = sync.apply_delta(&remote_delta)?;
    println!(
        "  Applied: {}, Skipped: {}, Conflicts resolved: {}",
        result.applied, result.skipped, result.conflicts,
    );

    // Verify the cloud's config was applied locally
    if let Some(rate) = sync.get("config:sample_rate") {
        println!(
            "  config:sample_rate = {} (from cloud)",
            String::from_utf8_lossy(&rate),
        );
    }

    // ── 6. Periodic cleanup ─────────────────────────────────────────────
    //
    // On constrained devices, periodic compaction reclaims memory from
    // deleted/expired entries.
    println!("\n--- Cleanup ---");
    let compact = db.compact()?;
    println!(
        "  Compaction: {} bytes reclaimed in {}ms",
        compact.bytes_reclaimed, compact.duration_ms,
    );

    // Sync stats
    let sync_stats = sync.stats();
    println!(
        "  Sync stats: {} syncs, {} changes synced, {} conflicts resolved",
        sync_stats.sync_count,
        sync_stats.changes_synced,
        sync_stats.conflicts_resolved,
    );

    // ── 7. Summary ──────────────────────────────────────────────────────
    println!("\n--- Final State ---");
    let final_stats = db.stats();
    println!("  Keys: {}", final_stats.keys_count);
    println!("  Memory: {} bytes", final_stats.memory_used_bytes);
    println!("  Total ops: {}", final_stats.ops_total);
    println!("  Uptime: {}s", final_stats.uptime_secs);

    println!("\n=== Done ===");
    Ok(())
}
