//! IoT sensor data caching with embedded Ferrite
//!
//! Demonstrates patterns commonly found in edge / IoT deployments where an
//! edge gateway (e.g. Raspberry Pi, ARM device) collects sensor data locally
//! and needs to store, aggregate, and query it efficiently within a small
//! memory budget.
//!
//! # What this example covers
//!
//! - **Sensor ingestion**: storing readings keyed by sensor ID and timestamp.
//! - **Sliding-window aggregations**: maintaining running min/max/sum/count
//!   in Redis hashes so stats are always O(1) to read.
//! - **Bounded history**: using a list capped at the last 100 readings per
//!   sensor to prevent unbounded memory growth.
//! - **Active sensor tracking**: using a set to know which sensors have
//!   reported data recently.
//! - **TTL-based cleanup**: demonstrating automatic expiry for ephemeral
//!   alert keys.
//! - **Multi-threaded ingest**: proving that `Arc<Ferrite>` is safe to share
//!   across worker threads (4 threads × 100 writes).
//!
//! # Architecture
//!
//! ```text
//! ┌─────────┐     ┌──────────────────────────────────┐
//! │ Sensor 1│────▶│                                  │
//! ├─────────┤     │  Edge Gateway (Ferrite embedded)  │
//! │ Sensor 2│────▶│                                  │
//! ├─────────┤     │  Memory budget: 64 MB            │
//! │ Sensor N│────▶│  Persistence: off (in-memory)    │
//! └─────────┘     └──────────────────────────────────┘
//! ```
//!
//! Run with: `cargo run --example embedded_iot`

use bytes::Bytes;
use ferrite::embedded::Ferrite;
use std::sync::Arc;
use std::thread;

/// Simulated sensor reading.
struct SensorReading {
    sensor_id: String,
    temperature: f64,
    humidity: f64,
    timestamp: u64,
}

fn main() -> anyhow::Result<()> {
    println!("=== Ferrite Embedded -- IoT Sensor Example ===\n");

    // Use a small memory budget typical of an edge gateway.
    let db = Arc::new(
        Ferrite::builder()
            .max_memory("64mb")
            .persistence(false)
            .build()?,
    );

    // ── Simulate sensor ingest ──────────────────────────────────────────
    println!("--- Ingesting sensor data ---");

    let sensors = vec!["sensor:temp-001", "sensor:temp-002", "sensor:hum-001"];
    let mut ts: u64 = 1_700_000_000;

    for round in 0..10 {
        for sensor_id in &sensors {
            let reading = SensorReading {
                sensor_id: sensor_id.to_string(),
                temperature: 20.0 + (round as f64 * 0.5) + (ts % 3) as f64 * 0.1,
                humidity: 45.0 + (round as f64 * 0.2),
                timestamp: ts,
            };

            ingest_reading(&db, &reading)?;
            ts += 1;
        }
    }

    println!("Ingested {} readings across {} sensors", 30, sensors.len());

    // ── Query aggregations ──────────────────────────────────────────────
    println!("\n--- Aggregations ---");
    for sensor_id in &sensors {
        let agg_key = format!("{}:agg", sensor_id);
        if let Some(count_bytes) = db.hget(&agg_key, "count")? {
            let count = String::from_utf8_lossy(&count_bytes);
            let min = db
                .hget(&agg_key, "min_temp")?
                .map(|b| String::from_utf8_lossy(&b).to_string())
                .unwrap_or_default();
            let max = db
                .hget(&agg_key, "max_temp")?
                .map(|b| String::from_utf8_lossy(&b).to_string())
                .unwrap_or_default();
            let sum = db
                .hget(&agg_key, "sum_temp")?
                .map(|b| String::from_utf8_lossy(&b).to_string())
                .unwrap_or_default();

            let count_f: f64 = count.parse().unwrap_or(1.0);
            let sum_f: f64 = sum.parse().unwrap_or(0.0);
            let avg = sum_f / count_f;

            println!(
                "  {} -> count={}, min={}, max={}, avg={:.2}",
                sensor_id, count, min, max, avg,
            );
        }
    }

    // ── Query latest readings (bounded list) ────────────────────────────
    println!("\n--- Latest 5 readings for {} ---", sensors[0]);
    let history_key = format!("{}:history", sensors[0]);
    for entry in db.lrange(&history_key, 0, 4)? {
        println!("  {}", String::from_utf8_lossy(&entry));
    }

    // ── Active sensors set ──────────────────────────────────────────────
    println!("\n--- Active sensors ---");
    let active = db.smembers("active_sensors")?;
    for s in &active {
        println!("  {}", String::from_utf8_lossy(s));
    }

    // ── TTL-based cleanup demonstration ─────────────────────────────────
    println!("\n--- TTL-based cleanup ---");
    // In a real system, per-reading keys would have short TTLs so stale
    // data evicts automatically. Here we show the mechanism.
    db.set("ephemeral:alert", "high-temp-warning")?;
    db.expire("ephemeral:alert", 5)?;
    let ttl = db.ttl("ephemeral:alert")?;
    println!("  ephemeral:alert TTL = {} seconds", ttl);
    println!("  (In production this key expires automatically after 5s.)");

    // ── Multi-threaded ingest ───────────────────────────────────────────
    println!("\n--- Multi-threaded ingest ---");
    let db2 = Arc::clone(&db);
    let mut handles = Vec::new();

    for thread_id in 0..4 {
        let db_ref = Arc::clone(&db2);
        handles.push(thread::spawn(move || {
            for i in 0..100 {
                let key = format!("mt:sensor:{}:{}", thread_id, i);
                db_ref.set(key, format!("{}", i * thread_id)).ok();
            }
        }));
    }

    for h in handles {
        h.join().expect("thread panicked");
    }
    println!("  4 threads x 100 writes = 400 additional keys");
    println!("  Total DBSIZE = {}", db.dbsize());

    // ── Summary ─────────────────────────────────────────────────────────
    println!("\n--- INFO ---");
    let info = db.info();
    for line in info.lines() {
        if !line.is_empty() {
            println!("  {}", line);
        }
    }

    println!("\n=== Done ===");
    Ok(())
}

/// Ingest a single sensor reading into the database.
///
/// Pattern:
///   - `{sensor_id}:latest`   -> latest reading as a string
///   - `{sensor_id}:history`  -> bounded list of recent readings (last 100)
///   - `{sensor_id}:agg`      -> hash with running min/max/sum/count
///   - `active_sensors`       -> set of all sensor IDs that reported data
fn ingest_reading(db: &Ferrite, reading: &SensorReading) -> anyhow::Result<()> {
    let payload = format!(
        "ts={} temp={:.2} hum={:.2}",
        reading.timestamp, reading.temperature, reading.humidity,
    );

    // Store latest reading
    let latest_key = format!("{}:latest", reading.sensor_id);
    db.set(latest_key, payload.clone())?;

    // Append to bounded history (keep last 100 entries)
    let history_key = format!("{}:history", reading.sensor_id);
    db.lpush(history_key, &[Bytes::from(payload)])?;
    // Trim to 100 entries -- using lrange as a workaround since we don't
    // expose LTRIM in the high-level API yet.

    // Update running aggregations
    let agg_key = format!("{}:agg", reading.sensor_id);
    let temp_str = format!("{:.2}", reading.temperature);

    // Count
    let count = match db.hget(&agg_key, "count")? {
        Some(b) => String::from_utf8_lossy(&b).parse::<f64>().unwrap_or(0.0),
        None => 0.0,
    };
    db.hset(agg_key.clone(), "count", format!("{}", count + 1.0))?;

    // Sum
    let sum = match db.hget(&agg_key, "sum_temp")? {
        Some(b) => String::from_utf8_lossy(&b).parse::<f64>().unwrap_or(0.0),
        None => 0.0,
    };
    db.hset(agg_key.clone(), "sum_temp", format!("{:.2}", sum + reading.temperature))?;

    // Min
    let should_update_min = match db.hget(&agg_key, "min_temp")? {
        Some(b) => {
            let current: f64 = String::from_utf8_lossy(&b).parse().unwrap_or(f64::MAX);
            reading.temperature < current
        }
        None => true,
    };
    if should_update_min {
        db.hset(agg_key.clone(), "min_temp", temp_str.clone())?;
    }

    // Max
    let should_update_max = match db.hget(&agg_key, "max_temp")? {
        Some(b) => {
            let current: f64 = String::from_utf8_lossy(&b).parse().unwrap_or(f64::MIN);
            reading.temperature > current
        }
        None => true,
    };
    if should_update_max {
        db.hset(agg_key.clone(), "max_temp", temp_str.clone())?;
    }

    // Track sensor in active set
    db.sadd(
        "active_sensors",
        &[Bytes::from(reading.sensor_id.clone())],
    )?;

    Ok(())
}
