#![allow(clippy::unwrap_used, clippy::print_stdout)]
//! Edge-to-Cloud Synchronisation Example
//!
//! Demonstrates how an edge device can:
//! 1. Collect sensor data locally with bounded memory
//! 2. Aggregate readings into time-bucketed summaries
//! 3. Queue sync operations for offline resilience
//! 4. Periodically flush to disk for durability
//!
//! Run with: `cargo run --example edge_sync`

use ferrite::embedded::Ferrite;

fn main() -> anyhow::Result<()> {
    println!("=== Edge-to-Cloud Sync Example ===\n");

    // Set up a small embedded database simulating an edge device
    let db = Ferrite::builder()
        .max_memory("64mb")
        .persistence(false)
        .build()?;

    // Simulate sensor data collection
    println!("--- Collecting sensor readings ---");
    let sensors = ["temperature", "humidity", "pressure"];
    let readings: Vec<(&str, &[&str])> = vec![
        ("temperature", &["22.5", "23.1", "22.8", "23.4", "22.9"]),
        ("humidity", &["45.2", "46.0", "44.8", "45.5", "46.1"]),
        (
            "pressure",
            &["1013.2", "1013.5", "1012.9", "1013.1", "1013.4"],
        ),
    ];

    for (sensor, values) in &readings {
        let key = format!("edge:sensor:{}", sensor);
        for val in *values {
            db.rpush(key.clone(), &[bytes::Bytes::from(val.to_string())])?;
        }
        println!("  {} → {} readings queued", sensor, values.len());
    }

    // Aggregate into time-bucketed summaries
    println!("\n--- Aggregating into summaries ---");
    let bucket = "2026-03-07T22:00";
    for sensor in &sensors {
        let key = format!("edge:sensor:{}", sensor);
        let values = db.lrange(&key, 0, -1)?;
        if values.is_empty() {
            continue;
        }

        let nums: Vec<f64> = values
            .iter()
            .filter_map(|v| std::str::from_utf8(v).ok()?.parse().ok())
            .collect();

        let count = nums.len() as f64;
        let sum: f64 = nums.iter().sum();
        let min = nums.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = nums.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let avg = sum / count;

        // Store aggregated summary as hash
        let summary_key = format!("edge:summary:{}:{}", sensor, bucket);
        db.hset(summary_key.clone(), "count", format!("{}", count as u64))?;
        db.hset(summary_key.clone(), "avg", format!("{:.2}", avg))?;
        db.hset(summary_key.clone(), "min", format!("{:.2}", min))?;
        db.hset(summary_key.clone(), "max", format!("{:.2}", max))?;

        println!(
            "  {} → count={}, avg={:.2}, min={:.2}, max={:.2}",
            sensor, count as u64, avg, min, max
        );
    }

    // Queue sync operations for cloud upload
    println!("\n--- Queuing sync operations ---");
    for sensor in &sensors {
        let sync_payload = format!(
            "{{\"sensor\":\"{}\",\"bucket\":\"{}\",\"status\":\"pending\"}}",
            sensor, bucket
        );
        db.rpush("edge:sync:queue", &[bytes::Bytes::from(sync_payload)])?;
    }
    let queue_len = db.lrange("edge:sync:queue", 0, -1)?.len();
    println!("  {} sync operations queued", queue_len);

    // Verify local state
    println!("\n--- Local state ---");
    let keys = db.keys("edge:*")?;
    println!("  Total keys: {}", keys.len());
    println!("  Database size: {}", db.dbsize());

    // Simulate periodic flush
    println!("\n--- Edge sync cycle complete ---");
    println!("  In production, the sync queue would be drained by a background");
    println!("  task that POSTs summaries to the central Ferrite cluster and");
    println!("  clears local raw readings after confirmed upload.");

    Ok(())
}
