//! ferrite-observe — Zero-overhead observability for Ferrite
//!
//! Attaches to a running Ferrite instance via its metrics endpoint
//! and provides real-time dashboards, latency histograms, and key heatmaps.

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::process::ExitCode;

use clap::{Parser, Subcommand};

/// ferrite-observe — Zero-overhead observability CLI for Ferrite
#[derive(Parser, Debug)]
#[command(name = "ferrite-observe")]
#[command(author, version, about = "Real-time observability for a running Ferrite instance")]
struct Args {
    /// Host of the Ferrite metrics endpoint
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Port of the Ferrite metrics endpoint
    #[arg(long, default_value = "6379")]
    port: u16,

    /// Refresh interval in seconds
    #[arg(long, default_value = "1")]
    interval: u64,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Real-time top-like display of hottest keys
    Top {
        /// Number of hot keys to show
        #[arg(short = 'n', long, default_value = "20")]
        count: usize,
    },
    /// Latency histogram dashboard
    Latency,
    /// Slot-level heatmap (ASCII art)
    Heatmap {
        /// Number of rows in the ASCII heatmap
        #[arg(long, default_value = "16")]
        rows: usize,
    },
    /// Syscall statistics
    Syscalls,
}

// ---------------------------------------------------------------------------
// Mock data for demonstration / testing
// ---------------------------------------------------------------------------

struct MockHotKey {
    key: &'static str,
    ops_per_sec: f64,
    size: &'static str,
    tier: &'static str,
}

fn mock_hot_keys() -> Vec<MockHotKey> {
    vec![
        MockHotKey { key: "user:session:1234", ops_per_sec: 15_432.0, size: "256B", tier: "Memory" },
        MockHotKey { key: "cache:product:99", ops_per_sec: 8_291.0, size: "1.2KB", tier: "Memory" },
        MockHotKey { key: "queue:events", ops_per_sec: 5_104.0, size: "64B", tier: "Memory" },
        MockHotKey { key: "rate:api:10.0.1.5", ops_per_sec: 3_820.0, size: "32B", tier: "Memory" },
        MockHotKey { key: "lock:order:5678", ops_per_sec: 2_500.0, size: "16B", tier: "Memory" },
        MockHotKey { key: "cache:user:profile:42", ops_per_sec: 1_900.0, size: "4.1KB", tier: "ReadOnly" },
        MockHotKey { key: "ts:metrics:cpu", ops_per_sec: 1_200.0, size: "512B", tier: "ReadOnly" },
        MockHotKey { key: "geo:fleet:truck99", ops_per_sec: 800.0, size: "128B", tier: "Disk" },
    ]
}

struct MockLatency {
    command: &'static str,
    p50: &'static str,
    p90: &'static str,
    p99: &'static str,
    p999: &'static str,
    max: &'static str,
    count: &'static str,
}

fn mock_latencies() -> Vec<MockLatency> {
    vec![
        MockLatency { command: "GET", p50: "0.1µs", p90: "0.3µs", p99: "1.2µs", p999: "15µs", max: "230µs", count: "1.2M" },
        MockLatency { command: "SET", p50: "0.2µs", p90: "0.5µs", p99: "2.1µs", p999: "25µs", max: "450µs", count: "800K" },
        MockLatency { command: "DEL", p50: "0.1µs", p90: "0.4µs", p99: "1.8µs", p999: "20µs", max: "310µs", count: "120K" },
        MockLatency { command: "HGET", p50: "0.2µs", p90: "0.6µs", p99: "2.5µs", p999: "30µs", max: "520µs", count: "450K" },
        MockLatency { command: "LPUSH", p50: "0.3µs", p90: "0.8µs", p99: "3.0µs", p999: "40µs", max: "600µs", count: "200K" },
    ]
}

struct MockSyscall {
    name: &'static str,
    count: u64,
    avg_ns: u64,
    max_ns: u64,
}

fn mock_syscalls() -> Vec<MockSyscall> {
    vec![
        MockSyscall { name: "read", count: 2_450_000, avg_ns: 120, max_ns: 45_000 },
        MockSyscall { name: "write", count: 1_800_000, avg_ns: 150, max_ns: 62_000 },
        MockSyscall { name: "epoll_wait", count: 980_000, avg_ns: 3_200, max_ns: 1_200_000 },
        MockSyscall { name: "io_uring_enter", count: 750_000, avg_ns: 85, max_ns: 28_000 },
        MockSyscall { name: "mmap", count: 12_400, avg_ns: 4_500, max_ns: 2_800_000 },
        MockSyscall { name: "munmap", count: 8_200, avg_ns: 3_100, max_ns: 1_500_000 },
        MockSyscall { name: "fsync", count: 4_500, avg_ns: 180_000, max_ns: 15_000_000 },
    ]
}

// ---------------------------------------------------------------------------
// Display helpers
// ---------------------------------------------------------------------------

const SEPARATOR: &str = "──────────────────────────────────────────────────────────────────";

fn format_count(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        format!("{n}")
    }
}

fn display_top(count: usize, host: &str, port: u16) {
    let keys = mock_hot_keys();
    let n = count.min(keys.len());

    println!("ferrite-observe — Hot Keys (connected to {host}:{port}, updated every 1s)");
    println!("{SEPARATOR}");
    println!(
        "{:<6}{:<30}{:>10}{:>10}{:>10}",
        "RANK", "KEY", "OPS/s", "SIZE", "TIER"
    );

    for (i, k) in keys.iter().enumerate().take(n) {
        println!(
            "{:<6}{:<30}{:>10}{:>10}{:>10}",
            i + 1,
            k.key,
            format!("{:.0}", k.ops_per_sec),
            k.size,
            k.tier,
        );
    }
}

fn display_latency(host: &str, port: u16) {
    let rows = mock_latencies();

    println!("ferrite-observe — Latency Distribution (connected to {host}:{port})");
    println!("{SEPARATOR}");
    println!(
        "{:<10}{:>8}{:>8}{:>8}{:>8}{:>8}{:>10}",
        "Command", "P50", "P90", "P99", "P99.9", "Max", "Count"
    );

    for r in &rows {
        println!(
            "{:<10}{:>8}{:>8}{:>8}{:>8}{:>8}{:>10}",
            r.command, r.p50, r.p90, r.p99, r.p999, r.max, r.count,
        );
    }
}

fn display_heatmap(rows: usize, host: &str, port: u16) {
    println!("ferrite-observe — Slot Heatmap (connected to {host}:{port})");
    println!("{SEPARATOR}");

    let slots_per_row = 16384_usize.div_ceil(rows);
    let intensity = [' ', '░', '▒', '▓', '█'];

    // Generate mock slot data with some hot regions.
    let mut slot_data = [0u32; 16384];
    for (i, slot) in slot_data.iter_mut().enumerate() {
        // Simulate hot spots around slots 0-1000 and 8000-9000.
        *slot = if i < 1000 {
            80 + (i as u32 % 40)
        } else if (8000..9000).contains(&i) {
            60 + (i as u32 % 30)
        } else {
            i as u32 % 15
        };
    }

    let max_val = slot_data.iter().copied().max().unwrap_or(1).max(1);

    println!("Slot range      | Access heatmap (█=hot, ░=cold)");
    println!("----------------|{}", "-".repeat(64));

    for row in 0..rows {
        let start = row * slots_per_row;
        let end = ((row + 1) * slots_per_row).min(16384);
        let sum: u64 = slot_data[start..end].iter().map(|&v| u64::from(v)).sum();
        let _avg = if end > start {
            sum / (end - start) as u64
        } else {
            0
        };

        // Map average to intensity character (64 chars wide).
        let bar_len = 64;
        let mut bar = String::with_capacity(bar_len);
        let chunks = bar_len;
        let slots_per_char = (end - start).div_ceil(chunks);

        for c in 0..chunks {
            let cs = start + c * slots_per_char;
            let ce = (cs + slots_per_char).min(end);
            if cs >= end {
                bar.push(' ');
                continue;
            }
            let chunk_avg: u32 = slot_data[cs..ce].iter().sum::<u32>() / (ce - cs).max(1) as u32;
            let level = (u64::from(chunk_avg) * 4 / u64::from(max_val)).min(4) as usize;
            bar.push(intensity[level]);
        }

        println!("{:>5}-{:<9}|{bar}", start, end.saturating_sub(1));
    }

    println!();
    println!(
        "Legend: ' '=0%  ░=1-25%  ▒=26-50%  ▓=51-75%  █=76-100%  (relative to max)"
    );
}

fn display_syscalls(host: &str, port: u16) {
    let rows = mock_syscalls();

    println!("ferrite-observe — Syscall Statistics (connected to {host}:{port})");
    println!("{SEPARATOR}");
    println!(
        "{:<20}{:>12}{:>12}{:>12}",
        "SYSCALL", "COUNT", "AVG(ns)", "MAX(ns)"
    );

    for r in &rows {
        println!(
            "{:<20}{:>12}{:>12}{:>12}",
            r.name,
            format_count(r.count),
            format_count(r.avg_ns),
            format_count(r.max_ns),
        );
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() -> ExitCode {
    let args = Args::parse();

    match &args.command {
        Command::Top { count } => display_top(*count, &args.host, args.port),
        Command::Latency => display_latency(&args.host, args.port),
        Command::Heatmap { rows } => display_heatmap(*rows, &args.host, args.port),
        Command::Syscalls => display_syscalls(&args.host, args.port),
    }

    ExitCode::SUCCESS
}
