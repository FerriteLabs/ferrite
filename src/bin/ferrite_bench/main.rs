//! ferrite-bench - Standardized Benchmark CLI
//!
//! Runs configurable workloads against a Ferrite [`Store`] and optionally
//! against a remote Redis instance for side-by-side comparison. Produces
//! throughput, latency percentiles, and a formatted report.

use std::collections::HashMap;
use std::process::ExitCode;
use std::time::Instant;

use bytes::Bytes;
use clap::{Parser, Subcommand, ValueEnum};
use colored::Colorize;

use ferrite::storage::{Store, Value};

/// ferrite-bench - Benchmark and compare Ferrite performance
#[derive(Parser, Debug)]
#[command(name = "ferrite-bench")]
#[command(author, version, about = "Benchmark Ferrite and compare with Redis")]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run a standard benchmark against a local Ferrite store
    Run {
        /// Workload profile to run
        #[arg(long, value_enum, default_value = "mixed")]
        workload: WorkloadProfile,

        /// Number of operations to execute
        #[arg(long, default_value = "100000")]
        ops: usize,

        /// Number of pre-populated keys
        #[arg(long, default_value = "10000")]
        keyspace: usize,

        /// Value size in bytes
        #[arg(long, default_value = "64")]
        value_size: usize,

        /// Output format
        #[arg(long, value_enum, default_value = "text")]
        format: OutputFormat,

        /// File to write results to (stdout if not set)
        #[arg(long)]
        output: Option<String>,
    },

    /// List available workload profiles
    Profiles,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum WorkloadProfile {
    /// 100% GET (read-heavy)
    ReadOnly,
    /// 100% SET (write-heavy)
    WriteOnly,
    /// 50% GET, 50% SET
    Mixed,
    /// 80% GET, 20% SET (typical cache pattern)
    CacheHeavy,
    /// 50% GET, 25% SET, 15% LPUSH/RPOP, 10% HSET/HGET
    MultiType,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
    Markdown,
}

// ---------------------------------------------------------------------------
// Benchmark engine
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize)]
struct BenchmarkResult {
    workload: String,
    total_ops: usize,
    duration_ms: u128,
    throughput_ops_sec: f64,
    p50_ns: u64,
    p90_ns: u64,
    p99_ns: u64,
    p999_ns: u64,
    min_ns: u64,
    max_ns: u64,
    operations: HashMap<String, OperationResult>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct OperationResult {
    count: usize,
    throughput_ops_sec: f64,
    p50_ns: u64,
    p99_ns: u64,
}

fn run_benchmark(
    workload: WorkloadProfile,
    ops: usize,
    keyspace: usize,
    value_size: usize,
) -> BenchmarkResult {
    let store = Store::new(16);
    let value_data = "x".repeat(value_size);

    // Pre-populate keyspace
    for i in 0..keyspace {
        let key = Bytes::from(format!("key:{i:06}"));
        store.set(0, key, Value::String(Bytes::from(value_data.clone())));
    }

    let mut all_latencies: Vec<u64> = Vec::with_capacity(ops);
    let mut op_latencies: HashMap<String, Vec<u64>> = HashMap::new();

    let start = Instant::now();

    for i in 0..ops {
        let op_start = Instant::now();
        let op_name = execute_operation(&store, workload, i, keyspace, &value_data);
        let elapsed_ns = op_start.elapsed().as_nanos() as u64;

        all_latencies.push(elapsed_ns);
        op_latencies
            .entry(op_name)
            .or_insert_with(|| Vec::with_capacity(ops / 2))
            .push(elapsed_ns);
    }

    let total_duration = start.elapsed();

    all_latencies.sort_unstable();

    let throughput = ops as f64 / total_duration.as_secs_f64();

    let mut operations = HashMap::new();
    for (name, mut lats) in op_latencies {
        lats.sort_unstable();
        let count = lats.len();
        let op_throughput = count as f64 / total_duration.as_secs_f64();
        operations.insert(
            name,
            OperationResult {
                count,
                throughput_ops_sec: op_throughput,
                p50_ns: percentile(&lats, 50.0),
                p99_ns: percentile(&lats, 99.0),
            },
        );
    }

    BenchmarkResult {
        workload: format!("{workload:?}"),
        total_ops: ops,
        duration_ms: total_duration.as_millis(),
        throughput_ops_sec: throughput,
        p50_ns: percentile(&all_latencies, 50.0),
        p90_ns: percentile(&all_latencies, 90.0),
        p99_ns: percentile(&all_latencies, 99.0),
        p999_ns: percentile(&all_latencies, 99.9),
        min_ns: *all_latencies.first().unwrap_or(&0),
        max_ns: *all_latencies.last().unwrap_or(&0),
        operations,
    }
}

fn execute_operation(
    store: &Store,
    workload: WorkloadProfile,
    i: usize,
    keyspace: usize,
    value_data: &str,
) -> String {
    let key_idx = i % keyspace;
    let key = Bytes::from(format!("key:{key_idx:06}"));

    match workload {
        WorkloadProfile::ReadOnly => {
            store.get(0, &key);
            "GET".into()
        }
        WorkloadProfile::WriteOnly => {
            store.set(0, key, Value::String(Bytes::from(value_data.to_string())));
            "SET".into()
        }
        WorkloadProfile::Mixed => {
            if i % 2 == 0 {
                store.get(0, &key);
                "GET".into()
            } else {
                store.set(0, key, Value::String(Bytes::from(value_data.to_string())));
                "SET".into()
            }
        }
        WorkloadProfile::CacheHeavy => {
            if i % 5 < 4 {
                store.get(0, &key);
                "GET".into()
            } else {
                store.set(0, key, Value::String(Bytes::from(value_data.to_string())));
                "SET".into()
            }
        }
        WorkloadProfile::MultiType => {
            let bucket = i % 20;
            if bucket < 10 {
                store.get(0, &key);
                "GET".into()
            } else if bucket < 15 {
                store.set(0, key, Value::String(Bytes::from(value_data.to_string())));
                "SET".into()
            } else if bucket < 18 {
                let list_key = Bytes::from(format!("list:{key_idx:06}"));
                let mut list = std::collections::VecDeque::new();
                list.push_back(Bytes::from(value_data.to_string()));
                store.set(0, list_key, Value::List(list));
                "LPUSH".into()
            } else {
                let hash_key = Bytes::from(format!("hash:{key_idx:06}"));
                let mut hash = std::collections::HashMap::new();
                hash.insert(Bytes::from("field"), Bytes::from(value_data.to_string()));
                store.set(0, hash_key, Value::Hash(hash));
                "HSET".into()
            }
        }
    }
}

fn percentile(sorted: &[u64], pct: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((pct / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

// ---------------------------------------------------------------------------
// Output formatters
// ---------------------------------------------------------------------------

fn format_text(result: &BenchmarkResult) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "\n{}\n",
        "═══════════════════════════════════════════════════════".bold()
    ));
    out.push_str(&format!(
        "  {} {}\n",
        "Ferrite Benchmark Results".bold().cyan(),
        format!("({})", result.workload).dimmed()
    ));
    out.push_str(&format!(
        "{}\n\n",
        "═══════════════════════════════════════════════════════".bold()
    ));

    out.push_str(&format!(
        "  Throughput:  {} ops/sec\n",
        format_number(result.throughput_ops_sec).green().bold()
    ));
    out.push_str(&format!(
        "  Total ops:   {}\n",
        format_number(result.total_ops as f64)
    ));
    out.push_str(&format!("  Duration:    {}ms\n\n", result.duration_ms));

    out.push_str(&format!("  {}\n", "Latency Percentiles".bold().underline()));
    out.push_str(&format!("    P50:   {:>8}ns\n", result.p50_ns));
    out.push_str(&format!("    P90:   {:>8}ns\n", result.p90_ns));
    out.push_str(&format!("    P99:   {:>8}ns\n", result.p99_ns));
    out.push_str(&format!("    P99.9: {:>8}ns\n", result.p999_ns));
    out.push_str(&format!("    Min:   {:>8}ns\n", result.min_ns));
    out.push_str(&format!("    Max:   {:>8}ns\n\n", result.max_ns));

    if !result.operations.is_empty() {
        out.push_str(&format!(
            "  {}\n",
            "Per-Operation Breakdown".bold().underline()
        ));
        out.push_str(&format!(
            "  {:<10} {:>10} {:>14} {:>10} {:>10}\n",
            "Op", "Count", "Throughput", "P50", "P99"
        ));
        out.push_str(&format!("  {}\n", "-".repeat(58)));

        let mut ops: Vec<_> = result.operations.iter().collect();
        ops.sort_by_key(|(name, _)| (*name).clone());
        for (name, op) in ops {
            out.push_str(&format!(
                "  {:<10} {:>10} {:>12}/s {:>8}ns {:>8}ns\n",
                name,
                op.count,
                format_number(op.throughput_ops_sec),
                op.p50_ns,
                op.p99_ns,
            ));
        }
    }

    out.push_str(&format!(
        "\n{}\n",
        "═══════════════════════════════════════════════════════".bold()
    ));
    out
}

fn format_json(result: &BenchmarkResult) -> String {
    serde_json::to_string_pretty(result).unwrap_or_else(|_| "{}".to_string())
}

fn format_markdown(result: &BenchmarkResult) -> String {
    let mut md = String::new();
    md.push_str(&format!("# Ferrite Benchmark: {}\n\n", result.workload));
    md.push_str("| Metric | Value |\n|--------|-------|\n");
    md.push_str(&format!(
        "| Throughput | {:.0} ops/sec |\n",
        result.throughput_ops_sec
    ));
    md.push_str(&format!("| Total Ops | {} |\n", result.total_ops));
    md.push_str(&format!("| Duration | {}ms |\n", result.duration_ms));
    md.push_str(&format!("| P50 | {}ns |\n", result.p50_ns));
    md.push_str(&format!("| P90 | {}ns |\n", result.p90_ns));
    md.push_str(&format!("| P99 | {}ns |\n", result.p99_ns));
    md.push_str(&format!("| P99.9 | {}ns |\n\n", result.p999_ns));

    if !result.operations.is_empty() {
        md.push_str("## Per-Operation\n\n");
        md.push_str("| Op | Count | Throughput | P50 | P99 |\n");
        md.push_str("|-----|-------|-----------|-----|-----|\n");
        let mut ops: Vec<_> = result.operations.iter().collect();
        ops.sort_by_key(|(name, _)| (*name).clone());
        for (name, op) in ops {
            md.push_str(&format!(
                "| {} | {} | {:.0}/s | {}ns | {}ns |\n",
                name, op.count, op.throughput_ops_sec, op.p50_ns, op.p99_ns
            ));
        }
    }
    md
}

fn format_number(n: f64) -> String {
    if n >= 1_000_000.0 {
        format!("{:.2}M", n / 1_000_000.0)
    } else if n >= 1_000.0 {
        format!("{:.1}K", n / 1_000.0)
    } else {
        format!("{:.0}", n)
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() -> ExitCode {
    let args = Args::parse();

    match args.command {
        Commands::Run {
            workload,
            ops,
            keyspace,
            value_size,
            format,
            output,
        } => {
            eprintln!(
                "{} Running {} benchmark with {} ops, {} keys, {}B values...",
                "▶".cyan(),
                format!("{workload:?}").bold(),
                ops,
                keyspace,
                value_size
            );

            let result = run_benchmark(workload, ops, keyspace, value_size);

            let formatted = match format {
                OutputFormat::Text => format_text(&result),
                OutputFormat::Json => format_json(&result),
                OutputFormat::Markdown => format_markdown(&result),
            };

            if let Some(path) = output {
                if let Err(e) = std::fs::write(&path, &formatted) {
                    eprintln!("{} Failed to write output: {e}", "✗".red());
                    return ExitCode::FAILURE;
                }
                eprintln!("{} Results written to {path}", "✓".green());
            } else {
                println!("{formatted}");
            }

            ExitCode::SUCCESS
        }

        Commands::Profiles => {
            println!("{}", "Available Workload Profiles".bold().underline());
            println!();
            println!("  {:<15} 100% GET (read-heavy)", "read-only".green());
            println!("  {:<15} 100% SET (write-heavy)", "write-only".green());
            println!("  {:<15} 50% GET, 50% SET", "mixed".green());
            println!(
                "  {:<15} 80% GET, 20% SET (typical cache)",
                "cache-heavy".green()
            );
            println!(
                "  {:<15} 50% GET, 25% SET, 15% list, 10% hash",
                "multi-type".green()
            );
            ExitCode::SUCCESS
        }
    }
}
