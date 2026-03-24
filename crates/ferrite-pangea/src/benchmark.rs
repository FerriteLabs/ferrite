//! Benchmark scaffold for CXL tier performance characterization.
//!
//! Provides tools to measure and compare latency/throughput across
//! different memory tiers (DRAM vs CXL).

use std::time::{Duration, Instant};

/// Results of a tier performance benchmark.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TierBenchmark {
    /// Name of the tier being benchmarked.
    pub tier_name: String,
    /// Number of operations executed.
    pub operations: usize,
    /// Total wall-clock duration.
    pub total_duration: Duration,
    /// Median latency in nanoseconds.
    pub p50_ns: u64,
    /// 99th-percentile latency in nanoseconds.
    pub p99_ns: u64,
    /// Throughput in operations per second.
    pub throughput_ops_sec: f64,
}

/// Comparison between a baseline and candidate tier.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TierComparison {
    /// Name of the baseline tier.
    pub baseline_tier: String,
    /// Name of the candidate tier.
    pub candidate_tier: String,
    /// Ratio of candidate p50 to baseline p50 (>1.0 = slower).
    pub p50_ratio: f64,
    /// Ratio of candidate p99 to baseline p99 (>1.0 = slower).
    pub p99_ratio: f64,
    /// Ratio of candidate throughput to baseline (<1.0 = slower).
    pub throughput_ratio: f64,
    /// Percentage regression at p99 (positive = slower).
    pub regression_pct: f64,
}

/// Run a read benchmark against an allocator.
///
/// Calls `read_fn` for each key in a cyclic pattern for `iterations` total
/// operations, collecting per-operation latency.
pub fn benchmark_reads<F: Fn(&str) -> Option<Vec<u8>>>(
    tier_name: &str,
    keys: &[String],
    read_fn: F,
    iterations: usize,
) -> TierBenchmark {
    assert!(!keys.is_empty(), "keys must not be empty");
    assert!(iterations > 0, "iterations must be > 0");

    let mut latencies = Vec::with_capacity(iterations);
    for key in keys.iter().cycle().take(iterations) {
        let start = Instant::now();
        let _ = read_fn(key);
        latencies.push(start.elapsed().as_nanos() as u64);
    }
    latencies.sort_unstable();

    let total: u64 = latencies.iter().sum();
    let p50_idx = latencies.len() / 2;
    let p99_idx = latencies.len() * 99 / 100;

    TierBenchmark {
        tier_name: tier_name.to_string(),
        operations: iterations,
        total_duration: Duration::from_nanos(total),
        p50_ns: latencies[p50_idx],
        p99_ns: latencies[p99_idx],
        throughput_ops_sec: iterations as f64 / (total as f64 / 1e9),
    }
}

/// Compare two tiers and report the performance ratio.
pub fn compare(baseline: &TierBenchmark, candidate: &TierBenchmark) -> TierComparison {
    TierComparison {
        baseline_tier: baseline.tier_name.clone(),
        candidate_tier: candidate.tier_name.clone(),
        p50_ratio: candidate.p50_ns as f64 / baseline.p50_ns.max(1) as f64,
        p99_ratio: candidate.p99_ns as f64 / baseline.p99_ns.max(1) as f64,
        throughput_ratio: candidate.throughput_ops_sec / baseline.throughput_ops_sec.max(1.0),
        regression_pct: ((candidate.p99_ns as f64 / baseline.p99_ns.max(1) as f64) - 1.0) * 100.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn benchmark_reads_returns_reasonable_p50_p99() {
        let mut store = HashMap::new();
        for i in 0..10 {
            store.insert(format!("key:{i}"), format!("value:{i}").into_bytes());
        }
        let keys: Vec<String> = store.keys().cloned().collect();

        let result = benchmark_reads("test-dram", &keys, |k| store.get(k).cloned(), 1000);

        assert_eq!(result.tier_name, "test-dram");
        assert_eq!(result.operations, 1000);
        assert!(result.p50_ns > 0, "p50 should be positive");
        assert!(result.p99_ns >= result.p50_ns, "p99 should be >= p50");
        assert!(
            result.throughput_ops_sec > 0.0,
            "throughput should be positive"
        );
        assert!(
            !result.total_duration.is_zero(),
            "total_duration should be non-zero"
        );
    }

    #[test]
    fn compare_computes_correct_ratios() {
        let baseline = TierBenchmark {
            tier_name: "DRAM".to_string(),
            operations: 1000,
            total_duration: Duration::from_micros(100),
            p50_ns: 100,
            p99_ns: 200,
            throughput_ops_sec: 10_000_000.0,
        };
        let candidate = TierBenchmark {
            tier_name: "CXL".to_string(),
            operations: 1000,
            total_duration: Duration::from_micros(300),
            p50_ns: 300,
            p99_ns: 600,
            throughput_ops_sec: 3_333_333.0,
        };

        let cmp = compare(&baseline, &candidate);

        assert_eq!(cmp.baseline_tier, "DRAM");
        assert_eq!(cmp.candidate_tier, "CXL");
        assert!(
            (cmp.p50_ratio - 3.0).abs() < f64::EPSILON,
            "p50 ratio should be 3.0"
        );
        assert!(
            (cmp.p99_ratio - 3.0).abs() < f64::EPSILON,
            "p99 ratio should be 3.0"
        );
        assert!(
            (cmp.regression_pct - 200.0).abs() < 0.1,
            "regression should be ~200%"
        );
        assert!(
            cmp.throughput_ratio < 1.0,
            "candidate should have lower throughput ratio"
        );
    }

    #[test]
    fn throughput_calculation_is_sensible() {
        let store: HashMap<String, Vec<u8>> =
            (0..5).map(|i| (format!("k{i}"), vec![i; 64])).collect();
        let keys: Vec<String> = store.keys().cloned().collect();

        let result = benchmark_reads("sanity", &keys, |k| store.get(k).cloned(), 500);

        // Throughput should be at least 100K ops/sec for in-memory HashMap lookups
        assert!(
            result.throughput_ops_sec > 100_000.0,
            "throughput {} ops/sec is suspiciously low for in-memory reads",
            result.throughput_ops_sec
        );
    }

    #[test]
    fn compare_with_identical_tiers_has_ratio_one() {
        let tier = TierBenchmark {
            tier_name: "same".to_string(),
            operations: 100,
            total_duration: Duration::from_micros(50),
            p50_ns: 500,
            p99_ns: 1000,
            throughput_ops_sec: 2_000_000.0,
        };

        let cmp = compare(&tier, &tier);
        assert!((cmp.p50_ratio - 1.0).abs() < f64::EPSILON);
        assert!((cmp.p99_ratio - 1.0).abs() < f64::EPSILON);
        assert!((cmp.throughput_ratio - 1.0).abs() < f64::EPSILON);
        assert!(cmp.regression_pct.abs() < 0.01);
    }

    #[test]
    fn serialization_round_trip() {
        let bench = TierBenchmark {
            tier_name: "test".to_string(),
            operations: 42,
            total_duration: Duration::from_nanos(12345),
            p50_ns: 100,
            p99_ns: 500,
            throughput_ops_sec: 999.9,
        };
        let json = serde_json::to_string(&bench).expect("serialize");
        let back: TierBenchmark = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.tier_name, bench.tier_name);
        assert_eq!(back.operations, bench.operations);
        assert_eq!(back.p50_ns, bench.p50_ns);
        assert_eq!(back.p99_ns, bench.p99_ns);
    }
}
