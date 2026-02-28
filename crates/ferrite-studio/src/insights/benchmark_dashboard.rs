//! Benchmark result ingestion, storage, comparison, and dashboard rendering.
//!
//! Provides a [`BenchmarkDashboard`] that collects [`BenchmarkRun`] results from
//! multiple systems (Ferrite, Redis, Dragonfly, Valkey, KeyDB, …), stores
//! comparison baselines, detects regressions, and produces [`DashboardData`]
//! ready for a web UI.

use std::collections::HashMap;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Core dashboard
// ---------------------------------------------------------------------------

/// Central hub for benchmark results, baselines, and comparisons.
pub struct BenchmarkDashboard {
    /// All ingested benchmark runs.
    results: RwLock<Vec<BenchmarkRun>>,
    /// Stored baselines keyed by system name.
    baselines: RwLock<HashMap<String, BenchmarkRun>>,
}

impl BenchmarkDashboard {
    /// Creates an empty dashboard.
    pub fn new() -> Self {
        Self {
            results: RwLock::new(Vec::new()),
            baselines: RwLock::new(HashMap::new()),
        }
    }

    /// Ingest a new benchmark run.
    pub fn ingest_result(&self, run: BenchmarkRun) {
        self.results.write().push(run);
    }

    /// Set (or replace) the comparison baseline for a given system.
    pub fn set_baseline(&self, system: &str, run: BenchmarkRun) {
        self.baselines.write().insert(system.to_string(), run);
    }

    /// Compare the latest runs of two systems across all shared scenarios.
    ///
    /// Returns `None` if either system has no ingested results.
    pub fn compare(&self, system_a: &str, system_b: &str) -> Option<ComparisonReport> {
        let results = self.results.read();

        let run_a = results.iter().rev().find(|r| r.system == system_a)?;
        let run_b = results.iter().rev().find(|r| r.system == system_b)?;

        let scenarios_a: HashMap<&str, &ScenarioResult> =
            run_a.scenarios.iter().map(|s| (s.name.as_str(), s)).collect();

        let mut scenario_comparisons = Vec::new();
        let mut a_wins = 0u32;
        let mut b_wins = 0u32;

        for sb in &run_b.scenarios {
            if let Some(sa) = scenarios_a.get(sb.name.as_str()) {
                let throughput_ratio = if sb.throughput_ops_sec > 0.0 {
                    sa.throughput_ops_sec / sb.throughput_ops_sec
                } else {
                    f64::INFINITY
                };

                let latency_p99_ratio = if sa.latency_p99_us > 0.0 {
                    sa.latency_p99_us / sb.latency_p99_us
                } else {
                    0.0
                };

                let memory_ratio = if sb.memory_rss_bytes > 0 {
                    sa.memory_rss_bytes as f64 / sb.memory_rss_bytes as f64
                } else {
                    0.0
                };

                // Winner: higher throughput and lower latency preferred
                let winner = if throughput_ratio > 1.0 && latency_p99_ratio <= 1.0 {
                    a_wins += 1;
                    system_a.to_string()
                } else if throughput_ratio < 1.0 && latency_p99_ratio >= 1.0 {
                    b_wins += 1;
                    system_b.to_string()
                } else if throughput_ratio >= 1.0 {
                    a_wins += 1;
                    system_a.to_string()
                } else {
                    b_wins += 1;
                    system_b.to_string()
                };

                scenario_comparisons.push(ScenarioComparison {
                    scenario: sb.name.clone(),
                    throughput_ratio,
                    latency_p99_ratio,
                    memory_ratio,
                    winner,
                });
            }
        }

        let overall_winner = if a_wins >= b_wins {
            system_a.to_string()
        } else {
            system_b.to_string()
        };

        let summary = format!(
            "{} wins {a_wins}/{} scenarios vs {}",
            overall_winner,
            scenario_comparisons.len(),
            if overall_winner == system_a {
                system_b
            } else {
                system_a
            }
        );

        Some(ComparisonReport {
            system_a: system_a.to_string(),
            system_b: system_b.to_string(),
            scenarios: scenario_comparisons,
            overall_winner,
            summary,
        })
    }

    /// Returns the latest run for each distinct system.
    pub fn latest_results(&self) -> Vec<BenchmarkRun> {
        let results = self.results.read();
        let mut latest: HashMap<&str, &BenchmarkRun> = HashMap::new();
        for run in results.iter() {
            latest
                .entry(run.system.as_str())
                .and_modify(|existing| {
                    if run.timestamp > existing.timestamp {
                        *existing = run;
                    }
                })
                .or_insert(run);
        }
        latest.into_values().cloned().collect()
    }

    /// Check for performance regressions against the stored baseline.
    ///
    /// Returns alerts for every scenario metric that degraded more than
    /// `threshold_pct` percent compared to the baseline.
    pub fn regression_check(&self, system: &str, threshold_pct: f64) -> Vec<RegressionAlert> {
        let baselines = self.baselines.read();
        let Some(baseline) = baselines.get(system) else {
            return Vec::new();
        };

        let results = self.results.read();
        let Some(current) = results.iter().rev().find(|r| r.system == system) else {
            return Vec::new();
        };

        let baseline_map: HashMap<&str, &ScenarioResult> =
            baseline.scenarios.iter().map(|s| (s.name.as_str(), s)).collect();

        let mut alerts = Vec::new();

        for scenario in &current.scenarios {
            if let Some(base) = baseline_map.get(scenario.name.as_str()) {
                // Throughput regression: current < baseline is bad
                if base.throughput_ops_sec > 0.0 {
                    let change_pct =
                        (scenario.throughput_ops_sec - base.throughput_ops_sec) / base.throughput_ops_sec * 100.0;
                    if change_pct < -threshold_pct {
                        alerts.push(RegressionAlert {
                            scenario: scenario.name.clone(),
                            metric: "throughput_ops_sec".to_string(),
                            baseline_value: base.throughput_ops_sec,
                            current_value: scenario.throughput_ops_sec,
                            change_pct,
                            severity: severity_from_change(change_pct),
                        });
                    }
                }

                // Latency p99 regression: current > baseline is bad
                if base.latency_p99_us > 0.0 {
                    let change_pct =
                        (scenario.latency_p99_us - base.latency_p99_us) / base.latency_p99_us * 100.0;
                    if change_pct > threshold_pct {
                        alerts.push(RegressionAlert {
                            scenario: scenario.name.clone(),
                            metric: "latency_p99_us".to_string(),
                            baseline_value: base.latency_p99_us,
                            current_value: scenario.latency_p99_us,
                            change_pct,
                            severity: severity_from_change(change_pct),
                        });
                    }
                }
            }
        }

        alerts
    }

    /// Produce a full [`DashboardData`] snapshot for the web UI.
    pub fn dashboard_data(&self) -> DashboardData {
        let latest_runs = self.latest_results();
        let systems: Vec<String> = {
            let mut s: Vec<String> = latest_runs.iter().map(|r| r.system.clone()).collect();
            s.sort();
            s.dedup();
            s
        };

        // Build pair-wise comparisons for all systems
        let mut comparisons = Vec::new();
        for i in 0..systems.len() {
            for j in (i + 1)..systems.len() {
                if let Some(report) = self.compare(&systems[i], &systems[j]) {
                    comparisons.push(report);
                }
            }
        }

        // Collect regressions (5 % default threshold)
        let mut regressions = Vec::new();
        for sys in &systems {
            regressions.extend(self.regression_check(sys, 5.0));
        }

        let last_updated = latest_runs.iter().map(|r| r.timestamp).max().unwrap_or(0);

        DashboardData {
            systems,
            latest_runs,
            comparisons,
            regressions,
            last_updated,
        }
    }

    /// Summary of a single scenario across all ingested systems.
    pub fn scenario_summary(&self, scenario: &str) -> Option<ScenarioSummary> {
        let results = self.latest_results();

        let mut results_by_system: Vec<(String, ScenarioResult)> = Vec::new();

        for run in &results {
            if let Some(sr) = run.scenarios.iter().find(|s| s.name == scenario) {
                results_by_system.push((run.system.clone(), sr.clone()));
            }
        }

        if results_by_system.is_empty() {
            return None;
        }

        let best_throughput = results_by_system
            .iter()
            .max_by(|a, b| {
                a.1.throughput_ops_sec
                    .partial_cmp(&b.1.throughput_ops_sec)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(sys, sr)| (sys.clone(), sr.throughput_ops_sec))
            .expect("results_by_system is non-empty");

        let best_latency = results_by_system
            .iter()
            .min_by(|a, b| {
                a.1.latency_p99_us
                    .partial_cmp(&b.1.latency_p99_us)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(sys, sr)| (sys.clone(), sr.latency_p99_us))
            .expect("results_by_system is non-empty");

        Some(ScenarioSummary {
            scenario: scenario.to_string(),
            results_by_system,
            best_throughput,
            best_latency,
        })
    }
}

impl Default for BenchmarkDashboard {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

/// A single benchmark run for one system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkRun {
    /// Unique identifier for this run.
    pub id: String,
    /// System under test (e.g. "ferrite", "redis", "dragonfly", "valkey", "keydb").
    pub system: String,
    /// Version string of the system.
    pub version: String,
    /// Unix epoch timestamp when the run was executed.
    pub timestamp: u64,
    /// Hardware the benchmark was executed on.
    pub hardware: HardwareSpec,
    /// Individual scenario results within this run.
    pub scenarios: Vec<ScenarioResult>,
}

/// Hardware specification for the benchmark host.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HardwareSpec {
    /// Cloud instance type (e.g. "c5.2xlarge").
    pub instance_type: String,
    /// Number of CPU cores.
    pub cpu_cores: u32,
    /// Memory in gigabytes.
    pub memory_gb: u32,
    /// Storage type (e.g. "nvme", "ebs-gp3").
    pub storage_type: String,
}

/// Result of a single benchmark scenario.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioResult {
    /// Scenario name (e.g. "get_only", "set_only", "mixed_50_50").
    pub name: String,
    /// Configuration used for this scenario.
    pub config: ScenarioConfig,
    /// Sustained throughput in operations per second.
    pub throughput_ops_sec: f64,
    /// Median latency in microseconds.
    pub latency_p50_us: f64,
    /// 95th-percentile latency in microseconds.
    pub latency_p95_us: f64,
    /// 99th-percentile latency in microseconds.
    pub latency_p99_us: f64,
    /// 99.9th-percentile latency in microseconds.
    pub latency_p999_us: f64,
    /// Resident set size in bytes.
    pub memory_rss_bytes: u64,
    /// Average CPU utilization during the run (0–100).
    pub cpu_utilization_pct: f64,
    /// Total error count.
    pub errors: u64,
    /// Wall-clock duration of the scenario in seconds.
    pub duration_secs: f64,
}

/// Configuration parameters for a benchmark scenario.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioConfig {
    /// Number of concurrent client connections.
    pub clients: u32,
    /// Number of benchmark driver threads.
    pub threads: u32,
    /// Pipeline depth (commands per batch).
    pub pipeline: u32,
    /// Read ratio (e.g. 50 for 50/50 mixed).
    pub ratio_read: u32,
    /// Write ratio.
    pub ratio_write: u32,
    /// Value size in bytes.
    pub data_size: u32,
    /// Total number of distinct keys.
    pub key_count: u64,
}

/// Side-by-side comparison of two systems.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonReport {
    /// First system in the comparison.
    pub system_a: String,
    /// Second system in the comparison.
    pub system_b: String,
    /// Per-scenario comparison details.
    pub scenarios: Vec<ScenarioComparison>,
    /// System that won the majority of scenarios.
    pub overall_winner: String,
    /// Human-readable summary.
    pub summary: String,
}

/// Per-scenario metrics comparison between two systems.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioComparison {
    /// Scenario name.
    pub scenario: String,
    /// Throughput ratio (a / b). > 1 means system A is faster.
    pub throughput_ratio: f64,
    /// Latency p99 ratio (a / b). < 1 means system A has lower latency.
    pub latency_p99_ratio: f64,
    /// Memory ratio (a / b). < 1 means system A uses less memory.
    pub memory_ratio: f64,
    /// Winner of this scenario.
    pub winner: String,
}

/// Alert raised when a metric regresses beyond a threshold.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegressionAlert {
    /// Scenario in which the regression was detected.
    pub scenario: String,
    /// Metric name (e.g. "throughput_ops_sec", "latency_p99_us").
    pub metric: String,
    /// Baseline value for the metric.
    pub baseline_value: f64,
    /// Current value for the metric.
    pub current_value: f64,
    /// Percentage change from baseline (negative = regression for throughput).
    pub change_pct: f64,
    /// Severity classification.
    pub severity: AlertSeverity,
}

/// Severity level for regression alerts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertSeverity {
    /// Informational — small but notable change.
    Info,
    /// Warning — significant degradation.
    Warning,
    /// Critical — severe regression requiring immediate attention.
    Critical,
}

/// Full dashboard payload for the web UI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardData {
    /// All known system names.
    pub systems: Vec<String>,
    /// Latest run per system.
    pub latest_runs: Vec<BenchmarkRun>,
    /// Pair-wise comparison reports.
    pub comparisons: Vec<ComparisonReport>,
    /// Active regression alerts.
    pub regressions: Vec<RegressionAlert>,
    /// Unix timestamp of the most recent run.
    pub last_updated: u64,
}

/// Aggregated view of one scenario across all systems.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioSummary {
    /// Scenario name.
    pub scenario: String,
    /// Results keyed by system name.
    pub results_by_system: Vec<(String, ScenarioResult)>,
    /// System with the highest throughput and its value.
    pub best_throughput: (String, f64),
    /// System with the lowest p99 latency and its value.
    pub best_latency: (String, f64),
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn severity_from_change(change_pct: f64) -> AlertSeverity {
    let abs = change_pct.abs();
    if abs >= 20.0 {
        AlertSeverity::Critical
    } else if abs >= 10.0 {
        AlertSeverity::Warning
    } else {
        AlertSeverity::Info
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_hardware() -> HardwareSpec {
        HardwareSpec {
            instance_type: "c5.2xlarge".to_string(),
            cpu_cores: 8,
            memory_gb: 16,
            storage_type: "nvme".to_string(),
        }
    }

    fn make_scenario(name: &str, throughput: f64, p99: f64, memory: u64) -> ScenarioResult {
        ScenarioResult {
            name: name.to_string(),
            config: ScenarioConfig {
                clients: 50,
                threads: 4,
                pipeline: 1,
                ratio_read: 50,
                ratio_write: 50,
                data_size: 64,
                key_count: 100_000,
            },
            throughput_ops_sec: throughput,
            latency_p50_us: p99 * 0.5,
            latency_p95_us: p99 * 0.8,
            latency_p99_us: p99,
            latency_p999_us: p99 * 1.5,
            memory_rss_bytes: memory,
            cpu_utilization_pct: 60.0,
            errors: 0,
            duration_secs: 60.0,
        }
    }

    fn make_run(id: &str, system: &str, ts: u64, scenarios: Vec<ScenarioResult>) -> BenchmarkRun {
        BenchmarkRun {
            id: id.to_string(),
            system: system.to_string(),
            version: "1.0.0".to_string(),
            timestamp: ts,
            hardware: make_hardware(),
            scenarios,
        }
    }

    // ---- 1. Ingest and retrieve results ----

    #[test]
    fn test_ingest_and_retrieve() {
        let dash = BenchmarkDashboard::new();
        let run = make_run(
            "r1",
            "ferrite",
            1000,
            vec![make_scenario("get_only", 500_000.0, 100.0, 1_000_000)],
        );
        dash.ingest_result(run);

        let latest = dash.latest_results();
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].system, "ferrite");
    }

    // ---- 2. Set baseline and compare ----

    #[test]
    fn test_set_baseline_and_compare() {
        let dash = BenchmarkDashboard::new();

        let ferrite = make_run(
            "r1",
            "ferrite",
            1000,
            vec![make_scenario("get_only", 600_000.0, 80.0, 900_000)],
        );
        let redis = make_run(
            "r2",
            "redis",
            1000,
            vec![make_scenario("get_only", 400_000.0, 120.0, 1_200_000)],
        );

        dash.set_baseline("ferrite", ferrite.clone());
        dash.ingest_result(ferrite);
        dash.ingest_result(redis);

        let report = dash.compare("ferrite", "redis").unwrap();
        assert_eq!(report.scenarios.len(), 1);
        assert!(report.scenarios[0].throughput_ratio > 1.0); // ferrite faster
    }

    // ---- 3. Comparison report winner determination ----

    #[test]
    fn test_comparison_winner() {
        let dash = BenchmarkDashboard::new();

        // ferrite wins 2 scenarios, redis wins 1
        let ferrite = make_run(
            "r1",
            "ferrite",
            1000,
            vec![
                make_scenario("get_only", 600_000.0, 80.0, 900_000),
                make_scenario("set_only", 550_000.0, 90.0, 950_000),
                make_scenario("mixed_50_50", 300_000.0, 200.0, 1_100_000),
            ],
        );
        let redis = make_run(
            "r2",
            "redis",
            1000,
            vec![
                make_scenario("get_only", 400_000.0, 120.0, 1_200_000),
                make_scenario("set_only", 380_000.0, 130.0, 1_100_000),
                make_scenario("mixed_50_50", 350_000.0, 150.0, 1_000_000),
            ],
        );

        dash.ingest_result(ferrite);
        dash.ingest_result(redis);

        let report = dash.compare("ferrite", "redis").unwrap();
        assert_eq!(report.overall_winner, "ferrite");
    }

    // ---- 4. Regression detection ----

    #[test]
    fn test_regression_detection() {
        let dash = BenchmarkDashboard::new();

        let baseline = make_run(
            "base",
            "ferrite",
            1000,
            vec![make_scenario("get_only", 500_000.0, 100.0, 1_000_000)],
        );
        dash.set_baseline("ferrite", baseline);

        // Current run: throughput dropped 15 %, latency increased 20 %
        let current = make_run(
            "cur",
            "ferrite",
            2000,
            vec![make_scenario("get_only", 425_000.0, 120.0, 1_000_000)],
        );
        dash.ingest_result(current);

        let alerts = dash.regression_check("ferrite", 5.0);
        assert!(!alerts.is_empty());
        assert!(alerts.iter().any(|a| a.metric == "throughput_ops_sec"));
        assert!(alerts.iter().any(|a| a.metric == "latency_p99_us"));
    }

    // ---- 5. Dashboard data aggregation ----

    #[test]
    fn test_dashboard_data_aggregation() {
        let dash = BenchmarkDashboard::new();

        dash.ingest_result(make_run(
            "r1",
            "ferrite",
            1000,
            vec![make_scenario("get_only", 600_000.0, 80.0, 900_000)],
        ));
        dash.ingest_result(make_run(
            "r2",
            "redis",
            1000,
            vec![make_scenario("get_only", 400_000.0, 120.0, 1_200_000)],
        ));

        let data = dash.dashboard_data();
        assert_eq!(data.systems.len(), 2);
        assert_eq!(data.latest_runs.len(), 2);
        assert_eq!(data.comparisons.len(), 1); // one pair
        assert!(data.last_updated > 0);
    }

    // ---- 6. Scenario summary ----

    #[test]
    fn test_scenario_summary() {
        let dash = BenchmarkDashboard::new();

        dash.ingest_result(make_run(
            "r1",
            "ferrite",
            1000,
            vec![make_scenario("get_only", 600_000.0, 80.0, 900_000)],
        ));
        dash.ingest_result(make_run(
            "r2",
            "redis",
            1000,
            vec![make_scenario("get_only", 400_000.0, 120.0, 1_200_000)],
        ));

        let summary = dash.scenario_summary("get_only").unwrap();
        assert_eq!(summary.results_by_system.len(), 2);
        assert_eq!(summary.best_throughput.0, "ferrite");
        assert_eq!(summary.best_latency.0, "ferrite");
    }

    // ---- 7. Multiple systems comparison ----

    #[test]
    fn test_multiple_systems() {
        let dash = BenchmarkDashboard::new();

        for (sys, tp, lat) in [
            ("ferrite", 600_000.0, 80.0),
            ("redis", 400_000.0, 120.0),
            ("dragonfly", 550_000.0, 90.0),
        ] {
            dash.ingest_result(make_run(
                &format!("r-{sys}"),
                sys,
                1000,
                vec![make_scenario("get_only", tp, lat, 1_000_000)],
            ));
        }

        let data = dash.dashboard_data();
        assert_eq!(data.systems.len(), 3);
        // 3 systems → 3 pair-wise comparisons
        assert_eq!(data.comparisons.len(), 3);
    }

    // ---- 8. Empty dashboard returns valid data ----

    #[test]
    fn test_empty_dashboard() {
        let dash = BenchmarkDashboard::new();
        let data = dash.dashboard_data();

        assert!(data.systems.is_empty());
        assert!(data.latest_runs.is_empty());
        assert!(data.comparisons.is_empty());
        assert!(data.regressions.is_empty());
        assert_eq!(data.last_updated, 0);
    }

    // ---- Additional coverage ----

    #[test]
    fn test_compare_nonexistent_system_returns_none() {
        let dash = BenchmarkDashboard::new();
        assert!(dash.compare("foo", "bar").is_none());
    }

    #[test]
    fn test_scenario_summary_missing_scenario() {
        let dash = BenchmarkDashboard::new();
        assert!(dash.scenario_summary("nonexistent").is_none());
    }

    #[test]
    fn test_regression_no_baseline() {
        let dash = BenchmarkDashboard::new();
        let alerts = dash.regression_check("ferrite", 5.0);
        assert!(alerts.is_empty());
    }

    #[test]
    fn test_severity_levels() {
        assert_eq!(severity_from_change(-6.0), AlertSeverity::Info);
        assert_eq!(severity_from_change(-12.0), AlertSeverity::Warning);
        assert_eq!(severity_from_change(-25.0), AlertSeverity::Critical);
    }

    #[test]
    fn test_latest_results_picks_newest() {
        let dash = BenchmarkDashboard::new();

        dash.ingest_result(make_run(
            "r1",
            "ferrite",
            1000,
            vec![make_scenario("get_only", 500_000.0, 100.0, 1_000_000)],
        ));
        dash.ingest_result(make_run(
            "r2",
            "ferrite",
            2000,
            vec![make_scenario("get_only", 600_000.0, 80.0, 900_000)],
        ));

        let latest = dash.latest_results();
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].id, "r2");
    }

    #[test]
    fn test_serde_roundtrip() {
        let run = make_run(
            "r1",
            "ferrite",
            1000,
            vec![make_scenario("get_only", 500_000.0, 100.0, 1_000_000)],
        );
        let json = serde_json::to_string(&run).unwrap();
        let deserialized: BenchmarkRun = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, "r1");
        assert_eq!(deserialized.scenarios.len(), 1);
    }

    #[test]
    fn test_default_impl() {
        let dash = BenchmarkDashboard::default();
        assert!(dash.latest_results().is_empty());
    }
}
