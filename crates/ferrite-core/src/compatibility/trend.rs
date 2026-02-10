//! Compatibility Score Trend Tracking
//!
//! Persists suite results over time so CI pipelines can track compatibility
//! score regressions and generate trend charts.

use super::suite::SuiteResults;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// A single recorded data point from a suite run.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrendEntry {
    /// ISO-8601 timestamp of the run.
    pub timestamp: String,
    /// Git commit SHA (if available).
    pub commit_sha: Option<String>,
    /// Git branch name (if available).
    pub branch: Option<String>,
    /// Overall compatibility score (0-100).
    pub score: f64,
    /// Critical command score (0-100).
    pub critical_score: f64,
    /// Total tests executed.
    pub total: usize,
    /// Tests that passed.
    pub passed: usize,
    /// Tests that failed.
    pub failed: usize,
    /// Per-category scores.
    pub categories: HashMap<String, f64>,
}

/// Persistent store for compatibility trend data.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrendHistory {
    /// All recorded entries, newest last.
    pub entries: Vec<TrendEntry>,
    /// Maximum entries to retain.
    #[serde(default = "default_max_entries")]
    pub max_entries: usize,
}

fn default_max_entries() -> usize {
    500
}

impl Default for TrendHistory {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            max_entries: default_max_entries(),
        }
    }
}

impl TrendHistory {
    /// Create a new empty history.
    pub fn new() -> Self {
        Self::default()
    }

    /// Load history from a JSON file, returning an empty history if the file
    /// does not exist or is malformed.
    pub fn load(path: &Path) -> Self {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }

    /// Persist the history to a JSON file.
    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(std::io::Error::other)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, json)
    }

    /// Record a new suite run.
    pub fn record(
        &mut self,
        results: &SuiteResults,
        commit_sha: Option<String>,
        branch: Option<String>,
    ) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        let timestamp = chrono::DateTime::from_timestamp(now.as_secs() as i64, 0)
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_else(|| "unknown".to_string());

        let categories = results
            .categories
            .iter()
            .map(|(k, v)| (k.clone(), v.score))
            .collect();

        self.entries.push(TrendEntry {
            timestamp,
            commit_sha,
            branch,
            score: results.score,
            critical_score: results.critical_score,
            total: results.total,
            passed: results.passed,
            failed: results.failed,
            categories,
        });

        // Evict oldest entries if over capacity.
        while self.entries.len() > self.max_entries {
            self.entries.remove(0);
        }
    }

    /// Compute the score delta between the last two runs.
    /// Returns `None` if fewer than two entries exist.
    pub fn score_delta(&self) -> Option<f64> {
        if self.entries.len() < 2 {
            return None;
        }
        let prev = &self.entries[self.entries.len() - 2];
        let curr = &self.entries[self.entries.len() - 1];
        Some(curr.score - prev.score)
    }

    /// Return `true` if the latest run regressed compared to the previous run
    /// by more than the given tolerance (in percentage points).
    pub fn has_regression(&self, tolerance: f64) -> bool {
        self.score_delta().map(|d| d < -tolerance).unwrap_or(false)
    }

    /// Return the most recent entry, if any.
    pub fn latest(&self) -> Option<&TrendEntry> {
        self.entries.last()
    }

    /// Number of recorded runs.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the history is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Convenience runner that executes the suite, records trends, and writes
/// all output artifacts to a directory.
pub struct CiRunner {
    output_dir: PathBuf,
    history_file: PathBuf,
}

impl CiRunner {
    /// Create a runner that writes to the given output directory.
    pub fn new(output_dir: PathBuf) -> Self {
        let history_file = output_dir.join("trend.json");
        Self {
            output_dir,
            history_file,
        }
    }

    /// Execute the suite, persist results, and generate dashboard artifacts.
    ///
    /// Returns the `SuiteResults` and whether a regression was detected.
    pub fn run(
        &self,
        store: &crate::storage::Store,
        commit_sha: Option<String>,
        branch: Option<String>,
        regression_tolerance: f64,
    ) -> std::io::Result<(SuiteResults, bool)> {
        use super::dashboard::DashboardGenerator;
        use super::suite::CompatibilitySuite;

        let suite = CompatibilitySuite::new();
        let results = suite.run(store);

        // Update trend history.
        let mut history = TrendHistory::load(&self.history_file);
        history.record(&results, commit_sha, branch);
        let regressed = history.has_regression(regression_tolerance);
        history.save(&self.history_file)?;

        // Write dashboard artifacts.
        std::fs::create_dir_all(&self.output_dir)?;
        std::fs::write(
            self.output_dir.join("dashboard.html"),
            DashboardGenerator::generate_html(&results),
        )?;
        std::fs::write(
            self.output_dir.join("results.json"),
            DashboardGenerator::generate_json(&results),
        )?;
        std::fs::write(
            self.output_dir.join("summary.md"),
            DashboardGenerator::generate_markdown(&results),
        )?;

        Ok((results, regressed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_results(score: f64, passed: usize, failed: usize) -> SuiteResults {
        SuiteResults {
            total: passed + failed,
            passed,
            failed,
            skipped: 0,
            score,
            critical_score: score,
            duration: std::time::Duration::from_millis(100),
            categories: HashMap::new(),
            results: Vec::new(),
        }
    }

    #[test]
    fn trend_record_and_delta() {
        let mut history = TrendHistory::new();
        assert!(history.score_delta().is_none());

        history.record(&dummy_results(80.0, 80, 20), None, None);
        assert!(history.score_delta().is_none());

        history.record(&dummy_results(85.0, 85, 15), None, None);
        assert_eq!(history.score_delta(), Some(5.0));
        assert!(!history.has_regression(1.0));
    }

    #[test]
    fn trend_detects_regression() {
        let mut history = TrendHistory::new();
        history.record(&dummy_results(90.0, 90, 10), None, None);
        history.record(&dummy_results(85.0, 85, 15), None, None);
        assert!(history.has_regression(1.0));
        assert!(!history.has_regression(10.0));
    }

    #[test]
    fn trend_evicts_old_entries() {
        let mut history = TrendHistory::new();
        history.max_entries = 3;
        for i in 0..5 {
            history.record(&dummy_results(80.0 + i as f64, 80 + i, 20 - i), None, None);
        }
        assert_eq!(history.len(), 3);
    }
}
