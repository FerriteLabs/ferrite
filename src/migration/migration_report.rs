//! Migration report generation
#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Phase & status types
// ---------------------------------------------------------------------------

/// Migration phase
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationPhase {
    Snapshot,
    Replication,
    Verification,
    Cutover,
    Complete,
    Failed,
}

impl MigrationPhase {
    fn label(&self) -> &'static str {
        match self {
            MigrationPhase::Snapshot => "Snapshot",
            MigrationPhase::Replication => "Replication",
            MigrationPhase::Verification => "Verification",
            MigrationPhase::Cutover => "Cutover",
            MigrationPhase::Complete => "Complete ✅",
            MigrationPhase::Failed => "Failed ❌",
        }
    }
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// Source Redis connection information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionInfo {
    pub host: String,
    pub port: u16,
    pub version: String,
    pub db_size: u64,
    pub memory_used: u64,
}

impl ConnectionInfo {
    fn url(&self) -> String {
        format!("redis://{}:{}", self.host, self.port)
    }
}

/// Migration progress counters.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MigrationProgress {
    pub keys_total: u64,
    pub keys_transferred: u64,
    pub bytes_transferred: u64,
    pub errors: u64,
    pub current_rate_keys_per_sec: f64,
}

impl MigrationProgress {
    fn pct(&self) -> f64 {
        if self.keys_total == 0 {
            return 0.0;
        }
        (self.keys_transferred as f64 / self.keys_total as f64) * 100.0
    }
}

/// Verification report after data comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationReport {
    pub sampled: u64,
    pub matched: u64,
    pub mismatched: u64,
    pub passed: bool,
}

/// Final migration report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationReport {
    pub phase: MigrationPhase,
    pub source: ConnectionInfo,
    pub progress: MigrationProgress,
    pub verification: Option<VerificationReport>,
    pub duration: Duration,
}

// ---------------------------------------------------------------------------
// Report generator
// ---------------------------------------------------------------------------

/// Builds a migration report incrementally.
pub struct MigrationReportGenerator {
    phase: MigrationPhase,
    started_at: Option<Instant>,
    source: Option<ConnectionInfo>,
    progress: MigrationProgress,
    verification: Option<VerificationReport>,
}

impl MigrationReportGenerator {
    /// Create a new report generator.
    pub fn new() -> Self {
        Self {
            phase: MigrationPhase::Snapshot,
            started_at: None,
            source: None,
            progress: MigrationProgress::default(),
            verification: None,
        }
    }

    /// Start tracking a migration from the given source.
    pub fn start_report(&mut self, source: ConnectionInfo) {
        self.source = Some(source);
        self.started_at = Some(Instant::now());
        self.phase = MigrationPhase::Snapshot;
    }

    /// Update the current progress counters.
    pub fn update_progress(&mut self, progress: MigrationProgress) {
        self.progress = progress;
    }

    /// Attach a verification result.
    pub fn set_verification(&mut self, result: VerificationReport) {
        self.phase = MigrationPhase::Verification;
        self.verification = Some(result);
    }

    /// Finalize the report, marking it as complete or failed.
    pub fn finalize(&mut self, success: bool) -> MigrationReport {
        self.phase = if success {
            MigrationPhase::Complete
        } else {
            MigrationPhase::Failed
        };

        let duration = self
            .started_at
            .map(|s| s.elapsed())
            .unwrap_or(Duration::ZERO);

        MigrationReport {
            phase: self.phase,
            source: self.source.clone().unwrap_or(ConnectionInfo {
                host: "unknown".into(),
                port: 0,
                version: "unknown".into(),
                db_size: 0,
                memory_used: 0,
            }),
            progress: self.progress.clone(),
            verification: self.verification.clone(),
            duration,
        }
    }

    /// Render a human-readable text report.
    pub fn render_text(&self) -> String {
        let source = self.source.as_ref();
        let url = source
            .map(|s| s.url())
            .unwrap_or_else(|| "unknown".to_string());
        let version = source
            .map(|s| s.version.as_str())
            .unwrap_or("unknown");
        let duration = self
            .started_at
            .map(|s| s.elapsed())
            .unwrap_or(Duration::ZERO);
        let dur_str = format_duration(duration);

        let pct = self.progress.pct();
        let bytes_str = format_bytes(self.progress.bytes_transferred);
        let rate = self.progress.current_rate_keys_per_sec;

        let mut out = String::new();
        out.push_str("╔══════════════════════════════════════════════════════╗\n");
        out.push_str("║         Ferrite Migration Report                     ║\n");
        out.push_str("╠══════════════════════════════════════════════════════╣\n");
        out.push_str(&format!(
            "║ Source: {} (Redis {}){}║\n",
            url,
            version,
            pad(52 - 12 - url.len() - version.len())
        ));
        out.push_str(&format!(
            "║ Phase: {}{}║\n",
            self.phase.label(),
            pad(52 - 9 - self.phase.label().len())
        ));
        out.push_str(&format!(
            "║ Duration: {}{}║\n",
            dur_str,
            pad(52 - 12 - dur_str.len())
        ));
        out.push_str("╠══════════════════════════════════════════════════════╣\n");
        out.push_str(&format!(
            "║ Keys Transferred:  {} / {} ({:.0}%){}║\n",
            format_num(self.progress.keys_transferred),
            format_num(self.progress.keys_total),
            pct,
            pad(8)
        ));
        out.push_str(&format!(
            "║ Data Transferred:  {}{}║\n",
            bytes_str,
            pad(52 - 21 - bytes_str.len())
        ));
        out.push_str(&format!(
            "║ Transfer Rate:     {} keys/sec{}║\n",
            format_num(rate as u64),
            pad(18)
        ));
        out.push_str(&format!(
            "║ Errors:            {}{}║\n",
            self.progress.errors,
            pad(52 - 21 - digit_count(self.progress.errors))
        ));

        if let Some(ref v) = self.verification {
            out.push_str("╠══════════════════════════════════════════════════════╣\n");
            let status = if v.passed { "✅ PASSED" } else { "❌ FAILED" };
            out.push_str(&format!(
                "║ Verification: {}{}║\n",
                status,
                pad(52 - 16 - status.len())
            ));
            out.push_str(&format!(
                "║   Sampled: {} keys{}║\n",
                format_num(v.sampled),
                pad(20)
            ));
            out.push_str(&format!(
                "║   Matched: {} ({:.0}%){}║\n",
                format_num(v.matched),
                if v.sampled > 0 {
                    v.matched as f64 / v.sampled as f64 * 100.0
                } else {
                    0.0
                },
                pad(20)
            ));
            out.push_str(&format!(
                "║   Mismatched: {}{}║\n",
                v.mismatched,
                pad(52 - 16 - digit_count(v.mismatched))
            ));
        }

        out.push_str("╚══════════════════════════════════════════════════════╝\n");
        out
    }

    /// Render the report as JSON.
    pub fn render_json(&self) -> serde_json::Value {
        let duration = self
            .started_at
            .map(|s| s.elapsed())
            .unwrap_or(Duration::ZERO);

        serde_json::json!({
            "phase": format!("{:?}", self.phase),
            "source": self.source.as_ref().map(|s| serde_json::json!({
                "url": s.url(),
                "version": s.version,
                "db_size": s.db_size,
                "memory_used": s.memory_used,
            })),
            "progress": {
                "keys_total": self.progress.keys_total,
                "keys_transferred": self.progress.keys_transferred,
                "bytes_transferred": self.progress.bytes_transferred,
                "errors": self.progress.errors,
                "rate_keys_per_sec": self.progress.current_rate_keys_per_sec,
            },
            "verification": self.verification.as_ref().map(|v| serde_json::json!({
                "sampled": v.sampled,
                "matched": v.matched,
                "mismatched": v.mismatched,
                "passed": v.passed,
            })),
            "duration_secs": duration.as_secs_f64(),
        })
    }
}

impl Default for MigrationReportGenerator {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

fn format_duration(d: Duration) -> String {
    let secs = d.as_secs();
    if secs >= 60 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}s", secs)
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

fn format_num(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
    }
    result.chars().rev().collect()
}

fn digit_count(n: u64) -> usize {
    if n == 0 {
        return 1;
    }
    (n as f64).log10() as usize + 1
}

fn pad(n: usize) -> String {
    " ".repeat(n.min(40))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn sample_source() -> ConnectionInfo {
        ConnectionInfo {
            host: "10.0.0.1".into(),
            port: 6379,
            version: "7.2.3".into(),
            db_size: 1_234_567,
            memory_used: 2_400_000_000,
        }
    }

    #[test]
    fn test_report_lifecycle() {
        let mut gen = MigrationReportGenerator::new();
        gen.start_report(sample_source());
        gen.update_progress(MigrationProgress {
            keys_total: 1_234_567,
            keys_transferred: 1_234_567,
            bytes_transferred: 2_400_000_000,
            errors: 0,
            current_rate_keys_per_sec: 4532.0,
        });
        gen.set_verification(VerificationReport {
            sampled: 10_000,
            matched: 10_000,
            mismatched: 0,
            passed: true,
        });
        let report = gen.finalize(true);
        assert_eq!(report.phase, MigrationPhase::Complete);
        assert_eq!(report.progress.keys_transferred, 1_234_567);
        assert!(report.verification.as_ref().is_some_and(|v| v.passed));
    }

    #[test]
    fn test_render_text_contains_key_info() {
        let mut gen = MigrationReportGenerator::new();
        gen.start_report(sample_source());
        gen.update_progress(MigrationProgress {
            keys_total: 100,
            keys_transferred: 50,
            bytes_transferred: 1024,
            errors: 0,
            current_rate_keys_per_sec: 100.0,
        });
        let text = gen.render_text();
        assert!(text.contains("Ferrite Migration Report"));
        assert!(text.contains("10.0.0.1"));
    }

    #[test]
    fn test_render_json() {
        let mut gen = MigrationReportGenerator::new();
        gen.start_report(sample_source());
        gen.update_progress(MigrationProgress {
            keys_total: 100,
            keys_transferred: 100,
            bytes_transferred: 5000,
            errors: 0,
            current_rate_keys_per_sec: 50.0,
        });
        let json = gen.render_json();
        assert_eq!(json["progress"]["keys_total"], 100);
        assert_eq!(json["progress"]["keys_transferred"], 100);
    }

    #[test]
    fn test_failed_report() {
        let mut gen = MigrationReportGenerator::new();
        gen.start_report(sample_source());
        let report = gen.finalize(false);
        assert_eq!(report.phase, MigrationPhase::Failed);
    }

    #[test]
    fn test_format_helpers() {
        assert_eq!(format_bytes(1_500_000_000), "1.4 GB");
        assert_eq!(format_bytes(5_242_880), "5.0 MB");
        assert_eq!(format_num(1_234_567), "1,234,567");
        assert_eq!(format_duration(Duration::from_secs(272)), "4m 32s");
    }
}
