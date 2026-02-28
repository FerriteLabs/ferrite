//! Jepsen test report generation
//!
//! Produces human-readable ASCII reports and summary tables from
//! [`TestResult`] data collected during Jepsen-style test runs.

#![allow(dead_code)]

use std::time::Duration;

use super::fault_injection::FaultEvent;
use super::linearizability::Violation;

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

/// A point-in-time event recorded on the test timeline.
#[derive(Debug, Clone)]
pub struct TimelineEvent {
    /// Offset from the start of the test.
    pub timestamp: Duration,
    /// Human-readable description of what happened.
    pub description: String,
}

/// The complete result of a single Jepsen test run.
#[derive(Debug, Clone)]
pub struct TestResult {
    /// Name of the test / workload that was executed.
    pub test_name: String,
    /// Wall-clock duration of the test.
    pub duration: Duration,
    /// Total number of operations attempted.
    pub ops_count: usize,
    /// Number of operations that succeeded.
    pub ok_count: usize,
    /// Number of operations that returned an error.
    pub error_count: usize,
    /// Number of operations that timed out.
    pub timeout_count: usize,
    /// Whether the history was found to be linearizable.
    pub linearizable: bool,
    /// Linearizability violations (empty when linearizable).
    pub violations: Vec<Violation>,
    /// The fault schedule that was applied during the test.
    pub fault_schedule: Vec<FaultEvent>,
    /// Chronological timeline of notable events.
    pub timeline: Vec<TimelineEvent>,
}

// ---------------------------------------------------------------------------
// Report generator
// ---------------------------------------------------------------------------

/// Generates human-readable reports from test results.
pub struct ReportGenerator;

impl ReportGenerator {
    /// Generate a detailed ASCII box-drawing report for a single test result.
    pub fn generate_text(result: &TestResult) -> String {
        let width = 44;
        let sep = "═".repeat(width);
        let mid = "═".repeat(width);

        let pass_fail = if result.linearizable {
            "✅ PASS (Linearizable)"
        } else {
            "❌ FAIL (NOT Linearizable)"
        };

        let duration_secs = result.duration.as_secs();
        let faults = result.fault_schedule.len();

        let mut lines = Vec::new();
        lines.push(format!("╔{sep}╗"));
        lines.push(Self::pad_line("Ferrite Jepsen Test Report", width));
        lines.push(format!("╠{mid}╣"));
        lines.push(Self::pad_line(
            &format!("Test: {}", result.test_name),
            width,
        ));
        lines.push(Self::pad_line(
            &format!("Duration: {duration_secs}s"),
            width,
        ));
        lines.push(Self::pad_line(
            &format!("Operations: {}", Self::format_number(result.ops_count)),
            width,
        ));
        lines.push(Self::pad_line(&format!("Faults injected: {faults}"), width));
        lines.push(format!("╠{mid}╣"));
        lines.push(Self::pad_line(&format!("Result: {pass_fail}"), width));
        lines.push(Self::pad_line(
            &format!(
                "OK: {} | Error: {} | Timeout: {}",
                Self::format_number(result.ok_count),
                Self::format_number(result.error_count),
                Self::format_number(result.timeout_count),
            ),
            width,
        ));
        lines.push(Self::pad_line(
            &format!("Violations: {}", result.violations.len()),
            width,
        ));
        lines.push(format!("╚{sep}╝"));

        // Append violation details if any.
        if !result.violations.is_empty() {
            lines.push(String::new());
            lines.push("Violations:".to_string());
            for (i, v) in result.violations.iter().enumerate() {
                lines.push(format!(
                    "  {}. [op {} vs op {}] {}",
                    i + 1,
                    v.op1_index,
                    v.op2_index,
                    v.description
                ));
            }
        }

        // Append timeline if non-empty.
        if !result.timeline.is_empty() {
            lines.push(String::new());
            lines.push("Timeline:".to_string());
            for event in &result.timeline {
                lines.push(format!(
                    "  [{:>6.1}s] {}",
                    event.timestamp.as_secs_f64(),
                    event.description
                ));
            }
        }

        lines.join("\n")
    }

    /// Generate a summary table for multiple test results.
    pub fn generate_summary(results: &[TestResult]) -> String {
        if results.is_empty() {
            return "No test results to summarize.".to_string();
        }

        let mut lines = Vec::new();
        let header = format!(
            "{:<30} {:>8} {:>10} {:>8} {:>6}",
            "Test", "Duration", "Ops", "Result", "Faults"
        );
        let divider = "─".repeat(header.len());

        lines.push("Ferrite Jepsen Test Summary".to_string());
        lines.push(divider.clone());
        lines.push(header);
        lines.push(divider.clone());

        let mut total_ops = 0usize;
        let mut total_pass = 0usize;

        for r in results {
            let status = if r.linearizable { "PASS" } else { "FAIL" };
            let dur = format!("{}s", r.duration.as_secs());
            lines.push(format!(
                "{:<30} {:>8} {:>10} {:>8} {:>6}",
                truncate(&r.test_name, 30),
                dur,
                Self::format_number(r.ops_count),
                status,
                r.fault_schedule.len(),
            ));
            total_ops += r.ops_count;
            if r.linearizable {
                total_pass += 1;
            }
        }

        lines.push(divider);
        lines.push(format!(
            "Total: {} tests, {} passed, {} failed, {} ops",
            results.len(),
            total_pass,
            results.len() - total_pass,
            Self::format_number(total_ops),
        ));

        lines.join("\n")
    }

    // -- helpers --

    /// Pad a line inside a box-drawing frame: `║ text …     ║`
    fn pad_line(text: &str, width: usize) -> String {
        // Account for "║ " prefix and " ║" suffix (4 chars of chrome).
        let inner_width = width.saturating_sub(2);
        let display_len = Self::display_width(text);
        let padding = if display_len < inner_width {
            inner_width - display_len
        } else {
            0
        };
        format!("║ {}{} ║", text, " ".repeat(padding))
    }

    /// Approximate display width, counting multi-byte emoji as width 1
    /// (good enough for box drawing in a terminal).
    fn display_width(s: &str) -> usize {
        // Count characters rather than bytes; emoji sequences still count
        // as a single "character" in most terminal renderers.
        s.chars().count()
    }

    /// Format a number with thousands separators (e.g. 12847 → "12,847").
    fn format_number(n: usize) -> String {
        let s = n.to_string();
        let mut result = String::with_capacity(s.len() + s.len() / 3);
        for (i, ch) in s.chars().rev().enumerate() {
            if i > 0 && i % 3 == 0 {
                result.push(',');
            }
            result.push(ch);
        }
        result.chars().rev().collect()
    }
}

/// Truncate a string to at most `max_len` characters, appending `…` if
/// truncated.
fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        let mut t: String = s.chars().take(max_len.saturating_sub(1)).collect();
        t.push('…');
        t
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_result(name: &str, linearizable: bool) -> TestResult {
        TestResult {
            test_name: name.to_string(),
            duration: Duration::from_secs(60),
            ops_count: 12_847,
            ok_count: 12_341,
            error_count: 506,
            timeout_count: 0,
            linearizable,
            violations: Vec::new(),
            fault_schedule: vec![FaultEvent {
                timestamp: Duration::from_secs(10),
                fault_type: super::super::fault_injection::FaultType::ProcessCrash,
                target: super::super::fault_injection::FaultTarget::Node("n1".into()),
                duration: Some(Duration::from_secs(5)),
            }],
            timeline: vec![TimelineEvent {
                timestamp: Duration::from_secs(10),
                description: "Fault injected: ProcessCrash -> Node(n1)".to_string(),
            }],
        }
    }

    #[test]
    fn text_report_contains_key_sections() {
        let report = ReportGenerator::generate_text(&sample_result("register", true));
        assert!(report.contains("Ferrite Jepsen Test Report"));
        assert!(report.contains("register"));
        assert!(report.contains("PASS"));
        assert!(report.contains("12,847"));
        assert!(report.contains("Violations: 0"));
    }

    #[test]
    fn text_report_fail_case() {
        let report = ReportGenerator::generate_text(&sample_result("register", false));
        assert!(report.contains("FAIL"));
    }

    #[test]
    fn summary_report_multiple() {
        let results = vec![
            sample_result("register", true),
            sample_result("counter", true),
            sample_result("set-ops", false),
        ];
        let summary = ReportGenerator::generate_summary(&results);
        assert!(summary.contains("3 tests"));
        assert!(summary.contains("2 passed"));
        assert!(summary.contains("1 failed"));
    }

    #[test]
    fn summary_report_empty() {
        let summary = ReportGenerator::generate_summary(&[]);
        assert!(summary.contains("No test results"));
    }

    #[test]
    fn format_number_works() {
        assert_eq!(ReportGenerator::format_number(0), "0");
        assert_eq!(ReportGenerator::format_number(999), "999");
        assert_eq!(ReportGenerator::format_number(1_000), "1,000");
        assert_eq!(ReportGenerator::format_number(12_847), "12,847");
        assert_eq!(ReportGenerator::format_number(1_000_000), "1,000,000");
    }
}
