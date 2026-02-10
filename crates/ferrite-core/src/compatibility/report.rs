//! Compatibility Report Generator
//!
//! Generates human-readable and machine-readable compatibility reports
//! from the tracker data.

use serde::{Deserialize, Serialize};

use super::tracker::{CommandCategory, CommandStatus, CompatibilityTracker};

/// Per-command report entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandReport {
    /// Command name (e.g., "GET").
    pub name: String,
    /// Human-readable compatibility status.
    pub status: String,
    /// Number of tests executed.
    pub tests_run: usize,
    /// Number of tests that passed.
    pub tests_passed: usize,
    /// Notes about differences or limitations.
    pub notes: Vec<String>,
}

/// Per-category summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryReport {
    /// Category name.
    pub category: String,
    /// Total commands in this category.
    pub total_commands: usize,
    /// Commands with full compatibility.
    pub full_compat: usize,
    /// Commands with partial compatibility.
    pub partial_compat: usize,
    /// Commands not yet implemented.
    pub not_implemented: usize,
    /// Commands that failed tests.
    pub failed: usize,
    /// Category compatibility score (0–100).
    pub score: f64,
}

/// Full compatibility report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatibilityReport {
    /// Overall compatibility percentage (0-100)
    pub overall_score: f64,
    /// Critical command compatibility percentage (0-100)
    pub critical_score: f64,
    /// Total commands tracked
    pub total_commands: usize,
    /// Commands with full compatibility
    pub full_compat: usize,
    /// Commands with partial compatibility
    pub partial_compat: usize,
    /// Commands not implemented
    pub not_implemented: usize,
    /// Commands that failed
    pub failed: usize,
    /// Per-category breakdowns
    pub categories: Vec<CategoryReport>,
    /// Per-command details
    pub commands: Vec<CommandReport>,
    /// Certification grade (A+, A, B, C, D, F)
    pub grade: String,
}

impl CompatibilityReport {
    /// Generate a report from a tracker
    pub fn from_tracker(tracker: &CompatibilityTracker) -> Self {
        let overall = tracker.compatibility_score() * 100.0;
        let critical = tracker.critical_score() * 100.0;

        let total = tracker.total_commands();
        let full = tracker.count_by_status(CommandStatus::Full);
        let partial = tracker.count_by_status(CommandStatus::Partial);
        let not_impl = tracker.count_by_status(CommandStatus::NotImplemented);
        let failed = tracker.count_by_status(CommandStatus::Failed);

        // Generate category reports
        let categories: Vec<CategoryReport> = CommandCategory::all()
            .iter()
            .map(|&cat| {
                let cmds = tracker.commands_in_category(cat);
                let cat_total = cmds.len();
                let cat_full = cmds
                    .iter()
                    .filter(|c| c.status == CommandStatus::Full)
                    .count();
                let cat_partial = cmds
                    .iter()
                    .filter(|c| c.status == CommandStatus::Partial)
                    .count();
                let cat_not_impl = cmds
                    .iter()
                    .filter(|c| c.status == CommandStatus::NotImplemented)
                    .count();
                let cat_failed = cmds
                    .iter()
                    .filter(|c| c.status == CommandStatus::Failed)
                    .count();
                let score = if cat_total > 0 {
                    (cat_full as f64 + cat_partial as f64 * 0.5) / cat_total as f64 * 100.0
                } else {
                    0.0
                };

                CategoryReport {
                    category: cat.to_string(),
                    total_commands: cat_total,
                    full_compat: cat_full,
                    partial_compat: cat_partial,
                    not_implemented: cat_not_impl,
                    failed: cat_failed,
                    score,
                }
            })
            .filter(|c| c.total_commands > 0)
            .collect();

        // Generate command reports
        let mut commands: Vec<CommandReport> = tracker
            .all_commands()
            .values()
            .map(|entry| {
                let tests_passed = entry.tests.iter().filter(|t| t.passed).count();
                CommandReport {
                    name: entry.name.clone(),
                    status: format!("{}", entry.status),
                    tests_run: entry.tests.len(),
                    tests_passed,
                    notes: entry.notes.clone(),
                }
            })
            .collect();
        commands.sort_by(|a, b| a.name.cmp(&b.name));

        let grade = match overall as u32 {
            98..=100 => "A+",
            95..=97 => "A",
            90..=94 => "A-",
            85..=89 => "B+",
            80..=84 => "B",
            70..=79 => "C",
            60..=69 => "D",
            _ => "F",
        }
        .to_string();

        Self {
            overall_score: overall,
            critical_score: critical,
            total_commands: total,
            full_compat: full,
            partial_compat: partial,
            not_implemented: not_impl,
            failed,
            categories,
            commands,
            grade,
        }
    }

    /// Generate a human-readable text report
    pub fn to_text(&self) -> String {
        let mut out = String::new();
        out.push_str("╔══════════════════════════════════════════════════════╗\n");
        out.push_str("║       FERRITE REDIS COMPATIBILITY REPORT            ║\n");
        out.push_str("╠══════════════════════════════════════════════════════╣\n");
        out.push_str(&format!(
            "║  Overall Score:   {:5.1}%  Grade: {:>3}                ║\n",
            self.overall_score, self.grade
        ));
        out.push_str(&format!(
            "║  Critical Score:  {:5.1}%                              ║\n",
            self.critical_score
        ));
        out.push_str(&format!(
            "║  Commands: {} total | {} full | {} partial | {} missing ║\n",
            self.total_commands, self.full_compat, self.partial_compat, self.not_implemented
        ));
        out.push_str("╠══════════════════════════════════════════════════════╣\n");
        out.push_str("║  Category Breakdown:                                ║\n");

        for cat in &self.categories {
            out.push_str(&format!(
                "║    {:<15} {:5.1}%  ({}/{})\n",
                cat.category, cat.score, cat.full_compat, cat.total_commands
            ));
        }

        out.push_str("╚══════════════════════════════════════════════════════╝\n");
        out
    }

    /// Serialize to JSON
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_report() {
        let tracker = CompatibilityTracker::new();
        let report = CompatibilityReport::from_tracker(&tracker);

        assert!(report.overall_score > 90.0);
        assert!(report.total_commands > 100);
        assert!(!report.categories.is_empty());
        assert!(!report.commands.is_empty());
        assert!(!report.grade.is_empty());
    }

    #[test]
    fn test_report_text() {
        let tracker = CompatibilityTracker::new();
        let report = CompatibilityReport::from_tracker(&tracker);
        let text = report.to_text();
        assert!(text.contains("FERRITE REDIS COMPATIBILITY REPORT"));
        assert!(text.contains("Grade:"));
    }

    #[test]
    fn test_report_json() {
        let tracker = CompatibilityTracker::new();
        let report = CompatibilityReport::from_tracker(&tracker);
        let json = report.to_json();
        assert!(json.contains("overall_score"));
        assert!(json.contains("grade"));
    }

    #[test]
    fn test_grade_a_plus() {
        let tracker = CompatibilityTracker::new();
        let report = CompatibilityReport::from_tracker(&tracker);
        // All commands initialized as Full → 100% → A+
        assert_eq!(report.grade, "A+");
    }

    #[test]
    fn test_grade_degrades_on_failure() {
        let mut tracker = CompatibilityTracker::new();
        // Fail many commands to lower score
        for cmd in [
            "GET", "SET", "DEL", "EXISTS", "EXPIRE", "LPUSH", "RPUSH", "HSET",
        ] {
            tracker.set_status(cmd, CommandStatus::Failed);
        }
        let report = CompatibilityReport::from_tracker(&tracker);
        // Score should be lower than A+
        assert!(report.overall_score < 100.0);
    }
}
