//! Redis compatibility certification scoring and badge generation.
//!
//! Computes a formal certification score based on command coverage, behavior
//! accuracy, and edge-case handling. Generates machine-readable certification
//! data suitable for public dashboards and badges.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Certification grade based on overall compatibility score.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum CertificationGrade {
    /// <50% compatibility — not recommended for production.
    Failing,
    /// 50-69% — basic compatibility with significant gaps.
    Bronze,
    /// 70-84% — good compatibility for most workloads.
    Silver,
    /// 85-94% — high compatibility, suitable for production.
    Gold,
    /// 95%+ — near-complete compatibility, drop-in replacement.
    Platinum,
}

impl CertificationGrade {
    /// Determines grade from a percentage score.
    pub fn from_score(score: f64) -> Self {
        match score {
            s if s >= 95.0 => Self::Platinum,
            s if s >= 85.0 => Self::Gold,
            s if s >= 70.0 => Self::Silver,
            s if s >= 50.0 => Self::Bronze,
            _ => Self::Failing,
        }
    }

    /// Returns a human-readable label.
    pub fn label(&self) -> &str {
        match self {
            Self::Platinum => "Platinum — Drop-in Replacement",
            Self::Gold => "Gold — Production Ready",
            Self::Silver => "Silver — Good Compatibility",
            Self::Bronze => "Bronze — Basic Compatibility",
            Self::Failing => "Failing — Not Recommended",
        }
    }

    /// Returns the badge color for shields.io.
    pub fn badge_color(&self) -> &str {
        match self {
            Self::Platinum => "brightgreen",
            Self::Gold => "green",
            Self::Silver => "yellow",
            Self::Bronze => "orange",
            Self::Failing => "red",
        }
    }
}

/// Full certification result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificationResult {
    /// Overall compatibility percentage (0-100).
    pub overall_score: f64,
    /// Grade based on the score.
    pub grade: CertificationGrade,
    /// Per-category scores.
    pub category_scores: HashMap<String, CategoryScore>,
    /// Ferrite version tested.
    pub ferrite_version: String,
    /// Redis version being compared against.
    pub redis_version: String,
    /// Timestamp of the certification run.
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Total commands tested.
    pub total_commands_tested: u32,
    /// Commands passing all tests.
    pub commands_passing: u32,
    /// Commands with known intentional divergences.
    pub intentional_divergences: u32,
    /// List of failing commands with reasons.
    pub failing_commands: Vec<FailingCommand>,
}

/// Per-category compatibility score.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryScore {
    /// Category name.
    pub category: String,
    /// Compatibility score (0–100).
    pub score: f64,
    /// Total tests in this category.
    pub total_tests: u32,
    /// Tests that passed.
    pub passing_tests: u32,
    /// Tests that failed.
    pub failing_tests: u32,
    /// Tests that were skipped.
    pub skipped_tests: u32,
}

/// A command that failed certification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailingCommand {
    /// Command name.
    pub command: String,
    /// Category the command belongs to.
    pub category: String,
    /// Reason for the failure.
    pub reason: String,
    /// Whether this divergence is intentional.
    pub is_intentional_divergence: bool,
}

/// Generates a shields.io compatible badge URL.
pub fn badge_url(result: &CertificationResult) -> String {
    let label = urlencoding::encode("Redis Compatible");
    let score_str = format!("{:.0}%", result.overall_score);
    let message = urlencoding::encode(&score_str);
    let color = result.grade.badge_color();
    format!(
        "https://img.shields.io/badge/{}-{}-{}",
        label, message, color
    )
}

/// Generates a markdown summary of the certification result.
pub fn markdown_summary(result: &CertificationResult) -> String {
    let mut md = String::new();
    md.push_str(&format!(
        "# Redis Compatibility Certification — {}\n\n",
        result.grade.label()
    ));
    md.push_str(&format!(
        "**Score**: {:.1}% | **Grade**: {:?}\n\n",
        result.overall_score, result.grade
    ));
    md.push_str(&format!(
        "**Ferrite**: {} | **Redis Baseline**: {}\n\n",
        result.ferrite_version, result.redis_version
    ));
    md.push_str(&format!(
        "**Commands**: {} tested, {} passing, {} intentional divergences\n\n",
        result.total_commands_tested, result.commands_passing, result.intentional_divergences
    ));

    md.push_str("## Category Breakdown\n\n");
    md.push_str("| Category | Score | Pass | Fail | Skip |\n");
    md.push_str("|----------|-------|------|------|------|\n");

    let mut categories: Vec<_> = result.category_scores.values().collect();
    categories.sort_by(|a, b| a.category.cmp(&b.category));
    for cat in categories {
        md.push_str(&format!(
            "| {} | {:.1}% | {} | {} | {} |\n",
            cat.category, cat.score, cat.passing_tests, cat.failing_tests, cat.skipped_tests
        ));
    }

    if !result.failing_commands.is_empty() {
        md.push_str("\n## Failing Commands\n\n");
        for cmd in &result.failing_commands {
            let marker = if cmd.is_intentional_divergence {
                "⚠️ (intentional)"
            } else {
                "❌"
            };
            md.push_str(&format!(
                "- {} **{}** ({}): {}\n",
                marker, cmd.command, cmd.category, cmd.reason
            ));
        }
    }

    md
}

/// Computes certification result from test data.
pub fn compute_certification(
    category_results: HashMap<String, CategoryScore>,
    ferrite_version: &str,
    redis_version: &str,
    intentional_divergences: &[FailingCommand],
) -> CertificationResult {
    let total_commands: u32 = category_results.values().map(|c| c.total_tests).sum();
    let passing_commands: u32 = category_results.values().map(|c| c.passing_tests).sum();
    let intentional_count = intentional_divergences.len() as u32;

    let adjusted_total = total_commands.saturating_sub(intentional_count);
    let overall_score = if adjusted_total > 0 {
        (passing_commands as f64 / adjusted_total as f64) * 100.0
    } else {
        100.0
    };

    let failing_commands: Vec<FailingCommand> = category_results
        .values()
        .flat_map(|_| Vec::<FailingCommand>::new()) // placeholder
        .chain(intentional_divergences.iter().cloned())
        .collect();

    CertificationResult {
        overall_score: overall_score.clamp(0.0, 100.0),
        grade: CertificationGrade::from_score(overall_score),
        category_scores: category_results,
        ferrite_version: ferrite_version.to_string(),
        redis_version: redis_version.to_string(),
        timestamp: chrono::Utc::now(),
        total_commands_tested: total_commands,
        commands_passing: passing_commands,
        intentional_divergences: intentional_count,
        failing_commands,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grade_from_score() {
        assert_eq!(
            CertificationGrade::from_score(98.0),
            CertificationGrade::Platinum
        );
        assert_eq!(
            CertificationGrade::from_score(90.0),
            CertificationGrade::Gold
        );
        assert_eq!(
            CertificationGrade::from_score(75.0),
            CertificationGrade::Silver
        );
        assert_eq!(
            CertificationGrade::from_score(55.0),
            CertificationGrade::Bronze
        );
        assert_eq!(
            CertificationGrade::from_score(30.0),
            CertificationGrade::Failing
        );
    }

    #[test]
    fn test_badge_url() {
        let result = CertificationResult {
            overall_score: 96.5,
            grade: CertificationGrade::Platinum,
            category_scores: HashMap::new(),
            ferrite_version: "0.1.0".to_string(),
            redis_version: "7.2".to_string(),
            timestamp: chrono::Utc::now(),
            total_commands_tested: 200,
            commands_passing: 193,
            intentional_divergences: 3,
            failing_commands: vec![],
        };
        let url = badge_url(&result);
        assert!(url.contains("brightgreen"));
        assert!(url.contains("96%")); // 193/200 = 96.5, {:.0} rounds to 96
    }

    #[test]
    fn test_compute_certification() {
        let mut categories = HashMap::new();
        categories.insert(
            "strings".to_string(),
            CategoryScore {
                category: "strings".to_string(),
                score: 95.0,
                total_tests: 20,
                passing_tests: 19,
                failing_tests: 1,
                skipped_tests: 0,
            },
        );
        categories.insert(
            "lists".to_string(),
            CategoryScore {
                category: "lists".to_string(),
                score: 100.0,
                total_tests: 15,
                passing_tests: 15,
                failing_tests: 0,
                skipped_tests: 0,
            },
        );

        let divergences = vec![FailingCommand {
            command: "OBJECT ENCODING".to_string(),
            category: "strings".to_string(),
            reason: "Ferrite uses different internal encoding names".to_string(),
            is_intentional_divergence: true,
        }];

        let result = compute_certification(categories, "0.1.0", "7.2", &divergences);
        assert!(result.overall_score > 90.0);
        assert!(result.grade >= CertificationGrade::Gold);
        assert_eq!(result.intentional_divergences, 1);
    }

    #[test]
    fn test_markdown_summary() {
        let mut categories = HashMap::new();
        categories.insert(
            "strings".to_string(),
            CategoryScore {
                category: "strings".to_string(),
                score: 100.0,
                total_tests: 10,
                passing_tests: 10,
                failing_tests: 0,
                skipped_tests: 0,
            },
        );

        let result = CertificationResult {
            overall_score: 100.0,
            grade: CertificationGrade::Platinum,
            category_scores: categories,
            ferrite_version: "0.1.0".to_string(),
            redis_version: "7.2".to_string(),
            timestamp: chrono::Utc::now(),
            total_commands_tested: 10,
            commands_passing: 10,
            intentional_divergences: 0,
            failing_commands: vec![],
        };

        let md = markdown_summary(&result);
        assert!(md.contains("Platinum"));
        assert!(md.contains("100.0%"));
        assert!(md.contains("strings"));
    }

    #[test]
    fn test_grade_ordering() {
        assert!(CertificationGrade::Platinum > CertificationGrade::Gold);
        assert!(CertificationGrade::Gold > CertificationGrade::Silver);
        assert!(CertificationGrade::Silver > CertificationGrade::Bronze);
        assert!(CertificationGrade::Bronze > CertificationGrade::Failing);
    }
}
