#![forbid(unsafe_code)]
//! Launch readiness checker and content template generator for coordinated
//! open-source release. Verifies prerequisites across code, documentation,
//! infrastructure, community, marketing, and legal categories, and produces
//! platform-specific content templates with variable substitution.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// High-level category for a readiness check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CheckCategory {
    Code,
    Documentation,
    Infrastructure,
    Community,
    Marketing,
    Legal,
}

impl std::fmt::Display for CheckCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Code => write!(f, "Code"),
            Self::Documentation => write!(f, "Documentation"),
            Self::Infrastructure => write!(f, "Infrastructure"),
            Self::Community => write!(f, "Community"),
            Self::Marketing => write!(f, "Marketing"),
            Self::Legal => write!(f, "Legal"),
        }
    }
}

/// Current status of a single readiness check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CheckStatus {
    NotStarted,
    InProgress,
    Done,
    Skipped,
    Blocked(String),
}

/// Target platform for a content template.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Platform {
    HackerNews,
    Reddit,
    Twitter,
    LinkedIn,
    Blog,
    Discord,
    GitHub,
}

impl std::fmt::Display for Platform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HackerNews => write!(f, "Hacker News"),
            Self::Reddit => write!(f, "Reddit"),
            Self::Twitter => write!(f, "Twitter/X"),
            Self::LinkedIn => write!(f, "LinkedIn"),
            Self::Blog => write!(f, "Blog"),
            Self::Discord => write!(f, "Discord"),
            Self::GitHub => write!(f, "GitHub"),
        }
    }
}

// ---------------------------------------------------------------------------
// Core structs
// ---------------------------------------------------------------------------

/// A single prerequisite check for launch readiness.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadinessCheck {
    /// Unique identifier (e.g. `"code-builds"`).
    pub id: String,
    /// High-level category this check belongs to.
    pub category: CheckCategory,
    /// Human-readable title.
    pub title: String,
    /// Longer description of what needs to be verified.
    pub description: String,
    /// If `true`, this check blocks launch; otherwise it is recommended.
    pub required: bool,
    /// Current status of this check.
    pub status: CheckStatus,
}

/// A platform-specific content template with `{{variable}}` placeholders.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentTemplate {
    /// Unique identifier for the template.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Target platform.
    pub platform: Platform,
    /// Template body with `{{variable}}` placeholders.
    pub template: String,
    /// Optional character limit for the platform.
    pub char_limit: Option<usize>,
    /// Helpful tips for posting on this platform.
    pub tips: Vec<String>,
}

/// Aggregated report for a single [`CheckCategory`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryReport {
    /// The category.
    pub category: CheckCategory,
    /// Total checks in this category.
    pub total: usize,
    /// Completed checks.
    pub completed: usize,
    /// Completion percentage (0.0–100.0).
    pub pct: f64,
}

/// Full launch-readiness report produced by [`LaunchReadinessChecker::run_checks`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaunchReport {
    pub total_checks: usize,
    pub completed: usize,
    pub in_progress: usize,
    pub not_started: usize,
    pub blocked: usize,
    pub required_completed: usize,
    pub required_total: usize,
    /// Percentage of all checks completed (0.0–100.0).
    pub readiness_pct: f64,
    /// Titles of required checks that are not yet done.
    pub blocking_items: Vec<String>,
    /// Per-category breakdown.
    pub categories: Vec<CategoryReport>,
    /// Rough estimate of days remaining (heuristic).
    pub estimated_days_to_ready: Option<u32>,
}

/// Overall readiness score with a go/no-go recommendation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadinessScore {
    /// Percentage of all checks completed (0.0–100.0).
    pub overall_pct: f64,
    /// `true` only when every *required* check is [`CheckStatus::Done`].
    pub can_launch: bool,
    /// Human-readable recommendation string.
    pub recommendation: String,
}

// ---------------------------------------------------------------------------
// LaunchReadinessChecker
// ---------------------------------------------------------------------------

/// Orchestrates launch-readiness verification and content generation.
#[derive(Debug, Clone)]
pub struct LaunchReadinessChecker {
    /// All readiness checks.
    pub checks: Vec<ReadinessCheck>,
    /// All content templates.
    pub content_templates: Vec<ContentTemplate>,
}

impl LaunchReadinessChecker {
    /// Create a new checker pre-populated with the default checks and content
    /// templates for a Ferrite open-source launch.
    pub fn new() -> Self {
        Self {
            checks: default_checks(),
            content_templates: default_templates(),
        }
    }

    /// Run all checks and produce a [`LaunchReport`].
    pub fn run_checks(&self) -> LaunchReport {
        let total_checks = self.checks.len();
        let completed = self.checks.iter().filter(|c| c.status == CheckStatus::Done).count();
        let in_progress = self
            .checks
            .iter()
            .filter(|c| c.status == CheckStatus::InProgress)
            .count();
        let blocked = self
            .checks
            .iter()
            .filter(|c| matches!(c.status, CheckStatus::Blocked(_)))
            .count();
        let not_started = self
            .checks
            .iter()
            .filter(|c| c.status == CheckStatus::NotStarted)
            .count();

        let required_total = self.checks.iter().filter(|c| c.required).count();
        let required_completed = self
            .checks
            .iter()
            .filter(|c| c.required && c.status == CheckStatus::Done)
            .count();

        let readiness_pct = if total_checks == 0 {
            100.0
        } else {
            (completed as f64 / total_checks as f64) * 100.0
        };

        let blocking_items: Vec<String> = self
            .checks
            .iter()
            .filter(|c| c.required && c.status != CheckStatus::Done)
            .map(|c| c.title.clone())
            .collect();

        let categories = self.category_reports();

        let remaining = total_checks.saturating_sub(completed);
        let estimated_days_to_ready = if remaining == 0 {
            None
        } else {
            Some(remaining as u32)
        };

        LaunchReport {
            total_checks,
            completed,
            in_progress,
            not_started,
            blocked,
            required_completed,
            required_total,
            readiness_pct,
            blocking_items,
            categories,
            estimated_days_to_ready,
        }
    }

    /// Look up a single check by its `id`.
    pub fn get_check(&self, id: &str) -> Option<&ReadinessCheck> {
        self.checks.iter().find(|c| c.id == id)
    }

    /// Return a slice of all content templates.
    pub fn content_templates(&self) -> &[ContentTemplate] {
        &self.content_templates
    }

    /// Render a content template by substituting `{{key}}` placeholders with
    /// values from `vars`. Returns `None` if `template_id` is not found.
    pub fn generate_content(
        &self,
        template_id: &str,
        vars: &HashMap<String, String>,
    ) -> Option<String> {
        let tpl = self.content_templates.iter().find(|t| t.id == template_id)?;
        let mut output = tpl.template.clone();
        for (key, value) in vars {
            let placeholder = format!("{{{{{}}}}}", key);
            output = output.replace(&placeholder, value);
        }
        Some(output)
    }

    /// Export all checks as a markdown checklist grouped by category.
    pub fn checklist_markdown(&self) -> String {
        let mut md = String::from("# Launch Readiness Checklist\n\n");

        let categories = [
            CheckCategory::Code,
            CheckCategory::Documentation,
            CheckCategory::Infrastructure,
            CheckCategory::Community,
            CheckCategory::Marketing,
            CheckCategory::Legal,
        ];

        for cat in &categories {
            let items: Vec<&ReadinessCheck> =
                self.checks.iter().filter(|c| c.category == *cat).collect();
            if items.is_empty() {
                continue;
            }
            md.push_str(&format!("## {cat}\n\n"));
            for item in &items {
                let marker = if item.status == CheckStatus::Done {
                    "x"
                } else {
                    " "
                };
                let req = if item.required { " **(required)**" } else { "" };
                md.push_str(&format!("- [{marker}] {}{req}\n", item.title));
            }
            md.push('\n');
        }
        md
    }

    /// Compute an overall [`ReadinessScore`].
    pub fn overall_readiness(&self) -> ReadinessScore {
        let report = self.run_checks();
        let can_launch = report.required_completed == report.required_total;
        let recommendation = if can_launch {
            if report.readiness_pct >= 90.0 {
                "Ready to launch! All required checks pass and most recommended items are done."
                    .to_string()
            } else {
                "All required checks pass. Consider completing recommended items before launch."
                    .to_string()
            }
        } else {
            let remaining = report.required_total - report.required_completed;
            format!(
                "Not ready to launch. {remaining} required check(s) still pending: {}",
                report.blocking_items.join(", ")
            )
        };

        ReadinessScore {
            overall_pct: report.readiness_pct,
            can_launch,
            recommendation,
        }
    }

    // -- private helpers ----------------------------------------------------

    fn category_reports(&self) -> Vec<CategoryReport> {
        let categories = [
            CheckCategory::Code,
            CheckCategory::Documentation,
            CheckCategory::Infrastructure,
            CheckCategory::Community,
            CheckCategory::Marketing,
            CheckCategory::Legal,
        ];

        categories
            .iter()
            .map(|cat| {
                let items: Vec<&ReadinessCheck> =
                    self.checks.iter().filter(|c| c.category == *cat).collect();
                let total = items.len();
                let completed = items.iter().filter(|c| c.status == CheckStatus::Done).count();
                let pct = if total == 0 {
                    100.0
                } else {
                    (completed as f64 / total as f64) * 100.0
                };
                CategoryReport {
                    category: *cat,
                    total,
                    completed,
                    pct,
                }
            })
            .collect()
    }
}

impl Default for LaunchReadinessChecker {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Default checks
// ---------------------------------------------------------------------------

fn check(
    id: &str,
    category: CheckCategory,
    title: &str,
    description: &str,
    required: bool,
) -> ReadinessCheck {
    ReadinessCheck {
        id: id.to_string(),
        category,
        title: title.to_string(),
        description: description.to_string(),
        required,
        status: CheckStatus::NotStarted,
    }
}

fn default_checks() -> Vec<ReadinessCheck> {
    use CheckCategory::*;
    vec![
        // Code — required
        check(
            "code-builds",
            Code,
            "Project builds on Linux, macOS, Windows",
            "Verify the project compiles without errors on all three major platforms.",
            true,
        ),
        check(
            "code-tests-pass",
            Code,
            "All tests pass on CI",
            "Ensure the full test suite passes in continuous integration.",
            true,
        ),
        check(
            "code-no-secrets",
            Code,
            "No hardcoded secrets, keys, or tokens",
            "Audit the codebase for accidental secret exposure.",
            true,
        ),
        check(
            "code-license-headers",
            Code,
            "Apache-2.0 license headers on all source files",
            "Every source file must carry the appropriate license header.",
            true,
        ),
        check(
            "code-deps-audit",
            Code,
            "cargo deny check passes (no vulnerable deps)",
            "Run cargo deny to ensure no known-vulnerable dependencies ship.",
            true,
        ),
        // Code — recommended
        check(
            "code-clippy-clean",
            Code,
            "cargo clippy passes with zero warnings",
            "Lint the codebase with clippy using -D warnings.",
            false,
        ),
        check(
            "code-coverage",
            Code,
            "Test coverage > 70%",
            "Measure code coverage and verify it exceeds the 70% threshold.",
            false,
        ),
        check(
            "code-benchmarks",
            Code,
            "At least one published benchmark comparison",
            "Publish benchmark results comparing Ferrite to similar systems.",
            false,
        ),
        // Documentation — required
        check(
            "docs-readme",
            Documentation,
            "README.md with quick start, features, badges",
            "The README must include quick start instructions, feature list, and status badges.",
            true,
        ),
        check(
            "docs-contributing",
            Documentation,
            "CONTRIBUTING.md with setup instructions",
            "Contributor guide with development environment setup.",
            true,
        ),
        check(
            "docs-license",
            Documentation,
            "LICENSE file present (Apache-2.0)",
            "A LICENSE file with the full Apache-2.0 text must be in the repo root.",
            true,
        ),
        check(
            "docs-changelog",
            Documentation,
            "CHANGELOG.md with at least v0.1.0 entry",
            "Maintain a changelog following Keep a Changelog format.",
            true,
        ),
        // Documentation — recommended
        check(
            "docs-architecture",
            Documentation,
            "Architecture documentation (ARCHITECTURE.md or ADRs)",
            "High-level architecture document or Architecture Decision Records.",
            false,
        ),
        check(
            "docs-api-reference",
            Documentation,
            "API reference documentation",
            "Generated or hand-written API reference for public crate interfaces.",
            false,
        ),
        check(
            "docs-website",
            Documentation,
            "Documentation website deployed",
            "Public documentation website (e.g. Docusaurus) is deployed and accessible.",
            false,
        ),
        // Infrastructure — required
        check(
            "infra-ci",
            Infrastructure,
            "CI pipeline (GitHub Actions) running on PRs and main",
            "Continuous integration must run on every pull request and the main branch.",
            true,
        ),
        check(
            "infra-docker",
            Infrastructure,
            "Docker image builds and publishes",
            "Docker image builds successfully and is pushed to a container registry.",
            true,
        ),
        check(
            "infra-release",
            Infrastructure,
            "Release automation (tag → build → publish)",
            "Tagging a release triggers automated build and publish workflows.",
            true,
        ),
        // Infrastructure — recommended
        check(
            "infra-homebrew",
            Infrastructure,
            "Homebrew formula available",
            "A Homebrew tap with a working formula for easy macOS/Linux install.",
            false,
        ),
        check(
            "infra-crates-io",
            Infrastructure,
            "Published on crates.io",
            "Crate published on crates.io for cargo install support.",
            false,
        ),
        // Community — required
        check(
            "community-coc",
            Community,
            "CODE_OF_CONDUCT.md present",
            "A code of conduct based on Contributor Covenant or similar.",
            true,
        ),
        check(
            "community-security",
            Community,
            "SECURITY.md with disclosure policy",
            "Security policy with responsible disclosure instructions.",
            true,
        ),
        // Community — recommended
        check(
            "community-discord",
            Community,
            "Discord/community channel set up",
            "A public Discord server or forum for community interaction.",
            false,
        ),
        check(
            "community-good-first-issues",
            Community,
            "At least 5 good-first-issue labeled issues",
            "Create approachable issues for new contributors.",
            false,
        ),
        check(
            "community-templates",
            Community,
            "Issue and PR templates configured",
            "GitHub issue and pull request templates in .github/ directory.",
            false,
        ),
        // Marketing — recommended
        check(
            "marketing-hn-post",
            Marketing,
            "Show HN post drafted",
            "Draft the Hacker News Show HN submission text.",
            false,
        ),
        check(
            "marketing-blog-post",
            Marketing,
            "Launch blog post written",
            "Write a launch blog post for the documentation site.",
            false,
        ),
        check(
            "marketing-social",
            Marketing,
            "Social media announcements prepared",
            "Draft announcements for Twitter/X, LinkedIn, and Reddit.",
            false,
        ),
        check(
            "marketing-demo-video",
            Marketing,
            "5-minute demo video recorded",
            "Record a short demo video showcasing key features.",
            false,
        ),
        // Legal — required
        check(
            "legal-license",
            Legal,
            "License is OSI-approved (Apache-2.0)",
            "Verify the chosen license is approved by the Open Source Initiative.",
            true,
        ),
        check(
            "legal-contributor-agreement",
            Legal,
            "CLA or DCO configured",
            "Set up a Contributor License Agreement or Developer Certificate of Origin.",
            true,
        ),
        check(
            "legal-trademark",
            Legal,
            "Project name available / no conflicts",
            "Confirm there are no trademark conflicts with the project name.",
            true,
        ),
    ]
}

// ---------------------------------------------------------------------------
// Default content templates
// ---------------------------------------------------------------------------

fn default_templates() -> Vec<ContentTemplate> {
    vec![
        ContentTemplate {
            id: "show-hn".to_string(),
            name: "Show HN Post".to_string(),
            platform: Platform::HackerNews,
            template: "Show HN: {{project_name}} – {{tagline}}\n\n\
                {{description}}\n\n\
                Key features:\n\
                {{features_list}}\n\n\
                Links:\n\
                - GitHub: {{github_url}}\n\
                - Docs: {{docs_url}}\n\
                - Quick start: {{quickstart_url}}\n\n\
                We'd love your feedback!"
                .to_string(),
            char_limit: Some(300),
            tips: vec![
                "Post on weekday mornings (US time) for best visibility.".to_string(),
                "Title must start with 'Show HN:'.".to_string(),
                "Be concise — HN readers prefer substance over hype.".to_string(),
            ],
        },
        ContentTemplate {
            id: "reddit-rust".to_string(),
            name: "Reddit r/rust Post".to_string(),
            platform: Platform::Reddit,
            template: "# {{project_name}}: {{tagline}}\n\n\
                {{description}}\n\n\
                ## Why we built this\n\
                {{motivation}}\n\n\
                ## Quick Start\n\
                ```bash\n\
                {{quickstart_command}}\n\
                ```\n\n\
                ## Benchmarks\n\
                {{benchmark_summary}}\n\n\
                GitHub: {{github_url}}"
                .to_string(),
            char_limit: None,
            tips: vec![
                "Include benchmark numbers — r/rust loves performance data.".to_string(),
                "Mention what makes this different from existing solutions.".to_string(),
                "Be ready to answer technical questions in comments.".to_string(),
            ],
        },
        ContentTemplate {
            id: "twitter-thread".to_string(),
            name: "Twitter/X Thread".to_string(),
            platform: Platform::Twitter,
            template: "🚀 Introducing {{project_name}} — {{tagline}}\n\n\
                {{short_description}} (280 chars max)\n\n\
                Thread 🧵👇"
                .to_string(),
            char_limit: Some(280),
            tips: vec![
                "Keep the first tweet under 280 characters.".to_string(),
                "Use a thread for details — first tweet should hook readers.".to_string(),
                "Include a screenshot or GIF for higher engagement.".to_string(),
            ],
        },
        ContentTemplate {
            id: "discord-welcome".to_string(),
            name: "Discord Welcome".to_string(),
            platform: Platform::Discord,
            template: "Welcome to the {{project_name}} community! 🎉\n\n\
                📖 Docs: {{docs_url}}\n\
                💻 GitHub: {{github_url}}\n\
                🐛 Issues: {{issues_url}}\n\n\
                Channels:\n\
                #general — Chat about anything\n\
                #help — Get help using {{project_name}}\n\
                #development — Discuss contributions\n\
                #showcase — Share what you've built"
                .to_string(),
            char_limit: None,
            tips: vec![
                "Pin this message in your #welcome or #rules channel.".to_string(),
                "Update links whenever URLs change.".to_string(),
            ],
        },
    ]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_populates_checks_and_templates() {
        let checker = LaunchReadinessChecker::new();
        assert!(!checker.checks.is_empty());
        assert!(!checker.content_templates.is_empty());
    }

    #[test]
    fn test_default_checks_count() {
        let checker = LaunchReadinessChecker::new();
        // 8 code + 7 docs + 5 infra + 5 community + 4 marketing + 3 legal = 32
        assert_eq!(checker.checks.len(), 32);
    }

    #[test]
    fn test_get_check_found_and_missing() {
        let checker = LaunchReadinessChecker::new();
        assert!(checker.get_check("code-builds").is_some());
        assert!(checker.get_check("nonexistent-check").is_none());
    }

    #[test]
    fn test_run_checks_all_not_started() {
        let checker = LaunchReadinessChecker::new();
        let report = checker.run_checks();
        assert_eq!(report.total_checks, 32);
        assert_eq!(report.completed, 0);
        assert_eq!(report.not_started, 32);
        assert_eq!(report.in_progress, 0);
        assert_eq!(report.blocked, 0);
        assert_eq!(report.readiness_pct, 0.0);
        assert!(!report.blocking_items.is_empty());
        assert!(report.estimated_days_to_ready.is_some());
    }

    #[test]
    fn test_run_checks_all_done() {
        let mut checker = LaunchReadinessChecker::new();
        for c in &mut checker.checks {
            c.status = CheckStatus::Done;
        }
        let report = checker.run_checks();
        assert_eq!(report.completed, report.total_checks);
        assert_eq!(report.readiness_pct, 100.0);
        assert!(report.blocking_items.is_empty());
        assert!(report.estimated_days_to_ready.is_none());
    }

    #[test]
    fn test_overall_readiness_cannot_launch() {
        let checker = LaunchReadinessChecker::new();
        let score = checker.overall_readiness();
        assert!(!score.can_launch);
        assert!(score.recommendation.contains("Not ready"));
    }

    #[test]
    fn test_overall_readiness_can_launch() {
        let mut checker = LaunchReadinessChecker::new();
        for c in &mut checker.checks {
            if c.required {
                c.status = CheckStatus::Done;
            }
        }
        let score = checker.overall_readiness();
        assert!(score.can_launch);
    }

    #[test]
    fn test_generate_content_substitutes_vars() {
        let checker = LaunchReadinessChecker::new();
        let mut vars = HashMap::new();
        vars.insert("project_name".to_string(), "Ferrite".to_string());
        vars.insert("tagline".to_string(), "Blazing fast KV store".to_string());
        let content = checker.generate_content("show-hn", &vars).unwrap();
        assert!(content.contains("Ferrite"));
        assert!(content.contains("Blazing fast KV store"));
        // Unreplaced placeholder still present
        assert!(content.contains("{{description}}"));
    }

    #[test]
    fn test_generate_content_unknown_template() {
        let checker = LaunchReadinessChecker::new();
        let vars = HashMap::new();
        assert!(checker.generate_content("nonexistent", &vars).is_none());
    }

    #[test]
    fn test_checklist_markdown_format() {
        let mut checker = LaunchReadinessChecker::new();
        checker.checks[0].status = CheckStatus::Done;
        let md = checker.checklist_markdown();
        assert!(md.starts_with("# Launch Readiness Checklist"));
        assert!(md.contains("## Code"));
        assert!(md.contains("[x]"));
        assert!(md.contains("[ ]"));
        assert!(md.contains("**(required)**"));
    }

    #[test]
    fn test_category_reports_coverage() {
        let checker = LaunchReadinessChecker::new();
        let report = checker.run_checks();
        assert_eq!(report.categories.len(), 6);
        let code_cat = report
            .categories
            .iter()
            .find(|c| c.category == CheckCategory::Code)
            .unwrap();
        assert_eq!(code_cat.total, 8);
        assert_eq!(code_cat.completed, 0);
    }

    #[test]
    fn test_blocked_status_counted() {
        let mut checker = LaunchReadinessChecker::new();
        checker.checks[0].status = CheckStatus::Blocked("waiting on CI".to_string());
        let report = checker.run_checks();
        assert_eq!(report.blocked, 1);
    }

    #[test]
    fn test_content_templates_accessor() {
        let checker = LaunchReadinessChecker::new();
        let templates = checker.content_templates();
        assert_eq!(templates.len(), 4);
        assert!(templates.iter().any(|t| t.id == "show-hn"));
        assert!(templates.iter().any(|t| t.id == "reddit-rust"));
        assert!(templates.iter().any(|t| t.id == "twitter-thread"));
        assert!(templates.iter().any(|t| t.id == "discord-welcome"));
    }
}
