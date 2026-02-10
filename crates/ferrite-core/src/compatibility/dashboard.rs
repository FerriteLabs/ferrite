//! HTML Dashboard Generator for Redis Compatibility Results
//!
//! Generates a self-contained HTML page showing compatibility scores,
//! category breakdowns, and individual test results.

use super::suite::SuiteResults;
use serde::Serialize;

/// Dashboard generator that creates HTML reports from [`SuiteResults`].
pub struct DashboardGenerator;

/// Grade descriptor used during rendering.
#[derive(Debug, Clone, Serialize)]
struct GradeInfo {
    letter: &'static str,
    label: &'static str,
    color: &'static str,
}

impl DashboardGenerator {
    /// Generate a self-contained HTML dashboard from suite results.
    pub fn generate_html(results: &SuiteResults) -> String {
        let grade = Self::grade_info(results.score);
        let mut html = String::with_capacity(16_384);

        // Header
        html.push_str("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n");
        html.push_str("<meta charset=\"utf-8\">\n");
        html.push_str("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n");
        html.push_str("<title>Ferrite — Redis Compatibility Dashboard</title>\n");
        Self::push_style(&mut html);
        html.push_str("</head>\n<body>\n");

        // Hero section
        html.push_str("<div class=\"hero\">\n");
        html.push_str("<h1>Ferrite Redis Compatibility</h1>\n");
        html.push_str(&format!(
            "<div class=\"score-ring\" style=\"--score-color:{}\">\
             <span class=\"score-value\">{:.1}%</span>\
             </div>\n",
            grade.color, results.score
        ));
        html.push_str(&format!(
            "<div class=\"grade\" style=\"color:{}\">{} — {}</div>\n",
            grade.color, grade.letter, grade.label
        ));
        html.push_str(&format!(
            "<p class=\"summary\">{} tests: <strong>{} passed</strong>, {} failed, {} skipped \
             &middot; critical score {:.1}% &middot; {:.1}ms</p>\n",
            results.total,
            results.passed,
            results.failed,
            results.skipped,
            results.critical_score,
            results.duration.as_secs_f64() * 1000.0,
        ));
        html.push_str("</div>\n");

        // Category table
        html.push_str("<h2>Category Breakdown</h2>\n");
        html.push_str(
            "<table>\n<thead><tr>\
            <th>Category</th><th>Total</th><th>Passed</th><th>Failed</th><th>Score</th>\
            </tr></thead>\n<tbody>\n",
        );
        let mut cats: Vec<_> = results.categories.iter().collect();
        cats.sort_by_key(|(name, _)| (*name).clone());
        for (name, cat) in &cats {
            let bar_color = Self::grade_info(cat.score).color;
            html.push_str(&format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td>\
                 <td><div class=\"bar-bg\"><div class=\"bar\" \
                 style=\"width:{:.1}%;background:{}\">{:.1}%</div></div></td></tr>\n",
                name, cat.total, cat.passed, cat.failed, cat.score, bar_color, cat.score,
            ));
        }
        html.push_str("</tbody>\n</table>\n");

        // Individual results
        html.push_str("<h2>Test Results</h2>\n");
        html.push_str("<div class=\"results\">\n");
        for r in &results.results {
            let icon = if r.passed { "✅" } else { "❌" };
            let cls = if r.passed { "pass" } else { "fail" };
            html.push_str(&format!(
                "<details class=\"{cls}\"><summary>{icon} <strong>{}</strong> \
                 <span class=\"cmd\">[{}]</span> \
                 <span class=\"cat\">{}</span> \
                 <span class=\"dur\">{:.2}ms</span></summary>\
                 <p>{}</p><p class=\"msg\">{}</p></details>\n",
                r.test.name,
                r.test.command,
                r.test.category,
                r.duration.as_secs_f64() * 1000.0,
                r.test.description,
                html_escape(&r.message),
            ));
        }
        html.push_str("</div>\n");

        html.push_str("</body>\n</html>");
        html
    }

    /// Generate a JSON report suitable for CI artifacts.
    pub fn generate_json(results: &SuiteResults) -> String {
        serde_json::to_string_pretty(results).unwrap_or_else(|e| format!("{{\"error\":\"{}\"}}", e))
    }

    /// Generate a compact markdown summary for PR comments.
    pub fn generate_markdown(results: &SuiteResults) -> String {
        let grade = Self::grade_info(results.score);
        let mut md = String::with_capacity(4_096);

        md.push_str(&format!(
            "## Redis Compatibility — {} {:.1}%\n\n",
            grade.letter, results.score,
        ));
        md.push_str(&format!(
            "**{}** tests | **{}** passed | **{}** failed | critical {:.1}%\n\n",
            results.total, results.passed, results.failed, results.critical_score,
        ));

        md.push_str("| Category | Total | Passed | Failed | Score |\n");
        md.push_str("|----------|------:|-------:|-------:|------:|\n");
        let mut cats: Vec<_> = results.categories.iter().collect();
        cats.sort_by_key(|(n, _)| (*n).clone());
        for (name, cat) in &cats {
            md.push_str(&format!(
                "| {} | {} | {} | {} | {:.1}% |\n",
                name, cat.total, cat.passed, cat.failed, cat.score,
            ));
        }

        if results.failed > 0 {
            md.push_str("\n### Failed Tests\n\n");
            for r in results.results.iter().filter(|r| !r.passed) {
                md.push_str(&format!(
                    "- ❌ **{}** (`{}`): {}\n",
                    r.test.name, r.test.command, r.message,
                ));
            }
        }

        md
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    fn grade_info(score: f64) -> GradeInfo {
        match score {
            s if s >= 97.0 => GradeInfo {
                letter: "A+",
                label: "Platinum",
                color: "#22c55e",
            },
            s if s >= 93.0 => GradeInfo {
                letter: "A",
                label: "Excellent",
                color: "#4ade80",
            },
            s if s >= 85.0 => GradeInfo {
                letter: "B",
                label: "Good",
                color: "#facc15",
            },
            s if s >= 70.0 => GradeInfo {
                letter: "C",
                label: "Fair",
                color: "#fb923c",
            },
            s if s >= 50.0 => GradeInfo {
                letter: "D",
                label: "Poor",
                color: "#f87171",
            },
            _ => GradeInfo {
                letter: "F",
                label: "Failing",
                color: "#ef4444",
            },
        }
    }

    fn push_style(html: &mut String) {
        html.push_str("<style>\n");
        html.push_str(
            ":root{--bg:#0f172a;--fg:#e2e8f0;--card:#1e293b;--border:#334155}\n\
             *{box-sizing:border-box;margin:0;padding:0}\n\
             body{font-family:system-ui,-apple-system,sans-serif;background:var(--bg);\
             color:var(--fg);max-width:960px;margin:0 auto;padding:2rem}\n\
             h1{font-size:1.8rem;margin-bottom:.5rem}\n\
             h2{font-size:1.3rem;margin:2rem 0 1rem;border-bottom:1px solid var(--border);\
             padding-bottom:.5rem}\n\
             .hero{text-align:center;padding:2rem 0}\n\
             .score-ring{display:inline-flex;align-items:center;justify-content:center;\
             width:120px;height:120px;border-radius:50%;\
             border:6px solid var(--score-color);margin:1rem 0}\n\
             .score-value{font-size:2rem;font-weight:700}\n\
             .grade{font-size:1.5rem;font-weight:700;margin:.5rem 0}\n\
             .summary{color:#94a3b8;font-size:.95rem}\n\
             table{width:100%;border-collapse:collapse}\n\
             th,td{padding:.6rem .8rem;text-align:left;border-bottom:1px solid var(--border)}\n\
             th{color:#94a3b8;font-weight:600;font-size:.85rem;text-transform:uppercase}\n\
             .bar-bg{background:var(--card);border-radius:4px;overflow:hidden;height:22px;\
             min-width:100px}\n\
             .bar{height:100%;display:flex;align-items:center;padding:0 6px;\
             font-size:.8rem;font-weight:600;color:#000;border-radius:4px;white-space:nowrap}\n\
             .results{display:flex;flex-direction:column;gap:.4rem}\n\
             details{background:var(--card);border-radius:6px;padding:.6rem 1rem}\n\
             details.fail{border-left:3px solid #ef4444}\n\
             details.pass{border-left:3px solid #22c55e}\n\
             summary{cursor:pointer;display:flex;align-items:center;gap:.5rem;flex-wrap:wrap}\n\
             .cmd{color:#60a5fa;font-size:.85rem}\n\
             .cat{color:#94a3b8;font-size:.8rem}\n\
             .dur{color:#64748b;font-size:.8rem;margin-left:auto}\n\
             .msg{color:#94a3b8;font-size:.9rem;margin-top:.3rem}\n",
        );
        html.push_str("</style>\n");
    }
}

/// Minimal HTML escaping for user-facing strings.
fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[cfg(test)]
mod tests {
    use super::super::suite::CompatibilitySuite;
    use super::*;
    use crate::storage::Store;

    fn run_suite() -> SuiteResults {
        let store = Store::new(16);
        let suite = CompatibilitySuite::new();
        suite.run(&store)
    }

    #[test]
    fn test_generate_html_not_empty() {
        let results = run_suite();
        let html = DashboardGenerator::generate_html(&results);
        assert!(html.contains("<!DOCTYPE html>"));
        assert!(html.contains("Ferrite"));
        assert!(html.contains("Category Breakdown"));
    }

    #[test]
    fn test_generate_html_contains_score() {
        let results = run_suite();
        let html = DashboardGenerator::generate_html(&results);
        assert!(html.contains('%'), "HTML should contain a percentage");
    }

    #[test]
    fn test_generate_json_valid() {
        let results = run_suite();
        let json = DashboardGenerator::generate_json(&results);
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("JSON should be valid");
        assert!(parsed.get("total").is_some());
        assert!(parsed.get("score").is_some());
    }

    #[test]
    fn test_generate_markdown_structure() {
        let results = run_suite();
        let md = DashboardGenerator::generate_markdown(&results);
        assert!(md.contains("## Redis Compatibility"));
        assert!(md.contains("| Category |"));
    }

    #[test]
    fn test_html_escape() {
        assert_eq!(html_escape("<b>hi</b>"), "&lt;b&gt;hi&lt;/b&gt;");
    }
}
