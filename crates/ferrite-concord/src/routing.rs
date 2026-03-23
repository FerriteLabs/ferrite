//! Data-sovereignty routing — per-key region pinning rules.

use serde::{Deserialize, Serialize};

/// A routing rule that pins keys matching a pattern to a specific region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingRule {
    /// Glob pattern, e.g. "*:user:eu:*"
    pub pattern: String,
    /// Target region, e.g. "eu-west-1"
    pub region: String,
    /// Higher priority rules are evaluated first.
    pub priority: u32,
}

/// Router that applies sovereignty rules to key lookups.
pub struct SovereigntyRouter {
    rules: Vec<RoutingRule>,
}

impl SovereigntyRouter {
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    pub fn add_rule(&mut self, rule: RoutingRule) {
        self.rules.push(rule);
        self.rules.sort_by(|a, b| b.priority.cmp(&a.priority));
    }

    /// Match key against rules (highest priority first).
    /// Returns the region if matched, `None` if no rule matches.
    pub fn route(&self, key: &str) -> Option<&str> {
        for rule in &self.rules {
            if glob_match(&rule.pattern, key) {
                return Some(&rule.region);
            }
        }
        None
    }

    pub fn rules(&self) -> &[RoutingRule] {
        &self.rules
    }
}

impl Default for SovereigntyRouter {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple glob matching supporting `*` wildcard (matches any sequence of chars).
fn glob_match(pattern: &str, text: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let txt: Vec<char> = text.chars().collect();
    let (plen, tlen) = (pat.len(), txt.len());
    let (mut pi, mut ti) = (0, 0);
    let (mut star_pi, mut star_ti) = (usize::MAX, 0);

    while ti < tlen {
        if pi < plen && (pat[pi] == '?' || pat[pi] == txt[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < plen && pat[pi] == '*' {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    while pi < plen && pat[pi] == '*' {
        pi += 1;
    }
    pi == plen
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_matches_highest_priority() {
        let mut router = SovereigntyRouter::new();
        router.add_rule(RoutingRule {
            pattern: "*:eu:*".to_string(),
            region: "eu-west-1".to_string(),
            priority: 10,
        });
        router.add_rule(RoutingRule {
            pattern: "*:eu:premium:*".to_string(),
            region: "eu-central-1".to_string(),
            priority: 20,
        });

        // Higher-priority rule matches first
        assert_eq!(router.route("data:eu:premium:key1"), Some("eu-central-1"));
        // Lower-priority rule catches the rest
        assert_eq!(router.route("data:eu:key2"), Some("eu-west-1"));
    }

    #[test]
    fn route_returns_none_when_no_match() {
        let mut router = SovereigntyRouter::new();
        router.add_rule(RoutingRule {
            pattern: "eu:*".to_string(),
            region: "eu-west-1".to_string(),
            priority: 1,
        });
        assert_eq!(router.route("us:data:key1"), None);
    }

    #[test]
    fn glob_matching_works() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("foo:*:bar", "foo:x:bar"));
        assert!(glob_match("foo:*:bar", "foo:x:y:z:bar"));
        assert!(!glob_match("foo:*:bar", "foo:x:baz"));
        assert!(glob_match("*:eu:*", "data:eu:key"));
        assert!(glob_match("prefix*", "prefix_and_more"));
        assert!(glob_match("*suffix", "something_suffix"));
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "notexact"));
    }
}
