//! Routing rules for custom routing behavior

use super::{IndexType, QueryPattern, RouteDecision, RouteTarget};
use bytes::Bytes;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// A routing rule that matches queries and provides routing decisions
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RoutingRule {
    /// Rule ID
    pub id: String,
    /// Rule name
    pub name: String,
    /// Rule priority (higher = checked first)
    pub priority: i32,
    /// Match condition
    #[serde(flatten)]
    pub matcher: RuleMatcher,
    /// Action to take
    pub action: RuleAction,
    /// Whether the rule is enabled
    pub enabled: bool,
}

impl RoutingRule {
    /// Create a new routing rule
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            priority: 0,
            matcher: RuleMatcher::default(),
            action: RuleAction::default(),
            enabled: true,
        }
    }

    /// Set priority
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Match command
    pub fn match_command(mut self, pattern: &str) -> Self {
        self.matcher.command_pattern = Some(pattern.to_string());
        self
    }

    /// Match key prefix
    pub fn match_key_prefix(mut self, prefix: &str) -> Self {
        self.matcher.key_prefix = Some(prefix.to_string());
        self
    }

    /// Match key pattern (regex)
    pub fn match_key_pattern(mut self, pattern: &str) -> Self {
        self.matcher.key_pattern = Some(pattern.to_string());
        self
    }

    /// Route to target
    pub fn route_to(mut self, target: RouteTarget) -> Self {
        self.action.target = Some(target);
        self
    }

    /// Use specific index
    pub fn use_index(mut self, index: IndexType) -> Self {
        self.action.index = Some(index);
        self
    }

    /// Set stale tolerance
    pub fn allow_stale(mut self, duration: Duration) -> Self {
        self.action.stale_tolerance_ms = Some(duration.as_millis() as u64);
        self
    }

    /// Check if this rule matches the query
    pub fn matches(&self, command: &str, args: &[Bytes], pattern: &QueryPattern) -> bool {
        if !self.enabled {
            return false;
        }

        // Check command pattern
        if let Some(ref cmd_pattern) = self.matcher.command_pattern {
            if !matches_glob(command, cmd_pattern) {
                return false;
            }
        }

        // Check key prefix
        if let Some(ref prefix) = self.matcher.key_prefix {
            let key = args.first().map(|b| String::from_utf8_lossy(b).to_string());
            if !key.as_ref().map(|k| k.starts_with(prefix)).unwrap_or(false) {
                return false;
            }
        }

        // Check key pattern (regex)
        if let Some(ref key_pattern) = self.matcher.key_pattern {
            let key = args.first().map(|b| String::from_utf8_lossy(b).to_string());
            if let Ok(re) = Regex::new(key_pattern) {
                if !key.as_ref().map(|k| re.is_match(k)).unwrap_or(false) {
                    return false;
                }
            }
        }

        // Check query type
        if let Some(ref query_types) = self.matcher.query_types {
            let type_str = format!("{:?}", pattern.query_type);
            if !query_types.contains(&type_str) {
                return false;
            }
        }

        // Check cost threshold
        if let Some(min_cost) = self.matcher.min_cost {
            if pattern.estimated_cost < min_cost {
                return false;
            }
        }

        true
    }

    /// Apply this rule to create a routing decision
    pub fn apply(&self) -> RouteDecision {
        let mut decision = RouteDecision::default();

        if let Some(ref target) = self.action.target {
            decision.target = target.clone();
        }

        if let Some(ref index) = self.action.index {
            decision.index = index.clone();
        }

        if let Some(ms) = self.action.stale_tolerance_ms {
            decision.stale_tolerance = Some(Duration::from_millis(ms));
        }

        if let Some(priority) = self.action.priority {
            decision.priority = priority;
        }

        decision
    }
}

/// Rule matching conditions
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RuleMatcher {
    /// Command pattern (glob)
    pub command_pattern: Option<String>,
    /// Key prefix to match
    pub key_prefix: Option<String>,
    /// Key pattern (regex)
    pub key_pattern: Option<String>,
    /// Query types to match
    pub query_types: Option<Vec<String>>,
    /// Minimum estimated cost
    pub min_cost: Option<u64>,
    /// Tags to match
    pub tags: Option<Vec<String>>,
}

/// Action to take when rule matches
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RuleAction {
    /// Target for routing
    pub target: Option<RouteTarget>,
    /// Index to use
    pub index: Option<IndexType>,
    /// Stale tolerance in ms
    pub stale_tolerance_ms: Option<u64>,
    /// Priority override
    pub priority: Option<u8>,
    /// Custom metadata
    pub metadata: Option<HashMap<String, String>>,
}

/// Collection of routing rules
#[derive(Debug, Default)]
pub struct RoutingRuleSet {
    /// Rules sorted by priority (highest first)
    rules: Vec<RoutingRule>,
}

impl RoutingRuleSet {
    /// Create a new rule set
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Add a rule
    pub fn add(&mut self, rule: RoutingRule) {
        self.rules.push(rule);
        self.rules.sort_by(|a, b| b.priority.cmp(&a.priority));
    }

    /// Remove a rule by ID
    pub fn remove(&mut self, id: &str) -> bool {
        let len_before = self.rules.len();
        self.rules.retain(|r| r.id != id);
        self.rules.len() < len_before
    }

    /// Get a rule by ID
    pub fn get(&self, id: &str) -> Option<&RoutingRule> {
        self.rules.iter().find(|r| r.id == id)
    }

    /// Get a mutable rule by ID
    pub fn get_mut(&mut self, id: &str) -> Option<&mut RoutingRule> {
        self.rules.iter_mut().find(|r| r.id == id)
    }

    /// Match a query against all rules
    pub fn match_query(
        &self,
        command: &str,
        args: &[Bytes],
        pattern: &QueryPattern,
    ) -> Option<RouteDecision> {
        for rule in &self.rules {
            if rule.matches(command, args, pattern) {
                return Some(rule.apply());
            }
        }
        None
    }

    /// Get all rules
    pub fn all(&self) -> &[RoutingRule] {
        &self.rules
    }

    /// Get enabled rules
    pub fn enabled(&self) -> impl Iterator<Item = &RoutingRule> {
        self.rules.iter().filter(|r| r.enabled)
    }

    /// Get rule count
    pub fn len(&self) -> usize {
        self.rules.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Clear all rules
    pub fn clear(&mut self) {
        self.rules.clear();
    }
}

/// Simple glob matching
fn matches_glob(s: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    if !pattern.contains('*') {
        return s.eq_ignore_ascii_case(pattern);
    }

    let parts: Vec<&str> = pattern.split('*').collect();

    if parts.len() == 2 {
        let prefix = parts[0];
        let suffix = parts[1];

        let matches_prefix =
            prefix.is_empty() || s.to_uppercase().starts_with(&prefix.to_uppercase());
        let matches_suffix =
            suffix.is_empty() || s.to_uppercase().ends_with(&suffix.to_uppercase());

        return matches_prefix && matches_suffix;
    }

    // Fallback for complex patterns
    s.to_uppercase()
        .contains(&pattern.replace('*', "").to_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::analyzer::QueryType;

    #[test]
    fn test_rule_creation() {
        let rule = RoutingRule::new("rule1", "Test Rule")
            .with_priority(10)
            .match_command("GET")
            .match_key_prefix("user:")
            .route_to(RouteTarget::AnyReplica);

        assert_eq!(rule.id, "rule1");
        assert_eq!(rule.priority, 10);
        assert!(rule.enabled);
    }

    #[test]
    fn test_rule_matching() {
        let rule = RoutingRule::new("r1", "Match user keys")
            .match_command("GET")
            .match_key_prefix("user:");

        let pattern = QueryPattern {
            command: "GET".to_string(),
            query_type: QueryType::PointLookup,
            is_read_only: true,
            estimated_cost: 1,
            key_count: 1,
            has_pattern: false,
            key_prefix: Some("user:".to_string()),
        };

        assert!(rule.matches("GET", &[Bytes::from("user:123")], &pattern));
        assert!(!rule.matches("GET", &[Bytes::from("session:123")], &pattern));
        assert!(!rule.matches("SET", &[Bytes::from("user:123")], &pattern));
    }

    #[test]
    fn test_rule_set() {
        let mut rules = RoutingRuleSet::new();

        rules.add(
            RoutingRule::new("r1", "Low priority")
                .with_priority(1)
                .match_command("*"),
        );

        rules.add(
            RoutingRule::new("r2", "High priority")
                .with_priority(10)
                .match_command("GET")
                .route_to(RouteTarget::AnyReplica),
        );

        // High priority rule should be first
        assert_eq!(rules.all()[0].id, "r2");
    }

    #[test]
    fn test_glob_matching() {
        assert!(matches_glob("GET", "GET"));
        assert!(matches_glob("GET", "*"));
        assert!(matches_glob("HGET", "H*"));
        assert!(matches_glob("HGETALL", "*ALL"));
        assert!(matches_glob("ZRANGEBYSCORE", "ZRANGE*"));
    }
}
