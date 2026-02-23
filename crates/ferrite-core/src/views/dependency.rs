#![forbid(unsafe_code)]

//! Dependency tracking — maps key patterns to materialized views.

use dashmap::DashMap;
use std::sync::Arc;

/// Tracks which views depend on which key patterns.
#[derive(Debug, Clone)]
pub struct DependencyGraph {
    /// Maps pattern → set of view names that depend on it.
    pattern_to_views: Arc<DashMap<String, Vec<String>>>,
    /// Maps view name → its registered patterns.
    view_to_patterns: Arc<DashMap<String, Vec<String>>>,
}

impl DependencyGraph {
    /// Create a new empty dependency graph.
    pub fn new() -> Self {
        Self {
            pattern_to_views: Arc::new(DashMap::new()),
            view_to_patterns: Arc::new(DashMap::new()),
        }
    }

    /// Register dependency patterns for a view.
    pub fn register(&self, view_name: &str, patterns: &[String]) {
        self.view_to_patterns
            .insert(view_name.to_string(), patterns.to_vec());

        for pattern in patterns {
            self.pattern_to_views
                .entry(pattern.clone())
                .or_default()
                .push(view_name.to_string());
        }
    }

    /// Remove all dependencies for a view.
    pub fn unregister(&self, view_name: &str) {
        if let Some((_, patterns)) = self.view_to_patterns.remove(view_name) {
            for pattern in &patterns {
                if let Some(mut views) = self.pattern_to_views.get_mut(pattern) {
                    views.retain(|v| v != view_name);
                    if views.is_empty() {
                        drop(views);
                        self.pattern_to_views.remove(pattern);
                    }
                }
            }
        }
    }

    /// Find all views affected by a key change using glob-style matching.
    pub fn affected_views(&self, key: &str) -> Vec<String> {
        let mut result = Vec::new();

        for entry in self.pattern_to_views.iter() {
            let pattern = entry.key();
            if glob_match(pattern, key) {
                for view_name in entry.value().iter() {
                    if !result.contains(view_name) {
                        result.push(view_name.clone());
                    }
                }
            }
        }

        result
    }
}

impl Default for DependencyGraph {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple glob matching: supports `*` as a wildcard for any sequence of characters.
fn glob_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    let parts: Vec<&str> = pattern.split('*').collect();

    if parts.len() == 1 {
        return pattern == text;
    }

    let mut pos = 0;

    // First part must match at the start
    if let Some(first) = parts.first() {
        if !first.is_empty() {
            if !text.starts_with(first) {
                return false;
            }
            pos = first.len();
        }
    }

    // Last part must match at the end
    if let Some(last) = parts.last() {
        if !last.is_empty() && !text.ends_with(last) {
            return false;
        }
    }

    // Middle parts must appear in order
    for part in &parts[1..parts.len().saturating_sub(1)] {
        if part.is_empty() {
            continue;
        }
        if let Some(found) = text[pos..].find(part) {
            pos += found + part.len();
        } else {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_match_exact() {
        assert!(glob_match("users:123", "users:123"));
        assert!(!glob_match("users:123", "users:456"));
    }

    #[test]
    fn test_glob_match_wildcard() {
        assert!(glob_match("users:*", "users:123"));
        assert!(glob_match("users:*", "users:abc"));
        assert!(!glob_match("users:*", "orders:123"));
    }

    #[test]
    fn test_glob_match_star_only() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("*", ""));
    }

    #[test]
    fn test_glob_match_prefix_suffix() {
        assert!(glob_match("*:active", "users:active"));
        assert!(!glob_match("*:active", "users:inactive"));
    }

    #[test]
    fn test_dependency_graph_register_and_lookup() {
        let graph = DependencyGraph::new();
        graph.register("user_summary", &["users:*".to_string()]);
        graph.register("order_summary", &["orders:*".to_string()]);

        let affected = graph.affected_views("users:123");
        assert_eq!(affected, vec!["user_summary"]);

        let affected = graph.affected_views("orders:456");
        assert_eq!(affected, vec!["order_summary"]);

        let affected = graph.affected_views("other:key");
        assert!(affected.is_empty());
    }

    #[test]
    fn test_dependency_graph_unregister() {
        let graph = DependencyGraph::new();
        graph.register("view1", &["users:*".to_string()]);

        assert_eq!(graph.affected_views("users:123").len(), 1);

        graph.unregister("view1");
        assert!(graph.affected_views("users:123").is_empty());
    }

    #[test]
    fn test_multiple_views_same_pattern() {
        let graph = DependencyGraph::new();
        graph.register("view_a", &["users:*".to_string()]);
        graph.register("view_b", &["users:*".to_string()]);

        let affected = graph.affected_views("users:42");
        assert_eq!(affected.len(), 2);
        assert!(affected.contains(&"view_a".to_string()));
        assert!(affected.contains(&"view_b".to_string()));
    }
}
