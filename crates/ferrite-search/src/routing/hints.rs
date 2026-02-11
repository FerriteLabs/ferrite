//! Query hints for routing control

use super::IndexType;
use std::time::Duration;

/// Hints to guide routing decisions
#[derive(Clone, Debug)]
pub enum QueryHint {
    /// Prefer routing to primary
    PreferPrimary,
    /// Prefer routing to replica
    PreferReplica,
    /// Prefer local node
    PreferLocal,
    /// Force a specific index
    ForceIndex(IndexType),
    /// Allow stale reads up to duration
    AllowStale(Duration),
    /// Require fresh data (no stale reads)
    RequireFresh,
    /// Target a specific node
    TargetNode(String),
    /// Timeout for the query
    Timeout(Duration),
    /// Priority level (1-10)
    Priority(u8),
    /// Retry count
    RetryCount(u8),
    /// No caching
    NoCache,
}

/// Collection of routing hints
#[derive(Clone, Debug, Default)]
pub struct RoutingHints {
    hints: Vec<QueryHint>,
}

impl RoutingHints {
    /// Create empty hints
    pub fn new() -> Self {
        Self { hints: Vec::new() }
    }

    /// Add a hint
    #[allow(clippy::should_implement_trait)]
    pub fn add(mut self, hint: QueryHint) -> Self {
        self.hints.push(hint);
        self
    }

    /// Prefer primary
    pub fn prefer_primary(self) -> Self {
        self.add(QueryHint::PreferPrimary)
    }

    /// Prefer replica
    pub fn prefer_replica(self) -> Self {
        self.add(QueryHint::PreferReplica)
    }

    /// Prefer local
    pub fn prefer_local(self) -> Self {
        self.add(QueryHint::PreferLocal)
    }

    /// Force index
    pub fn force_index(self, index: IndexType) -> Self {
        self.add(QueryHint::ForceIndex(index))
    }

    /// Allow stale reads
    pub fn allow_stale(self, duration: Duration) -> Self {
        self.add(QueryHint::AllowStale(duration))
    }

    /// Require fresh data
    pub fn require_fresh(self) -> Self {
        self.add(QueryHint::RequireFresh)
    }

    /// Target specific node
    pub fn target_node(self, node_id: impl Into<String>) -> Self {
        self.add(QueryHint::TargetNode(node_id.into()))
    }

    /// Set timeout
    pub fn timeout(self, duration: Duration) -> Self {
        self.add(QueryHint::Timeout(duration))
    }

    /// Set priority
    pub fn priority(self, level: u8) -> Self {
        self.add(QueryHint::Priority(level))
    }

    /// Set retry count
    pub fn retries(self, count: u8) -> Self {
        self.add(QueryHint::RetryCount(count))
    }

    /// Disable caching
    pub fn no_cache(self) -> Self {
        self.add(QueryHint::NoCache)
    }

    /// Get all hints
    pub fn iter(&self) -> impl Iterator<Item = &QueryHint> {
        self.hints.iter()
    }

    /// Convert to slice
    pub fn as_slice(&self) -> &[QueryHint] {
        &self.hints
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.hints.is_empty()
    }

    /// Get hint count
    pub fn len(&self) -> usize {
        self.hints.len()
    }

    /// Check for specific hint type
    pub fn has_prefer_primary(&self) -> bool {
        self.hints
            .iter()
            .any(|h| matches!(h, QueryHint::PreferPrimary))
    }

    pub fn has_prefer_replica(&self) -> bool {
        self.hints
            .iter()
            .any(|h| matches!(h, QueryHint::PreferReplica))
    }

    pub fn has_require_fresh(&self) -> bool {
        self.hints
            .iter()
            .any(|h| matches!(h, QueryHint::RequireFresh))
    }

    pub fn get_timeout(&self) -> Option<Duration> {
        self.hints.iter().find_map(|h| {
            if let QueryHint::Timeout(d) = h {
                Some(*d)
            } else {
                None
            }
        })
    }

    pub fn get_stale_tolerance(&self) -> Option<Duration> {
        self.hints.iter().find_map(|h| {
            if let QueryHint::AllowStale(d) = h {
                Some(*d)
            } else {
                None
            }
        })
    }

    pub fn get_priority(&self) -> Option<u8> {
        self.hints.iter().find_map(|h| {
            if let QueryHint::Priority(p) = h {
                Some(*p)
            } else {
                None
            }
        })
    }

    pub fn get_target_node(&self) -> Option<&str> {
        self.hints.iter().find_map(|h| {
            if let QueryHint::TargetNode(n) = h {
                Some(n.as_str())
            } else {
                None
            }
        })
    }

    pub fn get_forced_index(&self) -> Option<&IndexType> {
        self.hints.iter().find_map(|h| {
            if let QueryHint::ForceIndex(idx) = h {
                Some(idx)
            } else {
                None
            }
        })
    }
}

impl From<Vec<QueryHint>> for RoutingHints {
    fn from(hints: Vec<QueryHint>) -> Self {
        Self { hints }
    }
}

impl IntoIterator for RoutingHints {
    type Item = QueryHint;
    type IntoIter = std::vec::IntoIter<QueryHint>;

    fn into_iter(self) -> Self::IntoIter {
        self.hints.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hints_builder() {
        let hints = RoutingHints::new()
            .prefer_replica()
            .allow_stale(Duration::from_secs(5))
            .priority(8);

        assert_eq!(hints.len(), 3);
        assert!(hints.has_prefer_replica());
        assert_eq!(hints.get_stale_tolerance(), Some(Duration::from_secs(5)));
        assert_eq!(hints.get_priority(), Some(8));
    }

    #[test]
    fn test_hints_iteration() {
        let hints = RoutingHints::new()
            .prefer_primary()
            .timeout(Duration::from_secs(10));

        let count = hints.iter().count();
        assert_eq!(count, 2);
    }
}
