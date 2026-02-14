//! Data Retention Policies

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use super::GovernanceError;

/// Retention policy configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Policy ID
    pub id: String,
    /// Policy name
    pub name: String,
    /// Key patterns
    pub patterns: Vec<String>,
    /// Retention period
    pub retention_period: Duration,
    /// Action when expired
    pub expiry_action: RetentionAction,
    /// Enabled
    pub enabled: bool,
}

/// Retention rule
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetentionRule {
    /// Rule ID
    pub id: String,
    /// Key pattern
    pub pattern: String,
    /// Retention period in seconds
    pub retention_secs: u64,
    /// Action on expiry
    pub action: RetentionAction,
    /// Priority (higher = more specific)
    pub priority: u32,
}

/// Actions for expired data
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetentionAction {
    /// Delete the key
    Delete,
    /// Archive to cold storage
    Archive,
    /// Move to different tier
    MoveTier(String),
    /// Notify administrators
    Notify,
    /// Mark as expired (don't delete)
    Mark,
    /// No action (keep forever)
    None,
}

/// Retention engine
pub struct RetentionEngine {
    /// Retention rules
    rules: RwLock<Vec<RetentionRule>>,
    /// Default retention period
    default_retention: Duration,
    /// Default action
    default_action: RetentionAction,
}

impl RetentionEngine {
    /// Create a new retention engine
    pub fn new() -> Self {
        Self {
            rules: RwLock::new(Vec::new()),
            default_retention: Duration::from_secs(365 * 24 * 60 * 60), // 1 year
            default_action: RetentionAction::Delete,
        }
    }

    /// Add a retention rule
    pub fn add_rule(&self, rule: RetentionRule) -> Result<(), GovernanceError> {
        let mut rules = self.rules.write();
        rules.push(rule);
        rules.sort_by(|a, b| b.priority.cmp(&a.priority));
        Ok(())
    }

    /// Remove a retention rule
    pub fn remove_rule(&self, rule_id: &str) -> Result<(), GovernanceError> {
        self.rules.write().retain(|r| r.id != rule_id);
        Ok(())
    }

    /// Check retention for a key
    pub fn check(&self, key: &str, created_at: u64) -> RetentionAction {
        let rules = self.rules.read();

        for rule in rules.iter() {
            if self.matches_pattern(&rule.pattern, key) {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                let age = now.saturating_sub(created_at);

                if age > rule.retention_secs {
                    return rule.action.clone();
                } else {
                    return RetentionAction::None;
                }
            }
        }

        // Check against default retention
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let age = now.saturating_sub(created_at);

        if age > self.default_retention.as_secs() {
            self.default_action.clone()
        } else {
            RetentionAction::None
        }
    }

    /// Get retention period for a key
    pub fn get_retention_period(&self, key: &str) -> Duration {
        let rules = self.rules.read();

        for rule in rules.iter() {
            if self.matches_pattern(&rule.pattern, key) {
                return Duration::from_secs(rule.retention_secs);
            }
        }

        self.default_retention
    }

    /// Get all rules
    pub fn list_rules(&self) -> Vec<RetentionRule> {
        self.rules.read().clone()
    }

    /// Set default retention
    pub fn set_default_retention(&mut self, retention: Duration, action: RetentionAction) {
        self.default_retention = retention;
        self.default_action = action;
    }

    fn matches_pattern(&self, pattern: &str, key: &str) -> bool {
        if pattern == "*" {
            return true;
        }
        if let Some(prefix) = pattern.strip_suffix('*') {
            return key.starts_with(prefix);
        }
        pattern == key
    }
}

impl Default for RetentionEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_rule() {
        let engine = RetentionEngine::new();

        let rule = RetentionRule {
            id: "rule1".to_string(),
            pattern: "temp:*".to_string(),
            retention_secs: 86400, // 1 day
            action: RetentionAction::Delete,
            priority: 100,
        };

        engine.add_rule(rule).unwrap();

        let rules = engine.list_rules();
        assert_eq!(rules.len(), 1);
    }

    #[test]
    fn test_check_expired() {
        let engine = RetentionEngine::new();

        let rule = RetentionRule {
            id: "rule1".to_string(),
            pattern: "temp:*".to_string(),
            retention_secs: 3600, // 1 hour
            action: RetentionAction::Delete,
            priority: 100,
        };

        engine.add_rule(rule).unwrap();

        // Created 2 hours ago
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let created_at = now - 7200;

        let action = engine.check("temp:test", created_at);
        assert_eq!(action, RetentionAction::Delete);
    }

    #[test]
    fn test_check_not_expired() {
        let engine = RetentionEngine::new();

        let rule = RetentionRule {
            id: "rule1".to_string(),
            pattern: "temp:*".to_string(),
            retention_secs: 86400, // 1 day
            action: RetentionAction::Delete,
            priority: 100,
        };

        engine.add_rule(rule).unwrap();

        // Created just now
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let action = engine.check("temp:test", now);
        assert_eq!(action, RetentionAction::None);
    }

    #[test]
    fn test_get_retention_period() {
        let engine = RetentionEngine::new();

        let rule = RetentionRule {
            id: "rule1".to_string(),
            pattern: "session:*".to_string(),
            retention_secs: 3600,
            action: RetentionAction::Delete,
            priority: 100,
        };

        engine.add_rule(rule).unwrap();

        let period = engine.get_retention_period("session:123");
        assert_eq!(period, Duration::from_secs(3600));
    }
}
