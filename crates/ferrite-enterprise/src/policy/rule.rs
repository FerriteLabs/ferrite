//! Policy rules

use serde::{Deserialize, Serialize};

/// A policy rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyRule {
    /// Rule name
    pub name: String,
    /// Conditions that must be met
    pub conditions: Vec<RuleCondition>,
    /// Actions to take when conditions are met
    pub actions: Vec<PolicyAction>,
    /// Whether to stop processing on match
    pub terminal: bool,
}

impl PolicyRule {
    /// Create a new rule
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            conditions: Vec::new(),
            actions: Vec::new(),
            terminal: false,
        }
    }

    /// Add a condition
    pub fn with_condition(mut self, condition: RuleCondition) -> Self {
        self.conditions.push(condition);
        self
    }

    /// Add an action
    pub fn with_action(mut self, action: PolicyAction) -> Self {
        self.actions.push(action);
        self
    }

    /// Set as terminal (stop processing)
    pub fn terminal(mut self) -> Self {
        self.terminal = true;
        self
    }

    /// Evaluate conditions
    pub fn evaluate(&self, context: &RuleContext) -> bool {
        if self.conditions.is_empty() {
            return true; // No conditions = always match
        }

        self.conditions.iter().all(|c| c.evaluate(context))
    }

    /// Get TTL if this is a TTL rule
    pub fn get_ttl(&self) -> Option<u64> {
        for action in &self.actions {
            if let PolicyAction::SetTtl(ttl) = action {
                return Some(*ttl);
            }
        }
        None
    }
}

/// A condition for a rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RuleCondition {
    /// Key matches pattern
    KeyMatches(String),
    /// Value size is greater than
    ValueSizeGt(u64),
    /// Value size is less than
    ValueSizeLt(u64),
    /// Operation is one of
    OperationIn(Vec<String>),
    /// Time is after
    TimeAfter(u64),
    /// Time is before
    TimeBefore(u64),
    /// User is in list
    UserIn(Vec<String>),
    /// Tag matches
    TagMatches(String, String),
    /// Custom condition
    Custom(String),
    /// All conditions must match
    And(Vec<RuleCondition>),
    /// Any condition must match
    Or(Vec<RuleCondition>),
    /// Negate condition
    Not(Box<RuleCondition>),
}

impl RuleCondition {
    /// Evaluate condition against context
    pub fn evaluate(&self, context: &RuleContext) -> bool {
        match self {
            RuleCondition::KeyMatches(pattern) => super::matches_pattern(&context.key, pattern),
            RuleCondition::ValueSizeGt(size) => context.value_size > *size,
            RuleCondition::ValueSizeLt(size) => context.value_size < *size,
            RuleCondition::OperationIn(ops) => ops
                .iter()
                .any(|o| o.eq_ignore_ascii_case(&context.operation)),
            RuleCondition::TimeAfter(ts) => context.timestamp > *ts,
            RuleCondition::TimeBefore(ts) => context.timestamp < *ts,
            RuleCondition::UserIn(users) => context
                .user
                .as_ref()
                .map(|u| users.contains(u))
                .unwrap_or(false),
            RuleCondition::TagMatches(tag, value) => {
                context.tags.get(tag).map(|v| v == value).unwrap_or(false)
            }
            RuleCondition::Custom(_) => {
                true // Custom conditions need external evaluation
            }
            RuleCondition::And(conditions) => conditions.iter().all(|c| c.evaluate(context)),
            RuleCondition::Or(conditions) => conditions.iter().any(|c| c.evaluate(context)),
            RuleCondition::Not(condition) => !condition.evaluate(context),
        }
    }
}

/// Context for rule evaluation
#[derive(Debug, Clone, Default)]
pub struct RuleContext {
    /// Key being accessed
    pub key: String,
    /// Operation being performed
    pub operation: String,
    /// Value size (if applicable)
    pub value_size: u64,
    /// Current timestamp
    pub timestamp: u64,
    /// User performing operation
    pub user: Option<String>,
    /// Tags/metadata
    pub tags: std::collections::HashMap<String, String>,
}

impl RuleContext {
    /// Create a new context
    pub fn new(key: impl Into<String>, operation: impl Into<String>) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};

        Self {
            key: key.into(),
            operation: operation.into(),
            value_size: 0,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            user: None,
            tags: std::collections::HashMap::new(),
        }
    }

    /// Set value size
    pub fn with_size(mut self, size: u64) -> Self {
        self.value_size = size;
        self
    }

    /// Set user
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// Add tag
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }
}

/// Action to take when policy matches
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PolicyAction {
    /// Allow the operation
    Allow,
    /// Deny the operation
    Deny(String),
    /// Set TTL (in seconds)
    SetTtl(u64),
    /// Require encryption
    RequireEncryption,
    /// Log the operation
    Log(String),
    /// Notify (webhook, email, etc.)
    Notify(String),
    /// Redact value
    Redact,
    /// Mask value
    Mask(String),
    /// Rate limit
    RateLimit(u32),
    /// Custom action
    Custom(String),
}

impl PolicyAction {
    /// Check if this is a deny action
    pub fn is_deny(&self) -> bool {
        matches!(self, PolicyAction::Deny(_))
    }

    /// Get action name
    pub fn name(&self) -> &'static str {
        match self {
            PolicyAction::Allow => "allow",
            PolicyAction::Deny(_) => "deny",
            PolicyAction::SetTtl(_) => "set_ttl",
            PolicyAction::RequireEncryption => "require_encryption",
            PolicyAction::Log(_) => "log",
            PolicyAction::Notify(_) => "notify",
            PolicyAction::Redact => "redact",
            PolicyAction::Mask(_) => "mask",
            PolicyAction::RateLimit(_) => "rate_limit",
            PolicyAction::Custom(_) => "custom",
        }
    }
}

/// Result of policy matching
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyMatch {
    /// Policy ID that matched
    pub policy_id: String,
    /// Rule name that matched
    pub rule_name: String,
    /// Actions to execute
    pub actions: Vec<PolicyAction>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rule_creation() {
        let rule = PolicyRule::new("test-rule")
            .with_condition(RuleCondition::KeyMatches("users:*".to_string()))
            .with_action(PolicyAction::SetTtl(3600));

        assert_eq!(rule.name, "test-rule");
        assert_eq!(rule.get_ttl(), Some(3600));
    }

    #[test]
    fn test_condition_evaluation() {
        let context = RuleContext::new("users:123", "GET");

        assert!(RuleCondition::KeyMatches("users:*".to_string()).evaluate(&context));
        assert!(!RuleCondition::KeyMatches("orders:*".to_string()).evaluate(&context));
    }

    #[test]
    fn test_complex_conditions() {
        let context = RuleContext::new("users:123", "SET").with_size(1000);

        let condition = RuleCondition::And(vec![
            RuleCondition::KeyMatches("users:*".to_string()),
            RuleCondition::ValueSizeGt(500),
        ]);

        assert!(condition.evaluate(&context));

        let or_condition = RuleCondition::Or(vec![
            RuleCondition::KeyMatches("orders:*".to_string()),
            RuleCondition::ValueSizeGt(500),
        ]);

        assert!(or_condition.evaluate(&context));
    }
}
