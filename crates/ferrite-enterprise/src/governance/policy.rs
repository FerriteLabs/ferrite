//! Policy Engine

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{GovernanceError, MaskingRule, OperationType, RequestContext};

/// Policy definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Policy {
    /// Unique policy ID
    pub id: String,
    /// Policy name
    pub name: String,
    /// Policy description
    pub description: Option<String>,
    /// Policy priority (higher = evaluated first)
    pub priority: u32,
    /// Policy enabled
    pub enabled: bool,
    /// Policy rules
    pub rules: Vec<PolicyRule>,
    /// Default action if no rules match
    pub default_action: PolicyAction,
    /// Policy metadata
    pub metadata: PolicyMetadata,
}

/// Policy rule
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PolicyRule {
    /// Rule ID
    pub id: String,
    /// Conditions to match
    pub conditions: Vec<PolicyCondition>,
    /// Action to take
    pub action: PolicyAction,
}

/// Policy condition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PolicyCondition {
    /// Match key pattern
    KeyPattern(String),
    /// Match user ID
    UserId(String),
    /// Match role
    Role(String),
    /// Match operation type
    Operation(OperationType),
    /// Match client IP
    ClientIp(String),
    /// Match attribute
    Attribute { key: String, value: String },
    /// Time-based condition
    TimeRange { start_hour: u8, end_hour: u8 },
    /// All conditions must match
    And(Vec<PolicyCondition>),
    /// Any condition must match
    Or(Vec<PolicyCondition>),
    /// Negate condition
    Not(Box<PolicyCondition>),
}

/// Policy action
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PolicyAction {
    /// Allow access
    Allow,
    /// Deny access
    Deny,
    /// Apply masking
    Mask(MaskingRule),
    /// Warn but allow
    Warn(String),
    /// Audit the access
    Audit,
    /// Send notification
    Notify(String),
}

/// Policy metadata
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PolicyMetadata {
    /// Created timestamp
    pub created_at: u64,
    /// Updated timestamp
    pub updated_at: u64,
    /// Created by
    pub created_by: Option<String>,
    /// Tags
    pub tags: Vec<String>,
    /// Version
    pub version: u32,
}

/// Access policy (simplified policy for access control)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccessPolicy {
    /// Policy ID
    pub id: String,
    /// Allowed operations
    pub allowed_operations: Vec<OperationType>,
    /// Key patterns
    pub key_patterns: Vec<String>,
    /// Allowed roles
    pub allowed_roles: Vec<String>,
}

/// Data policy (simplified policy for data handling)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataPolicy {
    /// Policy ID
    pub id: String,
    /// Key patterns
    pub key_patterns: Vec<String>,
    /// Masking rule
    pub masking: Option<MaskingRule>,
    /// Retention days
    pub retention_days: Option<u32>,
}

/// Retention policy
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Policy ID
    pub id: String,
    /// Key patterns
    pub key_patterns: Vec<String>,
    /// Retention period in seconds
    pub retention_secs: u64,
    /// Action when expired
    pub expiry_action: ExpiryAction,
}

/// Expiry action
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ExpiryAction {
    /// Delete the key
    Delete,
    /// Archive the key
    Archive,
    /// Move to cold storage
    MoveToCold,
    /// Notify only
    NotifyOnly,
}

/// Policy evaluation result
#[derive(Clone, Debug)]
pub struct PolicyEvaluation {
    /// Matched policies
    pub matches: Vec<PolicyMatch>,
    /// Evaluation time (ns)
    pub eval_time_ns: u64,
}

/// Single policy match
#[derive(Clone, Debug)]
pub struct PolicyMatch {
    /// Policy ID
    pub policy_id: String,
    /// Rule ID
    pub rule_id: String,
    /// Action
    pub action: PolicyAction,
    /// Match reason
    pub reason: String,
}

/// Policy engine
pub struct PolicyEngine {
    /// Policies by ID
    policies: RwLock<HashMap<String, Policy>>,
}

impl PolicyEngine {
    /// Create a new policy engine
    pub fn new() -> Self {
        Self {
            policies: RwLock::new(HashMap::new()),
        }
    }

    /// Add a policy
    pub fn add_policy(&self, policy: Policy) -> Result<(), GovernanceError> {
        let id = policy.id.clone();
        if self.policies.read().contains_key(&id) {
            return Err(GovernanceError::PolicyExists(id));
        }
        self.policies.write().insert(id, policy);
        Ok(())
    }

    /// Remove a policy
    pub fn remove_policy(&self, policy_id: &str) -> Result<(), GovernanceError> {
        if self.policies.write().remove(policy_id).is_none() {
            return Err(GovernanceError::PolicyNotFound(policy_id.to_string()));
        }
        Ok(())
    }

    /// List all policies
    pub fn list_policies(&self) -> Vec<Policy> {
        self.policies.read().values().cloned().collect()
    }

    /// Evaluate policies for a request
    pub fn evaluate(&self, context: &RequestContext) -> PolicyEvaluation {
        let start = std::time::Instant::now();
        let mut matches = Vec::new();

        // Sort policies by priority
        let mut policies: Vec<_> = self.policies.read().values().cloned().collect();
        policies.sort_by(|a, b| b.priority.cmp(&a.priority));

        for policy in &policies {
            if !policy.enabled {
                continue;
            }

            for rule in &policy.rules {
                if self.evaluate_conditions(&rule.conditions, context) {
                    matches.push(PolicyMatch {
                        policy_id: policy.id.clone(),
                        rule_id: rule.id.clone(),
                        action: rule.action.clone(),
                        reason: format!("Matched rule {} in policy {}", rule.id, policy.id),
                    });
                }
            }
        }

        PolicyEvaluation {
            matches,
            eval_time_ns: start.elapsed().as_nanos() as u64,
        }
    }

    fn evaluate_conditions(
        &self,
        conditions: &[PolicyCondition],
        context: &RequestContext,
    ) -> bool {
        conditions
            .iter()
            .all(|c| self.evaluate_condition(c, context))
    }

    fn evaluate_condition(&self, condition: &PolicyCondition, context: &RequestContext) -> bool {
        match condition {
            PolicyCondition::KeyPattern(pattern) => {
                if let Some(ref key) = context.key {
                    self.matches_pattern(pattern, key)
                } else {
                    false
                }
            }
            PolicyCondition::UserId(id) => context.user_id.as_ref() == Some(id),
            PolicyCondition::Role(role) => context.roles.contains(role),
            PolicyCondition::Operation(op) => &context.operation == op,
            PolicyCondition::ClientIp(ip) => context.client_ip.as_ref() == Some(ip),
            PolicyCondition::Attribute { key, value } => context.attributes.get(key) == Some(value),
            PolicyCondition::TimeRange {
                start_hour,
                end_hour,
            } => {
                let hour = chrono::Local::now().hour() as u8;
                hour >= *start_hour && hour < *end_hour
            }
            PolicyCondition::And(conditions) => conditions
                .iter()
                .all(|c| self.evaluate_condition(c, context)),
            PolicyCondition::Or(conditions) => conditions
                .iter()
                .any(|c| self.evaluate_condition(c, context)),
            PolicyCondition::Not(inner) => !self.evaluate_condition(inner, context),
        }
    }

    fn matches_pattern(&self, pattern: &str, key: &str) -> bool {
        if pattern == "*" {
            return true;
        }
        if let Some(prefix) = pattern.strip_suffix('*') {
            return key.starts_with(prefix);
        }
        if let Some(suffix) = pattern.strip_prefix('*') {
            return key.ends_with(suffix);
        }
        pattern == key
    }
}

impl Default for PolicyEngine {
    fn default() -> Self {
        Self::new()
    }
}

// chrono types for time-based conditions
mod chrono {
    pub struct Local;

    impl Local {
        pub fn now() -> DateTime {
            let secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            DateTime { secs }
        }
    }

    pub struct DateTime {
        secs: u64,
    }

    impl DateTime {
        pub fn hour(&self) -> u32 {
            // Simplified: extract hour from unix timestamp
            ((self.secs % 86400) / 3600) as u32
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_policy() -> Policy {
        Policy {
            id: "test-policy".to_string(),
            name: "Test Policy".to_string(),
            description: Some("A test policy".to_string()),
            priority: 100,
            enabled: true,
            rules: vec![PolicyRule {
                id: "rule-1".to_string(),
                conditions: vec![PolicyCondition::KeyPattern("user:*".to_string())],
                action: PolicyAction::Allow,
            }],
            default_action: PolicyAction::Deny,
            metadata: PolicyMetadata::default(),
        }
    }

    #[test]
    fn test_add_policy() {
        let engine = PolicyEngine::new();
        let policy = create_test_policy();

        engine.add_policy(policy.clone()).unwrap();

        let policies = engine.list_policies();
        assert_eq!(policies.len(), 1);
        assert_eq!(policies[0].id, "test-policy");
    }

    #[test]
    fn test_evaluate_match() {
        let engine = PolicyEngine::new();
        let policy = create_test_policy();
        engine.add_policy(policy).unwrap();

        let context = RequestContext {
            user_id: Some("user1".to_string()),
            roles: vec![],
            client_ip: None,
            operation: OperationType::Read,
            key: Some("user:123".to_string()),
            pattern: None,
            attributes: HashMap::new(),
        };

        let result = engine.evaluate(&context);
        assert_eq!(result.matches.len(), 1);
    }

    #[test]
    fn test_evaluate_no_match() {
        let engine = PolicyEngine::new();
        let policy = create_test_policy();
        engine.add_policy(policy).unwrap();

        let context = RequestContext {
            user_id: Some("user1".to_string()),
            roles: vec![],
            client_ip: None,
            operation: OperationType::Read,
            key: Some("other:123".to_string()),
            pattern: None,
            attributes: HashMap::new(),
        };

        let result = engine.evaluate(&context);
        assert!(result.matches.is_empty());
    }
}
