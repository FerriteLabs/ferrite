//! Policy evaluation engine

use serde::{Deserialize, Serialize};

use super::rule::{PolicyAction, RuleContext};
use super::Policy;

/// Policy evaluation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyEvaluation {
    /// Whether the operation is allowed
    pub allowed: bool,
    /// Denial reason (if not allowed)
    pub denial_reason: Option<String>,
    /// Actions to execute
    pub actions: Vec<PolicyAction>,
    /// Matched policies
    pub matched_policies: Vec<String>,
}

impl PolicyEvaluation {
    /// Create an allow evaluation
    pub fn allow() -> Self {
        Self {
            allowed: true,
            denial_reason: None,
            actions: Vec::new(),
            matched_policies: Vec::new(),
        }
    }

    /// Create a deny evaluation
    pub fn deny(reason: impl Into<String>) -> Self {
        Self {
            allowed: false,
            denial_reason: Some(reason.into()),
            actions: Vec::new(),
            matched_policies: Vec::new(),
        }
    }

    /// Add an action
    pub fn with_action(mut self, action: PolicyAction) -> Self {
        self.actions.push(action);
        self
    }

    /// Add matched policy
    pub fn with_policy(mut self, policy_id: impl Into<String>) -> Self {
        self.matched_policies.push(policy_id.into());
        self
    }
}

/// Result of evaluating a single policy
#[derive(Debug, Clone)]
pub struct EvaluationResult {
    /// Whether allowed
    pub allowed: bool,
    /// Reason if denied
    pub reason: Option<String>,
    /// Actions to take
    pub actions: Vec<PolicyAction>,
}

impl EvaluationResult {
    /// Create allow result
    pub fn allow() -> Self {
        Self {
            allowed: true,
            reason: None,
            actions: Vec::new(),
        }
    }

    /// Create deny result
    pub fn deny(reason: impl Into<String>) -> Self {
        Self {
            allowed: false,
            reason: Some(reason.into()),
            actions: Vec::new(),
        }
    }

    /// Add actions
    pub fn with_actions(mut self, actions: Vec<PolicyAction>) -> Self {
        self.actions = actions;
        self
    }
}

/// Policy evaluation engine
pub struct PolicyEngine {
    /// Evaluation statistics
    evaluations: std::sync::atomic::AtomicU64,
    denials: std::sync::atomic::AtomicU64,
}

impl PolicyEngine {
    /// Create a new policy engine
    pub fn new() -> Self {
        Self {
            evaluations: std::sync::atomic::AtomicU64::new(0),
            denials: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Evaluate a policy for a key and operation
    pub fn evaluate(&self, policy: &Policy, key: &str, operation: &str) -> EvaluationResult {
        use std::sync::atomic::Ordering;

        self.evaluations.fetch_add(1, Ordering::Relaxed);

        if !policy.enabled {
            return EvaluationResult::allow();
        }

        // Create evaluation context
        let context = RuleContext::new(key, operation);

        let mut all_actions = Vec::new();

        // Evaluate each rule
        for rule in &policy.rules {
            if rule.evaluate(&context) {
                // Rule matched - check for deny actions
                for action in &rule.actions {
                    if let PolicyAction::Deny(reason) = action {
                        self.denials.fetch_add(1, Ordering::Relaxed);
                        return EvaluationResult::deny(reason);
                    }
                    all_actions.push(action.clone());
                }

                if rule.terminal {
                    break;
                }
            }
        }

        EvaluationResult::allow().with_actions(all_actions)
    }

    /// Evaluate with full context
    pub fn evaluate_with_context(
        &self,
        policy: &Policy,
        context: &RuleContext,
    ) -> EvaluationResult {
        use std::sync::atomic::Ordering;

        self.evaluations.fetch_add(1, Ordering::Relaxed);

        if !policy.enabled {
            return EvaluationResult::allow();
        }

        let mut all_actions = Vec::new();

        for rule in &policy.rules {
            if rule.evaluate(context) {
                for action in &rule.actions {
                    if let PolicyAction::Deny(reason) = action {
                        self.denials.fetch_add(1, Ordering::Relaxed);
                        return EvaluationResult::deny(reason);
                    }
                    all_actions.push(action.clone());
                }

                if rule.terminal {
                    break;
                }
            }
        }

        EvaluationResult::allow().with_actions(all_actions)
    }

    /// Get evaluation count
    pub fn evaluation_count(&self) -> u64 {
        use std::sync::atomic::Ordering;
        self.evaluations.load(Ordering::Relaxed)
    }

    /// Get denial count
    pub fn denial_count(&self) -> u64 {
        use std::sync::atomic::Ordering;
        self.denials.load(Ordering::Relaxed)
    }

    /// Reset statistics
    pub fn reset_stats(&self) {
        use std::sync::atomic::Ordering;
        self.evaluations.store(0, Ordering::Relaxed);
        self.denials.store(0, Ordering::Relaxed);
    }
}

impl Default for PolicyEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::rule::{PolicyRule, RuleCondition};
    use crate::policy::PolicyType;

    #[test]
    fn test_engine_allow() {
        let engine = PolicyEngine::new();

        let policy = Policy::new("test", PolicyType::Access);

        let result = engine.evaluate(&policy, "users:123", "GET");
        assert!(result.allowed);
    }

    #[test]
    fn test_engine_deny() {
        let engine = PolicyEngine::new();

        let mut policy = Policy::new("test", PolicyType::Access);
        policy.add_rule(
            PolicyRule::new("deny-writes")
                .with_condition(RuleCondition::OperationIn(vec![
                    "SET".to_string(),
                    "DEL".to_string(),
                ]))
                .with_action(PolicyAction::Deny("writes not allowed".to_string())),
        );

        let result = engine.evaluate(&policy, "users:123", "SET");
        assert!(!result.allowed);
        assert_eq!(result.reason, Some("writes not allowed".to_string()));
    }

    #[test]
    fn test_engine_stats() {
        let engine = PolicyEngine::new();
        let policy = Policy::new("test", PolicyType::Access);

        engine.evaluate(&policy, "key1", "GET");
        engine.evaluate(&policy, "key2", "SET");

        assert_eq!(engine.evaluation_count(), 2);
    }
}
