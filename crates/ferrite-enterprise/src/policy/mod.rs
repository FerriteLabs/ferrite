//! Declarative Data Policies
//!
//! Define and enforce data governance policies declaratively.
//! Supports TTL, retention, access control, and compliance rules.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                   Data Policy Engine                         │
//! ├─────────────────────────────────────────────────────────────┤
//! │  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐ │
//! │  │  Policy  │   │  Rule    │   │ Enforce  │   │  Audit   │ │
//! │  │  Parser  │──▶│  Engine  │──▶│  Actions │──▶│   Log    │ │
//! │  └──────────┘   └──────────┘   └──────────┘   └──────────┘ │
//! │       │              │              │              │        │
//! │       ▼              ▼              ▼              ▼        │
//! │  YAML/JSON       Conditions     Auto-Expire    Compliance  │
//! │   Policies       Evaluation      Encrypt       Reporting   │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Features
//!
//! - **TTL Policies**: Automatic data expiration
//! - **Retention Policies**: Minimum retention periods
//! - **Access Policies**: Key-pattern based access control
//! - **Encryption Policies**: At-rest encryption requirements
//! - **Compliance Policies**: GDPR, HIPAA, PCI-DSS rules

mod actions;
mod audit;
mod engine;
mod rule;

pub use actions::{ActionExecutor, ActionResult};
pub use audit::{AuditEntry, AuditLevel, PolicyAuditLog};
pub use engine::{EvaluationResult, PolicyEngine, PolicyEvaluation};
pub use rule::{PolicyAction, PolicyMatch, PolicyRule, RuleCondition};

use std::collections::HashMap;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Policy type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyType {
    /// Time-to-live policy
    Ttl,
    /// Retention policy
    Retention,
    /// Access control policy
    Access,
    /// Encryption policy
    Encryption,
    /// Compliance policy
    Compliance,
    /// Custom policy
    Custom,
}

impl PolicyType {
    /// Get string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            PolicyType::Ttl => "ttl",
            PolicyType::Retention => "retention",
            PolicyType::Access => "access",
            PolicyType::Encryption => "encryption",
            PolicyType::Compliance => "compliance",
            PolicyType::Custom => "custom",
        }
    }
}

/// A data policy definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    /// Policy ID
    pub id: String,
    /// Policy name
    pub name: String,
    /// Policy type
    pub policy_type: PolicyType,
    /// Policy description
    pub description: String,
    /// Key patterns this policy applies to
    pub key_patterns: Vec<String>,
    /// Rules for this policy
    pub rules: Vec<PolicyRule>,
    /// Whether policy is enabled
    pub enabled: bool,
    /// Priority (higher = evaluated first)
    pub priority: i32,
    /// Creation timestamp
    pub created_at: u64,
    /// Last updated timestamp
    pub updated_at: u64,
    /// Metadata
    pub metadata: HashMap<String, String>,
}

impl Policy {
    /// Create a new policy
    pub fn new(name: impl Into<String>, policy_type: PolicyType) -> Self {
        let now = current_timestamp();
        let name = name.into();

        Self {
            id: generate_id(),
            name: name.clone(),
            policy_type,
            description: String::new(),
            key_patterns: vec!["*".to_string()],
            rules: Vec::new(),
            enabled: true,
            priority: 0,
            created_at: now,
            updated_at: now,
            metadata: HashMap::new(),
        }
    }

    /// Set description
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = desc.into();
        self
    }

    /// Set key patterns
    pub fn with_patterns(mut self, patterns: Vec<String>) -> Self {
        self.key_patterns = patterns;
        self
    }

    /// Add a rule
    pub fn add_rule(&mut self, rule: PolicyRule) {
        self.rules.push(rule);
        self.updated_at = current_timestamp();
    }

    /// Set priority
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Check if policy matches a key
    pub fn matches_key(&self, key: &str) -> bool {
        for pattern in &self.key_patterns {
            if matches_pattern(key, pattern) {
                return true;
            }
        }
        false
    }
}

/// Policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyConfig {
    /// Enable policy enforcement
    pub enabled: bool,
    /// Enable audit logging
    pub audit_enabled: bool,
    /// Default TTL in seconds (0 = no default)
    pub default_ttl: u64,
    /// Default retention in seconds
    pub default_retention: u64,
    /// Require encryption for matching patterns
    pub encryption_patterns: Vec<String>,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            audit_enabled: true,
            default_ttl: 0,
            default_retention: 0,
            encryption_patterns: vec![],
        }
    }
}

/// Policy manager
pub struct PolicyManager {
    config: PolicyConfig,
    policies: RwLock<HashMap<String, Policy>>,
    engine: PolicyEngine,
    audit_log: PolicyAuditLog,
}

impl PolicyManager {
    /// Create a new policy manager
    pub fn new(config: PolicyConfig) -> Self {
        Self {
            config,
            policies: RwLock::new(HashMap::new()),
            engine: PolicyEngine::new(),
            audit_log: PolicyAuditLog::new(),
        }
    }

    /// Create with default config
    pub fn with_defaults() -> Self {
        Self::new(PolicyConfig::default())
    }

    /// Register a policy
    pub fn register(&self, policy: Policy) -> Result<String, PolicyError> {
        if policy.name.is_empty() {
            return Err(PolicyError::InvalidPolicy("name is required".to_string()));
        }

        let id = policy.id.clone();
        self.policies.write().insert(id.clone(), policy);

        self.audit_log.log(
            AuditEntry::new(AuditLevel::Info, "policy_registered").with_detail("policy_id", &id),
        );

        Ok(id)
    }

    /// Unregister a policy
    pub fn unregister(&self, id: &str) -> Result<(), PolicyError> {
        let mut policies = self.policies.write();

        if policies.remove(id).is_some() {
            self.audit_log.log(
                AuditEntry::new(AuditLevel::Info, "policy_unregistered")
                    .with_detail("policy_id", id),
            );
            Ok(())
        } else {
            Err(PolicyError::PolicyNotFound(id.to_string()))
        }
    }

    /// Get a policy by ID
    pub fn get(&self, id: &str) -> Option<Policy> {
        self.policies.read().get(id).cloned()
    }

    /// List all policies
    pub fn list(&self) -> Vec<Policy> {
        self.policies.read().values().cloned().collect()
    }

    /// List policies by type
    pub fn list_by_type(&self, policy_type: PolicyType) -> Vec<Policy> {
        self.policies
            .read()
            .values()
            .filter(|p| p.policy_type == policy_type)
            .cloned()
            .collect()
    }

    /// Evaluate policies for a key and operation
    pub fn evaluate(&self, key: &str, operation: &str) -> PolicyEvaluation {
        if !self.config.enabled {
            return PolicyEvaluation::allow();
        }

        let policies = self.policies.read();

        // Get applicable policies, sorted by priority
        let mut applicable: Vec<&Policy> = policies
            .values()
            .filter(|p| p.enabled && p.matches_key(key))
            .collect();

        applicable.sort_by(|a, b| b.priority.cmp(&a.priority));

        // Evaluate each policy
        let mut evaluation = PolicyEvaluation::allow();

        for policy in applicable {
            let result = self.engine.evaluate(policy, key, operation);

            if self.config.audit_enabled {
                self.audit_log.log(
                    AuditEntry::new(
                        if result.allowed {
                            AuditLevel::Debug
                        } else {
                            AuditLevel::Warn
                        },
                        "policy_evaluated",
                    )
                    .with_detail("key", key)
                    .with_detail("operation", operation)
                    .with_detail("policy_id", &policy.id)
                    .with_detail("allowed", result.allowed.to_string()),
                );
            }

            if !result.allowed {
                evaluation = PolicyEvaluation {
                    allowed: false,
                    denial_reason: result.reason,
                    actions: result.actions,
                    matched_policies: vec![policy.id.clone()],
                };
                break; // First deny wins
            }

            // Merge actions
            evaluation.actions.extend(result.actions);
            evaluation.matched_policies.push(policy.id.clone());
        }

        evaluation
    }

    /// Get required TTL for a key
    pub fn get_required_ttl(&self, key: &str) -> Option<u64> {
        let policies = self.policies.read();

        for policy in policies.values() {
            if policy.policy_type == PolicyType::Ttl && policy.enabled && policy.matches_key(key) {
                for rule in &policy.rules {
                    if let Some(ttl) = rule.get_ttl() {
                        return Some(ttl);
                    }
                }
            }
        }

        if self.config.default_ttl > 0 {
            Some(self.config.default_ttl)
        } else {
            None
        }
    }

    /// Check if key requires encryption
    pub fn requires_encryption(&self, key: &str) -> bool {
        // Check encryption patterns in config
        for pattern in &self.config.encryption_patterns {
            if matches_pattern(key, pattern) {
                return true;
            }
        }

        // Check encryption policies
        let policies = self.policies.read();
        for policy in policies.values() {
            if policy.policy_type == PolicyType::Encryption
                && policy.enabled
                && policy.matches_key(key)
            {
                return true;
            }
        }

        false
    }

    /// Get audit log
    pub fn audit_log(&self) -> &PolicyAuditLog {
        &self.audit_log
    }

    /// Get statistics
    pub fn stats(&self) -> PolicyStats {
        let policies = self.policies.read();

        PolicyStats {
            total_policies: policies.len() as u64,
            enabled_policies: policies.values().filter(|p| p.enabled).count() as u64,
            ttl_policies: policies
                .values()
                .filter(|p| p.policy_type == PolicyType::Ttl)
                .count() as u64,
            access_policies: policies
                .values()
                .filter(|p| p.policy_type == PolicyType::Access)
                .count() as u64,
            encryption_policies: policies
                .values()
                .filter(|p| p.policy_type == PolicyType::Encryption)
                .count() as u64,
            audit_entries: self.audit_log.count(),
        }
    }

    /// Get configuration
    pub fn config(&self) -> &PolicyConfig {
        &self.config
    }
}

impl Default for PolicyManager {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Policy statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyStats {
    /// Total policies
    pub total_policies: u64,
    /// Enabled policies
    pub enabled_policies: u64,
    /// TTL policies
    pub ttl_policies: u64,
    /// Access policies
    pub access_policies: u64,
    /// Encryption policies
    pub encryption_policies: u64,
    /// Audit log entries
    pub audit_entries: u64,
}

/// Policy errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum PolicyError {
    /// Policy not found
    #[error("policy not found: {0}")]
    PolicyNotFound(String),

    /// Invalid policy
    #[error("invalid policy: {0}")]
    InvalidPolicy(String),

    /// Policy violation
    #[error("policy violation: {0}")]
    Violation(String),

    /// Evaluation error
    #[error("evaluation error: {0}")]
    EvaluationError(String),
}

/// Pattern matching helper
fn matches_pattern(key: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    if let Some(prefix) = pattern.strip_suffix('*') {
        return key.starts_with(prefix);
    }

    if let Some(suffix) = pattern.strip_prefix('*') {
        return key.ends_with(suffix);
    }

    key == pattern
}

/// Generate unique ID
fn generate_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    let random: u32 = rand::random();
    format!("pol_{:x}{:x}", timestamp, random)
}

/// Get current timestamp
fn current_timestamp() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_policy_creation() {
        let policy = Policy::new("test-ttl", PolicyType::Ttl)
            .with_description("Test TTL policy")
            .with_patterns(vec!["temp:*".to_string()]);

        assert_eq!(policy.name, "test-ttl");
        assert!(policy.matches_key("temp:123"));
        assert!(!policy.matches_key("perm:123"));
    }

    #[test]
    fn test_pattern_matching() {
        assert!(matches_pattern("users:123", "users:*"));
        assert!(matches_pattern("anything", "*"));
        assert!(matches_pattern("file.json", "*.json"));
        assert!(!matches_pattern("users:123", "orders:*"));
    }

    #[test]
    fn test_policy_manager() {
        let manager = PolicyManager::with_defaults();

        let policy = Policy::new("test", PolicyType::Ttl).with_patterns(vec!["test:*".to_string()]);

        let id = manager.register(policy).unwrap();
        assert!(manager.get(&id).is_some());

        let stats = manager.stats();
        assert_eq!(stats.total_policies, 1);
    }
}
