//! Programmable Access Control Policy Engine
//!
//! Policy-as-code for fine-grained, context-aware access control.
//! Rules evaluate at request time using key patterns, time, client
//! attributes, data classification, and tenant context.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the policy engine.
#[derive(Debug, Clone)]
pub struct PolicyConfig {
    /// Whether the policy engine is enabled.
    pub enabled: bool,
    /// Maximum number of policies allowed.
    pub max_policies: usize,
    /// Default action when no policy matches.
    pub default_action: PolicyAction,
    /// Cache evaluation results.
    pub cache_evaluations: bool,
    /// Time-to-live for cached evaluations.
    pub cache_ttl: Duration,
    /// Log all evaluations for audit.
    pub audit_all: bool,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_policies: 1000,
            default_action: PolicyAction::Allow,
            cache_evaluations: true,
            cache_ttl: Duration::from_secs(60),
            audit_all: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Policy types
// ---------------------------------------------------------------------------

/// Action a policy can take.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PolicyAction {
    /// Allow the operation.
    Allow,
    /// Deny the operation.
    Deny,
    /// Allow but record in audit log.
    AllowWithAudit,
    /// Deny but only emit a warning (soft deny).
    DenyWithWarning,
}

impl PolicyAction {
    /// Parse from a string (case-insensitive).
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "ALLOW" => Some(Self::Allow),
            "DENY" => Some(Self::Deny),
            "ALLOWWITHAUDIT" | "ALLOW_WITH_AUDIT" => Some(Self::AllowWithAudit),
            "DENYWITHWARNING" | "DENY_WITH_WARNING" => Some(Self::DenyWithWarning),
            _ => None,
        }
    }

    /// Display name.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Allow => "ALLOW",
            Self::Deny => "DENY",
            Self::AllowWithAudit => "ALLOW_WITH_AUDIT",
            Self::DenyWithWarning => "DENY_WITH_WARNING",
        }
    }

    /// Whether this action permits the operation.
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allow | Self::AllowWithAudit)
    }
}

/// Scope that a policy effect applies to.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EffectScope {
    /// Commands this policy applies to (glob patterns).
    pub commands: Vec<String>,
    /// Key patterns this policy applies to.
    pub key_patterns: Vec<String>,
    /// Users this policy applies to.
    pub users: Vec<String>,
    /// Tenants this policy applies to.
    pub tenants: Vec<String>,
}

/// A single access control policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    /// Unique policy name.
    pub name: String,
    /// Human-readable description.
    pub description: String,
    /// Priority (higher = evaluated first).
    pub priority: i32,
    /// Action to take when conditions match.
    pub action: PolicyAction,
    /// Conditions that must all be true.
    pub conditions: Vec<PolicyCondition>,
    /// Scope of the policy effect.
    pub effect_scope: EffectScope,
    /// Whether this policy is active.
    pub enabled: bool,
    /// Unix timestamp of creation.
    pub created_at: u64,
    /// Schema version for migration.
    pub version: u32,
}

/// A condition that must be satisfied for a policy to match.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyCondition {
    /// The field to evaluate.
    pub field: ConditionField,
    /// The comparison operator.
    pub operator: ConditionOp,
    /// The value to compare against.
    pub value: serde_json::Value,
}

/// Fields available for condition evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ConditionField {
    /// Hour of day (0–23).
    TimeHour,
    /// Day of week (0=Sun … 6=Sat).
    TimeDay,
    /// Client IP address.
    ClientIp,
    /// Client tenant identifier.
    ClientTenant,
    /// Client user name.
    ClientUser,
    /// Classification tag on the key.
    KeyClassification,
    /// Sensitivity level of the data.
    DataSensitivity,
    /// Category of the command (read, write, admin).
    CommandCategory,
    /// Age of the key in seconds.
    KeyAge,
    /// Size of the value in bytes.
    ValueSize,
}

impl ConditionField {
    /// Parse from a string.
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "TIMEHOUR" | "TIME_HOUR" | "HOUR" => Some(Self::TimeHour),
            "TIMEDAY" | "TIME_DAY" | "DAY" => Some(Self::TimeDay),
            "CLIENTIP" | "CLIENT_IP" | "IP" => Some(Self::ClientIp),
            "CLIENTTENANT" | "CLIENT_TENANT" | "TENANT" => Some(Self::ClientTenant),
            "CLIENTUSER" | "CLIENT_USER" | "USER" => Some(Self::ClientUser),
            "KEYCLASSIFICATION" | "KEY_CLASSIFICATION" | "CLASSIFICATION" => {
                Some(Self::KeyClassification)
            }
            "DATASENSITIVITY" | "DATA_SENSITIVITY" | "SENSITIVITY" => {
                Some(Self::DataSensitivity)
            }
            "COMMANDCATEGORY" | "COMMAND_CATEGORY" | "CATEGORY" => Some(Self::CommandCategory),
            "KEYAGE" | "KEY_AGE" | "AGE" => Some(Self::KeyAge),
            "VALUESIZE" | "VALUE_SIZE" | "SIZE" => Some(Self::ValueSize),
            _ => None,
        }
    }
}

/// Comparison operators for conditions.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ConditionOp {
    Eq,
    Ne,
    Gt,
    Lt,
    Gte,
    Lte,
    /// Value must be `[min, max]`.
    Between,
    /// Value must be an array; field value in that array.
    In,
    /// Value must be an array; field value NOT in that array.
    NotIn,
    /// Regex match on strings.
    Matches,
    /// Substring match.
    Contains,
}

impl ConditionOp {
    /// Parse from a string.
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "EQ" | "=" | "==" => Some(Self::Eq),
            "NE" | "!=" | "<>" => Some(Self::Ne),
            "GT" | ">" => Some(Self::Gt),
            "LT" | "<" => Some(Self::Lt),
            "GTE" | ">=" => Some(Self::Gte),
            "LTE" | "<=" => Some(Self::Lte),
            "BETWEEN" => Some(Self::Between),
            "IN" => Some(Self::In),
            "NOTIN" | "NOT_IN" => Some(Self::NotIn),
            "MATCHES" | "REGEX" => Some(Self::Matches),
            "CONTAINS" | "LIKE" => Some(Self::Contains),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Evaluation context & result
// ---------------------------------------------------------------------------

/// Context provided to the policy engine for evaluation.
#[derive(Debug, Clone)]
pub struct EvaluationContext {
    /// Authenticated user.
    pub user: String,
    /// Client IP address.
    pub client_ip: String,
    /// Tenant the client belongs to, if any.
    pub tenant: Option<String>,
    /// Command being executed.
    pub command: String,
    /// Key being accessed, if any.
    pub key: Option<String>,
    /// Unix timestamp of the request.
    pub timestamp: u64,
    /// Classification of the key, if known.
    pub key_classification: Option<String>,
    /// Arbitrary custom attributes.
    pub custom: HashMap<String, serde_json::Value>,
}

impl EvaluationContext {
    /// Create a minimal context for testing.
    pub fn new(user: &str, command: &str) -> Self {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            user: user.to_string(),
            client_ip: "127.0.0.1".to_string(),
            tenant: None,
            command: command.to_string(),
            key: None,
            timestamp: ts,
            key_classification: None,
            custom: HashMap::new(),
        }
    }
}

/// Result of a policy evaluation.
#[derive(Debug, Clone)]
pub struct PolicyDecision {
    /// The action decided.
    pub action: PolicyAction,
    /// Name of the policy that matched, if any.
    pub matched_policy: Option<String>,
    /// Human-readable reason.
    pub reason: String,
    /// Time spent evaluating in microseconds.
    pub evaluation_us: u64,
}

/// Summary information about a policy.
#[derive(Debug, Clone)]
pub struct PolicyInfo {
    pub name: String,
    pub priority: i32,
    pub action: PolicyAction,
    pub conditions_count: usize,
    pub enabled: bool,
    pub evaluations: u64,
    pub denials: u64,
}

/// Aggregate statistics for the policy engine.
#[derive(Debug, Clone)]
pub struct PolicyStats {
    pub total_policies: usize,
    pub active: usize,
    pub evaluations: u64,
    pub denials: u64,
    pub cache_hits: u64,
    pub avg_eval_us: f64,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors returned by the policy engine.
#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    #[error("policy not found: {0}")]
    NotFound(String),
    #[error("policy already exists: {0}")]
    AlreadyExists(String),
    #[error("invalid condition: {0}")]
    InvalidCondition(String),
    #[error("maximum number of policies reached ({0})")]
    MaxPolicies(usize),
    #[error("failed to parse policy: {0}")]
    ParseError(String),
}

// ---------------------------------------------------------------------------
// Internal per-policy tracking
// ---------------------------------------------------------------------------

struct PolicyEntry {
    policy: Policy,
    evaluations: AtomicU64,
    denials: AtomicU64,
}

// ---------------------------------------------------------------------------
// PolicyEngine
// ---------------------------------------------------------------------------

/// The programmable policy engine.
pub struct PolicyEngine {
    config: PolicyConfig,
    policies: RwLock<Vec<PolicyEntry>>,
    total_evaluations: AtomicU64,
    total_denials: AtomicU64,
    cache_hits: AtomicU64,
    total_eval_us: AtomicU64,
}

impl PolicyEngine {
    /// Create a new policy engine with the given configuration.
    pub fn new(config: PolicyConfig) -> Self {
        Self {
            config,
            policies: RwLock::new(Vec::new()),
            total_evaluations: AtomicU64::new(0),
            total_denials: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            total_eval_us: AtomicU64::new(0),
        }
    }

    /// Add a new policy.
    pub fn add_policy(&self, policy: Policy) -> Result<(), PolicyError> {
        let mut policies = self.policies.write();
        if policies.len() >= self.config.max_policies {
            return Err(PolicyError::MaxPolicies(self.config.max_policies));
        }
        if policies.iter().any(|e| e.policy.name == policy.name) {
            return Err(PolicyError::AlreadyExists(policy.name.clone()));
        }
        policies.push(PolicyEntry {
            policy,
            evaluations: AtomicU64::new(0),
            denials: AtomicU64::new(0),
        });
        // Keep sorted by priority descending.
        policies.sort_by(|a, b| b.policy.priority.cmp(&a.policy.priority));
        Ok(())
    }

    /// Remove a policy by name.
    pub fn remove_policy(&self, name: &str) -> Result<(), PolicyError> {
        let mut policies = self.policies.write();
        let idx = policies
            .iter()
            .position(|e| e.policy.name == name)
            .ok_or_else(|| PolicyError::NotFound(name.to_string()))?;
        policies.remove(idx);
        Ok(())
    }

    /// Update an existing policy.
    pub fn update_policy(&self, name: &str, policy: Policy) -> Result<(), PolicyError> {
        let mut policies = self.policies.write();
        let entry = policies
            .iter_mut()
            .find(|e| e.policy.name == name)
            .ok_or_else(|| PolicyError::NotFound(name.to_string()))?;
        entry.policy = policy;
        // Re-sort after priority may have changed.
        policies.sort_by(|a, b| b.policy.priority.cmp(&a.policy.priority));
        Ok(())
    }

    /// Evaluate all matching policies and return a decision.
    pub fn evaluate(&self, ctx: &EvaluationContext) -> PolicyDecision {
        let start = Instant::now();
        self.total_evaluations.fetch_add(1, Ordering::Relaxed);

        let policies = self.policies.read();
        for entry in policies.iter() {
            if !entry.policy.enabled {
                continue;
            }
            if !scope_matches(&entry.policy.effect_scope, ctx) {
                continue;
            }
            if all_conditions_match(&entry.policy.conditions, ctx) {
                entry.evaluations.fetch_add(1, Ordering::Relaxed);
                if !entry.policy.action.is_allowed() {
                    entry.denials.fetch_add(1, Ordering::Relaxed);
                    self.total_denials.fetch_add(1, Ordering::Relaxed);
                }
                let elapsed = start.elapsed().as_micros() as u64;
                self.total_eval_us.fetch_add(elapsed, Ordering::Relaxed);
                return PolicyDecision {
                    action: entry.policy.action.clone(),
                    matched_policy: Some(entry.policy.name.clone()),
                    reason: format!("matched policy '{}'", entry.policy.name),
                    evaluation_us: elapsed,
                };
            }
        }

        let elapsed = start.elapsed().as_micros() as u64;
        self.total_eval_us.fetch_add(elapsed, Ordering::Relaxed);
        PolicyDecision {
            action: self.config.default_action.clone(),
            matched_policy: None,
            reason: "no matching policy; using default action".to_string(),
            evaluation_us: elapsed,
        }
    }

    /// Dry-run evaluation of a single named policy.
    pub fn test_policy(&self, policy_name: &str, ctx: &EvaluationContext) -> PolicyDecision {
        let start = Instant::now();
        let policies = self.policies.read();
        if let Some(entry) = policies.iter().find(|e| e.policy.name == policy_name) {
            let matches = scope_matches(&entry.policy.effect_scope, ctx)
                && all_conditions_match(&entry.policy.conditions, ctx);
            let elapsed = start.elapsed().as_micros() as u64;
            PolicyDecision {
                action: if matches {
                    entry.policy.action.clone()
                } else {
                    self.config.default_action.clone()
                },
                matched_policy: if matches {
                    Some(entry.policy.name.clone())
                } else {
                    None
                },
                reason: if matches {
                    format!("policy '{}' matched (dry-run)", policy_name)
                } else {
                    format!("policy '{}' did NOT match (dry-run)", policy_name)
                },
                evaluation_us: elapsed,
            }
        } else {
            PolicyDecision {
                action: self.config.default_action.clone(),
                matched_policy: None,
                reason: format!("policy '{}' not found", policy_name),
                evaluation_us: start.elapsed().as_micros() as u64,
            }
        }
    }

    /// List summary info for all policies.
    pub fn list_policies(&self) -> Vec<PolicyInfo> {
        let policies = self.policies.read();
        policies
            .iter()
            .map(|e| PolicyInfo {
                name: e.policy.name.clone(),
                priority: e.policy.priority,
                action: e.policy.action.clone(),
                conditions_count: e.policy.conditions.len(),
                enabled: e.policy.enabled,
                evaluations: e.evaluations.load(Ordering::Relaxed),
                denials: e.denials.load(Ordering::Relaxed),
            })
            .collect()
    }

    /// Get a policy by name.
    pub fn get_policy(&self, name: &str) -> Option<Policy> {
        let policies = self.policies.read();
        policies
            .iter()
            .find(|e| e.policy.name == name)
            .map(|e| e.policy.clone())
    }

    /// Return which policies would match the given context.
    pub fn matching_policies(&self, ctx: &EvaluationContext) -> Vec<PolicyInfo> {
        let policies = self.policies.read();
        policies
            .iter()
            .filter(|e| {
                e.policy.enabled
                    && scope_matches(&e.policy.effect_scope, ctx)
                    && all_conditions_match(&e.policy.conditions, ctx)
            })
            .map(|e| PolicyInfo {
                name: e.policy.name.clone(),
                priority: e.policy.priority,
                action: e.policy.action.clone(),
                conditions_count: e.policy.conditions.len(),
                enabled: e.policy.enabled,
                evaluations: e.evaluations.load(Ordering::Relaxed),
                denials: e.denials.load(Ordering::Relaxed),
            })
            .collect()
    }

    /// Aggregate statistics.
    pub fn stats(&self) -> PolicyStats {
        let policies = self.policies.read();
        let total_evals = self.total_evaluations.load(Ordering::Relaxed);
        let total_us = self.total_eval_us.load(Ordering::Relaxed);
        PolicyStats {
            total_policies: policies.len(),
            active: policies.iter().filter(|e| e.policy.enabled).count(),
            evaluations: total_evals,
            denials: self.total_denials.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            avg_eval_us: if total_evals > 0 {
                total_us as f64 / total_evals as f64
            } else {
                0.0
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Check whether the evaluation context falls within the policy's scope.
fn scope_matches(scope: &EffectScope, ctx: &EvaluationContext) -> bool {
    // Empty list = matches everything.
    let cmd_ok = scope.commands.is_empty()
        || scope
            .commands
            .iter()
            .any(|p| glob_match(p, &ctx.command));
    let key_ok = scope.key_patterns.is_empty()
        || ctx
            .key
            .as_ref()
            .map(|k| scope.key_patterns.iter().any(|p| glob_match(p, k)))
            .unwrap_or(true);
    let user_ok =
        scope.users.is_empty() || scope.users.iter().any(|u| glob_match(u, &ctx.user));
    let tenant_ok = scope.tenants.is_empty()
        || ctx
            .tenant
            .as_ref()
            .map(|t| scope.tenants.iter().any(|p| glob_match(p, t)))
            .unwrap_or(false);
    cmd_ok && key_ok && user_ok && tenant_ok
}

/// Evaluate all conditions; all must pass (AND logic).
fn all_conditions_match(conditions: &[PolicyCondition], ctx: &EvaluationContext) -> bool {
    conditions.iter().all(|c| condition_matches(c, ctx))
}

/// Evaluate a single condition.
fn condition_matches(cond: &PolicyCondition, ctx: &EvaluationContext) -> bool {
    let field_value = extract_field_value(&cond.field, ctx);
    apply_operator(&cond.operator, &field_value, &cond.value)
}

/// Extract a field value from the context as a JSON value.
fn extract_field_value(field: &ConditionField, ctx: &EvaluationContext) -> serde_json::Value {
    match field {
        ConditionField::TimeHour => {
            // Extract hour from unix timestamp.
            let secs = ctx.timestamp % 86400;
            let hour = secs / 3600;
            serde_json::Value::Number(serde_json::Number::from(hour))
        }
        ConditionField::TimeDay => {
            // Unix epoch (1970-01-01) was a Thursday (4).
            let day = ((ctx.timestamp / 86400) + 4) % 7;
            serde_json::Value::Number(serde_json::Number::from(day))
        }
        ConditionField::ClientIp => serde_json::Value::String(ctx.client_ip.clone()),
        ConditionField::ClientTenant => ctx
            .tenant
            .as_ref()
            .map(|t| serde_json::Value::String(t.clone()))
            .unwrap_or(serde_json::Value::Null),
        ConditionField::ClientUser => serde_json::Value::String(ctx.user.clone()),
        ConditionField::KeyClassification => ctx
            .key_classification
            .as_ref()
            .map(|c| serde_json::Value::String(c.clone()))
            .unwrap_or(serde_json::Value::Null),
        ConditionField::DataSensitivity => ctx
            .custom
            .get("data_sensitivity")
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        ConditionField::CommandCategory => ctx
            .custom
            .get("command_category")
            .cloned()
            .unwrap_or(serde_json::Value::String(ctx.command.clone())),
        ConditionField::KeyAge => ctx
            .custom
            .get("key_age")
            .cloned()
            .unwrap_or(serde_json::Value::Number(serde_json::Number::from(0))),
        ConditionField::ValueSize => ctx
            .custom
            .get("value_size")
            .cloned()
            .unwrap_or(serde_json::Value::Number(serde_json::Number::from(0))),
    }
}

/// Apply an operator, comparing `field_val` against `cond_val`.
fn apply_operator(
    op: &ConditionOp,
    field_val: &serde_json::Value,
    cond_val: &serde_json::Value,
) -> bool {
    match op {
        ConditionOp::Eq => field_val == cond_val,
        ConditionOp::Ne => field_val != cond_val,
        ConditionOp::Gt => compare_numbers(field_val, cond_val).map_or(false, |o| o > 0),
        ConditionOp::Lt => compare_numbers(field_val, cond_val).map_or(false, |o| o < 0),
        ConditionOp::Gte => compare_numbers(field_val, cond_val).map_or(false, |o| o >= 0),
        ConditionOp::Lte => compare_numbers(field_val, cond_val).map_or(false, |o| o <= 0),
        ConditionOp::Between => {
            if let serde_json::Value::Array(arr) = cond_val {
                if arr.len() == 2 {
                    let gte_min =
                        compare_numbers(field_val, &arr[0]).map_or(false, |o| o >= 0);
                    let lte_max =
                        compare_numbers(field_val, &arr[1]).map_or(false, |o| o <= 0);
                    return gte_min && lte_max;
                }
            }
            false
        }
        ConditionOp::In => {
            if let serde_json::Value::Array(arr) = cond_val {
                arr.contains(field_val)
            } else {
                false
            }
        }
        ConditionOp::NotIn => {
            if let serde_json::Value::Array(arr) = cond_val {
                !arr.contains(field_val)
            } else {
                true
            }
        }
        ConditionOp::Matches => {
            let pattern = cond_val.as_str().unwrap_or("");
            let text = field_val.as_str().unwrap_or("");
            regex::Regex::new(pattern)
                .map(|re| re.is_match(text))
                .unwrap_or(false)
        }
        ConditionOp::Contains => {
            let needle = cond_val.as_str().unwrap_or("");
            let hay = field_val.as_str().unwrap_or("");
            hay.contains(needle)
        }
    }
}

/// Compare two JSON values as f64 numbers, returning -1 / 0 / 1.
fn compare_numbers(a: &serde_json::Value, b: &serde_json::Value) -> Option<i8> {
    let a_num = json_to_f64(a)?;
    let b_num = json_to_f64(b)?;
    if (a_num - b_num).abs() < f64::EPSILON {
        Some(0)
    } else if a_num < b_num {
        Some(-1)
    } else {
        Some(1)
    }
}

fn json_to_f64(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

/// Simple glob matching supporting `*` and `?`.
fn glob_match(pattern: &str, text: &str) -> bool {
    glob_match_impl(&pattern.chars().collect::<Vec<_>>(), &text.chars().collect::<Vec<_>>())
}

fn glob_match_impl(pattern: &[char], text: &[char]) -> bool {
    let mut px = 0;
    let mut tx = 0;
    let mut star_px = usize::MAX;
    let mut star_tx = 0;

    while tx < text.len() {
        if px < pattern.len() && (pattern[px] == '?' || pattern[px] == text[tx]) {
            px += 1;
            tx += 1;
        } else if px < pattern.len() && pattern[px] == '*' {
            star_px = px;
            star_tx = tx;
            px += 1;
        } else if star_px != usize::MAX {
            px = star_px + 1;
            star_tx += 1;
            tx = star_tx;
        } else {
            return false;
        }
    }
    while px < pattern.len() && pattern[px] == '*' {
        px += 1;
    }
    px == pattern.len()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn now_ts() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn make_policy(name: &str, action: PolicyAction, priority: i32) -> Policy {
        Policy {
            name: name.to_string(),
            description: format!("test policy {}", name),
            priority,
            action,
            conditions: vec![],
            effect_scope: EffectScope::default(),
            enabled: true,
            created_at: now_ts(),
            version: 1,
        }
    }

    #[test]
    fn test_basic_allow() {
        let engine = PolicyEngine::new(PolicyConfig::default());
        engine
            .add_policy(make_policy("allow-all", PolicyAction::Allow, 10))
            .unwrap();
        let ctx = EvaluationContext::new("admin", "GET");
        let decision = engine.evaluate(&ctx);
        assert!(decision.action.is_allowed());
        assert_eq!(decision.matched_policy.as_deref(), Some("allow-all"));
    }

    #[test]
    fn test_basic_deny() {
        let engine = PolicyEngine::new(PolicyConfig::default());
        engine
            .add_policy(make_policy("deny-all", PolicyAction::Deny, 10))
            .unwrap();
        let ctx = EvaluationContext::new("user1", "SET");
        let decision = engine.evaluate(&ctx);
        assert!(!decision.action.is_allowed());
    }

    #[test]
    fn test_priority_ordering() {
        let engine = PolicyEngine::new(PolicyConfig::default());
        engine
            .add_policy(make_policy("low-allow", PolicyAction::Allow, 1))
            .unwrap();
        engine
            .add_policy(make_policy("high-deny", PolicyAction::Deny, 100))
            .unwrap();
        let ctx = EvaluationContext::new("user1", "SET");
        let decision = engine.evaluate(&ctx);
        assert_eq!(decision.matched_policy.as_deref(), Some("high-deny"));
        assert!(!decision.action.is_allowed());
    }

    #[test]
    fn test_default_action_when_no_match() {
        let config = PolicyConfig {
            default_action: PolicyAction::Deny,
            ..PolicyConfig::default()
        };
        let engine = PolicyEngine::new(config);
        let ctx = EvaluationContext::new("user1", "SET");
        let decision = engine.evaluate(&ctx);
        assert!(!decision.action.is_allowed());
        assert!(decision.matched_policy.is_none());
    }

    #[test]
    fn test_time_based_condition() {
        let engine = PolicyEngine::new(PolicyConfig::default());
        // Create a policy that denies access outside 9–17 hours.
        let mut policy = make_policy("business-hours", PolicyAction::Deny, 50);
        policy.conditions.push(PolicyCondition {
            field: ConditionField::TimeHour,
            operator: ConditionOp::Lt,
            value: serde_json::json!(9),
        });
        engine.add_policy(policy).unwrap();

        // Test with hour=8 (before 9) should match the deny rule.
        let mut ctx = EvaluationContext::new("user1", "SET");
        ctx.timestamp = 8 * 3600; // 08:00 UTC on epoch day
        let decision = engine.evaluate(&ctx);
        assert!(!decision.action.is_allowed());

        // Test with hour=12 should NOT match the deny rule → default allow.
        ctx.timestamp = 12 * 3600;
        let decision = engine.evaluate(&ctx);
        assert!(decision.action.is_allowed());
    }

    #[test]
    fn test_tenant_scoped() {
        let engine = PolicyEngine::new(PolicyConfig::default());
        let mut policy = make_policy("tenant-deny", PolicyAction::Deny, 50);
        policy.effect_scope.tenants = vec!["acme".to_string()];
        engine.add_policy(policy).unwrap();

        let mut ctx = EvaluationContext::new("user1", "SET");
        ctx.tenant = Some("acme".to_string());
        let decision = engine.evaluate(&ctx);
        assert!(!decision.action.is_allowed());

        ctx.tenant = Some("other-corp".to_string());
        let decision = engine.evaluate(&ctx);
        assert!(decision.action.is_allowed());
    }

    #[test]
    fn test_command_scope() {
        let engine = PolicyEngine::new(PolicyConfig::default());
        let mut policy = make_policy("deny-writes", PolicyAction::Deny, 50);
        policy.effect_scope.commands = vec!["SET".to_string(), "DEL".to_string()];
        engine.add_policy(policy).unwrap();

        let ctx_set = EvaluationContext::new("user1", "SET");
        assert!(!engine.evaluate(&ctx_set).action.is_allowed());

        let ctx_get = EvaluationContext::new("user1", "GET");
        assert!(engine.evaluate(&ctx_get).action.is_allowed());
    }

    #[test]
    fn test_dry_run() {
        let engine = PolicyEngine::new(PolicyConfig::default());
        engine
            .add_policy(make_policy("test-pol", PolicyAction::Deny, 10))
            .unwrap();
        let ctx = EvaluationContext::new("user1", "GET");
        let decision = engine.test_policy("test-pol", &ctx);
        assert!(decision.reason.contains("dry-run"));
    }

    #[test]
    fn test_duplicate_policy_error() {
        let engine = PolicyEngine::new(PolicyConfig::default());
        engine
            .add_policy(make_policy("dup", PolicyAction::Allow, 1))
            .unwrap();
        let result = engine.add_policy(make_policy("dup", PolicyAction::Deny, 2));
        assert!(matches!(result, Err(PolicyError::AlreadyExists(_))));
    }

    #[test]
    fn test_remove_policy() {
        let engine = PolicyEngine::new(PolicyConfig::default());
        engine
            .add_policy(make_policy("rm-me", PolicyAction::Deny, 10))
            .unwrap();
        assert!(engine.remove_policy("rm-me").is_ok());
        assert!(matches!(
            engine.remove_policy("rm-me"),
            Err(PolicyError::NotFound(_))
        ));
    }

    #[test]
    fn test_list_policies() {
        let engine = PolicyEngine::new(PolicyConfig::default());
        engine
            .add_policy(make_policy("a", PolicyAction::Allow, 1))
            .unwrap();
        engine
            .add_policy(make_policy("b", PolicyAction::Deny, 2))
            .unwrap();
        let list = engine.list_policies();
        assert_eq!(list.len(), 2);
        // Sorted by priority descending.
        assert_eq!(list[0].name, "b");
    }

    #[test]
    fn test_matching_policies() {
        let engine = PolicyEngine::new(PolicyConfig::default());
        let mut scoped = make_policy("scoped", PolicyAction::Deny, 50);
        scoped.effect_scope.users = vec!["admin".to_string()];
        engine.add_policy(scoped).unwrap();
        engine
            .add_policy(make_policy("global", PolicyAction::Allow, 1))
            .unwrap();

        let ctx = EvaluationContext::new("admin", "SET");
        let matching = engine.matching_policies(&ctx);
        assert_eq!(matching.len(), 2);

        let ctx2 = EvaluationContext::new("user1", "SET");
        let matching2 = engine.matching_policies(&ctx2);
        assert_eq!(matching2.len(), 1);
        assert_eq!(matching2[0].name, "global");
    }

    #[test]
    fn test_stats() {
        let engine = PolicyEngine::new(PolicyConfig::default());
        engine
            .add_policy(make_policy("s", PolicyAction::Deny, 1))
            .unwrap();
        let ctx = EvaluationContext::new("u", "GET");
        engine.evaluate(&ctx);
        engine.evaluate(&ctx);
        let stats = engine.stats();
        assert_eq!(stats.total_policies, 1);
        assert_eq!(stats.evaluations, 2);
        assert_eq!(stats.denials, 2);
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("user:*", "user:123"));
        assert!(glob_match("user:*:data", "user:123:data"));
        assert!(!glob_match("user:*:data", "user:123:meta"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("a?c", "abc"));
        assert!(!glob_match("a?c", "abbc"));
    }

    #[test]
    fn test_condition_between() {
        let cond = PolicyCondition {
            field: ConditionField::TimeHour,
            operator: ConditionOp::Between,
            value: serde_json::json!([9, 17]),
        };
        let mut ctx = EvaluationContext::new("u", "GET");
        ctx.timestamp = 12 * 3600; // hour = 12
        assert!(condition_matches(&cond, &ctx));

        ctx.timestamp = 5 * 3600; // hour = 5
        assert!(!condition_matches(&cond, &ctx));
    }

    #[test]
    fn test_condition_in() {
        let cond = PolicyCondition {
            field: ConditionField::ClientUser,
            operator: ConditionOp::In,
            value: serde_json::json!(["admin", "superuser"]),
        };
        let ctx = EvaluationContext::new("admin", "GET");
        assert!(condition_matches(&cond, &ctx));

        let ctx2 = EvaluationContext::new("nobody", "GET");
        assert!(!condition_matches(&cond, &ctx2));
    }
}
