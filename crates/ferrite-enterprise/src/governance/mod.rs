//! Policy-Based Data Governance
//!
//! This module provides comprehensive data governance capabilities:
//! - Data classification and labeling
//! - Access control policies
//! - Data retention policies
//! - Data masking and redaction
//! - Compliance rules and enforcement
//! - Data lineage tracking
//! - Policy auditing and reporting
//!
//! # Example
//!
//! ```ignore
//! use ferrite::governance::{GovernanceEngine, Policy, DataClassification};
//!
//! let engine = GovernanceEngine::new(config);
//!
//! // Define a policy
//! let policy = Policy::new("pii-protection")
//!     .classification(DataClassification::Confidential)
//!     .pattern("user:*:email")
//!     .mask(MaskingRule::EmailMask)
//!     .retention(Duration::from_days(365))
//!     .build();
//!
//! engine.add_policy(policy).await?;
//! ```

pub mod classification;
pub mod compliance;
pub mod lineage;
pub mod masking;
pub mod policy;
pub mod privacy;
pub mod retention;

pub use classification::{DataClassification, DataLabel, LabelManager, Sensitivity};
pub use compliance::{
    ComplianceCheck, ComplianceEngine, ComplianceReport, ComplianceRule, ComplianceViolation,
    Framework, ViolationSeverity,
};
pub use lineage::{DataLineage, LineageEdge, LineageEvent, LineageGraph, LineageNode};
pub use masking::{MaskingEngine, MaskingRule, MaskingStrategy, RedactionPolicy};
pub use policy::{
    AccessPolicy, DataPolicy, Policy, PolicyAction, PolicyCondition, PolicyEngine,
    PolicyEvaluation, PolicyMatch, PolicyRule, RetentionPolicy,
};
pub use retention::{
    RetentionAction, RetentionEngine, RetentionPolicy as RetentionConfig, RetentionRule,
};

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::info;

/// Governance configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GovernanceConfig {
    /// Enable governance
    pub enabled: bool,
    /// Enable data classification
    pub classification_enabled: bool,
    /// Enable access policies
    pub access_policies_enabled: bool,
    /// Enable data masking
    pub masking_enabled: bool,
    /// Enable retention policies
    pub retention_enabled: bool,
    /// Enable compliance checks
    pub compliance_enabled: bool,
    /// Enable lineage tracking
    pub lineage_enabled: bool,
    /// Default data classification
    pub default_classification: DataClassification,
    /// Policy evaluation mode
    pub evaluation_mode: EvaluationMode,
    /// Audit policy decisions
    pub audit_decisions: bool,
    /// Cache policy decisions
    pub cache_enabled: bool,
    /// Cache TTL in seconds
    pub cache_ttl_secs: u64,
}

impl Default for GovernanceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            classification_enabled: true,
            access_policies_enabled: true,
            masking_enabled: true,
            retention_enabled: true,
            compliance_enabled: true,
            lineage_enabled: false, // Can be expensive
            default_classification: DataClassification::Internal,
            evaluation_mode: EvaluationMode::Enforce,
            audit_decisions: true,
            cache_enabled: true,
            cache_ttl_secs: 300,
        }
    }
}

/// Policy evaluation mode
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvaluationMode {
    /// Only report violations, don't enforce
    Audit,
    /// Warn but allow
    Warn,
    /// Enforce policies strictly
    Enforce,
}

/// Governance metrics
#[derive(Debug, Default)]
pub struct GovernanceMetrics {
    /// Policies evaluated
    pub policies_evaluated: AtomicU64,
    /// Policy matches
    pub policy_matches: AtomicU64,
    /// Policy violations
    pub policy_violations: AtomicU64,
    /// Access denied
    pub access_denied: AtomicU64,
    /// Data masked
    pub data_masked: AtomicU64,
    /// Data retained
    pub data_retained: AtomicU64,
    /// Data expired
    pub data_expired: AtomicU64,
    /// Compliance checks run
    pub compliance_checks: AtomicU64,
    /// Compliance violations
    pub compliance_violations: AtomicU64,
}

/// Main Governance Engine
pub struct GovernanceEngine {
    /// Configuration
    config: GovernanceConfig,
    /// Policy engine
    policy_engine: Arc<PolicyEngine>,
    /// Masking engine
    masking_engine: Arc<MaskingEngine>,
    /// Retention engine
    retention_engine: Arc<RetentionEngine>,
    /// Compliance engine
    compliance_engine: Arc<ComplianceEngine>,
    /// Label manager
    label_manager: Arc<LabelManager>,
    /// Lineage graph (if enabled)
    lineage: Option<Arc<RwLock<LineageGraph>>>,
    /// Metrics
    metrics: Arc<GovernanceMetrics>,
    /// Decision cache
    cache: Arc<RwLock<DecisionCache>>,
    /// Running flag
    running: AtomicBool,
}

/// Decision cache for performance
struct DecisionCache {
    entries: HashMap<String, CachedDecision>,
    max_size: usize,
}

struct CachedDecision {
    result: PolicyDecision,
    cached_at: Instant,
    ttl: Duration,
}

impl DecisionCache {
    fn new(max_size: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_size,
        }
    }

    fn get(&self, key: &str) -> Option<PolicyDecision> {
        self.entries.get(key).and_then(|entry| {
            if entry.cached_at.elapsed() < entry.ttl {
                Some(entry.result.clone())
            } else {
                None
            }
        })
    }

    fn insert(&mut self, key: String, result: PolicyDecision, ttl: Duration) {
        // Evict old entries if full
        if self.entries.len() >= self.max_size {
            self.entries.retain(|_, v| v.cached_at.elapsed() < v.ttl);

            // If still full, remove oldest
            if self.entries.len() >= self.max_size {
                if let Some(oldest_key) = self
                    .entries
                    .iter()
                    .min_by_key(|(_, v)| v.cached_at)
                    .map(|(k, _)| k.clone())
                {
                    self.entries.remove(&oldest_key);
                }
            }
        }

        self.entries.insert(
            key,
            CachedDecision {
                result,
                cached_at: Instant::now(),
                ttl,
            },
        );
    }
}

/// Policy decision result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PolicyDecision {
    /// Allow the operation
    pub allow: bool,
    /// Matched policies
    pub matched_policies: Vec<String>,
    /// Actions to apply
    pub actions: Vec<PolicyAction>,
    /// Masking to apply
    pub masking: Option<MaskingRule>,
    /// Warnings
    pub warnings: Vec<String>,
    /// Audit trail ID
    pub audit_id: Option<String>,
}

impl Default for PolicyDecision {
    fn default() -> Self {
        Self {
            allow: true,
            matched_policies: vec![],
            actions: vec![],
            masking: None,
            warnings: vec![],
            audit_id: None,
        }
    }
}

/// Request context for policy evaluation
#[derive(Clone, Debug)]
pub struct RequestContext {
    /// User/client ID
    pub user_id: Option<String>,
    /// User roles
    pub roles: Vec<String>,
    /// Client IP
    pub client_ip: Option<String>,
    /// Operation type
    pub operation: OperationType,
    /// Key being accessed
    pub key: Option<String>,
    /// Key pattern
    pub pattern: Option<String>,
    /// Additional attributes
    pub attributes: HashMap<String, String>,
}

/// Operation types
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationType {
    Read,
    Write,
    Delete,
    Admin,
    Subscribe,
    Publish,
}

impl GovernanceEngine {
    /// Create a new governance engine
    pub fn new(config: GovernanceConfig) -> Self {
        let lineage = if config.lineage_enabled {
            Some(Arc::new(RwLock::new(LineageGraph::new())))
        } else {
            None
        };

        Self {
            config: config.clone(),
            policy_engine: Arc::new(PolicyEngine::new()),
            masking_engine: Arc::new(MaskingEngine::new()),
            retention_engine: Arc::new(RetentionEngine::new()),
            compliance_engine: Arc::new(ComplianceEngine::new()),
            label_manager: Arc::new(LabelManager::new()),
            lineage,
            metrics: Arc::new(GovernanceMetrics::default()),
            cache: Arc::new(RwLock::new(DecisionCache::new(10000))),
            running: AtomicBool::new(false),
        }
    }

    /// Start the governance engine
    pub async fn start(&self) -> Result<(), GovernanceError> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err(GovernanceError::AlreadyRunning);
        }

        info!("Starting governance engine");
        Ok(())
    }

    /// Stop the governance engine
    pub async fn stop(&self) -> Result<(), GovernanceError> {
        if !self.running.swap(false, Ordering::SeqCst) {
            return Err(GovernanceError::NotRunning);
        }

        info!("Governance engine stopped");
        Ok(())
    }

    // ========== Policy Management ==========

    /// Add a policy
    pub fn add_policy(&self, policy: Policy) -> Result<(), GovernanceError> {
        self.policy_engine.add_policy(policy)
    }

    /// Remove a policy
    pub fn remove_policy(&self, policy_id: &str) -> Result<(), GovernanceError> {
        self.policy_engine.remove_policy(policy_id)
    }

    /// Get all policies
    pub fn list_policies(&self) -> Vec<Policy> {
        self.policy_engine.list_policies()
    }

    // ========== Policy Evaluation ==========

    /// Evaluate policies for a request
    pub fn evaluate(&self, context: &RequestContext) -> PolicyDecision {
        if !self.config.enabled {
            return PolicyDecision::default();
        }

        // Check cache first
        let cache_key = self.make_cache_key(context);
        if self.config.cache_enabled {
            if let Some(cached) = self.cache.read().get(&cache_key) {
                return cached;
            }
        }

        // Evaluate policies
        self.metrics
            .policies_evaluated
            .fetch_add(1, Ordering::Relaxed);

        let evaluation = self.policy_engine.evaluate(context);

        let mut decision = PolicyDecision {
            allow: true,
            matched_policies: vec![],
            actions: vec![],
            masking: None,
            warnings: vec![],
            audit_id: None,
        };

        // Process matched policies
        for policy_match in &evaluation.matches {
            decision
                .matched_policies
                .push(policy_match.policy_id.clone());

            match policy_match.action {
                PolicyAction::Allow => {}
                PolicyAction::Deny => {
                    decision.allow = false;
                    self.metrics.access_denied.fetch_add(1, Ordering::Relaxed);
                }
                PolicyAction::Mask(ref rule) => {
                    decision.masking = Some(rule.clone());
                    self.metrics.data_masked.fetch_add(1, Ordering::Relaxed);
                }
                PolicyAction::Warn(ref msg) => {
                    decision.warnings.push(msg.clone());
                }
                PolicyAction::Audit => {
                    // Generate audit entry
                }
                PolicyAction::Notify(_) => {
                    // Send notification
                }
            }

            decision.actions.push(policy_match.action.clone());
        }

        self.metrics
            .policy_matches
            .fetch_add(evaluation.matches.len() as u64, Ordering::Relaxed);

        // Apply enforcement mode
        match self.config.evaluation_mode {
            EvaluationMode::Audit => {
                // Log but allow
                decision.allow = true;
            }
            EvaluationMode::Warn => {
                // Allow but add warning
                if !decision.allow {
                    decision
                        .warnings
                        .push("Access would be denied in enforce mode".to_string());
                    decision.allow = true;
                }
            }
            EvaluationMode::Enforce => {
                // Strict enforcement
            }
        }

        // Cache the decision
        if self.config.cache_enabled {
            let ttl = Duration::from_secs(self.config.cache_ttl_secs);
            self.cache.write().insert(cache_key, decision.clone(), ttl);
        }

        decision
    }

    fn make_cache_key(&self, context: &RequestContext) -> String {
        format!(
            "{}:{}:{:?}:{}",
            context.user_id.as_deref().unwrap_or("anon"),
            context.key.as_deref().unwrap_or("*"),
            context.operation,
            context.roles.join(",")
        )
    }

    // ========== Data Classification ==========

    /// Classify a key
    pub fn classify_key(
        &self,
        key: &str,
        classification: DataClassification,
    ) -> Result<(), GovernanceError> {
        self.label_manager.set_classification(key, classification)
    }

    /// Get key classification
    pub fn get_classification(&self, key: &str) -> DataClassification {
        self.label_manager
            .get_classification(key)
            .unwrap_or(self.config.default_classification.clone())
    }

    /// Add a label to a key
    pub fn add_label(&self, key: &str, label: DataLabel) -> Result<(), GovernanceError> {
        self.label_manager.add_label(key, label)
    }

    /// Get labels for a key
    pub fn get_labels(&self, key: &str) -> Vec<DataLabel> {
        self.label_manager.get_labels(key)
    }

    // ========== Data Masking ==========

    /// Apply masking to a value
    pub fn mask_value(&self, value: &str, rule: &MaskingRule) -> String {
        self.masking_engine.mask(value, rule)
    }

    /// Apply masking based on policy decision
    pub fn apply_masking(&self, value: &str, decision: &PolicyDecision) -> String {
        if let Some(ref rule) = decision.masking {
            self.masking_engine.mask(value, rule)
        } else {
            value.to_string()
        }
    }

    // ========== Retention ==========

    /// Check if a key should be retained
    pub fn check_retention(&self, key: &str, created_at: u64) -> RetentionAction {
        self.retention_engine.check(key, created_at)
    }

    /// Add a retention rule
    pub fn add_retention_rule(&self, rule: RetentionRule) -> Result<(), GovernanceError> {
        self.retention_engine.add_rule(rule)
    }

    // ========== Compliance ==========

    /// Run compliance checks
    pub fn check_compliance(&self) -> ComplianceReport {
        self.metrics
            .compliance_checks
            .fetch_add(1, Ordering::Relaxed);
        let report = self.compliance_engine.check_all();

        if !report.violations.is_empty() {
            self.metrics
                .compliance_violations
                .fetch_add(report.violations.len() as u64, Ordering::Relaxed);
        }

        report
    }

    /// Add a compliance rule
    pub fn add_compliance_rule(&self, rule: ComplianceRule) -> Result<(), GovernanceError> {
        self.compliance_engine.add_rule(rule)
    }

    // ========== Lineage ==========

    /// Record a lineage event
    pub fn record_lineage(&self, event: LineageEvent) {
        if let Some(ref lineage) = self.lineage {
            lineage.write().add_event(event);
        }
    }

    /// Get lineage for a key
    pub fn get_lineage(&self, key: &str) -> Option<DataLineage> {
        self.lineage.as_ref().map(|l| l.read().get_lineage(key))
    }

    // ========== Metrics ==========

    /// Get governance metrics
    pub fn get_metrics(&self) -> GovernanceMetricsSnapshot {
        GovernanceMetricsSnapshot {
            policies_evaluated: self.metrics.policies_evaluated.load(Ordering::Relaxed),
            policy_matches: self.metrics.policy_matches.load(Ordering::Relaxed),
            policy_violations: self.metrics.policy_violations.load(Ordering::Relaxed),
            access_denied: self.metrics.access_denied.load(Ordering::Relaxed),
            data_masked: self.metrics.data_masked.load(Ordering::Relaxed),
            data_retained: self.metrics.data_retained.load(Ordering::Relaxed),
            data_expired: self.metrics.data_expired.load(Ordering::Relaxed),
            compliance_checks: self.metrics.compliance_checks.load(Ordering::Relaxed),
            compliance_violations: self.metrics.compliance_violations.load(Ordering::Relaxed),
        }
    }
}

/// Metrics snapshot
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GovernanceMetricsSnapshot {
    pub policies_evaluated: u64,
    pub policy_matches: u64,
    pub policy_violations: u64,
    pub access_denied: u64,
    pub data_masked: u64,
    pub data_retained: u64,
    pub data_expired: u64,
    pub compliance_checks: u64,
    pub compliance_violations: u64,
}

/// Governance errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum GovernanceError {
    #[error("Governance engine already running")]
    AlreadyRunning,

    #[error("Governance engine not running")]
    NotRunning,

    #[error("Policy not found: {0}")]
    PolicyNotFound(String),

    #[error("Policy already exists: {0}")]
    PolicyExists(String),

    #[error("Invalid policy: {0}")]
    InvalidPolicy(String),

    #[error("Classification error: {0}")]
    ClassificationError(String),

    #[error("Compliance error: {0}")]
    ComplianceError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_governance_config_default() {
        let config = GovernanceConfig::default();
        assert!(!config.enabled);
        assert!(config.classification_enabled);
    }

    #[test]
    fn test_governance_engine_creation() {
        let config = GovernanceConfig::default();
        let engine = GovernanceEngine::new(config);
        assert!(!engine.running.load(Ordering::SeqCst));
    }

    #[test]
    fn test_policy_decision_default() {
        let decision = PolicyDecision::default();
        assert!(decision.allow);
        assert!(decision.matched_policies.is_empty());
    }

    #[test]
    fn test_evaluate_disabled() {
        let config = GovernanceConfig {
            enabled: false,
            ..Default::default()
        };
        let engine = GovernanceEngine::new(config);

        let context = RequestContext {
            user_id: Some("user1".to_string()),
            roles: vec!["admin".to_string()],
            client_ip: Some("127.0.0.1".to_string()),
            operation: OperationType::Read,
            key: Some("test:key".to_string()),
            pattern: None,
            attributes: HashMap::new(),
        };

        let decision = engine.evaluate(&context);
        assert!(decision.allow);
    }
}
