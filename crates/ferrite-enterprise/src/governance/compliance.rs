//! Compliance Engine

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::GovernanceError;

/// Compliance framework
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Framework {
    /// GDPR (General Data Protection Regulation)
    Gdpr,
    /// HIPAA (Health Insurance Portability and Accountability Act)
    Hipaa,
    /// PCI-DSS (Payment Card Industry Data Security Standard)
    PciDss,
    /// SOC 2 (Service Organization Control 2)
    Soc2,
    /// CCPA (California Consumer Privacy Act)
    Ccpa,
    /// SOX (Sarbanes-Oxley)
    Sox,
    /// Custom framework
    Custom(String),
}

/// Compliance rule
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ComplianceRule {
    /// Rule ID
    pub id: String,
    /// Rule name
    pub name: String,
    /// Description
    pub description: String,
    /// Applicable frameworks
    pub frameworks: Vec<Framework>,
    /// Check type
    pub check_type: CheckType,
    /// Severity
    pub severity: ViolationSeverity,
    /// Enabled
    pub enabled: bool,
    /// Auto-remediate
    pub auto_remediate: bool,
}

/// Check types
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CheckType {
    /// Data classification required
    ClassificationRequired { patterns: Vec<String> },
    /// Encryption required
    EncryptionRequired { patterns: Vec<String> },
    /// Retention policy required
    RetentionRequired {
        patterns: Vec<String>,
        max_days: u32,
    },
    /// Access logging required
    AuditRequired { operations: Vec<String> },
    /// Data masking required
    MaskingRequired { patterns: Vec<String> },
    /// No plaintext PII
    NoPiiPlaintext,
    /// Geographic restriction
    GeoRestriction { allowed_regions: Vec<String> },
    /// Custom check
    Custom {
        name: String,
        config: HashMap<String, String>,
    },
}

/// Violation severity
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ViolationSeverity {
    /// Informational
    Info,
    /// Low severity
    Low,
    /// Medium severity
    Medium,
    /// High severity
    High,
    /// Critical severity
    Critical,
}

/// Compliance check result
#[derive(Clone, Debug)]
pub struct ComplianceCheck {
    /// Rule ID
    pub rule_id: String,
    /// Passed
    pub passed: bool,
    /// Message
    pub message: String,
    /// Details
    pub details: HashMap<String, String>,
    /// Timestamp
    pub checked_at: u64,
}

/// Compliance violation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ComplianceViolation {
    /// Violation ID
    pub id: String,
    /// Rule ID
    pub rule_id: String,
    /// Rule name
    pub rule_name: String,
    /// Frameworks affected
    pub frameworks: Vec<Framework>,
    /// Severity
    pub severity: ViolationSeverity,
    /// Description
    pub description: String,
    /// Affected keys
    pub affected_keys: Vec<String>,
    /// Remediation steps
    pub remediation: String,
    /// Detected at
    pub detected_at: u64,
}

/// Compliance report
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ComplianceReport {
    /// Report ID
    pub id: String,
    /// Generated at
    pub generated_at: u64,
    /// Rules checked
    pub rules_checked: usize,
    /// Rules passed
    pub rules_passed: usize,
    /// Violations
    pub violations: Vec<ComplianceViolation>,
    /// Score (0-100)
    pub score: u8,
    /// Status
    pub status: ComplianceStatus,
}

/// Overall compliance status
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComplianceStatus {
    /// Fully compliant
    Compliant,
    /// Minor issues
    MinorIssues,
    /// Major issues
    MajorIssues,
    /// Non-compliant
    NonCompliant,
}

/// Compliance engine
pub struct ComplianceEngine {
    /// Rules
    rules: RwLock<Vec<ComplianceRule>>,
    /// Violation history
    violations: RwLock<Vec<ComplianceViolation>>,
    /// Next violation ID
    next_id: RwLock<u64>,
}

impl ComplianceEngine {
    /// Create a new compliance engine
    pub fn new() -> Self {
        Self {
            rules: RwLock::new(Vec::new()),
            violations: RwLock::new(Vec::new()),
            next_id: RwLock::new(1),
        }
    }

    /// Add a compliance rule
    pub fn add_rule(&self, rule: ComplianceRule) -> Result<(), GovernanceError> {
        self.rules.write().push(rule);
        Ok(())
    }

    /// Remove a rule
    pub fn remove_rule(&self, rule_id: &str) -> Result<(), GovernanceError> {
        self.rules.write().retain(|r| r.id != rule_id);
        Ok(())
    }

    /// Run all compliance checks
    pub fn check_all(&self) -> ComplianceReport {
        let rules = self.rules.read();
        let mut violations = Vec::new();
        let mut passed = 0;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        for rule in rules.iter() {
            if !rule.enabled {
                continue;
            }

            let check = self.run_check(rule);

            if check.passed {
                passed += 1;
            } else {
                let mut id = self.next_id.write();
                violations.push(ComplianceViolation {
                    id: format!("v_{}", *id),
                    rule_id: rule.id.clone(),
                    rule_name: rule.name.clone(),
                    frameworks: rule.frameworks.clone(),
                    severity: rule.severity.clone(),
                    description: check.message.clone(),
                    affected_keys: vec![],
                    remediation: self.get_remediation(rule),
                    detected_at: now,
                });
                *id += 1;
            }
        }

        let total = rules.len();
        let score = if total > 0 {
            ((passed * 100) / total) as u8
        } else {
            100
        };

        let status = if violations.is_empty() {
            ComplianceStatus::Compliant
        } else if violations
            .iter()
            .any(|v| v.severity == ViolationSeverity::Critical)
        {
            ComplianceStatus::NonCompliant
        } else if violations
            .iter()
            .any(|v| v.severity == ViolationSeverity::High)
        {
            ComplianceStatus::MajorIssues
        } else {
            ComplianceStatus::MinorIssues
        };

        ComplianceReport {
            id: format!("report_{}", now),
            generated_at: now,
            rules_checked: total,
            rules_passed: passed,
            violations,
            score,
            status,
        }
    }

    fn run_check(&self, rule: &ComplianceRule) -> ComplianceCheck {
        // Placeholder: in production, this would actually check the system
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        ComplianceCheck {
            rule_id: rule.id.clone(),
            passed: true, // Placeholder
            message: format!("Rule {} checked", rule.id),
            details: HashMap::new(),
            checked_at: now,
        }
    }

    fn get_remediation(&self, rule: &ComplianceRule) -> String {
        match &rule.check_type {
            CheckType::ClassificationRequired { .. } => {
                "Classify data according to sensitivity levels".to_string()
            }
            CheckType::EncryptionRequired { .. } => {
                "Enable encryption for sensitive data patterns".to_string()
            }
            CheckType::RetentionRequired { max_days, .. } => {
                format!("Set retention policy to {} days or less", max_days)
            }
            CheckType::AuditRequired { .. } => {
                "Enable audit logging for specified operations".to_string()
            }
            CheckType::MaskingRequired { .. } => {
                "Configure data masking for sensitive patterns".to_string()
            }
            CheckType::NoPiiPlaintext => "Encrypt or mask all PII data".to_string(),
            CheckType::GeoRestriction { .. } => {
                "Restrict data access to allowed regions".to_string()
            }
            CheckType::Custom { name, .. } => {
                format!("Review custom check: {}", name)
            }
        }
    }

    /// Get violation history
    pub fn get_violations(&self) -> Vec<ComplianceViolation> {
        self.violations.read().clone()
    }

    /// List all rules
    pub fn list_rules(&self) -> Vec<ComplianceRule> {
        self.rules.read().clone()
    }
}

impl Default for ComplianceEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_rule() {
        let engine = ComplianceEngine::new();

        let rule = ComplianceRule {
            id: "rule1".to_string(),
            name: "PII Classification".to_string(),
            description: "All PII must be classified".to_string(),
            frameworks: vec![Framework::Gdpr, Framework::Ccpa],
            check_type: CheckType::ClassificationRequired {
                patterns: vec!["user:*:pii".to_string()],
            },
            severity: ViolationSeverity::High,
            enabled: true,
            auto_remediate: false,
        };

        engine.add_rule(rule).unwrap();

        let rules = engine.list_rules();
        assert_eq!(rules.len(), 1);
    }

    #[test]
    fn test_check_all() {
        let engine = ComplianceEngine::new();

        let rule = ComplianceRule {
            id: "rule1".to_string(),
            name: "Test Rule".to_string(),
            description: "Test".to_string(),
            frameworks: vec![Framework::Gdpr],
            check_type: CheckType::NoPiiPlaintext,
            severity: ViolationSeverity::Medium,
            enabled: true,
            auto_remediate: false,
        };

        engine.add_rule(rule).unwrap();

        let report = engine.check_all();
        assert_eq!(report.rules_checked, 1);
    }

    #[test]
    fn test_compliance_status() {
        let report = ComplianceReport {
            id: "test".to_string(),
            generated_at: 0,
            rules_checked: 10,
            rules_passed: 10,
            violations: vec![],
            score: 100,
            status: ComplianceStatus::Compliant,
        };

        assert_eq!(report.status, ComplianceStatus::Compliant);
    }
}
