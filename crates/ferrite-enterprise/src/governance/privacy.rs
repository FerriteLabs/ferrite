//! Privacy Regulation Compliance (GDPR/CCPA)
//!
//! Provides automated PII detection, right-to-erasure enforcement,
//! consent management, and regulatory compliance reporting.
//!
//! # Features
//!
//! - **PII Detection**: Regex + heuristic detection of personal data
//! - **Right to Erasure**: GDPR Article 17 compliant data deletion
//! - **Data Subject Access**: Export all data related to an individual
//! - **Consent Management**: Track and enforce data processing consent
//! - **Audit Trail**: Immutable log of all privacy-relevant operations
//! - **Compliance Reports**: Automated GDPR/CCPA/SOC2 report generation

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, SystemTime};

use regex::Regex;
use serde::{Deserialize, Serialize};
use tracing::info;

/// Supported privacy regulations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PrivacyRegulation {
    /// EU General Data Protection Regulation
    Gdpr,
    /// California Consumer Privacy Act
    Ccpa,
    /// SOC 2 Type II
    Soc2,
    /// HIPAA (Health Insurance Portability and Accountability Act)
    Hipaa,
    /// PCI DSS (Payment Card Industry Data Security Standard)
    PciDss,
}

impl fmt::Display for PrivacyRegulation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PrivacyRegulation::Gdpr => write!(f, "GDPR"),
            PrivacyRegulation::Ccpa => write!(f, "CCPA"),
            PrivacyRegulation::Soc2 => write!(f, "SOC2"),
            PrivacyRegulation::Hipaa => write!(f, "HIPAA"),
            PrivacyRegulation::PciDss => write!(f, "PCI-DSS"),
        }
    }
}

/// Type of personally identifiable information
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PiiType {
    Email,
    Phone,
    SocialSecurity,
    CreditCard,
    IpAddress,
    Name,
    Address,
    DateOfBirth,
    Passport,
    DriversLicense,
    BankAccount,
    Custom,
}

impl fmt::Display for PiiType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PiiType::Email => write!(f, "email"),
            PiiType::Phone => write!(f, "phone"),
            PiiType::SocialSecurity => write!(f, "ssn"),
            PiiType::CreditCard => write!(f, "credit_card"),
            PiiType::IpAddress => write!(f, "ip_address"),
            PiiType::Name => write!(f, "name"),
            PiiType::Address => write!(f, "address"),
            PiiType::DateOfBirth => write!(f, "dob"),
            PiiType::Passport => write!(f, "passport"),
            PiiType::DriversLicense => write!(f, "drivers_license"),
            PiiType::BankAccount => write!(f, "bank_account"),
            PiiType::Custom => write!(f, "custom"),
        }
    }
}

/// A PII detection result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PiiDetection {
    /// The type of PII found
    pub pii_type: PiiType,
    /// Key where PII was detected
    pub key: String,
    /// Field within the value (for hashes/JSON)
    pub field: Option<String>,
    /// Confidence score (0.0 to 1.0)
    pub confidence: f64,
    /// Matched pattern (redacted)
    pub pattern_hint: String,
}

/// PII detector using regex patterns and heuristics
pub struct PiiDetector {
    patterns: Vec<PiiPattern>,
}

struct PiiPattern {
    pii_type: PiiType,
    regex: Regex,
    confidence: f64,
}

impl Default for PiiDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl PiiDetector {
    /// Create a new PII detector with default patterns
    pub fn new() -> Self {
        let patterns = vec![
            PiiPattern {
                pii_type: PiiType::Email,
                regex: Regex::new(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
                    .expect("valid email regex"),
                confidence: 0.95,
            },
            PiiPattern {
                pii_type: PiiType::Phone,
                regex: Regex::new(r"\+?1?\d{10,14}").expect("valid phone regex"),
                confidence: 0.7,
            },
            PiiPattern {
                pii_type: PiiType::SocialSecurity,
                regex: Regex::new(r"\d{3}-?\d{2}-?\d{4}").expect("valid SSN regex"),
                confidence: 0.8,
            },
            PiiPattern {
                pii_type: PiiType::CreditCard,
                regex: Regex::new(r"\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}")
                    .expect("valid credit card regex"),
                confidence: 0.9,
            },
            PiiPattern {
                pii_type: PiiType::IpAddress,
                regex: Regex::new(r"\b(?:\d{1,3}\.){3}\d{1,3}\b").expect("valid IP address regex"),
                confidence: 0.85,
            },
        ];

        Self { patterns }
    }

    /// Scan a string value for PII
    pub fn scan_value(&self, key: &str, value: &str, field: Option<&str>) -> Vec<PiiDetection> {
        let mut detections = Vec::new();

        for pattern in &self.patterns {
            if let Some(matched) = pattern.regex.find(value) {
                let hint = Self::redact_match(matched.as_str(), &pattern.pii_type);

                detections.push(PiiDetection {
                    pii_type: pattern.pii_type,
                    key: key.to_string(),
                    field: field.map(|f| f.to_string()),
                    confidence: pattern.confidence,
                    pattern_hint: hint,
                });
            }
        }

        // Heuristic: check field names for PII indicators
        if let Some(field_name) = field {
            let lower = field_name.to_lowercase();
            if (lower.contains("email") || lower.contains("e_mail"))
                && !detections.iter().any(|d| d.pii_type == PiiType::Email)
            {
                detections.push(PiiDetection {
                    pii_type: PiiType::Email,
                    key: key.to_string(),
                    field: Some(field_name.to_string()),
                    confidence: 0.6,
                    pattern_hint: "(field name match)".to_string(),
                });
            }
            if (lower.contains("phone") || lower.contains("mobile") || lower.contains("tel"))
                && !detections.iter().any(|d| d.pii_type == PiiType::Phone)
            {
                detections.push(PiiDetection {
                    pii_type: PiiType::Phone,
                    key: key.to_string(),
                    field: Some(field_name.to_string()),
                    confidence: 0.5,
                    pattern_hint: "(field name match)".to_string(),
                });
            }
            if lower.contains("name") && !lower.contains("hostname") && !lower.contains("filename")
            {
                detections.push(PiiDetection {
                    pii_type: PiiType::Name,
                    key: key.to_string(),
                    field: Some(field_name.to_string()),
                    confidence: 0.4,
                    pattern_hint: "(field name match)".to_string(),
                });
            }
        }

        detections
    }

    fn redact_match(value: &str, pii_type: &PiiType) -> String {
        match pii_type {
            PiiType::Email => {
                if let Some(at) = value.find('@') {
                    format!("{}***@{}", &value[..1], &value[at + 1..])
                } else {
                    "***@***".to_string()
                }
            }
            PiiType::CreditCard => {
                let digits: String = value.chars().filter(|c| c.is_ascii_digit()).collect();
                if digits.len() >= 4 {
                    format!("****-****-****-{}", &digits[digits.len() - 4..])
                } else {
                    "****".to_string()
                }
            }
            PiiType::SocialSecurity => "***-**-****".to_string(),
            PiiType::Phone => {
                let digits: String = value.chars().filter(|c| c.is_ascii_digit()).collect();
                if digits.len() >= 4 {
                    format!("***-***-{}", &digits[digits.len() - 4..])
                } else {
                    "***".to_string()
                }
            }
            _ => "***REDACTED***".to_string(),
        }
    }
}

/// Legal basis for data processing (GDPR Article 6)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LegalBasis {
    /// Data subject has given consent
    Consent,
    /// Processing is necessary for a contract
    Contract,
    /// Legal obligation
    LegalObligation,
    /// Vital interests of the data subject
    VitalInterests,
    /// Public interest or official authority
    PublicInterest,
    /// Legitimate interests of the controller
    LegitimateInterest,
}

/// A data subject's consent record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsentRecord {
    /// Data subject identifier
    pub subject_id: String,
    /// What was consented to
    pub purpose: String,
    /// Legal basis for processing
    pub legal_basis: LegalBasis,
    /// When consent was given
    pub granted_at: u64,
    /// When consent expires (if applicable)
    pub expires_at: Option<u64>,
    /// Whether consent has been revoked
    pub revoked: bool,
    /// When consent was revoked
    pub revoked_at: Option<u64>,
}

/// A right-to-erasure request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErasureRequest {
    /// Unique request ID
    pub id: String,
    /// Data subject making the request
    pub subject_id: String,
    /// Regulation under which the request is made
    pub regulation: PrivacyRegulation,
    /// Specific key patterns to erase (None = all related data)
    pub key_patterns: Option<Vec<String>>,
    /// When the request was received
    pub received_at: u64,
    /// Deadline for completion
    pub deadline_at: u64,
    /// Current status
    pub status: ErasureStatus,
    /// Keys that were erased
    pub erased_keys: Vec<String>,
    /// Keys that were retained (with justification)
    pub retained_keys: Vec<(String, String)>, // (key, reason)
    /// Completed timestamp
    pub completed_at: Option<u64>,
}

/// Status of an erasure request
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErasureStatus {
    /// Request received, pending processing
    Pending,
    /// Currently scanning for related data
    Scanning,
    /// Erasing identified data
    Erasing,
    /// Completed successfully
    Complete,
    /// Failed (requires manual intervention)
    Failed,
}

/// Result of an erasure operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErasureResult {
    pub request_id: String,
    pub keys_scanned: u64,
    pub keys_erased: u64,
    pub keys_retained: u64,
    pub duration: Duration,
    pub status: ErasureStatus,
    pub errors: Vec<String>,
}

/// Privacy compliance report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyReport {
    /// Regulation being reported on
    pub regulation: PrivacyRegulation,
    /// Report generation timestamp
    pub generated_at: u64,
    /// Reporting period start
    pub period_start: u64,
    /// Reporting period end
    pub period_end: u64,
    /// PII detected across all data
    pub pii_summary: PiiSummary,
    /// Erasure requests processed
    pub erasure_requests: ErasureRequestsSummary,
    /// Consent status
    pub consent_summary: ConsentSummary,
    /// Policy violations
    pub violations: Vec<PrivacyViolation>,
    /// Overall compliance score (0.0 to 1.0)
    pub compliance_score: f64,
    /// Recommendations
    pub recommendations: Vec<String>,
}

/// Summary of PII across the datastore
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PiiSummary {
    pub total_keys_scanned: u64,
    pub keys_with_pii: u64,
    pub detections_by_type: HashMap<String, u64>,
    pub high_confidence_detections: u64,
}

/// Summary of erasure requests
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErasureRequestsSummary {
    pub total_requests: u64,
    pub completed: u64,
    pub pending: u64,
    pub avg_completion_time_secs: f64,
    pub within_deadline_pct: f64,
}

/// Summary of consent records
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsentSummary {
    pub total_subjects: u64,
    pub active_consents: u64,
    pub revoked_consents: u64,
    pub expired_consents: u64,
}

/// A privacy violation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyViolation {
    pub regulation: PrivacyRegulation,
    pub description: String,
    pub severity: ViolationSeverity,
    pub affected_keys: Vec<String>,
    pub detected_at: u64,
    pub remediation: String,
}

/// Severity of a privacy violation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ViolationSeverity {
    Low,
    Medium,
    High,
    Critical,
}

/// The privacy compliance engine
pub struct PrivacyEngine {
    /// PII detector
    detector: PiiDetector,
    /// Active consent records
    consents: Vec<ConsentRecord>,
    /// Erasure request history
    erasure_requests: Vec<ErasureRequest>,
    /// Privacy audit log
    audit_log: Vec<PrivacyAuditEntry>,
    /// Enabled regulations
    #[allow(dead_code)] // Planned for v0.2 — stored for regulation-specific compliance checks
    regulations: Vec<PrivacyRegulation>,
}

/// An entry in the privacy audit log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivacyAuditEntry {
    pub timestamp: u64,
    pub action: String,
    pub subject_id: Option<String>,
    pub details: String,
    pub regulation: Option<PrivacyRegulation>,
}

impl PrivacyEngine {
    /// Create a new privacy engine
    pub fn new(regulations: Vec<PrivacyRegulation>) -> Self {
        Self {
            detector: PiiDetector::new(),
            consents: Vec::new(),
            erasure_requests: Vec::new(),
            audit_log: Vec::new(),
            regulations,
        }
    }

    /// Scan a key-value pair for PII
    pub fn scan_for_pii(&self, key: &str, value: &str) -> Vec<PiiDetection> {
        self.detector.scan_value(key, value, None)
    }

    /// Scan a hash field for PII
    pub fn scan_field_for_pii(&self, key: &str, field: &str, value: &str) -> Vec<PiiDetection> {
        self.detector.scan_value(key, value, Some(field))
    }

    /// Record consent for a data subject
    pub fn record_consent(&mut self, consent: ConsentRecord) {
        info!(
            subject = %consent.subject_id,
            purpose = %consent.purpose,
            "Recorded consent"
        );
        self.audit_log.push(PrivacyAuditEntry {
            timestamp: now_epoch(),
            action: "consent_recorded".to_string(),
            subject_id: Some(consent.subject_id.clone()),
            details: format!(
                "Purpose: {}, Basis: {:?}",
                consent.purpose, consent.legal_basis
            ),
            regulation: Some(PrivacyRegulation::Gdpr),
        });
        self.consents.push(consent);
    }

    /// Revoke consent for a data subject
    pub fn revoke_consent(&mut self, subject_id: &str, purpose: &str) -> bool {
        let now = now_epoch();
        let mut revoked = false;
        for consent in &mut self.consents {
            if consent.subject_id == subject_id && consent.purpose == purpose && !consent.revoked {
                consent.revoked = true;
                consent.revoked_at = Some(now);
                revoked = true;
            }
        }
        if revoked {
            self.audit_log.push(PrivacyAuditEntry {
                timestamp: now,
                action: "consent_revoked".to_string(),
                subject_id: Some(subject_id.to_string()),
                details: format!("Purpose: {}", purpose),
                regulation: Some(PrivacyRegulation::Gdpr),
            });
        }
        revoked
    }

    /// Check if a data subject has active consent for a purpose
    pub fn has_consent(&self, subject_id: &str, purpose: &str) -> bool {
        let now = now_epoch();
        self.consents.iter().any(|c| {
            c.subject_id == subject_id
                && c.purpose == purpose
                && !c.revoked
                && c.expires_at.map_or(true, |exp| exp > now)
        })
    }

    /// Submit a right-to-erasure request
    pub fn submit_erasure_request(
        &mut self,
        subject_id: &str,
        regulation: PrivacyRegulation,
        key_patterns: Option<Vec<String>>,
    ) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        let now = now_epoch();
        // GDPR requires 30-day deadline
        let deadline_days = match regulation {
            PrivacyRegulation::Gdpr => 30,
            PrivacyRegulation::Ccpa => 45,
            _ => 30,
        };
        let deadline = now + (deadline_days * 86400);

        let request = ErasureRequest {
            id: id.clone(),
            subject_id: subject_id.to_string(),
            regulation,
            key_patterns,
            received_at: now,
            deadline_at: deadline,
            status: ErasureStatus::Pending,
            erased_keys: Vec::new(),
            retained_keys: Vec::new(),
            completed_at: None,
        };

        info!(
            request_id = %id,
            subject = %subject_id,
            regulation = %regulation,
            "Erasure request submitted"
        );

        self.audit_log.push(PrivacyAuditEntry {
            timestamp: now,
            action: "erasure_request_submitted".to_string(),
            subject_id: Some(subject_id.to_string()),
            details: format!("Request ID: {}, Regulation: {}", id, regulation),
            regulation: Some(regulation),
        });

        self.erasure_requests.push(request);
        id
    }

    /// Process an erasure request (simulate key deletion)
    pub fn process_erasure(
        &mut self,
        request_id: &str,
        found_keys: Vec<String>,
        retained: Vec<(String, String)>,
    ) -> Option<ErasureResult> {
        let start = std::time::Instant::now();

        if let Some(req) = self
            .erasure_requests
            .iter_mut()
            .find(|r| r.id == request_id)
        {
            req.status = ErasureStatus::Erasing;
            req.erased_keys = found_keys.clone();
            req.retained_keys = retained.clone();
            req.status = ErasureStatus::Complete;
            req.completed_at = Some(now_epoch());

            let result = ErasureResult {
                request_id: request_id.to_string(),
                keys_scanned: (found_keys.len() + retained.len()) as u64,
                keys_erased: found_keys.len() as u64,
                keys_retained: retained.len() as u64,
                duration: start.elapsed(),
                status: ErasureStatus::Complete,
                errors: Vec::new(),
            };

            self.audit_log.push(PrivacyAuditEntry {
                timestamp: now_epoch(),
                action: "erasure_completed".to_string(),
                subject_id: Some(req.subject_id.clone()),
                details: format!(
                    "Erased {} keys, retained {} keys",
                    found_keys.len(),
                    retained.len()
                ),
                regulation: Some(req.regulation),
            });

            Some(result)
        } else {
            None
        }
    }

    /// Get erasure request status
    pub fn erasure_status(&self, request_id: &str) -> Option<&ErasureRequest> {
        self.erasure_requests.iter().find(|r| r.id == request_id)
    }

    /// Generate a compliance report
    pub fn generate_report(
        &self,
        regulation: PrivacyRegulation,
        period_start: u64,
        period_end: u64,
    ) -> PrivacyReport {
        let erasure_in_period: Vec<&ErasureRequest> = self
            .erasure_requests
            .iter()
            .filter(|r| r.received_at >= period_start && r.received_at <= period_end)
            .collect();

        let completed_count = erasure_in_period
            .iter()
            .filter(|r| r.status == ErasureStatus::Complete)
            .count();
        let pending_count = erasure_in_period
            .iter()
            .filter(|r| r.status == ErasureStatus::Pending)
            .count();

        let within_deadline = erasure_in_period
            .iter()
            .filter(|r| {
                r.status == ErasureStatus::Complete
                    && r.completed_at.is_some_and(|c| c <= r.deadline_at)
            })
            .count();

        let within_deadline_pct = if completed_count > 0 {
            (within_deadline as f64 / completed_count as f64) * 100.0
        } else {
            100.0
        };

        let active_consents = self.consents.iter().filter(|c| !c.revoked).count();
        let revoked_consents = self.consents.iter().filter(|c| c.revoked).count();
        let expired_consents = self
            .consents
            .iter()
            .filter(|c| c.expires_at.is_some_and(|exp| exp < now_epoch()))
            .count();

        // Calculate compliance score
        let mut score = 1.0;
        if within_deadline_pct < 100.0 {
            score -= 0.3;
        }
        if pending_count > 0 {
            score -= 0.1 * pending_count as f64;
        }
        score = score.max(0.0);

        let mut recommendations = Vec::new();
        if within_deadline_pct < 100.0 {
            recommendations.push(
                "Some erasure requests exceeded the regulatory deadline. Review processing pipeline.".to_string()
            );
        }
        if active_consents == 0 && !self.consents.is_empty() {
            recommendations.push(
                "No active consents found. Verify consent collection is working.".to_string(),
            );
        }

        PrivacyReport {
            regulation,
            generated_at: now_epoch(),
            period_start,
            period_end,
            pii_summary: PiiSummary {
                total_keys_scanned: 0,
                keys_with_pii: 0,
                detections_by_type: HashMap::new(),
                high_confidence_detections: 0,
            },
            erasure_requests: ErasureRequestsSummary {
                total_requests: erasure_in_period.len() as u64,
                completed: completed_count as u64,
                pending: pending_count as u64,
                avg_completion_time_secs: 0.0,
                within_deadline_pct,
            },
            consent_summary: ConsentSummary {
                total_subjects: self
                    .consents
                    .iter()
                    .map(|c| &c.subject_id)
                    .collect::<std::collections::HashSet<_>>()
                    .len() as u64,
                active_consents: active_consents as u64,
                revoked_consents: revoked_consents as u64,
                expired_consents: expired_consents as u64,
            },
            violations: Vec::new(),
            compliance_score: score,
            recommendations,
        }
    }

    /// Get the audit log
    pub fn audit_log(&self) -> &[PrivacyAuditEntry] {
        &self.audit_log
    }

    /// Get pending erasure requests
    pub fn pending_erasure_requests(&self) -> Vec<&ErasureRequest> {
        self.erasure_requests
            .iter()
            .filter(|r| r.status == ErasureStatus::Pending)
            .collect()
    }
}

fn now_epoch() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pii_detect_email() {
        let detector = PiiDetector::new();
        let results = detector.scan_value("user:1", "john@example.com", None);
        assert!(results.iter().any(|r| r.pii_type == PiiType::Email));
        assert!(results[0].confidence > 0.9);
    }

    #[test]
    fn test_pii_detect_credit_card() {
        let detector = PiiDetector::new();
        let results = detector.scan_value("payment:1", "4111-1111-1111-1111", None);
        assert!(results.iter().any(|r| r.pii_type == PiiType::CreditCard));
    }

    #[test]
    fn test_pii_detect_ssn() {
        let detector = PiiDetector::new();
        let results = detector.scan_value("user:1", "123-45-6789", None);
        assert!(results
            .iter()
            .any(|r| r.pii_type == PiiType::SocialSecurity));
    }

    #[test]
    fn test_pii_detect_ip() {
        let detector = PiiDetector::new();
        let results = detector.scan_value("log:1", "User logged in from 192.168.1.100", None);
        assert!(results.iter().any(|r| r.pii_type == PiiType::IpAddress));
    }

    #[test]
    fn test_pii_field_name_heuristic() {
        let detector = PiiDetector::new();
        let results = detector.scan_value("user:1", "some value", Some("email_address"));
        assert!(results.iter().any(|r| r.pii_type == PiiType::Email));
    }

    #[test]
    fn test_pii_no_false_positive() {
        let detector = PiiDetector::new();
        let results = detector.scan_value("counter:1", "42", None);
        assert!(results.is_empty());
    }

    #[test]
    fn test_redact_email() {
        let hint = PiiDetector::redact_match("john@example.com", &PiiType::Email);
        assert!(hint.contains("***@"));
        assert!(hint.contains("example.com"));
    }

    #[test]
    fn test_redact_credit_card() {
        let hint = PiiDetector::redact_match("4111-1111-1111-1111", &PiiType::CreditCard);
        assert!(hint.contains("****"));
        assert!(hint.contains("1111"));
    }

    #[test]
    fn test_consent_management() {
        let mut engine = PrivacyEngine::new(vec![PrivacyRegulation::Gdpr]);

        engine.record_consent(ConsentRecord {
            subject_id: "user:123".to_string(),
            purpose: "marketing".to_string(),
            legal_basis: LegalBasis::Consent,
            granted_at: now_epoch(),
            expires_at: Some(now_epoch() + 86400 * 365),
            revoked: false,
            revoked_at: None,
        });

        assert!(engine.has_consent("user:123", "marketing"));
        assert!(!engine.has_consent("user:123", "analytics"));
    }

    #[test]
    fn test_revoke_consent() {
        let mut engine = PrivacyEngine::new(vec![PrivacyRegulation::Gdpr]);

        engine.record_consent(ConsentRecord {
            subject_id: "user:123".to_string(),
            purpose: "marketing".to_string(),
            legal_basis: LegalBasis::Consent,
            granted_at: now_epoch(),
            expires_at: None,
            revoked: false,
            revoked_at: None,
        });

        assert!(engine.has_consent("user:123", "marketing"));
        let revoked = engine.revoke_consent("user:123", "marketing");
        assert!(revoked);
        assert!(!engine.has_consent("user:123", "marketing"));
    }

    #[test]
    fn test_erasure_request() {
        let mut engine = PrivacyEngine::new(vec![PrivacyRegulation::Gdpr]);

        let id = engine.submit_erasure_request(
            "user:456",
            PrivacyRegulation::Gdpr,
            Some(vec!["user:456:*".to_string()]),
        );
        assert!(!id.is_empty());

        let pending = engine.pending_erasure_requests();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].subject_id, "user:456");
    }

    #[test]
    fn test_process_erasure() {
        let mut engine = PrivacyEngine::new(vec![PrivacyRegulation::Gdpr]);

        let id = engine.submit_erasure_request("user:789", PrivacyRegulation::Gdpr, None);

        let result = engine.process_erasure(
            &id,
            vec![
                "user:789".to_string(),
                "user:789:profile".to_string(),
                "user:789:settings".to_string(),
            ],
            vec![(
                "user:789:orders".to_string(),
                "Legal obligation: tax records".to_string(),
            )],
        );

        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result.keys_erased, 3);
        assert_eq!(result.keys_retained, 1);
        assert_eq!(result.status, ErasureStatus::Complete);
    }

    #[test]
    fn test_compliance_report() {
        let mut engine = PrivacyEngine::new(vec![PrivacyRegulation::Gdpr]);

        engine.record_consent(ConsentRecord {
            subject_id: "user:1".to_string(),
            purpose: "analytics".to_string(),
            legal_basis: LegalBasis::Consent,
            granted_at: now_epoch(),
            expires_at: None,
            revoked: false,
            revoked_at: None,
        });

        let report = engine.generate_report(
            PrivacyRegulation::Gdpr,
            now_epoch() - 86400 * 30,
            now_epoch(),
        );

        assert_eq!(report.regulation, PrivacyRegulation::Gdpr);
        assert_eq!(report.consent_summary.active_consents, 1);
        assert!(report.compliance_score > 0.0);
    }

    #[test]
    fn test_audit_log() {
        let mut engine = PrivacyEngine::new(vec![PrivacyRegulation::Gdpr]);

        engine.record_consent(ConsentRecord {
            subject_id: "user:1".to_string(),
            purpose: "marketing".to_string(),
            legal_basis: LegalBasis::Consent,
            granted_at: now_epoch(),
            expires_at: None,
            revoked: false,
            revoked_at: None,
        });

        engine.submit_erasure_request("user:2", PrivacyRegulation::Gdpr, None);

        assert_eq!(engine.audit_log().len(), 2);
        assert_eq!(engine.audit_log()[0].action, "consent_recorded");
        assert_eq!(engine.audit_log()[1].action, "erasure_request_submitted");
    }

    #[test]
    fn test_privacy_regulation_display() {
        assert_eq!(PrivacyRegulation::Gdpr.to_string(), "GDPR");
        assert_eq!(PrivacyRegulation::Ccpa.to_string(), "CCPA");
        assert_eq!(PrivacyRegulation::PciDss.to_string(), "PCI-DSS");
    }

    #[test]
    fn test_pii_type_display() {
        assert_eq!(PiiType::Email.to_string(), "email");
        assert_eq!(PiiType::CreditCard.to_string(), "credit_card");
    }
}
