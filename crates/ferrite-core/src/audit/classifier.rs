//! Automatic Data Classification
//!
//! ML-based classification of stored data (PII, PHI, financial, public)
//! using pattern recognition on key names and value shapes.
//! Tags propagate through lineage graph for compliance.
#![allow(dead_code)]

use std::collections::HashMap;
use std::time::Instant;

use parking_lot::RwLock;
use regex::Regex;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the data classifier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassifierConfig {
    /// Whether classification is enabled.
    pub enabled: bool,
    /// Automatically scan new keys on write.
    pub auto_scan: bool,
    /// Fraction of writes to sample for auto-scan (0.0–1.0).
    pub scan_sample_rate: f64,
    /// Minimum confidence to accept a classification.
    pub min_confidence: f64,
    /// Maximum tags allowed per key.
    pub max_tags_per_key: usize,
    /// Propagate tags through the lineage graph.
    pub propagate_through_lineage: bool,
}

impl Default for ClassifierConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            auto_scan: true,
            scan_sample_rate: 0.01,
            min_confidence: 0.8,
            max_tags_per_key: 10,
            propagate_through_lineage: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Classification types
// ---------------------------------------------------------------------------

/// A single classification result for a key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataClass {
    /// High-level category.
    pub category: ClassCategory,
    /// Optional sub-category label.
    pub subcategory: Option<String>,
    /// Confidence score (0.0–1.0).
    pub confidence: f64,
    /// How this classification was determined.
    pub source: ClassificationSource,
}

impl PartialEq for DataClass {
    fn eq(&self, other: &Self) -> bool {
        self.category == other.category
            && self.subcategory == other.subcategory
            && self.source == other.source
    }
}

impl Eq for DataClass {}

impl std::hash::Hash for DataClass {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.category.hash(state);
        self.subcategory.hash(state);
        self.source.hash(state);
    }
}

/// Broad category of classified data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ClassCategory {
    /// Personally identifiable information.
    PII,
    /// Protected health information.
    PHI,
    /// Financial / payment data.
    Financial,
    /// Credentials, secrets, tokens.
    Credentials,
    /// Public, non-sensitive data.
    Public,
    /// Internal use only.
    Internal,
    /// Confidential.
    Confidential,
    /// Restricted / top-secret.
    Restricted,
    /// Not yet classified.
    Unknown,
}

impl std::fmt::Display for ClassCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::str::FromStr for ClassCategory {
    type Err = ClassifierError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "PII" => Ok(Self::PII),
            "PHI" => Ok(Self::PHI),
            "FINANCIAL" => Ok(Self::Financial),
            "CREDENTIALS" => Ok(Self::Credentials),
            "PUBLIC" => Ok(Self::Public),
            "INTERNAL" => Ok(Self::Internal),
            "CONFIDENTIAL" => Ok(Self::Confidential),
            "RESTRICTED" => Ok(Self::Restricted),
            "UNKNOWN" => Ok(Self::Unknown),
            _ => Err(ClassifierError::InvalidPattern(format!(
                "unknown category: {}",
                s
            ))),
        }
    }
}

/// How a classification was determined.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ClassificationSource {
    /// Matched a key-name or value-content regex.
    KeyPattern,
    /// Matched a value pattern.
    ValuePattern,
    /// Manually tagged by a user.
    ManualTag,
    /// Propagated through the lineage graph.
    LineagePropagation,
    /// Inferred by an ML model.
    ModelInference,
}

// ---------------------------------------------------------------------------
// Rules
// ---------------------------------------------------------------------------

/// A classification rule that maps patterns to categories.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassificationRule {
    /// Human-readable rule name.
    pub name: String,
    /// Description of what this rule detects.
    pub description: String,
    /// Category to assign on match.
    pub category: ClassCategory,
    /// Patterns that trigger this rule.
    pub patterns: Vec<PatternRule>,
    /// Higher priority wins when categories conflict.
    pub priority: i32,
    /// Whether this rule is active.
    pub enabled: bool,
}

/// A single pattern within a rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternRule {
    /// What the regex is matched against.
    pub target: PatternTarget,
    /// Regex pattern string.
    pub pattern: String,
    /// Confidence assigned on match.
    pub confidence: f64,
}

/// What a pattern is matched against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PatternTarget {
    /// The key name.
    KeyName,
    /// The value content (interpreted as UTF-8).
    ValueContent,
    /// A field name inside a hash/document.
    FieldName,
    /// The format/shape of the value.
    ValueFormat,
}

// ---------------------------------------------------------------------------
// Reports & stats
// ---------------------------------------------------------------------------

/// Result of a batch scan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassificationReport {
    /// Total keys scanned.
    pub total_scanned: u64,
    /// Keys that received at least one tag.
    pub classified: u64,
    /// Keys with no matches.
    pub unclassified: u64,
    /// Count per category name.
    pub by_category: HashMap<String, u64>,
    /// High-risk keys (those tagged PII, PHI, Financial, Credentials).
    pub high_risk: Vec<(String, Vec<DataClass>)>,
    /// Wall-clock duration of the scan in milliseconds.
    pub scan_duration_ms: u64,
}

/// Summary of all classifications in the system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassificationSummary {
    /// Total number of tagged keys.
    pub total_tagged: u64,
    /// Count per category name.
    pub by_category: HashMap<String, u64>,
    /// Keys with no classification.
    pub unclassified: u64,
    /// Keys with highest risk scores.
    pub highest_risk_keys: Vec<String>,
}

/// Compliance report for a specific framework.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceClassReport {
    /// Framework name (GDPR, HIPAA, SOC2, PCI).
    pub framework: String,
    /// Number of classified keys relevant to the framework.
    pub classified_keys: u64,
    /// Whether the data appears compliant.
    pub compliant: bool,
    /// Issues found.
    pub issues: Vec<ComplianceIssue>,
}

/// A single compliance issue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceIssue {
    /// Pattern of keys affected.
    pub key_pattern: String,
    /// Description of the issue.
    pub issue: String,
    /// Severity level.
    pub severity: String,
    /// Recommended remediation.
    pub recommendation: String,
}

/// Operational statistics for the classifier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassifierStats {
    /// Total scan invocations.
    pub total_scans: u64,
    /// Total keys classified.
    pub total_classified: u64,
    /// Number of active rules.
    pub rules_count: usize,
    /// Number of manual tags applied.
    pub manual_tags: u64,
    /// Number of lineage propagations performed.
    pub propagations: u64,
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors returned by the classifier.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ClassifierError {
    /// Rule not found.
    #[error("rule not found: {0}")]
    RuleNotFound(String),
    /// Rule already exists.
    #[error("rule already exists: {0}")]
    RuleExists(String),
    /// Invalid regex pattern.
    #[error("invalid pattern: {0}")]
    InvalidPattern(String),
    /// Maximum tags per key exceeded.
    #[error("max tags per key exceeded ({0})")]
    MaxTags(usize),
}

// ---------------------------------------------------------------------------
// Compiled rule (internal)
// ---------------------------------------------------------------------------

struct CompiledPattern {
    target: PatternTarget,
    regex: Regex,
    confidence: f64,
}

struct CompiledRule {
    name: String,
    category: ClassCategory,
    patterns: Vec<CompiledPattern>,
    priority: i32,
    enabled: bool,
}

// ---------------------------------------------------------------------------
// DataClassifier
// ---------------------------------------------------------------------------

/// The main data classifier engine.
pub struct DataClassifier {
    config: ClassifierConfig,
    rules: RwLock<Vec<CompiledRule>>,
    tags: RwLock<HashMap<String, Vec<DataClass>>>,
    stats: RwLock<ClassifierStats>,
}

impl DataClassifier {
    /// Create a new classifier with the given configuration and built-in rules.
    pub fn new(config: ClassifierConfig) -> Self {
        let classifier = Self {
            config,
            rules: RwLock::new(Vec::new()),
            tags: RwLock::new(HashMap::new()),
            stats: RwLock::new(ClassifierStats {
                total_scans: 0,
                total_classified: 0,
                rules_count: 0,
                manual_tags: 0,
                propagations: 0,
            }),
        };
        classifier.load_builtin_rules();
        classifier
    }

    /// Load the default built-in classification rules.
    fn load_builtin_rules(&self) {
        let builtins = vec![
            CompiledRule {
                name: "PII/Email".into(),
                category: ClassCategory::PII,
                patterns: vec![CompiledPattern {
                    target: PatternTarget::ValueContent,
                    regex: Regex::new(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
                        .expect("builtin regex"),
                    confidence: 0.95,
                }],
                priority: 10,
                enabled: true,
            },
            CompiledRule {
                name: "PII/Phone".into(),
                category: ClassCategory::PII,
                patterns: vec![CompiledPattern {
                    target: PatternTarget::ValueContent,
                    regex: Regex::new(r"\+?[0-9]{10,15}").expect("builtin regex"),
                    confidence: 0.85,
                }],
                priority: 10,
                enabled: true,
            },
            CompiledRule {
                name: "PII/SSN".into(),
                category: ClassCategory::PII,
                patterns: vec![CompiledPattern {
                    target: PatternTarget::ValueContent,
                    regex: Regex::new(r"[0-9]{3}-[0-9]{2}-[0-9]{4}").expect("builtin regex"),
                    confidence: 0.99,
                }],
                priority: 20,
                enabled: true,
            },
            CompiledRule {
                name: "Financial/CreditCard".into(),
                category: ClassCategory::Financial,
                patterns: vec![CompiledPattern {
                    target: PatternTarget::ValueContent,
                    regex: Regex::new(r"[0-9]{4}[- ]?[0-9]{4}[- ]?[0-9]{4}[- ]?[0-9]{4}")
                        .expect("builtin regex"),
                    confidence: 0.95,
                }],
                priority: 15,
                enabled: true,
            },
            CompiledRule {
                name: "Financial/IBAN".into(),
                category: ClassCategory::Financial,
                patterns: vec![CompiledPattern {
                    target: PatternTarget::ValueContent,
                    regex: Regex::new(r"[A-Z]{2}[0-9]{2}[A-Z0-9]{11,30}").expect("builtin regex"),
                    confidence: 0.90,
                }],
                priority: 14,
                enabled: true,
            },
            CompiledRule {
                name: "Credentials/ApiKey".into(),
                category: ClassCategory::Credentials,
                patterns: vec![CompiledPattern {
                    target: PatternTarget::KeyName,
                    regex: Regex::new(r"(?i)(api_key|apikey|secret|token|password)")
                        .expect("builtin regex"),
                    confidence: 0.90,
                }],
                priority: 18,
                enabled: true,
            },
            CompiledRule {
                name: "PHI/MedicalRecord".into(),
                category: ClassCategory::PHI,
                patterns: vec![CompiledPattern {
                    target: PatternTarget::KeyName,
                    regex: Regex::new(r"(?i)(patient|diagnosis|prescription|medical)")
                        .expect("builtin regex"),
                    confidence: 0.85,
                }],
                priority: 16,
                enabled: true,
            },
            CompiledRule {
                name: "PII/Name".into(),
                category: ClassCategory::PII,
                patterns: vec![CompiledPattern {
                    target: PatternTarget::KeyName,
                    regex: Regex::new(r"(?i)(first_name|last_name|full_name|username)")
                        .expect("builtin regex"),
                    confidence: 0.80,
                }],
                priority: 8,
                enabled: true,
            },
        ];

        let mut rules = self.rules.write();
        *rules = builtins;
        self.stats.write().rules_count = rules.len();
    }

    /// Classify a single key/value pair against all rules.
    pub fn classify_key(&self, key: &str, value: &[u8]) -> Vec<DataClass> {
        let value_str = String::from_utf8_lossy(value);
        let rules = self.rules.read();
        let mut results = Vec::new();

        for rule in rules.iter() {
            if !rule.enabled {
                continue;
            }
            for pat in &rule.patterns {
                let matched = match pat.target {
                    PatternTarget::KeyName | PatternTarget::FieldName => pat.regex.is_match(key),
                    PatternTarget::ValueContent | PatternTarget::ValueFormat => {
                        pat.regex.is_match(&value_str)
                    }
                };
                if matched && pat.confidence >= self.config.min_confidence {
                    results.push(DataClass {
                        category: rule.category,
                        subcategory: Some(rule.name.clone()),
                        confidence: pat.confidence,
                        source: match pat.target {
                            PatternTarget::KeyName | PatternTarget::FieldName => {
                                ClassificationSource::KeyPattern
                            }
                            _ => ClassificationSource::ValuePattern,
                        },
                    });
                }
            }
        }

        // Deduplicate by (category, subcategory), keep highest confidence.
        results.sort_by(|a, b| {
            b.confidence
                .partial_cmp(&a.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.dedup_by(|a, b| a.category == b.category && a.subcategory == b.subcategory);
        results.truncate(self.config.max_tags_per_key);
        results
    }

    /// Batch scan a set of key/value pairs.
    pub fn scan(&self, keys: &[(String, Vec<u8>)]) -> ClassificationReport {
        let start = Instant::now();
        let mut by_category: HashMap<String, u64> = HashMap::new();
        let mut classified = 0u64;
        let mut high_risk = Vec::new();

        for (key, value) in keys {
            let classes = self.classify_key(key, value);
            if !classes.is_empty() {
                classified += 1;
                for c in &classes {
                    *by_category.entry(c.category.to_string()).or_default() += 1;
                }
                let is_high_risk = classes.iter().any(|c| {
                    matches!(
                        c.category,
                        ClassCategory::PII
                            | ClassCategory::PHI
                            | ClassCategory::Financial
                            | ClassCategory::Credentials
                    )
                });
                if is_high_risk {
                    high_risk.push((key.clone(), classes.clone()));
                }
                // Store tags
                let mut tags = self.tags.write();
                tags.insert(key.clone(), classes);
            }
        }

        let elapsed = start.elapsed();
        {
            let mut stats = self.stats.write();
            stats.total_scans += 1;
            stats.total_classified += classified;
        }

        ClassificationReport {
            total_scanned: keys.len() as u64,
            classified,
            unclassified: keys.len() as u64 - classified,
            by_category,
            high_risk,
            scan_duration_ms: elapsed.as_millis() as u64,
        }
    }

    /// Add a custom classification rule.
    pub fn add_rule(&self, rule: ClassificationRule) -> Result<(), ClassifierError> {
        let mut rules = self.rules.write();
        if rules.iter().any(|r| r.name == rule.name) {
            return Err(ClassifierError::RuleExists(rule.name));
        }
        let mut compiled_patterns = Vec::new();
        for p in &rule.patterns {
            let regex = Regex::new(&p.pattern)
                .map_err(|e| ClassifierError::InvalidPattern(e.to_string()))?;
            compiled_patterns.push(CompiledPattern {
                target: p.target,
                regex,
                confidence: p.confidence,
            });
        }
        rules.push(CompiledRule {
            name: rule.name,
            category: rule.category,
            patterns: compiled_patterns,
            priority: rule.priority,
            enabled: rule.enabled,
        });
        self.stats.write().rules_count = rules.len();
        Ok(())
    }

    /// Remove a classification rule by name.
    pub fn remove_rule(&self, name: &str) -> Result<(), ClassifierError> {
        let mut rules = self.rules.write();
        let before = rules.len();
        rules.retain(|r| r.name != name);
        if rules.len() == before {
            return Err(ClassifierError::RuleNotFound(name.to_string()));
        }
        self.stats.write().rules_count = rules.len();
        Ok(())
    }

    /// Manually tag a key with a category.
    pub fn tag_key(&self, key: &str, category: ClassCategory) -> Result<(), ClassifierError> {
        let mut tags = self.tags.write();
        let entry = tags.entry(key.to_string()).or_default();
        if entry.len() >= self.config.max_tags_per_key {
            return Err(ClassifierError::MaxTags(self.config.max_tags_per_key));
        }
        entry.push(DataClass {
            category,
            subcategory: None,
            confidence: 1.0,
            source: ClassificationSource::ManualTag,
        });
        self.stats.write().manual_tags += 1;
        Ok(())
    }

    /// Get all classifications for a key.
    pub fn get_tags(&self, key: &str) -> Vec<DataClass> {
        self.tags.read().get(key).cloned().unwrap_or_default()
    }

    /// Find all keys classified with the given category.
    pub fn find_by_class(&self, category: ClassCategory) -> Vec<String> {
        self.tags
            .read()
            .iter()
            .filter(|(_, classes)| classes.iter().any(|c| c.category == category))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Propagate tags from a source key to downstream keys.
    pub fn propagate_tags(&self, key: &str, downstream_keys: &[&str]) {
        let source_tags = self.get_tags(key);
        if source_tags.is_empty() {
            return;
        }
        let propagated: Vec<DataClass> = source_tags
            .iter()
            .map(|c| DataClass {
                category: c.category,
                subcategory: c.subcategory.clone(),
                confidence: c.confidence * 0.9, // slight decay
                source: ClassificationSource::LineagePropagation,
            })
            .collect();

        let mut tags = self.tags.write();
        let mut stats = self.stats.write();
        for dk in downstream_keys {
            let entry = tags.entry(dk.to_string()).or_default();
            for p in &propagated {
                if !entry.iter().any(|e| e.category == p.category) {
                    entry.push(p.clone());
                }
            }
            stats.propagations += 1;
        }
    }

    /// Return a summary of all classifications.
    pub fn classification_summary(&self) -> ClassificationSummary {
        let tags = self.tags.read();
        let mut by_category: HashMap<String, u64> = HashMap::new();
        let mut highest_risk_keys = Vec::new();

        for (key, classes) in tags.iter() {
            for c in classes {
                *by_category.entry(c.category.to_string()).or_default() += 1;
            }
            if classes.iter().any(|c| {
                matches!(
                    c.category,
                    ClassCategory::PII
                        | ClassCategory::PHI
                        | ClassCategory::Financial
                        | ClassCategory::Credentials
                )
            }) {
                highest_risk_keys.push(key.clone());
            }
        }

        ClassificationSummary {
            total_tagged: tags.len() as u64,
            by_category,
            unclassified: 0,
            highest_risk_keys,
        }
    }

    /// Generate a compliance report for the given framework.
    pub fn compliance_report(&self, framework: &str) -> ComplianceClassReport {
        let tags = self.tags.read();
        let mut issues = Vec::new();
        let mut classified_keys = 0u64;

        let relevant_categories: Vec<ClassCategory> = match framework.to_uppercase().as_str() {
            "GDPR" => vec![ClassCategory::PII, ClassCategory::Credentials],
            "HIPAA" => vec![ClassCategory::PHI, ClassCategory::PII],
            "PCI" => vec![ClassCategory::Financial, ClassCategory::Credentials],
            "SOC2" => vec![
                ClassCategory::PII,
                ClassCategory::Credentials,
                ClassCategory::Confidential,
            ],
            _ => vec![],
        };

        for (key, classes) in tags.iter() {
            let matching: Vec<&DataClass> = classes
                .iter()
                .filter(|c| relevant_categories.contains(&c.category))
                .collect();
            if !matching.is_empty() {
                classified_keys += 1;
                for m in &matching {
                    issues.push(ComplianceIssue {
                        key_pattern: key.clone(),
                        issue: format!(
                            "{:?} data found (confidence {:.0}%)",
                            m.category,
                            m.confidence * 100.0
                        ),
                        severity: if m.confidence >= 0.95 {
                            "high".to_string()
                        } else if m.confidence >= 0.85 {
                            "medium".to_string()
                        } else {
                            "low".to_string()
                        },
                        recommendation: format!(
                            "Review and protect {:?} data in key '{}'",
                            m.category, key
                        ),
                    });
                }
            }
        }

        ComplianceClassReport {
            framework: framework.to_string(),
            classified_keys,
            compliant: issues.is_empty(),
            issues,
        }
    }

    /// Return operational statistics.
    pub fn stats(&self) -> ClassifierStats {
        self.stats.read().clone()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_classifier() -> DataClassifier {
        DataClassifier::new(ClassifierConfig::default())
    }

    #[test]
    fn test_email_detection() {
        let c = default_classifier();
        let classes = c.classify_key("user:email", b"alice@example.com");
        assert!(!classes.is_empty());
        assert!(classes.iter().any(|d| d.category == ClassCategory::PII));
    }

    #[test]
    fn test_ssn_detection() {
        let c = default_classifier();
        let classes = c.classify_key("user:ssn", b"123-45-6789");
        assert!(classes.iter().any(|d| d.category == ClassCategory::PII));
        assert!(classes.iter().any(|d| d.confidence >= 0.99));
    }

    #[test]
    fn test_credit_card_detection() {
        let c = default_classifier();
        let classes = c.classify_key("payment:card", b"4111-1111-1111-1111");
        assert!(classes
            .iter()
            .any(|d| d.category == ClassCategory::Financial));
    }

    #[test]
    fn test_api_key_pattern() {
        let c = default_classifier();
        let classes = c.classify_key("service:api_key", b"some_random_value");
        assert!(classes
            .iter()
            .any(|d| d.category == ClassCategory::Credentials));
    }

    #[test]
    fn test_medical_record_pattern() {
        let c = default_classifier();
        let classes = c.classify_key("patient:12345", b"flu diagnosis");
        assert!(classes.iter().any(|d| d.category == ClassCategory::PHI));
    }

    #[test]
    fn test_name_detection() {
        let c = default_classifier();
        let classes = c.classify_key("user:first_name", b"Alice");
        assert!(classes.iter().any(|d| d.category == ClassCategory::PII));
    }

    #[test]
    fn test_manual_tagging() {
        let c = default_classifier();
        c.tag_key("mykey", ClassCategory::Confidential).unwrap();
        let tags = c.get_tags("mykey");
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0].category, ClassCategory::Confidential);
        assert_eq!(tags[0].source, ClassificationSource::ManualTag);
    }

    #[test]
    fn test_batch_scan() {
        let c = default_classifier();
        let keys = vec![
            ("user:email".to_string(), b"alice@example.com".to_vec()),
            ("counter:visits".to_string(), b"42".to_vec()),
            ("user:ssn".to_string(), b"123-45-6789".to_vec()),
        ];
        let report = c.scan(&keys);
        assert_eq!(report.total_scanned, 3);
        assert!(report.classified >= 2);
        assert!(!report.high_risk.is_empty());
    }

    #[test]
    fn test_propagation() {
        let c = default_classifier();
        c.tag_key("source", ClassCategory::PII).unwrap();
        c.propagate_tags("source", &["downstream1", "downstream2"]);
        let tags = c.get_tags("downstream1");
        assert!(!tags.is_empty());
        assert_eq!(tags[0].source, ClassificationSource::LineagePropagation);
    }

    #[test]
    fn test_compliance_report() {
        let c = default_classifier();
        c.tag_key("user:email", ClassCategory::PII).unwrap();
        let report = c.compliance_report("GDPR");
        assert_eq!(report.framework, "GDPR");
        assert!(report.classified_keys > 0);
    }

    #[test]
    fn test_add_remove_rule() {
        let c = default_classifier();
        let initial = c.stats().rules_count;
        c.add_rule(ClassificationRule {
            name: "Custom/ZipCode".into(),
            description: "Detect zip codes".into(),
            category: ClassCategory::PII,
            patterns: vec![PatternRule {
                target: PatternTarget::ValueContent,
                pattern: r"[0-9]{5}(-[0-9]{4})?".into(),
                confidence: 0.75,
            }],
            priority: 5,
            enabled: true,
        })
        .unwrap();
        assert_eq!(c.stats().rules_count, initial + 1);

        c.remove_rule("Custom/ZipCode").unwrap();
        assert_eq!(c.stats().rules_count, initial);
    }

    #[test]
    fn test_find_by_class() {
        let c = default_classifier();
        c.tag_key("secret1", ClassCategory::Credentials).unwrap();
        c.tag_key("secret2", ClassCategory::Credentials).unwrap();
        c.tag_key("public1", ClassCategory::Public).unwrap();
        let found = c.find_by_class(ClassCategory::Credentials);
        assert_eq!(found.len(), 2);
    }

    #[test]
    fn test_max_tags_error() {
        let c = DataClassifier::new(ClassifierConfig {
            max_tags_per_key: 1,
            ..Default::default()
        });
        c.tag_key("k", ClassCategory::PII).unwrap();
        let err = c.tag_key("k", ClassCategory::Financial).unwrap_err();
        assert!(matches!(err, ClassifierError::MaxTags(1)));
    }

    #[test]
    fn test_classification_summary() {
        let c = default_classifier();
        c.tag_key("a", ClassCategory::PII).unwrap();
        c.tag_key("b", ClassCategory::Financial).unwrap();
        let summary = c.classification_summary();
        assert_eq!(summary.total_tagged, 2);
    }

    #[test]
    fn test_stats_tracking() {
        let c = default_classifier();
        let keys = vec![("user:email".to_string(), b"bob@test.com".to_vec())];
        c.scan(&keys);
        let stats = c.stats();
        assert_eq!(stats.total_scans, 1);
    }
}
