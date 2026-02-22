//! Data Classification and Labeling

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::GovernanceError;

/// Data classification levels
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DataClassification {
    /// Publicly accessible data
    Public,
    /// Internal use only
    #[default]
    Internal,
    /// Confidential data
    Confidential,
    /// Highly restricted data (PII, PHI, etc.)
    Restricted,
    /// Custom classification
    Custom(String),
}

/// Data sensitivity levels
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Sensitivity {
    /// Low sensitivity
    Low,
    /// Medium sensitivity
    Medium,
    /// High sensitivity
    High,
    /// Critical sensitivity
    Critical,
}

/// Data label for additional categorization
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataLabel {
    /// Label name
    pub name: String,
    /// Label value
    pub value: String,
    /// Label category
    pub category: LabelCategory,
}

/// Label categories
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LabelCategory {
    /// Regulatory (GDPR, HIPAA, etc.)
    Regulatory,
    /// Data type (PII, PHI, PCI, etc.)
    DataType,
    /// Business unit
    BusinessUnit,
    /// Geographic region
    Region,
    /// Custom category
    Custom(String),
}

/// Key classification entry
struct ClassificationEntry {
    classification: DataClassification,
    labels: Vec<DataLabel>,
    sensitivity: Option<Sensitivity>,
    #[allow(dead_code)] // Planned for v0.2 — stored for classification audit trail
    updated_at: u64,
}

/// Label manager for data classification
pub struct LabelManager {
    /// Classifications by key pattern
    classifications: RwLock<HashMap<String, ClassificationEntry>>,
    /// Pattern-based rules
    pattern_rules: RwLock<Vec<PatternRule>>,
}

/// Pattern-based classification rule
struct PatternRule {
    pattern: String,
    classification: DataClassification,
    labels: Vec<DataLabel>,
    priority: u32,
}

impl LabelManager {
    /// Create a new label manager
    pub fn new() -> Self {
        Self {
            classifications: RwLock::new(HashMap::new()),
            pattern_rules: RwLock::new(Vec::new()),
        }
    }

    /// Set classification for a key
    pub fn set_classification(
        &self,
        key: &str,
        classification: DataClassification,
    ) -> Result<(), GovernanceError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.classifications.write().insert(
            key.to_string(),
            ClassificationEntry {
                classification,
                labels: vec![],
                sensitivity: None,
                updated_at: now,
            },
        );

        Ok(())
    }

    /// Get classification for a key
    pub fn get_classification(&self, key: &str) -> Option<DataClassification> {
        // Check direct classification
        if let Some(entry) = self.classifications.read().get(key) {
            return Some(entry.classification.clone());
        }

        // Check pattern rules
        let rules = self.pattern_rules.read();
        for rule in rules.iter() {
            if self.matches_pattern(&rule.pattern, key) {
                return Some(rule.classification.clone());
            }
        }

        None
    }

    /// Add a label to a key
    pub fn add_label(&self, key: &str, label: DataLabel) -> Result<(), GovernanceError> {
        let mut classifications = self.classifications.write();

        let entry = classifications
            .entry(key.to_string())
            .or_insert_with(|| ClassificationEntry {
                classification: DataClassification::default(),
                labels: vec![],
                sensitivity: None,
                updated_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            });

        entry.labels.push(label);
        Ok(())
    }

    /// Get labels for a key
    pub fn get_labels(&self, key: &str) -> Vec<DataLabel> {
        // Direct labels
        let mut labels = self
            .classifications
            .read()
            .get(key)
            .map(|e| e.labels.clone())
            .unwrap_or_default();

        // Pattern-based labels
        let rules = self.pattern_rules.read();
        for rule in rules.iter() {
            if self.matches_pattern(&rule.pattern, key) {
                labels.extend(rule.labels.clone());
            }
        }

        labels
    }

    /// Add a pattern-based rule
    pub fn add_pattern_rule(
        &self,
        pattern: &str,
        classification: DataClassification,
        labels: Vec<DataLabel>,
        priority: u32,
    ) {
        let mut rules = self.pattern_rules.write();
        rules.push(PatternRule {
            pattern: pattern.to_string(),
            classification,
            labels,
            priority,
        });
        rules.sort_by(|a, b| b.priority.cmp(&a.priority));
    }

    fn matches_pattern(&self, pattern: &str, key: &str) -> bool {
        // Simple glob matching
        if pattern == "*" {
            return true;
        }
        if pattern.contains('*') {
            let parts: Vec<&str> = pattern.split('*').collect();
            if parts.iter().all(|p| p.is_empty()) {
                return true;
            }
            let mut remainder = key;
            for (idx, part) in parts.iter().enumerate() {
                if part.is_empty() {
                    continue;
                }
                if let Some(pos) = remainder.find(part) {
                    remainder = &remainder[pos + part.len()..];
                } else {
                    return false;
                }
                if idx == 0 && !pattern.starts_with('*') && !key.starts_with(part) {
                    return false;
                }
            }
            if !pattern.ends_with('*') && !parts.last().unwrap_or(&"").is_empty() {
                return key.ends_with(parts.last().unwrap_or(&""));
            }
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

    /// Set sensitivity for a key
    pub fn set_sensitivity(
        &self,
        key: &str,
        sensitivity: Sensitivity,
    ) -> Result<(), GovernanceError> {
        let mut classifications = self.classifications.write();

        let entry = classifications
            .entry(key.to_string())
            .or_insert_with(|| ClassificationEntry {
                classification: DataClassification::default(),
                labels: vec![],
                sensitivity: None,
                updated_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            });

        entry.sensitivity = Some(sensitivity);
        Ok(())
    }

    /// Get sensitivity for a key
    pub fn get_sensitivity(&self, key: &str) -> Option<Sensitivity> {
        self.classifications
            .read()
            .get(key)
            .and_then(|e| e.sensitivity.clone())
    }
}

impl Default for LabelManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_get_classification() {
        let manager = LabelManager::new();

        manager
            .set_classification("user:123:email", DataClassification::Restricted)
            .unwrap();

        let classification = manager.get_classification("user:123:email");
        assert_eq!(classification, Some(DataClassification::Restricted));
    }

    #[test]
    fn test_pattern_rule() {
        let manager = LabelManager::new();

        manager.add_pattern_rule(
            "user:*:pii",
            DataClassification::Restricted,
            vec![DataLabel {
                name: "data_type".to_string(),
                value: "PII".to_string(),
                category: LabelCategory::DataType,
            }],
            100,
        );

        let classification = manager.get_classification("user:456:pii");
        assert_eq!(classification, Some(DataClassification::Restricted));

        let labels = manager.get_labels("user:456:pii");
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].value, "PII");
    }

    #[test]
    fn test_add_labels() {
        let manager = LabelManager::new();

        manager
            .add_label(
                "data:key",
                DataLabel {
                    name: "compliance".to_string(),
                    value: "GDPR".to_string(),
                    category: LabelCategory::Regulatory,
                },
            )
            .unwrap();

        let labels = manager.get_labels("data:key");
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].name, "compliance");
    }
}
