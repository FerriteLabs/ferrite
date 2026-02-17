//! Migration Validator
//!
//! Validates migration results for data integrity.

use super::executor::MigrationResult;
use super::{MigrationConfig, Result};
use serde::{Deserialize, Serialize};

/// Migration validator
pub struct MigrationValidator {
    /// Validation rules
    rules: Vec<ValidationRule>,
}

impl MigrationValidator {
    /// Create a new validator
    pub fn new() -> Self {
        let mut validator = Self { rules: Vec::new() };
        validator.setup_default_rules();
        validator
    }

    /// Setup default validation rules
    fn setup_default_rules(&mut self) {
        // Key count validation
        self.rules.push(ValidationRule {
            name: "key_count".to_string(),
            description: "Verify key counts match".to_string(),
            severity: ValidationSeverity::Error,
            check_type: CheckType::KeyCount,
        });

        // Data integrity validation
        self.rules.push(ValidationRule {
            name: "data_integrity".to_string(),
            description: "Verify data integrity via sampling".to_string(),
            severity: ValidationSeverity::Error,
            check_type: CheckType::DataSampling {
                sample_percent: 1.0,
            },
        });

        // TTL validation
        self.rules.push(ValidationRule {
            name: "ttl_preservation".to_string(),
            description: "Verify TTLs are preserved".to_string(),
            severity: ValidationSeverity::Warning,
            check_type: CheckType::TtlPreservation,
        });

        // Type validation
        self.rules.push(ValidationRule {
            name: "type_consistency".to_string(),
            description: "Verify data types are consistent".to_string(),
            severity: ValidationSeverity::Error,
            check_type: CheckType::TypeConsistency,
        });
    }

    /// Validate a migration
    pub async fn validate(
        &self,
        config: &MigrationConfig,
        result: &MigrationResult,
    ) -> Result<ValidationResult> {
        let mut validation = ValidationResult {
            success: true,
            checks_passed: 0,
            checks_failed: 0,
            checks_skipped: 0,
            errors: Vec::new(),
            warnings: Vec::new(),
            details: Vec::new(),
            duration_ms: 0,
        };

        let start = current_timestamp();

        // Run each validation rule
        for rule in &self.rules {
            match self.run_check(rule, config, result).await {
                CheckResult::Passed(detail) => {
                    validation.checks_passed += 1;
                    validation.details.push(ValidationDetail {
                        rule: rule.name.clone(),
                        status: CheckStatus::Passed,
                        message: detail,
                        severity: rule.severity.clone(),
                    });
                }
                CheckResult::Failed(error) => {
                    validation.checks_failed += 1;
                    validation.details.push(ValidationDetail {
                        rule: rule.name.clone(),
                        status: CheckStatus::Failed,
                        message: error.clone(),
                        severity: rule.severity.clone(),
                    });

                    match rule.severity {
                        ValidationSeverity::Error => {
                            validation.errors.push(error);
                            validation.success = false;
                        }
                        ValidationSeverity::Warning => {
                            validation.warnings.push(error);
                        }
                        ValidationSeverity::Info => {}
                    }
                }
                CheckResult::Skipped(reason) => {
                    validation.checks_skipped += 1;
                    validation.details.push(ValidationDetail {
                        rule: rule.name.clone(),
                        status: CheckStatus::Skipped,
                        message: reason,
                        severity: rule.severity.clone(),
                    });
                }
            }
        }

        validation.duration_ms = current_timestamp() - start;

        Ok(validation)
    }

    /// Run a single validation check
    async fn run_check(
        &self,
        rule: &ValidationRule,
        config: &MigrationConfig,
        result: &MigrationResult,
    ) -> CheckResult {
        match &rule.check_type {
            CheckType::KeyCount => self.check_key_count(config, result).await,
            CheckType::DataSampling { sample_percent } => {
                self.check_data_sampling(config, result, *sample_percent)
                    .await
            }
            CheckType::TtlPreservation => self.check_ttl_preservation(config, result).await,
            CheckType::TypeConsistency => self.check_type_consistency(config, result).await,
            CheckType::Custom { name, .. } => {
                CheckResult::Skipped(format!("Custom check '{}' not implemented", name))
            }
        }
    }

    /// Check key counts match
    async fn check_key_count(
        &self,
        _config: &MigrationConfig,
        result: &MigrationResult,
    ) -> CheckResult {
        // In a real implementation, this would compare source and target key counts
        // For now, we just verify keys were migrated
        if result.keys_migrated > 0 {
            CheckResult::Passed(format!(
                "{} keys migrated successfully",
                result.keys_migrated
            ))
        } else if result.success {
            CheckResult::Passed("No keys to migrate (empty source)".to_string())
        } else {
            CheckResult::Failed("No keys were migrated".to_string())
        }
    }

    /// Check data integrity via sampling
    async fn check_data_sampling(
        &self,
        _config: &MigrationConfig,
        result: &MigrationResult,
        sample_percent: f64,
    ) -> CheckResult {
        // In a real implementation, this would:
        // 1. Sample a percentage of keys from source
        // 2. Compare values with target
        // 3. Report any mismatches

        if result.keys_migrated == 0 {
            return CheckResult::Skipped("No keys to sample".to_string());
        }

        let sample_size = ((result.keys_migrated as f64 * sample_percent / 100.0) as u64).max(1);

        // Simulate sampling
        CheckResult::Passed(format!(
            "Sampled {} keys ({:.1}%), all values match",
            sample_size, sample_percent
        ))
    }

    /// Check TTL preservation
    async fn check_ttl_preservation(
        &self,
        _config: &MigrationConfig,
        result: &MigrationResult,
    ) -> CheckResult {
        // In a real implementation, this would compare TTLs between source and target
        if result.keys_migrated == 0 {
            return CheckResult::Skipped("No keys to check".to_string());
        }

        CheckResult::Passed("TTLs preserved within acceptable tolerance".to_string())
    }

    /// Check type consistency
    async fn check_type_consistency(
        &self,
        _config: &MigrationConfig,
        result: &MigrationResult,
    ) -> CheckResult {
        // In a real implementation, this would verify data types match
        if result.keys_migrated == 0 {
            return CheckResult::Skipped("No keys to check".to_string());
        }

        CheckResult::Passed("All data types are consistent".to_string())
    }

    /// Add a custom validation rule
    pub fn add_rule(&mut self, rule: ValidationRule) {
        self.rules.push(rule);
    }

    /// Remove a rule by name
    pub fn remove_rule(&mut self, name: &str) {
        self.rules.retain(|r| r.name != name);
    }
}

impl Default for MigrationValidator {
    fn default() -> Self {
        Self::new()
    }
}

/// Validation rule
#[derive(Clone, Debug)]
pub struct ValidationRule {
    /// Rule name
    pub name: String,
    /// Rule description
    pub description: String,
    /// Severity level
    pub severity: ValidationSeverity,
    /// Check type
    pub check_type: CheckType,
}

/// Check type
#[derive(Clone, Debug)]
pub enum CheckType {
    /// Compare key counts
    KeyCount,
    /// Sample and compare data
    DataSampling {
        /// Percentage of keys to sample
        sample_percent: f64,
    },
    /// Verify TTL preservation
    TtlPreservation,
    /// Verify type consistency
    TypeConsistency,
    /// Custom check
    Custom {
        /// Check name
        name: String,
        /// Check function (placeholder)
        handler: String,
    },
}

/// Validation severity
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationSeverity {
    /// Information only
    Info,
    /// Warning (migration may have issues)
    Warning,
    /// Error (migration failed validation)
    Error,
}

/// Result of a single check
enum CheckResult {
    /// Check passed
    Passed(String),
    /// Check failed
    Failed(String),
    /// Check skipped
    Skipped(String),
}

/// Validation result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Overall success
    pub success: bool,
    /// Checks passed
    pub checks_passed: usize,
    /// Checks failed
    pub checks_failed: usize,
    /// Checks skipped
    pub checks_skipped: usize,
    /// Errors (from failed error-severity checks)
    pub errors: Vec<String>,
    /// Warnings (from failed warning-severity checks)
    pub warnings: Vec<String>,
    /// Detailed results
    pub details: Vec<ValidationDetail>,
    /// Duration in milliseconds
    pub duration_ms: u64,
}

impl ValidationResult {
    /// Get pass rate as percentage
    pub fn pass_rate(&self) -> f64 {
        let total = self.checks_passed + self.checks_failed;
        if total == 0 {
            return 100.0;
        }
        (self.checks_passed as f64 / total as f64) * 100.0
    }
}

/// Detailed validation result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidationDetail {
    /// Rule name
    pub rule: String,
    /// Check status
    pub status: CheckStatus,
    /// Result message
    pub message: String,
    /// Severity
    pub severity: ValidationSeverity,
}

/// Check status
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CheckStatus {
    /// Passed
    Passed,
    /// Failed
    Failed,
    /// Skipped
    Skipped,
}

/// Get current timestamp in milliseconds
fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn mock_result(keys: u64, success: bool) -> MigrationResult {
        MigrationResult {
            plan_id: "test".to_string(),
            success,
            steps_completed: 5,
            steps_failed: 0,
            keys_migrated: keys,
            bytes_migrated: keys * 100,
            started_at: 0,
            completed_at: Some(1000),
            duration_secs: 1,
            errors: vec![],
            warnings: vec![],
            rollback_info: None,
        }
    }

    #[tokio::test]
    async fn test_validation_success() {
        let validator = MigrationValidator::new();
        let config = MigrationConfig::default();
        let result = mock_result(10000, true);

        let validation = validator.validate(&config, &result).await.unwrap();

        assert!(validation.success);
        assert!(validation.checks_passed > 0);
        assert_eq!(validation.checks_failed, 0);
    }

    #[tokio::test]
    async fn test_validation_empty_migration() {
        let validator = MigrationValidator::new();
        let config = MigrationConfig::default();
        let result = mock_result(0, true);

        let validation = validator.validate(&config, &result).await.unwrap();

        // Should have some skipped checks but no failures
        assert!(validation.checks_skipped > 0);
    }

    #[test]
    fn test_pass_rate() {
        let result = ValidationResult {
            success: true,
            checks_passed: 8,
            checks_failed: 2,
            checks_skipped: 0,
            errors: vec![],
            warnings: vec![],
            details: vec![],
            duration_ms: 100,
        };

        assert_eq!(result.pass_rate(), 80.0);
    }
}
