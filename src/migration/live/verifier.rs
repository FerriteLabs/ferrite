//! Migration verifier — compares keys between source and target to
//! ensure consistency after a sync.

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};

/// Report produced by the verification step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationReport {
    /// Total number of keys checked.
    pub total_checked: u64,
    /// Keys that matched between source and target.
    pub matching: u64,
    /// Keys present in both but with differing values.
    pub mismatched: u64,
    /// Keys present in source but missing in target.
    pub missing_in_target: u64,
    /// Keys present in target but not in source.
    pub extra_in_target: u64,
    /// Percentage of total keys that were sampled.
    pub sample_percentage: f64,
}

impl VerificationReport {
    /// Returns `true` when every sampled key matched.
    pub fn is_consistent(&self) -> bool {
        self.mismatched == 0 && self.missing_in_target == 0
    }
}

/// Verifier that samples keys and compares values.
pub struct MigrationVerifier;

impl MigrationVerifier {
    /// Verify a snapshot of keys between source and target.
    ///
    /// In a full implementation this would accept a `RedisConnector` and
    /// a reference to the local `Store`, sample `sample_size` random keys,
    /// and compare their values. The current stub returns a clean report
    /// so the rest of the pipeline can be tested end-to-end.
    pub fn verify_snapshot(sample_size: usize) -> VerificationReport {
        // Placeholder — no live connection yet.
        VerificationReport {
            total_checked: sample_size as u64,
            matching: sample_size as u64,
            mismatched: 0,
            missing_in_target: 0,
            extra_in_target: 0,
            sample_percentage: 100.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_verification_report_consistent() {
        let report = MigrationVerifier::verify_snapshot(100);
        assert!(report.is_consistent());
        assert_eq!(report.total_checked, 100);
        assert_eq!(report.matching, 100);
    }

    #[test]
    fn test_verification_report_inconsistent() {
        let report = VerificationReport {
            total_checked: 100,
            matching: 95,
            mismatched: 3,
            missing_in_target: 2,
            extra_in_target: 0,
            sample_percentage: 10.0,
        };
        assert!(!report.is_consistent());
    }
}
