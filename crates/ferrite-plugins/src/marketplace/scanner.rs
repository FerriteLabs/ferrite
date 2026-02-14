//! Security scanner for plugin validation before installation.

use serde::{Deserialize, Serialize};

/// Security scan result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanResult {
    pub plugin_name: String,
    pub version: String,
    pub passed: bool,
    pub findings: Vec<SecurityFinding>,
    pub risk_score: f64,
}

/// A single security finding.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityFinding {
    pub severity: FindingSeverity,
    pub category: FindingCategory,
    pub description: String,
    pub recommendation: String,
}

/// Severity of a security finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum FindingSeverity {
    Info,
    Low,
    Medium,
    High,
    Critical,
}

/// Category of security finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FindingCategory {
    /// Plugin requests excessive permissions.
    ExcessivePermissions,
    /// Plugin accesses filesystem outside sandbox.
    FilesystemAccess,
    /// Plugin makes network connections.
    NetworkAccess,
    /// Plugin uses unsafe code.
    UnsafeCode,
    /// Known vulnerability in a dependency.
    KnownVulnerability,
    /// Plugin binary is not signed.
    UnsignedBinary,
}

/// Performs a basic security scan on plugin metadata.
pub fn scan_metadata(
    name: &str,
    version: &str,
    permissions: &[String],
    has_network: bool,
    has_filesystem: bool,
) -> ScanResult {
    let mut findings = Vec::new();
    let mut risk_score: f64 = 0.0;

    if has_network {
        findings.push(SecurityFinding {
            severity: FindingSeverity::Medium,
            category: FindingCategory::NetworkAccess,
            description: "Plugin requests network access".to_string(),
            recommendation: "Verify the plugin needs network access for its functionality"
                .to_string(),
        });
        risk_score += 0.3;
    }

    if has_filesystem {
        findings.push(SecurityFinding {
            severity: FindingSeverity::Medium,
            category: FindingCategory::FilesystemAccess,
            description: "Plugin requests filesystem access".to_string(),
            recommendation: "Ensure filesystem access is sandboxed to plugin directory".to_string(),
        });
        risk_score += 0.2;
    }

    if permissions.len() > 10 {
        findings.push(SecurityFinding {
            severity: FindingSeverity::High,
            category: FindingCategory::ExcessivePermissions,
            description: format!("Plugin requests {} permissions", permissions.len()),
            recommendation: "Review if all permissions are necessary".to_string(),
        });
        risk_score += 0.4;
    }

    let passed = !findings.iter().any(|f| f.severity >= FindingSeverity::High) || risk_score < 0.7;

    ScanResult {
        plugin_name: name.to_string(),
        version: version.to_string(),
        passed,
        findings,
        risk_score: risk_score.clamp(0.0, 1.0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clean_scan() {
        let result = scan_metadata("test", "1.0.0", &[], false, false);
        assert!(result.passed);
        assert!(result.findings.is_empty());
        assert_eq!(result.risk_score, 0.0);
    }

    #[test]
    fn test_network_finding() {
        let result = scan_metadata("test", "1.0.0", &[], true, false);
        assert!(result.passed); // Medium findings don't fail
        assert_eq!(result.findings.len(), 1);
        assert_eq!(result.findings[0].category, FindingCategory::NetworkAccess);
    }

    #[test]
    fn test_excessive_permissions() {
        let perms: Vec<String> = (0..15).map(|i| format!("perm-{}", i)).collect();
        let result = scan_metadata("test", "1.0.0", &perms, false, false);
        assert!(result
            .findings
            .iter()
            .any(|f| f.category == FindingCategory::ExcessivePermissions));
    }

    #[test]
    fn test_severity_ordering() {
        assert!(FindingSeverity::Critical > FindingSeverity::High);
        assert!(FindingSeverity::High > FindingSeverity::Medium);
        assert!(FindingSeverity::Medium > FindingSeverity::Low);
    }
}
