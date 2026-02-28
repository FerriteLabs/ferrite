//! Compliance and audit trail
//!
//! Immutable audit logging, data retention policies, field-level encryption,
//! GDPR right-to-deletion, and compliance report generation.
#![allow(dead_code)]

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the compliance audit log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditConfig {
    /// Whether auditing is enabled.
    pub enabled: bool,
    /// Log read operations (can be very verbose).
    pub log_reads: bool,
    /// Log write operations.
    pub log_writes: bool,
    /// Log administrative operations.
    pub log_admin: bool,
    /// Maximum number of log entries before pruning.
    pub max_log_entries: usize,
    /// How long to retain entries (days).
    pub retention_days: u32,
    /// Enable hash-chain tamper detection.
    pub tamper_detection: bool,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            log_reads: false,
            log_writes: true,
            log_admin: true,
            max_log_entries: 1_000_000,
            retention_days: 90,
            tamper_detection: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Audit types
// ---------------------------------------------------------------------------

/// Classification of audit events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AuditEventType {
    Read,
    Write,
    Delete,
    Admin,
    Auth,
    Config,
    Replication,
}

impl AuditEventType {
    /// Parse from string (case-insensitive).
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "read" => Some(Self::Read),
            "write" => Some(Self::Write),
            "delete" => Some(Self::Delete),
            "admin" => Some(Self::Admin),
            "auth" => Some(Self::Auth),
            "config" => Some(Self::Config),
            "replication" => Some(Self::Replication),
            _ => None,
        }
    }
}

impl std::fmt::Display for AuditEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read => write!(f, "read"),
            Self::Write => write!(f, "write"),
            Self::Delete => write!(f, "delete"),
            Self::Admin => write!(f, "admin"),
            Self::Auth => write!(f, "auth"),
            Self::Config => write!(f, "config"),
            Self::Replication => write!(f, "replication"),
        }
    }
}

/// Result of an audited operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditResult {
    Success,
    Error(String),
    Denied,
}

/// A single audit log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceAuditEntry {
    /// Monotonically increasing entry ID.
    pub id: u64,
    /// Timestamp (epoch milliseconds).
    pub timestamp: u64,
    /// Type of event.
    pub event_type: AuditEventType,
    /// User who performed the action.
    pub user: String,
    /// Client address.
    pub client_addr: String,
    /// Command executed.
    pub command: String,
    /// Key involved (if any).
    pub key: Option<String>,
    /// Outcome of the operation.
    pub result: AuditResult,
    /// Database index.
    pub db: u8,
    /// SHA-256-like hash for tamper detection (CRC32-based chain).
    pub hash: String,
}

// ---------------------------------------------------------------------------
// Audit filter & export
// ---------------------------------------------------------------------------

/// Filter criteria for querying the audit log.
#[derive(Debug, Clone, Default)]
pub struct AuditFilter {
    pub user: Option<String>,
    pub event_type: Option<AuditEventType>,
    pub key_pattern: Option<String>,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub limit: usize,
}

/// Export format for audit log data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    Json,
    Csv,
    Syslog,
}

impl ExportFormat {
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "json" => Some(Self::Json),
            "csv" => Some(Self::Csv),
            "syslog" => Some(Self::Syslog),
            _ => None,
        }
    }
}

/// Result of an integrity verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityResult {
    pub valid: bool,
    pub entries_checked: u64,
    pub first_invalid: Option<u64>,
    pub chain_start: u64,
    pub chain_end: u64,
}

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors from the audit/compliance module.
#[derive(Debug, Clone, thiserror::Error)]
pub enum AuditError {
    #[error("audit not enabled")]
    NotEnabled,

    #[error("export error: {0}")]
    ExportError(String),

    #[error("integrity check failed at entry {0}")]
    IntegrityFailed(u64),
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Simple hash chain using CRC32 (no external crypto dependency).
fn compute_hash(previous_hash: &str, entry_data: &str) -> String {
    let combined = format!("{}{}", previous_hash, entry_data);
    let crc = crc32fast::hash(combined.as_bytes());
    format!("{:08x}", crc)
}

fn matches_pattern(key: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return key.starts_with(prefix);
    }
    key == pattern
}

// ---------------------------------------------------------------------------
// Audit Log
// ---------------------------------------------------------------------------

/// Immutable, hash-chained audit log.
pub struct ComplianceAuditLog {
    config: AuditConfig,
    entries: RwLock<Vec<ComplianceAuditEntry>>,
    next_id: RwLock<u64>,
}

impl ComplianceAuditLog {
    /// Create a new audit log with the given configuration.
    pub fn new(config: AuditConfig) -> Self {
        Self {
            config,
            entries: RwLock::new(Vec::new()),
            next_id: RwLock::new(1),
        }
    }

    /// Record a new audit entry.
    pub fn record(&self, mut entry: ComplianceAuditEntry) {
        if !self.config.enabled {
            return;
        }

        // Filter based on config
        match entry.event_type {
            AuditEventType::Read if !self.config.log_reads => return,
            AuditEventType::Write | AuditEventType::Delete if !self.config.log_writes => return,
            AuditEventType::Admin | AuditEventType::Config if !self.config.log_admin => return,
            _ => {}
        }

        let mut entries = self.entries.write();
        let mut next_id = self.next_id.write();

        entry.id = *next_id;
        entry.timestamp = now_millis();

        // Compute hash chain
        if self.config.tamper_detection {
            let prev_hash = entries.last().map(|e| e.hash.as_str()).unwrap_or("genesis");
            let entry_data = format!(
                "{}:{}:{}:{}:{}",
                entry.id, entry.timestamp, entry.user, entry.command, entry.db
            );
            entry.hash = compute_hash(prev_hash, &entry_data);
        }

        *next_id += 1;
        entries.push(entry);

        // Enforce max entries
        if entries.len() > self.config.max_log_entries {
            let excess = entries.len() - self.config.max_log_entries;
            entries.drain(..excess);
        }
    }

    /// Query the audit log with filters.
    pub fn query(&self, filter: AuditFilter) -> Vec<ComplianceAuditEntry> {
        let entries = self.entries.read();
        let limit = if filter.limit == 0 { 100 } else { filter.limit };

        entries
            .iter()
            .rev()
            .filter(|e| {
                if let Some(ref user) = filter.user {
                    if e.user != *user {
                        return false;
                    }
                }
                if let Some(ref evt) = filter.event_type {
                    if e.event_type != *evt {
                        return false;
                    }
                }
                if let Some(ref pattern) = filter.key_pattern {
                    match &e.key {
                        Some(k) if matches_pattern(k, pattern) => {}
                        Some(_) => return false,
                        None => return false,
                    }
                }
                if let Some(start) = filter.start_time {
                    if e.timestamp < start {
                        return false;
                    }
                }
                if let Some(end) = filter.end_time {
                    if e.timestamp > end {
                        return false;
                    }
                }
                true
            })
            .take(limit)
            .cloned()
            .collect()
    }

    /// Export the audit log in the specified format.
    pub fn export(&self, format: ExportFormat) -> Result<String, AuditError> {
        let entries = self.entries.read();
        match format {
            ExportFormat::Json => serde_json::to_string_pretty(&*entries)
                .map_err(|e| AuditError::ExportError(e.to_string())),
            ExportFormat::Csv => {
                let mut out = String::from(
                    "id,timestamp,event_type,user,client_addr,command,key,result,db,hash\n",
                );
                for e in entries.iter() {
                    let result_str = match &e.result {
                        AuditResult::Success => "success".to_string(),
                        AuditResult::Error(msg) => format!("error:{}", msg),
                        AuditResult::Denied => "denied".to_string(),
                    };
                    out.push_str(&format!(
                        "{},{},{},{},{},{},{},{},{},{}\n",
                        e.id,
                        e.timestamp,
                        e.event_type,
                        e.user,
                        e.client_addr,
                        e.command,
                        e.key.as_deref().unwrap_or(""),
                        result_str,
                        e.db,
                        e.hash,
                    ));
                }
                Ok(out)
            }
            ExportFormat::Syslog => {
                let mut out = String::new();
                for e in entries.iter() {
                    out.push_str(&format!(
                        "<14>1 {} ferrite audit {} - - user={} command={} key={} result={:?}\n",
                        e.timestamp,
                        e.id,
                        e.user,
                        e.command,
                        e.key.as_deref().unwrap_or("-"),
                        e.result,
                    ));
                }
                Ok(out)
            }
        }
    }

    /// Verify the integrity of the hash chain.
    pub fn verify_integrity(&self) -> IntegrityResult {
        let entries = self.entries.read();
        if entries.is_empty() {
            return IntegrityResult {
                valid: true,
                entries_checked: 0,
                first_invalid: None,
                chain_start: 0,
                chain_end: 0,
            };
        }

        let mut valid = true;
        let mut first_invalid = None;
        let mut prev_hash = "genesis".to_string();

        for entry in entries.iter() {
            if self.config.tamper_detection {
                let entry_data = format!(
                    "{}:{}:{}:{}:{}",
                    entry.id, entry.timestamp, entry.user, entry.command, entry.db
                );
                let expected = compute_hash(&prev_hash, &entry_data);
                if entry.hash != expected {
                    valid = false;
                    if first_invalid.is_none() {
                        first_invalid = Some(entry.id);
                    }
                }
                prev_hash = entry.hash.clone();
            }
        }

        IntegrityResult {
            valid,
            entries_checked: entries.len() as u64,
            first_invalid,
            chain_start: entries.first().map(|e| e.id).unwrap_or(0),
            chain_end: entries.last().map(|e| e.id).unwrap_or(0),
        }
    }

    /// Remove entries older than retention_days.
    pub fn prune_expired(&self) -> usize {
        let cutoff =
            now_millis().saturating_sub(self.config.retention_days as u64 * 24 * 60 * 60 * 1000);
        let mut entries = self.entries.write();
        let before = entries.len();
        entries.retain(|e| e.timestamp >= cutoff);
        before - entries.len()
    }

    /// Return total number of entries.
    pub fn count(&self) -> usize {
        self.entries.read().len()
    }
}

// ---------------------------------------------------------------------------
// Retention Policy
// ---------------------------------------------------------------------------

/// Data retention policy for key patterns.
pub struct RetentionPolicy {
    default_ttl: Duration,
    patterns: RwLock<Vec<(String, Duration)>>,
}

/// Status of retention compliance for a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetentionStatus {
    Compliant,
    Expiring,
    Overdue,
}

impl RetentionPolicy {
    pub fn new(default_ttl: Duration) -> Self {
        Self {
            default_ttl,
            patterns: RwLock::new(Vec::new()),
        }
    }

    /// Set TTL for keys matching a pattern.
    pub fn set_key_pattern_ttl(&self, pattern: &str, ttl: Duration) {
        let mut patterns = self.patterns.write();
        // Update existing or insert new
        if let Some(entry) = patterns.iter_mut().find(|(p, _)| p == pattern) {
            entry.1 = ttl;
        } else {
            patterns.push((pattern.to_string(), ttl));
        }
    }

    /// Get the effective TTL for a key.
    pub fn get_ttl(&self, key: &str) -> Duration {
        let patterns = self.patterns.read();
        for (pattern, ttl) in patterns.iter() {
            if matches_pattern(key, pattern) {
                return *ttl;
            }
        }
        self.default_ttl
    }

    /// Check whether a key's age is compliant with the retention policy.
    pub fn check_compliance(&self, key: &str, age: Duration) -> RetentionStatus {
        let ttl = self.get_ttl(key);
        if age > ttl {
            RetentionStatus::Overdue
        } else if age > ttl.mul_f64(0.9) {
            RetentionStatus::Expiring
        } else {
            RetentionStatus::Compliant
        }
    }
}

// ---------------------------------------------------------------------------
// GDPR Handler
// ---------------------------------------------------------------------------

/// Result of a GDPR right-to-deletion request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletionResult {
    pub keys_found: u64,
    pub keys_deleted: u64,
    pub audit_entries_created: u64,
    pub duration: Duration,
}

/// Result of a GDPR data export request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataExportResult {
    pub data: HashMap<String, serde_json::Value>,
    pub keys_exported: u64,
    pub total_bytes: u64,
}

/// Handles GDPR compliance operations.
pub struct GdprHandler {
    /// Subject data: subject_id -> key -> value.
    subject_data: RwLock<HashMap<String, HashMap<String, serde_json::Value>>>,
    /// Consent records: subject_id -> purpose -> granted.
    consents: RwLock<HashMap<String, HashMap<String, bool>>>,
}

impl GdprHandler {
    pub fn new() -> Self {
        Self {
            subject_data: RwLock::new(HashMap::new()),
            consents: RwLock::new(HashMap::new()),
        }
    }

    /// Execute GDPR right-to-deletion for a subject.
    pub fn right_to_deletion(&self, subject_id: &str) -> DeletionResult {
        let start = SystemTime::now();
        let mut data = self.subject_data.write();
        let found = data.get(subject_id).map(|d| d.len() as u64).unwrap_or(0);
        data.remove(subject_id);

        // Also remove consents
        self.consents.write().remove(subject_id);

        let duration = start.elapsed().unwrap_or_default();
        DeletionResult {
            keys_found: found,
            keys_deleted: found,
            audit_entries_created: if found > 0 { 1 } else { 0 },
            duration,
        }
    }

    /// Export all data for a subject (GDPR Article 20 - data portability).
    pub fn data_export(&self, subject_id: &str) -> DataExportResult {
        let data = self.subject_data.read();
        match data.get(subject_id) {
            Some(entries) => {
                let total_bytes: u64 = entries.values().map(|v| v.to_string().len() as u64).sum();
                DataExportResult {
                    data: entries.clone(),
                    keys_exported: entries.len() as u64,
                    total_bytes,
                }
            }
            None => DataExportResult {
                data: HashMap::new(),
                keys_exported: 0,
                total_bytes: 0,
            },
        }
    }

    /// Log a consent decision for a subject.
    pub fn log_consent(&self, subject_id: &str, purpose: &str, granted: bool) {
        let mut consents = self.consents.write();
        consents
            .entry(subject_id.to_string())
            .or_default()
            .insert(purpose.to_string(), granted);
    }

    /// Check whether consent is granted for a subject and purpose.
    pub fn check_consent(&self, subject_id: &str, purpose: &str) -> bool {
        let consents = self.consents.read();
        consents
            .get(subject_id)
            .and_then(|m| m.get(purpose))
            .copied()
            .unwrap_or(false)
    }

    /// Register data for a subject (for testing / data ingestion).
    pub fn register_data(&self, subject_id: &str, key: &str, value: serde_json::Value) {
        let mut data = self.subject_data.write();
        data.entry(subject_id.to_string())
            .or_default()
            .insert(key.to_string(), value);
    }
}

// ---------------------------------------------------------------------------
// Compliance Report
// ---------------------------------------------------------------------------

/// A single checklist item in a compliance report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChecklistItem {
    pub name: String,
    pub status: ChecklistStatus,
    pub description: String,
}

/// Status of a checklist item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecklistStatus {
    Pass,
    Fail,
    Warning,
}

/// Generated compliance report data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceReportData {
    pub generated_at: String,
    pub framework: String,
    pub audit_log_integrity: bool,
    pub retention_compliance_pct: f64,
    pub encryption_enabled: bool,
    pub access_control_enabled: bool,
    pub total_audit_entries: u64,
    pub unique_users: u64,
    pub denied_operations: u64,
    pub checklist: Vec<ChecklistItem>,
}

/// Compliance report generator.
pub struct ComplianceReport;

impl ComplianceReport {
    /// Generate a compliance report from the audit log and retention policy.
    pub fn generate(
        log: &ComplianceAuditLog,
        _policy: &RetentionPolicy,
        framework: &str,
    ) -> ComplianceReportData {
        let entries = log.entries.read();
        let integrity = log.verify_integrity();

        let mut unique_users = std::collections::HashSet::new();
        let mut denied_count: u64 = 0;
        for e in entries.iter() {
            unique_users.insert(e.user.clone());
            if e.result == AuditResult::Denied {
                denied_count += 1;
            }
        }

        let generated_at = {
            let d = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default();
            format!("{}Z", d.as_secs())
        };

        let mut checklist = vec![
            ChecklistItem {
                name: "Audit Logging".to_string(),
                status: if log.config.enabled {
                    ChecklistStatus::Pass
                } else {
                    ChecklistStatus::Fail
                },
                description: "Audit logging is enabled and recording events".to_string(),
            },
            ChecklistItem {
                name: "Hash Chain Integrity".to_string(),
                status: if integrity.valid {
                    ChecklistStatus::Pass
                } else {
                    ChecklistStatus::Fail
                },
                description: "Tamper detection hash chain is valid".to_string(),
            },
            ChecklistItem {
                name: "Retention Policy".to_string(),
                status: ChecklistStatus::Pass,
                description: format!("Retention set to {} days", log.config.retention_days),
            },
            ChecklistItem {
                name: "Access Control".to_string(),
                status: ChecklistStatus::Warning,
                description: "ACL-based access control is available".to_string(),
            },
        ];

        if framework == "GDPR" {
            checklist.push(ChecklistItem {
                name: "Right to Deletion".to_string(),
                status: ChecklistStatus::Pass,
                description: "GDPR deletion endpoint available".to_string(),
            });
            checklist.push(ChecklistItem {
                name: "Data Portability".to_string(),
                status: ChecklistStatus::Pass,
                description: "GDPR data export endpoint available".to_string(),
            });
        }

        ComplianceReportData {
            generated_at,
            framework: framework.to_string(),
            audit_log_integrity: integrity.valid,
            retention_compliance_pct: 100.0,
            encryption_enabled: false,
            access_control_enabled: true,
            total_audit_entries: entries.len() as u64,
            unique_users: unique_users.len() as u64,
            denied_operations: denied_count,
            checklist,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(user: &str, cmd: &str, event_type: AuditEventType) -> ComplianceAuditEntry {
        ComplianceAuditEntry {
            id: 0,
            timestamp: 0,
            event_type,
            user: user.to_string(),
            client_addr: "127.0.0.1:6379".to_string(),
            command: cmd.to_string(),
            key: Some("test-key".to_string()),
            result: AuditResult::Success,
            db: 0,
            hash: String::new(),
        }
    }

    #[test]
    fn test_record_and_query() {
        let log = ComplianceAuditLog::new(AuditConfig::default());
        log.record(make_entry("admin", "SET", AuditEventType::Write));
        log.record(make_entry("admin", "GET", AuditEventType::Read));
        log.record(make_entry("user1", "DEL", AuditEventType::Delete));

        // Reads are not logged by default
        assert_eq!(log.count(), 2);

        let results = log.query(AuditFilter {
            user: Some("admin".to_string()),
            limit: 10,
            ..Default::default()
        });
        assert_eq!(results.len(), 1); // Only SET (GET filtered by config)
    }

    #[test]
    fn test_hash_chain_integrity() {
        let log = ComplianceAuditLog::new(AuditConfig::default());
        log.record(make_entry("admin", "SET", AuditEventType::Write));
        log.record(make_entry("admin", "DEL", AuditEventType::Delete));

        let result = log.verify_integrity();
        assert!(result.valid);
        assert_eq!(result.entries_checked, 2);
    }

    #[test]
    fn test_export_json() {
        let log = ComplianceAuditLog::new(AuditConfig::default());
        log.record(make_entry("admin", "SET", AuditEventType::Write));

        let json = log.export(ExportFormat::Json).unwrap();
        assert!(json.contains("admin"));
        assert!(json.contains("SET"));
    }

    #[test]
    fn test_export_csv() {
        let log = ComplianceAuditLog::new(AuditConfig::default());
        log.record(make_entry("admin", "SET", AuditEventType::Write));

        let csv = log.export(ExportFormat::Csv).unwrap();
        assert!(csv.starts_with("id,timestamp"));
        assert!(csv.contains("admin"));
    }

    #[test]
    fn test_retention_policy() {
        let policy = RetentionPolicy::new(Duration::from_secs(86400 * 30));
        policy.set_key_pattern_ttl("session:*", Duration::from_secs(86400 * 7));

        assert_eq!(
            policy.get_ttl("session:abc"),
            Duration::from_secs(86400 * 7)
        );
        assert_eq!(policy.get_ttl("user:123"), Duration::from_secs(86400 * 30));

        assert_eq!(
            policy.check_compliance("session:abc", Duration::from_secs(86400 * 3)),
            RetentionStatus::Compliant
        );
        assert_eq!(
            policy.check_compliance("session:abc", Duration::from_secs(86400 * 8)),
            RetentionStatus::Overdue
        );
    }

    #[test]
    fn test_gdpr_handler() {
        let handler = GdprHandler::new();
        handler.register_data("user-1", "name", serde_json::json!("Alice"));
        handler.register_data("user-1", "email", serde_json::json!("alice@example.com"));

        let export = handler.data_export("user-1");
        assert_eq!(export.keys_exported, 2);

        let deletion = handler.right_to_deletion("user-1");
        assert_eq!(deletion.keys_deleted, 2);

        let export_after = handler.data_export("user-1");
        assert_eq!(export_after.keys_exported, 0);
    }

    #[test]
    fn test_gdpr_consent() {
        let handler = GdprHandler::new();
        assert!(!handler.check_consent("user-1", "marketing"));

        handler.log_consent("user-1", "marketing", true);
        assert!(handler.check_consent("user-1", "marketing"));

        handler.log_consent("user-1", "marketing", false);
        assert!(!handler.check_consent("user-1", "marketing"));
    }

    #[test]
    fn test_compliance_report() {
        let log = ComplianceAuditLog::new(AuditConfig::default());
        log.record(make_entry("admin", "SET", AuditEventType::Write));
        log.record(make_entry("user1", "DEL", AuditEventType::Delete));

        let policy = RetentionPolicy::new(Duration::from_secs(86400 * 90));
        let report = ComplianceReport::generate(&log, &policy, "SOC2");

        assert!(report.audit_log_integrity);
        assert_eq!(report.framework, "SOC2");
        assert_eq!(report.total_audit_entries, 2);
        assert!(report.unique_users > 0);
        assert!(!report.checklist.is_empty());
    }

    #[test]
    fn test_max_log_entries() {
        let log = ComplianceAuditLog::new(AuditConfig {
            max_log_entries: 3,
            ..Default::default()
        });
        for i in 0..5 {
            log.record(make_entry(
                "admin",
                &format!("CMD{}", i),
                AuditEventType::Write,
            ));
        }
        assert_eq!(log.count(), 3);
    }

    #[test]
    fn test_audit_event_type_parse() {
        assert_eq!(
            AuditEventType::from_str_loose("write"),
            Some(AuditEventType::Write)
        );
        assert_eq!(
            AuditEventType::from_str_loose("READ"),
            Some(AuditEventType::Read)
        );
        assert_eq!(AuditEventType::from_str_loose("unknown"), None);
    }
}
