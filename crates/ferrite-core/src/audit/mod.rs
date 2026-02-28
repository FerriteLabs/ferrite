//! Audit logging module
//!
//! This module provides compliance-focused audit logging for Ferrite.
//! It logs command executions, authentication attempts, and administrative
//! operations to a file or stdout in JSON or text format.
//!
//! # Features
//!
//! - **Non-blocking**: Uses async channels to avoid impacting command latency
//! - **Configurable filtering**: Log specific commands or exclude certain ones
//! - **Log rotation**: Automatic rotation based on file size
//! - **Multiple formats**: JSON (structured) or plain text
//!
//! # Configuration
//!
//! ```toml
//! [audit]
//! enabled = true
//! log_file = "./data/audit.log"
//! format = "json"
//! log_commands = []  # Empty = all commands
//! exclude_commands = ["PING", "ECHO"]
//! log_success = true
//! log_failures = true
//! log_auth = true
//! log_admin = true
//! max_file_size = 104857600  # 100MB
//! max_files = 10
//! ```

pub mod classifier;
pub mod compliance;
pub mod contracts;
pub mod lineage;
mod logger;

pub use compliance::{
    AuditConfig, AuditError, AuditEventType, AuditFilter, AuditResult, ChecklistItem,
    ChecklistStatus, ComplianceAuditEntry, ComplianceAuditLog, ComplianceReport,
    ComplianceReportData, DataExportResult, DeletionResult, ExportFormat, GdprHandler,
    IntegrityResult, RetentionPolicy, RetentionStatus,
};
pub use logger::{noop_handle, AuditEntry, AuditHandle, AuditLogger, SharedAuditHandle};
