//! Policy audit logging

use std::collections::HashMap;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Audit log level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditLevel {
    /// Debug level
    Debug,
    /// Info level
    Info,
    /// Warning level
    Warn,
    /// Error level
    Error,
}

impl AuditLevel {
    /// Get string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            AuditLevel::Debug => "DEBUG",
            AuditLevel::Info => "INFO",
            AuditLevel::Warn => "WARN",
            AuditLevel::Error => "ERROR",
        }
    }
}

/// An audit log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    /// Timestamp
    pub timestamp: u64,
    /// Log level
    pub level: AuditLevel,
    /// Event type
    pub event: String,
    /// Additional details
    pub details: HashMap<String, String>,
}

impl AuditEntry {
    /// Create a new audit entry
    pub fn new(level: AuditLevel, event: impl Into<String>) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};

        Self {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            level,
            event: event.into(),
            details: HashMap::new(),
        }
    }

    /// Add detail
    pub fn with_detail(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.details.insert(key.into(), value.into());
        self
    }
}

/// Policy audit log
pub struct PolicyAuditLog {
    /// Log entries
    entries: RwLock<Vec<AuditEntry>>,
    /// Maximum entries to keep
    max_entries: usize,
}

impl PolicyAuditLog {
    /// Create a new audit log
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
            max_entries: 10000,
        }
    }

    /// Create with custom max entries
    pub fn with_max_entries(max: usize) -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
            max_entries: max,
        }
    }

    /// Log an entry
    pub fn log(&self, entry: AuditEntry) {
        let mut entries = self.entries.write();
        entries.push(entry);

        // Trim if needed
        if entries.len() > self.max_entries {
            let drain_count = entries.len() - self.max_entries;
            entries.drain(0..drain_count);
        }
    }

    /// Get entry count
    pub fn count(&self) -> u64 {
        self.entries.read().len() as u64
    }

    /// Get entries
    pub fn get_entries(&self, limit: Option<usize>) -> Vec<AuditEntry> {
        let entries = self.entries.read();
        let limit = limit.unwrap_or(entries.len());

        entries.iter().rev().take(limit).cloned().collect()
    }

    /// Get entries by level
    pub fn get_by_level(&self, level: AuditLevel, limit: Option<usize>) -> Vec<AuditEntry> {
        let entries = self.entries.read();
        let limit = limit.unwrap_or(entries.len());

        entries
            .iter()
            .rev()
            .filter(|e| e.level == level)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Get entries by event type
    pub fn get_by_event(&self, event: &str, limit: Option<usize>) -> Vec<AuditEntry> {
        let entries = self.entries.read();
        let limit = limit.unwrap_or(entries.len());

        entries
            .iter()
            .rev()
            .filter(|e| e.event == event)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Get entries in time range
    pub fn get_in_range(&self, start: u64, end: u64, limit: Option<usize>) -> Vec<AuditEntry> {
        let entries = self.entries.read();
        let limit = limit.unwrap_or(entries.len());

        entries
            .iter()
            .rev()
            .filter(|e| e.timestamp >= start && e.timestamp <= end)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Clear the log
    pub fn clear(&self) {
        self.entries.write().clear();
    }

    /// Get statistics
    pub fn stats(&self) -> AuditStats {
        let entries = self.entries.read();

        let mut by_level: HashMap<String, u64> = HashMap::new();
        let mut by_event: HashMap<String, u64> = HashMap::new();

        for entry in entries.iter() {
            *by_level
                .entry(entry.level.as_str().to_string())
                .or_default() += 1;
            *by_event.entry(entry.event.clone()).or_default() += 1;
        }

        AuditStats {
            total_entries: entries.len() as u64,
            by_level,
            by_event,
        }
    }
}

impl Default for PolicyAuditLog {
    fn default() -> Self {
        Self::new()
    }
}

/// Audit statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditStats {
    /// Total entries
    pub total_entries: u64,
    /// Entries by level
    pub by_level: HashMap<String, u64>,
    /// Entries by event type
    pub by_event: HashMap<String, u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_log() {
        let log = PolicyAuditLog::new();

        log.log(AuditEntry::new(AuditLevel::Info, "test_event").with_detail("key", "value"));

        assert_eq!(log.count(), 1);

        let entries = log.get_entries(None);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].event, "test_event");
    }

    #[test]
    fn test_audit_log_limit() {
        let log = PolicyAuditLog::with_max_entries(5);

        for i in 0..10 {
            log.log(AuditEntry::new(AuditLevel::Info, format!("event_{}", i)));
        }

        assert_eq!(log.count(), 5);
    }

    #[test]
    fn test_get_by_level() {
        let log = PolicyAuditLog::new();

        log.log(AuditEntry::new(AuditLevel::Info, "info_event"));
        log.log(AuditEntry::new(AuditLevel::Warn, "warn_event"));
        log.log(AuditEntry::new(AuditLevel::Info, "info_event2"));

        let warns = log.get_by_level(AuditLevel::Warn, None);
        assert_eq!(warns.len(), 1);

        let infos = log.get_by_level(AuditLevel::Info, None);
        assert_eq!(infos.len(), 2);
    }
}
