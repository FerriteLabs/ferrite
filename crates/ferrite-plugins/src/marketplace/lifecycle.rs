//! Plugin Lifecycle Manager
//!
//! Manages the full lifecycle of plugins: install, upgrade, uninstall,
//! enable, disable, with audit logging for all operations.
//!
//! # Lifecycle States
//!
//! ```text
//!  ┌──────────┐    install    ┌──────────┐
//!  │ Available │─────────────▶│ Installed │
//!  └──────────┘               └────┬─────┘
//!                                  │
//!                     ┌────────────┼────────────┐
//!                     ▼            │            ▼
//!              ┌──────────┐       │     ┌───────────┐
//!              │  Enabled  │◀─────┘     │ Disabled  │
//!              └─────┬────┘             └─────┬─────┘
//!                    │                        │
//!                    │        upgrade          │
//!                    └────────┬───────────────┘
//!                             ▼
//!                      ┌──────────┐
//!                      │ Upgraded │
//!                      └──────────┘
//! ```

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::SystemTime;

use super::MarketplaceError;

// ---------------------------------------------------------------------------
// Audit log
// ---------------------------------------------------------------------------

/// A single audit log entry for a plugin operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    /// Timestamp of the operation (Unix millis).
    pub timestamp: u64,
    /// Plugin name.
    pub plugin_name: String,
    /// Type of operation performed.
    pub operation: AuditOperation,
    /// Whether the operation succeeded.
    pub success: bool,
    /// Optional detail message.
    pub detail: Option<String>,
}

/// Types of auditable plugin operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditOperation {
    /// Plugin was installed.
    Install,
    /// Plugin was upgraded to a new version.
    Upgrade,
    /// Plugin was uninstalled.
    Uninstall,
    /// Plugin was enabled.
    Enable,
    /// Plugin was disabled.
    Disable,
    /// Plugin WASM module was hot-swapped.
    HotSwap,
}

impl std::fmt::Display for AuditOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuditOperation::Install => write!(f, "install"),
            AuditOperation::Upgrade => write!(f, "upgrade"),
            AuditOperation::Uninstall => write!(f, "uninstall"),
            AuditOperation::Enable => write!(f, "enable"),
            AuditOperation::Disable => write!(f, "disable"),
            AuditOperation::HotSwap => write!(f, "hot-swap"),
        }
    }
}

// ---------------------------------------------------------------------------
// Lifecycle Manager
// ---------------------------------------------------------------------------

/// Manages the full lifecycle of installed plugins with audit logging.
pub struct PluginLifecycleManager {
    /// Maximum audit log entries to retain.
    max_audit_entries: usize,
    /// Audit log (most recent last).
    audit_log: RwLock<VecDeque<AuditEntry>>,
}

impl PluginLifecycleManager {
    /// Create a new lifecycle manager.
    pub fn new(max_audit_entries: usize) -> Self {
        Self {
            max_audit_entries,
            audit_log: RwLock::new(VecDeque::new()),
        }
    }

    /// Record an audit entry for a plugin operation.
    pub fn record_audit(
        &self,
        plugin_name: &str,
        operation: AuditOperation,
        success: bool,
        detail: Option<String>,
    ) {
        let entry = AuditEntry {
            timestamp: now_millis(),
            plugin_name: plugin_name.to_string(),
            operation,
            success,
            detail,
        };

        let mut log = self.audit_log.write();
        log.push_back(entry);
        while log.len() > self.max_audit_entries {
            log.pop_front();
        }
    }

    /// Perform an install operation with audit logging.
    pub fn audit_install(&self, plugin_name: &str, version: &str) -> Result<(), MarketplaceError> {
        self.record_audit(
            plugin_name,
            AuditOperation::Install,
            true,
            Some(format!("version {}", version)),
        );
        Ok(())
    }

    /// Perform an upgrade operation with audit logging.
    ///
    /// In a real implementation this would:
    /// 1. Download the new version
    /// 2. Verify integrity
    /// 3. Hot-swap the WASM module if supported
    /// 4. Fall back to disable → replace → enable if not
    pub fn audit_upgrade(
        &self,
        plugin_name: &str,
        from_version: &str,
        to_version: &str,
    ) -> Result<(), MarketplaceError> {
        self.record_audit(
            plugin_name,
            AuditOperation::Upgrade,
            true,
            Some(format!("{} → {}", from_version, to_version)),
        );
        Ok(())
    }

    /// Perform an uninstall operation with audit logging.
    pub fn audit_uninstall(&self, plugin_name: &str) -> Result<(), MarketplaceError> {
        self.record_audit(plugin_name, AuditOperation::Uninstall, true, None);
        Ok(())
    }

    /// Perform an enable operation with audit logging.
    pub fn audit_enable(&self, plugin_name: &str) -> Result<(), MarketplaceError> {
        self.record_audit(plugin_name, AuditOperation::Enable, true, None);
        Ok(())
    }

    /// Perform a disable operation with audit logging.
    pub fn audit_disable(&self, plugin_name: &str) -> Result<(), MarketplaceError> {
        self.record_audit(plugin_name, AuditOperation::Disable, true, None);
        Ok(())
    }

    /// Record a hot-swap event.
    pub fn audit_hot_swap(&self, plugin_name: &str, success: bool, detail: Option<String>) {
        self.record_audit(plugin_name, AuditOperation::HotSwap, success, detail);
    }

    /// Get the full audit log.
    pub fn audit_log(&self) -> Vec<AuditEntry> {
        self.audit_log.read().iter().cloned().collect()
    }

    /// Get audit log entries for a specific plugin.
    pub fn audit_log_for(&self, plugin_name: &str) -> Vec<AuditEntry> {
        self.audit_log
            .read()
            .iter()
            .filter(|e| e.plugin_name == plugin_name)
            .cloned()
            .collect()
    }

    /// Get the most recent N audit entries.
    pub fn recent_audits(&self, n: usize) -> Vec<AuditEntry> {
        let log = self.audit_log.read();
        log.iter().rev().take(n).cloned().collect()
    }

    /// Total number of audit entries.
    pub fn audit_count(&self) -> usize {
        self.audit_log.read().len()
    }

    /// Clear the audit log.
    pub fn clear_audit_log(&self) {
        self.audit_log.write().clear();
    }
}

impl Default for PluginLifecycleManager {
    fn default() -> Self {
        Self::new(10_000)
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lifecycle_audit_install() {
        let mgr = PluginLifecycleManager::new(100);
        mgr.audit_install("my-plugin", "1.0.0").unwrap();

        let log = mgr.audit_log();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].plugin_name, "my-plugin");
        assert_eq!(log[0].operation, AuditOperation::Install);
        assert!(log[0].success);
    }

    #[test]
    fn test_lifecycle_audit_upgrade() {
        let mgr = PluginLifecycleManager::new(100);
        mgr.audit_upgrade("my-plugin", "1.0.0", "2.0.0").unwrap();

        let log = mgr.audit_log();
        assert_eq!(log.len(), 1);
        assert!(log[0].detail.as_ref().unwrap().contains("1.0.0"));
        assert!(log[0].detail.as_ref().unwrap().contains("2.0.0"));
    }

    #[test]
    fn test_lifecycle_audit_full_cycle() {
        let mgr = PluginLifecycleManager::new(100);

        mgr.audit_install("plugin-a", "1.0.0").unwrap();
        mgr.audit_enable("plugin-a").unwrap();
        mgr.audit_disable("plugin-a").unwrap();
        mgr.audit_upgrade("plugin-a", "1.0.0", "2.0.0").unwrap();
        mgr.audit_enable("plugin-a").unwrap();
        mgr.audit_uninstall("plugin-a").unwrap();

        assert_eq!(mgr.audit_count(), 6);

        let plugin_log = mgr.audit_log_for("plugin-a");
        assert_eq!(plugin_log.len(), 6);
    }

    #[test]
    fn test_lifecycle_audit_log_truncation() {
        let mgr = PluginLifecycleManager::new(3);

        for i in 0..5 {
            mgr.audit_install(&format!("plugin-{}", i), "1.0.0")
                .unwrap();
        }

        // Only the last 3 should remain
        assert_eq!(mgr.audit_count(), 3);
        let log = mgr.audit_log();
        assert_eq!(log[0].plugin_name, "plugin-2");
        assert_eq!(log[2].plugin_name, "plugin-4");
    }

    #[test]
    fn test_lifecycle_recent_audits() {
        let mgr = PluginLifecycleManager::new(100);
        mgr.audit_install("a", "1.0.0").unwrap();
        mgr.audit_install("b", "1.0.0").unwrap();
        mgr.audit_install("c", "1.0.0").unwrap();

        let recent = mgr.recent_audits(2);
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].plugin_name, "c"); // most recent first
        assert_eq!(recent[1].plugin_name, "b");
    }

    #[test]
    fn test_lifecycle_hot_swap_audit() {
        let mgr = PluginLifecycleManager::new(100);
        mgr.audit_hot_swap("plugin-x", true, Some("v1 → v2".to_string()));
        mgr.audit_hot_swap("plugin-x", false, Some("compilation failed".to_string()));

        let log = mgr.audit_log_for("plugin-x");
        assert_eq!(log.len(), 2);
        assert!(log[0].success);
        assert!(!log[1].success);
    }

    #[test]
    fn test_lifecycle_clear_audit() {
        let mgr = PluginLifecycleManager::new(100);
        mgr.audit_install("a", "1.0.0").unwrap();
        assert_eq!(mgr.audit_count(), 1);

        mgr.clear_audit_log();
        assert_eq!(mgr.audit_count(), 0);
    }

    #[test]
    fn test_audit_operation_display() {
        assert_eq!(AuditOperation::Install.to_string(), "install");
        assert_eq!(AuditOperation::HotSwap.to_string(), "hot-swap");
    }
}
