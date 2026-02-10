//! Async audit logger implementation
//!
//! This module provides non-blocking audit logging that doesn't impact
//! command processing performance.

use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::Serialize;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

use crate::config::{AuditConfig, AuditFormat};

/// Maximum channel buffer size for audit entries
const AUDIT_CHANNEL_SIZE: usize = 10000;

/// An audit log entry
#[derive(Debug, Clone, Serialize)]
pub struct AuditEntry {
    /// Timestamp when the command was received
    pub timestamp: String,
    /// Timestamp as Unix epoch milliseconds
    pub timestamp_ms: u64,
    /// Client address
    pub client_addr: Option<String>,
    /// Authenticated user
    pub user: String,
    /// Database index
    pub database: u8,
    /// Command name (uppercase)
    pub command: String,
    /// Command arguments (keys, not values for security)
    pub keys: Vec<String>,
    /// Whether the command succeeded
    pub success: bool,
    /// Error message if failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Command duration in microseconds
    pub duration_us: u64,
    /// Protocol version (2 or 3)
    pub protocol_version: u8,
}

impl AuditEntry {
    /// Create a new audit entry
    pub fn new(
        client_addr: Option<SocketAddr>,
        user: String,
        database: u8,
        command: String,
        keys: Vec<String>,
        protocol_version: u8,
    ) -> Self {
        let now = SystemTime::now();
        let timestamp_ms = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Format timestamp as ISO 8601
        let timestamp = chrono::Utc::now()
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string();

        Self {
            timestamp,
            timestamp_ms,
            client_addr: client_addr.map(|a| a.to_string()),
            user,
            database,
            command,
            keys,
            success: true,
            error: None,
            duration_us: 0,
            protocol_version,
        }
    }

    /// Mark the entry as completed with duration
    pub fn complete(&mut self, duration: Duration, success: bool, error: Option<String>) {
        self.duration_us = duration.as_micros() as u64;
        self.success = success;
        self.error = error;
    }

    /// Format as JSON
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }

    /// Format as text line
    pub fn to_text(&self) -> String {
        let status = if self.success { "OK" } else { "ERR" };
        let keys_str = self.keys.join(" ");
        let error_str = self.error.as_deref().unwrap_or("");

        format!(
            "{} {} user={} db={} cmd={} keys=[{}] status={} duration={}us {}",
            self.timestamp,
            self.client_addr.as_deref().unwrap_or("-"),
            self.user,
            self.database,
            self.command,
            keys_str,
            status,
            self.duration_us,
            error_str
        )
    }
}

/// Handle to send audit entries to the logger
#[derive(Clone)]
pub struct AuditHandle {
    sender: mpsc::Sender<AuditEntry>,
    config: Arc<AuditConfig>,
}

impl AuditHandle {
    /// Log an audit entry if auditing is enabled and the command matches filters
    pub async fn log(&self, entry: AuditEntry) {
        if !self.should_log(&entry) {
            return;
        }

        if let Err(e) = self.sender.try_send(entry) {
            // Channel full or closed - log warning but don't block
            match e {
                mpsc::error::TrySendError::Full(_) => {
                    warn!("Audit log channel full, dropping entry");
                }
                mpsc::error::TrySendError::Closed(_) => {
                    debug!("Audit log channel closed");
                }
            }
        }
    }

    /// Check if an entry should be logged based on config
    fn should_log(&self, entry: &AuditEntry) -> bool {
        // Check success/failure filters
        if entry.success && !self.config.log_success {
            return false;
        }
        if !entry.success && !self.config.log_failures {
            return false;
        }

        // Check exclude list first
        if self
            .config
            .exclude_commands
            .iter()
            .any(|c| c.eq_ignore_ascii_case(&entry.command))
        {
            return false;
        }

        // Check include list (empty = all commands)
        if !self.config.log_commands.is_empty()
            && !self
                .config
                .log_commands
                .iter()
                .any(|c| c.eq_ignore_ascii_case(&entry.command))
        {
            return false;
        }

        true
    }

    /// Check if this is an admin command that should be logged
    pub fn is_admin_command(command: &str) -> bool {
        matches!(
            command.to_uppercase().as_str(),
            "CONFIG"
                | "FLUSHDB"
                | "FLUSHALL"
                | "SHUTDOWN"
                | "DEBUG"
                | "BGREWRITEAOF"
                | "BGSAVE"
                | "SAVE"
                | "SLAVEOF"
                | "REPLICAOF"
                | "ACL"
                | "MODULE"
                | "CLUSTER"
                | "MIGRATE"
                | "FAILOVER"
        )
    }
}

/// Async audit logger that writes entries to file or stdout
pub struct AuditLogger {
    receiver: mpsc::Receiver<AuditEntry>,
    config: Arc<AuditConfig>,
    file: Option<File>,
    current_file_size: u64,
}

impl AuditLogger {
    /// Create a new audit logger and return the handle for sending entries
    pub async fn new(config: AuditConfig) -> std::io::Result<(Self, AuditHandle)> {
        let config = Arc::new(config);
        let (sender, receiver) = mpsc::channel(AUDIT_CHANNEL_SIZE);

        // Open log file if configured
        let file = if let Some(path) = &config.log_file {
            // Ensure parent directory exists
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            Some(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .await?,
            )
        } else {
            None
        };

        let current_file_size = if let Some(ref f) = file {
            f.metadata().await?.len()
        } else {
            0
        };

        let handle = AuditHandle {
            sender,
            config: config.clone(),
        };

        let logger = Self {
            receiver,
            config,
            file,
            current_file_size,
        };

        Ok((logger, handle))
    }

    /// Run the logger, processing entries until shutdown
    pub async fn run(mut self) {
        debug!("Audit logger started");

        while let Some(entry) = self.receiver.recv().await {
            if let Err(e) = self.write_entry(&entry).await {
                error!("Failed to write audit entry: {}", e);
            }
        }

        debug!("Audit logger shutting down");
    }

    /// Write a single entry
    async fn write_entry(&mut self, entry: &AuditEntry) -> std::io::Result<()> {
        let line = match self.config.format {
            AuditFormat::Json => format!("{}\n", entry.to_json()),
            AuditFormat::Text => format!("{}\n", entry.to_text()),
        };

        let bytes = line.as_bytes();
        let bytes_len = bytes.len() as u64;

        // Check if rotation is needed before writing
        let needs_rotation = self.file.is_some()
            && self.config.max_file_size > 0
            && self.current_file_size + bytes_len > self.config.max_file_size;

        if needs_rotation {
            self.rotate_log().await?;
        }

        if let Some(ref mut file) = self.file {
            file.write_all(bytes).await?;
            file.flush().await?;
            self.current_file_size += bytes_len;
        } else {
            // Write to stdout
            let mut stdout = std::io::stdout().lock();
            stdout.write_all(bytes)?;
            stdout.flush()?;
        }

        Ok(())
    }

    /// Rotate log file
    async fn rotate_log(&mut self) -> std::io::Result<()> {
        let Some(path) = &self.config.log_file else {
            return Ok(());
        };

        // Close current file
        self.file = None;

        // Rotate existing files
        for i in (1..self.config.max_files).rev() {
            let from = rotated_path(path, i);
            let to = rotated_path(path, i + 1);
            if tokio::fs::try_exists(&from).await.unwrap_or(false) {
                let _ = tokio::fs::rename(&from, &to).await;
            }
        }

        // Move current file to .1
        let rotated = rotated_path(path, 1);
        if tokio::fs::try_exists(path).await.unwrap_or(false) {
            tokio::fs::rename(path, &rotated).await?;
        }

        // Delete oldest if we have too many
        let oldest = rotated_path(path, self.config.max_files);
        if tokio::fs::try_exists(&oldest).await.unwrap_or(false) {
            let _ = tokio::fs::remove_file(&oldest).await;
        }

        // Open new file
        self.file = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .await?,
        );
        self.current_file_size = 0;

        Ok(())
    }
}

/// Get path for rotated log file
fn rotated_path(base: &Path, index: usize) -> PathBuf {
    let mut path = base.to_path_buf();
    let filename = path.file_name().unwrap_or_default().to_string_lossy();
    path.set_file_name(format!("{}.{}", filename, index));
    path
}

/// Shared audit handle type
pub type SharedAuditHandle = Arc<AuditHandle>;

/// Create a no-op audit handle for when auditing is disabled
pub fn noop_handle() -> SharedAuditHandle {
    let config = AuditConfig {
        enabled: false,
        ..Default::default()
    };
    let (sender, _receiver) = mpsc::channel(1);
    Arc::new(AuditHandle {
        sender,
        config: Arc::new(config),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_entry_new() {
        let entry = AuditEntry::new(
            None,
            "default".to_string(),
            0,
            "SET".to_string(),
            vec!["mykey".to_string()],
            2,
        );

        assert_eq!(entry.command, "SET");
        assert_eq!(entry.user, "default");
        assert_eq!(entry.database, 0);
        assert!(entry.success);
        assert!(entry.error.is_none());
    }

    #[test]
    fn test_audit_entry_complete() {
        let mut entry = AuditEntry::new(
            None,
            "default".to_string(),
            0,
            "GET".to_string(),
            vec!["key".to_string()],
            2,
        );

        entry.complete(Duration::from_micros(150), true, None);
        assert_eq!(entry.duration_us, 150);
        assert!(entry.success);

        entry.complete(
            Duration::from_micros(200),
            false,
            Some("ERR not found".to_string()),
        );
        assert_eq!(entry.duration_us, 200);
        assert!(!entry.success);
        assert_eq!(entry.error, Some("ERR not found".to_string()));
    }

    #[test]
    fn test_audit_entry_to_json() {
        let entry = AuditEntry::new(
            None,
            "user1".to_string(),
            1,
            "HSET".to_string(),
            vec!["hash".to_string(), "field".to_string()],
            3,
        );

        let json = entry.to_json();
        assert!(json.contains("\"command\":\"HSET\""));
        assert!(json.contains("\"user\":\"user1\""));
        assert!(json.contains("\"database\":1"));
    }

    #[test]
    fn test_audit_entry_to_text() {
        let entry = AuditEntry::new(
            None,
            "default".to_string(),
            0,
            "DEL".to_string(),
            vec!["key1".to_string(), "key2".to_string()],
            2,
        );

        let text = entry.to_text();
        assert!(text.contains("cmd=DEL"));
        assert!(text.contains("keys=[key1 key2]"));
        assert!(text.contains("user=default"));
    }

    #[test]
    fn test_is_admin_command() {
        assert!(AuditHandle::is_admin_command("CONFIG"));
        assert!(AuditHandle::is_admin_command("config"));
        assert!(AuditHandle::is_admin_command("FLUSHDB"));
        assert!(AuditHandle::is_admin_command("ACL"));
        assert!(!AuditHandle::is_admin_command("GET"));
        assert!(!AuditHandle::is_admin_command("SET"));
        assert!(!AuditHandle::is_admin_command("HSET"));
    }

    #[test]
    fn test_should_log_filters() {
        let config = Arc::new(AuditConfig {
            enabled: true,
            log_success: true,
            log_failures: true,
            log_commands: vec!["GET".to_string(), "SET".to_string()],
            exclude_commands: vec!["PING".to_string()],
            ..Default::default()
        });

        let (sender, _) = mpsc::channel(1);
        let handle = AuditHandle { sender, config };

        // Should log GET (in include list)
        let entry = AuditEntry::new(None, "default".to_string(), 0, "GET".to_string(), vec![], 2);
        assert!(handle.should_log(&entry));

        // Should not log DEL (not in include list)
        let entry = AuditEntry::new(None, "default".to_string(), 0, "DEL".to_string(), vec![], 2);
        assert!(!handle.should_log(&entry));

        // Should not log PING (in exclude list)
        let entry = AuditEntry::new(
            None,
            "default".to_string(),
            0,
            "PING".to_string(),
            vec![],
            2,
        );
        assert!(!handle.should_log(&entry));
    }

    #[test]
    fn test_should_log_success_failure() {
        let config = Arc::new(AuditConfig {
            enabled: true,
            log_success: false,
            log_failures: true,
            log_commands: vec![],
            exclude_commands: vec![],
            ..Default::default()
        });

        let (sender, _) = mpsc::channel(1);
        let handle = AuditHandle { sender, config };

        // Should not log successful commands
        let entry = AuditEntry::new(None, "default".to_string(), 0, "GET".to_string(), vec![], 2);
        assert!(!handle.should_log(&entry));

        // Should log failed commands
        let mut entry =
            AuditEntry::new(None, "default".to_string(), 0, "GET".to_string(), vec![], 2);
        entry.success = false;
        assert!(handle.should_log(&entry));
    }

    #[test]
    fn test_rotated_path() {
        let base = PathBuf::from("/var/log/audit.log");
        assert_eq!(
            rotated_path(&base, 1),
            PathBuf::from("/var/log/audit.log.1")
        );
        assert_eq!(
            rotated_path(&base, 5),
            PathBuf::from("/var/log/audit.log.5")
        );
    }
}
