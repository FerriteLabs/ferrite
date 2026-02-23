//! Configuration module for Ferrite
//!
//! This module handles loading and parsing configuration from TOML files,
//! with sensible defaults for all optional values.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::error::{FerriteError, Result};

/// TLS configuration (defined here to avoid circular dependency with server)
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to the TLS certificate file.
    pub cert_file: String,
    /// Path to the TLS private key file.
    pub key_file: String,
    /// Optional path to the CA certificate for client verification.
    pub ca_file: Option<String>,
    /// Whether to require client certificates (mTLS).
    pub require_client_cert: bool,
}

impl TlsConfig {
    /// Create a new TLS configuration with the given certificate and key files.
    pub fn new(cert_file: String, key_file: String) -> Self {
        Self {
            cert_file,
            key_file,
            ca_file: None,
            require_client_cert: false,
        }
    }

    /// Enable client certificate authentication (mTLS) with the given CA file.
    pub fn with_client_auth(mut self, ca_file: String, require: bool) -> Self {
        self.ca_file = Some(ca_file);
        self.require_client_cert = require;
        self
    }
}

/// Typed configuration keys for hot-reload and listing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConfigKey {
    /// Server bind address (`server.bind`).
    ServerBind,
    /// Server listening port (`server.port`).
    ServerPort,
    /// Maximum concurrent connections (`server.max_connections`).
    ServerMaxConnections,
    /// TCP keepalive interval in seconds (`server.tcp_keepalive`).
    ServerTcpKeepalive,
    /// Connection timeout in seconds (`server.timeout`).
    ServerTimeout,
    /// Log level (`logging.level`).
    LoggingLevel,
    /// Log output format (`logging.format`).
    LoggingFormat,
    /// Storage backend type (`storage.backend`).
    StorageBackend,
    /// Number of databases (`storage.databases`).
    StorageDatabases,
    /// Maximum memory usage in bytes (`storage.max_memory`).
    StorageMaxMemory,
    /// AOF persistence enabled (`persistence.aof_enabled`).
    PersistenceAofEnabled,
    /// AOF sync policy (`persistence.aof_sync`).
    PersistenceAofSync,
    /// Checkpointing enabled (`persistence.checkpoint_enabled`).
    PersistenceCheckpointEnabled,
    /// Metrics collection enabled (`metrics.enabled`).
    MetricsEnabled,
    /// Metrics endpoint bind address (`metrics.bind`).
    MetricsBind,
    /// Metrics endpoint port (`metrics.port`).
    MetricsPort,
    /// TLS enabled (`tls.enabled`).
    TlsEnabled,
    /// TLS listening port (`tls.port`).
    TlsPort,
    /// TLS certificate file path (`tls.cert_file`).
    TlsCertFile,
    /// TLS private key file path (`tls.key_file`).
    TlsKeyFile,
    /// Audit logging enabled (`audit.enabled`).
    AuditEnabled,
    /// Encryption at rest enabled (`encryption.enabled`).
    EncryptionEnabled,
    /// Encryption algorithm (`encryption.algorithm`).
    EncryptionAlgorithm,
    /// Encryption key file path (`encryption.key_file`).
    EncryptionKeyFile,
    /// Cluster mode enabled (`cluster.enabled`).
    ClusterEnabled,
    /// Protocol max bulk string length (`server.proto_max_bulk_len`).
    ServerProtoMaxBulkLen,
    /// Protocol max array elements (`server.proto_max_multi_bulk_len`).
    ServerProtoMaxMultiBulkLen,
    /// Protocol max nesting depth (`server.proto_max_nesting_depth`).
    ServerProtoMaxNestingDepth,
    /// Storage max key size (`storage.max_key_size`).
    StorageMaxKeySize,
    /// Storage max value size (`storage.max_value_size`).
    StorageMaxValueSize,
}

impl ConfigKey {
    /// Return the dotted config path string for this key (e.g., `"server.port"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            ConfigKey::ServerBind => "server.bind",
            ConfigKey::ServerPort => "server.port",
            ConfigKey::ServerMaxConnections => "server.max_connections",
            ConfigKey::ServerTcpKeepalive => "server.tcp_keepalive",
            ConfigKey::ServerTimeout => "server.timeout",
            ConfigKey::LoggingLevel => "logging.level",
            ConfigKey::LoggingFormat => "logging.format",
            ConfigKey::StorageBackend => "storage.backend",
            ConfigKey::StorageDatabases => "storage.databases",
            ConfigKey::StorageMaxMemory => "storage.max_memory",
            ConfigKey::PersistenceAofEnabled => "persistence.aof_enabled",
            ConfigKey::PersistenceAofSync => "persistence.aof_sync",
            ConfigKey::PersistenceCheckpointEnabled => "persistence.checkpoint_enabled",
            ConfigKey::MetricsEnabled => "metrics.enabled",
            ConfigKey::MetricsBind => "metrics.bind",
            ConfigKey::MetricsPort => "metrics.port",
            ConfigKey::TlsEnabled => "tls.enabled",
            ConfigKey::TlsPort => "tls.port",
            ConfigKey::TlsCertFile => "tls.cert_file",
            ConfigKey::TlsKeyFile => "tls.key_file",
            ConfigKey::AuditEnabled => "audit.enabled",
            ConfigKey::EncryptionEnabled => "encryption.enabled",
            ConfigKey::EncryptionAlgorithm => "encryption.algorithm",
            ConfigKey::EncryptionKeyFile => "encryption.key_file",
            ConfigKey::ClusterEnabled => "cluster.enabled",
            ConfigKey::ServerProtoMaxBulkLen => "server.proto_max_bulk_len",
            ConfigKey::ServerProtoMaxMultiBulkLen => "server.proto_max_multi_bulk_len",
            ConfigKey::ServerProtoMaxNestingDepth => "server.proto_max_nesting_depth",
            ConfigKey::StorageMaxKeySize => "storage.max_key_size",
            ConfigKey::StorageMaxValueSize => "storage.max_value_size",
        }
    }

    /// Parse a dotted config path string into a `ConfigKey`, if valid.
    pub fn parse_str(path: &str) -> Option<Self> {
        Some(match path {
            "server.bind" => ConfigKey::ServerBind,
            "server.port" => ConfigKey::ServerPort,
            "server.max_connections" => ConfigKey::ServerMaxConnections,
            "server.tcp_keepalive" => ConfigKey::ServerTcpKeepalive,
            "server.timeout" => ConfigKey::ServerTimeout,
            "logging.level" => ConfigKey::LoggingLevel,
            "logging.format" => ConfigKey::LoggingFormat,
            "storage.backend" => ConfigKey::StorageBackend,
            "storage.databases" => ConfigKey::StorageDatabases,
            "storage.max_memory" => ConfigKey::StorageMaxMemory,
            "persistence.aof_enabled" => ConfigKey::PersistenceAofEnabled,
            "persistence.aof_sync" => ConfigKey::PersistenceAofSync,
            "persistence.checkpoint_enabled" => ConfigKey::PersistenceCheckpointEnabled,
            "metrics.enabled" => ConfigKey::MetricsEnabled,
            "metrics.bind" => ConfigKey::MetricsBind,
            "metrics.port" => ConfigKey::MetricsPort,
            "tls.enabled" => ConfigKey::TlsEnabled,
            "tls.port" => ConfigKey::TlsPort,
            "tls.cert_file" => ConfigKey::TlsCertFile,
            "tls.key_file" => ConfigKey::TlsKeyFile,
            "audit.enabled" => ConfigKey::AuditEnabled,
            "encryption.enabled" => ConfigKey::EncryptionEnabled,
            "encryption.algorithm" => ConfigKey::EncryptionAlgorithm,
            "encryption.key_file" => ConfigKey::EncryptionKeyFile,
            "cluster.enabled" => ConfigKey::ClusterEnabled,
            "server.proto_max_bulk_len" => ConfigKey::ServerProtoMaxBulkLen,
            "server.proto_max_multi_bulk_len" => ConfigKey::ServerProtoMaxMultiBulkLen,
            "server.proto_max_nesting_depth" => ConfigKey::ServerProtoMaxNestingDepth,
            "storage.max_key_size" => ConfigKey::StorageMaxKeySize,
            "storage.max_value_size" => ConfigKey::StorageMaxValueSize,
            _ => return None,
        })
    }

    /// Return `true` if changing this key requires a server restart to take effect.
    pub fn requires_restart(&self) -> bool {
        matches!(
            self,
            ConfigKey::ServerBind
                | ConfigKey::ServerPort
                | ConfigKey::StorageBackend
                | ConfigKey::StorageDatabases
                | ConfigKey::StorageMaxMemory
                | ConfigKey::TlsEnabled
                | ConfigKey::TlsPort
                | ConfigKey::TlsCertFile
                | ConfigKey::TlsKeyFile
                | ConfigKey::EncryptionEnabled
                | ConfigKey::EncryptionKeyFile
                | ConfigKey::ClusterEnabled
        )
    }

    /// Return a slice containing all known configuration keys.
    pub fn all() -> &'static [ConfigKey] {
        &[
            ConfigKey::ServerBind,
            ConfigKey::ServerPort,
            ConfigKey::ServerMaxConnections,
            ConfigKey::ServerTcpKeepalive,
            ConfigKey::ServerTimeout,
            ConfigKey::LoggingLevel,
            ConfigKey::LoggingFormat,
            ConfigKey::StorageBackend,
            ConfigKey::StorageDatabases,
            ConfigKey::StorageMaxMemory,
            ConfigKey::PersistenceAofEnabled,
            ConfigKey::PersistenceAofSync,
            ConfigKey::PersistenceCheckpointEnabled,
            ConfigKey::MetricsEnabled,
            ConfigKey::MetricsBind,
            ConfigKey::MetricsPort,
            ConfigKey::TlsEnabled,
            ConfigKey::TlsPort,
            ConfigKey::TlsCertFile,
            ConfigKey::TlsKeyFile,
            ConfigKey::AuditEnabled,
            ConfigKey::EncryptionEnabled,
            ConfigKey::EncryptionAlgorithm,
            ConfigKey::EncryptionKeyFile,
            ConfigKey::ClusterEnabled,
            ConfigKey::ServerProtoMaxBulkLen,
            ConfigKey::ServerProtoMaxMultiBulkLen,
            ConfigKey::ServerProtoMaxNestingDepth,
            ConfigKey::StorageMaxKeySize,
            ConfigKey::StorageMaxValueSize,
        ]
    }
}

/// Default replication backlog size in bytes (1MB)
pub const DEFAULT_REPLICATION_BACKLOG_BYTES: u64 = 1024 * 1024;

/// Main configuration structure for Ferrite
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Server configuration
    pub server: ServerConfig,

    /// Storage configuration
    pub storage: StorageConfig,

    /// Persistence configuration
    pub persistence: PersistenceConfig,

    /// Metrics configuration
    pub metrics: MetricsConfig,

    /// Logging configuration
    pub logging: LoggingConfig,

    /// TLS configuration
    pub tls: TlsConfigSettings,

    /// Cloud tiering configuration
    pub cloud: CloudTieringConfig,

    /// Cluster configuration
    pub cluster: ClusterConfigSettings,

    /// Replication configuration
    pub replication: ReplicationConfig,

    /// Audit logging configuration
    pub audit: AuditConfig,

    /// Encryption at rest configuration
    pub encryption: EncryptionConfig,

    /// OpenTelemetry configuration
    pub otel: OpenTelemetryConfig,
}

impl Config {
    /// Load configuration from a TOML file
    pub fn from_file(path: &PathBuf) -> Result<Self> {
        let contents = std::fs::read_to_string(path).map_err(|e| {
            FerriteError::Config(format!("Failed to read config file {:?}: {}", path, e))
        })?;

        Self::parse_str(&contents)
    }

    /// Parse configuration from a TOML string
    pub fn parse_str(contents: &str) -> Result<Self> {
        toml::from_str(contents)
            .map_err(|e| FerriteError::Config(format!("Failed to parse config: {}", e)))
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        // Validate server config
        if self.server.port == 0 {
            return Err(FerriteError::Config("Port cannot be 0".to_string()));
        }

        if self.server.max_connections == 0 {
            return Err(FerriteError::Config(
                "Max connections cannot be 0".to_string(),
            ));
        }

        // Validate storage config
        if self.storage.databases == 0 || self.storage.databases > 16 {
            return Err(FerriteError::Config(
                "Number of databases must be between 1 and 16".to_string(),
            ));
        }

        if self.storage.max_memory == 0 {
            return Err(FerriteError::Config("Max memory cannot be 0".to_string()));
        }

        // Validate TLS config
        self.tls.validate()?;

        // Validate encryption config
        self.encryption.validate()?;

        Ok(())
    }

    /// Write configuration to a TOML file
    ///
    /// Uses atomic write (temp file + rename) to prevent corruption.
    pub fn to_file(&self, path: &Path) -> Result<()> {
        let toml_str = toml::to_string_pretty(self)
            .map_err(|e| FerriteError::Config(format!("Failed to serialize config: {}", e)))?;

        // Write to temp file first
        let temp_path = path.with_extension("toml.tmp");
        std::fs::write(&temp_path, toml_str).map_err(|e| {
            FerriteError::Config(format!("Failed to write temp config file: {}", e))
        })?;

        // Atomic rename
        std::fs::rename(&temp_path, path).map_err(|e| {
            // Try to clean up temp file
            let _ = std::fs::remove_file(&temp_path);
            FerriteError::Config(format!("Failed to rename config file: {}", e))
        })?;

        Ok(())
    }

    /// Get a config parameter by path (e.g., "server.port", "logging.level")
    ///
    /// Returns the value as a string, or None if the parameter doesn't exist.
    pub fn get_param(&self, path: &str) -> Option<String> {
        // Convert config to TOML value for easy traversal
        let value = toml::Value::try_from(self).ok()?;

        // Split path and traverse
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = &value;

        for part in &parts {
            current = current.get(part)?;
        }

        // Convert to string representation
        match current {
            toml::Value::String(s) => Some(s.clone()),
            toml::Value::Integer(i) => Some(i.to_string()),
            toml::Value::Float(f) => Some(f.to_string()),
            toml::Value::Boolean(b) => Some(b.to_string()),
            toml::Value::Array(_) | toml::Value::Table(_) => {
                Some(toml::to_string_pretty(current).ok()?)
            }
            toml::Value::Datetime(dt) => Some(dt.to_string()),
        }
    }

    /// Set a config parameter by path (e.g., "server.port", "logging.level")
    ///
    /// Returns Ok(true) if the parameter was set, Ok(false) if the parameter
    /// doesn't exist or can't be modified, or an error if the value is invalid.
    pub fn set_param(&mut self, path: &str, value: &str) -> Result<bool> {
        let key = match ConfigKey::parse_str(path) {
            Some(key) => key,
            None => return Ok(false),
        };
        if key.requires_restart() {
            return Ok(false);
        }

        match key {
            ConfigKey::LoggingLevel => {
                self.logging.level = value.to_string();
                Ok(true)
            }
            ConfigKey::PersistenceAofSync => {
                let policy = match value.to_lowercase().as_str() {
                    "always" => SyncPolicy::Always,
                    "everysecond" | "everysec" => SyncPolicy::EverySecond,
                    "no" => SyncPolicy::No,
                    _ => {
                        return Err(FerriteError::Config(format!(
                            "Invalid aof_sync value: {}. Expected: always, everysecond, no",
                            value
                        )))
                    }
                };
                self.persistence.aof_sync = policy;
                Ok(true)
            }
            ConfigKey::MetricsEnabled => {
                let enabled = value.parse::<bool>().map_err(|_| {
                    FerriteError::Config(format!(
                        "Invalid boolean value: {}. Expected: true, false",
                        value
                    ))
                })?;
                self.metrics.enabled = enabled;
                Ok(true)
            }
            ConfigKey::AuditEnabled => {
                let enabled = value.parse::<bool>().map_err(|_| {
                    FerriteError::Config(format!(
                        "Invalid boolean value: {}. Expected: true, false",
                        value
                    ))
                })?;
                self.audit.enabled = enabled;
                Ok(true)
            }
            ConfigKey::ServerTcpKeepalive => {
                let keepalive = value.parse::<u64>().map_err(|_| {
                    FerriteError::Config(format!("Invalid integer value: {}", value))
                })?;
                self.server.tcp_keepalive = keepalive;
                Ok(true)
            }
            ConfigKey::ServerTimeout => {
                let timeout = value.parse::<u64>().map_err(|_| {
                    FerriteError::Config(format!("Invalid integer value: {}", value))
                })?;
                self.server.timeout = timeout;
                Ok(true)
            }
            ConfigKey::ServerMaxConnections => {
                let max_conn = value.parse::<usize>().map_err(|_| {
                    FerriteError::Config(format!("Invalid integer value: {}", value))
                })?;
                self.server.max_connections = max_conn;
                Ok(true)
            }
            ConfigKey::ServerProtoMaxBulkLen => {
                let v = value.parse::<usize>().map_err(|_| {
                    FerriteError::Config(format!("Invalid integer value: {}", value))
                })?;
                self.server.proto_max_bulk_len = v;
                Ok(true)
            }
            ConfigKey::ServerProtoMaxMultiBulkLen => {
                let v = value.parse::<usize>().map_err(|_| {
                    FerriteError::Config(format!("Invalid integer value: {}", value))
                })?;
                self.server.proto_max_multi_bulk_len = v;
                Ok(true)
            }
            ConfigKey::ServerProtoMaxNestingDepth => {
                let v = value.parse::<usize>().map_err(|_| {
                    FerriteError::Config(format!("Invalid integer value: {}", value))
                })?;
                self.server.proto_max_nesting_depth = v;
                Ok(true)
            }
            ConfigKey::StorageMaxKeySize => {
                let v = value.parse::<usize>().map_err(|_| {
                    FerriteError::Config(format!("Invalid integer value: {}", value))
                })?;
                self.storage.max_key_size = v;
                Ok(true)
            }
            ConfigKey::StorageMaxValueSize => {
                let v = value.parse::<usize>().map_err(|_| {
                    FerriteError::Config(format!("Invalid integer value: {}", value))
                })?;
                self.storage.max_value_size = v;
                Ok(true)
            }
            ConfigKey::PersistenceAofEnabled => {
                let enabled = value.parse::<bool>().map_err(|_| {
                    FerriteError::Config(format!("Invalid boolean value: {}", value))
                })?;
                self.persistence.aof_enabled = enabled;
                Ok(true)
            }
            ConfigKey::PersistenceCheckpointEnabled => {
                let enabled = value.parse::<bool>().map_err(|_| {
                    FerriteError::Config(format!("Invalid boolean value: {}", value))
                })?;
                self.persistence.checkpoint_enabled = enabled;
                Ok(true)
            }
            ConfigKey::LoggingFormat => {
                let fmt = match value.to_lowercase().as_str() {
                    "json" => LogFormat::Json,
                    "pretty" | "text" => LogFormat::Pretty,
                    _ => {
                        return Err(FerriteError::Config(format!(
                            "Invalid logging format: {}. Expected: json, pretty",
                            value
                        )));
                    }
                };
                self.logging.format = fmt;
                Ok(true)
            }
            ConfigKey::MetricsBind => {
                self.metrics.bind = value.to_string();
                Ok(true)
            }
            ConfigKey::MetricsPort => {
                let port = value.parse::<u16>().map_err(|_| {
                    FerriteError::Config(format!("Invalid port value: {}", value))
                })?;
                self.metrics.port = port;
                Ok(true)
            }
            ConfigKey::EncryptionAlgorithm => {
                let algo = match value.to_lowercase().as_str() {
                    "aes-256-gcm" | "aes256gcm" => EncryptionAlgorithm::Aes256Gcm,
                    "chacha20-poly1305" | "chacha20poly1305" => EncryptionAlgorithm::ChaCha20Poly1305,
                    _ => {
                        return Err(FerriteError::Config(format!(
                            "Invalid encryption algorithm: {}. Expected: aes-256-gcm, chacha20-poly1305",
                            value
                        )));
                    }
                };
                self.encryption.algorithm = algo;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Check if a parameter requires restart to take effect
    pub fn requires_restart(path: &str) -> bool {
        ConfigKey::parse_str(path)
            .map(|key| key.requires_restart())
            .unwrap_or(false)
    }
}

/// Thread-safe shared configuration wrapper
///
/// Provides concurrent read access and exclusive write access to configuration.
/// Used for hot config reload functionality.
#[derive(Clone)]
pub struct SharedConfig {
    inner: Arc<RwLock<Config>>,
    /// Path to the config file (for REWRITE command)
    config_path: Arc<RwLock<Option<PathBuf>>>,
}

impl SharedConfig {
    /// Create a new SharedConfig from a Config
    pub fn new(config: Config) -> Self {
        Self {
            inner: Arc::new(RwLock::new(config)),
            config_path: Arc::new(RwLock::new(None)),
        }
    }

    /// Create a new SharedConfig from a Config with associated file path
    pub fn with_path(config: Config, path: PathBuf) -> Self {
        Self {
            inner: Arc::new(RwLock::new(config)),
            config_path: Arc::new(RwLock::new(Some(path))),
        }
    }

    /// Get read access to the configuration
    pub fn read(&self) -> RwLockReadGuard<'_, Config> {
        self.inner.read()
    }

    /// Get write access to the configuration
    pub fn write(&self) -> RwLockWriteGuard<'_, Config> {
        self.inner.write()
    }

    /// Set the config file path
    pub fn set_path(&self, path: PathBuf) {
        *self.config_path.write() = Some(path);
    }

    /// Get the config file path
    pub fn get_path(&self) -> Option<PathBuf> {
        self.config_path.read().clone()
    }

    /// Reload configuration from the associated file
    pub fn reload_from_file(&self) -> Result<()> {
        let path = self.config_path.read().clone().ok_or_else(|| {
            FerriteError::Config("No config file path set for reload".to_string())
        })?;

        let new_config = Config::from_file(&path)?;
        new_config.validate()?;

        *self.inner.write() = new_config;
        Ok(())
    }

    /// Write current configuration to the associated file
    pub fn rewrite(&self) -> Result<()> {
        let path = self.config_path.read().clone().ok_or_else(|| {
            FerriteError::Config("No config file path set for rewrite".to_string())
        })?;

        self.inner.read().to_file(&path)
    }

    /// Get a config parameter by path
    pub fn get_param(&self, path: &str) -> Option<String> {
        self.inner.read().get_param(path)
    }

    /// Set a config parameter by path (hot reload)
    pub fn set_param(&self, path: &str, value: &str) -> Result<bool> {
        self.inner.write().set_param(path, value)
    }

    /// List all known config parameters with their current values
    pub fn list_params(&self, pattern: &str) -> Vec<(String, String)> {
        let mut results = Vec::new();

        // Convert pattern to simple glob matching
        let matches = |name: &str| -> bool {
            if pattern == "*" {
                return true;
            }
            if let Some(prefix) = pattern.strip_suffix('*') {
                return name.starts_with(prefix);
            }
            name == pattern
        };
        for key in ConfigKey::all() {
            let name = key.as_str();
            if !matches(name) {
                continue;
            }
            if let Some(value) = self.get_param(name) {
                results.push((name.to_string(), value));
            }
        }

        results
    }
}

impl std::fmt::Debug for SharedConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedConfig")
            .field("config", &*self.inner.read())
            .field("config_path", &*self.config_path.read())
            .finish()
    }
}

/// Server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// Bind address
    pub bind: String,

    /// Port to listen on
    pub port: u16,

    /// Maximum number of concurrent connections
    pub max_connections: usize,

    /// TCP keepalive interval in seconds (0 to disable)
    pub tcp_keepalive: u64,

    /// Connection timeout in seconds
    pub timeout: u64,

    /// ACL file path for persistence (None to disable)
    pub acl_file: Option<PathBuf>,

    /// Maximum bulk string size in bytes (0 = default 512MB, matches Redis proto-max-bulk-len)
    pub proto_max_bulk_len: usize,

    /// Maximum number of elements in arrays/maps/sets (0 = default 1,048,576)
    pub proto_max_multi_bulk_len: usize,

    /// Maximum nesting depth for RESP frames (0 = default 64)
    pub proto_max_nesting_depth: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".to_string(),
            port: 6379,
            max_connections: 10000,
            tcp_keepalive: 300,
            timeout: 0, // 0 means no timeout
            acl_file: Some(PathBuf::from("./data/users.acl")),
            proto_max_bulk_len: 512 * 1024 * 1024,  // 512MB (Redis default)
            proto_max_multi_bulk_len: 1_048_576,     // 1M elements
            proto_max_nesting_depth: 64,
        }
    }
}

impl ServerConfig {
    /// Get the full bind address
    pub fn address(&self) -> String {
        format!("{}:{}", self.bind, self.port)
    }

    /// Build protocol parser limits from this config
    pub fn parser_limits(&self) -> crate::protocol::ParserLimits {
        crate::protocol::ParserLimits {
            max_bulk_string_size: self.proto_max_bulk_len,
            max_array_elements: self.proto_max_multi_bulk_len,
            max_nesting_depth: self.proto_max_nesting_depth,
        }
    }
}

/// Storage backend type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackendType {
    /// In-memory storage using DashMap (default, fastest)
    #[default]
    Memory,
    /// HybridLog storage with tiered memory/disk access
    HybridLog,
}

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// Storage backend type
    pub backend: StorageBackendType,

    /// Maximum memory to use in bytes
    pub max_memory: usize,

    /// Number of databases (0-15)
    pub databases: u8,

    /// Data directory for persistence
    pub data_dir: PathBuf,

    /// Maximum key size in bytes
    pub max_key_size: usize,

    /// Maximum value size in bytes
    pub max_value_size: usize,

    /// HybridLog mutable region size (bytes)
    pub hybridlog_mutable_size: usize,

    /// HybridLog read-only region size (bytes)
    pub hybridlog_readonly_size: usize,

    /// HybridLog auto-tiering enabled
    pub hybridlog_auto_tiering: bool,

    /// HybridLog migration threshold (0.0-1.0)
    pub hybridlog_migration_threshold: f64,

    /// Enable prefetching for disk reads
    pub prefetch_enabled: bool,

    /// Prefetch buffer size in bytes (default 64KB)
    pub prefetch_buffer_size: usize,

    /// Number of entries to read ahead (default 16)
    pub prefetch_read_ahead_entries: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: StorageBackendType::Memory,
            max_memory: 1024 * 1024 * 1024, // 1GB
            databases: 16,
            data_dir: PathBuf::from("./data"),
            max_key_size: 512 * 1024 * 1024, // 512MB (Redis default)
            max_value_size: 512 * 1024 * 1024, // 512MB (Redis default)
            hybridlog_mutable_size: 64 * 1024 * 1024, // 64MB
            hybridlog_readonly_size: 256 * 1024 * 1024, // 256MB
            hybridlog_auto_tiering: true,
            hybridlog_migration_threshold: 0.8,
            prefetch_enabled: true,
            prefetch_buffer_size: 64 * 1024, // 64KB
            prefetch_read_ahead_entries: 16,
        }
    }
}

/// Persistence configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PersistenceConfig {
    /// Enable AOF persistence
    pub aof_enabled: bool,

    /// AOF file path
    pub aof_path: PathBuf,

    /// AOF sync policy
    pub aof_sync: SyncPolicy,

    /// Enable checkpointing
    pub checkpoint_enabled: bool,

    /// Checkpoint interval in seconds
    #[serde(with = "humantime_serde")]
    pub checkpoint_interval: Duration,

    /// Checkpoint directory
    pub checkpoint_dir: PathBuf,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            aof_enabled: true,
            aof_path: PathBuf::from("./data/appendonly.aof"),
            aof_sync: SyncPolicy::EverySecond,
            checkpoint_enabled: true,
            checkpoint_interval: Duration::from_secs(300), // 5 minutes
            checkpoint_dir: PathBuf::from("./data/checkpoints"),
        }
    }
}

/// AOF sync policy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SyncPolicy {
    /// fsync after every write
    Always,

    /// fsync once per second (accepts "everysecond" or "everysec")
    #[default]
    #[serde(alias = "everysec")]
    EverySecond,

    /// Let OS decide when to sync
    No,
}

/// Metrics configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    /// Enable metrics
    pub enabled: bool,

    /// Metrics HTTP endpoint port
    pub port: u16,

    /// Metrics HTTP endpoint bind address
    pub bind: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 9090,
            bind: "127.0.0.1".to_string(),
        }
    }
}

impl MetricsConfig {
    /// Get the full metrics endpoint address
    pub fn address(&self) -> String {
        format!("{}:{}", self.bind, self.port)
    }
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    /// Log level (trace, debug, info, warn, error)
    pub level: String,

    /// Log format (pretty, json)
    pub format: LogFormat,

    /// Log file path (None for stdout)
    pub file: Option<PathBuf>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: LogFormat::Pretty,
            file: None,
        }
    }
}

/// Log output format
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Human-readable format
    #[default]
    Pretty,

    /// JSON format
    Json,
}

/// TLS configuration settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TlsConfigSettings {
    /// Enable TLS
    pub enabled: bool,

    /// TLS port (separate from main port for dual-port support)
    pub port: u16,

    /// Path to certificate file (PEM format)
    pub cert_file: Option<PathBuf>,

    /// Path to private key file (PEM format)
    pub key_file: Option<PathBuf>,

    /// Path to CA certificate for client verification (mTLS)
    pub ca_file: Option<PathBuf>,

    /// Whether client certificates are required (mTLS)
    pub require_client_cert: bool,
}

impl Default for TlsConfigSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            port: 6380, // TLS port one above default Redis port
            cert_file: None,
            key_file: None,
            ca_file: None,
            require_client_cert: false,
        }
    }
}

impl TlsConfigSettings {
    /// Validate TLS configuration
    pub fn validate(&self) -> Result<()> {
        if self.enabled {
            if self.cert_file.is_none() {
                return Err(FerriteError::Config(
                    "TLS is enabled but no certificate file specified".to_string(),
                ));
            }
            if self.key_file.is_none() {
                return Err(FerriteError::Config(
                    "TLS is enabled but no key file specified".to_string(),
                ));
            }
            if self.require_client_cert && self.ca_file.is_none() {
                return Err(FerriteError::Config(
                    "Client certificate required but no CA file specified".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Convert to TlsConfig for the TLS listener
    pub fn to_tls_config(&self) -> Option<TlsConfig> {
        if !self.enabled {
            return None;
        }

        let cert_file = self.cert_file.as_ref()?.to_string_lossy().to_string();
        let key_file = self.key_file.as_ref()?.to_string_lossy().to_string();

        let mut config = TlsConfig::new(cert_file, key_file);

        if let Some(ca_file) = &self.ca_file {
            config = config.with_client_auth(
                ca_file.to_string_lossy().to_string(),
                self.require_client_cert,
            );
        }

        Some(config)
    }

    /// Get the TLS address
    pub fn address(&self, bind: &str) -> String {
        format!("{}:{}", bind, self.port)
    }
}

/// Cloud tiering configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CloudTieringConfig {
    /// Enable cloud tiering
    pub enabled: bool,

    /// Cloud provider (s3, gcs, azure, local)
    pub provider: CloudProvider,

    /// Bucket/container name
    pub bucket: String,

    /// Key prefix for all objects
    #[serde(default)]
    pub prefix: Option<String>,

    /// Region (for S3 and Azure)
    #[serde(default)]
    pub region: Option<String>,

    /// Custom endpoint URL (for S3-compatible stores)
    #[serde(default)]
    pub endpoint: Option<String>,

    /// Access key ID (for S3/Azure)
    #[serde(default)]
    pub access_key_id: Option<String>,

    /// Secret access key (for S3/Azure)
    #[serde(default)]
    pub secret_access_key: Option<String>,

    /// Enable compression for uploads
    #[serde(default = "default_compression_enabled")]
    pub compression_enabled: bool,

    /// Compression level (1-9, default 6)
    #[serde(default = "default_compression_level")]
    pub compression_level: u32,

    /// Minimum age (in seconds) before data can be tiered to cloud
    #[serde(default = "default_min_age_seconds")]
    pub min_age_seconds: u64,

    /// Size threshold (in bytes) - only tier items larger than this
    #[serde(default = "default_size_threshold")]
    pub size_threshold: usize,

    /// Maximum cloud storage usage (in bytes, 0 = unlimited)
    #[serde(default)]
    pub max_cloud_size: u64,
}

fn default_compression_enabled() -> bool {
    true
}

fn default_compression_level() -> u32 {
    6
}

fn default_min_age_seconds() -> u64 {
    3600 // 1 hour
}

fn default_size_threshold() -> usize {
    1024 // 1KB minimum
}

impl Default for CloudTieringConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: CloudProvider::Local,
            bucket: "ferrite-data".to_string(),
            prefix: Some("cold/".to_string()),
            region: None,
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            compression_enabled: default_compression_enabled(),
            compression_level: default_compression_level(),
            min_age_seconds: default_min_age_seconds(),
            size_threshold: default_size_threshold(),
            max_cloud_size: 0,
        }
    }
}

/// Cloud storage provider
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum CloudProvider {
    /// Amazon S3
    S3,
    /// Google Cloud Storage
    Gcs,
    /// Azure Blob Storage
    Azure,
    /// Local filesystem (for testing/development)
    #[default]
    Local,
}

/// Cluster configuration settings
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterConfigSettings {
    /// Enable cluster mode
    pub enabled: bool,

    /// This node's cluster bus address (if different from server address)
    #[serde(default)]
    pub node_addr: Option<String>,

    /// Cluster bus port offset (default: 10000)
    pub bus_port_offset: u16,

    /// Cluster node timeout in milliseconds
    pub node_timeout: u64,

    /// Number of replicas per primary
    pub replica_count: u8,

    /// Whether to enable automatic failover
    pub failover_enabled: bool,

    /// Minimum number of primaries for cluster to be operational
    pub min_primaries: usize,

    /// Whether to require full slot coverage for cluster to be up
    pub require_full_coverage: bool,

    /// Known cluster nodes (for joining an existing cluster)
    #[serde(default)]
    pub known_nodes: Vec<String>,
}

impl Default for ClusterConfigSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            node_addr: None,
            bus_port_offset: 10000,
            node_timeout: 15000,
            replica_count: 1,
            failover_enabled: true,
            min_primaries: 1,
            require_full_coverage: true,
            known_nodes: Vec::new(),
        }
    }
}

impl ClusterConfigSettings {
    /// Get the cluster bus port based on the server port
    pub fn bus_port(&self, server_port: u16) -> u16 {
        server_port + self.bus_port_offset
    }

    /// Convert to cluster::ClusterConfig
    pub fn to_cluster_config(&self) -> crate::cluster::ClusterConfig {
        crate::cluster::ClusterConfig {
            enabled: self.enabled,
            node_addr: self.node_addr.as_ref().and_then(|s| s.parse().ok()),
            bus_port_offset: self.bus_port_offset,
            node_timeout: self.node_timeout,
            replica_count: self.replica_count,
            failover_enabled: self.failover_enabled,
            min_primaries: self.min_primaries,
            require_full_coverage: self.require_full_coverage,
        }
    }
}

/// Replication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ReplicationConfig {
    /// Primary server address for replication (empty = this is a primary)
    /// Format: "host:port"
    pub replicaof: Option<String>,

    /// Whether this replica is read-only
    pub replica_read_only: bool,

    /// Replication backlog size in bytes
    pub backlog_size: Option<u64>,

    /// Replication timeout in seconds
    pub repl_timeout: u64,

    /// Reconnection delay in seconds
    pub reconnect_delay: u64,

    /// Maximum reconnection attempts before giving up (0 = unlimited)
    pub max_reconnect_attempts: u32,

    /// Base delay for exponential backoff in milliseconds
    pub backoff_base_ms: u64,

    /// Maximum delay cap for exponential backoff in milliseconds
    pub backoff_max_ms: u64,

    /// Whether to persist the replication offset across restarts
    pub offset_persistence: bool,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            replicaof: None,
            replica_read_only: true,
            backlog_size: Some(DEFAULT_REPLICATION_BACKLOG_BYTES),
            repl_timeout: 60,
            reconnect_delay: 5,
            max_reconnect_attempts: 0,
            backoff_base_ms: 1000,
            backoff_max_ms: 30_000,
            offset_persistence: true,
        }
    }
}

/// Audit logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AuditConfig {
    /// Enable audit logging
    pub enabled: bool,

    /// Path to audit log file (None = stdout)
    pub log_file: Option<PathBuf>,

    /// Audit log format
    pub format: AuditFormat,

    /// Commands to audit (empty = all commands)
    pub log_commands: Vec<String>,

    /// Commands to exclude from audit
    pub exclude_commands: Vec<String>,

    /// Log successful commands
    pub log_success: bool,

    /// Log failed commands
    pub log_failures: bool,

    /// Log authentication attempts
    pub log_auth: bool,

    /// Log admin commands (CONFIG, FLUSHDB, etc.)
    pub log_admin: bool,

    /// Maximum audit log file size in bytes before rotation (0 = no rotation)
    pub max_file_size: u64,

    /// Number of rotated log files to keep
    pub max_files: usize,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            log_file: None,
            format: AuditFormat::default(),
            log_commands: vec![],
            exclude_commands: vec![],
            log_success: true,
            log_failures: true,
            log_auth: true,
            log_admin: true,
            max_file_size: 100 * 1024 * 1024, // 100MB
            max_files: 10,
        }
    }
}

/// Audit log output format
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum AuditFormat {
    /// JSON format (structured)
    #[default]
    Json,
    /// Plain text format
    Text,
}

/// Encryption at rest configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EncryptionConfig {
    /// Enable encryption at rest
    pub enabled: bool,

    /// Path to encryption key file (32 bytes, base64 or raw)
    pub key_file: Option<PathBuf>,

    /// Encryption algorithm
    pub algorithm: EncryptionAlgorithm,

    /// Encrypt AOF entries
    pub encrypt_aof: bool,

    /// Encrypt RDB snapshots
    pub encrypt_rdb: bool,

    /// Encrypt checkpoints
    pub encrypt_checkpoints: bool,
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            key_file: None,
            algorithm: EncryptionAlgorithm::default(),
            encrypt_aof: true,
            encrypt_rdb: true,
            encrypt_checkpoints: true,
        }
    }
}

impl EncryptionConfig {
    /// Validate encryption configuration
    pub fn validate(&self) -> Result<()> {
        if self.enabled && self.key_file.is_none() {
            return Err(FerriteError::Config(
                "Encryption is enabled but no key file specified".to_string(),
            ));
        }
        Ok(())
    }
}

/// Encryption algorithm
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum EncryptionAlgorithm {
    /// ChaCha20-Poly1305 (default, recommended for most use cases)
    #[default]
    ChaCha20Poly1305,
    /// AES-256-GCM (hardware-accelerated on x86)
    Aes256Gcm,
}

/// OpenTelemetry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OpenTelemetryConfig {
    /// Enable OpenTelemetry integration
    pub enabled: bool,

    /// OTLP endpoint URL (e.g., "http://localhost:4317")
    pub endpoint: String,

    /// Service name for tracing
    pub service_name: String,

    /// Enable distributed tracing
    pub traces_enabled: bool,

    /// Enable metrics export via OTLP
    pub metrics_enabled: bool,

    /// Batch span processor max queue size
    pub batch_max_queue_size: usize,

    /// Batch span processor max export batch size
    pub batch_max_export_batch_size: usize,
}

impl Default for OpenTelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: "http://localhost:4317".to_string(),
            service_name: "ferrite".to_string(),
            traces_enabled: true,
            metrics_enabled: true,
            batch_max_queue_size: 2048,
            batch_max_export_batch_size: 512,
        }
    }
}

/// Helper module for Duration serialization
mod humantime_serde {
    use serde::{self, Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(duration.as_secs())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(secs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.server.port, 6379);
        assert_eq!(config.server.bind, "127.0.0.1");
        assert_eq!(config.storage.databases, 16);
        assert!(config.persistence.aof_enabled);
        assert!(config.metrics.enabled);
    }

    #[test]
    fn test_config_from_str() {
        let toml = r#"
[server]
port = 6380
bind = "0.0.0.0"
max_connections = 5000

[storage]
max_memory = 2147483648
databases = 8

[persistence]
aof_enabled = false
aof_sync = "always"

[metrics]
enabled = false
"#;

        let config = Config::parse_str(toml).unwrap();
        assert_eq!(config.server.port, 6380);
        assert_eq!(config.server.bind, "0.0.0.0");
        assert_eq!(config.server.max_connections, 5000);
        assert_eq!(config.storage.max_memory, 2147483648);
        assert_eq!(config.storage.databases, 8);
        assert!(!config.persistence.aof_enabled);
        assert_eq!(config.persistence.aof_sync, SyncPolicy::Always);
        assert!(!config.metrics.enabled);
    }

    #[test]
    fn test_config_validation() {
        let mut config = Config::default();
        assert!(config.validate().is_ok());

        config.server.port = 0;
        assert!(config.validate().is_err());

        config.server.port = 6379;
        config.storage.databases = 17;
        assert!(config.validate().is_err());

        config.storage.databases = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_server_address() {
        let config = ServerConfig::default();
        assert_eq!(config.address(), "127.0.0.1:6379");
    }

    #[test]
    fn test_metrics_address() {
        let config = MetricsConfig::default();
        assert_eq!(config.address(), "127.0.0.1:9090");
    }

    #[test]
    fn test_sync_policy_serde() {
        let toml = r#"aof_sync = "always""#;
        #[derive(Deserialize)]
        struct Test {
            aof_sync: SyncPolicy,
        }
        let test: Test = toml::from_str(toml).unwrap();
        assert_eq!(test.aof_sync, SyncPolicy::Always);

        let toml = r#"aof_sync = "everysecond""#;
        let test: Test = toml::from_str(toml).unwrap();
        assert_eq!(test.aof_sync, SyncPolicy::EverySecond);

        let toml = r#"aof_sync = "no""#;
        let test: Test = toml::from_str(toml).unwrap();
        assert_eq!(test.aof_sync, SyncPolicy::No);

        // "everysec" is accepted as an alias for "everysecond"
        let toml = r#"aof_sync = "everysec""#;
        let test: Test = toml::from_str(toml).unwrap();
        assert_eq!(test.aof_sync, SyncPolicy::EverySecond);
    }

    #[test]
    fn test_partial_config() {
        // Test that missing sections use defaults
        let toml = r#"
[server]
port = 6380
"#;

        let config = Config::parse_str(toml).unwrap();
        assert_eq!(config.server.port, 6380);
        // Other sections should have defaults
        assert_eq!(config.storage.databases, 16);
        assert!(config.persistence.aof_enabled);
    }

    #[test]
    fn test_tls_config_default() {
        let config = TlsConfigSettings::default();
        assert!(!config.enabled);
        assert_eq!(config.port, 6380);
        assert!(config.cert_file.is_none());
        assert!(config.key_file.is_none());
        assert!(config.ca_file.is_none());
        assert!(!config.require_client_cert);
    }

    #[test]
    fn test_tls_config_from_str() {
        let toml = r#"
[tls]
enabled = true
port = 6381
cert_file = "/path/to/cert.pem"
key_file = "/path/to/key.pem"
"#;

        let config = Config::parse_str(toml).unwrap();
        assert!(config.tls.enabled);
        assert_eq!(config.tls.port, 6381);
        assert_eq!(
            config.tls.cert_file,
            Some(PathBuf::from("/path/to/cert.pem"))
        );
        assert_eq!(config.tls.key_file, Some(PathBuf::from("/path/to/key.pem")));
    }

    #[test]
    fn test_tls_config_mtls() {
        let toml = r#"
[tls]
enabled = true
cert_file = "/path/to/cert.pem"
key_file = "/path/to/key.pem"
ca_file = "/path/to/ca.pem"
require_client_cert = true
"#;

        let config = Config::parse_str(toml).unwrap();
        assert!(config.tls.enabled);
        assert!(config.tls.require_client_cert);
        assert_eq!(config.tls.ca_file, Some(PathBuf::from("/path/to/ca.pem")));
    }

    #[test]
    fn test_tls_validation() {
        // Disabled TLS should pass validation
        let mut config = TlsConfigSettings::default();
        assert!(config.validate().is_ok());

        // Enabled TLS without cert should fail
        config.enabled = true;
        assert!(config.validate().is_err());

        // With cert but no key should fail
        config.cert_file = Some(PathBuf::from("cert.pem"));
        assert!(config.validate().is_err());

        // With cert and key should pass
        config.key_file = Some(PathBuf::from("key.pem"));
        assert!(config.validate().is_ok());

        // Require client cert without CA should fail
        config.require_client_cert = true;
        assert!(config.validate().is_err());

        // With CA file should pass
        config.ca_file = Some(PathBuf::from("ca.pem"));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_tls_address() {
        let config = TlsConfigSettings::default();
        assert_eq!(config.address("127.0.0.1"), "127.0.0.1:6380");
    }

    #[test]
    fn test_shared_config_basic() {
        let config = Config::default();
        let shared = SharedConfig::new(config);

        // Test read access
        assert_eq!(shared.read().server.port, 6379);
        assert_eq!(shared.read().storage.databases, 16);
    }

    #[test]
    fn test_shared_config_write() {
        let config = Config::default();
        let shared = SharedConfig::new(config);

        // Modify config
        shared.write().server.port = 6380;

        // Verify change persists
        assert_eq!(shared.read().server.port, 6380);
    }

    #[test]
    fn test_shared_config_get_param() {
        let config = Config::default();
        let shared = SharedConfig::new(config);

        assert_eq!(shared.get_param("server.port"), Some("6379".to_string()));
        assert_eq!(
            shared.get_param("server.bind"),
            Some("127.0.0.1".to_string())
        );
        assert_eq!(shared.get_param("logging.level"), Some("info".to_string()));
        assert!(shared.get_param("nonexistent.param").is_none());
    }

    #[test]
    fn test_shared_config_set_param() {
        let config = Config::default();
        let shared = SharedConfig::new(config);

        // Hot-reloadable parameter
        assert!(shared.set_param("logging.level", "debug").unwrap());
        assert_eq!(shared.get_param("logging.level"), Some("debug".to_string()));

        // Another hot-reloadable parameter
        assert!(shared.set_param("persistence.aof_sync", "always").unwrap());
        assert_eq!(shared.read().persistence.aof_sync, SyncPolicy::Always);

        // Restart-required parameter
        assert!(!shared.set_param("server.port", "6380").unwrap());
    }

    #[test]
    fn test_shared_config_list_params() {
        let config = Config::default();
        let shared = SharedConfig::new(config);

        // List all server params
        let params = shared.list_params("server.*");
        assert!(params.iter().any(|(k, _)| k == "server.port"));
        assert!(params.iter().any(|(k, _)| k == "server.bind"));

        // List specific param
        let params = shared.list_params("server.port");
        assert_eq!(params.len(), 1);
        assert_eq!(params[0], ("server.port".to_string(), "6379".to_string()));

        // List all params
        let params = shared.list_params("*");
        assert!(params.len() > 10);
    }

    #[test]
    fn test_config_to_file() {
        use tempfile::tempdir;

        let config = Config::default();
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.toml");

        // Write config to file
        config.to_file(&path).unwrap();
        assert!(path.exists());

        // Load it back
        let loaded = Config::from_file(&path.to_path_buf()).unwrap();
        assert_eq!(loaded.server.port, config.server.port);
        assert_eq!(loaded.server.bind, config.server.bind);
        assert_eq!(loaded.storage.databases, config.storage.databases);
    }

    #[test]
    fn test_shared_config_rewrite() {
        use tempfile::tempdir;

        let mut config = Config::default();
        config.server.port = 6380;

        let dir = tempdir().unwrap();
        let path = dir.path().join("config.toml");

        let shared = SharedConfig::with_path(config, path.clone());

        // Modify and rewrite
        shared.write().logging.level = "debug".to_string();
        shared.rewrite().unwrap();

        // Load and verify
        let loaded = Config::from_file(&path).unwrap();
        assert_eq!(loaded.server.port, 6380);
        assert_eq!(loaded.logging.level, "debug");
    }

    #[test]
    fn test_config_requires_restart() {
        assert!(Config::requires_restart("server.port"));
        assert!(Config::requires_restart("server.bind"));
        assert!(Config::requires_restart("storage.backend"));
        assert!(Config::requires_restart("tls.enabled"));
        assert!(Config::requires_restart("encryption.enabled"));

        // Hot-reloadable
        assert!(!Config::requires_restart("logging.level"));
        assert!(!Config::requires_restart("persistence.aof_sync"));
        assert!(!Config::requires_restart("metrics.enabled"));
    }
}
