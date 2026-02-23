//! Runtime Configuration Hot-Reload Manager
//!
//! Provides dynamic configuration management with hot-reload support,
//! validation, change tracking, and subscription notifications.
//!
//! # Architecture
//!
//! ```text
//! CONFIG SET key value
//!     │
//!     ▼
//! ┌──────────────────────────────────────┐
//! │          ConfigManager               │
//! ├──────────────────────────────────────┤
//! │ parameters: PARAMETER_REGISTRY       │
//! │ values: DashMap<String, ConfigValue>  │
//! │ history: RwLock<Vec<ConfigChange>>    │
//! │ subscriptions: DashMap<String, ...>   │
//! └──────────────────────────────────────┘
//!     │
//!     ▼ (on hot-reloadable change)
//! Validate → Apply → Record → Notify
//! ```

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors returned by configuration operations
#[derive(Error, Debug)]
pub enum ConfigError {
    /// The parameter key is not recognized
    #[error("Unknown parameter: {0}")]
    UnknownParameter(String),

    /// The parameter cannot be changed at runtime
    #[error("Parameter is not hot-reloadable: {0}")]
    NotHotReloadable(String),

    /// The supplied value failed validation
    #[error("Validation failed for '{key}': {reason}")]
    ValidationFailed {
        /// Parameter key
        key: String,
        /// Human-readable reason
        reason: String,
    },

    /// The supplied value type does not match the parameter definition
    #[error("Type mismatch for '{key}': expected {expected}, got {got}")]
    TypeMismatch {
        /// Parameter key
        key: String,
        /// Expected type name
        expected: String,
        /// Actual type name
        got: String,
    },

    /// A numeric value is outside the allowed range
    #[error("Value for '{key}' out of range [{min}, {max}], got {got}")]
    OutOfRange {
        /// Parameter key
        key: String,
        /// Minimum allowed
        min: f64,
        /// Maximum allowed
        max: f64,
        /// Supplied value
        got: f64,
    },

    /// Serialization to TOML failed
    #[error("Rewrite failed: {0}")]
    RewriteFailed(String),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Value types
// ---------------------------------------------------------------------------

/// Runtime configuration value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConfigValue {
    /// String value
    String(String),
    /// Signed 64-bit integer
    Int(i64),
    /// 64-bit floating point
    Float(f64),
    /// Boolean flag
    Bool(bool),
    /// Duration in milliseconds
    Duration(u64),
    /// Size in bytes
    Size(u64),
    /// List of strings
    List(Vec<String>),
}

impl fmt::Display for ConfigValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigValue::String(s) => write!(f, "{}", s),
            ConfigValue::Int(v) => write!(f, "{}", v),
            ConfigValue::Float(v) => write!(f, "{}", v),
            ConfigValue::Bool(v) => write!(f, "{}", v),
            ConfigValue::Duration(ms) => write!(f, "{}ms", ms),
            ConfigValue::Size(b) => write!(f, "{}B", b),
            ConfigValue::List(l) => write!(f, "{}", l.join(" ")),
        }
    }
}

impl ConfigValue {
    /// Return the human-readable type name
    fn type_name(&self) -> &'static str {
        match self {
            ConfigValue::String(_) => "String",
            ConfigValue::Int(_) => "Integer",
            ConfigValue::Float(_) => "Float",
            ConfigValue::Bool(_) => "Boolean",
            ConfigValue::Duration(_) => "Duration",
            ConfigValue::Size(_) => "Size",
            ConfigValue::List(_) => "StringList",
        }
    }
}

/// Expected value type for a configuration parameter
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ValueType {
    /// UTF-8 string
    String,
    /// Signed 64-bit integer
    Integer,
    /// 64-bit float
    Float,
    /// Boolean
    Boolean,
    /// Duration (stored as milliseconds)
    Duration,
    /// Size (stored as bytes)
    Size,
    /// List of strings
    StringList,
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ValueType::String => "String",
            ValueType::Integer => "Integer",
            ValueType::Float => "Float",
            ValueType::Boolean => "Boolean",
            ValueType::Duration => "Duration",
            ValueType::Size => "Size",
            ValueType::StringList => "StringList",
        };
        write!(f, "{}", s)
    }
}

// ---------------------------------------------------------------------------
// Validator
// ---------------------------------------------------------------------------

/// Validator attached to a configuration parameter
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ValidatorType {
    /// Numeric range (inclusive)
    Range {
        /// Minimum allowed value
        min: f64,
        /// Maximum allowed value
        max: f64,
    },
    /// Enumerated set of allowed string values
    Enum {
        /// Allowed values
        values: Vec<String>,
    },
    /// Regex pattern the value must match
    Pattern {
        /// Regular expression
        regex: String,
    },
    /// Named custom validator (handled externally)
    Custom(String),
}

// ---------------------------------------------------------------------------
// Parameter definition
// ---------------------------------------------------------------------------

/// Definition of a single configuration parameter
#[derive(Debug, Clone)]
pub struct ConfigParameter {
    /// Dotted key name (e.g. `maxmemory`)
    pub key: String,
    /// Human-readable description
    pub description: String,
    /// Default value
    pub default_value: ConfigValue,
    /// Expected value type
    pub value_type: ValueType,
    /// Whether the parameter can be changed at runtime
    pub hot_reloadable: bool,
    /// Optional minimum (numeric types)
    pub min_value: Option<f64>,
    /// Optional maximum (numeric types)
    pub max_value: Option<f64>,
    /// Optional validator
    pub validator: Option<ValidatorType>,
}

// ---------------------------------------------------------------------------
// Change record
// ---------------------------------------------------------------------------

/// Record of a single configuration change
#[derive(Debug, Clone)]
pub struct ConfigChange {
    /// Parameter key
    pub key: String,
    /// Previous value (`None` for first-time sets)
    pub old_value: Option<ConfigValue>,
    /// New value
    pub new_value: ConfigValue,
    /// When the change was applied
    pub changed_at: DateTime<Utc>,
    /// Who/what initiated the change
    pub changed_by: String,
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

/// Point-in-time snapshot of all configuration values
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigSnapshot {
    /// Current values keyed by parameter name
    pub values: HashMap<String, ConfigValue>,
    /// Monotonically increasing version counter
    pub version: u64,
    /// Timestamp of the last change
    pub updated_at: DateTime<Utc>,
}

impl ConfigSnapshot {
    /// Create an empty snapshot
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
            version: 0,
            updated_at: Utc::now(),
        }
    }
}

impl Default for ConfigSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Subscription ID
// ---------------------------------------------------------------------------

/// Opaque subscription identifier returned by [`ConfigManager::subscribe`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConfigSubscriptionId(pub u64);

impl ConfigSubscriptionId {
    fn next() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        Self(NEXT_ID.fetch_add(1, Ordering::SeqCst))
    }
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Operational statistics for the configuration manager
#[derive(Debug, Clone, Default)]
pub struct ConfigStats {
    /// Total number of CONFIG SET operations
    pub total_sets: u64,
    /// Total number of CONFIG RESETSTAT / reset-to-default operations
    pub total_resets: u64,
    /// Total number of CONFIG REWRITE operations
    pub total_rewrites: u64,
    /// Number of entries in the change history
    pub changes_history_size: usize,
    /// Number of active subscriptions
    pub subscriptions_active: usize,
    /// Number of parameters that support hot-reload
    pub hot_reloadable_params: usize,
    /// Number of parameters that require restart
    pub cold_params: usize,
}

// ---------------------------------------------------------------------------
// Parameter registry (lazy-built)
// ---------------------------------------------------------------------------

/// Build the full set of known configuration parameters.
fn build_parameter_registry() -> Vec<ConfigParameter> {
    vec![
        // -- Memory --
        ConfigParameter {
            key: "maxmemory".into(),
            description: "Maximum memory usage in bytes (0 = unlimited)".into(),
            default_value: ConfigValue::Size(0),
            value_type: ValueType::Size,
            hot_reloadable: true,
            min_value: Some(0.0),
            max_value: None,
            validator: None,
        },
        ConfigParameter {
            key: "maxmemory-policy".into(),
            description: "Eviction policy when maxmemory is reached".into(),
            default_value: ConfigValue::String("noeviction".into()),
            value_type: ValueType::String,
            hot_reloadable: true,
            min_value: None,
            max_value: None,
            validator: Some(ValidatorType::Enum {
                values: vec![
                    "noeviction".into(),
                    "allkeys-lru".into(),
                    "volatile-lru".into(),
                    "allkeys-lfu".into(),
                    "volatile-lfu".into(),
                    "allkeys-random".into(),
                    "volatile-random".into(),
                    "volatile-ttl".into(),
                ],
            }),
        },
        // -- Server --
        ConfigParameter {
            key: "hz".into(),
            description: "Server event-loop frequency in Hz".into(),
            default_value: ConfigValue::Int(10),
            value_type: ValueType::Integer,
            hot_reloadable: true,
            min_value: Some(1.0),
            max_value: Some(500.0),
            validator: Some(ValidatorType::Range {
                min: 1.0,
                max: 500.0,
            }),
        },
        ConfigParameter {
            key: "timeout".into(),
            description: "Client idle timeout in ms (0 = no timeout)".into(),
            default_value: ConfigValue::Duration(0),
            value_type: ValueType::Duration,
            hot_reloadable: true,
            min_value: Some(0.0),
            max_value: None,
            validator: None,
        },
        ConfigParameter {
            key: "tcp-keepalive".into(),
            description: "TCP keepalive interval in ms".into(),
            default_value: ConfigValue::Duration(300_000),
            value_type: ValueType::Duration,
            hot_reloadable: true,
            min_value: Some(0.0),
            max_value: None,
            validator: None,
        },
        ConfigParameter {
            key: "loglevel".into(),
            description: "Server log verbosity level".into(),
            default_value: ConfigValue::String("notice".into()),
            value_type: ValueType::String,
            hot_reloadable: true,
            min_value: None,
            max_value: None,
            validator: Some(ValidatorType::Enum {
                values: vec![
                    "debug".into(),
                    "verbose".into(),
                    "notice".into(),
                    "warning".into(),
                ],
            }),
        },
        // -- Slow log --
        ConfigParameter {
            key: "slowlog-log-slower-than".into(),
            description: "Slow log threshold in microseconds".into(),
            default_value: ConfigValue::Duration(10_000),
            value_type: ValueType::Duration,
            hot_reloadable: true,
            min_value: Some(0.0),
            max_value: None,
            validator: None,
        },
        ConfigParameter {
            key: "slowlog-max-len".into(),
            description: "Maximum number of slow log entries".into(),
            default_value: ConfigValue::Int(128),
            value_type: ValueType::Integer,
            hot_reloadable: true,
            min_value: Some(0.0),
            max_value: None,
            validator: None,
        },
        // -- Clients --
        ConfigParameter {
            key: "maxclients".into(),
            description: "Maximum number of simultaneous client connections".into(),
            default_value: ConfigValue::Int(10_000),
            value_type: ValueType::Integer,
            hot_reloadable: true,
            min_value: Some(1.0),
            max_value: None,
            validator: None,
        },
        // -- AOF / Persistence --
        ConfigParameter {
            key: "appendonly".into(),
            description: "Enable append-only file persistence".into(),
            default_value: ConfigValue::Bool(false),
            value_type: ValueType::Boolean,
            hot_reloadable: true,
            min_value: None,
            max_value: None,
            validator: None,
        },
        ConfigParameter {
            key: "appendfsync".into(),
            description: "AOF fsync policy".into(),
            default_value: ConfigValue::String("everysec".into()),
            value_type: ValueType::String,
            hot_reloadable: true,
            min_value: None,
            max_value: None,
            validator: Some(ValidatorType::Enum {
                values: vec!["always".into(), "everysec".into(), "no".into()],
            }),
        },
        ConfigParameter {
            key: "no-appendfsync-on-rewrite".into(),
            description: "Skip fsync during AOF/RDB rewrite".into(),
            default_value: ConfigValue::Bool(false),
            value_type: ValueType::Boolean,
            hot_reloadable: true,
            min_value: None,
            max_value: None,
            validator: None,
        },
        ConfigParameter {
            key: "auto-aof-rewrite-percentage".into(),
            description: "Trigger AOF rewrite when growth exceeds this percentage".into(),
            default_value: ConfigValue::Int(100),
            value_type: ValueType::Integer,
            hot_reloadable: true,
            min_value: Some(0.0),
            max_value: None,
            validator: None,
        },
        ConfigParameter {
            key: "auto-aof-rewrite-min-size".into(),
            description: "Minimum AOF size before auto-rewrite triggers".into(),
            default_value: ConfigValue::Size(67_108_864), // 64 MB
            value_type: ValueType::Size,
            hot_reloadable: true,
            min_value: Some(0.0),
            max_value: None,
            validator: None,
        },
        ConfigParameter {
            key: "save".into(),
            description: "RDB save schedule (seconds changes pairs)".into(),
            default_value: ConfigValue::List(vec![
                "3600 1".into(),
                "300 100".into(),
                "60 10000".into(),
            ]),
            value_type: ValueType::StringList,
            hot_reloadable: true,
            min_value: None,
            max_value: None,
            validator: None,
        },
        // -- Network (cold) --
        ConfigParameter {
            key: "bind".into(),
            description: "Network interfaces to bind to".into(),
            default_value: ConfigValue::String("127.0.0.1".into()),
            value_type: ValueType::String,
            hot_reloadable: false,
            min_value: None,
            max_value: None,
            validator: None,
        },
        ConfigParameter {
            key: "port".into(),
            description: "TCP port to listen on".into(),
            default_value: ConfigValue::Int(6379),
            value_type: ValueType::Integer,
            hot_reloadable: false,
            min_value: Some(0.0),
            max_value: Some(65535.0),
            validator: Some(ValidatorType::Range {
                min: 0.0,
                max: 65535.0,
            }),
        },
        // -- Security --
        ConfigParameter {
            key: "requirepass".into(),
            description: "Server password for AUTH".into(),
            default_value: ConfigValue::String(String::new()),
            value_type: ValueType::String,
            hot_reloadable: true,
            min_value: None,
            max_value: None,
            validator: None,
        },
        ConfigParameter {
            key: "rename-command".into(),
            description: "Rename or disable commands".into(),
            default_value: ConfigValue::String(String::new()),
            value_type: ValueType::String,
            hot_reloadable: false,
            min_value: None,
            max_value: None,
            validator: None,
        },
        ConfigParameter {
            key: "databases".into(),
            description: "Number of databases".into(),
            default_value: ConfigValue::Int(16),
            value_type: ValueType::Integer,
            hot_reloadable: false,
            min_value: Some(1.0),
            max_value: None,
            validator: None,
        },
        // -- Tiering (Ferrite-specific) --
        ConfigParameter {
            key: "tiering-enabled".into(),
            description: "Enable tiered storage".into(),
            default_value: ConfigValue::Bool(false),
            value_type: ValueType::Boolean,
            hot_reloadable: true,
            min_value: None,
            max_value: None,
            validator: None,
        },
        ConfigParameter {
            key: "tiering-memory-threshold".into(),
            description: "Memory pressure threshold to trigger tiering (0.0-1.0)".into(),
            default_value: ConfigValue::Float(0.8),
            value_type: ValueType::Float,
            hot_reloadable: true,
            min_value: Some(0.0),
            max_value: Some(1.0),
            validator: Some(ValidatorType::Range { min: 0.0, max: 1.0 }),
        },
        ConfigParameter {
            key: "tiering-disk-path".into(),
            description: "Disk path for tiered storage".into(),
            default_value: ConfigValue::String("./data/tier".into()),
            value_type: ValueType::String,
            hot_reloadable: false,
            min_value: None,
            max_value: None,
            validator: None,
        },
        // -- Cluster --
        ConfigParameter {
            key: "cluster-enabled".into(),
            description: "Enable cluster mode".into(),
            default_value: ConfigValue::Bool(false),
            value_type: ValueType::Boolean,
            hot_reloadable: false,
            min_value: None,
            max_value: None,
            validator: None,
        },
        // -- Replication --
        ConfigParameter {
            key: "repl-backlog-size".into(),
            description: "Replication backlog size in bytes".into(),
            default_value: ConfigValue::Size(1_048_576), // 1 MB
            value_type: ValueType::Size,
            hot_reloadable: true,
            min_value: Some(0.0),
            max_value: None,
            validator: None,
        },
        ConfigParameter {
            key: "min-replicas-to-write".into(),
            description: "Minimum replicas required for write acceptance".into(),
            default_value: ConfigValue::Int(0),
            value_type: ValueType::Integer,
            hot_reloadable: true,
            min_value: Some(0.0),
            max_value: None,
            validator: None,
        },
        // -- Client buffers --
        ConfigParameter {
            key: "client-output-buffer-limit".into(),
            description: "Client output buffer hard limit in bytes".into(),
            default_value: ConfigValue::Size(0),
            value_type: ValueType::Size,
            hot_reloadable: true,
            min_value: Some(0.0),
            max_value: None,
            validator: None,
        },
        // -- Scripting --
        ConfigParameter {
            key: "lua-time-limit".into(),
            description: "Maximum execution time for Lua scripts in ms".into(),
            default_value: ConfigValue::Duration(5_000),
            value_type: ValueType::Duration,
            hot_reloadable: true,
            min_value: Some(0.0),
            max_value: None,
            validator: None,
        },
        // -- Defragmentation --
        ConfigParameter {
            key: "active-defrag-enabled".into(),
            description: "Enable active defragmentation".into(),
            default_value: ConfigValue::Bool(false),
            value_type: ValueType::Boolean,
            hot_reloadable: true,
            min_value: None,
            max_value: None,
            validator: None,
        },
        ConfigParameter {
            key: "active-defrag-threshold-lower".into(),
            description: "Minimum fragmentation percentage to start defrag".into(),
            default_value: ConfigValue::Int(10),
            value_type: ValueType::Integer,
            hot_reloadable: true,
            min_value: Some(0.0),
            max_value: Some(100.0),
            validator: Some(ValidatorType::Range {
                min: 0.0,
                max: 100.0,
            }),
        },
    ]
}

/// Lazily-built global parameter registry
fn parameter_registry() -> &'static Vec<ConfigParameter> {
    use std::sync::OnceLock;
    static REGISTRY: OnceLock<Vec<ConfigParameter>> = OnceLock::new();
    REGISTRY.get_or_init(build_parameter_registry)
}

// ---------------------------------------------------------------------------
// ConfigManager
// ---------------------------------------------------------------------------

/// Runtime configuration manager with hot-reload, validation, and history.
pub struct ConfigManager {
    /// Current live values
    values: DashMap<String, ConfigValue>,
    /// Parameter definitions indexed by key
    parameters: DashMap<String, ConfigParameter>,
    /// Monotonically increasing version counter
    version: AtomicU64,
    /// Ordered change history
    history: RwLock<Vec<ConfigChange>>,
    /// Active subscriptions: key → set of subscription ids
    subscriptions: DashMap<String, Vec<ConfigSubscriptionId>>,
    /// Operational counters
    total_sets: AtomicU64,
    total_resets: AtomicU64,
    total_rewrites: AtomicU64,
}

impl ConfigManager {
    /// Create a new configuration manager seeded with `initial` values.
    ///
    /// Unknown keys present in the snapshot are silently accepted; known
    /// parameter definitions from [`PARAMETER_REGISTRY`] are loaded
    /// automatically.
    pub fn new(initial: ConfigSnapshot) -> Self {
        let parameters: DashMap<String, ConfigParameter> = DashMap::new();
        let values: DashMap<String, ConfigValue> = DashMap::new();

        // Load registry definitions and their defaults
        for param in parameter_registry() {
            parameters.insert(param.key.clone(), param.clone());
            values.insert(param.key.clone(), param.default_value.clone());
        }

        // Override with supplied initial values
        for (k, v) in &initial.values {
            values.insert(k.clone(), v.clone());
        }

        Self {
            values,
            parameters,
            version: AtomicU64::new(initial.version),
            history: RwLock::new(Vec::new()),
            subscriptions: DashMap::new(),
            total_sets: AtomicU64::new(0),
            total_resets: AtomicU64::new(0),
            total_rewrites: AtomicU64::new(0),
        }
    }

    // -- Reads ---------------------------------------------------------------

    /// Get the current value for `key`, or `None` if unknown.
    pub fn get(&self, key: &str) -> Option<ConfigValue> {
        self.values.get(key).map(|v| v.value().clone())
    }

    /// Return a full snapshot of the current configuration.
    pub fn get_all(&self) -> ConfigSnapshot {
        let values: HashMap<String, ConfigValue> = self
            .values
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        ConfigSnapshot {
            values,
            version: self.version.load(Ordering::SeqCst),
            updated_at: Utc::now(),
        }
    }

    /// Return all changes recorded after `since`.
    pub fn get_changes_since(&self, since: DateTime<Utc>) -> Vec<ConfigChange> {
        let history = self.history.read();
        history
            .iter()
            .filter(|c| c.changed_at > since)
            .cloned()
            .collect()
    }

    /// Check whether `key` is defined as hot-reloadable.
    pub fn is_hot_reloadable(&self, key: &str) -> bool {
        self.parameters
            .get(key)
            .map(|p| p.hot_reloadable)
            .unwrap_or(false)
    }

    // -- Writes --------------------------------------------------------------

    /// Set a single configuration parameter.
    ///
    /// Returns the [`ConfigChange`] on success, or a [`ConfigError`] if the
    /// parameter is unknown, not hot-reloadable, or the value fails
    /// validation.
    pub fn set(&self, key: &str, value: ConfigValue) -> Result<ConfigChange, ConfigError> {
        self.validate_and_set(key, value, "config_set")
    }

    /// Atomically set multiple configuration parameters.
    ///
    /// All parameters are validated up-front; if any fail, no changes are
    /// applied.
    pub fn set_multiple(
        &self,
        changes: Vec<(String, ConfigValue)>,
    ) -> Result<Vec<ConfigChange>, ConfigError> {
        // Pre-validate all
        for (key, value) in &changes {
            self.validate(key, value)?;
        }
        // Apply
        let mut results = Vec::with_capacity(changes.len());
        for (key, value) in changes {
            let change = self.apply_change(&key, value, "config_set_multi");
            results.push(change);
        }
        Ok(results)
    }

    /// Reset a parameter to its registered default value.
    pub fn reset_to_default(&self, key: &str) -> Result<ConfigChange, ConfigError> {
        let param = self
            .parameters
            .get(key)
            .ok_or_else(|| ConfigError::UnknownParameter(key.to_string()))?;

        if !param.hot_reloadable {
            return Err(ConfigError::NotHotReloadable(key.to_string()));
        }

        let default = param.default_value.clone();
        drop(param); // release DashMap ref before mutating

        let change = self.apply_change(key, default, "config_reset");
        self.total_resets.fetch_add(1, Ordering::Relaxed);
        Ok(change)
    }

    // -- Rewrite -------------------------------------------------------------

    /// Serialize the current configuration to TOML.
    pub fn rewrite(&self) -> Result<String, ConfigError> {
        self.total_rewrites.fetch_add(1, Ordering::Relaxed);

        let snapshot = self.get_all();
        // Build a BTreeMap so keys are sorted
        let sorted: std::collections::BTreeMap<String, toml::Value> = snapshot
            .values
            .into_iter()
            .map(|(k, v)| (k, config_value_to_toml(&v)))
            .collect();

        toml::to_string_pretty(&sorted).map_err(|e| ConfigError::RewriteFailed(e.to_string()))
    }

    // -- Subscriptions -------------------------------------------------------

    /// Register interest in changes to `key`.
    ///
    /// Returns a [`ConfigSubscriptionId`] that can be used to identify the
    /// subscription.
    pub fn subscribe(&self, key: &str) -> ConfigSubscriptionId {
        let id = ConfigSubscriptionId::next();
        self.subscriptions
            .entry(key.to_string())
            .or_default()
            .push(id);
        id
    }

    // -- Stats ---------------------------------------------------------------

    /// Return operational statistics.
    pub fn get_stats(&self) -> ConfigStats {
        let (hot, cold) = self.parameters.iter().fold((0usize, 0usize), |(h, c), e| {
            if e.value().hot_reloadable {
                (h + 1, c)
            } else {
                (h, c + 1)
            }
        });

        let subs_active: usize = self.subscriptions.iter().map(|e| e.value().len()).sum();

        ConfigStats {
            total_sets: self.total_sets.load(Ordering::Relaxed),
            total_resets: self.total_resets.load(Ordering::Relaxed),
            total_rewrites: self.total_rewrites.load(Ordering::Relaxed),
            changes_history_size: self.history.read().len(),
            subscriptions_active: subs_active,
            hot_reloadable_params: hot,
            cold_params: cold,
        }
    }

    // -- Internal helpers ----------------------------------------------------

    /// Validate *and* apply a single change.
    fn validate_and_set(
        &self,
        key: &str,
        value: ConfigValue,
        changed_by: &str,
    ) -> Result<ConfigChange, ConfigError> {
        self.validate(key, &value)?;
        Ok(self.apply_change(key, value, changed_by))
    }

    /// Validate a value against its parameter definition.
    fn validate(&self, key: &str, value: &ConfigValue) -> Result<(), ConfigError> {
        let param = self
            .parameters
            .get(key)
            .ok_or_else(|| ConfigError::UnknownParameter(key.to_string()))?;

        // Hot-reload gate
        if !param.hot_reloadable {
            return Err(ConfigError::NotHotReloadable(key.to_string()));
        }

        // Type check
        self.check_type(key, value, &param.value_type)?;

        // Range check (min/max on the parameter definition)
        if let Some(numeric) = numeric_value(value) {
            if let Some(min) = param.min_value {
                if let Some(max) = param.max_value {
                    if numeric < min || numeric > max {
                        return Err(ConfigError::OutOfRange {
                            key: key.to_string(),
                            min,
                            max,
                            got: numeric,
                        });
                    }
                }
            }
        }

        // Validator
        if let Some(ref validator) = param.validator {
            self.run_validator(key, value, validator)?;
        }

        Ok(())
    }

    /// Check that `value` matches the expected `ValueType`.
    fn check_type(
        &self,
        key: &str,
        value: &ConfigValue,
        expected: &ValueType,
    ) -> Result<(), ConfigError> {
        let ok = matches!(
            (value, expected),
            (ConfigValue::String(_), ValueType::String)
                | (ConfigValue::Int(_), ValueType::Integer)
                | (ConfigValue::Float(_), ValueType::Float)
                | (ConfigValue::Bool(_), ValueType::Boolean)
                | (ConfigValue::Duration(_), ValueType::Duration)
                | (ConfigValue::Size(_), ValueType::Size)
                | (ConfigValue::List(_), ValueType::StringList)
        );
        if !ok {
            return Err(ConfigError::TypeMismatch {
                key: key.to_string(),
                expected: expected.to_string(),
                got: value.type_name().to_string(),
            });
        }
        Ok(())
    }

    /// Execute a [`ValidatorType`] against `value`.
    fn run_validator(
        &self,
        key: &str,
        value: &ConfigValue,
        validator: &ValidatorType,
    ) -> Result<(), ConfigError> {
        match validator {
            ValidatorType::Range { min, max } => {
                if let Some(n) = numeric_value(value) {
                    if n < *min || n > *max {
                        return Err(ConfigError::OutOfRange {
                            key: key.to_string(),
                            min: *min,
                            max: *max,
                            got: n,
                        });
                    }
                }
            }
            ValidatorType::Enum { values: allowed } => {
                let s = match value {
                    ConfigValue::String(s) => s.clone(),
                    other => other.to_string(),
                };
                if !allowed.contains(&s) {
                    return Err(ConfigError::ValidationFailed {
                        key: key.to_string(),
                        reason: format!(
                            "value '{}' not in allowed set: [{}]",
                            s,
                            allowed.join(", ")
                        ),
                    });
                }
            }
            ValidatorType::Pattern { regex: pattern } => {
                let s = match value {
                    ConfigValue::String(s) => s.clone(),
                    other => other.to_string(),
                };
                let re =
                    regex::Regex::new(pattern).map_err(|e| ConfigError::Internal(e.to_string()))?;
                if !re.is_match(&s) {
                    return Err(ConfigError::ValidationFailed {
                        key: key.to_string(),
                        reason: format!("value '{}' does not match pattern '{}'", s, pattern),
                    });
                }
            }
            ValidatorType::Custom(name) => {
                return Err(ConfigError::ValidationFailed {
                    key: key.to_string(),
                    reason: format!("custom validator '{}' not implemented", name),
                });
            }
        }
        Ok(())
    }

    /// Apply a pre-validated change: swap value, bump version, record history.
    fn apply_change(&self, key: &str, value: ConfigValue, changed_by: &str) -> ConfigChange {
        let old = self.values.insert(key.to_string(), value.clone());
        self.version.fetch_add(1, Ordering::SeqCst);
        self.total_sets.fetch_add(1, Ordering::Relaxed);

        let change = ConfigChange {
            key: key.to_string(),
            old_value: old,
            new_value: value,
            changed_at: Utc::now(),
            changed_by: changed_by.to_string(),
        };

        self.history.write().push(change.clone());
        change
    }
}

impl Default for ConfigManager {
    fn default() -> Self {
        Self::new(ConfigSnapshot::default())
    }
}

/// Shared configuration manager handle
pub type SharedConfigManager = Arc<ConfigManager>;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract a numeric value from a [`ConfigValue`] for range checking.
fn numeric_value(v: &ConfigValue) -> Option<f64> {
    match v {
        ConfigValue::Int(n) => Some(*n as f64),
        ConfigValue::Float(n) => Some(*n),
        ConfigValue::Duration(n) => Some(*n as f64),
        ConfigValue::Size(n) => Some(*n as f64),
        _ => None,
    }
}

/// Convert a [`ConfigValue`] to a [`toml::Value`].
fn config_value_to_toml(v: &ConfigValue) -> toml::Value {
    match v {
        ConfigValue::String(s) => toml::Value::String(s.clone()),
        ConfigValue::Int(n) => toml::Value::Integer(*n),
        ConfigValue::Float(f) => toml::Value::Float(*f),
        ConfigValue::Bool(b) => toml::Value::Boolean(*b),
        ConfigValue::Duration(ms) => toml::Value::Integer(*ms as i64),
        ConfigValue::Size(b) => toml::Value::Integer(*b as i64),
        ConfigValue::List(l) => {
            toml::Value::Array(l.iter().map(|s| toml::Value::String(s.clone())).collect())
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a manager with registry defaults.
    fn default_manager() -> ConfigManager {
        ConfigManager::new(ConfigSnapshot::new())
    }

    // -- get / set -----------------------------------------------------------

    #[test]
    fn test_get_default_value() {
        let mgr = default_manager();
        let val = mgr.get("maxmemory");
        assert_eq!(val, Some(ConfigValue::Size(0)));
    }

    #[test]
    fn test_get_unknown_key_returns_none() {
        let mgr = default_manager();
        assert!(mgr.get("no-such-key").is_none());
    }

    #[test]
    fn test_set_hot_reloadable() {
        let mgr = default_manager();
        let change = mgr
            .set("maxmemory", ConfigValue::Size(1_073_741_824))
            .expect("set should succeed");
        assert_eq!(change.key, "maxmemory");
        assert_eq!(change.old_value, Some(ConfigValue::Size(0)));
        assert_eq!(change.new_value, ConfigValue::Size(1_073_741_824));
        assert_eq!(mgr.get("maxmemory"), Some(ConfigValue::Size(1_073_741_824)));
    }

    // -- hot-reload vs cold --------------------------------------------------

    #[test]
    fn test_set_cold_parameter_rejected() {
        let mgr = default_manager();
        let err = mgr
            .set("port", ConfigValue::Int(6380))
            .expect_err("should reject cold param");
        assert!(matches!(err, ConfigError::NotHotReloadable(_)));
    }

    #[test]
    fn test_is_hot_reloadable() {
        let mgr = default_manager();
        assert!(mgr.is_hot_reloadable("maxmemory"));
        assert!(mgr.is_hot_reloadable("hz"));
        assert!(!mgr.is_hot_reloadable("port"));
        assert!(!mgr.is_hot_reloadable("bind"));
        assert!(!mgr.is_hot_reloadable("nonexistent"));
    }

    // -- validation: range ---------------------------------------------------

    #[test]
    fn test_range_validation_ok() {
        let mgr = default_manager();
        mgr.set("hz", ConfigValue::Int(100))
            .expect("100 Hz is in range");
    }

    #[test]
    fn test_range_validation_too_low() {
        let mgr = default_manager();
        let err = mgr
            .set("hz", ConfigValue::Int(0))
            .expect_err("0 Hz is below range");
        assert!(matches!(err, ConfigError::OutOfRange { .. }));
    }

    #[test]
    fn test_range_validation_too_high() {
        let mgr = default_manager();
        let err = mgr
            .set("hz", ConfigValue::Int(501))
            .expect_err("501 Hz is above range");
        assert!(matches!(err, ConfigError::OutOfRange { .. }));
    }

    #[test]
    fn test_float_range_validation() {
        let mgr = default_manager();
        mgr.set("tiering-memory-threshold", ConfigValue::Float(0.5))
            .expect("0.5 is in 0.0..1.0");
        let err = mgr
            .set("tiering-memory-threshold", ConfigValue::Float(1.5))
            .expect_err("1.5 is out of range");
        assert!(matches!(err, ConfigError::OutOfRange { .. }));
    }

    // -- validation: enum ----------------------------------------------------

    #[test]
    fn test_enum_validation_ok() {
        let mgr = default_manager();
        mgr.set(
            "maxmemory-policy",
            ConfigValue::String("allkeys-lru".into()),
        )
        .expect("valid policy");
    }

    #[test]
    fn test_enum_validation_invalid() {
        let mgr = default_manager();
        let err = mgr
            .set("maxmemory-policy", ConfigValue::String("bad-policy".into()))
            .expect_err("invalid policy");
        assert!(matches!(err, ConfigError::ValidationFailed { .. }));
    }

    #[test]
    fn test_enum_loglevel() {
        let mgr = default_manager();
        for level in &["debug", "verbose", "notice", "warning"] {
            mgr.set("loglevel", ConfigValue::String((*level).into()))
                .expect("valid loglevel");
        }
        let err = mgr
            .set("loglevel", ConfigValue::String("trace".into()))
            .expect_err("invalid loglevel");
        assert!(matches!(err, ConfigError::ValidationFailed { .. }));
    }

    // -- validation: type mismatch -------------------------------------------

    #[test]
    fn test_type_mismatch() {
        let mgr = default_manager();
        let err = mgr
            .set("hz", ConfigValue::String("fast".into()))
            .expect_err("string for int param");
        assert!(matches!(err, ConfigError::TypeMismatch { .. }));
    }

    #[test]
    fn test_type_mismatch_bool_for_int() {
        let mgr = default_manager();
        let err = mgr
            .set("maxclients", ConfigValue::Bool(true))
            .expect_err("bool for int param");
        assert!(matches!(err, ConfigError::TypeMismatch { .. }));
    }

    // -- unknown parameter ---------------------------------------------------

    #[test]
    fn test_set_unknown_parameter() {
        let mgr = default_manager();
        let err = mgr
            .set("nonexistent", ConfigValue::Int(42))
            .expect_err("unknown key");
        assert!(matches!(err, ConfigError::UnknownParameter(_)));
    }

    // -- change history ------------------------------------------------------

    #[test]
    fn test_change_history() {
        let mgr = default_manager();
        let before = Utc::now();

        mgr.set("maxmemory", ConfigValue::Size(100))
            .expect("set ok");
        mgr.set("hz", ConfigValue::Int(50)).expect("set ok");

        let changes = mgr.get_changes_since(before - chrono::Duration::seconds(1));
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].key, "maxmemory");
        assert_eq!(changes[1].key, "hz");
    }

    #[test]
    fn test_change_history_filtered_by_time() {
        let mgr = default_manager();
        mgr.set("maxmemory", ConfigValue::Size(100))
            .expect("set ok");

        let after = Utc::now();
        let changes = mgr.get_changes_since(after);
        assert!(changes.is_empty());
    }

    // -- CONFIG REWRITE (TOML) -----------------------------------------------

    #[test]
    fn test_rewrite_produces_toml() {
        let mgr = default_manager();
        mgr.set("maxmemory", ConfigValue::Size(2_000_000))
            .expect("set ok");

        let toml_str = mgr.rewrite().expect("rewrite should succeed");
        assert!(toml_str.contains("maxmemory"));
        // The value should be parseable TOML
        let parsed: std::collections::BTreeMap<String, toml::Value> =
            toml::from_str(&toml_str).expect("valid TOML");
        assert_eq!(
            parsed.get("maxmemory"),
            Some(&toml::Value::Integer(2_000_000))
        );
    }

    #[test]
    fn test_rewrite_includes_all_keys() {
        let mgr = default_manager();
        let toml_str = mgr.rewrite().expect("rewrite");
        // Spot-check a few keys
        assert!(toml_str.contains("hz"));
        assert!(toml_str.contains("port"));
        assert!(toml_str.contains("loglevel"));
        assert!(toml_str.contains("maxmemory-policy"));
    }

    // -- reset to default ----------------------------------------------------

    #[test]
    fn test_reset_to_default() {
        let mgr = default_manager();
        mgr.set("hz", ConfigValue::Int(200)).expect("set ok");
        assert_eq!(mgr.get("hz"), Some(ConfigValue::Int(200)));

        let change = mgr.reset_to_default("hz").expect("reset ok");
        assert_eq!(change.old_value, Some(ConfigValue::Int(200)));
        assert_eq!(change.new_value, ConfigValue::Int(10)); // default
        assert_eq!(mgr.get("hz"), Some(ConfigValue::Int(10)));
    }

    #[test]
    fn test_reset_cold_parameter_rejected() {
        let mgr = default_manager();
        let err = mgr.reset_to_default("port").expect_err("cold param reset");
        assert!(matches!(err, ConfigError::NotHotReloadable(_)));
    }

    // -- set_multiple (atomic) -----------------------------------------------

    #[test]
    fn test_set_multiple_success() {
        let mgr = default_manager();
        let changes = mgr
            .set_multiple(vec![
                ("maxmemory".into(), ConfigValue::Size(500)),
                ("hz".into(), ConfigValue::Int(20)),
                ("appendonly".into(), ConfigValue::Bool(true)),
            ])
            .expect("batch set ok");
        assert_eq!(changes.len(), 3);
        assert_eq!(mgr.get("maxmemory"), Some(ConfigValue::Size(500)));
        assert_eq!(mgr.get("hz"), Some(ConfigValue::Int(20)));
        assert_eq!(mgr.get("appendonly"), Some(ConfigValue::Bool(true)));
    }

    #[test]
    fn test_set_multiple_rolls_back_on_validation_failure() {
        let mgr = default_manager();
        let original_hz = mgr.get("hz").expect("has default");

        let err = mgr
            .set_multiple(vec![
                ("maxmemory".into(), ConfigValue::Size(500)),
                ("hz".into(), ConfigValue::Int(9999)), // out of range
            ])
            .expect_err("should fail");
        assert!(matches!(err, ConfigError::OutOfRange { .. }));

        // Neither change should have been applied
        assert_eq!(mgr.get("hz"), Some(original_hz));
        // maxmemory should still be default since validation failed before apply
        assert_eq!(mgr.get("maxmemory"), Some(ConfigValue::Size(0)));
    }

    // -- stats ---------------------------------------------------------------

    #[test]
    fn test_stats_initial() {
        let mgr = default_manager();
        let stats = mgr.get_stats();
        assert_eq!(stats.total_sets, 0);
        assert_eq!(stats.total_resets, 0);
        assert_eq!(stats.total_rewrites, 0);
        assert_eq!(stats.changes_history_size, 0);
        assert!(stats.hot_reloadable_params > 0);
        assert!(stats.cold_params > 0);
    }

    #[test]
    fn test_stats_after_operations() {
        let mgr = default_manager();
        mgr.set("maxmemory", ConfigValue::Size(100)).expect("ok");
        mgr.set("hz", ConfigValue::Int(20)).expect("ok");
        mgr.reset_to_default("hz").expect("ok");
        mgr.rewrite().expect("ok");

        let stats = mgr.get_stats();
        assert_eq!(stats.total_sets, 3); // 2 sets + 1 reset (which also counts as a set internally)
        assert_eq!(stats.total_resets, 1);
        assert_eq!(stats.total_rewrites, 1);
        assert_eq!(stats.changes_history_size, 3);
    }

    // -- subscriptions -------------------------------------------------------

    #[test]
    fn test_subscribe_returns_unique_ids() {
        let mgr = default_manager();
        let id1 = mgr.subscribe("maxmemory");
        let id2 = mgr.subscribe("maxmemory");
        let id3 = mgr.subscribe("hz");
        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
    }

    #[test]
    fn test_subscribe_counted_in_stats() {
        let mgr = default_manager();
        mgr.subscribe("maxmemory");
        mgr.subscribe("hz");
        mgr.subscribe("hz");

        let stats = mgr.get_stats();
        assert_eq!(stats.subscriptions_active, 3);
    }

    // -- snapshot / initial values -------------------------------------------

    #[test]
    fn test_initial_snapshot_overrides_defaults() {
        let mut initial = ConfigSnapshot::new();
        initial.values.insert("hz".into(), ConfigValue::Int(42));
        initial
            .values
            .insert("maxmemory".into(), ConfigValue::Size(999));

        let mgr = ConfigManager::new(initial);
        assert_eq!(mgr.get("hz"), Some(ConfigValue::Int(42)));
        assert_eq!(mgr.get("maxmemory"), Some(ConfigValue::Size(999)));
        // Other params still have defaults
        assert_eq!(mgr.get("maxclients"), Some(ConfigValue::Int(10_000)));
    }

    #[test]
    fn test_get_all_returns_complete_snapshot() {
        let mgr = default_manager();
        let snap = mgr.get_all();
        // Should contain all registry params
        assert!(snap.values.contains_key("maxmemory"));
        assert!(snap.values.contains_key("port"));
        assert!(snap.values.contains_key("hz"));
        assert!(snap.values.len() >= 30);
    }

    // -- appendfsync enum ----------------------------------------------------

    #[test]
    fn test_appendfsync_enum_validation() {
        let mgr = default_manager();
        for policy in &["always", "everysec", "no"] {
            mgr.set("appendfsync", ConfigValue::String((*policy).into()))
                .expect("valid appendfsync");
        }
        let err = mgr
            .set("appendfsync", ConfigValue::String("sometimes".into()))
            .expect_err("invalid appendfsync");
        assert!(matches!(err, ConfigError::ValidationFailed { .. }));
    }

    // -- defrag threshold range ----------------------------------------------

    #[test]
    fn test_defrag_threshold_range() {
        let mgr = default_manager();
        mgr.set("active-defrag-threshold-lower", ConfigValue::Int(50))
            .expect("50 is valid");
        let err = mgr
            .set("active-defrag-threshold-lower", ConfigValue::Int(101))
            .expect_err("101 out of 0-100");
        assert!(matches!(err, ConfigError::OutOfRange { .. }));
    }

    // -- config value display ------------------------------------------------

    #[test]
    fn test_config_value_display() {
        assert_eq!(ConfigValue::String("hello".into()).to_string(), "hello");
        assert_eq!(ConfigValue::Int(42).to_string(), "42");
        assert_eq!(ConfigValue::Bool(true).to_string(), "true");
        assert_eq!(ConfigValue::Duration(5000).to_string(), "5000ms");
        assert_eq!(ConfigValue::Size(1024).to_string(), "1024B");
        assert_eq!(
            ConfigValue::List(vec!["a".into(), "b".into()]).to_string(),
            "a b"
        );
    }

    // -- error display -------------------------------------------------------

    #[test]
    fn test_error_display() {
        let err = ConfigError::UnknownParameter("foo".into());
        assert!(err.to_string().contains("foo"));

        let err = ConfigError::NotHotReloadable("bind".into());
        assert!(err.to_string().contains("bind"));

        let err = ConfigError::TypeMismatch {
            key: "hz".into(),
            expected: "Integer".into(),
            got: "String".into(),
        };
        assert!(err.to_string().contains("Integer"));
        assert!(err.to_string().contains("String"));
    }
}
