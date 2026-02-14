//! Plugin Sandbox
//!
//! Provides isolation and resource limits for plugin execution.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Duration;

/// Sandbox configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SandboxConfig {
    /// Resource limits
    pub limits: SandboxLimits,
    /// Allowed host functions
    pub allowed_functions: HashSet<String>,
    /// Allowed key patterns
    pub allowed_key_patterns: Vec<String>,
    /// Blocked commands
    pub blocked_commands: HashSet<String>,
    /// Allow network access
    pub allow_network: bool,
    /// Allow filesystem access
    pub allow_filesystem: bool,
    /// Enable syscall filtering
    pub syscall_filtering: bool,
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            limits: SandboxLimits::default(),
            allowed_functions: default_allowed_functions(),
            allowed_key_patterns: vec!["*".to_string()],
            blocked_commands: default_blocked_commands(),
            allow_network: false,
            allow_filesystem: false,
            syscall_filtering: true,
        }
    }
}

impl SandboxConfig {
    /// Create a restrictive sandbox configuration
    pub fn restrictive() -> Self {
        Self {
            limits: SandboxLimits::restrictive(),
            allowed_functions: minimal_allowed_functions(),
            allowed_key_patterns: vec![],
            blocked_commands: default_blocked_commands(),
            allow_network: false,
            allow_filesystem: false,
            syscall_filtering: true,
        }
    }

    /// Create a permissive sandbox configuration (for trusted plugins)
    pub fn permissive() -> Self {
        Self {
            limits: SandboxLimits::permissive(),
            allowed_functions: all_host_functions(),
            allowed_key_patterns: vec!["*".to_string()],
            blocked_commands: HashSet::new(),
            allow_network: true,
            allow_filesystem: false,
            syscall_filtering: false,
        }
    }

    /// Builder: set limits
    pub fn with_limits(mut self, limits: SandboxLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Builder: allow function
    pub fn allow_function(mut self, func: &str) -> Self {
        self.allowed_functions.insert(func.to_string());
        self
    }

    /// Builder: allow key pattern
    pub fn allow_key_pattern(mut self, pattern: &str) -> Self {
        self.allowed_key_patterns.push(pattern.to_string());
        self
    }

    /// Builder: block command
    pub fn block_command(mut self, cmd: &str) -> Self {
        self.blocked_commands.insert(cmd.to_uppercase());
        self
    }

    /// Check if a function is allowed
    pub fn is_function_allowed(&self, func: &str) -> bool {
        self.allowed_functions.contains(func) || self.allowed_functions.contains("*")
    }

    /// Check if a key matches allowed patterns
    pub fn is_key_allowed(&self, key: &str) -> bool {
        for pattern in &self.allowed_key_patterns {
            if pattern == "*" || Self::matches_pattern(key, pattern) {
                return true;
            }
        }
        false
    }

    /// Check if a command is blocked
    pub fn is_command_blocked(&self, cmd: &str) -> bool {
        self.blocked_commands.contains(&cmd.to_uppercase())
    }

    /// Simple glob pattern matching
    fn matches_pattern(key: &str, pattern: &str) -> bool {
        if pattern == "*" {
            return true;
        }

        if let Some(prefix) = pattern.strip_suffix('*') {
            return key.starts_with(prefix);
        }

        if let Some(suffix) = pattern.strip_prefix('*') {
            return key.ends_with(suffix);
        }

        key == pattern
    }
}

/// Resource limits for sandbox
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SandboxLimits {
    /// Maximum memory in bytes
    pub max_memory: usize,
    /// Maximum execution time
    pub max_execution_time: Duration,
    /// Maximum WASM file size
    pub max_wasm_size: usize,
    /// Maximum fuel (instruction count)
    pub max_fuel: u64,
    /// Maximum stack size in bytes
    pub max_stack_size: usize,
    /// Maximum number of tables
    pub max_tables: u32,
    /// Maximum table elements
    pub max_table_elements: u32,
    /// Maximum instances
    pub max_instances: u32,
    /// Maximum linear memories
    pub max_memories: u32,
    /// Maximum globals
    pub max_globals: u32,
}

impl Default for SandboxLimits {
    fn default() -> Self {
        Self {
            max_memory: 64 * 1024 * 1024, // 64 MB
            max_execution_time: Duration::from_secs(5),
            max_wasm_size: 10 * 1024 * 1024, // 10 MB
            max_fuel: 10_000_000,
            max_stack_size: 1024 * 1024, // 1 MB
            max_tables: 10,
            max_table_elements: 10_000,
            max_instances: 10,
            max_memories: 1,
            max_globals: 100,
        }
    }
}

impl SandboxLimits {
    /// Create restrictive limits
    pub fn restrictive() -> Self {
        Self {
            max_memory: 16 * 1024 * 1024, // 16 MB
            max_execution_time: Duration::from_secs(1),
            max_wasm_size: 1024 * 1024, // 1 MB
            max_fuel: 1_000_000,
            max_stack_size: 256 * 1024, // 256 KB
            max_tables: 1,
            max_table_elements: 1_000,
            max_instances: 1,
            max_memories: 1,
            max_globals: 10,
        }
    }

    /// Create permissive limits
    pub fn permissive() -> Self {
        Self {
            max_memory: 256 * 1024 * 1024, // 256 MB
            max_execution_time: Duration::from_secs(60),
            max_wasm_size: 100 * 1024 * 1024, // 100 MB
            max_fuel: 1_000_000_000,
            max_stack_size: 8 * 1024 * 1024, // 8 MB
            max_tables: 100,
            max_table_elements: 100_000,
            max_instances: 100,
            max_memories: 10,
            max_globals: 1000,
        }
    }

    /// Builder: set max memory
    pub fn with_max_memory(mut self, bytes: usize) -> Self {
        self.max_memory = bytes;
        self
    }

    /// Builder: set max execution time
    pub fn with_max_execution_time(mut self, duration: Duration) -> Self {
        self.max_execution_time = duration;
        self
    }

    /// Builder: set max fuel
    pub fn with_max_fuel(mut self, fuel: u64) -> Self {
        self.max_fuel = fuel;
        self
    }
}

/// Plugin sandbox for executing plugins
pub struct PluginSandbox {
    /// Configuration
    config: SandboxConfig,
    /// Execution statistics
    stats: SandboxStats,
}

impl PluginSandbox {
    /// Create a new sandbox
    pub fn new(config: SandboxConfig) -> Self {
        Self {
            config,
            stats: SandboxStats::default(),
        }
    }

    /// Check if an operation is allowed
    pub fn check_permission(&self, operation: &SandboxOperation) -> Result<(), SandboxViolation> {
        match operation {
            SandboxOperation::CallHostFunction(func) => {
                if !self.config.is_function_allowed(func) {
                    return Err(SandboxViolation::FunctionNotAllowed(func.clone()));
                }
            }
            SandboxOperation::AccessKey(key) => {
                if !self.config.is_key_allowed(key) {
                    return Err(SandboxViolation::KeyNotAllowed(key.clone()));
                }
            }
            SandboxOperation::ExecuteCommand(cmd) => {
                if self.config.is_command_blocked(cmd) {
                    return Err(SandboxViolation::CommandBlocked(cmd.clone()));
                }
            }
            SandboxOperation::NetworkAccess => {
                if !self.config.allow_network {
                    return Err(SandboxViolation::NetworkNotAllowed);
                }
            }
            SandboxOperation::FilesystemAccess => {
                if !self.config.allow_filesystem {
                    return Err(SandboxViolation::FilesystemNotAllowed);
                }
            }
            SandboxOperation::AllocateMemory(bytes) => {
                if *bytes > self.config.limits.max_memory {
                    return Err(SandboxViolation::MemoryLimitExceeded {
                        requested: *bytes,
                        limit: self.config.limits.max_memory,
                    });
                }
            }
        }

        Ok(())
    }

    /// Record a sandbox check
    pub fn record_check(&mut self, allowed: bool) {
        self.stats.total_checks += 1;
        if allowed {
            self.stats.allowed_checks += 1;
        } else {
            self.stats.denied_checks += 1;
        }
    }

    /// Get configuration
    pub fn config(&self) -> &SandboxConfig {
        &self.config
    }

    /// Get statistics
    pub fn stats(&self) -> &SandboxStats {
        &self.stats
    }

    /// Get limits
    pub fn limits(&self) -> &SandboxLimits {
        &self.config.limits
    }
}

impl Default for PluginSandbox {
    fn default() -> Self {
        Self::new(SandboxConfig::default())
    }
}

/// Sandbox operations that can be checked
#[derive(Clone, Debug)]
pub enum SandboxOperation {
    /// Call a host function
    CallHostFunction(String),
    /// Access a key
    AccessKey(String),
    /// Execute a command
    ExecuteCommand(String),
    /// Network access
    NetworkAccess,
    /// Filesystem access
    FilesystemAccess,
    /// Allocate memory
    AllocateMemory(usize),
}

/// Sandbox violation errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum SandboxViolation {
    /// Host function not allowed
    #[error("host function not allowed: {0}")]
    FunctionNotAllowed(String),

    /// Key pattern not allowed
    #[error("key not allowed: {0}")]
    KeyNotAllowed(String),

    /// Command blocked
    #[error("command blocked: {0}")]
    CommandBlocked(String),

    /// Network access not allowed
    #[error("network access not allowed")]
    NetworkNotAllowed,

    /// Filesystem access not allowed
    #[error("filesystem access not allowed")]
    FilesystemNotAllowed,

    /// Memory limit exceeded
    #[error("memory limit exceeded: requested {requested}, limit {limit}")]
    MemoryLimitExceeded {
        /// Bytes requested
        requested: usize,
        /// Limit
        limit: usize,
    },

    /// Execution timeout
    #[error("execution timeout")]
    ExecutionTimeout,

    /// Fuel exhausted
    #[error("fuel exhausted")]
    FuelExhausted,
}

/// Sandbox statistics
#[derive(Clone, Debug, Default)]
pub struct SandboxStats {
    /// Total permission checks
    pub total_checks: u64,
    /// Allowed checks
    pub allowed_checks: u64,
    /// Denied checks
    pub denied_checks: u64,
}

/// Default allowed host functions
fn default_allowed_functions() -> HashSet<String> {
    let mut funcs = HashSet::new();
    funcs.insert("ferrite_get".to_string());
    funcs.insert("ferrite_set".to_string());
    funcs.insert("ferrite_del".to_string());
    funcs.insert("ferrite_exists".to_string());
    funcs.insert("ferrite_incr".to_string());
    funcs.insert("ferrite_decr".to_string());
    funcs.insert("ferrite_expire".to_string());
    funcs.insert("ferrite_ttl".to_string());
    funcs.insert("ferrite_keys".to_string());
    funcs.insert("ferrite_log".to_string());
    funcs
}

/// Minimal allowed host functions
fn minimal_allowed_functions() -> HashSet<String> {
    let mut funcs = HashSet::new();
    funcs.insert("ferrite_get".to_string());
    funcs.insert("ferrite_log".to_string());
    funcs
}

/// All host functions
fn all_host_functions() -> HashSet<String> {
    let mut funcs = HashSet::new();
    funcs.insert("*".to_string());
    funcs
}

/// Default blocked commands
fn default_blocked_commands() -> HashSet<String> {
    let mut cmds = HashSet::new();
    cmds.insert("CONFIG".to_string());
    cmds.insert("DEBUG".to_string());
    cmds.insert("SHUTDOWN".to_string());
    cmds.insert("SLAVEOF".to_string());
    cmds.insert("REPLICAOF".to_string());
    cmds.insert("CLUSTER".to_string());
    cmds.insert("MIGRATE".to_string());
    cmds.insert("MODULE".to_string());
    cmds.insert("ACL".to_string());
    cmds.insert("BGSAVE".to_string());
    cmds.insert("BGREWRITEAOF".to_string());
    cmds.insert("SAVE".to_string());
    cmds.insert("FLUSHALL".to_string());
    cmds.insert("FLUSHDB".to_string());
    cmds
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sandbox_config_default() {
        let config = SandboxConfig::default();
        assert!(!config.allow_network);
        assert!(!config.allow_filesystem);
        assert!(config.syscall_filtering);
    }

    #[test]
    fn test_sandbox_config_restrictive() {
        let config = SandboxConfig::restrictive();
        assert!(config.allowed_key_patterns.is_empty());
        assert!(!config.allow_network);
    }

    #[test]
    fn test_sandbox_config_permissive() {
        let config = SandboxConfig::permissive();
        assert!(config.allow_network);
        assert!(config.blocked_commands.is_empty());
    }

    #[test]
    fn test_sandbox_limits_default() {
        let limits = SandboxLimits::default();
        assert_eq!(limits.max_memory, 64 * 1024 * 1024);
        assert_eq!(limits.max_execution_time, Duration::from_secs(5));
    }

    #[test]
    fn test_sandbox_limits_restrictive() {
        let limits = SandboxLimits::restrictive();
        assert_eq!(limits.max_memory, 16 * 1024 * 1024);
        assert_eq!(limits.max_execution_time, Duration::from_secs(1));
    }

    #[test]
    fn test_sandbox_is_function_allowed() {
        let config = SandboxConfig::default();
        assert!(config.is_function_allowed("ferrite_get"));
        assert!(config.is_function_allowed("ferrite_set"));
        assert!(!config.is_function_allowed("ferrite_dangerous"));

        let permissive = SandboxConfig::permissive();
        assert!(permissive.is_function_allowed("anything"));
    }

    #[test]
    fn test_sandbox_is_key_allowed() {
        let config = SandboxConfig::default();
        assert!(config.is_key_allowed("any_key"));

        let mut config = SandboxConfig::restrictive();
        config.allowed_key_patterns = vec!["user:*".to_string()];
        assert!(config.is_key_allowed("user:123"));
        assert!(!config.is_key_allowed("admin:456"));
    }

    #[test]
    fn test_sandbox_is_command_blocked() {
        let config = SandboxConfig::default();
        assert!(config.is_command_blocked("CONFIG"));
        assert!(config.is_command_blocked("SHUTDOWN"));
        assert!(!config.is_command_blocked("GET"));
        assert!(!config.is_command_blocked("SET"));
    }

    #[test]
    fn test_sandbox_matches_pattern() {
        assert!(SandboxConfig::matches_pattern("test", "*"));
        assert!(SandboxConfig::matches_pattern("user:123", "user:*"));
        assert!(!SandboxConfig::matches_pattern("admin:123", "user:*"));
        assert!(SandboxConfig::matches_pattern("file.txt", "*.txt"));
        assert!(SandboxConfig::matches_pattern("exact", "exact"));
    }

    #[test]
    fn test_plugin_sandbox_check_permission() {
        let sandbox = PluginSandbox::default();

        // Allowed operations
        assert!(sandbox
            .check_permission(&SandboxOperation::CallHostFunction(
                "ferrite_get".to_string()
            ))
            .is_ok());

        assert!(sandbox
            .check_permission(&SandboxOperation::AccessKey("mykey".to_string()))
            .is_ok());

        assert!(sandbox
            .check_permission(&SandboxOperation::ExecuteCommand("GET".to_string()))
            .is_ok());

        // Denied operations
        assert!(sandbox
            .check_permission(&SandboxOperation::NetworkAccess)
            .is_err());

        assert!(sandbox
            .check_permission(&SandboxOperation::FilesystemAccess)
            .is_err());

        assert!(sandbox
            .check_permission(&SandboxOperation::ExecuteCommand("CONFIG".to_string()))
            .is_err());
    }

    #[test]
    fn test_plugin_sandbox_memory_limit() {
        let sandbox = PluginSandbox::default();

        // Within limit
        assert!(sandbox
            .check_permission(&SandboxOperation::AllocateMemory(1024))
            .is_ok());

        // Exceeds limit
        let result = sandbox.check_permission(&SandboxOperation::AllocateMemory(100 * 1024 * 1024));
        assert!(matches!(
            result,
            Err(SandboxViolation::MemoryLimitExceeded { .. })
        ));
    }

    #[test]
    fn test_plugin_sandbox_record_check() {
        let mut sandbox = PluginSandbox::default();

        sandbox.record_check(true);
        sandbox.record_check(true);
        sandbox.record_check(false);

        assert_eq!(sandbox.stats().total_checks, 3);
        assert_eq!(sandbox.stats().allowed_checks, 2);
        assert_eq!(sandbox.stats().denied_checks, 1);
    }

    #[test]
    fn test_sandbox_violation_display() {
        let v = SandboxViolation::FunctionNotAllowed("test".to_string());
        assert!(v.to_string().contains("test"));

        let v = SandboxViolation::MemoryLimitExceeded {
            requested: 100,
            limit: 50,
        };
        assert!(v.to_string().contains("100"));
        assert!(v.to_string().contains("50"));
    }
}
