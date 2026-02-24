//! # WebAssembly User Functions
//!
//! Run custom WebAssembly modules at the data layer, providing a portable,
//! secure, and polyglot alternative to Lua scripting.
//!
//! ## Why WebAssembly?
//!
//! Traditional Redis uses Lua for server-side scripting. WASM provides:
//!
//! - **Polyglot**: Write in Rust, Go, TypeScript, C++, or any WASM-targeting language
//! - **Secure**: Memory-safe sandbox with explicit capability grants
//! - **Fast**: Near-native performance with AOT compilation
//! - **Portable**: Same module runs on Ferrite, CDN edges, browsers
//! - **Isolated**: Each function runs in its own sandbox
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     WASM Runtime (wasmtime)                     │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  ┌──────────────────────────────────────────────────────────┐   │
//! │  │                    Instance Pool                          │   │
//! │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐               │   │
//! │  │  │Instance 1│  │Instance 2│  │Instance N│  (warm start) │   │
//! │  │  └────┬─────┘  └────┬─────┘  └────┬─────┘               │   │
//! │  └───────│─────────────│─────────────│──────────────────────┘   │
//! │          └─────────────┼─────────────┘                          │
//! │                        ▼                                         │
//! │  ┌──────────────────────────────────────────────────────────┐   │
//! │  │                   Host Functions                          │   │
//! │  │   ferrite_get()  ferrite_set()  ferrite_hget()  ...      │   │
//! │  └──────────────────────────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Quick Start
//!
//! ### Loading a WASM Function
//!
//! ```no_run
//! use ferrite::wasm::{FunctionRegistry, FunctionMetadata, FunctionPermissions, ResourceLimits};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create function registry
//! let registry = FunctionRegistry::new()?;
//!
//! // Load WASM bytes (compiled from Rust, Go, etc.)
//! let wasm_bytes = std::fs::read("./wasm/validate_user.wasm")?;
//!
//! // Define function metadata
//! let metadata = FunctionMetadata {
//!     name: "validate_user".to_string(),
//!     description: Some("Validates user data before storing".to_string()),
//!     permissions: FunctionPermissions {
//!         read_keys: vec!["user:*".to_string()],
//!         write_keys: vec![],  // Read-only validation
//!         allow_network: false,
//!         allow_time: true,
//!     },
//!     resource_limits: ResourceLimits {
//!         max_memory: 16 * 1024 * 1024,  // 16MB
//!         max_fuel: 100_000_000,          // ~100ms CPU
//!         max_execution_time_ms: 1000,    // 1 second timeout
//!     },
//! };
//!
//! // Load function
//! registry.load("validate_user", &wasm_bytes, metadata)?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Calling a WASM Function
//!
//! ```no_run
//! use ferrite::wasm::{FunctionRegistry, WasmValue};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let registry = FunctionRegistry::new()?;
//!
//! // Call function with arguments
//! let result = registry.call(
//!     "validate_user",
//!     &["user:123"],  // Keys the function will access
//!     &[
//!         WasmValue::String(r#"{"name": "Alice", "age": 30}"#.to_string()),
//!     ],
//! ).await?;
//!
//! match result {
//!     WasmValue::Bool(true) => println!("Validation passed"),
//!     WasmValue::Bool(false) => println!("Validation failed"),
//!     WasmValue::String(err) => println!("Error: {}", err),
//!     _ => println!("Unexpected result"),
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Writing WASM Functions (Rust)
//!
//! Create a function in Rust and compile to WASM:
//!
//! ```rust,ignore
//! // In your WASM project (Cargo.toml: crate-type = ["cdylib"])
//! use ferrite_wasm_sdk::*;
//!
//! #[ferrite_function]
//! fn validate_email(email: &str) -> bool {
//!     // Simple email validation
//!     email.contains('@') && email.contains('.')
//! }
//!
//! #[ferrite_function]
//! fn rate_limit(user_id: &str, limit: i64) -> Result<bool, String> {
//!     // Get current count
//!     let key = format!("ratelimit:{}", user_id);
//!     let count: i64 = ferrite::get(&key)
//!         .and_then(|v| v.parse().ok())
//!         .unwrap_or(0);
//!
//!     if count >= limit {
//!         return Ok(false);  // Rate limited
//!     }
//!
//!     // Increment and set TTL
//!     ferrite::incr(&key)?;
//!     ferrite::expire(&key, 60)?;  // Reset every minute
//!
//!     Ok(true)
//! }
//!
//! #[ferrite_function]
//! fn transform_json(input: &str) -> String {
//!     // Parse, transform, return
//!     let mut json: serde_json::Value = serde_json::from_str(input).expect("valid JSON input");
//!     json["processed"] = serde_json::Value::Bool(true);
//!     json["timestamp"] = serde_json::Value::Number(
//!         ferrite::time_ms().into()
//!     );
//!     serde_json::to_string(&json).expect("JSON serialization failed")
//! }
//! ```
//!
//! Compile with: `cargo build --target wasm32-unknown-unknown --release`
//!
//! ## Host Functions (Available to WASM)
//!
//! | Function | Description | Permission |
//! |----------|-------------|------------|
//! | `ferrite_get` | Get string value | read |
//! | `ferrite_set` | Set string value | write |
//! | `ferrite_del` | Delete key | write |
//! | `ferrite_exists` | Check key exists | read |
//! | `ferrite_incr` | Increment counter | write |
//! | `ferrite_hget` | Get hash field | read |
//! | `ferrite_hset` | Set hash field | write |
//! | `ferrite_lpush` | Push to list | write |
//! | `ferrite_rpop` | Pop from list | write |
//! | `ferrite_sadd` | Add to set | write |
//! | `ferrite_sismember` | Check set membership | read |
//! | `ferrite_log` | Write to log | always |
//! | `ferrite_time_ms` | Current time (ms) | time |
//!
//! ## Redis CLI Commands
//!
//! ```text
//! # Load a WASM function
//! FUNCTION.LOAD myfunction ./myfunction.wasm
//!
//! # Load with permissions
//! FUNCTION.LOAD myfunction ./myfunction.wasm
//!   READ user:* session:*
//!   WRITE audit:*
//!   MEMORY 16MB
//!   TIMEOUT 1000
//!
//! # Call a function
//! FCALL myfunction 1 user:123 "arg1" "arg2"
//!
//! # List functions
//! FUNCTION.LIST
//!
//! # Get function info
//! FUNCTION.INFO myfunction
//!
//! # Delete a function
//! FUNCTION.DELETE myfunction
//!
//! # Get execution stats
//! FUNCTION.STATS myfunction
//! ```
//!
//! ## Security Model
//!
//! ```no_run
//! use ferrite::wasm::{FunctionPermissions, SecurityConfig, DefaultPermissionLevel};
//!
//! // Minimal permissions for a validation function
//! let permissions = FunctionPermissions {
//!     read_keys: vec!["user:*".to_string()],   // Can read user keys
//!     write_keys: vec![],                       // Cannot write
//!     allow_network: false,                     // No network access
//!     allow_time: true,                         // Can get current time
//! };
//!
//! // Global security settings
//! let security = SecurityConfig {
//!     allow_network: false,      // Block all WASI network
//!     allow_filesystem: false,   // Block filesystem access
//!     default_permissions: DefaultPermissionLevel::ReadOnly,
//! };
//! ```
//!
//! ## Resource Limits
//!
//! ```no_run
//! use ferrite::wasm::ResourceLimits;
//!
//! let limits = ResourceLimits {
//!     max_memory: 64 * 1024 * 1024,    // 64MB memory limit
//!     max_fuel: 1_000_000_000,          // ~1 second CPU
//!     max_execution_time_ms: 5000,      // 5 second wall clock
//! };
//!
//! // Fuel is consumed per instruction:
//! // - Simple ops (add, load): 1 fuel
//! // - Memory ops: 1-10 fuel
//! // - Host calls: 100-1000 fuel
//! ```
//!
//! ## Instance Pooling
//!
//! Pre-warm instances for low latency:
//!
//! ```no_run
//! use ferrite::wasm::{WasmConfig, PoolConfig};
//!
//! let config = WasmConfig {
//!     pool: PoolConfig {
//!         min_instances: 4,           // Keep 4 warm instances
//!         max_instances: 32,          // Scale up to 32
//!         idle_timeout_secs: 300,     // Reclaim after 5 min idle
//!     },
//!     ..Default::default()
//! };
//! ```
//!
//! ## Performance
//!
//! | Metric | Cold Start | Warm Start |
//! |--------|------------|------------|
//! | Instantiation | 1-5ms | <10μs |
//! | Simple function | 10-50μs | 5-20μs |
//! | Complex function | 100μs-10ms | Based on CPU |
//!
//! ## Best Practices
//!
//! 1. **Minimize permissions**: Only grant access to required keys
//! 2. **Set resource limits**: Prevent runaway functions
//! 3. **Use pooling**: Keep instances warm for low latency
//! 4. **Compile with optimization**: Use `--release` and `wasm-opt`
//! 5. **Test locally**: Use `wasmtime` CLI before deploying
//! 6. **Log sparingly**: Logging adds overhead

mod executor;
mod host;
pub mod hot_reload;
pub mod observability;
mod registry;
mod types;

pub use executor::{ExecutionResult, WasmExecutor};
pub use host::{HostContext, HostFunction, NoopStoreBackend, StoreBackend};
pub use observability::{FunctionMetrics, FunctionMetricsSnapshot, WasmMetricsRegistry};
pub use registry::FunctionRegistry;
pub use types::{
    FunctionDependency, FunctionMetadata, FunctionPermissions, FunctionVersion, ResourceLimits,
    ResourceUsage, TypeDescriptor, WasmFunction, WasmValue,
};

use serde::{Deserialize, Serialize};

/// Configuration for WebAssembly functions
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WasmConfig {
    /// Enable WASM function support
    pub enabled: bool,
    /// Directory for WASM modules
    pub module_dir: String,
    /// Default resource limits
    pub limits: ResourceLimits,
    /// Instance pool configuration
    pub pool: PoolConfig,
    /// Security configuration
    pub security: SecurityConfig,
    /// Cache configuration
    pub cache: CacheConfig,
    /// Maximum number of WASM instances across all modules
    pub max_instances_per_server: usize,
    /// Hot reload configuration
    pub hot_reload: HotReloadConfig,
}

impl Default for WasmConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            module_dir: "./wasm".to_string(),
            limits: ResourceLimits::default(),
            pool: PoolConfig::default(),
            security: SecurityConfig::default(),
            cache: CacheConfig::default(),
            max_instances_per_server: 256,
            hot_reload: HotReloadConfig::default(),
        }
    }
}

/// Instance pool configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PoolConfig {
    /// Minimum instances to keep warm
    pub min_instances: usize,
    /// Maximum instances
    pub max_instances: usize,
    /// Idle timeout in seconds
    pub idle_timeout_secs: u64,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_instances: 2,
            max_instances: 16,
            idle_timeout_secs: 300,
        }
    }
}

/// Security configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SecurityConfig {
    /// Allow network access (WASI)
    pub allow_network: bool,
    /// Allow filesystem access (WASI)
    pub allow_filesystem: bool,
    /// Default permission level
    pub default_permissions: DefaultPermissionLevel,
    /// Isolate environment variables from WASM modules
    pub env_isolation: bool,
    /// WASI capability grants
    pub wasi_capabilities: WasiCapabilities,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            allow_network: false,
            allow_filesystem: false,
            default_permissions: DefaultPermissionLevel::ReadOnly,
            env_isolation: true,
            wasi_capabilities: WasiCapabilities::default(),
        }
    }
}

/// WASI capability-based security grants
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WasiCapabilities {
    /// Allowed filesystem directories (preopen dirs), empty = no access
    pub preopened_dirs: Vec<String>,
    /// Allowed environment variables, empty = none exposed
    pub allowed_env_vars: Vec<String>,
    /// Allow stdout/stderr
    pub allow_stdio: bool,
    /// Allow clock/time access
    pub allow_clock: bool,
    /// Allow random number generation
    pub allow_random: bool,
}

impl Default for WasiCapabilities {
    fn default() -> Self {
        Self {
            preopened_dirs: Vec::new(),
            allowed_env_vars: Vec::new(),
            allow_stdio: false,
            allow_clock: true,
            allow_random: true,
        }
    }
}

impl WasiCapabilities {
    /// Create fully locked-down capabilities
    pub fn deny_all() -> Self {
        Self {
            preopened_dirs: Vec::new(),
            allowed_env_vars: Vec::new(),
            allow_stdio: false,
            allow_clock: false,
            allow_random: false,
        }
    }
}

/// Default permission level for new functions
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DefaultPermissionLevel {
    /// Read-only access to data
    ReadOnly,
    /// Read and write access
    ReadWrite,
    /// Full access (includes admin commands)
    Full,
}

/// Cache configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Enable compiled module caching
    pub enabled: bool,
    /// Directory for cached compilations
    pub compiled_cache_dir: String,
    /// Maximum cache size in bytes
    pub max_cache_size: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            compiled_cache_dir: "./cache/wasm".to_string(),
            max_cache_size: 1024 * 1024 * 1024, // 1GB
        }
    }
}

/// Hot reload configuration for WASM modules
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HotReloadConfig {
    /// Enable hot reload watching
    pub enabled: bool,
    /// Poll interval for checking file changes
    pub poll_interval_secs: u64,
    /// Maximum time to wait for in-flight calls to finish during swap
    pub drain_timeout_secs: u64,
    /// Number of previous versions to keep for rollback
    pub rollback_versions: usize,
}

impl Default for HotReloadConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval_secs: 5,
            drain_timeout_secs: 30,
            rollback_versions: 3,
        }
    }
}

/// Errors that can occur in WASM operations
#[derive(Debug, Clone, thiserror::Error)]
pub enum WasmError {
    /// Function not found
    #[error("function not found: {0}")]
    FunctionNotFound(String),

    /// Function already exists
    #[error("function already exists: {0}")]
    FunctionExists(String),

    /// Invalid WASM module
    #[error("invalid WASM module: {0}")]
    InvalidModule(String),

    /// Compilation error
    #[error("compilation error: {0}")]
    CompilationError(String),

    /// Execution error
    #[error("execution error: {0}")]
    ExecutionError(String),

    /// Permission denied
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// Resource limit exceeded
    #[error("resource limit exceeded: {0}")]
    ResourceLimitExceeded(String),

    /// Fuel exhausted (instruction limit)
    #[error("fuel exhausted: execution exceeded instruction limit")]
    FuelExhausted,

    /// Timeout
    #[error("execution timeout after {0}ms")]
    Timeout(u64),

    /// Memory limit exceeded
    #[error("memory limit exceeded: {limit} bytes")]
    MemoryLimitExceeded {
        /// The limit that was exceeded
        limit: usize,
    },

    /// Host function error
    #[error("host function error: {0}")]
    HostError(String),

    /// Invalid argument
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// Internal error
    #[error("internal error: {0}")]
    Internal(String),

    /// Instance limit exceeded
    #[error("instance limit exceeded: maximum {limit} instances per server")]
    InstanceLimitExceeded {
        /// The limit that was exceeded
        limit: usize,
    },

    /// Dependency not satisfied
    #[error("dependency not satisfied: {0}")]
    DependencyNotSatisfied(String),

    /// Version conflict
    #[error("version conflict: {0}")]
    VersionConflict(String),

    /// Hot reload failure
    #[error("hot reload failed: {0}")]
    HotReloadFailed(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wasm_config_default() {
        let config = WasmConfig::default();
        assert!(config.enabled);
        assert_eq!(config.module_dir, "./wasm");
        assert_eq!(config.pool.min_instances, 2);
    }

    #[test]
    fn test_security_config_default() {
        let config = SecurityConfig::default();
        assert!(!config.allow_network);
        assert!(!config.allow_filesystem);
        assert_eq!(config.default_permissions, DefaultPermissionLevel::ReadOnly);
        assert!(config.env_isolation);
        assert!(config.wasi_capabilities.preopened_dirs.is_empty());
        assert!(config.wasi_capabilities.allowed_env_vars.is_empty());
    }

    #[test]
    fn test_wasi_capabilities_deny_all() {
        let caps = WasiCapabilities::deny_all();
        assert!(!caps.allow_stdio);
        assert!(!caps.allow_clock);
        assert!(!caps.allow_random);
        assert!(caps.preopened_dirs.is_empty());
    }

    #[test]
    fn test_hot_reload_config_default() {
        let config = HotReloadConfig::default();
        assert!(config.enabled);
        assert_eq!(config.poll_interval_secs, 5);
        assert_eq!(config.drain_timeout_secs, 30);
        assert_eq!(config.rollback_versions, 3);
    }

    #[test]
    fn test_resource_limits_default() {
        let limits = ResourceLimits::default();
        assert_eq!(limits.max_memory, 64 * 1024 * 1024);
        assert!(limits.max_fuel > 0);
    }

    #[test]
    fn test_wasm_config_max_instances() {
        let config = WasmConfig::default();
        assert_eq!(config.max_instances_per_server, 256);
    }

    #[test]
    fn test_wasm_error_variants() {
        let err = WasmError::InstanceLimitExceeded { limit: 256 };
        assert!(err.to_string().contains("256"));

        let err = WasmError::DependencyNotSatisfied("helper v1.0".to_string());
        assert!(err.to_string().contains("helper"));

        let err = WasmError::VersionConflict("v1 vs v2".to_string());
        assert!(err.to_string().contains("v1 vs v2"));

        let err = WasmError::HotReloadFailed("bad module".to_string());
        assert!(err.to_string().contains("bad module"));
    }
}
