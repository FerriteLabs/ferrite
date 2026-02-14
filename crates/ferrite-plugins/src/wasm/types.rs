//! WASM function types and metadata

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::{Duration, SystemTime};

/// A loaded WASM function
#[derive(Clone)]
pub struct WasmFunction {
    /// Function metadata
    pub metadata: FunctionMetadata,
    /// Compiled WASM bytes
    pub wasm_bytes: Vec<u8>,
    /// Exported function names
    pub exports: Vec<String>,
}

impl WasmFunction {
    /// Create a new WASM function
    pub fn new(metadata: FunctionMetadata, wasm_bytes: Vec<u8>) -> Self {
        Self {
            metadata,
            wasm_bytes,
            exports: Vec::new(),
        }
    }

    /// Set exported function names
    pub fn with_exports(mut self, exports: Vec<String>) -> Self {
        self.exports = exports;
        self
    }

    /// Get the function name
    pub fn name(&self) -> &str {
        &self.metadata.name
    }

    /// Get the source hash
    pub fn source_hash(&self) -> &str {
        &self.metadata.source_hash
    }
}

/// Metadata for a WASM function
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FunctionMetadata {
    /// Function name
    pub name: String,
    /// Description
    pub description: Option<String>,
    /// Author of the function
    pub author: Option<String>,
    /// Semantic version string
    pub version: FunctionVersion,
    /// Input type descriptors
    pub input_types: Vec<TypeDescriptor>,
    /// Output type descriptors
    pub output_types: Vec<TypeDescriptor>,
    /// Permissions granted to this function
    pub permissions: FunctionPermissions,
    /// Resource limits
    pub limits: ResourceLimits,
    /// Creation timestamp
    pub created_at: SystemTime,
    /// Last modified timestamp
    pub modified_at: SystemTime,
    /// Source hash for versioning
    pub source_hash: String,
    /// Dependencies on other functions
    pub dependencies: Vec<FunctionDependency>,
    /// Number of times called
    pub call_count: u64,
    /// Total execution time
    pub total_execution_time_ms: u64,
}

impl FunctionMetadata {
    /// Create new metadata with defaults
    pub fn new(name: String, source_hash: String) -> Self {
        let now = SystemTime::now();
        Self {
            name,
            description: None,
            author: None,
            version: FunctionVersion::default(),
            input_types: Vec::new(),
            output_types: Vec::new(),
            permissions: FunctionPermissions::default(),
            limits: ResourceLimits::default(),
            created_at: now,
            modified_at: now,
            source_hash,
            dependencies: Vec::new(),
            call_count: 0,
            total_execution_time_ms: 0,
        }
    }

    /// Set description
    pub fn with_description(mut self, description: String) -> Self {
        self.description = Some(description);
        self
    }

    /// Set author
    pub fn with_author(mut self, author: String) -> Self {
        self.author = Some(author);
        self
    }

    /// Set version
    pub fn with_version(mut self, version: FunctionVersion) -> Self {
        self.version = version;
        self
    }

    /// Set input types
    pub fn with_input_types(mut self, types: Vec<TypeDescriptor>) -> Self {
        self.input_types = types;
        self
    }

    /// Set output types
    pub fn with_output_types(mut self, types: Vec<TypeDescriptor>) -> Self {
        self.output_types = types;
        self
    }

    /// Set permissions
    pub fn with_permissions(mut self, permissions: FunctionPermissions) -> Self {
        self.permissions = permissions;
        self
    }

    /// Set resource limits
    pub fn with_limits(mut self, limits: ResourceLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Set dependencies
    pub fn with_dependencies(mut self, deps: Vec<FunctionDependency>) -> Self {
        self.dependencies = deps;
        self
    }

    /// Record a function call
    pub fn record_call(&mut self, execution_time_ms: u64) {
        self.call_count += 1;
        self.total_execution_time_ms += execution_time_ms;
    }

    /// Get average execution time
    pub fn avg_execution_time_ms(&self) -> f64 {
        if self.call_count == 0 {
            0.0
        } else {
            self.total_execution_time_ms as f64 / self.call_count as f64
        }
    }
}

/// Permissions for a WASM function
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FunctionPermissions {
    /// Allowed key patterns (glob)
    pub allowed_keys: Vec<String>,
    /// Denied key patterns (takes precedence)
    pub denied_keys: Vec<String>,
    /// Allowed commands
    pub allowed_commands: HashSet<String>,
    /// Denied commands (takes precedence)
    pub denied_commands: HashSet<String>,
    /// Can call external URLs?
    pub allow_network: bool,
    /// Can write data?
    pub allow_write: bool,
    /// Can call admin commands?
    pub allow_admin: bool,
}

impl Default for FunctionPermissions {
    fn default() -> Self {
        Self {
            allowed_keys: vec!["*".to_string()], // All keys by default
            denied_keys: Vec::new(),
            allowed_commands: HashSet::new(), // Empty means all allowed
            denied_commands: HashSet::new(),
            allow_network: false,
            allow_write: true,
            allow_admin: false,
        }
    }
}

impl FunctionPermissions {
    /// Create read-only permissions
    pub fn read_only() -> Self {
        Self {
            allow_write: false,
            ..Default::default()
        }
    }

    /// Create full permissions
    pub fn full() -> Self {
        Self {
            allow_network: true,
            allow_write: true,
            allow_admin: true,
            ..Default::default()
        }
    }

    /// Check if a key is allowed
    pub fn is_key_allowed(&self, key: &str) -> bool {
        // Check denied patterns first
        for pattern in &self.denied_keys {
            if glob_match(pattern, key) {
                return false;
            }
        }

        // Check allowed patterns
        if self.allowed_keys.is_empty() {
            return true; // Empty means all allowed
        }

        for pattern in &self.allowed_keys {
            if glob_match(pattern, key) {
                return true;
            }
        }

        false
    }

    /// Check if a command is allowed
    pub fn is_command_allowed(&self, command: &str) -> bool {
        let cmd_upper = command.to_uppercase();

        // Check denied commands first
        if self.denied_commands.contains(&cmd_upper) {
            return false;
        }

        // Check if it's an admin command
        if is_admin_command(&cmd_upper) && !self.allow_admin {
            return false;
        }

        // Check if it's a write command
        if is_write_command(&cmd_upper) && !self.allow_write {
            return false;
        }

        // Check allowed commands
        if self.allowed_commands.is_empty() {
            return true; // Empty means all allowed
        }

        self.allowed_commands.contains(&cmd_upper)
    }
}

/// Simple glob pattern matching supporting * as wildcard
fn glob_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    // Handle patterns with * in the middle (e.g., "user:*:password")
    if let Some(star_pos) = pattern.find('*') {
        let prefix = &pattern[..star_pos];
        let suffix = &pattern[star_pos + 1..];

        // Text must start with prefix and end with suffix
        if suffix.is_empty() {
            // Pattern ends with *, just check prefix
            return text.starts_with(prefix);
        } else if prefix.is_empty() {
            // Pattern starts with *, just check suffix
            return text.ends_with(suffix);
        } else {
            // Pattern has both prefix and suffix around *
            // e.g., "user:*:password" should match "user:123:password"
            if !text.starts_with(prefix) {
                return false;
            }
            if !text.ends_with(suffix) {
                return false;
            }
            // Ensure the text is long enough
            return text.len() >= prefix.len() + suffix.len();
        }
    }

    pattern == text
}

/// Check if a command is an admin command
fn is_admin_command(cmd: &str) -> bool {
    matches!(
        cmd,
        "FLUSHDB"
            | "FLUSHALL"
            | "SHUTDOWN"
            | "CONFIG"
            | "DEBUG"
            | "BGSAVE"
            | "BGREWRITEAOF"
            | "SLAVEOF"
            | "REPLICAOF"
            | "CLUSTER"
            | "MODULE"
    )
}

/// Check if a command modifies data
fn is_write_command(cmd: &str) -> bool {
    matches!(
        cmd,
        "SET"
            | "SETNX"
            | "SETEX"
            | "PSETEX"
            | "MSET"
            | "MSETNX"
            | "APPEND"
            | "INCR"
            | "INCRBY"
            | "INCRBYFLOAT"
            | "DECR"
            | "DECRBY"
            | "DEL"
            | "UNLINK"
            | "EXPIRE"
            | "EXPIREAT"
            | "PEXPIRE"
            | "PEXPIREAT"
            | "PERSIST"
            | "RENAME"
            | "RENAMENX"
            | "LPUSH"
            | "RPUSH"
            | "LPOP"
            | "RPOP"
            | "LSET"
            | "LINSERT"
            | "LTRIM"
            | "LREM"
            | "HSET"
            | "HSETNX"
            | "HMSET"
            | "HINCRBY"
            | "HINCRBYFLOAT"
            | "HDEL"
            | "SADD"
            | "SREM"
            | "SPOP"
            | "SMOVE"
            | "ZADD"
            | "ZREM"
            | "ZINCRBY"
            | "ZPOPMIN"
            | "ZPOPMAX"
            | "XADD"
            | "XTRIM"
            | "XDEL"
    )
}

/// Resource limits for WASM execution
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourceLimits {
    /// Maximum memory in bytes
    pub max_memory: usize,
    /// Maximum execution time
    pub max_time: Duration,
    /// Maximum fuel (instruction count)
    pub max_fuel: u64,
    /// Maximum stack depth
    pub max_stack: u32,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_memory: 64 * 1024 * 1024,     // 64MB
            max_time: Duration::from_secs(5), // 5 seconds
            max_fuel: 10_000_000,             // ~10M instructions
            max_stack: 128,
        }
    }
}

impl ResourceLimits {
    /// Create limits for quick functions
    pub fn quick() -> Self {
        Self {
            max_memory: 4 * 1024 * 1024,          // 4MB
            max_time: Duration::from_millis(100), // 100ms
            max_fuel: 1_000_000,                  // 1M instructions
            max_stack: 64,
        }
    }

    /// Create limits for batch operations
    pub fn batch() -> Self {
        Self {
            max_memory: 64 * 1024 * 1024,      // 64MB
            max_time: Duration::from_secs(30), // 30 seconds
            max_fuel: 100_000_000,             // 100M instructions
            max_stack: 256,
        }
    }
}

/// Semantic version for WASM functions
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FunctionVersion {
    /// Major version
    pub major: u32,
    /// Minor version
    pub minor: u32,
    /// Patch version
    pub patch: u32,
}

impl FunctionVersion {
    /// Create a new version
    pub fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// Parse from a string like "1.2.3"
    pub fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 3 {
            return None;
        }
        Some(Self {
            major: parts[0].parse().ok()?,
            minor: parts[1].parse().ok()?,
            patch: parts[2].parse().ok()?,
        })
    }

    /// Check if this version is compatible with `other` (same major)
    pub fn is_compatible(&self, other: &Self) -> bool {
        self.major == other.major
    }
}

impl Default for FunctionVersion {
    fn default() -> Self {
        Self {
            major: 0,
            minor: 1,
            patch: 0,
        }
    }
}

impl std::fmt::Display for FunctionVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Dependency on another WASM function
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FunctionDependency {
    /// Name of the required function
    pub name: String,
    /// Minimum version required (if any)
    pub min_version: Option<FunctionVersion>,
    /// Whether this dependency is optional
    pub optional: bool,
}

impl FunctionDependency {
    /// Create a required dependency
    pub fn required(name: String) -> Self {
        Self {
            name,
            min_version: None,
            optional: false,
        }
    }

    /// Create an optional dependency
    pub fn optional(name: String) -> Self {
        Self {
            name,
            min_version: None,
            optional: true,
        }
    }

    /// Set minimum version
    pub fn with_min_version(mut self, version: FunctionVersion) -> Self {
        self.min_version = Some(version);
        self
    }
}

/// Type descriptor for function inputs/outputs
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TypeDescriptor {
    /// 32-bit integer
    I32,
    /// 64-bit integer
    I64,
    /// 32-bit float
    F32,
    /// 64-bit float
    F64,
    /// Byte array / string
    Bytes,
    /// JSON value
    Json,
    /// Boolean
    Bool,
}

/// Resource usage tracking
#[derive(Clone, Debug, Default)]
pub struct ResourceUsage {
    /// Memory used in bytes
    pub memory_used: usize,
    /// Peak memory usage
    pub memory_peak: usize,
    /// Fuel consumed
    pub fuel_consumed: u64,
    /// Execution time
    pub execution_time: Duration,
    /// Host function calls
    pub host_calls: u64,
}

impl ResourceUsage {
    /// Create new resource usage tracker
    pub fn new() -> Self {
        Self::default()
    }

    /// Record memory usage
    pub fn record_memory(&mut self, bytes: usize) {
        self.memory_used = bytes;
        if bytes > self.memory_peak {
            self.memory_peak = bytes;
        }
    }

    /// Record fuel consumption
    pub fn record_fuel(&mut self, fuel: u64) {
        self.fuel_consumed = fuel;
    }

    /// Record a host function call
    pub fn record_host_call(&mut self) {
        self.host_calls += 1;
    }

    /// Set execution time
    pub fn set_execution_time(&mut self, time: Duration) {
        self.execution_time = time;
    }
}

/// WASM value type for function arguments and returns
#[derive(Clone, Debug, PartialEq)]
pub enum WasmValue {
    /// 32-bit integer
    I32(i32),
    /// 64-bit integer
    I64(i64),
    /// 32-bit float
    F32(f32),
    /// 64-bit float
    F64(f64),
    /// Byte array (for strings/data)
    Bytes(Vec<u8>),
}

impl WasmValue {
    /// Get as i32
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            WasmValue::I32(v) => Some(*v),
            _ => None,
        }
    }

    /// Get as i64
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            WasmValue::I64(v) => Some(*v),
            _ => None,
        }
    }

    /// Get as bytes
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            WasmValue::Bytes(v) => Some(v),
            _ => None,
        }
    }

    /// Get as string
    pub fn as_str(&self) -> Option<&str> {
        match self {
            WasmValue::Bytes(v) => std::str::from_utf8(v).ok(),
            _ => None,
        }
    }
}

impl From<i32> for WasmValue {
    fn from(v: i32) -> Self {
        WasmValue::I32(v)
    }
}

impl From<i64> for WasmValue {
    fn from(v: i64) -> Self {
        WasmValue::I64(v)
    }
}

impl From<f32> for WasmValue {
    fn from(v: f32) -> Self {
        WasmValue::F32(v)
    }
}

impl From<f64> for WasmValue {
    fn from(v: f64) -> Self {
        WasmValue::F64(v)
    }
}

impl From<Vec<u8>> for WasmValue {
    fn from(v: Vec<u8>) -> Self {
        WasmValue::Bytes(v)
    }
}

impl From<&str> for WasmValue {
    fn from(v: &str) -> Self {
        WasmValue::Bytes(v.as_bytes().to_vec())
    }
}

impl From<String> for WasmValue {
    fn from(v: String) -> Self {
        WasmValue::Bytes(v.into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_function_metadata() {
        let mut metadata = FunctionMetadata::new("test".to_string(), "abc123".to_string())
            .with_description("Test function".to_string());

        assert_eq!(metadata.name, "test");
        assert_eq!(metadata.description, Some("Test function".to_string()));
        assert_eq!(metadata.call_count, 0);

        metadata.record_call(100);
        metadata.record_call(200);

        assert_eq!(metadata.call_count, 2);
        assert_eq!(metadata.total_execution_time_ms, 300);
        assert_eq!(metadata.avg_execution_time_ms(), 150.0);
    }

    #[test]
    fn test_permissions_key_allowed() {
        let perms = FunctionPermissions {
            allowed_keys: vec!["user:*".to_string()],
            denied_keys: vec!["user:*:password".to_string()],
            ..Default::default()
        };

        assert!(perms.is_key_allowed("user:123"));
        assert!(perms.is_key_allowed("user:123:name"));
        assert!(!perms.is_key_allowed("user:123:password"));
        assert!(!perms.is_key_allowed("other:key"));
    }

    #[test]
    fn test_permissions_command_allowed() {
        let perms = FunctionPermissions {
            allow_write: false,
            allow_admin: false,
            ..Default::default()
        };

        assert!(perms.is_command_allowed("GET"));
        assert!(perms.is_command_allowed("HGET"));
        assert!(!perms.is_command_allowed("SET"));
        assert!(!perms.is_command_allowed("DEL"));
        assert!(!perms.is_command_allowed("FLUSHDB"));
    }

    #[test]
    fn test_permissions_read_only() {
        let perms = FunctionPermissions::read_only();

        assert!(!perms.allow_write);
        assert!(!perms.allow_admin);
        assert!(!perms.allow_network);
        assert!(perms.is_command_allowed("GET"));
        assert!(!perms.is_command_allowed("SET"));
    }

    #[test]
    fn test_permissions_full() {
        let perms = FunctionPermissions::full();

        assert!(perms.allow_write);
        assert!(perms.allow_admin);
        assert!(perms.allow_network);
    }

    #[test]
    fn test_resource_limits_default() {
        let limits = ResourceLimits::default();

        assert_eq!(limits.max_memory, 64 * 1024 * 1024);
        assert_eq!(limits.max_time, Duration::from_secs(5));
        assert_eq!(limits.max_fuel, 10_000_000);
    }

    #[test]
    fn test_resource_limits_quick() {
        let limits = ResourceLimits::quick();

        assert_eq!(limits.max_memory, 4 * 1024 * 1024);
        assert_eq!(limits.max_time, Duration::from_millis(100));
    }

    #[test]
    fn test_resource_usage() {
        let mut usage = ResourceUsage::new();

        usage.record_memory(1000);
        usage.record_memory(2000);
        usage.record_memory(1500);

        assert_eq!(usage.memory_used, 1500);
        assert_eq!(usage.memory_peak, 2000);

        usage.record_host_call();
        usage.record_host_call();

        assert_eq!(usage.host_calls, 2);
    }

    #[test]
    fn test_wasm_value() {
        let i32_val = WasmValue::from(42i32);
        assert_eq!(i32_val.as_i32(), Some(42));

        let str_val = WasmValue::from("hello");
        assert_eq!(str_val.as_str(), Some("hello"));

        let bytes_val = WasmValue::from(vec![1u8, 2, 3]);
        assert_eq!(bytes_val.as_bytes(), Some(&[1u8, 2, 3][..]));
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("user:*", "user:123"));
        assert!(glob_match("*:suffix", "prefix:suffix"));
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("user:*", "other:123"));
    }

    #[test]
    fn test_wasm_function() {
        let metadata = FunctionMetadata::new("test".to_string(), "hash".to_string());
        let func = WasmFunction::new(metadata, vec![0, 97, 115, 109]) // WASM magic bytes
            .with_exports(vec!["run".to_string(), "init".to_string()]);

        assert_eq!(func.name(), "test");
        assert_eq!(func.exports.len(), 2);
    }

    #[test]
    fn test_function_version() {
        let v = FunctionVersion::new(1, 2, 3);
        assert_eq!(v.to_string(), "1.2.3");

        let parsed = FunctionVersion::parse("1.2.3").unwrap();
        assert_eq!(parsed, v);

        assert!(FunctionVersion::parse("invalid").is_none());
        assert!(FunctionVersion::parse("1.2").is_none());
    }

    #[test]
    fn test_function_version_compatibility() {
        let v1 = FunctionVersion::new(1, 0, 0);
        let v1_1 = FunctionVersion::new(1, 1, 0);
        let v2 = FunctionVersion::new(2, 0, 0);

        assert!(v1.is_compatible(&v1_1));
        assert!(!v1.is_compatible(&v2));
    }

    #[test]
    fn test_function_dependency() {
        let dep = FunctionDependency::required("helper".to_string())
            .with_min_version(FunctionVersion::new(1, 0, 0));

        assert_eq!(dep.name, "helper");
        assert!(!dep.optional);
        assert_eq!(dep.min_version, Some(FunctionVersion::new(1, 0, 0)));

        let opt = FunctionDependency::optional("utils".to_string());
        assert!(opt.optional);
    }

    #[test]
    fn test_metadata_with_version_and_author() {
        let metadata = FunctionMetadata::new("test".to_string(), "hash".to_string())
            .with_author("alice".to_string())
            .with_version(FunctionVersion::new(1, 0, 0))
            .with_input_types(vec![TypeDescriptor::I32, TypeDescriptor::I32])
            .with_output_types(vec![TypeDescriptor::I32])
            .with_dependencies(vec![FunctionDependency::required("helper".to_string())]);

        assert_eq!(metadata.author, Some("alice".to_string()));
        assert_eq!(metadata.version, FunctionVersion::new(1, 0, 0));
        assert_eq!(metadata.input_types.len(), 2);
        assert_eq!(metadata.output_types.len(), 1);
        assert_eq!(metadata.dependencies.len(), 1);
    }
}
